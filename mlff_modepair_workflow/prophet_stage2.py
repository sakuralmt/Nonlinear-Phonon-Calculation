"""Uniform 9x9 v3 Stage2 with atomic point checkpoints and safe sharding."""

from __future__ import annotations

import argparse
import csv
import fcntl
import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np

from .core import ModePairFrozenPhononBuilder, analyze_pair_grid, load_atoms_from_qe
from .prophet_backend import make_prophet_calculator, process_resource_metrics, sha256_file
from .units import CONTRACT_VERSION, NORMALIZATION_VERSION, UNITS


AXES = np.linspace(-2.0, 2.0, 9)
FIT_WINDOW = 1.0


def _atomic_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    with temp.open("w") as handle:
        json.dump(payload, handle, indent=2, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def _ensure_run_signature(output_dir: Path, signature: dict, model_meta: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    signature_path = output_dir / "run_signature.json"
    with (output_dir / ".run_signature.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if signature_path.exists():
            if json.loads(signature_path.read_text()).get("signature") != signature:
                raise ValueError("Output directory already contains results from different inputs")
        else:
            _atomic_json(signature_path, {"signature": signature, "backend": model_meta})


def _signature(pair_source: dict, structure: Path, model_meta: dict) -> dict:
    signature = {
        "version": CONTRACT_VERSION,
        "mode_pairs_sha256": pair_source["mode_pairs_sha256"],
        "structure_sha256": sha256_file(structure),
        "checkpoint_sha256": model_meta["checkpoint_sha256"],
        "source_commit": model_meta["source_commit"],
        "energy_accumulation": model_meta["energy_accumulation"],
        "geometry_source": pair_source["geometry_source"],
        "normalization_version": NORMALIZATION_VERSION,
        "a1_values": AXES.tolist(),
        "a2_values": AXES.tolist(),
        "fit_window": FIT_WINDOW,
    }
    # Keep signatures of already-running Prophet campaigns byte-for-byte stable.
    if model_meta["backend"] != "prophet":
        signature["backend"] = model_meta["backend"]
    return signature


def _open_checkpoint(path: Path, signature: dict, pair_code: str) -> dict:
    if not path.exists():
        return {
            "kind": f"{signature.get('backend', 'prophet')}_pair_checkpoint", "signature": signature,
            "pair_code": pair_code, "energies_ev_supercell": [[None] * 9 for _ in range(9)],
            "elapsed_seconds": 0.0,
        }
    payload = json.loads(path.read_text())
    if payload.get("signature") != signature or payload.get("pair_code") != pair_code:
        raise ValueError(f"Checkpoint input signature mismatch: {path}")
    grid = payload.get("energies_ev_supercell")
    if not isinstance(grid, list) or len(grid) != 9 or any(not isinstance(row, list) or len(row) != 9 for row in grid):
        raise ValueError(f"Damaged checkpoint grid: {path}")
    for row in grid:
        for value in row:
            if value is not None and not np.isfinite(value):
                raise ValueError(f"Nonfinite checkpoint energy: {path}")
    return payload


def _quality_flags(analysis: dict, pair: dict, energy_grid: np.ndarray):
    flags = []
    if analysis["fit_design_rank"] != 13 or analysis["fit_condition_number"] > 1e4:
        flags.append("ill_conditioned_fit")
    if abs(pair["gamma_mode"]["freq_thz"]) < 0.5:
        flags.append("gamma_near_acoustic")
    if pair["target_mode"]["freq_thz"] < 0:
        flags.append("target_imaginary_harmonic_mode")
    if np.unique(energy_grid).size < 50:
        flags.append("repeated_energy_values")
    return flags


def _grid_sha256(grid: np.ndarray) -> str:
    return hashlib.sha256(np.ascontiguousarray(grid, dtype="<f8").tobytes()).hexdigest()


def _evaluate_pair_unlocked(pair: dict, primitive, calc, model_meta: dict, signature: dict, pair_dir: Path,
                            stop_after: int | None = None, global_count: list[int] | None = None):
    pair_dir.mkdir(parents=True, exist_ok=True)
    checkpoint_path = pair_dir / "checkpoint.json"
    checkpoint = _open_checkpoint(checkpoint_path, signature, pair["pair_code"])
    builder = ModePairFrozenPhononBuilder(pair, primitive)
    started = time.perf_counter()
    for row, a2 in enumerate(AXES):
        for col, a1 in enumerate(AXES):
            if checkpoint["energies_ev_supercell"][row][col] is not None:
                continue
            atoms = builder.build_atoms(float(a1), float(a2))
            atoms.calc = calc
            energy = float(atoms.get_potential_energy())
            if not np.isfinite(energy):
                raise ValueError(f"Nonfinite Stage2 energy for {pair['pair_code']} at ({a1}, {a2})")
            checkpoint["energies_ev_supercell"][row][col] = energy
            checkpoint["elapsed_seconds"] += time.perf_counter() - started
            _atomic_json(checkpoint_path, checkpoint)
            started = time.perf_counter()
            if global_count is not None:
                global_count[0] += 1
                if stop_after is not None and global_count[0] >= stop_after:
                    raise RuntimeError("Intentional diagnostic interruption after a saved grid point")
    grid = np.asarray(checkpoint["energies_ev_supercell"], dtype=float)
    if grid.shape != (9, 9) or not np.isfinite(grid).all():
        raise AssertionError("Incomplete energy grid was marked complete")
    analysis = analyze_pair_grid(pair, grid, AXES, AXES, fit_window=FIT_WINDOW)
    if analysis["fit_design_rank"] != 13 or not np.isfinite(analysis["fit_condition_number"]):
        raise ValueError(f"Unusable 13-parameter PES fit for {pair['pair_code']}")
    np.savetxt(pair_dir / "energy_grid_eV.dat", grid, fmt="%.12f")
    np.save(pair_dir / "energy_grid_eV.npy", grid)
    summary = {
        "kind": f"{model_meta['backend']}_pair_pes", "version": CONTRACT_VERSION,
        "pair_code": pair["pair_code"], "pair": pair, "signature": signature,
        "units": UNITS, "builder": builder.metadata(), "backend": model_meta,
        "a1_values": AXES.tolist(), "a2_values": AXES.tolist(),
        "analysis": analysis, "quality_flags": _quality_flags(analysis, pair, grid),
        "energy_grid_sha256": _grid_sha256(grid),
        "energy_resolution_diagnostics": {
            "unique_values": int(np.unique(grid).size),
            "minimum_nonzero_step_ev": float(np.min(np.diff(np.unique(grid)))) if np.unique(grid).size > 1 else None,
            "float32_total_energy_spacing_ev": float(abs(np.spacing(np.float32(np.median(grid))))),
        },
        "elapsed_seconds": checkpoint["elapsed_seconds"],
        "resources": process_resource_metrics(model_meta.get("device", "cpu")),
        "completed_points": 81,
    }
    _atomic_json(pair_dir / "summary.json", summary)
    return summary


def evaluate_pair(pair: dict, primitive, calc, model_meta: dict, signature: dict, pair_dir: Path,
                  stop_after: int | None = None, global_count: list[int] | None = None):
    pair_dir.mkdir(parents=True, exist_ok=True)
    with (pair_dir / ".pair.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        return _evaluate_pair_unlocked(pair, primitive, calc, model_meta, signature, pair_dir,
                                       stop_after=stop_after, global_count=global_count)


def _ranking_row(summary: dict):
    pair = summary["pair"]
    analysis = summary["analysis"]
    physical = analysis["physics"]
    first = analysis["axis_checks"]["mode1_axis_fit"]["freq"].get("thz")
    second = analysis["axis_checks"]["mode2_axis_fit"]["freq"].get("thz")
    q = pair["target_mode"]["q_frac"]
    return {
        "pair_code": pair["pair_code"], "coupling_type": pair["coupling_type"],
        "point_label": pair["target_mode"]["point_label"],
        "qx": q[0], "qy": q[1], "qz": q[2],
        "n_super": summary["builder"]["n_super"],
        "gamma_mode_code": pair["gamma_mode"]["mode_code"],
        "gamma_mode_number": pair["gamma_mode"]["mode_number_one_based"],
        "gamma_freq_ref_thz": pair["gamma_mode"]["freq_thz"],
        "gamma_freq_fit_thz": first,
        "gamma_freq_abs_err_thz": None if first is None else abs(first - pair["gamma_mode"]["freq_thz"]),
        "target_mode_code": pair["target_mode"]["mode_code"],
        "target_mode_number": pair["target_mode"]["mode_number_one_based"],
        "target_freq_ref_thz": pair["target_mode"]["freq_thz"],
        "target_freq_fit_thz": second,
        "target_freq_abs_err_thz": None if second is None else abs(second - pair["target_mode"]["freq_thz"]),
        "phi122_mev": physical["phi_122_mev_per_A3amu32"],
        "phi122_mev_per_A3amu32": physical["phi_122_mev_per_A3amu32"],
        "phi112_mev_per_A3amu32": physical["phi_112_mev_per_A3amu32"],
        "phi1122_mev_per_A4amu2": physical["phi_1122_mev_per_A4amu2"],
        "phi1111_mev_per_A4amu2": physical["phi_1111_mev_per_A4amu2"],
        "phi2222_mev_per_A4amu2": physical["phi_2222_mev_per_A4amu2"],
        "r2": analysis["r2"], "rmse_ev_supercell": analysis["rmse_ev_supercell"],
        "center_fit_r2": analysis["center_fit_r2"],
        "center_fit_rmse_ev_supercell": analysis["center_fit_rmse_ev_supercell"],
        "fit_design_rank": analysis["fit_design_rank"],
        "fit_condition_number": analysis["fit_condition_number"],
        "elapsed_sec": summary["elapsed_seconds"], "source_stage": "screening",
        "quality_flags": ";".join(summary["quality_flags"]),
    }


def finalize(output_dir: Path, pairs: list[dict], signature: dict, model_meta: dict | None = None):
    expected_codes = {pair["pair_code"] for pair in pairs}
    completed_codes = {path.parent.name for path in (output_dir / "pairs").glob("*/summary.json")}
    extra = sorted(completed_codes - expected_codes)
    if extra:
        raise ValueError(f"Output directory has results for unexpected pairs: {extra[:3]}")
    rows = []
    missing = []
    for pair in pairs:
        path = output_dir / "pairs" / pair["pair_code"] / "summary.json"
        if not path.exists():
            missing.append(pair["pair_code"])
            continue
        summary = json.loads(path.read_text())
        if (summary.get("signature") != signature or summary.get("completed_points") != 81
                or summary.get("pair_code") != pair["pair_code"]):
            raise ValueError(f"Invalid or mixed completed pair: {path}")
        checkpoint = _open_checkpoint(path.parent / "checkpoint.json", signature, pair["pair_code"])
        if any(value is None for row in checkpoint["energies_ev_supercell"] for value in row):
            raise ValueError(f"Incomplete grid checkpoint for completed pair: {path}")
        checkpoint_grid = np.asarray(checkpoint["energies_ev_supercell"], dtype=float)
        saved_grid = np.load(path.parent / "energy_grid_eV.npy", allow_pickle=False)
        if (summary.get("energy_grid_sha256") != _grid_sha256(checkpoint_grid)
                or summary["energy_grid_sha256"] != _grid_sha256(saved_grid)):
            raise ValueError(f"Energy grid checksum mismatch for completed pair: {path}")
        rows.append(_ranking_row(summary))
        if model_meta is None:
            model_meta = summary["backend"]
    if missing:
        raise RuntimeError(f"Cannot finalize: {len(missing)} of {len(pairs)} pairs missing; first={missing[:3]}")
    rows.sort(key=lambda row: (-abs(row["phi122_mev"]), row["pair_code"]))
    output_dir.mkdir(parents=True, exist_ok=True)
    fields = ["rank", *rows[0].keys()] if rows else ["rank"]
    temp = output_dir / f"pair_ranking.csv.tmp.{os.getpid()}"
    with temp.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for rank, row in enumerate(rows, start=1):
            writer.writerow({"rank": rank, **row})
    os.replace(temp, output_dir / "pair_ranking.csv")
    _atomic_json(output_dir / "pair_ranking.json", {
        "kind": f"{model_meta['backend']}_pair_ranking", "version": CONTRACT_VERSION,
        "signature": signature, "units": UNITS, "backend": model_meta,
        "pairs": [{"rank": rank, **row} for rank, row in enumerate(rows, start=1)],
    })
    _atomic_json(output_dir / "runtime_config_used.json", {
        "runtime": {"strategy": "uniform_full", "grid_size": 9, "fit_window": FIT_WINDOW,
                    "effective_batch_size": 1},
        "meta": {"no_pair_selection": True},
    })
    _atomic_json(output_dir / "run_meta.json", {
        "kind": f"{model_meta['backend']}_stage2_run", "version": CONTRACT_VERSION,
        "signature": signature, "n_pairs": len(rows), "n_energy_evaluations": 81 * len(rows),
        "backend": model_meta, "units": UNITS,
    })
    return output_dir / "pair_ranking.csv"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode-pairs-json", type=Path, required=True)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--backend", choices=["prophet", "mattersim"], default="prophet")
    parser.add_argument("--model", default="prophet_oame_mbd")
    parser.add_argument("--device", choices=["cpu", "cuda"], default="cpu")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--run-tag", default="prophet")
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--finalize-only", action="store_true")
    parser.add_argument("--limit", type=int, default=None, help="Diagnostic partial run; cannot produce a final ranking")
    parser.add_argument("--pair-code", type=str, default=None, help="Diagnostic single-pair run; cannot produce a final ranking")
    parser.add_argument("--diagnostic-stop-after", type=int, default=None)
    args = parser.parse_args(argv)
    if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
        parser.error("shard-index must be in [0, shard-count)")
    if args.backend == "mattersim" and args.model == "prophet_oame_mbd":
        parser.error("MatterSim requires an explicit --model checkpoint path")
    pair_file = args.mode_pairs_json.resolve()
    structure = args.structure.resolve()
    payload = json.loads(pair_file.read_text())
    if payload.get("version") != CONTRACT_VERSION or payload.get("source", {}).get("normalization_version") != NORMALIZATION_VERSION:
        raise ValueError("Stage2 requires v3 mode pairs with the real-unit-mass normalization")
    if payload["source"].get("structure_sha256") != sha256_file(structure):
        raise ValueError("Stage2 structure does not match the Stage1 structure hash")
    pairs = payload["pairs"]
    if len({row["pair_code"] for row in pairs}) != len(pairs):
        raise ValueError("Duplicate pair codes")
    output_dir = args.output_root.resolve() / args.run_tag / "screening"
    if args.finalize_only:
        existing = json.loads((output_dir / "run_signature.json").read_text())
        if existing["backend"]["backend"] != args.backend:
            raise ValueError("Finalization backend differs from the recorded Stage2 run")
        expected = _signature({"mode_pairs_sha256": sha256_file(pair_file),
                               "geometry_source": payload["source"].get("geometry_source", "unrecorded")},
                              structure, existing["backend"])
        if existing["signature"] != expected:
            raise ValueError("Finalization inputs differ from the recorded Stage2 run signature")
        print(finalize(output_dir, pairs, existing["signature"]))
        return 0
    primitive = load_atoms_from_qe(structure)
    if args.backend == "prophet":
        calc, model_meta = make_prophet_calculator(args.model, args.device, primitive)
    else:
        from .mattersim_backend import make_mattersim_calculator

        calc, model_meta = make_mattersim_calculator(args.model, args.device, primitive)
    pair_source = {
        "mode_pairs_sha256": sha256_file(pair_file),
        "geometry_source": payload["source"].get("geometry_source", "unrecorded"),
    }
    signature = _signature(pair_source, structure, model_meta)
    _ensure_run_signature(output_dir, signature, model_meta)
    selected = [pair for idx, pair in enumerate(pairs) if idx % args.shard_count == args.shard_index]
    if args.pair_code is not None:
        selected = [pair for pair in selected if pair["pair_code"] == args.pair_code]
        if not selected:
            raise ValueError(f"Unknown pair code in selected shard: {args.pair_code}")
    if args.limit is not None:
        selected = selected[:args.limit]
    counter = [0]
    for position, pair in enumerate(selected, start=1):
        summary = evaluate_pair(
            pair, primitive, calc, model_meta, signature, output_dir / "pairs" / pair["pair_code"],
            stop_after=args.diagnostic_stop_after, global_count=counter,
        )
        print(f"[{position}/{len(selected)}] {pair['pair_code']} 81/81 in {summary['elapsed_seconds']:.1f}s", flush=True)
    _atomic_json(output_dir / "shards" / f"shard_{args.shard_index:03d}_of_{args.shard_count:03d}.json", {
        "signature": signature, "completed_pairs": len(selected), "new_energy_evaluations": counter[0],
    })
    if args.shard_count == 1 and args.limit is None and args.pair_code is None:
        print(finalize(output_dir, pairs, signature, model_meta), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
