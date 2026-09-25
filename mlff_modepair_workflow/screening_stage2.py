"""Checkpointed MatterSim six-point screen and selected 5x5/9x9 PES fits."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import socket
import time
from pathlib import Path

import numpy as np

from .core import ModePairFrozenPhononBuilder, analyze_pair_grid, load_atoms_from_qe
from .mattersim_backend import ENERGY_ACCUMULATION, make_mattersim_calculator
from .prophet_backend import process_resource_metrics, sha256_file
from .units import CONTRACT_VERSION, NORMALIZATION_VERSION, UNITS


AXIS = np.arange(-2.0, 2.01, 0.5)
SIX = [(x, y) for x in (-1.0, 1.0) for y in (-1.0, 0.0, 1.0)]
CENTER = [
    (float(x), float(y)) for x in AXIS if abs(x) <= 1 for y in AXIS if abs(y) <= 1
]
FULL = [(float(x), float(y)) for x in AXIS for y in AXIS]


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w") as stream:
        json.dump(payload, stream, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def _key(x: float, y: float) -> str:
    # Sign changes under symmetry must not create a second key for zero.
    x, y = (0.0 if value == 0 else value for value in (x, y))
    return f"{x:+.1f},{y:+.1f}"


def _grid(points: dict, axis: np.ndarray) -> np.ndarray:
    return np.asarray(
        [[points[_key(float(x), float(y))] for x in axis] for y in axis], dtype=float
    )


def _proxy(points: dict) -> float:
    h = 1.0

    def curvature(x):
        return (
            points[_key(x, h)] - 2 * points[_key(x, 0)] + points[_key(x, -h)]
        ) / h**2

    return float(1000 * (curvature(h) - curvature(-h)) / (2 * h))


def _identity(
    pair_path: Path, structure: Path, checkpoint: Path, top_channels: int
) -> dict:
    return {
        "contract_version": CONTRACT_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "mode_pairs_sha256": sha256_file(pair_path),
        "structure_sha256": sha256_file(structure),
        "checkpoint_sha256": sha256_file(checkpoint),
        "energy_accumulation": ENERGY_ACCUMULATION,
        "top_channels": top_channels,
        "coarse_step": 1.0,
        "center_fit_window": 1.0,
        "axis": AXIS.tolist(),
    }


def validate_relaxed_stage1(payload: dict, structure: Path) -> None:
    """Require the same, traceable model-relaxed geometry in both stages."""
    source = payload.get("source", {})
    relaxation = source.get("relaxation", {})
    structure_hash = sha256_file(structure)
    if source.get("geometry_source") != "model_relaxed":
        raise ValueError("Stage2 requires a model-relaxed Stage1 structure")
    if source.get("symmetry", {}).get("covariance_contract") != "gamma_and_finite_q_v2":
        raise ValueError("Stage1 must verify Gamma and finite-q symmetry covariance")
    if (
        source.get("structure_sha256") != structure_hash
        or relaxation.get("optimized_structure_sha256") != structure_hash
    ):
        raise ValueError("Stage1, Stage2 and relaxation structures differ")
    summary_path = Path(relaxation.get("summary", ""))
    if not summary_path.is_file():
        summary_path = structure.parent / "relax_summary.json"
    if not summary_path.is_file() or relaxation.get("summary_sha256") != sha256_file(
        summary_path
    ):
        raise ValueError("Missing or altered model relaxation summary")
    summary = json.loads(summary_path.read_text())
    if (
        summary.get("backend") != source.get("backend")
        or summary.get("optimized_structure_sha256") != structure_hash
        or summary.get("source_structure_sha256")
        != relaxation.get("source_structure_sha256")
        or summary.get("relaxation_protocol_version")
        != relaxation.get("protocol_version")
    ):
        raise ValueError("Stage1 relaxation provenance is inconsistent")
    mode_selection = source.get("gamma_mode_selection", {})
    acoustic = set(mode_selection.get("acoustic_modes_one_based", []))
    optical = set(mode_selection.get("optical_modes_one_based", []))
    nmode = 3 * source.get("natoms_primitive", 0)
    if (
        payload.get("selection") != "gamma_optical_and_momentum_conservation_only"
        or len(acoustic) != 3
        or len(optical) != nmode - 3
        or acoustic & optical
        or acoustic | optical != set(range(1, nmode + 1))
        or len(payload.get("pairs", []))
        != len(payload.get("finite_q_orbits", [])) * nmode * (nmode - 3)
        or any(
            pair["gamma_mode"]["mode_number_one_based"] not in optical
            for pair in payload["pairs"]
        )
    ):
        raise ValueError("Stage1 candidates do not contain only Gamma optical modes")


def _locked_identity(root: Path, identity: dict) -> None:
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".identity.lock").open("a+") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        path = root / "identity.json"
        if path.exists():
            if json.loads(path.read_text()) != identity:
                raise ValueError(
                    "Stage2 output belongs to different inputs or settings"
                )
        else:
            _write(path, identity)


def _checkpoint(path: Path, identity: dict, code: str) -> dict:
    if not path.exists():
        return {
            "identity": identity,
            "pair_code": code,
            "energies_ev": {},
            "elapsed_seconds": 0.0,
        }
    data = json.loads(path.read_text())
    if data.get("identity") != identity or data.get("pair_code") != code:
        raise ValueError(f"Mismatched point checkpoint: {path}")
    if any(not np.isfinite(value) for value in data["energies_ev"].values()):
        raise ValueError(f"Nonfinite point checkpoint: {path}")
    return data


def _calculate(
    pair: dict,
    primitive,
    calc,
    root: Path,
    identity: dict,
    coordinates: list[tuple[float, float]],
) -> int:
    path = root / "pairs" / pair["pair_code"] / "points.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with (path.parent / ".pair.lock").open("a+") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        state = _checkpoint(path, identity, pair["pair_code"])
        builder = ModePairFrozenPhononBuilder(pair, primitive)
        evaluated = 0
        for x, y in coordinates:
            key = _key(x, y)
            if key in state["energies_ev"]:
                continue
            atoms = builder.build_atoms(x, y)
            atoms.calc = calc
            started = time.perf_counter()
            energy = float(atoms.get_potential_energy())
            if not np.isfinite(energy):
                raise ValueError(
                    f"Nonfinite MatterSim energy for {pair['pair_code']} at {key}"
                )
            state["energies_ev"][key] = energy
            state["elapsed_seconds"] += time.perf_counter() - started
            _write(path, state)
            evaluated += 1
        return evaluated


def _states(
    root: Path,
    pairs: list[dict],
    identity: dict,
    coordinates: list[tuple[float, float]],
) -> dict:
    values = {}
    expected = {_key(x, y) for x, y in coordinates}
    for pair in pairs:
        path = root / "pairs" / pair["pair_code"] / "points.json"
        if not path.exists():
            raise ValueError(f"Missing pair checkpoint: {pair['pair_code']}")
        state = _checkpoint(path, identity, pair["pair_code"])
        if not expected <= set(state["energies_ev"]):
            raise ValueError(f"Incomplete pair checkpoint: {pair['pair_code']}")
        values[pair["pair_code"]] = state
    return values


def _selection(root: Path, payload: dict, identity: dict, top_channels: int) -> dict:
    path = root / "screen_ranking.json"
    if not path.is_file():
        raise ValueError("Complete screen ranking is required before refinement")
    screen = json.loads(path.read_text())
    if screen["identity"] != identity:
        raise ValueError("Screen ranking and refinement identity differ")
    channels = payload["equivalent_pair_channels"]["channels"]
    by_code = {row["pair_code"]: row["phi122_proxy_mev"] for row in screen["pairs"]}
    expected = {row["pair_code"] for row in payload["pairs"]}
    listed = [code for channel in channels for code in channel["pair_codes"]]
    if (
        len(by_code) != len(screen["pairs"])
        or set(by_code) != expected
        or len(listed) != len(set(listed))
        or set(listed) != expected
    ):
        raise ValueError(
            "Screen ranking or physical channels do not cover every pair exactly once"
        )
    scored = sorted(
        (
            {
                "channel_code": row["channel_code"],
                "pair_codes": row["pair_codes"],
                "q_orbit_number": row["q_orbit_number"],
                "gamma_modes_one_based": row["gamma_modes_one_based"],
                "finite_q_degenerate_group_one_based": row[
                    "finite_q_degenerate_group_one_based"
                ],
                "basis_dependent_target_branch": row["basis_dependent_target_branch"],
                "target_mode_number_one_based": row["target_mode_number_one_based"],
                "score_mev": float(
                    np.linalg.norm([by_code[code] for code in row["pair_codes"]])
                ),
            }
            for row in channels
        ),
        key=lambda row: (-row["score_mev"], row["channel_code"]),
    )
    if top_channels > len(scored):
        raise ValueError("Requested top channels exceed available channels")
    nominated = scored[:top_channels]
    selected_channels = {row["channel_code"] for row in nominated}
    for row in nominated:
        if not row["basis_dependent_target_branch"]:
            continue
        selected_channels.update(
            other["channel_code"]
            for other in scored
            if other["q_orbit_number"] == row["q_orbit_number"]
            and other["gamma_modes_one_based"] == row["gamma_modes_one_based"]
            and other["target_mode_number_one_based"]
            in row["finite_q_degenerate_group_one_based"]
        )
    chosen = [row for row in scored if row["channel_code"] in selected_channels]
    selected = {code for channel in chosen for code in channel["pair_codes"]}
    if len(selected) != sum(len(channel["pair_codes"]) for channel in chosen):
        raise ValueError("Physical channels overlap in selected mode pairs")
    result = {
        "identity": identity,
        "top_channels": top_channels,
        "channels": chosen,
        "pair_codes": sorted(selected),
    }
    output = root / "selection.json"
    if output.exists() and json.loads(output.read_text()) != result:
        raise ValueError("Existing refinement selection has changed")
    _write(output, result)
    return result


def finalize(
    root: Path, payload: dict, identity: dict, phase: str, top_channels: int
) -> Path:
    pairs = payload["pairs"]
    if not pairs:
        raise ValueError("This primitive cell has no Gamma optical modes to screen")
    expected = {pair["pair_code"] for pair in pairs}
    found = {path.parent.name for path in (root / "pairs").glob("*/points.json")}
    if len(expected) != len(pairs) or found - expected:
        raise ValueError("Duplicated or unexpected mode-pair checkpoint")
    if phase == "screen":
        states = _states(root, pairs, identity, SIX)
        rows = [
            {
                "pair_code": pair["pair_code"],
                "phi122_proxy_mev": _proxy(states[pair["pair_code"]]["energies_ev"]),
            }
            for pair in pairs
        ]
        rows.sort(key=lambda row: (-abs(row["phi122_proxy_mev"]), row["pair_code"]))
        result = {
            "phase": phase,
            "identity": identity,
            "units": UNITS,
            "pair_count": len(rows),
            "pairs": rows,
        }
    else:
        selection = _selection(root, payload, identity, top_channels)
        by_code = {row["pair_code"]: row for row in pairs}
        if phase == "refine":
            chosen = [by_code[code] for code in selection["pair_codes"]]
            coordinates, axis = CENTER, AXIS[2:7]
        elif phase == "audit":
            refinement = json.loads((root / "refine_ranking.json").read_text())
            if refinement["identity"] != identity:
                raise ValueError("Audit and refinement identities differ")
            top = {row["channel_code"] for row in refinement["channels"][:5]}
            codes = {
                code
                for channel in selection["channels"]
                if channel["channel_code"] in top
                for code in channel["pair_codes"]
            }
            chosen = [by_code[code] for code in sorted(codes)]
            coordinates, axis = FULL, AXIS
        else:
            raise ValueError(f"Unknown Stage2 phase: {phase}")
        states = _states(root, chosen, identity, coordinates)
        rows = []
        for pair in chosen:
            code = pair["pair_code"]
            grid = _grid(states[code]["energies_ev"], axis)
            fit = analyze_pair_grid(pair, grid, axis, axis, fit_window=1.0)
            unique = np.unique(grid)
            fit["energy_resolution"] = {
                "point_count": int(grid.size),
                "unique_energy_count": int(unique.size),
                "minimum_nonzero_gap_ev": float(np.min(np.diff(unique)))
                if unique.size > 1
                else None,
                "note": "Repeated energies can also arise from physical symmetry",
            }
            if phase == "audit":
                fit["window_sensitivity"] = {
                    str(window): analyze_pair_grid(
                        pair, grid, axis, axis, fit_window=window
                    )
                    for window in (1.0, 1.5, 2.0)
                }
            if fit["fit_design_rank"] != 13 or not np.isfinite(
                fit["fit_condition_number"]
            ):
                raise ValueError(f"Invalid central 13-column fit for {code}")
            rows.append(
                {
                    "pair_code": code,
                    "analysis": fit,
                    "elapsed_seconds": states[code]["elapsed_seconds"],
                }
            )
        by_fit = {
            row["pair_code"]: row["analysis"]["physics"]["phi_122_mev_per_A3amu32"]
            for row in rows
        }
        channels = sorted(
            (
                {
                    "channel_code": channel["channel_code"],
                    "pair_codes": channel["pair_codes"],
                    "score_mev": float(
                        np.linalg.norm([by_fit[code] for code in channel["pair_codes"]])
                    ),
                }
                for channel in selection["channels"]
                if set(channel["pair_codes"]) <= set(by_fit)
            ),
            key=lambda row: (-row["score_mev"], row["channel_code"]),
        )
        result = {
            "phase": phase,
            "identity": identity,
            "units": UNITS,
            "pair_count": len(rows),
            "channels": channels,
            "pairs": rows,
        }
    path = root / f"{phase}_ranking.json"
    _write(path, result)
    return path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["screen", "refine", "audit"])
    parser.add_argument("--mode-pairs-json", type=Path, required=True)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--device", choices=["cpu", "cuda"], default="cpu")
    parser.add_argument("--top-channels", type=int, default=20)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--finalize-only", action="store_true")
    args = parser.parse_args(argv)
    if (
        args.top_channels < 1
        or args.shard_count < 1
        or not 0 <= args.shard_index < args.shard_count
    ):
        parser.error("Invalid top-channels or shard assignment")
    pair_path, structure, checkpoint = (
        path.resolve()
        for path in (args.mode_pairs_json, args.structure, args.checkpoint)
    )
    payload = json.loads(pair_path.read_text())
    if (
        payload.get("version") != CONTRACT_VERSION
        or payload["source"].get("normalization_version") != NORMALIZATION_VERSION
    ):
        raise ValueError("Stage2 requires the optical-Gamma v5 mode-pair contract")
    validate_relaxed_stage1(payload, structure)
    identity = _identity(pair_path, structure, checkpoint, args.top_channels)
    root = args.output_dir.resolve()
    _locked_identity(root, identity)
    if args.finalize_only:
        print(finalize(root, payload, identity, args.phase, args.top_channels))
        return 0
    pairs = payload["pairs"]
    if not pairs:
        raise ValueError("This primitive cell has no Gamma optical modes to screen")
    if len({pair["pair_code"] for pair in pairs}) != len(pairs):
        raise ValueError("Repeated Stage1 mode-pair code")
    if args.phase == "screen":
        selected, coordinates = pairs, SIX
    else:
        selection = _selection(root, payload, identity, args.top_channels)
        selected_codes = set(selection["pair_codes"])
        if args.phase == "audit":
            refinement = json.loads((root / "refine_ranking.json").read_text())
            top = {row["channel_code"] for row in refinement["channels"][:5]}
            selected_codes = {
                code
                for channel in selection["channels"]
                if channel["channel_code"] in top
                for code in channel["pair_codes"]
            }
        selected = [pair for pair in pairs if pair["pair_code"] in selected_codes]
        coordinates = CENTER if args.phase == "refine" else FULL
    worker_started = time.perf_counter()
    primitive = load_atoms_from_qe(structure)
    calc, _ = make_mattersim_calculator(checkpoint, args.device, primitive)
    loading_seconds = time.perf_counter() - worker_started
    done = 0
    for index, pair in enumerate(selected):
        if index % args.shard_count == args.shard_index:
            done += _calculate(pair, primitive, calc, root, identity, coordinates)
    _write(
        root / "workers" / f"{args.phase}-{args.shard_index}-{os.getpid()}.json",
        {
            "identity": identity,
            "phase": args.phase,
            "shard": [args.shard_index, args.shard_count],
            "new_energy_points": done,
            "loading_seconds": loading_seconds,
            "worker_wall_seconds": time.perf_counter() - worker_started,
            "resources": process_resource_metrics(args.device),
            "hostname": socket.gethostname(),
            "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
            "omp_num_threads": os.environ.get("OMP_NUM_THREADS"),
        },
    )
    print(
        json.dumps(
            {
                "phase": args.phase,
                "new_energy_points": done,
                "shard": [args.shard_index, args.shard_count],
            }
        )
    )
    if args.shard_count == 1:
        print(finalize(root, payload, identity, args.phase, args.top_channels))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
