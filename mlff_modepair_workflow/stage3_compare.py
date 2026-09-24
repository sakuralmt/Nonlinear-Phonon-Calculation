"""Compare two v3 MLFF PES grids with QE on the same selected displacements."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from .core import analyze_pair_grid
from .prophet_backend import sha256_file
from .prophet_stage2 import AXES
from .units import NORMALIZATION_VERSION


def _metrics(error: np.ndarray) -> dict:
    values = np.asarray(error, dtype=float).ravel()
    if values.size == 0 or not np.isfinite(values).all():
        raise ValueError("Comparison contains no finite values")
    return {"mae": float(np.mean(np.abs(values))), "rmse": float(np.sqrt(np.mean(values**2))),
            "max_abs": float(np.max(np.abs(values))), "n": int(values.size)}


def _ranks(values: dict[str, float]) -> dict[str, int]:
    return {code: rank for rank, code in enumerate(
        sorted(values, key=lambda code: (-abs(values[code]), code)), 1)}


def _spearman(left: dict[str, float], right: dict[str, float]) -> float | None:
    codes = sorted(set(left) & set(right))
    if len(codes) < 2:
        return None
    x = np.asarray([_ranks(left)[code] for code in codes], dtype=float)
    y = np.asarray([_ranks(right)[code] for code in codes], dtype=float)
    if np.std(x) == 0 or np.std(y) == 0:
        return None
    return float(np.corrcoef(x, y)[0, 1])


def _physics(pair: dict, grid: np.ndarray, fit_window: float) -> dict:
    result = analyze_pair_grid(pair, grid, AXES, AXES, fit_window=fit_window)
    if result["fit_design_rank"] != 13:
        raise ValueError("Comparison fit is rank deficient")
    return {"phi122": float(result["physics"]["phi_122_mev_per_A3amu32"]),
            "phi112": float(result["physics"]["phi_112_mev_per_A3amu32"]),
            "phi1122": float(result["physics"]["phi_1122_mev_per_A4amu2"]),
            "fit_rmse_eV": float(result["center_fit_rmse_ev_supercell"]),
            "fit_condition_number": float(result["fit_condition_number"])}


def compare(qe_root: Path, stage2_roots: dict[str, Path]) -> dict:
    qe_root = Path(qe_root).resolve()
    manifest = json.loads((qe_root / "run_manifest.json").read_text())
    selection = json.loads((qe_root / "selection.json").read_text())
    structure_hash = sha256_file(Path(manifest["structure"]))
    pair_hash = sha256_file(Path(manifest["mode_pairs_json"]))
    if selection["structure_sha256"] != structure_hash or selection["mode_pairs_sha256"] != pair_hash:
        raise ValueError("QE selection has changed structure or mode pairs")
    pairs = json.loads(Path(manifest["mode_pairs_json"]).read_text())
    by_code = {row["pair_code"]: row for row in pairs["pairs"]}
    codes = manifest["selected_pair_codes"]
    if len(codes) != len(set(codes)):
        raise ValueError("QE selection contains duplicate mode pairs")
    models = {}
    for backend, path in stage2_roots.items():
        root = Path(path).resolve()
        ranking = json.loads((root / "pair_ranking.json").read_text())
        signature = ranking["signature"]
        if (signature["mode_pairs_sha256"] != pair_hash
                or signature["structure_sha256"] != structure_hash
                or signature["normalization_version"] != NORMALIZATION_VERSION):
            raise ValueError(f"{backend} Stage2 is not on the QE displacement basis")
        if ranking["backend"]["backend"] != backend:
            raise ValueError(f"Stage2 backend mismatch for {backend}")
        models[backend] = root
    rows = []
    pooled_errors = {backend: {"full": [], "center": [], "forces": []} for backend in models}
    for code in codes:
        qe_pair_root = qe_root / "pairs" / code
        qe_summary = json.loads((qe_pair_root / "summary.json").read_text())
        if not qe_summary.get("complete") or qe_summary.get("completed_points") != 81:
            raise ValueError(f"QE pair is incomplete: {code}")
        qe_grid = np.load(qe_pair_root / "energy_grid_eV.npy", allow_pickle=False)
        if qe_grid.shape != (9, 9) or not np.isfinite(qe_grid).all():
            raise ValueError(f"QE pair has an invalid 9x9 energy grid: {code}")
        qe_physics = {str(window): _physics(by_code[code], qe_grid, window)
                      for window in (1.0, 1.5, 2.0)}
        row = {"pair_code": code, "qe": qe_physics, "models": {}}
        for backend, root in models.items():
            stage2_root = root / "pairs" / code
            summary = json.loads((stage2_root / "summary.json").read_text())
            if summary["pair_code"] != code or summary["completed_points"] != 81:
                raise ValueError(f"Incomplete {backend} Stage2 pair: {code}")
            grid = np.load(stage2_root / "energy_grid_eV.npy", allow_pickle=False)
            if grid.shape != (9, 9) or not np.isfinite(grid).all():
                raise ValueError(f"Invalid {backend} Stage2 grid: {code}")
            error = (grid - grid[4, 4]) - (qe_grid - qe_grid[4, 4])
            pooled_errors[backend]["full"].append(error.ravel())
            pooled_errors[backend]["center"].append(error[2:7, 2:7].ravel())
            model_physics = {str(window): _physics(by_code[code], grid, window)
                             for window in (1.0, 1.5, 2.0)}
            delta = {key: model_physics["1.0"][key] - qe_physics["1.0"][key]
                     for key in ("phi122", "phi112", "phi1122")}
            force_error = None
            force_path = stage2_root / "forces_eV_per_A.npy"
            qe_force_path = qe_pair_root / "forces_eV_per_A.npy"
            if force_path.exists() and qe_force_path.exists():
                forces = np.load(force_path, allow_pickle=False)
                qe_forces = np.load(qe_force_path, allow_pickle=False)
                if forces.shape != qe_forces.shape or not np.isfinite(forces).all():
                    raise ValueError(f"Invalid {backend} force grid: {code}")
                force_error = _metrics(forces - qe_forces)
                pooled_errors[backend]["forces"].append((forces - qe_forces).ravel())
            row["models"][backend] = {
                "energy_error_full9_eV": _metrics(error),
                "energy_error_center5_eV": _metrics(error[2:7, 2:7]),
                "force_error_eV_per_A": force_error,
                "physics": model_physics, "physics_error": delta,
                "energy_resolution_diagnostics": summary.get("energy_resolution_diagnostics"),
                "quality_flags": summary.get("quality_flags", []),
            }
        rows.append(row)
    qe_phi = {row["pair_code"]: row["qe"]["1.0"]["phi122"] for row in rows}
    qe_ranks = _ranks(qe_phi)
    model_summary = {}
    for backend in models:
        phi = {row["pair_code"]: row["models"][backend]["physics"]["1.0"]["phi122"] for row in rows}
        ranks = _ranks(phi)
        full_error = np.concatenate(pooled_errors[backend]["full"])
        center_error = np.concatenate(pooled_errors[backend]["center"])
        model_summary[backend] = {
            "energy_error_full9_eV": _metrics(full_error),
            "energy_error_center5_eV": _metrics(center_error),
            "force_error_eV_per_A": (_metrics(np.concatenate(pooled_errors[backend]["forces"]))
                                      if len(pooled_errors[backend]["forces"]) == len(rows) else None),
            "phi122_error_mev_per_A3amu32": _metrics(np.asarray([
                row["models"][backend]["physics_error"]["phi122"] for row in rows])),
            "phi112_error_mev_per_A3amu32": _metrics(np.asarray([
                row["models"][backend]["physics_error"]["phi112"] for row in rows])),
            "phi1122_error_mev_per_A4amu2": _metrics(np.asarray([
                row["models"][backend]["physics_error"]["phi1122"] for row in rows])),
            "spearman_abs_phi122_within_selected": _spearman(qe_phi, phi),
            "top5_overlap_with_qe": sorted(code for code in codes if qe_ranks[code] <= 5 and ranks[code] <= 5),
            "top10_overlap_with_qe": sorted(code for code in codes if qe_ranks[code] <= 10 and ranks[code] <= 10),
            "qe_top10_missing_from_model_top10": sorted(code for code in codes if qe_ranks[code] <= 10 and ranks[code] > 10),
        }
    return {"kind": "v3_same_displacement_qe_mlff_comparison", "selection_size": len(codes),
            "structure_sha256": structure_hash, "mode_pairs_sha256": pair_hash,
            "models": model_summary, "pairs": rows,
            "note": "Ranking statistics are restricted to the selected QE pairs; force errors require separate MLFF force grids."}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qe-root", type=Path, required=True)
    parser.add_argument("--prophet-stage2", type=Path)
    parser.add_argument("--mattersim-stage2", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    roots = {key: value for key, value in (("prophet", args.prophet_stage2),
                                          ("mattersim", args.mattersim_stage2)) if value is not None}
    if not roots:
        parser.error("At least one Stage2 result directory is required")
    result = compare(args.qe_root, roots)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
