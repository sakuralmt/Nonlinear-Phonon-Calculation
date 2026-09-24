#!/usr/bin/env python3
"""Audit directly aligned PES grids; use the separate phase-aware coupling audit for all modes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from mlff_modepair_workflow.core import ModePairFrozenPhononBuilder, analyze_pair_grid, load_atoms_from_qe
from mlff_modepair_workflow.prophet_stage2 import AXES


def _signed_overlap(a: np.ndarray, b: np.ndarray, masses: np.ndarray) -> float:
    weights = np.sqrt(masses)[:, None]
    x = (a * weights).ravel()
    y = (b * weights).ravel()
    return float(np.dot(x, y) / (np.linalg.norm(x) * np.linalg.norm(y)))


def _physics(summary: dict) -> tuple[float, float, float]:
    p = summary["analysis"]["physics"]
    return (float(p["phi_122_mev_per_A3amu32"]),
            float(p["phi_112_mev_per_A3amu32"]),
            float(p["phi_1122_mev_per_A4amu2"]))


def _stats(values: list[float]) -> dict | None:
    if not values:
        return None
    a = np.asarray(values, dtype=float)
    return {"count": len(a), "mean": float(np.mean(a)), "median": float(np.median(a)),
            "p95": float(np.percentile(a, 95)), "max": float(np.max(a))}


def compare(old_dir: Path, new_dir: Path, structure: Path) -> dict:
    primitive = load_atoms_from_qe(structure)
    old_paths = {p.parent.name: p for p in (old_dir / "pairs").glob("*/summary.json")}
    new_paths = {p.parent.name: p for p in (new_dir / "pairs").glob("*/summary.json")}
    rows = []
    for code in sorted(old_paths.keys() & new_paths.keys()):
        old = json.loads(old_paths[code].read_text())
        new = json.loads(new_paths[code].read_text())
        for key in ("structure_sha256", "checkpoint_sha256", "source_commit",
                    "energy_accumulation", "geometry_source", "normalization_version",
                    "a1_values", "a2_values", "fit_window"):
            if old["signature"][key] != new["signature"][key]:
                raise ValueError(f"Stage2 signature {key} differs for {code}")
        if old["completed_points"] != 81 or new["completed_points"] != 81:
            raise ValueError(f"Incomplete pair: {code}")
        old_builder = ModePairFrozenPhononBuilder(old["pair"], primitive)
        new_builder = ModePairFrozenPhononBuilder(new["pair"], primitive)
        masses = old_builder.supercell.get_masses()
        if old_builder.n_super != new_builder.n_super:
            raise ValueError(f"Supercell differs for {code}")
        gamma_overlap = _signed_overlap(old_builder.displacement_cart(1, 0),
                                        new_builder.displacement_cart(1, 0), masses)
        q_overlap = _signed_overlap(old_builder.displacement_cart(0, 1),
                                    new_builder.displacement_cart(0, 1), masses)
        matched_basis = abs(gamma_overlap) >= 0.99 and abs(q_overlap) >= 0.99
        old_grid = np.load(old_paths[code].parent / "energy_grid_eV.npy")
        new_grid = np.load(new_paths[code].parent / "energy_grid_eV.npy")
        if old_grid.shape != (9, 9) or new_grid.shape != (9, 9):
            raise ValueError(f"Incorrect PES grid shape: {code}")
        # Rows are q-mode amplitudes and columns are Gamma-mode amplitudes.
        aligned_new_grid = new_grid[::1 if q_overlap >= 0 else -1,
                                    ::1 if gamma_overlap >= 0 else -1]
        old_delta = old_grid - old_grid[4, 4]
        new_delta = aligned_new_grid - aligned_new_grid[4, 4]
        delta_mev = 1000 * (new_delta - old_delta)
        phi_old = _physics(old)
        phi_new = _physics(new)
        phi_new_aligned = (np.sign(gamma_overlap) * phi_new[0],
                           np.sign(q_overlap) * phi_new[1], phi_new[2])
        row = {
            "pair_code": code,
            "gamma_signed_overlap": gamma_overlap,
            "q_signed_overlap": q_overlap,
            "matched_basis": matched_basis,
            "gamma_freq_old_thz": old["pair"]["gamma_mode"]["freq_thz"],
            "gamma_freq_new_thz": new["pair"]["gamma_mode"]["freq_thz"],
            "q_freq_old_thz": old["pair"]["target_mode"]["freq_thz"],
            "q_freq_new_thz": new["pair"]["target_mode"]["freq_thz"],
            "grid_mae_mev": float(np.mean(np.abs(delta_mev))) if matched_basis else None,
            "grid_rmse_mev": float(np.sqrt(np.mean(delta_mev**2))) if matched_basis else None,
            "center5_mae_mev": float(np.mean(np.abs(delta_mev[2:7, 2:7]))) if matched_basis else None,
            "phi122_old": phi_old[0], "phi122_new_aligned": phi_new_aligned[0],
            "phi112_old": phi_old[1], "phi112_new_aligned": phi_new_aligned[1],
            "phi1122_old": phi_old[2], "phi1122_new_aligned": phi_new_aligned[2],
            "old_fit_rmse_mev": 1000 * old["analysis"]["center_fit_rmse_ev_supercell"],
            "new_fit_rmse_mev": 1000 * new["analysis"]["center_fit_rmse_ev_supercell"],
        }
        if matched_basis:
            row["phi122_abs_difference"] = abs(phi_old[0] - phi_new_aligned[0])
            row["phi112_abs_difference"] = abs(phi_old[1] - phi_new_aligned[1])
            row["phi1122_abs_difference"] = abs(phi_old[2] - phi_new_aligned[2])
            for window, label in ((1.5, "center7"), (2.0, "full9")):
                po = analyze_pair_grid(old["pair"], old_grid, AXES, AXES, fit_window=window)["physics"]
                pn = analyze_pair_grid(new["pair"], new_grid, AXES, AXES, fit_window=window)["physics"]
                row[f"{label}_phi122_old"] = po["phi_122_mev_per_A3amu32"]
                row[f"{label}_phi122_new_aligned"] = np.sign(gamma_overlap) * pn["phi_122_mev_per_A3amu32"]
                row[f"{label}_phi1122_old"] = po["phi_1122_mev_per_A4amu2"]
                row[f"{label}_phi1122_new_aligned"] = pn["phi_1122_mev_per_A4amu2"]
        rows.append(row)
    matched = [r for r in rows if r["matched_basis"]]
    old_top20 = {r["pair_code"] for r in sorted(matched, key=lambda r: -abs(r["phi122_old"]))[:20]}
    new_top20 = {r["pair_code"] for r in sorted(matched, key=lambda r: -abs(r["phi122_new_aligned"]))[:20]}
    return {
        "kind": "prophet_custom_vs_phonopy_stage2_matter_sim",
        "old_pair_count": len(old_paths), "new_pair_count": len(new_paths),
        "common_complete_pairs": len(rows), "matched_basis_pairs": len(matched),
        "unmatched_basis_pairs": [r["pair_code"] for r in rows if not r["matched_basis"]],
        "all_486_complete": len(old_paths) == len(new_paths) == len(rows) == 486,
        "grid_mae_mev": _stats([r["grid_mae_mev"] for r in matched]),
        "center5_mae_mev": _stats([r["center5_mae_mev"] for r in matched]),
        "phi122_abs_difference": _stats([r["phi122_abs_difference"] for r in matched]),
        "phi1122_abs_difference": _stats([r["phi1122_abs_difference"] for r in matched]),
        "top20_overlap_within_matched_pairs": len(old_top20 & new_top20) if len(matched) >= 20 else None,
        "pairs": rows,
        "note": "Real-mode overlaps >=0.99 are required only for pointwise subtraction "
                "of grids at the same coordinates. Null grid errors do not mean the complex "
                "phonon modes are physically different: compare phase- and subspace-invariant "
                "couplings with compare_equivalent_phonopy_couplings.py. "
                "Reported grid errors are center-aligned and signs are aligned. "
                "Top-20 overlap here is restricted to the pointwise-comparable subset.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-screening", type=Path, required=True)
    parser.add_argument("--new-screening", type=Path, required=True)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compare(args.old_screening, args.new_screening, args.structure)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps({k: v for k, v in result.items() if k != "pairs"}, indent=2))


if __name__ == "__main__":
    main()
