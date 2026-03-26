#!/usr/bin/env python3
"""
Mode-level screening for hexagonal Gamma-q-qbar coupling workflow.

Output:
- gamma_candidate_modes: Gamma optical modes used as pump candidates
- target_modes: q-point modes to be considered in later frozen-phonon calculations

The selection-rule switch is intentionally simple:
- off: keep all extracted non-Gamma modes
- on: keep modes at q-points whose little group admits at least one totally
      symmetric Gamma optical mode. The output remains mode-resolved.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np

from common import (
    HEX_RECIPROCAL_OPERATIONS_2D,
    apply_hex_reciprocal_op,
    load_structure_from_qe,
    q_equiv_delta_frac,
)


MASS_DICT = {"W": 183.84, "Se": 78.960}


def decode_complex_mode(mode):
    arr = np.array([[c[0] + 1j * c[1] for c in vec] for vec in mode], dtype=np.complex128)
    return arr


def direct_op_from_reciprocal(op2d: np.ndarray):
    r2d = np.linalg.inv(op2d).T
    out = np.eye(3)
    out[:2, :2] = r2d
    return np.rint(out).astype(int)


def row_cart_transform(rotation_frac: np.ndarray, prim_cell: np.ndarray):
    return np.linalg.inv(prim_cell) @ rotation_frac.T @ prim_cell


def build_primitive_permutation(rotation_frac: np.ndarray, frac_positions: np.ndarray, atomic_numbers: np.ndarray):
    mapping = []
    max_error = 0.0
    for frac, z in zip(frac_positions, atomic_numbers):
        new_frac = frac @ rotation_frac.T
        new_frac = new_frac - np.floor(new_frac)
        best = None
        for j, (target_frac, zt) in enumerate(zip(frac_positions, atomic_numbers)):
            if zt != z:
                continue
            delta = new_frac - target_frac
            delta = delta - np.round(delta)
            err = float(np.linalg.norm(delta))
            if best is None or err < best[0]:
                best = (err, j)
        if best is None:
            raise RuntimeError("Failed to map primitive atom under symmetry operation")
        max_error = max(max_error, best[0])
        mapping.append(best[1])
    return mapping, max_error


def transform_gamma_pattern(pattern: np.ndarray, rotation_frac: np.ndarray, permutation, prim_cell: np.ndarray):
    s_row = row_cart_transform(rotation_frac, prim_cell)
    transformed = np.zeros_like(pattern)
    for src, dst in enumerate(permutation):
        transformed[dst] = pattern[src] @ s_row
    return transformed


def mode_character(pattern: np.ndarray, transformed: np.ndarray):
    denom = float(np.sum(pattern * pattern))
    lam = float(np.sum(pattern * transformed) / denom)
    residual = float(np.linalg.norm(transformed - lam * pattern) / np.sqrt(denom))
    return lam, residual


def parse_args():
    p = argparse.ArgumentParser(description="Select modes for Gamma-q-qbar coupling workflow")
    p.add_argument("--run-root", type=str, required=True, help="Workflow run root containing screening/ and extracted/")
    p.add_argument("--scf-template", type=str, required=True)
    p.add_argument("--output-dir", type=str, default=None)
    p.add_argument("--apply-selection-rules", action="store_true")
    p.add_argument("--gamma-optical-only", action="store_true")
    p.add_argument("--acoustic-thz-threshold", type=float, default=0.5)
    p.add_argument("--gamma-residual-tol", type=float, default=5.0e-2)
    p.add_argument("--gamma-lambda-tol", type=float, default=5.0e-2)
    return p.parse_args()


def main():
    args = parse_args()

    run_root = Path(args.run_root).expanduser().resolve()
    screening_json = run_root / "screening" / "screening_summary.json"
    extracted_json = run_root / "extracted" / "screened_eigenvectors.json"
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir is not None else (run_root / "mode_selection")
    output_dir.mkdir(parents=True, exist_ok=True)

    screening = json.loads(screening_json.read_text())
    extracted = json.loads(extracted_json.read_text())
    points = extracted["points"]
    point_map = {item["target_index"]: item for item in points}
    point_index_by_q = {
        tuple(round(float(x), 10) for x in item["q_target_frac"]): int(item["target_index"])
        for item in points
    }

    gamma_point = next(item for item in points if item["label"] == "Gamma")
    gamma_freqs = np.array(gamma_point["freqs_thz"], dtype=float)

    atoms = load_structure_from_qe(Path(args.scf_template).expanduser().resolve())
    prim_cell = atoms.cell.array.copy()
    frac_positions = atoms.get_scaled_positions()
    atomic_numbers = atoms.get_atomic_numbers()
    masses = np.array([MASS_DICT[s] for s in atoms.get_chemical_symbols()], dtype=float)[:, None]

    gamma_patterns = []
    for mode in gamma_point["modes"]:
        gamma_patterns.append(np.real(decode_complex_mode(mode)) / np.sqrt(masses))

    gamma_candidate_modes = []
    for idx, freq in enumerate(gamma_freqs):
        is_optical = abs(freq) > args.acoustic_thz_threshold
        if args.gamma_optical_only and not is_optical:
            continue
        gamma_candidate_modes.append(
            {
                "mode_code": f"Gamma_p{gamma_point['target_index']}_m{idx + 1}",
                "point_index": gamma_point["target_index"],
                "point_label": "Gamma",
                "q_frac": gamma_point["q_target_frac"],
                "mode_index_zero_based": idx,
                "mode_number_one_based": idx + 1,
                "freq_thz": float(freq),
                "is_optical": bool(is_optical),
            }
        )

    direct_ops = [direct_op_from_reciprocal(op) for op in HEX_RECIPROCAL_OPERATIONS_2D]
    perm_cache = {}
    for rotation_frac in direct_ops:
        key = tuple(rotation_frac.reshape(-1).tolist())
        perm_cache[key] = build_primitive_permutation(rotation_frac, frac_positions, atomic_numbers)

    little_group_allowed_gamma = {}
    for item in screening["selected_points"]:
        q = np.array(item["rep_q_frac"], dtype=float)
        if item["label"] == "Gamma":
            continue
        point_index = point_index_by_q[tuple(round(float(x), 10) for x in item["rep_q_frac"])]

        little_ops = []
        for op2d, rotation_frac in zip(HEX_RECIPROCAL_OPERATIONS_2D, direct_ops):
            q_img = apply_hex_reciprocal_op(op2d, q)
            if np.linalg.norm(q_equiv_delta_frac(q_img, q)) < 1.0e-8:
                little_ops.append(rotation_frac)

        allowed_gamma_modes = []
        for gamma_mode in gamma_candidate_modes:
            idx = gamma_mode["mode_index_zero_based"]
            pattern = gamma_patterns[idx]
            totally_symmetric = True
            diagnostics = []
            for rotation_frac in little_ops:
                key = tuple(rotation_frac.reshape(-1).tolist())
                permutation, map_error = perm_cache[key]
                transformed = transform_gamma_pattern(pattern, rotation_frac, permutation, prim_cell)
                lam, residual = mode_character(pattern, transformed)
                diagnostics.append(
                    {
                        "rotation": rotation_frac.tolist(),
                        "lambda": lam,
                        "residual": residual,
                        "map_error": map_error,
                    }
                )
                if residual > args.gamma_residual_tol or abs(lam - 1.0) > args.gamma_lambda_tol:
                    totally_symmetric = False
            if totally_symmetric:
                allowed_gamma_modes.append(
                    {
                        "mode_index_zero_based": idx,
                        "mode_number_one_based": idx + 1,
                        "freq_thz": float(gamma_freqs[idx]),
                        "diagnostics": diagnostics,
                    }
                )

        little_group_allowed_gamma[point_index] = allowed_gamma_modes

    target_modes = []
    for item in points:
        if item["label"] == "Gamma":
            continue
        allowed_gamma_modes = little_group_allowed_gamma.get(item["target_index"], [])
        keep_point = True if not args.apply_selection_rules else len(allowed_gamma_modes) > 0
        if not keep_point:
            continue

        for mode_idx, freq in enumerate(item["freqs_thz"]):
            target_modes.append(
                {
                    "mode_code": f"{item['label']}_p{item['target_index']}_m{mode_idx + 1}",
                    "point_index": item["target_index"],
                    "point_label": item["label"],
                    "q_frac": item["q_target_frac"],
                    "mode_index_zero_based": mode_idx,
                    "mode_number_one_based": mode_idx + 1,
                    "freq_thz": float(freq),
                    "allowed_gamma_modes": [
                        {
                            "mode_index_zero_based": g["mode_index_zero_based"],
                            "mode_number_one_based": g["mode_number_one_based"],
                            "freq_thz": g["freq_thz"],
                        }
                        for g in allowed_gamma_modes
                    ],
                }
            )

    summary = {
        "kind": "mode_selection_qgamma_qpair",
        "run_root": str(run_root),
        "apply_selection_rules": bool(args.apply_selection_rules),
        "gamma_optical_only": bool(args.gamma_optical_only),
        "gamma_candidate_modes": gamma_candidate_modes,
        "allowed_gamma_by_point": {
            str(k): [
                {
                    "mode_index_zero_based": item["mode_index_zero_based"],
                    "mode_number_one_based": item["mode_number_one_based"],
                    "freq_thz": item["freq_thz"],
                }
                for item in v
            ]
            for k, v in little_group_allowed_gamma.items()
        },
        "target_modes": target_modes,
    }

    out_json = output_dir / "selected_modes.json"
    out_json.write_text(json.dumps(summary, indent=2))

    gamma_csv = output_dir / "gamma_candidate_modes.csv"
    with gamma_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["mode_code", "mode_index_zero_based", "mode_number_one_based", "freq_thz", "is_optical"])
        for item in gamma_candidate_modes:
            writer.writerow(
                [
                    item["mode_code"],
                    item["mode_index_zero_based"],
                    item["mode_number_one_based"],
                    f"{item['freq_thz']:.6f}",
                    int(item["is_optical"]),
                ]
            )

    target_csv = output_dir / "target_modes.csv"
    with target_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "point_index",
                "point_label",
                "qx",
                "qy",
                "qz",
                "mode_code",
                "mode_index_zero_based",
                "mode_number_one_based",
                "freq_thz",
                "allowed_gamma_mode_numbers",
            ]
        )
        for item in target_modes:
            q = item["q_frac"]
            gamma_modes = ",".join(str(g["mode_number_one_based"]) for g in item["allowed_gamma_modes"])
            writer.writerow(
                [
                    item["point_index"],
                    item["point_label"],
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    item["mode_code"],
                    item["mode_index_zero_based"],
                    item["mode_number_one_based"],
                    f"{item['freq_thz']:.6f}",
                    gamma_modes,
                ]
            )

    print(f"gamma candidate modes: {len(gamma_candidate_modes)}")
    print(f"selected target modes: {len(target_modes)}")
    print(f"saved: {out_json}")
    print(f"saved: {target_csv}")


if __name__ == "__main__":
    main()
