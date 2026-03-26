#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np

from common import (
    HEX_RECIPROCAL_OPERATIONS_2D,
    apply_hex_reciprocal_op,
    canonicalize_q,
    choose_display_rep,
    classify_hex_qpoint,
    is_hexagonal_2d,
    load_structure_from_qe,
    q_equiv_delta_frac,
    snap_q_to_grid,
    snap_tuple,
)


def parse_args():
    p = argparse.ArgumentParser(description="Screen 6x6 hexagonal q-grid for Q_gamma Q_q Q_-q workflow")
    p.add_argument("--work-dir", type=str, default=".", help="Directory containing scf.inp")
    p.add_argument("--scf-template", type=str, default="scf.inp")
    p.add_argument("--grid-n", type=int, default=6)
    p.add_argument("--output-dir", type=str, default=None)
    p.add_argument("--hex-length-tol", type=float, default=5.0e-2)
    p.add_argument("--hex-angle-tol-deg", type=float, default=3.0)
    p.add_argument("--q-tol", type=float, default=1.0e-6)
    return p.parse_args()


def main():
    args = parse_args()
    work_dir = Path(args.work_dir).expanduser().resolve()
    scf_template = (work_dir / args.scf_template).resolve()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir is not None else (Path(__file__).resolve().parent / "outputs" / f"{work_dir.name}_grid{args.grid_n}")
    output_dir.mkdir(parents=True, exist_ok=True)

    atoms = load_structure_from_qe(scf_template)
    prim_cell = atoms.cell.array.copy()
    is_hex, hex_info = is_hexagonal_2d(prim_cell, args.hex_length_tol, args.hex_angle_tol_deg)
    if not is_hex:
        raise ValueError(f"Structure is not recognized as hexagonal 2D within tolerances: {hex_info}")

    all_points = []
    rep_map = {}

    for i in range(args.grid_n):
        for j in range(args.grid_n):
            q = np.array([i / args.grid_n, j / args.grid_n, 0.0], dtype=float)
            q = canonicalize_q(q)

            star = []
            little_group_order = 0
            for op in HEX_RECIPROCAL_OPERATIONS_2D:
                q_img = apply_hex_reciprocal_op(np.array(op), q)
                q_img = snap_q_to_grid(q_img, args.grid_n, tol=args.q_tol)
                star.append(snap_tuple(q_img, args.grid_n))
                if np.linalg.norm(q_equiv_delta_frac(q_img, q)) < args.q_tol:
                    little_group_order += 1

            star_unique = sorted(set(star))
            rep = choose_display_rep(star_unique, args.q_tol)
            rep_key = tuple(star_unique[0])
            label = classify_hex_qpoint(q)
            is_high_symmetry = little_group_order > 1

            point_info = {
                "q_frac": [float(x) for x in q.tolist()],
                "rep_q_frac": [float(x) for x in rep],
                "label": label,
                "star_size": int(len(star_unique)),
                "little_group_order_inplane": int(little_group_order),
                "selected_for_qgamma_qpair": bool(is_high_symmetry),
                "star_members": [list(x) for x in star_unique],
            }
            all_points.append(point_info)
            if rep_key not in rep_map:
                rep_map[rep_key] = point_info

    irreducible_points = sorted(rep_map.values(), key=lambda item: (item["rep_q_frac"][0], item["rep_q_frac"][1], item["rep_q_frac"][2]))
    selected_points = [item for item in irreducible_points if item["selected_for_qgamma_qpair"]]

    summary = {
        "kind": "hex_qgamma_qpair_screening",
        "work_dir": str(work_dir),
        "scf_template": str(scf_template),
        "grid_n": args.grid_n,
        "criterion": "selected_for_qgamma_qpair means nontrivial in-plane little group under hexagonal reciprocal operations",
        "hex_check": hex_info,
        "spacegroup": {
            "international": "assumed_hexagonal_2d",
            "number": None,
            "pointgroup": "hex_inplane_6ops",
            "n_operations": 6,
        },
        "selected_points": selected_points,
        "irreducible_points": irreducible_points,
    }

    summary_path = output_dir / "screening_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))

    irr_csv = output_dir / "irreducible_qpoints.csv"
    with irr_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rep_qx",
                "rep_qy",
                "rep_qz",
                "label",
                "star_size",
                "little_group_order_inplane",
                "selected_for_qgamma_qpair",
            ]
        )
        for item in irreducible_points:
            writer.writerow(
                [
                    f"{item['rep_q_frac'][0]:.6f}",
                    f"{item['rep_q_frac'][1]:.6f}",
                    f"{item['rep_q_frac'][2]:.6f}",
                    item["label"],
                    item["star_size"],
                    item["little_group_order_inplane"],
                    int(item["selected_for_qgamma_qpair"]),
                ]
            )

    selected_csv = output_dir / "selected_qpoints.csv"
    with selected_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["qx", "qy", "qz", "label", "star_size", "little_group_order_inplane"])
        for item in selected_points:
            writer.writerow(
                [
                    f"{item['rep_q_frac'][0]:.6f}",
                    f"{item['rep_q_frac'][1]:.6f}",
                    f"{item['rep_q_frac'][2]:.6f}",
                    item["label"],
                    item["star_size"],
                    item["little_group_order_inplane"],
                ]
            )

    print("space group: assumed_hexagonal_2d, point group hex_inplane_6ops")
    print(f"irreducible q-points on {args.grid_n}x{args.grid_n}x1 grid: {len(irreducible_points)}")
    print(f"selected q-points for Q_gamma Q_q Q_-q workflow: {len(selected_points)}")
    print(f"selected reps: {[item['rep_q_frac'] for item in selected_points]}")
    print(f"saved: {summary_path}")
    print(f"saved: {selected_csv}")


if __name__ == "__main__":
    main()
