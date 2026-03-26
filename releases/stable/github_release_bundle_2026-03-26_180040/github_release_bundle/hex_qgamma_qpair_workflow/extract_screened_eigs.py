#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np

from common import (
    convert_qe_cart_q_to_fractional,
    load_structure_from_qe,
    parse_multiq_eig_file,
    q_equiv_delta_frac,
    snap_q_to_grid,
)


def encode_complex_mode(mode):
    return [
        [
            [float(vec[0].real), float(vec[0].imag)],
            [float(vec[1].real), float(vec[1].imag)],
            [float(vec[2].real), float(vec[2].imag)],
        ]
        for vec in mode
    ]


def parse_args():
    p = argparse.ArgumentParser(description="Extract screened q-point eigenvectors from matdyn eig output")
    p.add_argument("--eig-file", type=str, required=True)
    p.add_argument("--screening-json", type=str, required=True)
    p.add_argument("--scf-template", type=str, default=None)
    p.add_argument("--nat", type=int, default=3)
    p.add_argument("--q-format", choices=["auto", "fractional", "qe-cart"], default="auto")
    p.add_argument("--grid-n", type=int, default=6)
    p.add_argument("--q-tol", type=float, default=1.0e-4)
    p.add_argument("--output-dir", type=str, default=None)
    return p.parse_args()


def main():
    args = parse_args()

    eig_file = Path(args.eig_file).expanduser().resolve()
    screening_json = Path(args.screening_json).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir is not None else eig_file.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    screening = json.loads(screening_json.read_text())
    target_points = screening["selected_points"]
    nat = int(args.nat)

    prim_cell = None
    if args.q_format in ("auto", "qe-cart"):
        if args.scf_template is not None:
            atoms = load_structure_from_qe(Path(args.scf_template).expanduser().resolve())
            nat = len(atoms)
            prim_cell = atoms.cell.array.copy()
        elif args.q_format == "qe-cart":
            raise ValueError("--scf-template is required when --q-format qe-cart")

    q_blocks = parse_multiq_eig_file(eig_file, nat)
    extracted = []

    for block in q_blocks:
        q_raw = np.array(block["q_raw"], dtype=float)
        candidates = []

        if args.q_format in ("auto", "fractional"):
            q_frac_direct = snap_q_to_grid(q_raw, args.grid_n, tol=args.q_tol)
            candidates.append(("fractional", q_frac_direct))

        if args.q_format in ("auto", "qe-cart") and prim_cell is not None:
            q_frac_cart = snap_q_to_grid(
                convert_qe_cart_q_to_fractional(q_raw, prim_cell),
                args.grid_n,
                tol=args.q_tol,
            )
            candidates.append(("qe-cart", q_frac_cart))

        best = None
        for method, q_frac in candidates:
            for target_idx, item in enumerate(target_points):
                target_star = [np.array(q, dtype=float) for q in item["star_members"]]
                for target in target_star:
                    dist = float(np.linalg.norm(q_equiv_delta_frac(q_frac, target)))
                    if best is None or dist < best[0]:
                        best = (dist, method, q_frac, target_idx)

        if best is None:
            continue

        dist, method, q_frac, target_idx = best
        if dist > args.q_tol:
            continue

        extracted.append(
            {
                "target_index": target_idx,
                "label": target_points[target_idx]["label"],
                "q_target_frac": target_points[target_idx]["rep_q_frac"],
                "q_raw": q_raw.tolist(),
                "q_match_method": method,
                "q_matched_frac": q_frac.tolist(),
                "freqs_thz": [float(x) for x in block["freqs_thz"].tolist()],
                "modes": [encode_complex_mode(mode) for mode in block["modes"]],
            }
        )

    extracted.sort(key=lambda item: item["target_index"])

    out_json = output_dir / "screened_eigenvectors.json"
    out_json.write_text(json.dumps({"kind": "screened_eigenvectors_qpair", "points": extracted}, indent=2))

    out_csv = output_dir / "screened_eigenvectors_summary.csv"
    with out_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["target_index", "label", "qx", "qy", "qz", "n_modes", "match_method"])
        for item in extracted:
            q = item["q_target_frac"]
            writer.writerow(
                [
                    item["target_index"],
                    item["label"],
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    len(item["freqs_thz"]),
                    item["q_match_method"],
                ]
            )

    found_targets = {item["target_index"] for item in extracted}
    missing = [i for i in range(len(target_points)) if i not in found_targets]

    print(f"parsed q blocks: {len(q_blocks)}")
    print(f"matched screened q-points: {len(extracted)}")
    if missing:
        print(f"missing target indices: {missing}")
    print(f"saved: {out_json}")
    print(f"saved: {out_csv}")


if __name__ == "__main__":
    main()
