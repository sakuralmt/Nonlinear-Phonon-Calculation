#!/usr/bin/env python3
"""
Generate explicit Gamma-q mode pairs for later frozen-phonon calculations.

Each pair record contains:
- Gamma mode identity
- q-point mode identity
- representative q and conjugate -q
- frequencies
- detailed primitive-cell eigenvector coordinates for Gamma and q

This is still a screening-level object, but it is the first output that is
directly usable for downstream pair calculations.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np

from common import q_equiv_delta_frac


def decode_complex_mode(mode):
    return np.array([[c[0] + 1j * c[1] for c in vec] for vec in mode], dtype=np.complex128)


def encode_complex_mode(mode):
    return [
        {
            "x": {"re": float(vec[0].real), "im": float(vec[0].imag)},
            "y": {"re": float(vec[1].real), "im": float(vec[1].imag)},
            "z": {"re": float(vec[2].real), "im": float(vec[2].imag)},
        }
        for vec in mode
    ]


def canonical_minus_q(q):
    q = np.array(q, dtype=float)
    qbar = -q
    qbar = qbar - np.floor(qbar)
    qbar[np.abs(qbar) < 1.0e-8] = 0.0
    qbar[np.abs(qbar - 1.0) < 1.0e-8] = 0.0
    return qbar.tolist()


def canonical_q_family(q):
    q = list(map(float, q))
    qbar = canonical_minus_q(q)
    q_key = tuple(round(x, 10) for x in q)
    qbar_key = tuple(round(x, 10) for x in qbar)
    if q_key <= qbar_key:
        return q, qbar
    return qbar, q


def parse_args():
    p = argparse.ArgumentParser(description="Generate explicit Gamma-q mode pairs with eigenvectors")
    p.add_argument("--run-root", type=str, required=True)
    p.add_argument("--output-dir", type=str, default=None)
    return p.parse_args()


def main():
    args = parse_args()
    run_root = Path(args.run_root).expanduser().resolve()
    mode_selection_json = run_root / "mode_selection" / "selected_modes.json"
    extracted_json = run_root / "extracted" / "screened_eigenvectors.json"
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir is not None else (run_root / "mode_pairs")
    output_dir.mkdir(parents=True, exist_ok=True)

    mode_selection = json.loads(mode_selection_json.read_text())
    extracted = json.loads(extracted_json.read_text())

    points = {int(item["target_index"]): item for item in extracted["points"]}
    gamma_point = next(item for item in extracted["points"] if item["label"] == "Gamma")

    gamma_mode_lookup = {}
    for item in mode_selection["gamma_candidate_modes"]:
        gamma_mode_lookup[int(item["mode_number_one_based"])] = item

    pair_records = []
    seen_family_keys = set()
    for target in mode_selection["target_modes"]:
        point_index = int(target["point_index"])
        point = points[point_index]
        q_mode_idx = int(target["mode_index_zero_based"])
        q_mode_number = int(target["mode_number_one_based"])
        q_mode = decode_complex_mode(point["modes"][q_mode_idx])

        for gamma_allowed in target["allowed_gamma_modes"]:
            gamma_mode_number = int(gamma_allowed["mode_number_one_based"])
            gamma_mode_idx = int(gamma_allowed["mode_index_zero_based"])
            gamma_mode = decode_complex_mode(gamma_point["modes"][gamma_mode_idx])
            gamma_meta = gamma_mode_lookup[gamma_mode_number]

            q_frac = list(map(float, target["q_frac"]))
            q_canon, qbar_canon = canonical_q_family(q_frac)
            family_key = (
                gamma_meta["mode_code"],
                target["point_label"],
                tuple(round(x, 10) for x in q_canon),
                int(q_mode_number),
            )
            if family_key in seen_family_keys:
                continue
            seen_family_keys.add(family_key)

            pair_code = f"{gamma_meta['mode_code']}__{target['point_label']}_q_{q_canon[0]:.3f}_{q_canon[1]:.3f}_{q_canon[2]:.3f}_m{q_mode_number}".replace("-", "m")
            q_frac = target["q_frac"]
            qbar_frac = qbar_canon
            self_conjugate = np.linalg.norm(q_equiv_delta_frac(q_frac, qbar_frac)) < 1.0e-8

            pair_records.append(
                {
                    "pair_code": pair_code,
                    "coupling_type": "Q_gamma*Q_q^2" if self_conjugate else "Q_gamma*Q_q*Q_-q",
                    "gamma_mode": {
                        "mode_code": gamma_meta["mode_code"],
                        "point_index": gamma_meta["point_index"],
                        "point_label": gamma_meta["point_label"],
                        "q_frac": gamma_meta["q_frac"],
                        "mode_index_zero_based": gamma_meta["mode_index_zero_based"],
                        "mode_number_one_based": gamma_meta["mode_number_one_based"],
                        "freq_thz": gamma_meta["freq_thz"],
                        "eigenvector": encode_complex_mode(gamma_mode),
                    },
                    "target_mode": {
                        "mode_code": target["mode_code"],
                        "point_index": point_index,
                        "point_label": target["point_label"],
                        "q_frac": q_frac,
                        "qbar_frac": qbar_frac,
                        "self_conjugate": bool(self_conjugate),
                        "mode_index_zero_based": q_mode_idx,
                        "mode_number_one_based": q_mode_number,
                        "freq_thz": float(target["freq_thz"]),
                        "eigenvector_q": encode_complex_mode(q_mode),
                        "eigenvector_qbar_by_conjugation": encode_complex_mode(np.conjugate(q_mode)),
                    },
                }
            )

    out_json = output_dir / "selected_mode_pairs.json"
    out_json.write_text(json.dumps({"kind": "mode_pairs_qgamma_qpair", "pairs": pair_records}, indent=2))

    out_csv = output_dir / "selected_mode_pairs.csv"
    with out_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "pair_code",
                "coupling_type",
                "gamma_mode_code",
                "gamma_mode_number",
                "gamma_freq_thz",
                "target_mode_code",
                "point_label",
                "qx",
                "qy",
                "qz",
                "qbar_x",
                "qbar_y",
                "qbar_z",
                "target_mode_number",
                "target_freq_thz",
            ]
        )
        for item in pair_records:
            q = item["target_mode"]["q_frac"]
            qbar = item["target_mode"]["qbar_frac"]
            writer.writerow(
                [
                    item["pair_code"],
                    item["coupling_type"],
                    item["gamma_mode"]["mode_code"],
                    item["gamma_mode"]["mode_number_one_based"],
                    f"{item['gamma_mode']['freq_thz']:.6f}",
                    item["target_mode"]["mode_code"],
                    item["target_mode"]["point_label"],
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    f"{qbar[0]:.6f}",
                    f"{qbar[1]:.6f}",
                    f"{qbar[2]:.6f}",
                    item["target_mode"]["mode_number_one_based"],
                    f"{item['target_mode']['freq_thz']:.6f}",
                ]
            )

    print(f"selected mode pairs: {len(pair_records)}")
    print(f"saved: {out_json}")
    print(f"saved: {out_csv}")


if __name__ == "__main__":
    main()
