"""Compare complete v3 Prophet and MatterSim Stage2 runs on identical inputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np


COMMON_KEYS = (
    "version", "mode_pairs_sha256", "structure_sha256", "geometry_source",
    "normalization_version", "a1_values", "a2_values", "fit_window",
)


def compare(prophet_dir: Path, mattersim_dir: Path) -> dict:
    records = []
    for directory, expected_backend in ((prophet_dir, "prophet"), (mattersim_dir, "mattersim")):
        meta = json.loads((directory / "run_meta.json").read_text())
        ranking = json.loads((directory / "pair_ranking.json").read_text())
        if meta["backend"]["backend"] != expected_backend:
            raise ValueError(f"Expected {expected_backend} results in {directory}")
        if meta["n_pairs"] != 486 or meta["n_energy_evaluations"] != 486 * 81:
            raise ValueError(f"Incomplete Stage2 run in {directory}")
        if len(ranking["pairs"]) != 486:
            raise ValueError(f"Incomplete ranking in {directory}")
        records.append((meta, ranking))
    (prophet_meta, prophet), (mattersim_meta, mattersim) = records
    for key in COMMON_KEYS:
        if prophet_meta["signature"][key] != mattersim_meta["signature"][key]:
            raise ValueError(f"Cannot compare different {key}")
    if prophet_meta["units"] != mattersim_meta["units"]:
        raise ValueError("Cannot compare different units")
    p = {row["pair_code"]: row for row in prophet["pairs"]}
    m = {row["pair_code"]: row for row in mattersim["pairs"]}
    if len(p) != 486 or len(m) != 486 or p.keys() != m.keys():
        raise ValueError("The runs do not contain the same 486 unique mode pairs")
    codes = sorted(p)
    phi_p = np.asarray([p[code]["phi122_mev_per_A3amu32"] for code in codes], dtype=float)
    phi_m = np.asarray([m[code]["phi122_mev_per_A3amu32"] for code in codes], dtype=float)
    t_p = np.asarray([p[code]["elapsed_sec"] for code in codes], dtype=float)
    t_m = np.asarray([m[code]["elapsed_sec"] for code in codes], dtype=float)
    if not all(np.isfinite(arr).all() for arr in (phi_p, phi_m, t_p, t_m)):
        raise ValueError("Nonfinite coupling or timing in ranking")
    top_p = set(sorted(codes, key=lambda code: -abs(p[code]["phi122_mev_per_A3amu32"]))[:20])
    top_m = set(sorted(codes, key=lambda code: -abs(m[code]["phi122_mev_per_A3amu32"]))[:20])
    return {
        "geometry_source": prophet_meta["signature"]["geometry_source"],
        "structure_sha256": prophet_meta["signature"]["structure_sha256"],
        "mode_pairs_sha256": prophet_meta["signature"]["mode_pairs_sha256"],
        "n_pairs": len(codes),
        "top20_abs_phi122_overlap": len(top_p & top_m),
        "phi122_signed_pearson": float(np.corrcoef(phi_p, phi_m)[0, 1]),
        "phi122_median_abs_difference_mev_per_A3amu32": float(np.median(np.abs(phi_p - phi_m))),
        "prophet_total_pair_seconds": float(t_p.sum()),
        "mattersim_total_pair_seconds": float(t_m.sum()),
        "sum_pair_time_ratio_prophet_over_mattersim": float(t_p.sum() / t_m.sum()),
        "prophet_median_seconds_per_pair": float(np.median(t_p)),
        "mattersim_median_seconds_per_pair": float(np.median(t_m)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prophet-screening", type=Path, required=True)
    parser.add_argument("--mattersim-screening", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compare(args.prophet_screening, args.mattersim_screening)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
