#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RUNS_DIR = SCRIPT_DIR / "runs"


def parse_args():
    p = argparse.ArgumentParser(description="Compare multiple screening runs and build a consensus pair ranking.")
    p.add_argument("--runs-dir", type=str, default=str(DEFAULT_RUNS_DIR))
    p.add_argument("--run-tags", nargs="+", required=True, help="Run tags to combine")
    p.add_argument("--top-n", type=int, default=20)
    return p.parse_args()


def load_rows(path: Path):
    return list(csv.DictReader(path.open()))


def main():
    args = parse_args()
    runs_dir = Path(args.runs_dir).expanduser().resolve()
    run_tags = list(args.run_tags)

    by_pair = {}
    for tag in run_tags:
        ranking_csv = runs_dir / tag / "screening" / "pair_ranking.csv"
        rows = load_rows(ranking_csv)
        total = len(rows)
        for idx, row in enumerate(rows, start=1):
            pair_code = row["pair_code"]
            entry = by_pair.setdefault(
                pair_code,
                {
                    "pair_code": pair_code,
                    "coupling_type": row["coupling_type"],
                    "point_label": row["point_label"],
                    "qx": row["qx"],
                    "qy": row["qy"],
                    "qz": row["qz"],
                    "gamma_mode_code": row["gamma_mode_code"],
                    "target_mode_code": row["target_mode_code"],
                    "per_run": {},
                },
            )
            entry["per_run"][tag] = {
                "rank": idx,
                "norm_rank": idx / total,
                "phi122_mev": float(row["phi122_mev"]),
                "gamma_freq_fit_thz": float(row["gamma_freq_fit_thz"]) if row["gamma_freq_fit_thz"] else None,
                "target_freq_fit_thz": float(row["target_freq_fit_thz"]) if row["target_freq_fit_thz"] else None,
            }

    consensus_rows = []
    for pair_code, entry in by_pair.items():
        if any(tag not in entry["per_run"] for tag in run_tags):
            continue
        norm_ranks = [entry["per_run"][tag]["norm_rank"] for tag in run_tags]
        phi_vals = [entry["per_run"][tag]["phi122_mev"] for tag in run_tags]
        consensus_rows.append(
            {
                "pair_code": pair_code,
                "coupling_type": entry["coupling_type"],
                "point_label": entry["point_label"],
                "qx": entry["qx"],
                "qy": entry["qy"],
                "qz": entry["qz"],
                "gamma_mode_code": entry["gamma_mode_code"],
                "target_mode_code": entry["target_mode_code"],
                "mean_norm_rank": sum(norm_ranks) / len(norm_ranks),
                "max_norm_rank": max(norm_ranks),
                "phi122_mean_mev": sum(phi_vals) / len(phi_vals),
                "phi122_min_mev": min(phi_vals),
                "phi122_max_mev": max(phi_vals),
                "per_run": entry["per_run"],
            }
        )

    consensus_rows.sort(key=lambda item: (item["mean_norm_rank"], item["max_norm_rank"]))
    consensus_rows = consensus_rows[: args.top_n]

    out_dir = runs_dir / f"consensus_{'_'.join(run_tags)}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_json = out_dir / "consensus_pair_ranking.json"
    out_json.write_text(json.dumps({"run_tags": run_tags, "rows": consensus_rows}, indent=2))

    out_csv = out_dir / "consensus_pair_ranking.csv"
    with out_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rank",
                "pair_code",
                "coupling_type",
                "point_label",
                "qx",
                "qy",
                "qz",
                "gamma_mode_code",
                "target_mode_code",
                "mean_norm_rank",
                "max_norm_rank",
                "phi122_mean_mev",
                "phi122_min_mev",
                "phi122_max_mev",
            ]
        )
        for idx, row in enumerate(consensus_rows, start=1):
            writer.writerow(
                [
                    idx,
                    row["pair_code"],
                    row["coupling_type"],
                    row["point_label"],
                    row["qx"],
                    row["qy"],
                    row["qz"],
                    row["gamma_mode_code"],
                    row["target_mode_code"],
                    f"{row['mean_norm_rank']:.6f}",
                    f"{row['max_norm_rank']:.6f}",
                    f"{row['phi122_mean_mev']:.6f}",
                    f"{row['phi122_min_mev']:.6f}",
                    f"{row['phi122_max_mev']:.6f}",
                ]
            )

    print(f"saved: {out_csv}")
    print(f"saved: {out_json}")


if __name__ == "__main__":
    main()
