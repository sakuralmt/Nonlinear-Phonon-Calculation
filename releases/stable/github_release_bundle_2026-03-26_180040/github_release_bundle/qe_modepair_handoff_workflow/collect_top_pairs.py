#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from common import dump_json, extract_energy_ry, fit_pair_grid


def parse_args():
    p = argparse.ArgumentParser(description="Collect completed QE top-pair jobs and fit PES")
    p.add_argument("--run-root", type=str, required=True)
    return p.parse_args()


def main():
    args = parse_args()
    run_root = Path(args.run_root).expanduser().resolve()
    manifest = json.loads((run_root / "run_manifest.json").read_text())
    pair_dirs = [Path(p) for p in manifest["pair_dirs"]]

    final_rows = []
    for pair_dir in pair_dirs:
        pair_meta = json.loads((pair_dir / "pair_meta.json").read_text())
        a1_vals = np.array(pair_meta["a1_vals"], dtype=float)
        a2_vals = np.array(pair_meta["a2_vals"], dtype=float)

        e_grid = np.full((len(a2_vals), len(a1_vals)), np.nan, dtype=float)
        with (pair_dir / "amplitude_grid.csv").open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                a1 = float(row["a1"])
                a2 = float(row["a2"])
                i = int(np.argmin(np.abs(a2_vals - a2)))
                j = int(np.argmin(np.abs(a1_vals - a1)))
                e = extract_energy_ry(pair_dir / row["job_name"] / "scf.out")
                if e is not None:
                    e_grid[i, j] = e

        complete = not np.isnan(e_grid).any()
        pair_result_dir = run_root / "pair_results" / pair_dir.name
        pair_result_dir.mkdir(parents=True, exist_ok=True)
        np.savetxt(pair_result_dir / "energy_grid_ry.dat", e_grid, fmt="%.10f")

        result = {
            "pair_code": pair_dir.name,
            "rank": pair_meta["rank"],
            "consensus": pair_meta["consensus"],
            "n_super": pair_meta["n_super"],
            "n_cells": pair_meta["n_cells"],
            "complete": bool(complete),
        }

        if complete:
            analysis = fit_pair_grid(a1_vals, a2_vals, e_grid, fit_window=1.0)
            result["analysis"] = analysis

            e_ev = e_grid * 13.605693009
            fig, ax = plt.subplots(figsize=(6.0, 5.0))
            c = ax.contourf(a1_vals, a2_vals, e_ev - np.nanmin(e_ev), levels=30, cmap="viridis")
            fig.colorbar(c, ax=ax)
            ax.set_xlabel("A1")
            ax.set_ylabel("A2")
            ax.set_aspect("equal")
            ax.set_title(pair_dir.name)
            fig.tight_layout()
            fig.savefig(pair_result_dir / "pes_map.png", dpi=220, bbox_inches="tight")
            plt.close(fig)

            final_rows.append(
                {
                    "pair_code": pair_dir.name,
                    "rank": pair_meta["rank"],
                    "point_label": pair_meta["consensus"]["point_label"],
                    "qx": pair_meta["consensus"]["qx"],
                    "qy": pair_meta["consensus"]["qy"],
                    "qz": pair_meta["consensus"]["qz"],
                    "gamma_mode_code": pair_meta["consensus"]["gamma_mode_code"],
                    "target_mode_code": pair_meta["consensus"]["target_mode_code"],
                    "consensus_phi122_mean_mev": float(pair_meta["consensus"]["phi122_mean_mev"]),
                    "qe_gamma_axis_freq_thz": analysis["axis_checks"]["mode1_axis_fit"]["freq"].get("thz"),
                    "qe_target_axis_freq_thz": analysis["axis_checks"]["mode2_axis_fit"]["freq"].get("thz"),
                    "qe_phi122_mev": analysis["physics"]["phi_122_mev_per_A3amu32"],
                    "qe_phi112_mev": analysis["physics"]["phi_112_mev_per_A3amu32"],
                    "qe_r2": analysis["r2"],
                    "qe_rmse_ev_supercell": analysis["rmse_ev_supercell"],
                }
            )

        dump_json(pair_result_dir / "summary.json", result)

    final_rows.sort(key=lambda item: abs(item["qe_phi122_mev"]), reverse=True)

    results_dir = run_root / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    ranking_csv = results_dir / "qe_ranking.csv"
    with ranking_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rank",
                "pair_code",
                "point_label",
                "qx",
                "qy",
                "qz",
                "gamma_mode_code",
                "target_mode_code",
                "consensus_phi122_mean_mev",
                "qe_gamma_axis_freq_thz",
                "qe_target_axis_freq_thz",
                "qe_phi122_mev",
                "qe_phi112_mev",
                "qe_r2",
                "qe_rmse_ev_supercell",
            ]
        )
        for rank, row in enumerate(final_rows, start=1):
            writer.writerow(
                [
                    rank,
                    row["pair_code"],
                    row["point_label"],
                    row["qx"],
                    row["qy"],
                    row["qz"],
                    row["gamma_mode_code"],
                    row["target_mode_code"],
                    f"{row['consensus_phi122_mean_mev']:.6f}",
                    f"{row['qe_gamma_axis_freq_thz']:.6f}",
                    f"{row['qe_target_axis_freq_thz']:.6f}",
                    f"{row['qe_phi122_mev']:.6f}",
                    f"{row['qe_phi112_mev']:.6f}",
                    f"{row['qe_r2']:.6f}",
                    f"{row['qe_rmse_ev_supercell']:.6f}",
                ]
            )

    dump_json(results_dir / "qe_ranking.json", {"rows": final_rows})

    print(f"completed pairs: {len(final_rows)}")
    print(f"saved: {ranking_csv}")


if __name__ == "__main__":
    main()
