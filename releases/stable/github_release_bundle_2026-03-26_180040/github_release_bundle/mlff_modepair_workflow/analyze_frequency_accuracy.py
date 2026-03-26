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


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_RUNS_DIR = SCRIPT_DIR / "runs"


def parse_args():
    p = argparse.ArgumentParser(description="Summarize MLFF frequency accuracy from pair screening outputs.")
    p.add_argument("--backend", type=str, default="chgnet")
    p.add_argument("--run-tag", type=str, default=None, help="Optional run tag; defaults to backend")
    p.add_argument("--runs-dir", type=str, default=str(DEFAULT_RUNS_DIR))
    return p.parse_args()


def main():
    args = parse_args()
    runs_dir = Path(args.runs_dir).expanduser().resolve()
    run_tag = args.run_tag or args.backend
    ranking_csv = runs_dir / run_tag / "screening" / "pair_ranking.csv"
    out_dir = runs_dir / run_tag / "analysis"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = list(csv.DictReader(ranking_csv.open()))
    for row in rows:
        row["gamma_freq_ref_thz"] = float(row["gamma_freq_ref_thz"])
        row["gamma_freq_fit_thz"] = float(row["gamma_freq_fit_thz"]) if row["gamma_freq_fit_thz"] else np.nan
        row["target_freq_ref_thz"] = float(row["target_freq_ref_thz"])
        row["target_freq_fit_thz"] = float(row["target_freq_fit_thz"]) if row["target_freq_fit_thz"] else np.nan
        row["phi122_mev"] = float(row["phi122_mev"])
        row["phi112_mev"] = float(row["phi112_mev"])
        row["r2"] = float(row["r2"])
        row["rmse_ev_supercell"] = float(row["rmse_ev_supercell"])

    point_labels = sorted(set(row["point_label"] for row in rows))
    stats = {}
    for point in point_labels:
        sub = [row for row in rows if row["point_label"] == point]
        dg = np.array([row["gamma_freq_fit_thz"] - row["gamma_freq_ref_thz"] for row in sub], dtype=float)
        dq = np.array([row["target_freq_fit_thz"] - row["target_freq_ref_thz"] for row in sub], dtype=float)
        stats[point] = {
            "count": len(sub),
            "gamma_freq_mean_error_thz": float(np.nanmean(dg)),
            "gamma_freq_mean_abs_error_thz": float(np.nanmean(np.abs(dg))),
            "q_freq_mean_error_thz": float(np.nanmean(dq)),
            "q_freq_mean_abs_error_thz": float(np.nanmean(np.abs(dq))),
            "phi122_mean_mev": float(np.nanmean([row["phi122_mev"] for row in sub])),
            "phi122_mean_abs_mev": float(np.nanmean([abs(row["phi122_mev"]) for row in sub])),
        }

    dump = {
        "backend": args.backend,
        "run_tag": run_tag,
        "n_pairs": len(rows),
        "group_stats_by_point_label": stats,
        "overall": {
            "gamma_freq_mean_abs_error_thz": float(np.nanmean([abs(row["gamma_freq_fit_thz"] - row["gamma_freq_ref_thz"]) for row in rows])),
            "q_freq_mean_abs_error_thz": float(np.nanmean([abs(row["target_freq_fit_thz"] - row["target_freq_ref_thz"]) for row in rows])),
            "phi122_mean_abs_mev": float(np.nanmean([abs(row["phi122_mev"]) for row in rows])),
        },
    }
    (out_dir / "frequency_accuracy_summary.json").write_text(json.dumps(dump, indent=2))

    color_map = {"M": "#1B998B", "K": "#2E86AB", "line": "#F4A261"}
    colors = [color_map.get(row["point_label"], "#666666") for row in rows]

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.8))

    xg = np.array([row["gamma_freq_ref_thz"] for row in rows], dtype=float)
    yg = np.array([row["gamma_freq_fit_thz"] for row in rows], dtype=float)
    axes[0].scatter(xg, yg, c=colors, s=28, alpha=0.85)
    mn = min(np.nanmin(xg), np.nanmin(yg))
    mx = max(np.nanmax(xg), np.nanmax(yg))
    axes[0].plot([mn, mx], [mn, mx], "k--", lw=1.0)
    axes[0].set_xlabel("QE Gamma-mode frequency (THz)")
    axes[0].set_ylabel(f"{args.backend} fitted Gamma-axis frequency (THz)")
    axes[0].set_title("Gamma Frequency Comparison")
    axes[0].grid(alpha=0.25)

    xq = np.array([row["target_freq_ref_thz"] for row in rows], dtype=float)
    yq = np.array([row["target_freq_fit_thz"] for row in rows], dtype=float)
    axes[1].scatter(xq, yq, c=colors, s=28, alpha=0.85)
    mn = min(np.nanmin(xq), np.nanmin(yq))
    mx = max(np.nanmax(xq), np.nanmax(yq))
    axes[1].plot([mn, mx], [mn, mx], "k--", lw=1.0)
    axes[1].set_xlabel("QE q-mode frequency (THz)")
    axes[1].set_ylabel(f"{args.backend} fitted q-axis frequency (THz)")
    axes[1].set_title("Finite-q Frequency Comparison")
    axes[1].grid(alpha=0.25)

    fig.tight_layout()
    fig.savefig(out_dir / "freq_qe_vs_mlff_scatter.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.8))
    bins = np.linspace(-5, 5, 25)
    for point in point_labels:
        sub = [row for row in rows if row["point_label"] == point]
        dq = np.array([row["target_freq_fit_thz"] - row["target_freq_ref_thz"] for row in sub], dtype=float)
        axes[0].hist(dq, bins=bins, alpha=0.55, label=point)
    axes[0].axvline(0.0, color="k", ls="--", lw=1.0)
    axes[0].set_xlabel(f"{args.backend} - QE q-mode frequency (THz)")
    axes[0].set_ylabel("Count")
    axes[0].set_title("Finite-q Frequency Error")
    axes[0].legend()

    means = [stats[p]["q_freq_mean_abs_error_thz"] for p in point_labels]
    axes[1].bar(point_labels, means, color=[color_map.get(p, "#666666") for p in point_labels])
    axes[1].set_ylabel("Mean |Delta omega_q| (THz)")
    axes[1].set_title("Average q-mode Error by q Family")
    axes[1].grid(alpha=0.25, axis="y")

    fig.tight_layout()
    fig.savefig(out_dir / "freq_error_summary.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    print(f"saved: {out_dir / 'frequency_accuracy_summary.json'}")
    print(f"saved: {out_dir / 'freq_qe_vs_mlff_scatter.png'}")
    print(f"saved: {out_dir / 'freq_error_summary.png'}")


if __name__ == "__main__":
    main()
