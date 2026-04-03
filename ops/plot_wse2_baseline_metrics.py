#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

PHI_UNIT = r"meV/(\AA\,amu)^{3/2}"


def parse_args():
    p = argparse.ArgumentParser(description="Plot WSe2 baseline comparison metric charts.")
    p.add_argument("--data-json", required=True)
    p.add_argument("--out-dir", required=True)
    return p.parse_args()


def short_label(pair_label: str) -> str:
    return pair_label.replace("_", "\n")


def plot_phi122_bars(pair_rows: list[dict]):
    labels = [short_label(row["pair_label"]) for row in pair_rows]
    x = np.arange(len(pair_rows))
    width = 0.2
    method_fields = [
        ("GPTFF v2", "phi122_gptff_v2_mev", "#0b6e4f"),
        ("GPTFF v1", "phi122_gptff_v1_mev", "#6a994e"),
        ("CHGNet stage2", "phi122_chgnet_stage2_mev", "#bc4749"),
        ("QE stage3", "phi122_qe_stage3_mev", "#1d3557"),
    ]
    fig, ax = plt.subplots(figsize=(15, 8))
    for idx, (label, field, color) in enumerate(method_fields):
        vals = [row[field] for row in pair_rows]
        ax.bar(x + (idx - 1.5) * width, vals, width=width, label=label, color=color)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel(rf"$\phi_{{122}}$ ({PHI_UNIT})")
    ax.set_title("WSe2 Real-Stage1 Candidate Set: Pairwise $\\phi_{122}$ Comparison")
    ax.legend(frameon=False, ncols=2)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    return fig


def plot_phi122_error(pair_rows: list[dict]):
    labels = [short_label(row["pair_label"]) for row in pair_rows]
    x = np.arange(len(pair_rows))
    width = 0.24
    method_fields = [
        ("GPTFF v2", "delta_phi122_gptff_v2_minus_qe_mev", "#0b6e4f"),
        ("GPTFF v1", "delta_phi122_gptff_v1_minus_qe_mev", "#6a994e"),
        ("CHGNet stage2", "delta_phi122_chgnet_stage2_minus_qe_mev", "#bc4749"),
    ]
    fig, ax = plt.subplots(figsize=(15, 8))
    for idx, (label, field, color) in enumerate(method_fields):
        vals = [row[field] for row in pair_rows]
        ax.bar(x + (idx - 1) * width, vals, width=width, label=label, color=color)
    ax.axhline(0.0, color="black", linewidth=1.0)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel(rf"$\Delta \phi_{{122}}$ relative to QE ({PHI_UNIT})")
    ax.set_title("Deviation from QE Stage3 Reference")
    ax.legend(frameon=False)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    return fig


def plot_rmse(pair_rows: list[dict]):
    labels = [short_label(row["pair_label"]) for row in pair_rows]
    x = np.arange(len(pair_rows))
    method_fields = [
        ("GPTFF v2", "rmse_gptff_v2_ev_supercell", "#0b6e4f"),
        ("GPTFF v1", "rmse_gptff_v1_ev_supercell", "#6a994e"),
        ("CHGNet stage2", "rmse_chgnet_stage2_ev_supercell", "#bc4749"),
        ("QE stage3", "rmse_qe_stage3_ev_supercell", "#1d3557"),
    ]
    fig, ax = plt.subplots(figsize=(15, 8))
    for label, field, color in method_fields:
        vals = [row[field] for row in pair_rows]
        ax.plot(x, vals, marker="o", linewidth=2.0, label=label, color=color)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_yscale("log")
    ax.set_ylabel("RMSE (eV/supercell, log scale)")
    ax.set_title("Residual Comparison Across Screening and QE Fits")
    ax.legend(frameon=False, ncols=2)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    return fig


def plot_summary_metrics(aggregate_rows: list[dict]):
    labels = ["MAE", "RMSE", "MaxAE"]
    x = np.arange(len(labels))
    width = 0.24
    field_names = ["phi122_mae_mev", "phi122_rmse_mev", "phi122_maxae_mev"]
    colors = {
        "GPTFF v2": "#0b6e4f",
        "GPTFF v1": "#6a994e",
        "CHGNet stage2": "#bc4749",
    }
    fig, ax = plt.subplots(figsize=(10, 7))
    for idx, row in enumerate(aggregate_rows):
        vals = [row[field] for field in field_names]
        ax.bar(x + (idx - 1) * width, vals, width=width, label=row["method"], color=colors[row["method"]])
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel(rf"Error magnitude ({PHI_UNIT})")
    ax.set_title("Aggregate Error Metrics Relative to QE Stage3")
    ax.legend(frameon=False)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    return fig


def save_fig(fig, out_dir: Path, stem: str):
    png = out_dir / f"{stem}.png"
    pdf = out_dir / f"{stem}.pdf"
    fig.savefig(png, dpi=220, bbox_inches="tight")
    fig.savefig(pdf, bbox_inches="tight")
    plt.close(fig)
    return [str(png), str(pdf)]


def main():
    args = parse_args()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = json.loads(Path(args.data_json).read_text())
    plt.rcParams.update({"font.size": 12, "axes.spines.top": False, "axes.spines.right": False})
    outputs: list[str] = []
    outputs.extend(save_fig(plot_phi122_bars(payload["pair_rows"]), out_dir, "wse2_phi122_bar_comparison"))
    outputs.extend(save_fig(plot_phi122_error(payload["pair_rows"]), out_dir, "wse2_phi122_error_vs_qe"))
    outputs.extend(save_fig(plot_rmse(payload["pair_rows"]), out_dir, "wse2_rmse_comparison"))
    outputs.extend(save_fig(plot_summary_metrics(payload["aggregate_error_stats"]), out_dir, "wse2_aggregate_error_metrics"))
    for item in outputs:
        print(item)


if __name__ == "__main__":
    main()
