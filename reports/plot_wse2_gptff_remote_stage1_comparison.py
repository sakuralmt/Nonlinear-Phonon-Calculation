#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR = ROOT / "reports" / "figures"
DATA_JSON = ROOT / "reports" / "data" / "wse2_remote_stage1_comparison_data.json"
EXTRACT_SCRIPT = ROOT / "reports" / "extract_wse2_remote_stage1_comparison_data.py"


def ensure_data():
    if DATA_JSON.exists():
        return
    subprocess.run([sys.executable, str(EXTRACT_SCRIPT)], check=True, cwd=str(ROOT))


def load_payload():
    ensure_data()
    return json.loads(DATA_JSON.read_text())


def short_label(pair_label: str) -> str:
    return pair_label.replace("_", "\n")


def plot_phi122_bars(pair_rows: list[dict]):
    labels = [short_label(row["pair_label"]) for row in pair_rows]
    x = np.arange(len(pair_rows))
    width = 0.2
    method_fields = [
        ("GPTFF v2", "phi122_gptff_v2_mev", "#0b6e4f"),
        ("GPTFF v1", "phi122_gptff_v1_mev", "#6a994e"),
        ("MatterSim v1 5M", "phi122_mattersim_v1_5m_mev", "#f77f00"),
        ("CHGNet stage2", "phi122_chgnet_stage2_mev", "#bc4749"),
        ("QE stage3", "phi122_qe_stage3_mev", "#1d3557"),
    ]
    method_fields = [(label, field, color) for (label, field, color) in method_fields if any(row.get(field) is not None for row in pair_rows)]

    fig, ax = plt.subplots(figsize=(15, 8))
    center = (len(method_fields) - 1) / 2.0
    for idx, (label, field, color) in enumerate(method_fields):
        vals = [row[field] for row in pair_rows]
        ax.bar(x + (idx - center) * width, vals, width=width, label=label, color=color)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel(r"$\phi_{122}$ (meV)")
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
        ("MatterSim v1 5M", "delta_phi122_mattersim_v1_5m_minus_qe_mev", "#f77f00"),
        ("CHGNet stage2", "delta_phi122_chgnet_stage2_minus_qe_mev", "#bc4749"),
    ]
    method_fields = [(label, field, color) for (label, field, color) in method_fields if any(row.get(field) is not None for row in pair_rows)]

    fig, ax = plt.subplots(figsize=(15, 8))
    center = (len(method_fields) - 1) / 2.0
    for idx, (label, field, color) in enumerate(method_fields):
        vals = [row[field] for row in pair_rows]
        ax.bar(x + (idx - center) * width, vals, width=width, label=label, color=color)

    ax.axhline(0.0, color="black", linewidth=1.0)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel(r"$\Delta \phi_{122}$ relative to QE (meV)")
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
        ("MatterSim v1 5M", "rmse_mattersim_v1_5m_ev_supercell", "#f77f00"),
        ("CHGNet stage2", "rmse_chgnet_stage2_ev_supercell", "#bc4749"),
        ("QE stage3", "rmse_qe_stage3_ev_supercell", "#1d3557"),
    ]
    method_fields = [(label, field, color) for (label, field, color) in method_fields if any(row.get(field) is not None for row in pair_rows)]

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
    field_names = [
        ("phi122_mae_mev", "MAE"),
        ("phi122_rmse_mev", "RMSE"),
        ("phi122_maxae_mev", "MaxAE"),
    ]
    colors = {
        "GPTFF v2": "#0b6e4f",
        "GPTFF v1": "#6a994e",
        "MatterSim v1 5M": "#f77f00",
        "CHGNet stage2": "#bc4749",
    }
    aggregate_rows = [row for row in aggregate_rows if row["method"] in colors]

    fig, ax = plt.subplots(figsize=(10, 7))
    center = (len(aggregate_rows) - 1) / 2.0
    for idx, row in enumerate(aggregate_rows):
        vals = [row[field] for field, _ in field_names]
        ax.bar(x + (idx - center) * width, vals, width=width, label=row["method"], color=colors[row["method"]])

    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.set_ylabel("Error magnitude (meV)")
    ax.set_title("Aggregate Error Metrics Relative to QE Stage3")
    ax.legend(frameon=False)
    ax.grid(axis="y", linestyle="--", alpha=0.3)
    fig.tight_layout()
    return fig


def save_fig(fig, stem: str):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    png = REPORT_DIR / f"{stem}.png"
    pdf = REPORT_DIR / f"{stem}.pdf"
    fig.savefig(png, dpi=220, bbox_inches="tight")
    fig.savefig(pdf, bbox_inches="tight")
    plt.close(fig)
    return png, pdf


def main():
    plt.rcParams.update({"font.size": 12, "axes.spines.top": False, "axes.spines.right": False})
    payload = load_payload()
    pair_rows = payload["pair_rows"]
    aggregate_rows = payload["aggregate_error_stats"]
    outputs = []
    outputs.extend(save_fig(plot_phi122_bars(pair_rows), "wse2_phi122_bar_comparison"))
    outputs.extend(save_fig(plot_phi122_error(pair_rows), "wse2_phi122_error_vs_qe"))
    outputs.extend(save_fig(plot_rmse(pair_rows), "wse2_rmse_comparison"))
    outputs.extend(save_fig(plot_summary_metrics(aggregate_rows), "wse2_aggregate_error_metrics"))
    for path in outputs:
        print(path)


if __name__ == "__main__":
    main()
