#!/usr/bin/env python3
"""Plot the measured screening-replay tradeoff and modeled energy-call cost."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analysis", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data = json.loads(args.analysis.read_text())
    runs = [run for structures in data["materials"].values()
            for models in structures.values() for run in models.values()]
    names = ["six_point_h0.5", "six_point_h1", "six_point_h1.5", "six_point_h2"]
    labels = ["±0.5", "±1.0", "±1.5", "±2.0"]
    mae = [np.mean([run["screening"][name]["pair_phi122_mae_mev"] for run in runs])
           for name in names]
    worst_recall = [min(run["screening"][name]["reference_top20_recall_by_proxy_top_n"]["30"]
                        for run in runs) for name in names]
    expanded = [run["screening"]["six_point_h1"]["expanded_pairs_by_proxy_top_n"]["30"]
                for run in runs]
    staged = [6 * 486 + 19 * count for count in expanded]
    costs = [486 * 81, 486 * 25, 486 * 6, int(np.mean(staged))]
    fig, axes = plt.subplots(1, 3, figsize=(12.4, 3.6), constrained_layout=True)
    colors = ["#8c9cab", "#226b89", "#90a7ad", "#90a7ad"]
    axes[0].bar(labels, mae, color=colors)
    axes[0].set(title="Six-point step: error vs 25-point fit", ylabel="Mean |ΔΦ₁₂₂| (meV)")
    for i, value in enumerate(mae):
        axes[0].text(i, value + 0.015, f"{value:.3f}", ha="center", fontsize=8)
    axes[1].bar(labels, worst_recall, color=colors)
    axes[1].set(title="Worst top-20 recall in proxy top-30", ylabel="Channels recovered / 20", ylim=(0, 21))
    for i, value in enumerate(worst_recall):
        axes[1].text(i, value + 0.2, str(value), ha="center", fontsize=8)
    cost_labels = ["81 all", "25 all", "6 all", "6 + 25\ntop-30"]
    axes[2].bar(cost_labels, costs, color=["#798e9b", "#6b9eae", "#92b9bd", "#226b89"])
    axes[2].set(title="Energy calls per 486-pair run", ylabel="Calculated points")
    for i, value in enumerate(costs):
        axes[2].text(i, value + 500, f"{value:,}", ha="center", fontsize=8)
    fig.suptitle("Offline replay: 16 MatterSim runs on MoS₂ and WSe₂", fontsize=12)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=180)
    plt.close(fig)
    print(args.output)


if __name__ == "__main__":
    main()
