#!/usr/bin/env python3
"""Generate publication-style figures from the validated phonon campaign data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from scripts.analyze_phonopy_model_campaign import MODELS, _paths


LABELS = {"prophet": "Prophet", "equiformer-v3": "EquiformerV3",
          "tece": "TECE", "equflashv2": "EquFlashV2"}
COLORS = {"prophet": "#577590", "equiformer-v3": "#277da1",
          "tece": "#43aa8b", "equflashv2": "#f8961e"}
MATERIALS = ("mos2", "wse2")


def _save(fig, path: Path) -> None:
    fig.savefig(path, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _style() -> None:
    plt.rcParams.update({"font.size": 9, "axes.titlesize": 10, "axes.labelsize": 9,
                         "xtick.labelsize": 8, "ytick.labelsize": 8,
                         "axes.spines.top": False, "axes.spines.right": False,
                         "grid.alpha": 0.22})


def stage1(campaign: dict, validation: Path, output: Path) -> None:
    x = np.arange(4)
    fig, ax = plt.subplots(1, 3, figsize=(12.4, 3.25), constrained_layout=True)
    for i, material in enumerate(MATERIALS):
        values = [campaign["materials"][material]["models"][model]
                  ["frequency_vs_archived_qe"]["all_mae_thz"] for model in MODELS]
        ax[0].bar(x + (i - .5) * .36, values, .34,
                  color=("#3b708b" if i == 0 else "#b97843"),
                  label=("MoS2" if i == 0 else "WSe2"))
        overlaps = [campaign["materials"][material]["models"][model]
                    ["frequency_vs_archived_qe"]["isolated_mode_median_overlap_squared"]
                    for model in MODELS]
        ax[1].plot(x, overlaps, "o-", color=("#3b708b" if i == 0 else "#b97843"),
                   label=("MoS2" if i == 0 else "WSe2"))
    times = []
    for model in MODELS:
        dataset_file = _paths(validation, "mos2", model, "shared_dft")[0]
        diagnostics = json.loads(dataset_file.read_text())["diagnostics"]
        times.append(diagnostics.get("total_elapsed_seconds", diagnostics.get("elapsed_seconds")))
    ax[2].bar(x, times, color=[COLORS[model] for model in MODELS])
    ax[2].set_yscale("log")
    for i, seconds in enumerate(times):
        ax[2].text(i, seconds * 1.13, f"{seconds:.0f}s", ha="center", fontsize=7.4)
    ax[0].set(title="Matched phonon frequency MAE", ylabel="THz")
    ax[1].set(title="Isolated-mode overlap (median)", ylabel="Overlap squared", ylim=(.994, 1.0004))
    ax[2].set(title="MoS2 Stage1 wall time on CPU", ylabel="seconds, log scale", ylim=(30, 3000))
    for panel in ax:
        panel.set_xticks(x, [LABELS[model] for model in MODELS], rotation=30, ha="right")
        panel.grid(axis="y")
    ax[0].legend(frameon=False, loc="upper left")
    _save(fig, output)


def structure(campaign: dict, output: Path) -> None:
    fig, ax = plt.subplots(1, 2, figsize=(11.5, 3.2), constrained_layout=True)
    x = np.arange(4)
    for i, material in enumerate(MATERIALS):
        block = campaign["materials"][material]
        change = [block["own_geometry"][model]["phonons_vs_same_model_shared_structure"]
                  ["frequency_mae_thz"] for model in MODELS]
        ratio = [block["own_geometry"][model]["top_abs_phi122_mev"]
                 / block["models"][model]["stage2"]["top_abs_phi122_mev"]
                 for model in MODELS]
        offset = (i - .5) * .36
        col = "#3b708b" if i == 0 else "#b97843"
        ax[0].bar(x + offset, change, .34, color=col, label="MoS2" if i == 0 else "WSe2")
        ax[1].bar(x + offset, ratio, .34, color=col, label="MoS2" if i == 0 else "WSe2")
    ax[0].set(title="Frequency shift after model relaxation", ylabel="Matched MAE (THz)")
    ax[1].set(title="Largest coupling after model relaxation", ylabel="Own / shared structure", ylim=(0.75, 1.0))
    for panel in ax:
        panel.set_xticks(x, [LABELS[model] for model in MODELS], rotation=25, ha="right")
        panel.grid(axis="y")
    ax[0].legend(frameon=False)
    _save(fig, output)


def rank_agreement(campaign: dict, output: Path) -> None:
    rows = [(m, model) for m in MATERIALS for model in MODELS[1:]]
    ks = (5, 10, 20, 30)
    overlap = np.asarray([[campaign["materials"][m]["vs_prophet"][model]
                           ["top_k"][str(k)]["overlap"] / k for k in ks]
                          for m, model in rows])
    rho = [campaign["materials"][m]["vs_prophet"][model]
           ["spearman_strength_rank_reliable_channels"] for m, model in rows]
    fig, ax = plt.subplots(1, 2, figsize=(10.9, 3.4), gridspec_kw={"width_ratios": [1.7, 1]},
                           constrained_layout=True)
    im = ax[0].imshow(overlap, vmin=.75, vmax=1, cmap="YlGnBu", aspect="auto")
    ax[0].set(yticks=np.arange(6), yticklabels=[f"{m.upper()} / {LABELS[model]}" for m, model in rows],
              xticks=np.arange(4), xticklabels=[f"Top {k}" for k in ks],
              title="Physical-channel overlap vs Prophet Stage1")
    for i, (m, model) in enumerate(rows):
        for j, k in enumerate(ks):
            count = campaign["materials"][m]["vs_prophet"][model]["top_k"][str(k)]["overlap"]
            ax[0].text(j, i, f"{count}/{k}", ha="center", va="center",
                       color="white" if overlap[i, j] >= .96 else "#142333", fontsize=8)
    ax[1].barh(np.arange(6), rho, color=[COLORS[model] for _, model in rows])
    ax[1].set(title="Rank correlation on reliable channels", xlabel="Spearman rho", xlim=(0, 1),
              yticks=np.arange(6), yticklabels=[])
    ax[1].invert_yaxis()
    for i, value in enumerate(rho):
        ax[1].text(value + .015, i, f"{value:.3f}", va="center", fontsize=8)
    fig.colorbar(im, ax=ax[0], fraction=.03, pad=.03, label="Overlap fraction")
    _save(fig, output)


def curvature(campaign: dict, output: Path) -> None:
    x = np.arange(4)
    fig, ax = plt.subplots(1, 2, figsize=(11, 3.2), constrained_layout=True)
    for j, material in enumerate(MATERIALS):
        values = [campaign["materials"][material]["models"][model]["stage2"]
                  ["target_frequency_fit_vs_stage1_median_abs_difference_thz"] for model in MODELS]
        ax[0].bar(x + (j - .5) * .36, values, .34,
                  color="#3b708b" if j == 0 else "#b97843",
                  label="MoS2" if j == 0 else "WSe2")
        cost = [campaign["materials"][material]["models"][model]["stage2"]
                ["sum_pair_seconds"] / 3600 for model in MODELS]
        ax[1].plot(x, cost, "o-", color="#3b708b" if j == 0 else "#b97843",
                   label="MoS2" if j == 0 else "WSe2")
    ax[0].set(title="Stage2 fitted curvature vs Stage1 frequency", ylabel="Median |Δf| (THz)")
    ax[1].set(title="MatterSim evaluation cost is Stage1-independent", ylabel="Sum of pair times (h)",
              ylim=(1.05, 1.35))
    for panel in ax:
        panel.set_xticks(x, [LABELS[model] for model in MODELS], rotation=25, ha="right")
        panel.grid(axis="y")
    ax[0].legend(frameon=False)
    _save(fig, output)


def archived_dft(campaign: dict, output: Path) -> None:
    fig, ax = plt.subplots(1, 2, figsize=(10.7, 3.45), constrained_layout=True)
    for j, material in enumerate(MATERIALS):
        for model in MODELS:
            rows = campaign["materials"][material]["models"][model]["archived_qe_five"]["rows"]
            xx = [row["qe_abs_phi122_mev"] for row in rows]
            yy = [row["candidate_abs_phi122_mev"] for row in rows]
            ax[j].scatter(xx, yy, s=22, alpha=.85, color=COLORS[model], label=LABELS[model])
        lo, hi = (0, 115) if material == "mos2" else (0, 35)
        ax[j].plot([lo, hi], [lo, hi], "--", linewidth=.8, color="#657781")
        ax[j].set(title=f"{material.upper()}: 5 preselected QE directions",
                  xlabel="Archived QE |Phi122| (meV)", ylabel="MatterSim |Phi122| (meV)",
                  xlim=(lo, hi), ylim=(lo, hi))
        ax[j].grid(alpha=.18)
    ax[0].legend(frameon=False, fontsize=7.3, ncol=2, loc="upper left")
    _save(fig, output)


def screening(replay: dict, output: Path) -> None:
    runs = [d for gs in replay["materials"].values() for ms in gs.values() for d in ms.values()]
    steps = (.5, 1, 1.5, 2)
    methods = [f"six_point_h{s:g}" for s in steps]
    mae = [np.mean([d["screening"][name]["pair_phi122_mae_mev"] for d in runs]) for name in methods]
    rho = [np.mean([d["screening"][name]["channel_score_spearman"] for d in runs]) for name in methods]
    worst = [min(d["screening"][name]["reference_top20_recall_by_proxy_top_n"]["30"]
                 for d in runs) for name in methods]
    fig, ax = plt.subplots(1, 3, figsize=(11.5, 3.0), constrained_layout=True)
    labels = [f"±{s:g}" for s in steps]
    for panel, values, title, ylabel, ylim in (
        (ax[0], mae, "Error relative to 25-point fit", "Mean |ΔPhi122| (meV)", None),
        (ax[1], rho, "Physical-channel rank correlation", "Mean Spearman rho", (0, 1.04)),
        (ax[2], worst, "Worst top-20 recall in proxy top-30", "Recovered channels / 20", (0, 21))):
        panel.bar(labels, values, color=["#9daeb7", "#277da1", "#78a6b3", "#78a6b3"])
        panel.set(title=title, ylabel=ylabel)
        if ylim:
            panel.set_ylim(*ylim)
        for i, value in enumerate(values):
            panel.text(i, value + (.02 if panel != ax[0] else .015),
                       f"{value:.3f}" if panel != ax[2] else str(value), ha="center", fontsize=8)
    _save(fig, output)


def cost(replay: dict, output: Path) -> None:
    runs = [d for gs in replay["materials"].values() for ms in gs.values() for d in ms.values()]
    x = np.asarray((20, 25, 30, 40, 50, 60))
    recall = np.asarray([[d["screening"]["six_point_h1"]
                          ["reference_top20_recall_by_proxy_top_n"][str(n)] for n in x]
                         for d in runs])
    cost = np.asarray([[6 * 486 + 19 * d["screening"]["six_point_h1"]
                        ["expanded_pairs_by_proxy_top_n"][str(n)] for n in x] for d in runs])
    fig, ax = plt.subplots(1, 2, figsize=(10.8, 3.15), constrained_layout=True)
    ax[0].fill_between(x, recall.min(axis=0), recall.max(axis=0), color="#a8cbd1", alpha=.65,
                       label="Range across 16 runs")
    ax[0].plot(x, np.median(recall, axis=0), "o-", color="#277da1", label="Median")
    ax[0].axhline(20, ls="--", lw=.8, color="#77919d")
    ax[0].set(title="Reference top-20 recovered", xlabel="Proxy-selected channels",
              ylabel="Recall / 20", ylim=(17.8, 20.4))
    ax[0].legend(frameon=False, fontsize=7.5)
    ax[1].fill_between(x, cost.min(axis=0), cost.max(axis=0), color="#a8cbd1", alpha=.65)
    ax[1].plot(x, np.median(cost, axis=0), "o-", color="#277da1")
    ax[1].axhline(486 * 25, ls="--", lw=.8, color="#b97843", label="25 points × all 486")
    ax[1].set(title="Energy calls: 6 all + 19 per selected pair", xlabel="Proxy-selected channels",
              ylabel="Energy evaluations", ylim=(2900, 13000))
    ax[1].legend(frameon=False, fontsize=7.5)
    _save(fig, output)


def fourth_order(replay: dict, output: Path) -> None:
    runs = [d for gs in replay["materials"].values() for ms in gs.values() for d in ms.values()]
    steps = (.5, 1, 1.5, 2)
    stencil_mae = [np.mean([d["fourth_order_nine_point"][f"nine_point_h{s:g}"]["mae_mev"]
                            for d in runs]) for s in steps]
    stencil_rho = [np.mean([d["fourth_order_nine_point"][f"nine_point_h{s:g}"]["abs_spearman"]
                            for d in runs]) for s in steps]
    windows = (1.5, 2)
    window_mae = [np.mean([d["window_sensitivity"][f"window_{w:g}"]["fourth_order_mae_mev"]
                           for d in runs]) for w in windows]
    window_rho = [np.mean([d["window_sensitivity"][f"window_{w:g}"]["fourth_order_abs_spearman"]
                           for d in runs]) for w in windows]
    fig, ax = plt.subplots(1, 2, figsize=(10.8, 3.2), constrained_layout=True)
    ax[0].plot(steps, stencil_mae, "o-", color="#b97843", label="9-point stencil")
    ax[0].plot(windows, window_mae, "s--", color="#277da1", label="13-term fit window")
    ax[0].set(title="Fourth-order sensitivity vs center 25 fit", xlabel="|Q| or fit-window limit (Å√amu)",
              ylabel="Mean |ΔPhi1122| (meV)", yscale="log", ylim=(.3, 20))
    ax[0].legend(frameon=False, fontsize=7.5)
    ax[1].plot(steps, stencil_rho, "o-", color="#b97843", label="9-point stencil")
    ax[1].plot(windows, window_rho, "s--", color="#277da1", label="13-term fit window")
    ax[1].set(title="Fourth-order absolute-rank stability", xlabel="|Q| or fit-window limit (Å√amu)",
              ylabel="Mean Spearman rho", ylim=(0, 1.05))
    _save(fig, output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--validation-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    _style()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    campaign = json.loads(args.campaign.read_text())
    replay = json.loads(args.replay.read_text())
    for name, build in (
        ("01_stage1", lambda p: stage1(campaign, args.validation_root, p)),
        ("02_structure", lambda p: structure(campaign, p)),
        ("03_ranking", lambda p: rank_agreement(campaign, p)),
        ("04_curvature", lambda p: curvature(campaign, p)),
        ("05_archived_dft", lambda p: archived_dft(campaign, p)),
        ("06_stencils", lambda p: screening(replay, p)),
        ("07_cost", lambda p: cost(replay, p)),
        ("08_fourth_order", lambda p: fourth_order(replay, p)),
    ):
        path = args.output_dir / f"{name}.png"
        build(path)
        print(path)


if __name__ == "__main__":
    main()
