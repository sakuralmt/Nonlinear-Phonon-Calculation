"""Plot the audited Phonopy Stage1 and selected archived-QE comparison."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


MODELS = (
    ("prophet", "Prophet", "#3663a7", "o"),
    ("equiformer-v3", "EquiformerV3", "#099c8f", "s"),
    ("tece", "TECE", "#d47724", "^"),
    ("equflashv2", "EquFlashV2 CPU*", "#a56bb2", "D"),
)


def plot(report: dict, output: Path) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(10.6, 8.4))
    fig.subplots_adjust(left=0.10, right=0.97, bottom=0.11, top=0.93,
                        wspace=0.28, hspace=0.58)
    for col, material in enumerate(("mos2", "wse2")):
        material_data = report["materials"][material]
        ax = axes[0, col]
        for index, (code, label, color, _) in enumerate(MODELS):
            run = material_data["models"][code]
            frequency = run.get("frequency_vs_archived_qe") or run.get(
                "stage1_frequency_vs_archived_qe"
            )
            if frequency is None:
                continue
            ax.bar(index, frequency["all_mae_thz"], color=color, width=0.69)
            ax.text(index, frequency["all_mae_thz"] + 0.002,
                    f"{frequency['all_mae_thz']:.3f}", ha="center", va="bottom", fontsize=8)
        ax.set_xticks(range(len(MODELS)), [entry[1].replace(" CPU*", "*") for entry in MODELS],
                      rotation=18, ha="right")
        ax.set_ylim(0, 0.2)
        ax.set_ylabel("Matched 6x6 phonon MAE (THz)")
        ax.set_title(f"{material.upper()}: archived QE harmonic reference")
        ax.grid(axis="y", alpha=0.2)

        ax = axes[1, col]
        completed = []
        for code, label, color, marker in MODELS:
            run = material_data["models"][code]
            if run["status"] != "complete":
                continue
            rows = run["archived_qe_five"]["rows"]
            x = [row["qe_abs_phi122_mev"] for row in rows]
            y = [row["candidate_abs_phi122_mev"] for row in rows]
            ax.scatter(x, y, c=color, marker=marker, s=45, alpha=0.88,
                       edgecolors="white", linewidths=0.4, label=label)
            completed.append((x, y))
        if completed:
            all_values = np.concatenate([np.asarray(v) for pair in completed for v in pair])
            minimum, maximum = float(np.min(all_values)), float(np.max(all_values))
            pad = max(1.0, 0.07 * (maximum - minimum))
            ax.plot([minimum - pad, maximum + pad], [minimum - pad, maximum + pad],
                    color="#777777", linewidth=1.0, linestyle="--")
            ax.set_xlim(minimum - pad, maximum + pad)
            ax.set_ylim(minimum - pad, maximum + pad)
        ax.set_xlabel(r"Archived QE $|\Phi_{122}|$")
        ax.set_ylabel(r"MatterSim mapped-mode $|\Phi_{122}|$")
        ax.set_title(f"{material.upper()}: five selected directional proxies")
        ax.grid(alpha=0.2)
        if col == 1:
            ax.legend(frameon=False, fontsize=8, loc="upper left")
    fig.text(0.01, 0.012,
             "* EquFlash CPU compatibility path lacks official CUDA parity. Archived QE geometry is unverified; "
             "WSe2 QE Gamma acoustic ASR differs.\n"
             "Coupling units: meV/(Angstrom^3 amu^1.5). Five couplings are preselected mapped-direction "
             "values, not a DFT ranking.",
             ha="left", va="bottom", fontsize=7, color="#555555")
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=190)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plot(json.loads(args.report.read_text()), args.output)
    print(args.output)


if __name__ == "__main__":
    main()
