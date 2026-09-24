"""Plot the weakest isolated-mode overlap at each q of a complete mesh."""

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", nargs="+", type=Path, required=True)
    parser.add_argument("--labels", nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if len(args.results) != len(args.labels):
        parser.error("--results and --labels must have equal length")
    fig, axes = plt.subplots(1, len(args.results), figsize=(4.4 * len(args.results), 4.0), squeeze=False, constrained_layout=True)
    image = None
    for ax, path, label in zip(axes[0], args.results, args.labels):
        data = json.loads(path.read_text())
        mesh = data["mesh"]
        minimum = np.empty((mesh, mesh))
        for row in data["q_records"]:
            qx, qy = np.rint(np.array(row["q_frac"][:2]) * mesh).astype(int) % mesh
            isolated = {
                mode for group in row["degeneracy_groups"]
                if len(group["qe_modes"]) == 1 for mode in group["qe_modes"]
            }
            minimum[qx, qy] = min(row["individual_overlap_squared"][mode - 1] for mode in isolated)
        image = ax.imshow(minimum.T, origin="lower", extent=[-0.5, mesh - 0.5, -0.5, mesh - 0.5], vmin=0.5, vmax=1.0, cmap="viridis", interpolation="nearest")
        ax.set_title(label)
        ax.set_xticks(range(mesh), [f"{i}/{mesh}" for i in range(mesh)], rotation=45)
        ax.set_yticks(range(mesh), [f"{i}/{mesh}" for i in range(mesh)])
        ax.set_xlabel(r"$q_x$ (fractional)")
        ax.set_ylabel(r"$q_y$ (fractional)")
        ax.plot([2, 3], [2, 1], "x", color="white", ms=7, mew=1.5)
    fig.colorbar(image, ax=axes[0], shrink=0.8, label=r"Minimum isolated-mode $|\langle e_{QE}|e_{MLFF}\rangle|^2$")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=200)
    fig.savefig(args.output.with_suffix(".pdf"))


if __name__ == "__main__":
    main()
