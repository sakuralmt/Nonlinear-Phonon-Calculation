#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401


def parse_args():
    p = argparse.ArgumentParser(description="Compare quartic and quintic refits for one PES grid.")
    p.add_argument("--label", required=True)
    p.add_argument("--pair-code", required=True)
    p.add_argument("--summary", required=True)
    p.add_argument("--grid", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--summary-out", default=None)
    return p.parse_args()


def fit_func(xy, *params):
    x, y = xy
    return sum(coeff * (x**i) * (y**j) for coeff, (i, j) in params)


def fit_surface(a: np.ndarray, grid: np.ndarray, basis: list[tuple[str, tuple[int, int]]]):
    x = np.repeat(a, len(a))
    y = np.tile(a, len(a))
    z = (grid - np.min(grid)).T.reshape(-1)
    design = np.column_stack([(x**i) * (y**j) for _, (i, j) in basis])
    coef, *_ = np.linalg.lstsq(design, z, rcond=None)
    pred = design @ coef
    resid = pred - z
    rmse = float(np.sqrt(np.mean(resid**2)))
    sst = float(np.sum((z - np.mean(z)) ** 2))
    r2 = 1.0 - float(np.sum(resid**2)) / sst if sst > 0 else 1.0
    coeff_map = {name: float(v) for (name, _), v in zip(basis, coef)}
    xg, yg = np.meshgrid(a, a, indexing="xy")
    fitg = np.zeros_like(xg, dtype=float)
    for name, (i, j) in basis:
        fitg += coeff_map[name] * (xg**i) * (yg**j)
    phi122 = None if "c12" not in coeff_map else 2.0 * coeff_map["c12"] * 1000.0
    return {"fit_grid": fitg, "r2": r2, "rmse": rmse, "phi122_mev": phi122}


def plot_panel(ax, a: np.ndarray, grid: np.ndarray, fit_grid: np.ndarray, title: str, zmax: float):
    xg, yg = np.meshgrid(a, a, indexing="xy")
    data = grid - np.min(grid)
    fit = fit_grid - np.min(fit_grid)
    ax.plot_surface(xg, yg, fit, cmap="viridis", alpha=0.55, linewidth=0, antialiased=True)
    ax.scatter(
        xg.reshape(-1),
        yg.reshape(-1),
        data.reshape(-1) + max(1e-4, 0.006 * zmax),
        c=data.reshape(-1),
        cmap="viridis",
        s=22,
        edgecolors="black",
        linewidths=0.25,
        depthshade=False,
    )
    ax.set_xlabel("A1")
    ax.set_ylabel("A2")
    ax.set_zlabel("ΔE (eV/supercell)")
    ax.set_zlim(0.0, zmax)
    ax.view_init(elev=28, azim=-58)
    ax.set_title(title)


def main():
    args = parse_args()
    summary = json.loads(Path(args.summary).read_text())
    grid = np.loadtxt(args.grid)
    grid_axes = summary.get("grid_axes", {})
    if grid_axes:
        a = np.array(grid_axes["a1_vals"], dtype=float)
    else:
        a = np.linspace(-2.0, 2.0, grid.shape[1])
    quartic_basis = [
        ("c20", (2, 0)),
        ("c02", (0, 2)),
        ("c12", (1, 2)),
        ("c21", (2, 1)),
        ("c30", (3, 0)),
        ("c03", (0, 3)),
        ("c11", (1, 1)),
        ("c40", (4, 0)),
        ("c04", (0, 4)),
        ("c22", (2, 2)),
        ("c10", (1, 0)),
        ("c01", (0, 1)),
        ("c00", (0, 0)),
    ]
    quintic_basis = [(f"c{i}{j}", (i, j)) for deg in range(0, 6) for i in range(deg, -1, -1) for j in [deg - i]]
    quartic = fit_surface(a, grid, quartic_basis)
    quintic = fit_surface(a, grid, quintic_basis)
    zmax = max(
        float(np.max(grid - np.min(grid))),
        float(np.max(quartic["fit_grid"] - np.min(quartic["fit_grid"]))),
        float(np.max(quintic["fit_grid"] - np.min(quintic["fit_grid"]))),
    )
    fig = plt.figure(figsize=(13.5, 6.0))
    ax1 = fig.add_subplot(121, projection="3d")
    ax2 = fig.add_subplot(122, projection="3d")
    plot_panel(
        ax1,
        a,
        grid,
        quartic["fit_grid"],
        f"{args.label} | quartic\nphi122={quartic['phi122_mev']:.3f} meV, r2={quartic['r2']:.6f}, rmse={quartic['rmse']:.6f} eV",
        zmax,
    )
    plot_panel(
        ax2,
        a,
        grid,
        quintic["fit_grid"],
        f"{args.label} | quintic\nphi122={quintic['phi122_mev']:.3f} meV, r2={quintic['r2']:.6f}, rmse={quintic['rmse']:.6f} eV",
        zmax,
    )
    fig.suptitle(f"{args.label} high-order refit check for {args.pair_code}", y=0.98, fontsize=14)
    fig.tight_layout()
    out = Path(args.out).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=220, bbox_inches="tight")
    print(out)
    delta_phi = quintic["phi122_mev"] - quartic["phi122_mev"]
    print(
        f"quartic phi122={quartic['phi122_mev']:.6f} meV, quintic phi122={quintic['phi122_mev']:.6f} meV, "
        f"delta={delta_phi:.6f} meV; quartic rmse={quartic['rmse']:.6f} eV, "
        f"quintic rmse={quintic['rmse']:.6f} eV"
    )
    if args.summary_out:
        summary_path = Path(args.summary_out).expanduser().resolve()
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        quartic_summary = {k: v for k, v in quartic.items() if k != "fit_grid"}
        quintic_summary = {k: v for k, v in quintic.items() if k != "fit_grid"}
        summary_path.write_text(
            json.dumps(
                {
                    "label": args.label,
                    "pair_code": args.pair_code,
                    "quartic": quartic_summary,
                    "quintic": quintic_summary,
                    "phi122_delta_mev": delta_phi,
                    "artifact": str(out),
                },
                indent=2,
            )
        )
        print(summary_path)


if __name__ == "__main__":
    main()
