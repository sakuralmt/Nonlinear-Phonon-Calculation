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

RY_TO_EV = 13.605693009


def parse_args():
    p = argparse.ArgumentParser(
        description="Plot a 2x2 PES comparison figure for GPTFF v1 / GPTFF v2 / CHGNet / QE."
    )
    p.add_argument("--pair-code", required=True)
    p.add_argument("--gptff-v1-summary", required=True)
    p.add_argument("--gptff-v1-grid", required=True)
    p.add_argument("--gptff-v2-summary", required=True)
    p.add_argument("--gptff-v2-grid", required=True)
    p.add_argument("--chgnet-summary", required=True)
    p.add_argument("--chgnet-grid", required=True)
    p.add_argument("--qe-summary", required=True)
    p.add_argument("--qe-grid", required=True)
    p.add_argument("--qe-pair-meta", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--title", default=None)
    return p.parse_args()


def fit_func(xy, c20, c02, c12, c21, c30, c03, c11, c40, c04, c22, c10, c01, c00):
    x, y = xy
    return (
        c20 * x**2
        + c02 * y**2
        + c12 * x * y**2
        + c21 * x**2 * y
        + c30 * x**3
        + c03 * y**3
        + c11 * x * y
        + c40 * x**4
        + c04 * y**4
        + c22 * x**2 * y**2
        + c10 * x
        + c01 * y
        + c00
    )


def fit_polynomial(a1_vals: np.ndarray, a2_vals: np.ndarray, e_shift: np.ndarray, fit_window: float = 1.0):
    x = np.repeat(a1_vals, len(a2_vals))
    y = np.tile(a2_vals, len(a1_vals))
    z = e_shift.T.reshape(-1)
    mask = (np.abs(x) <= fit_window) & (np.abs(y) <= fit_window)
    x_fit = x[mask]
    y_fit = y[mask]
    z_fit = z[mask]
    design = np.column_stack(
        [
            x_fit**2,
            y_fit**2,
            x_fit * y_fit**2,
            x_fit**2 * y_fit,
            x_fit**3,
            y_fit**3,
            x_fit * y_fit,
            x_fit**4,
            y_fit**4,
            x_fit**2 * y_fit**2,
            x_fit,
            y_fit,
            np.ones_like(x_fit),
        ]
    )
    params, _, _, _ = np.linalg.lstsq(design, z_fit, rcond=None)
    pred = fit_func((x, y), *params)
    residuals = pred - z
    sse = float(np.sum(residuals**2))
    sst = float(np.sum((z - np.mean(z)) ** 2))
    r2 = 1.0 - sse / sst if sst > 0 else 1.0
    rmse = float(np.sqrt(np.mean(residuals**2)))
    names = ["c20", "c02", "c12", "c21", "c30", "c03", "c11", "c40", "c04", "c22", "c10", "c01", "c00"]
    coeffs = {name: float(value) for name, value in zip(names, params)}
    return coeffs, r2, rmse


def evaluate_fit_surface(a1_vals: np.ndarray, a2_vals: np.ndarray, coeffs: dict) -> np.ndarray:
    xg, yg = np.meshgrid(a1_vals, a2_vals, indexing="xy")
    ordered = [
        coeffs["c20"],
        coeffs["c02"],
        coeffs["c12"],
        coeffs["c21"],
        coeffs["c30"],
        coeffs["c03"],
        coeffs["c11"],
        coeffs["c40"],
        coeffs["c04"],
        coeffs["c22"],
        coeffs["c10"],
        coeffs["c01"],
        coeffs["c00"],
    ]
    return fit_func((xg.reshape(-1), yg.reshape(-1)), *ordered).reshape(xg.shape)


def _load_stage2(summary_path: Path, grid_path: Path, label: str) -> dict:
    summary = json.loads(summary_path.read_text())
    data = np.loadtxt(grid_path, dtype=float)
    grid_axes = summary.get("grid_axes", {})
    if grid_axes:
        a1 = np.array(grid_axes["a1_vals"], dtype=float)
        a2 = np.array(grid_axes["a2_vals"], dtype=float)
    else:
        n2, n1 = data.shape
        a1 = np.linspace(-2.0, 2.0, n1)
        a2 = np.linspace(-2.0, 2.0, n2)
    e_shift = data - np.nanmin(data)
    coeffs, r2, rmse = fit_polynomial(a1, a2, e_shift, fit_window=float(summary["analysis"].get("fit_window", 1.0)))
    return {
        "label": label,
        "pair_code": summary["pair_code"],
        "a1": a1,
        "a2": a2,
        "data": data,
        "fit": evaluate_fit_surface(a1, a2, coeffs),
        "phi122_mev": summary["analysis"]["physics"]["phi_122_mev_per_A3amu32"],
        "r2": r2,
        "rmse_ev": rmse,
        "c10": coeffs["c10"],
        "c01": coeffs["c01"],
    }


def _load_qe(summary_path: Path, pair_meta_path: Path, grid_path: Path, label: str) -> dict:
    summary = json.loads(summary_path.read_text())
    pair_meta = json.loads(pair_meta_path.read_text())
    data = np.loadtxt(grid_path, dtype=float) * RY_TO_EV
    a1 = np.array(pair_meta["a1_vals"], dtype=float)
    a2 = np.array(pair_meta["a2_vals"], dtype=float)
    e_shift = data - np.nanmin(data)
    coeffs, r2, rmse = fit_polynomial(a1, a2, e_shift, fit_window=float(summary["analysis"].get("fit_window", 1.0)))
    return {
        "label": label,
        "pair_code": summary["pair_code"],
        "a1": a1,
        "a2": a2,
        "data": data,
        "fit": evaluate_fit_surface(a1, a2, coeffs),
        "phi122_mev": summary["analysis"]["physics"]["phi_122_mev_per_A3amu32"],
        "r2": r2,
        "rmse_ev": rmse,
        "c10": coeffs["c10"],
        "c01": coeffs["c01"],
    }


def _shifted(arr: np.ndarray) -> np.ndarray:
    return arr - np.nanmin(arr)


def _plot_panel(ax, payload: dict, zmax: float) -> None:
    xg, yg = np.meshgrid(payload["a1"], payload["a2"], indexing="xy")
    data = _shifted(payload["data"])
    fit = _shifted(payload["fit"])
    ax.plot_surface(
        xg,
        yg,
        fit,
        cmap="viridis",
        alpha=0.55,
        linewidth=0,
        antialiased=True,
    )
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
    ax.set_title(
        f"{payload['label']}\n"
        f"{payload['pair_code']}\n"
        f"phi122={payload['phi122_mev']:.3f} meV, r2={payload['r2']:.6f}, rmse={payload['rmse_ev']:.6f} eV\n"
        f"c10={payload['c10']:.6f}, c01={payload['c01']:.6f}"
    )


def main():
    args = parse_args()
    payloads = [
        _load_stage2(Path(args.gptff_v1_summary), Path(args.gptff_v1_grid), "Stage2 GPTFF v1"),
        _load_stage2(Path(args.gptff_v2_summary), Path(args.gptff_v2_grid), "Stage2 GPTFF v2"),
        _load_stage2(Path(args.chgnet_summary), Path(args.chgnet_grid), "Stage2 CHGNet"),
        _load_qe(Path(args.qe_summary), Path(args.qe_pair_meta), Path(args.qe_grid), "Stage3 QE"),
    ]
    for payload in payloads:
        if payload["pair_code"] != args.pair_code:
            raise ValueError(f"Pair mismatch: expected {args.pair_code}, got {payload['pair_code']}")
    zmax = max(
        max(float(np.nanmax(_shifted(p["data"]))), float(np.nanmax(_shifted(p["fit"]))))
        for p in payloads
    )
    fig = plt.figure(figsize=(14.5, 12.0))
    axes = [
        fig.add_subplot(221, projection="3d"),
        fig.add_subplot(222, projection="3d"),
        fig.add_subplot(223, projection="3d"),
        fig.add_subplot(224, projection="3d"),
    ]
    for ax, payload in zip(axes, payloads):
        _plot_panel(ax, payload, zmax)
    title = args.title or f"PES comparison for {args.pair_code}: GPTFF v1 / GPTFF v2 / CHGNet / QE"
    fig.suptitle(title, y=0.98, fontsize=15)
    fig.tight_layout()
    out = Path(args.out).expanduser().resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=220, bbox_inches="tight")
    print(out)


if __name__ == "__main__":
    main()
