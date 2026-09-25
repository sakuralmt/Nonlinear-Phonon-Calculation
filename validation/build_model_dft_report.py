"""Build the thesis-style model comparison from archived DFT and MLFF grids.

No DFT generation: the reference set stays at the five rechecked pairs per
material. New grids are the stable MatterSim re-evaluation of existing ML modes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import spearmanr

from mlff_modepair_workflow.units import energies_to_ev
from mlff_modepair_workflow.core import (
    decode_complex_mode,
    infer_commensurate_supercell_n,
)

MODELS = ("tece", "prophet", "equiformer-v3")
LABELS = {
    "tece": "TECE + MS",
    "prophet": "Prophet + MS",
    "equiformer-v3": "EquiformerV3 + MS",
    "qe-ms": "QE + MS (archive)",
}
COLORS = {
    "qe-ms": "#7d7d7d",
    "tece": "#26778e",
    "prophet": "#d27330",
    "equiformer-v3": "#7556a2",
}
PAIR_NAMES = ["M9", "M8", "M7", "K9", "L8"]
UNIT3 = r"meV / ($\AA^3$ amu$^{3/2}$)"
UNIT4 = r"meV / ($\AA^4$ amu$^2$)"


def read(p):
    return json.loads(p.read_text())


def digest(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def norms(pair):
    q = np.array(pair["target_mode"]["q_frac"])
    n = infer_commensurate_supercell_n(q)
    phase = np.exp(
        2j * np.pi * (np.array([[i, j, 0] for i in range(n) for j in range(n)]) @ q)
    )
    g = decode_complex_mode(pair["gamma_mode"]["eigenvector"])
    v = decode_complex_mode(pair["target_mode"]["eigenvector_q"])
    return float(np.linalg.norm(g.real)), float(
        np.linalg.norm((phase[:, None, None] * v).real) / n
    )


def metrics(reference, predicted):
    r, p = np.asarray(reference), np.asarray(predicted)
    if (
        len(r) == 0
        or r.shape != p.shape
        or not np.isfinite(r).all()
        or not np.isfinite(p).all()
    ):
        raise ValueError("Invalid metric input")
    d = p - r
    return {
        "count": len(r),
        "mae": float(np.mean(abs(d))),
        "rmse": float(np.sqrt(np.mean(d * d))),
        "maxae": float(max(abs(d))),
        "bias": float(np.mean(d)),
        "mape_percent": float(np.mean(abs(d / r)) * 100) if np.all(r != 0) else None,
        "spearman": float(spearmanr(r, p).statistic) if len(r) > 1 else None,
    }


def csv_write(path, rows):
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--results", type=Path, required=True)
    p.add_argument("--reference", type=Path, required=True)
    p.add_argument("--materials", type=Path, required=True)
    p.add_argument("--evidence", type=Path, required=True)
    p.add_argument("--accepted-runs", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    out = a.output
    data = out / "acceptance_data"
    figdir = out / "figures"
    data.mkdir(exist_ok=True, parents=True)
    figdir.mkdir(exist_ok=True)
    old_rows = list(csv.DictReader((data / "archived_dft_couplings.csv").open()))
    archive = read(data / "archived_dft_comparison.json")
    states = [read(f) for f in sorted(a.results.glob("*/*/*.json"))]
    if len(states) != 30:
        raise ValueError(f"Need 30 complete grids, got {len(states)}")
    lookup = {(s["material"], s["model"], s["qe_pair"]): s for s in states}
    if len(lookup) != 30:
        raise ValueError("Duplicate result")
    records = []
    summary = {}
    references = {}
    sources = []
    for mat in ("mos2", "wse2"):
        mos = mat == "mos2"
        pairpath = a.reference / (
            "mos2_stage2_stage3_core_20260402/baseline_local/mos2_gptff_v1_stage3_run/stage1/outputs/mode_pairs.selected.json"
            if mos
            else "wse2_stage2_stage3_core_20260402/baseline_local/wse2_stage3_run/stage1/outputs/mode_pairs.selected.json"
        )
        qepath = a.reference / (
            "mos2_stage2_stage3_core_20260402/baseline_local/mos2_gptff_v1_stage3_run/stage3_qe/gptff/results/qe_ranking.json"
            if mos
            else "wse2_stage2_stage3_core_20260402/baseline_local/wse2_stage3_run/stage3_qe/chgnet/results/qe_ranking.json"
        )
        mspath = a.materials / (
            "mos2/core_packages/mos2_stage2_stage3_core_20260402/stage2_models/mattersim_v1_5m/stage2_outputs/mattersim_v1_5m/screening/pair_ranking.json"
            if mos
            else "wse2/supporting_stage2/wse2_mattersim_v1_5m_stage2_20260403/stage2/outputs/mattersim_v1_5m/screening/pair_ranking.json"
        )
        pairs = {x["pair_code"]: x for x in read(pairpath)["pairs"]}
        qe = {x["pair_code"]: x for x in read(qepath)["rows"]}
        ms = {x["pair_code"]: x for x in read(mspath)["pairs"]}
        for path in (pairpath, qepath, mspath):
            sources.append({"path": str(path), "sha256": digest(path)})
        codes = list(archive["legacy_fits"][mat])
        references[mat] = {}
        for code in codes:
            fit = archive["legacy_fits"][mat][code]
            gn, qn = norms(pairs[code])
            scale = 1 / (gn * qn**2)
            archived_phi = abs(qe[code]["qe_phi122_mev"]) * scale
            refphi = (
                abs(fit["analysis"]["physics"]["phi_122_mev_per_A3amu32"])
                if "analysis" in fit
                else archived_phi
            )
            if abs(archived_phi - refphi) > 0.001:
                raise ValueError(
                    "Recovered DFT differs from normalized archived third derivative"
                )
            ref4 = (
                fit["analysis"]["physics"]["phi_1122_mev_per_A4amu2"]
                if "analysis" in fit
                else None
            )
            references[mat][code] = {
                "phi122": refphi,
                "phi1122": ref4,
                "legacy_phi122": qe[code]["qe_phi122_mev"],
                "legacy_to_unit_norm_scale": scale,
                "gamma_frequency": pairs[code]["gamma_mode"]["freq_thz"],
                "q_frequency": pairs[code]["target_mode"]["freq_thz"],
                "fit": fit,
                "pair": pairs[code],
            }
            for model in ("qe-ms", *MODELS):
                ref = references[mat][code]
                base = {
                    "material": mat,
                    "model": model,
                    "qe_pair": code,
                    "qe_phi122": refphi,
                    "qe_phi1122": ref4,
                    "qe_gamma_thz": ref["gamma_frequency"],
                    "qe_q_thz": ref["q_frequency"],
                    "legacy_to_unit_norm_scale": scale,
                }
                if model == "qe-ms":
                    v = ms[code]
                    base.update(
                        {
                            "phi122": abs(v["phi122_mev"]) * scale,
                            "phi1122": None,
                            "stage1_gamma_thz": ref["gamma_frequency"],
                            "stage1_q_thz": ref["q_frequency"],
                            "stage2_gamma_thz": v["gamma_freq_fit_thz"] / gn,
                            "stage2_q_thz": v["target_freq_fit_thz"] / qn,
                            "fit_center_rmse_ev": None,
                            "fit_wide_rmse_ev": v["rmse_ev_supercell"],
                            "gamma_overlap2": 1.0,
                            "q_overlap2": 1.0,
                            "q_subspace_dimension": None,
                            "phi111": None,
                            "phi112": None,
                            "phi222": None,
                            "c10_ev": None,
                            "c01_ev": None,
                            "wide_phi122": None,
                            "wide_phi1122": None,
                            "energy_accumulation": "archived_original",
                            "grid_points": None,
                            "inference_seconds": None,
                            "grid_min_q1": None,
                            "grid_min_q2": None,
                        }
                    )
                else:
                    s = lookup[(mat, model, code)]
                    v = s["central"]
                    w = s["wide"]
                    ph = v["physics"]
                    old = next(
                        x
                        for x in old_rows
                        if x["material"] == mat
                        and x["model"] == model
                        and x["qe_pair"] == code
                        and x["geometry"] == "model_relaxed"
                    )
                    if (
                        len(s["points"]) != 81
                        or s["central"]["fit_design_rank"] != 13
                        or s["identity"]["calculator"]["energy_accumulation"]
                        != "atomic_float32_sum_float64_v2"
                    ):
                        raise ValueError("Incomplete or uncorrected ML grid")
                    grid = np.array(s["energy_grid_ev"])
                    iy, ix = np.unravel_index(np.argmin(grid), grid.shape)
                    pair = s["identity"]["pair"]
                    ev = s["identity"]["values"]
                    base.update(
                        {
                            "phi122": abs(ph["phi_122_mev_per_A3amu32"]),
                            "phi1122": ph["phi_1122_mev_per_A4amu2"],
                            "stage1_gamma_thz": pair["gamma_mode"]["freq_thz"],
                            "stage1_q_thz": pair["target_mode"]["freq_thz"],
                            "stage2_gamma_thz": ph["freq_mode1"].get("thz"),
                            "stage2_q_thz": ph["freq_mode2"].get("thz"),
                            "fit_center_rmse_ev": v["center_fit_rmse_ev_supercell"],
                            "fit_wide_rmse_ev": w["center_fit_rmse_ev_supercell"],
                            "gamma_overlap2": float(old["gamma_overlap_squared"]),
                            "q_overlap2": float(old["q_overlap_squared"]),
                            "q_subspace_dimension": int(old["q_subspace_dimension"]),
                            "phi111": ph["phi_111_mev_per_A3amu32"],
                            "phi112": ph["phi_112_mev_per_A3amu32"],
                            "phi222": ph["phi_222_mev_per_A3amu32"],
                            "c10_ev": ph["coefficients_ev"]["c10"],
                            "c01_ev": ph["coefficients_ev"]["c01"],
                            "wide_phi122": abs(w["physics"]["phi_122_mev_per_A3amu32"]),
                            "wide_phi1122": w["physics"]["phi_1122_mev_per_A4amu2"],
                            "energy_accumulation": s["identity"]["calculator"][
                                "energy_accumulation"
                            ],
                            "grid_points": 81,
                            "inference_seconds": sum(
                                v["seconds"] for v in s["points"].values()
                            ),
                            "grid_min_q1": ev[ix],
                            "grid_min_q2": ev[iy],
                        }
                    )
                base["phi122_error"] = base["phi122"] - refphi
                base["phi122_relative_error_percent"] = (
                    100 * base["phi122_error"] / refphi
                )
                base["phi1122_error"] = (
                    base["phi1122"] - ref4
                    if ref4 is not None and base["phi1122"] is not None
                    else None
                )
                records.append(base)
        for model in ("qe-ms", *MODELS):
            rr = [r for r in records if r["material"] == mat and r["model"] == model]
            m = metrics([r["qe_phi122"] for r in rr], [r["phi122"] for r in rr])
            q = [r for r in rr if r["phi1122_error"] is not None]
            summary[f"{mat}/{model}"] = {
                "third": m,
                "fourth": metrics(
                    [r["qe_phi1122"] for r in q], [r["phi1122"] for r in q]
                )
                if q
                else None,
                "stage1_frequency": metrics(
                    [r["qe_q_thz"] for r in rr], [r["stage1_q_thz"] for r in rr]
                ),
                "stage2_frequency": metrics(
                    [r["qe_q_thz"] for r in rr], [r["stage2_q_thz"] for r in rr]
                ),
                "rank_codes": [
                    r["qe_pair"] for r in sorted(rr, key=lambda v: -v["phi122"])
                ],
                "qe_rank_codes": [
                    r["qe_pair"] for r in sorted(rr, key=lambda v: -v["qe_phi122"])
                ],
            }
    csv_write(data / "model_dft_top5.csv", records)
    result = {
        "scope": "matched_DFT_top5_final_observable_comparison",
        "summary": summary,
        "references": references,
        "sources": sources,
        "new_mlff_grids": states,
        "total_new_mlff_points": sum(len(s["points"]) for s in states),
        "units": {"third": UNIT3, "fourth": UNIT4},
        "new_DFT_calculations": 0,
    }
    (data / "model_dft_top5.json").write_text(
        json.dumps(result, indent=2, allow_nan=False) + "\n"
    )
    plt.rcParams.update(
        {
            "font.size": 9,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "savefig.dpi": 175,
        }
    )
    # Thesis figures 3-1 and 3-4: identical pair axes and a visible DFT reference.
    fig, axs = plt.subplots(2, 1, figsize=(10, 7), layout="constrained")
    for ax, mat in zip(axs, ("mos2", "wse2")):
        codes = list(references[mat])
        x = np.arange(5)
        ax.bar(
            x - 0.32,
            [references[mat][c]["phi122"] for c in codes],
            0.16,
            color="#202c35",
            label="DFT recheck",
        )
        for k, model in enumerate(("qe-ms", *MODELS)):
            rr = [
                next(
                    r
                    for r in records
                    if r["material"] == mat
                    and r["model"] == model
                    and r["qe_pair"] == c
                )
                for c in codes
            ]
            ax.bar(
                x - 0.16 + k * 0.16,
                [r["phi122"] for r in rr],
                0.16,
                color=COLORS[model],
                label=LABELS[model],
            )
        ax.set(
            xticks=x,
            xticklabels=PAIR_NAMES,
            ylabel=UNIT3,
            title=mat.upper() + " | matched Gamma optical - finite q pairs",
        )
        ax.grid(axis="y", alpha=0.15)
    axs[0].legend(ncol=3, fontsize=8)
    fig.savefig(figdir / "dft_top5_couplings.png")
    plt.close(fig)
    fig, axs = plt.subplots(1, 2, figsize=(10, 3.5), layout="constrained")
    for ax, mat in zip(axs, ("mos2", "wse2")):
        for k, model in enumerate(("qe-ms", *MODELS)):
            m = summary[f"{mat}/{model}"]["third"]
            ax.bar(
                np.arange(3) - 0.27 + 0.18 * k,
                [m[x] for x in ("mae", "rmse", "maxae")],
                0.18,
                color=COLORS[model],
                label=LABELS[model],
            )
        ax.set(
            xticks=np.arange(3),
            xticklabels=["MAE", "RMSE", "MaxAE"],
            ylabel=UNIT3,
            title=mat.upper() + " | relative to DFT",
        )
    axs[1].legend(fontsize=7)
    fig.savefig(figdir / "dft_top5_errors.png")
    plt.close(fig)
    # Stage1 harmonic frequencies and Stage2 PES curvature are separate estimates.
    fig, axs = plt.subplots(2, 2, figsize=(10, 6.4), layout="constrained")
    for i, mat in enumerate(("mos2", "wse2")):
        codes = list(references[mat])
        ref = [references[mat][c]["q_frequency"] for c in codes]
        for j, field in enumerate(("stage1_q_thz", "stage2_q_thz")):
            ax = axs[i, j]
            ax.plot(PAIR_NAMES, ref, "ko--", label="QE harmonic")
            for model in MODELS if j == 0 else ("qe-ms", *MODELS):
                rr = [
                    next(
                        r
                        for r in records
                        if r["material"] == mat
                        and r["model"] == model
                        and r["qe_pair"] == c
                    )
                    for c in codes
                ]
                ax.plot(
                    PAIR_NAMES,
                    [r[field] for r in rr],
                    marker=".",
                    color=COLORS[model],
                    label=LABELS[model],
                )
            ax.set(
                title=mat.upper()
                + (" | Stage1 frequency" if j == 0 else " | Stage2 curvature"),
                ylabel="THz",
            )
    axs[0, 1].legend(fontsize=7)
    fig.savefig(figdir / "dft_top5_frequencies.png")
    plt.close(fig)
    # All four PES panels use their own energy at Q=0; no false pointwise DFT MAE.
    fig = plt.figure(figsize=(10, 6))
    mat = "wse2"
    code = list(references[mat])[0]
    qegrid = energies_to_ev(
        np.loadtxt(
            a.reference
            / "wse2_stage2_stage3_core_20260402/stage3_real_top1/energy_grid_ry.dat"
        ),
        "Ry",
    )
    panels = [("DFT recheck", qegrid)]
    gref = decode_complex_mode(
        references[mat][code]["pair"]["gamma_mode"]["eigenvector"]
    ).ravel()
    for model in MODELS:
        s = lookup[(mat, model, code)]
        grid = np.array(s["energy_grid_ev"])
        g = decode_complex_mode(
            s["identity"]["pair"]["gamma_mode"]["eigenvector"]
        ).ravel()
        if np.vdot(gref, g).real < 0:
            grid = grid[:, ::-1]
        panels.append((LABELS[model], grid))
    vmax = max(float(np.max(g - g[4, 4])) for _, g in panels)
    xx, yy = np.meshgrid(np.linspace(-2, 2, 9), np.linspace(-2, 2, 9))
    for i, (label, grid) in enumerate(panels):
        ax = fig.add_subplot(2, 2, i + 1, projection="3d")
        z = grid - grid[4, 4]
        ax.plot_surface(xx, yy, z, cmap="viridis", vmin=0, vmax=vmax, alpha=0.85)
        ax.scatter(xx, yy, z, s=3, c="k")
        ax.set(
            title=label,
            xlabel=r"$Q_\Gamma$",
            ylabel=r"$Q_q$",
            zlabel="E - E(0) [eV]",
            zlim=(min(0, float(z.min())), vmax),
        )
        ax.view_init(25, -60)
    fig.suptitle("WSe2 | Gamma8 - M9 | measured 9 x 9 frozen-mode grids")
    fig.tight_layout()
    fig.savefig(figdir / "dft_top5_pes.png")
    plt.close(fig)
    fig, axs = plt.subplots(1, 2, figsize=(10, 3.6), layout="constrained")
    for ax, mat in zip(axs, ("mos2", "wse2")):
        codes = list(references[mat])
        available = [
            (i, c)
            for i, c in enumerate(codes)
            if references[mat][c]["phi1122"] is not None
        ]
        ax.scatter(
            [i for i, _ in available],
            [references[mat][c]["phi1122"] for _, c in available],
            marker="D",
            c="k",
            s=45,
            label="DFT",
        )
        for model in MODELS:
            rr = [
                next(
                    r
                    for r in records
                    if r["material"] == mat
                    and r["model"] == model
                    and r["qe_pair"] == c
                )
                for c in codes
            ]
            ax.plot(
                range(5),
                [r["phi1122"] for r in rr],
                ".-",
                color=COLORS[model],
                label=LABELS[model],
            )
        ax.set(
            xticks=range(5),
            xticklabels=PAIR_NAMES,
            title=mat.upper() + f" | DFT coverage {len(available)}/5",
            ylabel=UNIT4,
        )
    axs[0].legend(fontsize=7)
    fig.savefig(figdir / "dft_top5_quartic.png")
    plt.close(fig)
    fig, axs = plt.subplots(2, 2, figsize=(10, 6), layout="constrained")
    for i, mat in enumerate(("mos2", "wse2")):
        for model in MODELS:
            rr = [r for r in records if r["material"] == mat and r["model"] == model]
            axs[i, 0].plot(
                PAIR_NAMES,
                [r["fit_center_rmse_ev"] * 1000 for r in rr],
                ".-",
                color=COLORS[model],
                label=LABELS[model],
            )
            axs[i, 1].plot(
                PAIR_NAMES,
                [r["fit_wide_rmse_ev"] * 1000 for r in rr],
                ".-",
                color=COLORS[model],
                label=LABELS[model],
            )
        axs[i, 0].set(
            title=mat.upper() + " | central fit residual", ylabel="meV / supercell"
        )
        axs[i, 1].set(
            title=mat.upper() + " | wide fit residual", ylabel="meV / supercell"
        )
    axs[0, 1].legend(fontsize=7)
    fig.savefig(figdir / "dft_top5_fit_residuals.png")
    plt.close(fig)
    # Pair mapping confidence, as distinct from frequency accuracy.
    fig, axs = plt.subplots(1, 2, figsize=(10, 3.4), layout="constrained")
    for ax, mat in zip(axs, ("mos2", "wse2")):
        for model in MODELS:
            rr = [r for r in records if r["material"] == mat and r["model"] == model]
            ax.plot(
                PAIR_NAMES,
                [r["q_overlap2"] for r in rr],
                ".-",
                color=COLORS[model],
                label=LABELS[model],
            )
        ax.set(
            ylim=(0.99, 1.0005),
            ylabel="Finite-q eigenvector overlap squared",
            title=mat.upper(),
        )
    axs[0].legend(fontsize=7)
    fig.savefig(figdir / "dft_top5_overlap.png")
    plt.close(fig)
    fig, axs = plt.subplots(2, 3, figsize=(11, 6), layout="constrained")
    for i, mat in enumerate(("mos2", "wse2")):
        for j, field in enumerate(("phi111", "phi112", "phi222")):
            ax = axs[i, j]
            for model in MODELS:
                rr = [
                    r for r in records if r["material"] == mat and r["model"] == model
                ]
                ax.plot(
                    PAIR_NAMES,
                    [abs(r[field]) for r in rr],
                    ".-",
                    color=COLORS[model],
                    label=LABELS[model],
                )
            for k, ref in enumerate(references[mat].values()):
                if "analysis" in ref["fit"]:
                    value = ref["fit"]["analysis"]["physics"][
                        "phi_" + field[3:] + "_mev_per_A3amu32"
                    ]
                    ax.scatter(k, abs(value), c="black", marker="D", s=25)
            ax.set(title=mat.upper() + " | " + field, ylabel=UNIT3)
    axs[0, 0].legend(fontsize=7)
    fig.savefig(figdir / "dft_top5_other_cubic.png")
    plt.close(fig)

    fig, axs = plt.subplots(2, 2, figsize=(10, 6), layout="constrained")
    for i, mat in enumerate(("mos2", "wse2")):
        for model in MODELS:
            rr = [r for r in records if r["material"] == mat and r["model"] == model]
            axs[i, 0].plot(
                PAIR_NAMES,
                [100 * (r["wide_phi122"] / r["phi122"] - 1) for r in rr],
                ".-",
                color=COLORS[model],
                label=LABELS[model],
            )
            axs[i, 1].plot(
                PAIR_NAMES,
                [r["wide_phi1122"] - r["phi1122"] for r in rr],
                ".-",
                color=COLORS[model],
                label=LABELS[model],
            )
        axs[i, 0].set(
            title=mat.upper() + " | cubic window sensitivity",
            ylabel="Wide / central - 1 [%]",
        )
        axs[i, 1].set(
            title=mat.upper() + " | quartic window sensitivity",
            ylabel="Wide - central [" + UNIT4 + "]",
        )
    axs[0, 0].legend(fontsize=7)
    fig.savefig(figdir / "dft_top5_windows.png")
    plt.close(fig)
    campaign = read(a.evidence / "campaign_audit_accepted.json")
    fig, axs = plt.subplots(2, 3, figsize=(11, 6.5), layout="constrained")
    distribution = {}
    for i, material in enumerate(("mose2", "ws2")):
        for j, model in enumerate(MODELS):
            root = a.accepted_runs / model / material
            pairs_file = root / "stage1/mode_pairs.selected.json"
            screen_file = root / "stage2/screen_ranking.json"
            payload, screen = read(pairs_file), read(screen_file)
            if screen["identity"]["mode_pairs_sha256"] != digest(pairs_file):
                raise ValueError("Screen distribution source mismatch")
            by_pair = {r["pair_code"]: r["phi122_proxy_mev"] for r in screen["pairs"]}
            channels = payload["equivalent_pair_channels"]["channels"]
            groups = {}
            scored = []
            for c in channels:
                strength = float(np.linalg.norm([by_pair[x] for x in c["pair_codes"]]))
                label = "Gamma " + "/".join(map(str, c["gamma_modes_one_based"]))
                groups.setdefault(label, []).append(strength)
                scored.append({**c, "six_point_norm": strength})
            ax = axs[i, j]
            for label, values in groups.items():
                values = sorted(values, reverse=True)
                ax.plot(
                    np.arange(1, len(values) + 1),
                    values,
                    ".-",
                    markersize=2,
                    label=label,
                )
            ax.set_yscale("symlog", linthresh=0.02)
            ax.set(
                title=material.upper() + " | " + LABELS[model],
                xlabel="Rank within Gamma group",
                ylabel="Six-point norm [" + UNIT3 + "]",
            )
            ax.legend(fontsize=6)
            distribution[model + "/" + material] = {
                "pairs_sha256": digest(pairs_file),
                "screen_sha256": digest(screen_file),
                "channels": scored,
            }
    fig.savefig(figdir / "model_channel_distributions.png")
    plt.close(fig)
    (data / "model_channel_distributions.json").write_text(
        json.dumps(distribution, indent=2, allow_nan=False) + "\n"
    )
    frequency_lines = [
        "| 材料／路线 | Stage1频率 MAE / RMSE (THz) | Stage2曲率频率 MAE / RMSE (THz) |",
        "|---|---:|---:|",
    ]
    for key, value in summary.items():
        first, second = value["stage1_frequency"], value["stage2_frequency"]
        frequency_lines.append(
            f"| {key} | {first['mae']:.3f} / {first['rmse']:.3f} | {second['mae']:.3f} / {second['rmse']:.3f} |"
        )
    cost_lines = [
        "| 材料／路线 | Stage1谐波段/s | Stage2独立点数 | Stage2累计推理/s | 相对全候选81点的点数减少 |",
        "|---|---:|---:|---:|---:|",
    ]
    harmonic = {
        "tece/mose2": 67.44,
        "tece/ws2": 67.45,
        "prophet/mose2": 99.19,
        "prophet/ws2": 114.15,
        "equiformer-v3/mose2": 47.31,
        "equiformer-v3/ws2": 48.69,
    }
    for key, value in campaign["runs"].items():
        cost_lines.append(
            f"| {key} | {harmonic[key]:.2f} | {value['all_unique_energy_points']} | {value['sum_point_compute_seconds']:.1f} | {100 * (1 - value['all_unique_energy_points'] / (value['candidate_pairs'] * 81)):.2f}% |"
        )
    # Preserve the main report and append the scientific report as its companion.
    lines = [
        "# 模型路线与 DFT 精算的定量比较",
        "",
        "本报告按本科论文第3、4章的比较顺序组织：同一声子对的耦合、误差、势能面、频率、拟合残差和其余展开项。科学参照是已有QE精算五对；TECE、Prophet、EquiformerV3各自弛豫并生成Phonopy模式，Stage2均为MatterSim。另列论文原有QE模式＋MatterSim历史基线。",
        "",
        "论文比较的重点是固定DFT结构和模式后的不同Stage2力场；本轮比较的重点则是换成MLFF自行弛豫及生成模式后的完整两阶段路线。因此既要看筛选顺序是否保留，也要检验绝对耦合和二阶曲率是否偏离DFT。GPTFF不再进入现行路线比较。",
        "",
        "主要结果：三条全MLFF路线在MoS₂五对上的三阶平均相对误差约14.2–14.5%，WSe₂约15.9–16.3%；论文式QE＋MatterSim历史基线分别约6.25%和1.45%。三条新路线的三阶均系统性偏低，路线间差别远小于它们共同相对DFT的偏差。筛选顺序较稳定不等于绝对耦合已达到论文原方案精度。",
        "",
        "## 1. 相同五对的三阶耦合：DFT 是参照标准",
        "",
        "这五对是此前筛选后经过DFT复核的固定集合，不代表已穷举得到DFT全候选前五。各材料均为Γ8与M9、M8、M7、K9、L8的对应方向，L=(1/3,2/3,0)；编号沿用QE参照，MLFF编号按本征矢匹配，不要求编号相同。",
        "",
        "![五对逐项对照](figures/dft_top5_couplings.png)",
        "",
        "![相对DFT误差](figures/dft_top5_errors.png)",
        "",
        "所有三阶数值均为 |Φ122|，单位 meV/(Å³·amu³ᐟ²)；四阶为 Φ1122，单位 meV/(Å⁴·amu²)。多项式E含c12 QΓ Qq²与c22 QΓ²Qq²时，Φ122=2×1000c12，Φ1122=4×1000c22。非自共轭旧实模的范数约1/√2，因此K/L的旧三阶数值换到单位范数坐标后约乘2；这不是耦合物理增强。原始数值与转换因子随JSON保留。",
        "",
        "| 材料／路线 | MAE | RMSE | 最大绝对误差 | 平均相对误差 | 五对内部Spearman |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for key, v in summary.items():
        m = v["third"]
        lines.append(
            f"| {key} | {m['mae']:.3f} | {m['rmse']:.3f} | {m['maxae']:.3f} | {m['mape_percent']:.2f}% | {m['spearman']:.2f} |"
        )
    lines += [
        "",
        "MAE/RMSE衡量最终耦合相对DFT的差异，拟合残差另列，二者不能互相代替。模型结构和模态均自行生成，因此误差包含几何、模态、势能预测及有限拟合窗口的共同影响。此处按用户要求比较匹配后的最终物理量，不要求跨模型位移逐点相同。",
        "",
        "MoS₂各路线在五对内部的Spearman为0.90：最强三对顺序保留，K9和L8互换；WSe₂为1.00。这里的排名由换算后的|Φ122|重新排序，没有误用归档中代表原筛选顺序的rank字段。只验证过这五对，不能据此宣称DFT全候选top5召回为100%。",
        "",
        "## 2. 逐对数值与模式对应",
        "",
        "| 材料／声子对 | DFT | QE＋MS历史 | TECE＋MS | Prophet＋MS | EquiformerV3＋MS |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for mat in ("mos2", "wse2"):
        for name, code in zip(PAIR_NAMES, references[mat]):
            vals = [
                next(
                    r["phi122"]
                    for r in records
                    if r["material"] == mat and r["model"] == m and r["qe_pair"] == code
                )
                for m in ("qe-ms", *MODELS)
            ]
            lines.append(
                f"| {mat}/Γ8–{name} | {references[mat][code]['phi122']:.3f} | "
                + " | ".join(f"{v:.3f}" for v in vals)
                + " |"
            )
    lines += [
        "",
        "![模式重叠](figures/dft_top5_overlap.png)",
        "",
        "模式映射使用质量加权复本征矢重叠，并核对q点和频率。Γ8在本集合为单模；部分有限q目标属于近简并组，CSV逐行标出组维数。这些单方向比较保留为论文式方向指标，不能据其细微差异宣布某个网络具有唯一的子空间物理优势。",
        "",
        "这五对的有限q单模重叠平方最低为MoS₂ 0.99449、WSe₂ 0.99639，但耦合幅值仍有上述系统偏差。这说明高本征矢重叠本身不足以保证高阶耦合精度，不能把完整流程的误差只归因于本征矢。相同声子对的DFT与MLFF使用相同最小相容超胞：M为2×2、K/L为3×3；6×6是Stage1的q网格。",
        "",
        "## 3. 二阶频率与势能面形状",
        "",
        "![二阶频率](figures/dft_top5_frequencies.png)",
        "",
        "左列是Stage1谐波频率；右列是Stage2势能面中心曲率。黑线均为对应QE谐波模态的频率。二者分别检验模式生成和势能预测，不能把右列与DFT的差异全部归因于Stage1。",
        "",
        *frequency_lines,
        "",
        "频率统计只取这五对的有限q模式，每对一个值。QE＋MS的Stage1零误差是直接使用QE模式的定义结果，不是独立预测。三条新路线的Stage2频率很接近，但本样本MoS₂有限q频率MAE仍约0.55–0.56 THz、WSe₂约0.41 THz；耦合排序保留不表示二阶曲率误差已消失。",
        "",
        "![WSe2势能面](figures/dft_top5_pes.png)",
        "",
        "势能面取WSe₂ Γ8–M9，沿各自匹配模式绘制9×9原始能量点，能量以各自原点为零。Γ相位按本征矢内积对齐；所有面使用相同纵轴。图可比较软硬程度、非对称性和平衡位置；这里没有把不同结构上的网格相减当作能量DFT MAE。",
        "",
        "## 4. 拟合残差、三阶余项与四阶系数",
        "",
        "![拟合残差](figures/dft_top5_fit_residuals.png)",
        "",
        "中心残差是在[-1,1]的25点上拟合并评估；宽窗口残差是在[-2,2]的81点上拟合并评估。低拟合RMSE说明多项式能描述该模型的局部能量，不能直接证明模型接近DFT。",
        "",
        "本次中心拟合残差为MoS₂ 0.0085–0.1246 meV/超胞、WSe₂ 0.0032–0.0284 meV/超胞，远小于对应耦合误差所表现的系统偏离；因此单纯提高多项式拟合优度不能解决相对DFT的偏差。",
        "",
        "![四阶对照](figures/dft_top5_quartic.png)",
        "",
        "![其他三阶项](figures/dft_top5_other_cubic.png)",
        "",
        "余项图使用幅值，避免任意模态正负号影响比较；黑色菱形为已恢复原始网格的DFT拟合值。Φ112是有限q下受动量禁止的项，非零值视为数值和拟合诊断，不能解释为新的耦合通道。",
        "",
        "![拟合窗口敏感性](figures/dft_top5_windows.png)",
        "",
        "从中心5×5改到完整9×9拟合，三阶幅值最大变化为MoS₂ 3.98%、WSe₂ 1.29%；四阶Φ1122最大绝对变化却为6.50和0.68。因此四阶比三阶更依赖拟合窗口。三阶误差较小不能自动推导四阶也已收敛。",
        "",
        "| 材料／路线 | 四阶DFT对数 | 四阶MAE | 四阶RMSE | 最大四阶误差 |",
        "|---|---:|---:|---:|---:|",
    ]
    for key, v in summary.items():
        m = v["fourth"]
        if m:
            lines.append(
                f"| {key} | {m['count']} | {m['mae']:.3f} | {m['rmse']:.3f} | {m['maxae']:.3f} |"
            )
    lines += [
        "",
        "WSe₂另外四对已有DFT三阶系数，但当前收集的原始能量网格不足以恢复四阶；图中保留缺失，不填零。WSe₂的一对四阶误差只描述该对。三阶余项Φ111、Φ112、Φ222、线性项c10/c01、宽窗口三/四阶系数及原始能量均在配套CSV/JSON。有限q的Φ112按动量守恒应为零，用于数值诊断；Φ222是否允许取决于3q是否为倒格矢。",
        "",
        "## 5. 全网格与更多材料：科学验证和软件验收分别解读",
        "",
        "MoS₂/WSe₂已有每模型36q×9支的频率与模态记录，详见验收报告全网格表。其全网格WSe₂参照是早期未施加ASR的导出，不能与本报告关键模式/PES归档混称为同一数据源；Γ声学支不进入筛选。",
        "",
        "![历史频率结构对照](figures/historical_frequency_geometry.png)",
        "",
        "MoSe₂/WS₂的六组自弛豫全量实验已完成324候选的六点筛选、前20完整通道精算及最强通道窗口检查；模型间三/四阶和效率见验收报告。这些新增材料的模型间一致性不能替代同材料的DFT精算标准。",
        "",
        "![更多材料模型间比较](figures/matched_couplings.png)",
        "",
        "![全候选Gamma分组强度分布](figures/model_channel_distributions.png)",
        "",
        "与论文Γ分支耦合分布图对应，上图使用新增两种材料的全部六点筛选结果。每条曲线在同一Γ组内按强度排序，Γ近简并多重态合并完整分量的范数；有限q简并单支仍是基底相关方向。各图的Γ编号是该模型自己的编号，跨图的物理同一性以已保存的模态映射为准。纵轴近零区采用线性尺度、较强耦合区采用对数尺度。这些是粗筛代理值，不充当DFT精算系数。",
        "",
        "强通道整体分布相近：MoSe₂ Γ6组最大粗筛范数为34.13–34.21，WS₂ Γ8组为86.04–87.00。但弱通道差别更明显：EquiformerV3的Γ4/5组最大值在MoSe₂/WS₂为1.83/3.93，TECE与Prophet均不超过0.074。不能只凭最强通道相近就认定三个网络全范围一致；弱通道差别也不能在没有DFT标签时直接解释为真实非线性增强。",
        "",
        "粗筛评价同时保留成本和召回：此前六组MoS₂/WSe₂归档回放中，六点前20对中心精算前20召回为19/20或20/20，前30为20/20。这是对MLFF精算的召回，绝不称为DFT全候选召回。6点全量＋前20完整通道5×5＋强通道9×9保留为默认；用户可把前20提高到前30。",
        "",
        "## 6. 计算成本与粗筛收益",
        "",
        *cost_lines,
        "",
        "上表Stage1是程序记录的声子计算段，不含弛豫和模型加载；Stage2是所有工作进程累计推理时间，不是墙钟，两列不可直接相加。正式Stage1作业历史墙钟：TECE两材料均97s，Prophet141/155s，EquiformerV3均60s；具体数值随环境与节点负载变化。",
        "",
        "同节点108原子、16线程：TECE／Prophet热推理1.871／2.862s（相差1.53倍），加载14.66／12.82s，进程峰值内存约5805／6083MB。EquiformerV3缺少同条件微基准，不作单次推理排名。当前Stage1与Stage2均需优化。",
        "",
        "## 7. 数据来源与复现",
        "",
        f"本次新增的仅为已匹配声子方向的MatterSim复算：30对×81点＝{sum(len(s['points']) for s in states)}点，使用修正后的逐原子能量float64累加。Stage1沿用已存自行弛豫结构及模态，结构哈希逐一核验。没有新增DFT、弛豫或MD。",
        "",
        "历史QE＋MS基线按原有拟合结果换算单位，未伪称为本次同环境重跑。全量来源哈希、旧归一化转换、每对两档拟合及Slurm作业号保存在[JSON](acceptance_data/model_dft_top5.json)，逐对数值与误差见[CSV](acceptance_data/model_dft_top5.csv)。[软件验收与性能报告](ACCEPTANCE.md)保留全部既有验收信息。",
        "",
        "| 数据集合 | 材料 | 本报告覆盖 |",
        "|---|---|---|",
        "| DFT精算三阶参照 | MoS₂、WSe₂ | 每材料5对；固定既有集合 |",
        "| DFT原始网格重拟合四阶 | MoS₂、WSe₂ | 分别5对、1对；缺失不填补 |",
        "| 累加修正后MLFF复算 | 两材料×三Stage1模型 | 每组5对×81点；中心25点拟合 |",
        "| 全网格频率／模态 | MoS₂、WSe₂ | 每模型36q×9支；区分结构线 |",
        "| 新材料粗筛／精算 | MoSe₂、WS₂ | 六组各324候选；前20完整通道精算 |",
        "",
        "专项复算作业1019616仅占1个CPU节点、申请8 CPU和16GB，两个工作进程各4线程，作业墙钟95s。30对检查均为81个有限能量值，两档拟合均13列满秩，两个实模质量范数均为1；对40条最终比较记录独立复算误差统计。申请内存不是峰值实测内存。",
        "",
        "本报告的精度指标是匹配声子对最终物理量相对DFT的偏差，不是不同结构能量逐点相减；拟合残差是对各自能量数据的拟合误差。所有排名只在明确标出的集合内定义；近简并单方向结果保留基底相关标记。",
        "",
        "复现工具：validation/refine_reference_pairs.py负责已有匹配模式的MLFF点级续算；validation/build_model_dft_report.py负责读取归档、计算指标和绘图；validation/render_report_pdf.py生成PDF。它们是测试／报告辅助程序，不含Stage3提交或DFT重算入口。",
    ]
    (out / "MODEL_DFT_COMPARISON.md").write_text("\n".join(lines) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
