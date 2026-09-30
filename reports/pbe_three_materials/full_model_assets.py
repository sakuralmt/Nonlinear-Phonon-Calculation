"""Recompute full-model benchmark metrics from raw matched energy/force grids."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))
from mlff_modepair_workflow.units import UNITS
from mlff_modepair_workflow.core import infer_commensurate_supercell_n
from mlff_modepair_workflow.advanced_stage1 import MODEL_SOURCES
from mlff_modepair_workflow.prophet_backend import OAME_MBD_SHA256
from reports.pbe_three_materials.generate_assets import (
    MATS,
    MODELS,
    MODEL_LABEL,
    COLORS,
    LABEL,
    PLOT_LABEL,
    THIRD,
    FOURTH,
    read,
    sha,
    save_table,
    save_fig,
    fnum,
    plt,
)

DATA = REPO / "docs/reference_data/pbe_20260925"


def independent_fit(axis, grid, extent):
    x, y = np.meshgrid(axis, axis)
    mask = (abs(x) <= extent) & (abs(y) <= extent)
    x, y = x[mask], y[mask]
    z = np.asarray(grid)[mask]
    z = z - z.min()
    design = np.column_stack(
        [
            x * x,
            y * y,
            x * y * y,
            x * x * y,
            x**3,
            y**3,
            x * y,
            x**4,
            y**4,
            x * x * y * y,
            x,
            y,
            np.ones_like(x),
        ]
    )
    coef, _, rank, _ = np.linalg.lstsq(design, z, rcond=None)
    if rank != 13:
        raise ValueError("Independent fit not full rank")
    return {
        "phi122_abs": abs(float(2000 * coef[2])),
        "phi1122": float(4000 * coef[9]),
        "residual_rmse_mev": float(1000 * np.sqrt(np.mean((design @ coef - z) ** 2))),
    }


def error_metrics(pred, ref):
    delta = np.asarray(pred) - np.asarray(ref)
    return {
        "count": len(delta),
        "mae": float(np.mean(abs(delta))),
        "rmse": float(np.sqrt(np.mean(delta**2))),
        "bias": float(np.mean(delta)),
        "maxae": float(np.max(abs(delta))),
    }


def validate(materials=MATS, models=MODELS):
    base = read(DATA / "coupling_comparison.json")
    summary = {
        "units": UNITS,
        "scope": "15 matched channels, own-relaxed full flows and separate identical-PBE-input diagnostics",
        "materials": {},
    }
    for material in materials:
        refs = {r["pbe_pair_code"]: r for r in base["materials"][material]["rows"]}
        qe = {
            r["pbe_pair_code"]: r
            for r in read(DATA / f"results_{material}.json")["rows"]
        }
        summary["materials"][material] = {"models": {}}
        for model in models:
            path = DATA / f"full_model_{material}_{model}.json"
            result = read(path)
            manifest_path = DATA / "full_model_manifests" / f"{material}-{model}.json"
            if result["identity"]["manifest_sha256"] != sha(manifest_path):
                raise ValueError("Benchmark manifest changed")
            manifest = read(manifest_path)
            tasks = {t["task_id"]: t for t in manifest["tasks"]}
            if (
                len(tasks) != 10
                or len(result["rows"]) != 10
                or len({r["task_id"] for r in result["rows"]}) != 10
            ):
                raise ValueError("Repeated benchmark task")

            expected_sha = (
                OAME_MBD_SHA256
                if model == "prophet"
                else MODEL_SOURCES[
                    {"tece": "tece-oam-rra-1.0", "equiformer-v3": "equiformer-v3-oam"}[
                        model
                    ]
                ]["checkpoint_sha256"]
            )
            if (
                result["units"] != UNITS
                or result["identity"]["checkpoint_sha256"] != expected_sha
                or result["complete_points"] != 586
                or result["complete_tasks"] != 10
            ):
                raise ValueError("Incomplete or wrong-model benchmark")
            byscope = {
                scope: []
                for scope in ["own_relaxed_full_flow", "fixed_pbe_dft_diagnostic"]
            }
            for row in result["rows"]:
                original = tasks[row["task_id"]]
                if any(row[k] != original[k] for k in original):
                    raise ValueError("Benchmark task differs from input")
                structure_file = DATA / (
                    f"full_model_structures/{material}-{model}.scf.inp"
                    if row["scope"] == "own_relaxed_full_flow"
                    else f"structures/{material}.scf.inp"
                )
                if sha(structure_file) != row["structure_sha256"]:
                    raise ValueError("Benchmark structure provenance mismatch")
                axis = np.asarray(row["axis"])
                grid = np.asarray(row["energy_grid_ev"])
                force = np.asarray(row["force_grid_ev_per_A"])
                n = infer_commensurate_supercell_n(row["pair"]["target_mode"]["q_frac"])
                nat = len(row["pair"]["gamma_mode"]["eigenvector"]) * n**2
                if grid.shape != (len(axis), len(axis)) or force.shape != (
                    len(axis),
                    len(axis),
                    nat,
                    3,
                ):
                    raise ValueError("Wrong grid shape")
                if (
                    not np.isfinite(grid).all()
                    or not np.isfinite(force).all()
                    or row["complete_points"] != grid.size
                    or not np.allclose(row["mass_norms"], 1, atol=1e-10)
                ):
                    raise ValueError("Invalid raw benchmark grid")
                fit = independent_fit(axis, grid, 1)
                for key, rawkey in [("phi122_abs", THIRD), ("phi1122", FOURTH)]:
                    stored = row["central"]["physics"][rawkey]
                    stored = abs(stored) if key == "phi122_abs" else stored
                    if not np.isclose(fit[key], stored, atol=2e-8, rtol=0):
                        raise ValueError("Independent derivative mismatch")
                item = {
                    "physical_channel": row["physical_channel"],
                    "qe_pair": row["qe_pair"],
                    "rank": row["rank"],
                    **fit,
                    "stage1_gamma_frequency_thz": row["pair"]["gamma_mode"]["freq_thz"],
                    "stage1_q_frequency_thz": row["pair"]["target_mode"]["freq_thz"],
                    "stage2_gamma_frequency": row["central"]["physics"]["freq_mode1"],
                    "stage2_q_frequency": row["central"]["physics"]["freq_mode2"],
                    "points": grid.size,
                    "inference_seconds": row["inference_seconds"],
                }
                if len(axis) == 9:
                    item["wide"] = independent_fit(axis, grid, 2)
                if row["scope"] == "fixed_pbe_dft_diagnostic":
                    origin = list(axis).index(0)
                    pointmap = {
                        (p["q_gamma"], p["q_finite"]): p
                        for p in qe[row["qe_pair"]]["source_points"]
                    }
                    if len(pointmap) != grid.size:
                        raise ValueError("QE and model grids differ")
                    qegrid = np.asarray(
                        [[pointmap[x, y]["energy_ev"] for x in axis] for y in axis]
                    )
                    qeforce = np.asarray(
                        [
                            [pointmap[x, y]["forces_ev_per_A"] for x in axis]
                            for y in axis
                        ]
                    )
                    delta = 1000 * (
                        (grid - grid[origin, origin])
                        - (qegrid - qegrid[origin, origin])
                    )
                    df = force - qeforce
                    center = abs(axis) <= 1
                    cmask = np.ix_(center, center)
                    item["errors"] = {
                        "energy_all": error_metrics(
                            delta.ravel(), np.zeros(delta.size)
                        ),
                        "energy_center": error_metrics(
                            delta[cmask].ravel(), np.zeros(delta[cmask].size)
                        ),
                        "force_all": error_metrics(df.ravel(), np.zeros(df.size)),
                        "force_center": error_metrics(
                            df[cmask].ravel(), np.zeros(df[cmask].size)
                        ),
                    }
                    item["even_mixed_mev"] = {}
                    for a in [0.5, 1.0]:
                        indices = [list(axis).index(-a), list(axis).index(a)]

                        def mixed(g):
                            return float(
                                1000
                                * (
                                    g[np.ix_(indices, indices)].mean()
                                    - g[indices, origin].mean()
                                    - g[origin, indices].mean()
                                    + g[origin, origin]
                                )
                            )

                        item["even_mixed_mev"][str(a)] = {
                            "qe": mixed(qegrid),
                            "model": mixed(grid),
                        }
                byscope[row["scope"]].append(item)
            item = {
                "result_sha256": sha(path),
                "job_id": result["slurm_job_id"],
                "wall_seconds": result["wall_seconds"],
                "loading_seconds": result["loading_seconds"],
                "resources": result["resources"],
                "scopes": {},
            }
            for scope, rows in byscope.items():
                rows.sort(key=lambda x: x["rank"])
                if len(rows) != 5 or len({r["qe_pair"] for r in rows}) != 5:
                    raise ValueError("Missing/repeated matched channel")
                metrics = {}
                for target in ("pbe", "lda"):
                    metrics[target] = {}
                    for key in ("phi122_abs", "phi1122"):
                        available = [
                            r
                            for r in rows
                            if refs[r["qe_pair"]][target][key] is not None
                        ]
                        if available:
                            metrics[target][key] = error_metrics(
                                [r[key] for r in available],
                                [refs[r["qe_pair"]][target][key] for r in available],
                            )
                metrics["rank_within_selected_set"] = float(
                    spearmanr(
                        [r["phi122_abs"] for r in rows],
                        [refs[r["qe_pair"]]["pbe"]["phi122_abs"] for r in rows],
                    ).statistic
                )
                item["scopes"][scope] = {"rows": rows, "metrics": metrics}
            summary["materials"][material]["models"][model] = item
    return summary


def main():
    summary = validate()
    (DATA / "full_model_comparison.json").write_text(
        json.dumps(summary, indent=2, allow_nan=False) + "\n"
    )
    rows = []
    for m in MATS:
        for model in MODELS:
            item = summary["materials"][m]["models"][model]
            own = item["scopes"]["own_relaxed_full_flow"]["metrics"]["pbe"]
            fixed = item["scopes"]["fixed_pbe_dft_diagnostic"]["metrics"]["pbe"]
            rows.append(
                [
                    LABEL[m],
                    model,
                    *[
                        fnum(v[k]["mae"])
                        for v in [own, fixed]
                        for k in ["phi122_abs", "phi1122"]
                    ],
                    fnum(item["wall_seconds"], 1),
                ]
            )
    save_table(
        "full_model_metrics",
        [
            "材料",
            "Stage2",
            "全流程三阶MAE",
            "全流程四阶MAE",
            "DFT基三阶MAE",
            "DFT基四阶MAE",
            "墙钟(s)",
        ],
        rows,
    )
    fig, axes = plt.subplots(2, 3, figsize=(11, 6), layout="constrained")
    old = read(DATA / "coupling_comparison.json")
    for j, m in enumerate(MATS):
        for i, key in enumerate(["phi122_abs", "phi1122"]):
            ax = axes[i, j]
            x = np.arange(3)
            ax.bar(
                x - 0.18,
                [
                    old["materials"][m]["metrics"]["pbe"][model][key]["mae"]
                    for model in MODELS
                ],
                0.35,
                label="Same Stage1 + MatterSim",
                color="#b8c5cf",
            )
            ax.bar(
                x + 0.18,
                [
                    summary["materials"][m]["models"][model]["scopes"][
                        "own_relaxed_full_flow"
                    ]["metrics"]["pbe"][key]["mae"]
                    for model in MODELS
                ],
                0.35,
                label="Same Stage1 + same model",
                color="#3176a0",
            )
            ax.set_xticks(x, ["TECE", "Prophet", "E3"])
            ax.set_title(PLOT_LABEL[m] + (" / cubic" if i == 0 else " / quartic"))
            ax.set_ylabel("MAE vs PBE")
            ax.grid(axis="y", alpha=0.2)
    axes[0, 0].legend(frameon=False, fontsize=8)
    save_fig(fig, "full_model_metrics")
    ef_rows = []

    def combined(rows, key):
        entries = [r["errors"][key] for r in rows]
        n = sum(e["count"] for e in entries)
        return [
            sum(e["mae"] * e["count"] for e in entries) / n,
            np.sqrt(sum(e["rmse"] ** 2 * e["count"] for e in entries) / n),
        ]

    diag, axes = plt.subplots(3, 3, figsize=(11, 8), layout="constrained")
    diagnostic_rows = []
    for j, m in enumerate(MATS):
        selected = read(DATA / f"selection_{m}.json")["selected"]
        qref = {r["pair"]["pair_code"]: r["pair"] for r in selected}
        for k, model in enumerate(MODELS):
            item = summary["materials"][m]["models"][model]
            own = item["scopes"]["own_relaxed_full_flow"]["rows"]
            fixed = item["scopes"]["fixed_pbe_dft_diagnostic"]["rows"]
            ef_rows.append(
                [
                    LABEL[m],
                    model,
                    *[
                        fnum(v, 4)
                        for key in [
                            "energy_all",
                            "energy_center",
                            "force_all",
                            "force_center",
                        ]
                        for v in combined(fixed, key)
                    ],
                ]
            )
            fp = []
            fr = []
            for r in own:
                for field, mode in [
                    ("stage2_gamma_frequency", "gamma_mode"),
                    ("stage2_q_frequency", "target_mode"),
                ]:
                    if r[field]["stable"]:
                        fp.append(r[field]["thz"])
                        fr.append(qref[r["qe_pair"]][mode]["freq_thz"])
            fm = error_metrics(fp, fr)
            rmse = np.mean([r["residual_rmse_mev"] for r in own])
            first = own[0]
            change = 100 * (first["wide"]["phi1122"] / first["phi1122"] - 1)
            diagnostic_rows.append(
                [
                    LABEL[m],
                    model,
                    fnum(fm["mae"], 4),
                    fnum(rmse, 4),
                    fnum(first["phi1122"]),
                    fnum(first["wide"]["phi1122"]),
                    fnum(change, 2),
                ]
            )
            for i, val in enumerate([fm["mae"], rmse, change]):
                axes[i, j].bar(k, val, color=["#b6602a", "#7762a1", "#34866a"][k])
        for i in range(3):
            axes[i, j].set_xticks(range(3), ["TECE", "Prophet", "E3"])
            axes[i, j].set_title(PLOT_LABEL[m])
            axes[i, j].grid(axis="y", alpha=0.2)
            axes[i, j].set_ylabel(
                [
                    "Stage2 frequency MAE (THz)",
                    "Mean center fit RMSE (meV)",
                    "Leading quartic window change (%)",
                ][i]
            )
    save_fig(diag, "full_model_diagnostics")
    save_table(
        "full_model_diagnostics",
        [
            "材料",
            "路线",
            "曲率频率MAE",
            "拟合RMSE(meV)",
            "中心四阶",
            "宽窗四阶",
            r"变化(\%)",
        ],
        diagnostic_rows,
    )
    for m in MATS:
        result = read(DATA / f"pbe_dft_mattersim_{m}.json")
        ef = []
        for prefix in ("", "central_"):
            for kind in ("relative_energy", "force"):
                mae_key = prefix + (
                    "relative_energy_mae_mev"
                    if kind == "relative_energy"
                    else "force_mae_ev_per_A"
                )
                rmse_key = prefix + (
                    "relative_energy_rmse_mev"
                    if kind == "relative_energy"
                    else "force_rmse_ev_per_A"
                )
                entries = []
                for r in result["rows"]:
                    n = 25 if prefix else r["points"]
                    if kind == "force":
                        n *= len(r["force_grid_ev_per_A"][0][0]) * 3
                    entries.append((n, r[mae_key], r[rmse_key]))
                n = sum(x[0] for x in entries)
                ef.append(
                    [
                        sum(x[0] * x[1] for x in entries) / n,
                        np.sqrt(sum(x[0] * x[2] ** 2 for x in entries) / n),
                    ]
                )
        values = ef[0] + ef[2] + ef[1] + ef[3]
        ef_rows.append([LABEL[m], "MatterSim", *[fnum(v, 4) for v in values]])
    save_table(
        "full_model_ef",
        [
            "材料",
            "Stage2",
            "E MAE",
            "E RMSE",
            "中心E MAE",
            "中心E RMSE",
            "F MAE",
            "F RMSE",
            "中心F MAE",
            "中心F RMSE",
        ],
        ef_rows,
    )
    fig, ax = plt.subplots(figsize=(8, 3.5), layout="constrained")
    for i, model in enumerate(MODELS):
        rows = summary["materials"]["ws2"]["models"][model]["scopes"][
            "fixed_pbe_dft_diagnostic"
        ]["rows"]
        m6 = next(r for r in rows if r["physical_channel"] == "Gamma8-M6")
        values = [m6["even_mixed_mev"][str(a)]["model"] for a in [0.5, 1.0]]
        ax.bar(np.arange(2) + (i - 1.5) * 0.16, values, 0.15, label=model)
    ms = read(DATA / "pbe_dft_mattersim_ws2.json")
    r = next(r for r in ms["rows"] if r["pair_code"].endswith("m6"))
    grid = np.asarray(r["grid_ev"])
    axis = r["axis"]
    o = axis.index(0.0)
    vals = []
    for a in [0.5, 1.0]:
        ix = [axis.index(-a), axis.index(a)]
        vals.append(
            1000
            * (
                grid[np.ix_(ix, ix)].mean()
                - grid[ix, o].mean()
                - grid[o, ix].mean()
                + grid[o, o]
            )
        )
    ax.bar(np.arange(2) + 2.5 * 0.16, vals, 0.15, label="MatterSim", color="#cc7733")
    values = [m6["even_mixed_mev"][str(a)]["qe"] for a in [0.5, 1.0]]
    ax.bar(np.arange(2) + 1.5 * 0.16, values, 0.15, label="QE PBE", color="#253b4a")
    ax.axhline(0, color="black", lw=0.6)
    ax.set_xticks(range(2), ["Q=0.5", "Q=1.0"])
    ax.set_ylabel("Raw even-mixed energy (meV/supercell)")
    ax.legend(frameon=False)
    save_fig(fig, "full_model_m6")
    inputs = [DATA / f"full_model_{m}_{model}.json" for m in MATS for model in MODELS]
    inputs.extend(
        DATA / name
        for name in [
            "full_model_comparison.json",
            "full_model_preflight.json",
            "full_model_resource_audit.json",
            "coupling_comparison.json",
            "pbe_dft_mattersim_comparison.json",
        ]
    )
    inputs.extend(DATA / f"results_{m}.json" for m in MATS)
    for folder in ["full_model_manifests", "full_model_structures", "structures"]:
        inputs.extend(p for p in (DATA / folder).glob("*") if p.is_file())
    inputs.extend(
        [REPO / "mlff_modepair_workflow/full_model_benchmark.py", Path(__file__)]
    )
    (HERE / "full_model_sources.json").write_text(
        json.dumps({str(p.relative_to(REPO)): sha(p) for p in inputs}, indent=2) + "\n"
    )
    print(
        json.dumps(
            {
                m: {
                    model: v["scopes"]["own_relaxed_full_flow"]["metrics"]["pbe"]
                    for model, v in d["models"].items()
                }
                for m, d in summary["materials"].items()
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
