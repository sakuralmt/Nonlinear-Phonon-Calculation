"""Independent same-PBE-input validation of shortlisted small Stage2 models."""

from __future__ import annotations
import json
import sys
from pathlib import Path
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from reports.pbe_three_materials.full_model_assets import independent_fit, error_metrics
from reports.pbe_three_materials.generate_assets import (
    MATS,
    LABEL,
    THIRD,
    FOURTH,
    read,
    sha,
    save_table,
    save_fig,
    plt,
)
from mlff_modepair_workflow.small_model_benchmark import SPECS
from mlff_modepair_workflow.units import UNITS
from mlff_modepair_workflow.core import infer_commensurate_supercell_n

REPO = Path(__file__).resolve().parents[2]
DATA = REPO / "docs/reference_data/pbe_20260925"
MODELS = list(SPECS)
NAMES = {
    "grace-1l-oam": "GRACE-1L",
    "eqnorm-mptrj": "Eqnorm",
    "dpa-3.1-3m-ft": "DPA-3.1-FT",
    "mattersim": "MatterSim",
    "tece": "TECE",
}


def validate():
    summary = {
        "units": UNITS,
        "scope": "15 fixed PBE DFT channels; no new Stage1 or DFT calculations",
        "materials": {},
    }
    for mat in MATS:
        refs = {
            r["pbe_pair_code"]: r for r in read(DATA / f"results_{mat}.json")["rows"]
        }
        couplings = {
            r["pbe_pair_code"]: r["pbe"]
            for r in read(DATA / "coupling_comparison.json")["materials"][mat]["rows"]
        }
        summary["materials"][mat] = {"models": {}}
        for model in MODELS:
            path = DATA / f"small_model_{mat}_{model}.json"
            result = read(path)
            mp = DATA / "small_model_manifests" / f"{mat}-{model}.json"
            manifest = read(mp)
            if (
                result["identity"]["manifest_sha256"] != sha(mp)
                or result["identity"]["checkpoint_sha256"] != SPECS[model][2]
                or result["units"] != UNITS
                or result["complete_points"] != 181
                or len(result["rows"]) != 5
            ):
                raise ValueError("Small model provenance/completeness mismatch")
            if result["provenance"]["package_version"] != SPECS[model][1]:
                raise ValueError("Package mismatch")
            if result["external_adapter_sha256"] != manifest["external_adapter_sha256"]:
                raise ValueError("Adapter provenance mismatch")
            accepted = {
                r["qe_pair"]: r
                for r in read(DATA / f"full_model_{mat}_tece.json")["rows"]
                if r["scope"] == "fixed_pbe_dft_diagnostic"
            }
            originals = {t["task_id"]: t for t in manifest["tasks"]}
            if len(originals) != 5 or len({r["task_id"] for r in result["rows"]}) != 5:
                raise ValueError("Duplicate tasks")
            rows = []
            energy = []
            forces = []
            center_e = []
            center_f = []
            for row in result["rows"]:
                if any(
                    row[k] != originals[row["task_id"]][k]
                    for k in originals[row["task_id"]]
                ):
                    raise ValueError("Task mismatch")
                if sha(DATA / f"structures/{mat}.scf.inp") != row["structure_sha256"]:
                    raise ValueError("Structure mismatch")
                if not np.allclose(row["mass_norms"], 1, atol=1e-10):
                    raise ValueError("Normalization mismatch")
                if (
                    row["pair"] != accepted[row["qe_pair"]]["pair"]
                    or row["axis"] != accepted[row["qe_pair"]]["axis"]
                ):
                    raise ValueError(
                        "Small-model configurations differ from audited PBE benchmark"
                    )
                axis = np.array(row["axis"])
                grid = np.array(row["energy_grid_ev"])
                force = np.array(row["force_grid_ev_per_A"])
                origin = list(axis).index(0)
                n = infer_commensurate_supercell_n(row["pair"]["target_mode"]["q_frac"])
                nat = len(row["pair"]["gamma_mode"]["eigenvector"]) * n * n
                if (
                    grid.shape != (len(axis), len(axis))
                    or force.shape != (len(axis), len(axis), nat, 3)
                    or not np.isfinite(grid).all()
                    or not np.isfinite(force).all()
                ):
                    raise ValueError("Invalid grid")
                fit = independent_fit(axis, grid, 1)
                if not np.isclose(
                    fit["phi1122"], row["central"]["physics"][FOURTH], atol=2e-8, rtol=0
                ) or not np.isclose(
                    fit["phi122_abs"],
                    abs(row["central"]["physics"][THIRD]),
                    atol=2e-8,
                    rtol=0,
                ):
                    raise ValueError("Derivative mismatch")
                pointmap = {
                    (p["q_gamma"], p["q_finite"]): p
                    for p in refs[row["qe_pair"]]["source_points"]
                }
                if len(pointmap) != grid.size:
                    raise ValueError("QE grid mismatch")
                qe = np.array(
                    [[pointmap[x, y]["energy_ev"] for x in axis] for y in axis]
                )
                qef = np.array(
                    [[pointmap[x, y]["forces_ev_per_A"] for x in axis] for y in axis]
                )
                de = 1000 * ((grid - grid[origin, origin]) - (qe - qe[origin, origin]))
                df = force - qef
                mask = np.ix_(abs(axis) <= 1, abs(axis) <= 1)
                energy.extend(de.ravel())
                forces.extend(df.ravel())
                center_e.extend(de[mask].ravel())
                center_f.extend(df[mask].ravel())
                item = {
                    "channel": row["physical_channel"],
                    "qe_pair": row["qe_pair"],
                    **fit,
                    "points": grid.size,
                    "energy_mae_mev": float(np.mean(abs(de))),
                    "force_mae_ev_per_A": float(np.mean(abs(df))),
                    "even_mixed_mev": {},
                }
                for a in [0.5, 1]:
                    ids = [list(axis).index(-a), list(axis).index(a)]

                    def mixed(g):
                        return float(
                            1000
                            * (
                                g[np.ix_(ids, ids)].mean()
                                - g[ids, origin].mean()
                                - g[origin, ids].mean()
                                + g[origin, origin]
                            )
                        )

                    item["even_mixed_mev"][str(a)] = {
                        "qe": mixed(qe),
                        "model": mixed(grid),
                    }
                if len(axis) == 9:
                    item["wide"] = independent_fit(axis, grid, 2)
                rows.append(item)
            metrics = {
                key: error_metrics(
                    [r[key] for r in rows], [couplings[r["qe_pair"]][key] for r in rows]
                )
                for key in ["phi122_abs", "phi1122"]
            }
            for key, v in [
                ("energy_all_mev", energy),
                ("force_all_ev_per_A", forces),
                ("energy_center_mev", center_e),
                ("force_center_ev_per_A", center_f),
            ]:
                metrics[key] = error_metrics(v, np.zeros(len(v)))
            metrics["quartic_sign_matches"] = int(
                sum(
                    np.sign(r["phi1122"]) == np.sign(couplings[r["qe_pair"]]["phi1122"])
                    for r in rows
                )
            )
            summary["materials"][mat]["models"][model] = {
                "rows": rows,
                "metrics": metrics,
                "result_sha256": sha(path),
                "wall_seconds": result["total_wall_seconds_including_load"],
                "cpu_threads": result["cpu_threads"],
                "slurm_job_id": result["slurm_job_id"],
                "resources": result["resources"],
            }
    return summary


def main():
    d = validate()
    (DATA / "small_model_comparison.json").write_text(
        json.dumps(d, indent=2, allow_nan=False) + "\n"
    )
    tables = []
    details = []
    fig, axes = plt.subplots(2, 3, figsize=(11, 6), layout="constrained")
    for j, mat in enumerate(MATS):
        all_models = ["mattersim", "tece"] + MODELS
        baseline = read(DATA / "full_model_comparison.json")["materials"][mat][
            "models"
        ]["tece"]["scopes"]["fixed_pbe_dft_diagnostic"]
        ms = read(DATA / "pbe_dft_mattersim_comparison.json")["materials"][mat]
        # Existing audited MatterSim metrics use the same fixed PBE configurations.
        metrics = {
            model: d["materials"][mat]["models"][model]["metrics"] for model in MODELS
        }
        metrics["tece"] = baseline["metrics"]["pbe"]
        # Derive MatterSim coefficient metrics directly from the old summary rows.
        old = read(DATA / f"pbe_dft_mattersim_{mat}.json")
        refs = {
            r["pbe_pair_code"]: r["pbe"]
            for r in read(DATA / "coupling_comparison.json")["materials"][mat]["rows"]
        }
        metrics["mattersim"] = {
            key: error_metrics(
                [
                    (
                        abs(r["center_fit"]["physics"][THIRD])
                        if key == "phi122_abs"
                        else r["center_fit"]["physics"][FOURTH]
                    )
                    for r in old["rows"]
                ],
                [refs[r["pair_code"]][key] for r in old["rows"]],
            )
            for key in ["phi122_abs", "phi1122"]
        }
        for i, key in enumerate(["phi122_abs", "phi1122"]):
            ax = axes[i, j]
            ax.bar(
                [NAMES[x] for x in all_models],
                [metrics[x][key]["mae"] for x in all_models],
                color=["#8b8b8b", "#228833", "#4477aa", "#ee6677", "#aa3377"],
            )
            ax.set_title(LABEL[mat])
            ax.set_ylabel(("Cubic" if i == 0 else "Signed quartic") + " MAE")
            ax.tick_params(axis="x", rotation=35)
        for model in MODELS:
            r = d["materials"][mat]["models"][model]
            m = r["metrics"]
            tables.append(
                [
                    LABEL[mat],
                    NAMES[model],
                    f"{m['phi122_abs']['mae']:.3f}",
                    f"{m['phi1122']['mae']:.3f}",
                    f"{m['quartic_sign_matches']}/5",
                    f"{m['energy_all_mev']['mae']:.3f}",
                    f"{m['force_all_ev_per_A']['mae']:.4f}",
                ]
            )
            for row in r["rows"]:
                details.append(
                    [
                        LABEL[mat],
                        row["channel"],
                        NAMES[model],
                        f"{row['phi122_abs']:.3f}",
                        f"{row['phi1122']:.3f}",
                        f"{row['residual_rmse_mev']:.4f}",
                    ]
                )
    save_table(
        "small_model_metrics",
        ["材料", "模型", "三阶MAE", "四阶MAE", "四阶符号", "E MAE", "F MAE"],
        tables,
    )
    save_table(
        "small_model_details",
        ["材料", "通道", "模型", "三阶强度", "有符号四阶", "拟合RMSE"],
        details,
    )
    save_table(
        "small_model_ws2",
        ["材料", "通道", "模型", "三阶强度", "有符号四阶", "拟合RMSE"],
        [r for r in details if r[0] == LABEL["ws2"]],
    )
    save_fig(fig, "small_model_errors")
    fig, ax = plt.subplots(figsize=(8, 4), layout="constrained")
    ms_m6 = next(
        r
        for r in read(DATA / "pbe_dft_mattersim_ws2.json")["rows"]
        if "__M_" in r["pair_code"] and r["pair_code"].endswith("m6")
    )
    tece_m6 = next(
        r
        for r in read(DATA / "full_model_comparison.json")["materials"]["ws2"][
            "models"
        ]["tece"]["scopes"]["fixed_pbe_dft_diagnostic"]["rows"]
        if r["physical_channel"] == "Gamma8-M6"
    )
    g = np.array(ms_m6["grid_ev"])
    axis = list(ms_m6["axis"])
    ids = [axis.index(-1), axis.index(1)]
    o = axis.index(0)
    ms_contrast = float(
        1000
        * (g[np.ix_(ids, ids)].mean() - g[ids, o].mean() - g[o, ids].mean() + g[o, o])
    )
    tc = tece_m6["even_mixed_mev"]["1.0"]
    vals = []
    for model in MODELS:
        r = next(
            r
            for r in d["materials"]["ws2"]["models"][model]["rows"]
            if r["channel"] == "Gamma8-M6"
        )
        vals.append(r["even_mixed_mev"]["1"]["model"])
    ax.bar(
        ["QE", "MatterSim", "TECE"] + [NAMES[x] for x in MODELS],
        [tc["qe"], ms_contrast, tc["model"]] + vals,
        color=["black", "#888888", "#228833", "#4477aa", "#ee6677", "#aa3377"],
    )
    ax.axhline(0, color="black", lw=0.7)
    ax.set_ylabel("Even mixed energy contrast (meV/supercell)")
    ax.set_title("WS2 Γ8–M6; identical PBE inputs, Q = 1")
    save_fig(fig, "small_model_m6")
    inputs = list(DATA.glob("small_model_*.json")) + [
        DATA / "small_model_resource_audit.txt"
    ]
    inputs.extend(
        DATA / name
        for name in [
            "full_model_comparison.json",
            "coupling_comparison.json",
            "pbe_dft_mattersim_comparison.json",
        ]
    )
    inputs.extend(DATA / f"results_{m}.json" for m in MATS)
    inputs.extend(DATA / f"pbe_dft_mattersim_{m}.json" for m in MATS)
    for folder in [
        "small_model_manifests",
        "structures",
        "historical_small_model_screen",
    ]:
        inputs.extend(p for p in (DATA / folder).glob("*") if p.is_file())
    inputs.extend(
        [REPO / "mlff_modepair_workflow/small_model_benchmark.py", Path(__file__)]
    )
    sources = {str(p.relative_to(REPO)): sha(p) for p in inputs}
    (Path(__file__).parent / "small_model_sources.json").write_text(
        json.dumps(sources, indent=2) + "\n"
    )


if __name__ == "__main__":
    main()
