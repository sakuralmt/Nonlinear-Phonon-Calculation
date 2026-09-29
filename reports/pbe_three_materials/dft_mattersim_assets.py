"""Validate and plot the completed PBE-DFPT + MatterSim Stage2 baseline."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO))

from mlff_modepair_workflow.core import analyze_pair_grid
from mlff_modepair_workflow.units import UNITS
from reports.pbe_three_materials.generate_assets import (
    MATS,
    MODELS,
    LABEL,
    PLOT_LABEL,
    COLORS,
    MODEL_LABEL,
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
ROUTE = "dft-pbe-ms"
WEIGHT_SHA = "e3df9fa708725e3d453140646c7d1838324b347a3d1214cf1440522146f872b5"


def validate() -> dict:
    comparison = read(DATA / "coupling_comparison.json")
    summary = {
        "scope": "New PBE-relaxed DFT geometry and DFPT modes + MatterSim Stage2",
        "units": UNITS,
        "materials": {},
    }
    for material in MATS:
        path = DATA / f"pbe_dft_mattersim_{material}.json"
        result = read(path)
        qe = read(DATA / f"results_{material}.json")
        selection = read(DATA / f"selection_{material}.json")
        if (
            result["complete_pairs"] != 5
            or result["complete_points"] != 181
            or result["units"] != UNITS
            or result["identity"]["checkpoint_sha256"] != WEIGHT_SHA
            or result["identity"]["structure_sha256"]
            != sha(DATA / f"structures/{material}.scf.inp")
            or result["identity"]["reference_results_sha256"]
            != sha(DATA / f"results_{material}.json")
            or result["geometry_preflight"]["verified_points"] != 181
            or result["geometry_preflight"]["max_coordinate_difference_A"] >= 1e-6
        ):
            raise ValueError(f"{material}: incomplete or mismatched DFT+MS source")
        pairs = {
            item["pair"]["pair_code"]: item["pair"] for item in selection["selected"]
        }
        qe_rows = {row["pbe_pair_code"]: row for row in qe["rows"]}
        rows = {row["pair_code"]: row for row in result["rows"]}
        if len(rows) != 5 or set(rows) != set(pairs) or set(rows) != set(qe_rows):
            raise ValueError("DFT+MS channel set mismatch")
        output = []
        all_energy, all_force = [], []
        for channel in comparison["materials"][material]["rows"]:
            code = channel["pbe_pair_code"]
            row = rows[code]
            axis = row["axis"]
            energies = np.asarray(row["grid_ev"])
            forces = np.asarray(row["force_grid_ev_per_A"])
            ref = {
                (p["q_gamma"], p["q_finite"]): p for p in qe_rows[code]["source_points"]
            }
            if len(ref) != row["points"] or row["points"] != len(axis) ** 2:
                raise ValueError("DFT+MS duplicate/missing point")
            qe_grid = np.array([[ref[g, q]["energy_ev"] for g in axis] for q in axis])
            qe_forces = np.array(
                [[ref[g, q]["forces_ev_per_A"] for g in axis] for q in axis]
            )
            if (
                forces.shape != qe_forces.shape
                or not np.isfinite(forces).all()
                or not np.isfinite(energies).all()
            ):
                raise ValueError("DFT+MS invalid force/energy grid")
            fit = analyze_pair_grid(
                pairs[code], energies, np.asarray(axis), np.asarray(axis), fit_window=1
            )
            qe_fit = analyze_pair_grid(
                pairs[code], qe_grid, np.asarray(axis), np.asarray(axis), fit_window=1
            )
            if fit["fit_design_rank"] != 13 or qe_fit["fit_design_rank"] != 13:
                raise ValueError("DFT+MS rank-deficient fit")
            for key in (THIRD, FOURTH):
                if not np.isclose(
                    fit["physics"][key],
                    row["center_fit"]["physics"][key],
                    atol=1e-9,
                    rtol=0,
                ):
                    raise ValueError("DFT+MS fit does not reproduce")
                if not np.isclose(
                    qe_fit["physics"][key],
                    qe_rows[code]["center_fit"]["physics"][key],
                    atol=1e-9,
                    rtol=0,
                ):
                    raise ValueError("QE original fit does not reproduce")
            center = axis.index(0.0)
            delta = (
                (energies - energies[center, center])
                - (qe_grid - qe_grid[center, center])
            ) * 1000
            force_delta = forces - qe_forces
            mask = np.asarray([abs(a) <= 1 for a in axis])
            central_delta = delta[np.ix_(mask, mask)]
            central_force = force_delta[np.ix_(mask, mask)]
            for key, array, operation in (
                ("relative_energy_mae_mev", delta, lambda x: np.mean(abs(x))),
                ("relative_energy_rmse_mev", delta, lambda x: np.sqrt(np.mean(x * x))),
                ("force_mae_ev_per_A", force_delta, lambda x: np.mean(abs(x))),
                ("force_rmse_ev_per_A", force_delta, lambda x: np.sqrt(np.mean(x * x))),
                (
                    "central_relative_energy_mae_mev",
                    central_delta,
                    lambda x: np.mean(abs(x)),
                ),
                (
                    "central_relative_energy_rmse_mev",
                    central_delta,
                    lambda x: np.sqrt(np.mean(x * x)),
                ),
                (
                    "central_force_mae_ev_per_A",
                    central_force,
                    lambda x: np.mean(abs(x)),
                ),
                (
                    "central_force_rmse_ev_per_A",
                    central_force,
                    lambda x: np.sqrt(np.mean(x * x)),
                ),
            ):
                if not np.isclose(operation(array), row[key], atol=1e-10, rtol=0):
                    raise ValueError(f"DFT+MS {key} does not reproduce")
            all_energy.extend(delta.ravel().tolist())
            all_force.extend(force_delta.ravel().tolist())
            output.append(
                {
                    "physical_channel": channel["physical_channel"],
                    "pair_code": code,
                    "phi122_abs": abs(fit["physics"][THIRD]),
                    "phi1122": fit["physics"][FOURTH],
                    "central_energy_mae_mev": row["central_relative_energy_mae_mev"],
                    "central_force_mae_ev_per_A": row["central_force_mae_ev_per_A"],
                    "fit_rmse_mev": fit["center_fit_rmse_ev_supercell"] * 1000,
                }
            )
        metrics = {}
        for key in ("phi122_abs", "phi1122"):
            delta = np.array(
                [
                    row[key] - channel["pbe"][key]
                    for row, channel in zip(
                        output, comparison["materials"][material]["rows"]
                    )
                ]
            )
            metrics[key] = {
                "count": 5,
                "mae": float(np.mean(abs(delta))),
                "rmse": float(np.sqrt(np.mean(delta * delta))),
            }
        for label, values in (
            ("relative_energy_mev", all_energy),
            ("force_component_ev_per_A", all_force),
        ):
            a = np.asarray(values)
            metrics[label] = {
                "count": len(a),
                "mae": float(np.mean(abs(a))),
                "rmse": float(np.sqrt(np.mean(a * a))),
            }
        summary["materials"][material] = {
            "result_sha256": sha(path),
            "rows": output,
            "metrics": metrics,
            "inference_wall_seconds": result["wall_seconds"],
        }
    return summary


def main() -> None:
    summary = validate()
    comparison = read(DATA / "coupling_comparison.json")
    table = []
    for material, item in summary["materials"].items():
        for row in item["rows"]:
            table.append(
                [
                    LABEL[material],
                    row["physical_channel"].replace("Gamma", r"$\Gamma$"),
                    fnum(row["phi122_abs"]),
                    fnum(row["phi1122"]),
                    fnum(row["central_energy_mae_mev"]),
                    fnum(row["central_force_mae_ev_per_A"], 5),
                    fnum(row["fit_rmse_mev"], 4),
                ]
            )
    save_table(
        "pbe_dft_ms_details",
        [
            "材料",
            "声子对",
            r"$|\Phi_{122}|$",
            r"$\Phi_{1122}$",
            "中心E MAE",
            "中心F MAE",
            "拟合RMSE",
        ],
        table,
    )
    table = []
    for material, item in summary["materials"].items():
        e = item["metrics"]
        table.append(
            [
                LABEL[material],
                fnum(e["phi122_abs"]["mae"]),
                fnum(e["phi122_abs"]["rmse"]),
                fnum(e["phi1122"]["mae"]),
                fnum(e["phi1122"]["rmse"]),
                fnum(e["relative_energy_mev"]["mae"]),
                fnum(e["force_component_ev_per_A"]["mae"], 5),
            ]
        )
    save_table(
        "pbe_dft_ms_metrics",
        [
            "材料",
            "三阶MAE",
            "三阶RMSE",
            "四阶MAE",
            "四阶RMSE",
            "全网格E MAE",
            "全网格F MAE",
        ],
        table,
    )
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8), layout="constrained")
    for ax, key, title in zip(
        axes,
        ("phi122_abs", "phi1122"),
        ("Cubic MAE vs PBE", "Signed quartic MAE vs PBE"),
    ):
        x = np.arange(3)
        for i, route in enumerate((ROUTE, *MODELS)):
            values = [
                (
                    summary["materials"][m]["metrics"][key]["mae"]
                    if route == ROUTE
                    else comparison["materials"][m]["metrics"]["pbe"][route][key]["mae"]
                )
                for m in MATS
            ]
            ax.bar(
                x + (i - 1.5) * 0.2,
                values,
                0.19,
                label="PBE DFT+MS" if route == ROUTE else MODEL_LABEL[route],
                color="#c44b55" if route == ROUTE else COLORS[route],
            )
        ax.set_xticks(x, [PLOT_LABEL[m] for m in MATS])
        ax.set_title(title)
        ax.grid(axis="y", alpha=0.2)
        ax.set_ylabel(
            "meV / (A^3 amu^1.5)" if key == "phi122_abs" else "meV / (A^4 amu^2)"
        )
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        ncol=2,
        loc="upper center",
        bbox_to_anchor=(0.5, 1.17),
        frameon=False,
    )
    save_fig(fig, "pbe_dft_ms_errors")
    (DATA / "pbe_dft_mattersim_comparison.json").write_text(
        json.dumps(summary, indent=2, allow_nan=False) + "\n"
    )
    inputs = [DATA / f"pbe_dft_mattersim_{m}.json" for m in MATS]
    (HERE / "pbe_dft_ms_sources.json").write_text(
        json.dumps({str(p.relative_to(REPO)): sha(p) for p in inputs}, indent=2) + "\n"
    )
    print(
        json.dumps(
            {m: item["metrics"] for m, item in summary["materials"].items()}, indent=2
        )
    )


if __name__ == "__main__":
    main()
