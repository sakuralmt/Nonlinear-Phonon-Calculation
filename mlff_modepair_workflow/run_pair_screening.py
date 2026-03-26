#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from core import (
    analyze_pair_grid,
    compare_golden_metrics,
    compare_mode_frequency_metrics,
    compare_with_reference_grid,
    dump_json,
    evaluate_pair_grid,
    find_golden_pair,
    load_golden_reference,
    load_mode_pair_reference,
    load_pairs,
    make_calculator,
    save_pair_plot,
)


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_MODE_PAIRS = ROOT / "hex_qgamma_qpair_workflow" / "hex_qgamma_qpair_run" / "mode_pairs" / "selected_mode_pairs.json"
DEFAULT_STRUCTURE = ROOT / "nonlocal phonon" / "scf.inp"
DEFAULT_REF_GRID = ROOT / "n7" / "E_tot_ph72_new.dat"
DEFAULT_GOLDEN_FIT = ROOT / "n7" / "fit_outputs_n7" / "fit_results.json"
DEFAULT_OUT_ROOT = SCRIPT_DIR / "runs"

A1_VALS = np.linspace(-2.0, 2.0, 9)
A2_VALS = np.linspace(-2.0, 2.0, 9)


def parse_args():
    p = argparse.ArgumentParser(description="Batch MLFF screening over selected Gamma-q mode pairs.")
    p.add_argument("--backend", type=str, default="chgnet")
    p.add_argument("--device", type=str, default="auto")
    p.add_argument("--model", type=str, default=None)
    p.add_argument("--run-tag", type=str, default=None, help="Optional output subdirectory tag")
    p.add_argument("--mode-pairs-json", type=str, default=str(DEFAULT_MODE_PAIRS))
    p.add_argument("--structure", type=str, default=str(DEFAULT_STRUCTURE))
    p.add_argument("--golden-fit-json", type=str, default=str(DEFAULT_GOLDEN_FIT))
    p.add_argument("--golden-ref-grid", type=str, default=str(DEFAULT_REF_GRID))
    p.add_argument("--output-root", type=str, default=str(DEFAULT_OUT_ROOT))
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--fit-window", type=float, default=1.0)
    return p.parse_args()


def main():
    args = parse_args()
    mode_pairs_json = Path(args.mode_pairs_json).expanduser().resolve()
    structure = Path(args.structure).expanduser().resolve()
    golden_fit_json = Path(args.golden_fit_json).expanduser().resolve()
    golden_ref_grid = Path(args.golden_ref_grid).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    run_tag = args.run_tag or args.backend
    output_dir = output_root / run_tag / "screening"
    output_dir.mkdir(parents=True, exist_ok=True)

    pair_records = load_pairs(mode_pairs_json)
    if args.limit is not None:
        pair_records = pair_records[: args.limit]

    golden_pair = find_golden_pair(load_pairs(mode_pairs_json))
    golden_pair_code = golden_pair["pair_code"]
    mode_pair_reference = load_mode_pair_reference(golden_pair)
    golden_reference = load_golden_reference(golden_fit_json)

    calc, backend_meta = make_calculator(backend=args.backend, device=args.device, model=args.model)

    ranking = []
    for idx, pair in enumerate(pair_records, start=1):
        pair_dir = output_dir / pair["pair_code"]
        pair_dir.mkdir(parents=True, exist_ok=True)

        e_grid, builder = evaluate_pair_grid(
            pair,
            structure_path=structure,
            calc=calc,
            a1_vals=A1_VALS,
            a2_vals=A2_VALS,
            row_callback=None,
        )
        analysis = analyze_pair_grid(pair, e_grid, A1_VALS, A2_VALS, fit_window=args.fit_window)

        mode_pair_compare = None
        golden_compare = None
        ref_compare = None
        if pair["pair_code"] == golden_pair_code:
            mode_pair_compare = compare_mode_frequency_metrics(analysis, mode_pair_reference)
            golden_compare = compare_golden_metrics(analysis, golden_reference)
            ref_compare = compare_with_reference_grid(golden_ref_grid, e_grid)

        summary = {
            "pair_code": pair["pair_code"],
            "structure": str(structure),
            "backend": backend_meta,
            "builder": builder.metadata(),
            "analysis": analysis,
            "mode_pair_reference": mode_pair_reference if pair["pair_code"] == golden_pair_code else None,
            "mode_pair_frequency_compare": mode_pair_compare,
            "golden_pes_reference": golden_reference if pair["pair_code"] == golden_pair_code else None,
            "golden_pes_compare": golden_compare,
            "golden_compare": golden_compare,
            "reference_grid_compare": ref_compare,
        }
        np.savetxt(pair_dir / "energy_grid_eV.dat", e_grid, fmt="%.10f")
        dump_json(pair_dir / "summary.json", summary)
        save_pair_plot(pair_dir / "pes_map.png", e_grid, A1_VALS, A2_VALS, title=pair["pair_code"])

        gamma_axis = analysis["axis_checks"]["mode1_axis_fit"]["freq"]
        target_axis = analysis["axis_checks"]["mode2_axis_fit"]["freq"]
        ranking.append(
            {
                "pair_code": pair["pair_code"],
                "coupling_type": pair["coupling_type"],
                "point_label": pair["target_mode"]["point_label"],
                "q_frac": pair["target_mode"]["q_frac"],
                "n_super": builder.n_super,
                "gamma_mode_code": pair["gamma_mode"]["mode_code"],
                "gamma_mode_number": pair["gamma_mode"]["mode_number_one_based"],
                "gamma_freq_ref_thz": pair["gamma_mode"]["freq_thz"],
                "gamma_freq_fit_thz": gamma_axis.get("thz"),
                "target_mode_code": pair["target_mode"]["mode_code"],
                "target_mode_number": pair["target_mode"]["mode_number_one_based"],
                "target_freq_ref_thz": pair["target_mode"]["freq_thz"],
                "target_freq_fit_thz": target_axis.get("thz"),
                "phi122_mev": analysis["physics"]["phi_122_mev_per_A3amu32"],
                "phi112_mev": analysis["physics"]["phi_112_mev_per_A3amu32"],
                "r2": analysis["r2"],
                "rmse_ev_supercell": analysis["rmse_ev_supercell"],
                "golden_phi122_abs_err_mev": None if golden_compare is None else golden_compare["phi122_abs_error_mev_per_A3amu32"],
            }
        )
        print(f"[{idx}/{len(pair_records)}] {pair['pair_code']} done")

    ranking.sort(key=lambda item: abs(item["phi122_mev"]), reverse=True)

    ranking_csv = output_dir / "pair_ranking.csv"
    with ranking_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rank",
                "pair_code",
                "coupling_type",
                "point_label",
                "qx",
                "qy",
                "qz",
                "n_super",
                "gamma_mode_code",
                "gamma_mode_number",
                "gamma_freq_ref_thz",
                "gamma_freq_fit_thz",
                "target_mode_code",
                "target_mode_number",
                "target_freq_ref_thz",
                "target_freq_fit_thz",
                "phi122_mev",
                "phi112_mev",
                "r2",
                "rmse_ev_supercell",
            ]
        )
        for rank, item in enumerate(ranking, start=1):
            q = item["q_frac"]
            writer.writerow(
                [
                    rank,
                    item["pair_code"],
                    item["coupling_type"],
                    item["point_label"],
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    item["n_super"],
                    item["gamma_mode_code"],
                    item["gamma_mode_number"],
                    f"{item['gamma_freq_ref_thz']:.6f}",
                    f"{item['gamma_freq_fit_thz']:.6f}" if item["gamma_freq_fit_thz"] is not None else "",
                    item["target_mode_code"],
                    item["target_mode_number"],
                    f"{item['target_freq_ref_thz']:.6f}",
                    f"{item['target_freq_fit_thz']:.6f}" if item["target_freq_fit_thz"] is not None else "",
                    f"{item['phi122_mev']:.6f}",
                    f"{item['phi112_mev']:.6f}",
                    f"{item['r2']:.6f}",
                    f"{item['rmse_ev_supercell']:.6f}",
                ]
            )

    dump_json(output_dir / "pair_ranking.json", {"backend": backend_meta, "pairs": ranking})

    x = np.array([item["gamma_freq_ref_thz"] for item in ranking], dtype=float)
    y = np.array([item["target_freq_ref_thz"] for item in ranking], dtype=float)
    z = np.array([item["phi122_mev"] for item in ranking], dtype=float)

    fig = plt.figure(figsize=(8.0, 6.0))
    ax = fig.add_subplot(111, projection="3d")
    sc = ax.scatter(x, y, z, c=np.abs(z), cmap="viridis", s=36)
    fig.colorbar(sc, ax=ax, shrink=0.75, label="|phi122| (meV)")
    ax.set_xlabel("Gamma mode freq (THz)")
    ax.set_ylabel("q-mode freq (THz)")
    ax.set_zlabel("phi122 (meV)")
    ax.set_title(f"{args.backend} Pair Screening")
    fig.tight_layout()
    fig.savefig(output_dir / "pair_screening_3d.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    top_n = min(15, len(ranking))
    fig, ax = plt.subplots(figsize=(9.0, 5.5))
    labels = [item["pair_code"] for item in ranking[:top_n]]
    vals = [abs(item["phi122_mev"]) for item in ranking[:top_n]]
    ax.barh(range(top_n), vals, color="#2E86AB")
    ax.set_yticks(range(top_n))
    ax.set_yticklabels(labels, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("|phi122| (meV)")
    ax.set_title("Top Coupling Pairs")
    fig.tight_layout()
    fig.savefig(output_dir / "pair_ranking_top15.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    dump_json(output_dir / "run_meta.json", {"run_tag": run_tag, "backend": backend_meta, "structure": str(structure), "n_pairs": len(ranking)})

    print(f"backend used: {args.backend}")
    print(f"screened pairs: {len(ranking)}")
    print(f"saved: {ranking_csv}")
    print(f"saved: {output_dir / 'pair_screening_3d.png'}")
    print(f"saved: {output_dir / 'pair_ranking_top15.png'}")


if __name__ == "__main__":
    main()
