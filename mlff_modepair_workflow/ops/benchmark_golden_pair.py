#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import numpy as np

from core import (
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
    analyze_pair_grid,
)


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_OUT_ROOT = SCRIPT_DIR / "runs"

A1_VALS = np.linspace(-2.0, 2.0, 9)
A2_VALS = np.linspace(-2.0, 2.0, 9)


def parse_args():
    p = argparse.ArgumentParser(description="Benchmark MLFF backends on the golden Gamma(8)+M(3) pair.")
    p.add_argument("--backend", type=str, default="chgnet", help="Backend name or comma-separated list, or all")
    p.add_argument("--device", type=str, default="auto")
    p.add_argument("--model", type=str, default=None, help="Backend-specific model spec")
    p.add_argument("--run-tag", type=str, default=None, help="Optional output subdirectory tag")
    p.add_argument("--mode-pairs-json", type=str, required=True)
    p.add_argument("--structure", type=str, required=True)
    p.add_argument("--ref-grid", type=str, required=True)
    p.add_argument("--golden-fit-json", type=str, required=True)
    p.add_argument("--output-root", type=str, default=str(DEFAULT_OUT_ROOT))
    return p.parse_args()


def parse_backends(text: str):
    if text.strip().lower() == "all":
        return ["chgnet", "mace"]
    return [item.strip().lower() for item in text.split(",") if item.strip()]


def main():
    args = parse_args()
    mode_pairs_json = Path(args.mode_pairs_json).expanduser().resolve()
    structure = Path(args.structure).expanduser().resolve()
    ref_grid = Path(args.ref_grid).expanduser().resolve()
    golden_fit_json = Path(args.golden_fit_json).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(mode_pairs_json)
    golden_pair = find_golden_pair(pairs)
    mode_pair_reference = load_mode_pair_reference(golden_pair)
    golden_reference = load_golden_reference(golden_fit_json)

    ranking_rows = []
    for backend in parse_backends(args.backend):
        run_tag = args.run_tag or backend
        if len(parse_backends(args.backend)) > 1 and args.run_tag is None:
            run_tag = backend
        backend_out = output_root / run_tag / "benchmark"
        backend_out.mkdir(parents=True, exist_ok=True)

        calc, backend_meta = make_calculator(backend=backend, device=args.device, model=args.model)

        e_grid, builder = evaluate_pair_grid(
            golden_pair,
            structure_path=structure,
            calc=calc,
            a1_vals=A1_VALS,
            a2_vals=A2_VALS,
            row_callback=lambda i_a2, _a2: print(f"[{backend}] row {i_a2 + 1}/{len(A2_VALS)}"),
        )
        analysis = analyze_pair_grid(golden_pair, e_grid, A1_VALS, A2_VALS, fit_window=1.0)
        mode_pair_compare = compare_mode_frequency_metrics(analysis, mode_pair_reference)
        golden_compare = compare_golden_metrics(analysis, golden_reference)
        ref_compare = compare_with_reference_grid(ref_grid, e_grid)

        summary = {
            "pair_code": golden_pair["pair_code"],
            "structure": str(structure),
            "backend": backend_meta,
            "builder": builder.metadata(),
            "analysis": analysis,
            "mode_pair_reference": mode_pair_reference,
            "mode_pair_frequency_compare": mode_pair_compare,
            "golden_pes_reference": golden_reference,
            "golden_pes_compare": golden_compare,
            "golden_reference": golden_reference,
            "golden_compare": golden_compare,
            "reference_grid_compare": ref_compare,
            "reference_semantics": {
                "mode_pair_reference": "Raw Gamma/M mode frequencies from selected_mode_pairs.json (QE mode labeling reference).",
                "golden_pes_reference": "Frequencies and phi122 extracted from the fitted n7 PES reference.",
                "golden_compare_alias": "Deprecated compatibility alias for golden_pes_compare.",
            },
        }

        np.savetxt(backend_out / "energy_grid_eV.dat", e_grid, fmt="%.10f")
        dump_json(backend_out / "summary.json", summary)
        save_pair_plot(backend_out / "pes_map.png", e_grid, A1_VALS, A2_VALS, title=f"{backend}: {golden_pair['pair_code']}")

        ranking_rows.append(
            {
                "backend": backend,
                "device": backend_meta["device"],
                "gamma_fit_thz": golden_compare["gamma_freq_fit_thz"],
                "gamma_abs_err_thz": golden_compare["gamma_freq_abs_error_thz"],
                "target_fit_thz": golden_compare["target_freq_fit_thz"],
                "target_abs_err_thz": golden_compare["target_freq_abs_error_thz"],
                "phi122_fit_mev": golden_compare["phi122_fit_mev_per_A3amu32"],
                "phi122_abs_err_mev": golden_compare["phi122_abs_error_mev_per_A3amu32"],
                "grid_rmse_ev_supercell": None if ref_compare is None else ref_compare["rmse_ev_supercell"],
            }
        )

        print(f"[{backend}] gamma fit = {golden_compare['gamma_freq_fit_thz']}")
        print(f"[{backend}] target fit = {golden_compare['target_freq_fit_thz']}")
        print(f"[{backend}] phi122 = {golden_compare['phi122_fit_mev_per_A3amu32']:.6f} meV/(A*sqrt(amu))^3")

    ranking_name = "golden_pair_ranking.csv" if args.run_tag is None else f"golden_pair_ranking_{args.run_tag}.csv"
    ranking_csv = output_root / ranking_name
    with ranking_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "backend",
                "device",
                "gamma_fit_thz",
                "gamma_abs_err_thz",
                "target_fit_thz",
                "target_abs_err_thz",
                "phi122_fit_mev",
                "phi122_abs_err_mev",
                "grid_rmse_ev_supercell",
            ]
        )
        for row in sorted(
            ranking_rows,
            key=lambda item: (
                1.0e9 if item["gamma_abs_err_thz"] is None else item["gamma_abs_err_thz"],
                1.0e9 if item["target_abs_err_thz"] is None else item["target_abs_err_thz"],
                item["phi122_abs_err_mev"],
            ),
        ):
            writer.writerow(
                [
                    row["backend"],
                    row["device"],
                    "" if row["gamma_fit_thz"] is None else f"{row['gamma_fit_thz']:.6f}",
                    "" if row["gamma_abs_err_thz"] is None else f"{row['gamma_abs_err_thz']:.6f}",
                    "" if row["target_fit_thz"] is None else f"{row['target_fit_thz']:.6f}",
                    "" if row["target_abs_err_thz"] is None else f"{row['target_abs_err_thz']:.6f}",
                    f"{row['phi122_fit_mev']:.6f}",
                    f"{row['phi122_abs_err_mev']:.6f}",
                    "" if row["grid_rmse_ev_supercell"] is None else f"{row['grid_rmse_ev_supercell']:.6f}",
                ]
            )

    print(f"saved: {ranking_csv}")


if __name__ == "__main__":
    main()
