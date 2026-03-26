#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np

from benchmark_golden_pair import A1_VALS, A2_VALS
from build_ml_mode_pairs import RUN_CONFIGS, build_mode_alignment, build_mode_pairs_from_alignment, write_outputs
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
    make_calculator,
    save_pair_plot,
)


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_QE_EXTRACTED = ROOT / "hex_qgamma_qpair_workflow" / "hex_qgamma_qpair_run" / "extracted" / "screened_eigenvectors.json"
DEFAULT_QE_MODE_PAIRS = ROOT / "hex_qgamma_qpair_workflow" / "hex_qgamma_qpair_run" / "mode_pairs" / "selected_mode_pairs.json"
DEFAULT_REF_GRID = ROOT / "n7" / "E_tot_ph72_new.dat"
DEFAULT_GOLDEN_FIT = ROOT / "n7" / "fit_outputs_n7" / "fit_results.json"
DEFAULT_OUT_ROOT = SCRIPT_DIR / "runs"


def load_ranking(path: Path):
    with path.open() as f:
        rows = list(csv.DictReader(f))
    rank_map = {}
    for row in rows:
        rank_map[row["pair_code"]] = {
            "rank": int(row["rank"]),
            "phi122_mev": float(row["phi122_mev"]),
            "gamma_freq_fit_thz": float(row["gamma_freq_fit_thz"]) if row["gamma_freq_fit_thz"] else None,
            "target_freq_fit_thz": float(row["target_freq_fit_thz"]) if row["target_freq_fit_thz"] else None,
        }
    return rows, rank_map


def rankdata_desc(values: np.ndarray):
    order = np.argsort(-values)
    ranks = np.empty(len(values), dtype=float)
    i = 0
    while i < len(values):
        j = i + 1
        while j < len(values) and values[order[j]] == values[order[i]]:
            j += 1
        avg_rank = 0.5 * (i + 1 + j)
        ranks[order[i:j]] = avg_rank
        i = j
    return ranks


def spearman_abs_phi(rows_a, rows_b):
    common = sorted(set(rows_a) & set(rows_b))
    if len(common) < 2:
        return None
    a_vals = np.array([abs(rows_a[key]["phi122_mev"]) for key in common], dtype=float)
    b_vals = np.array([abs(rows_b[key]["phi122_mev"]) for key in common], dtype=float)
    ra = rankdata_desc(a_vals)
    rb = rankdata_desc(b_vals)
    corr = np.corrcoef(ra, rb)[0, 1]
    return float(corr)


def compare_runs(baseline_ranking_csv: Path, new_ranking_csv: Path, golden_pair_code: str, top_n: int = 10):
    base_rows, base_map = load_ranking(baseline_ranking_csv)
    new_rows, new_map = load_ranking(new_ranking_csv)

    base_top = [row["pair_code"] for row in base_rows[:top_n]]
    new_top = [row["pair_code"] for row in new_rows[:top_n]]
    top_overlap = sorted(set(base_top) & set(new_top))

    comparison_rows = []
    for pair_code in sorted(set(base_map) & set(new_map), key=lambda code: new_map[code]["rank"]):
        comparison_rows.append(
            {
                "pair_code": pair_code,
                "baseline_rank": base_map[pair_code]["rank"],
                "new_rank": new_map[pair_code]["rank"],
                "rank_shift": new_map[pair_code]["rank"] - base_map[pair_code]["rank"],
                "baseline_phi122_mev": base_map[pair_code]["phi122_mev"],
                "new_phi122_mev": new_map[pair_code]["phi122_mev"],
            }
        )

    return {
        "golden_pair": {
            "pair_code": golden_pair_code,
            "baseline_rank": base_map[golden_pair_code]["rank"],
            "new_rank": new_map[golden_pair_code]["rank"],
            "baseline_phi122_mev": base_map[golden_pair_code]["phi122_mev"],
            "new_phi122_mev": new_map[golden_pair_code]["phi122_mev"],
        },
        "top_overlap": {
            "top_n": top_n,
            "baseline_top_pairs": base_top,
            "new_top_pairs": new_top,
            "common_pairs": top_overlap,
            "overlap_fraction": len(top_overlap) / float(top_n),
        },
        "spearman_abs_phi": spearman_abs_phi(base_map, new_map),
        "rank_differences": comparison_rows,
    }


def run_benchmark(mode_pairs_payload: dict, structure: Path, calc, backend_meta: dict, golden_fit_json: Path, ref_grid: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    golden_pair = find_golden_pair(mode_pairs_payload["pairs"])
    mode_pair_reference = load_mode_pair_reference(golden_pair)
    golden_reference = load_golden_reference(golden_fit_json)

    e_grid, builder = evaluate_pair_grid(
        golden_pair,
        structure_path=structure,
        calc=calc,
        a1_vals=A1_VALS,
        a2_vals=A2_VALS,
        row_callback=lambda i_a2, _a2: print(f"[benchmark] row {i_a2 + 1}/{len(A2_VALS)}"),
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
        "mode_source": "ml_phonopy_matched_to_qe",
    }

    np.savetxt(out_dir / "energy_grid_eV.dat", e_grid, fmt="%.10f")
    dump_json(out_dir / "summary.json", summary)
    save_pair_plot(out_dir / "pes_map.png", e_grid, A1_VALS, A2_VALS, title=f"ml modes: {golden_pair['pair_code']}")
    return summary


def run_screening(mode_pairs_payload: dict, structure: Path, calc, backend_meta: dict, golden_fit_json: Path, ref_grid: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    golden_pair = find_golden_pair(mode_pairs_payload["pairs"])
    mode_pair_reference = load_mode_pair_reference(golden_pair)
    golden_reference = load_golden_reference(golden_fit_json)
    ranking = []

    for idx, pair in enumerate(mode_pairs_payload["pairs"], start=1):
        pair_dir = out_dir / pair["pair_code"]
        pair_dir.mkdir(parents=True, exist_ok=True)
        e_grid, builder = evaluate_pair_grid(pair, structure_path=structure, calc=calc, a1_vals=A1_VALS, a2_vals=A2_VALS, row_callback=None)
        analysis = analyze_pair_grid(pair, e_grid, A1_VALS, A2_VALS, fit_window=1.0)

        mode_pair_compare = None
        golden_compare = None
        ref_compare = None
        if pair["pair_code"] == golden_pair["pair_code"]:
            mode_pair_compare = compare_mode_frequency_metrics(analysis, mode_pair_reference)
            golden_compare = compare_golden_metrics(analysis, golden_reference)
            ref_compare = compare_with_reference_grid(ref_grid, e_grid)

        summary = {
            "pair_code": pair["pair_code"],
            "structure": str(structure),
            "backend": backend_meta,
            "builder": builder.metadata(),
            "analysis": analysis,
            "mode_pair_reference": mode_pair_reference if pair["pair_code"] == golden_pair["pair_code"] else None,
            "mode_pair_frequency_compare": mode_pair_compare,
            "golden_pes_reference": golden_reference if pair["pair_code"] == golden_pair["pair_code"] else None,
            "golden_pes_compare": golden_compare,
            "golden_compare": golden_compare,
            "reference_grid_compare": ref_compare,
            "mode_source": "ml_phonopy_matched_to_qe",
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
            }
        )
        print(f"[screening] {idx}/{len(mode_pairs_payload['pairs'])} {pair['pair_code']}")

    ranking.sort(key=lambda item: abs(item["phi122_mev"]), reverse=True)

    ranking_csv = out_dir / "pair_ranking.csv"
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

    dump_json(out_dir / "pair_ranking.json", {"backend": backend_meta, "pairs": ranking, "mode_source": "ml_phonopy_matched_to_qe"})
    return ranking_csv


def parse_args():
    p = argparse.ArgumentParser(description="End-to-end test: ML phonon eigenvectors -> mode pairs -> benchmark -> screening.")
    p.add_argument("--run-tag", type=str, default="chgnet_r2scan_relaxed", choices=sorted(RUN_CONFIGS))
    p.add_argument("--baseline-run-tag", type=str, default="chgnet_r2scan_relaxed")
    p.add_argument("--qe-extracted-json", type=str, default=str(DEFAULT_QE_EXTRACTED))
    p.add_argument("--qe-mode-pairs-json", type=str, default=str(DEFAULT_QE_MODE_PAIRS))
    p.add_argument("--golden-fit-json", type=str, default=str(DEFAULT_GOLDEN_FIT))
    p.add_argument("--ref-grid", type=str, default=str(DEFAULT_REF_GRID))
    p.add_argument("--output-root", type=str, default=str(DEFAULT_OUT_ROOT))
    p.add_argument("--skip-screening", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    qe_extracted_json = Path(args.qe_extracted_json).expanduser().resolve()
    qe_mode_pairs_json = Path(args.qe_mode_pairs_json).expanduser().resolve()
    golden_fit_json = Path(args.golden_fit_json).expanduser().resolve()
    ref_grid = Path(args.ref_grid).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    run_root = output_root / f"{args.run_tag}_ml_modes"
    run_root.mkdir(parents=True, exist_ok=True)

    alignment = build_mode_alignment(args.run_tag, qe_extracted_json)
    mode_pairs_payload = build_mode_pairs_from_alignment(qe_mode_pairs_json, alignment)
    write_outputs(run_root / "ml_modes", alignment, mode_pairs_payload)

    config = RUN_CONFIGS[args.run_tag]
    structure = Path(config["structure"]).expanduser().resolve()
    calc, backend_meta = make_calculator(config["backend"], device=config["device"], model=config["model"])

    benchmark_summary = run_benchmark(
        mode_pairs_payload=mode_pairs_payload,
        structure=structure,
        calc=calc,
        backend_meta=backend_meta,
        golden_fit_json=golden_fit_json,
        ref_grid=ref_grid,
        out_dir=run_root / "benchmark",
    )

    screening_comparison = None
    if not args.skip_screening:
        new_ranking_csv = run_screening(
            mode_pairs_payload=mode_pairs_payload,
            structure=structure,
            calc=calc,
            backend_meta=backend_meta,
            golden_fit_json=golden_fit_json,
            ref_grid=ref_grid,
            out_dir=run_root / "screening",
        )
        baseline_ranking_csv = output_root / args.baseline_run_tag / "screening" / "pair_ranking.csv"
        if baseline_ranking_csv.exists():
            golden_pair_code = find_golden_pair(mode_pairs_payload["pairs"])["pair_code"]
            screening_comparison = compare_runs(baseline_ranking_csv, new_ranking_csv, golden_pair_code=golden_pair_code, top_n=10)
            dump_json(run_root / "mode_source_comparison.json", screening_comparison)

    summary = {
        "run_tag": args.run_tag,
        "structure": str(structure),
        "backend": backend_meta,
        "mode_source": "ml_phonopy_matched_to_qe",
        "benchmark": {
            "gamma_fit_thz": benchmark_summary["golden_compare"]["gamma_freq_fit_thz"],
            "target_fit_thz": benchmark_summary["golden_compare"]["target_freq_fit_thz"],
            "phi122_fit_mev": benchmark_summary["golden_compare"]["phi122_fit_mev_per_A3amu32"],
            "gamma_abs_err_thz": benchmark_summary["golden_compare"]["gamma_freq_abs_error_thz"],
            "target_abs_err_thz": benchmark_summary["golden_compare"]["target_freq_abs_error_thz"],
            "phi122_abs_err_mev": benchmark_summary["golden_compare"]["phi122_abs_error_mev_per_A3amu32"],
        },
        "screening_comparison": screening_comparison,
    }
    dump_json(run_root / "run_summary.json", summary)

    print(f"saved: {run_root / 'run_summary.json'}")
    if screening_comparison is not None:
        print(f"saved: {run_root / 'mode_source_comparison.json'}")


if __name__ == "__main__":
    main()
