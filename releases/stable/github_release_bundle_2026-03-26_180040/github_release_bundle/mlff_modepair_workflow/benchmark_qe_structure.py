#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

try:
    from benchmark_golden_pair import A1_VALS, A2_VALS
    from core import (
        analyze_pair_grid,
        compare_golden_metrics,
        compare_mode_frequency_metrics,
        compare_with_reference_grid,
        dump_json,
        evaluate_pair_grid,
        load_golden_reference,
        load_mode_pair_reference,
        load_pairs,
        make_calculator,
        save_pair_plot,
    )
    from native_dualtrack_core import (
        BACKEND_SPECS,
        BASE_GOLDEN_FIT,
        BASE_REF_GRID,
        BASE_STRUCTURE,
    )
except ModuleNotFoundError:
    from .benchmark_golden_pair import A1_VALS, A2_VALS
    from .core import (
        analyze_pair_grid,
        compare_golden_metrics,
        compare_mode_frequency_metrics,
        compare_with_reference_grid,
        dump_json,
        evaluate_pair_grid,
        load_golden_reference,
        load_mode_pair_reference,
        load_pairs,
        make_calculator,
        save_pair_plot,
    )
    from .native_dualtrack_core import (
        BACKEND_SPECS,
        BASE_GOLDEN_FIT,
        BASE_REF_GRID,
        BASE_STRUCTURE,
    )


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = ROOT / "native_dualtrack_runs"
DEFAULT_BACKENDS = ["chgnet_r2scan", "gptff_v2"]


def parse_args():
    p = argparse.ArgumentParser(description="Benchmark golden pair on the QE input structure (no ML relaxation).")
    p.add_argument("--backends", nargs="+", default=DEFAULT_BACKENDS, choices=sorted(BACKEND_SPECS))
    p.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    return p.parse_args()


def find_qe_golden_pair(pairs: list[dict]):
    for pair in pairs:
        if (
            int(pair["gamma_mode"]["mode_number_one_based"]) == 8
            and pair["target_mode"]["point_label"] == "M"
            and int(pair["target_mode"]["mode_number_one_based"]) == 3
        ):
            return pair
    raise RuntimeError("Golden pair Gamma(8)+M(3) not found in QE mode pairs.")


def main():
    args = parse_args()
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    golden_reference = load_golden_reference(BASE_GOLDEN_FIT)

    qe_pairs_json = (Path(__file__).resolve().parent.parent / "hex_qgamma_qpair_workflow" / "hex_qgamma_qpair_run" / "mode_pairs" / "selected_mode_pairs.json").resolve()
    qe_pairs = load_pairs(qe_pairs_json)
    golden_pair = find_qe_golden_pair(qe_pairs)
    mode_pair_reference = load_mode_pair_reference(golden_pair)

    for backend_tag in args.backends:
        backend_spec = BACKEND_SPECS[backend_tag]
        out_dir = output_root / backend_tag / "benchmark_qe_structure"
        out_dir.mkdir(parents=True, exist_ok=True)

        calc, backend_meta = make_calculator(
            backend=backend_spec["backend"],
            device=backend_spec["device"],
            model=None if backend_spec["model"] is None else str(backend_spec["model"]),
        )

        e_grid, builder = evaluate_pair_grid(
            golden_pair,
            structure_path=BASE_STRUCTURE,
            calc=calc,
            a1_vals=A1_VALS,
            a2_vals=A2_VALS,
            row_callback=None,
        )
        analysis = analyze_pair_grid(golden_pair, e_grid, A1_VALS, A2_VALS, fit_window=1.0)
        mode_pair_compare = compare_mode_frequency_metrics(analysis, mode_pair_reference)
        golden_compare = compare_golden_metrics(analysis, golden_reference)
        ref_compare = compare_with_reference_grid(BASE_REF_GRID, e_grid)

        summary = {
            "run_tag": f"{backend_tag}:qe_structure_benchmark",
            "backend": backend_meta,
            "structure": str(BASE_STRUCTURE),
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

        np.savetxt(out_dir / "energy_grid_eV.dat", e_grid, fmt="%.10f")
        dump_json(out_dir / "summary.json", summary)
        save_pair_plot(out_dir / "pes_map.png", e_grid, A1_VALS, A2_VALS, title=golden_pair["pair_code"])
        print(f"saved: {out_dir / 'summary.json'}")


if __name__ == "__main__":
    main()
