#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from native_dualtrack_core import (
        BACKEND_SPECS,
        BASE_QE_EXTRACTED,
        BASE_QE_MODE_PAIRS,
        BASE_STRUCTURE,
        build_mode_alignment_from_bundle,
        build_phonon_bundle,
        build_qe_aligned_screened_payload,
        build_qpoint_diagnostics,
        build_self_consistent_screened_payload,
        compare_rankings,
        dump_json,
        find_golden_pair_for_track,
        load_baseline_benchmark,
        map_self_pair_codes_to_qe,
        passes_golden_gate,
        prepare_track_run_root,
        run_fixed_cell_relax,
        run_track_benchmark,
        run_track_screening,
    )
except ModuleNotFoundError:
    from .native_dualtrack_core import (
        BACKEND_SPECS,
        BASE_QE_EXTRACTED,
        BASE_QE_MODE_PAIRS,
        BASE_STRUCTURE,
        build_mode_alignment_from_bundle,
        build_phonon_bundle,
        build_qe_aligned_screened_payload,
        build_qpoint_diagnostics,
        build_self_consistent_screened_payload,
        compare_rankings,
        dump_json,
        find_golden_pair_for_track,
        load_baseline_benchmark,
        map_self_pair_codes_to_qe,
        passes_golden_gate,
        prepare_track_run_root,
        run_fixed_cell_relax,
        run_track_benchmark,
        run_track_screening,
    )


# ============================
# User configuration
# ============================
DEFAULT_BACKENDS = ["chgnet_r2scan", "gptff_v2"]
DEFAULT_OUTPUT_ROOT = Path(__file__).resolve().parent / "native_dualtrack_runs"
FORCE_RELAX = False


def parse_args():
    p = argparse.ArgumentParser(description="Native relaxed + native phonons dual-track controller for CHGNet/GPTFF.")
    p.add_argument("--backends", nargs="+", default=DEFAULT_BACKENDS, choices=sorted(BACKEND_SPECS))
    p.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    p.add_argument("--force-relax", action="store_true", default=FORCE_RELAX)
    return p.parse_args()


def build_overview_payload(output_root: Path):
    backends = []
    for backend_dir in sorted(output_root.iterdir()):
        if not backend_dir.is_dir():
            continue
        summary_path = backend_dir / "run_summary.json"
        if not summary_path.exists():
            continue

        payload = json.loads(summary_path.read_text())
        qe_track = payload.get("mode_pairs_qe_aligned", {})
        ml_track = payload.get("mode_pairs_ml_self_consistent", {})
        backends.append(
            {
                "backend_tag": payload.get("backend_tag"),
                "gate_pass": payload.get("gate_pass"),
                "gate_reasons": payload.get("gate_reasons"),
                "relax_final_fmax_eV_per_A": payload.get("relax_summary", {}).get("final_max_force_eV_per_A"),
                "qpoint_highlights": payload.get("qpoint_highlights", {}),
                "qe_aligned": {
                    "golden_pair_code": qe_track.get("golden_pair_code"),
                    "mode_pair_frequency_compare": qe_track.get("mode_pair_frequency_compare"),
                    "qe_mode_reference_compare": qe_track.get("qe_mode_reference_compare"),
                    "golden_pes_compare": qe_track.get("golden_pes_compare", qe_track.get("golden_compare")),
                    "screening_csv": qe_track.get("screening_csv"),
                    "screening_skipped_reason": qe_track.get("screening_skipped_reason"),
                },
                "ml_self_consistent": {
                    "golden_pair_code": ml_track.get("golden_pair_code"),
                    "mapped_qe_golden_pair_code": ml_track.get("mapped_qe_golden_pair_code"),
                    "mode_pair_frequency_compare": ml_track.get("mode_pair_frequency_compare"),
                    "qe_mode_reference_compare": ml_track.get("qe_mode_reference_compare"),
                    "golden_pes_compare": ml_track.get("golden_pes_compare", ml_track.get("golden_compare")),
                    "screening_csv": ml_track.get("screening_csv"),
                    "screening_skipped_reason": ml_track.get("screening_skipped_reason"),
                },
            }
        )
    return {"output_root": str(output_root), "backends": backends}


def main():
    args = parse_args()
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    qe_base_pairs = json.loads(Path(BASE_QE_MODE_PAIRS).read_text())
    qe_golden_reference_pair = next(
        pair
        for pair in qe_base_pairs["pairs"]
        if int(pair["gamma_mode"]["mode_number_one_based"]) == 8
        and pair["target_mode"]["point_label"] == "M"
        and int(pair["target_mode"]["mode_number_one_based"]) == 3
    )

    for backend_tag in args.backends:
        backend_spec = BACKEND_SPECS[backend_tag]
        backend_root = output_root / backend_tag
        backend_root.mkdir(parents=True, exist_ok=True)

        relax_summary = run_fixed_cell_relax(BASE_STRUCTURE, backend_spec, backend_root / "relax", force=bool(args.force_relax))
        relaxed_scf = Path(relax_summary["relaxed_structure_scf"]).expanduser().resolve()

        bundle = build_phonon_bundle(backend_spec, relaxed_scf, BASE_QE_EXTRACTED)
        alignment = build_mode_alignment_from_bundle(bundle)
        diagnostics = build_qpoint_diagnostics(alignment, backend_root / "qpoint_diagnostics")

        qe_aligned_payload = build_qe_aligned_screened_payload(alignment)
        self_payload = build_self_consistent_screened_payload(bundle)

        qe_aligned_pairs = prepare_track_run_root(backend_root / "mode_pairs_qe_aligned", qe_aligned_payload, relaxed_scf, mode_source="qe_aligned")
        self_pairs = prepare_track_run_root(backend_root / "mode_pairs_ml_self_consistent", self_payload, relaxed_scf, mode_source="ml_self_consistent")

        qe_aligned_golden = find_golden_pair_for_track(qe_aligned_pairs, alignment, mode_source="qe_aligned")
        self_golden = find_golden_pair_for_track(self_pairs, alignment, mode_source="ml_self_consistent")

        qe_aligned_benchmark = run_track_benchmark(
            backend_root / "mode_pairs_qe_aligned",
            qe_aligned_pairs,
            qe_aligned_golden,
            relaxed_scf,
            backend_spec,
            mode_source="qe_aligned",
            qe_reference_pair=qe_golden_reference_pair,
        )
        self_benchmark = run_track_benchmark(
            backend_root / "mode_pairs_ml_self_consistent",
            self_pairs,
            self_golden,
            relaxed_scf,
            backend_spec,
            mode_source="ml_self_consistent",
            qe_reference_pair=qe_golden_reference_pair,
        )

        gate_pass, gate_reasons = passes_golden_gate(qe_aligned_benchmark["golden_pes_compare"])
        self_mapping = map_self_pair_codes_to_qe(self_pairs, alignment, BASE_QE_MODE_PAIRS)

        qe_screening_csv = None
        self_screening_csv = None
        qe_comparison = None
        self_comparison = None
        skip_payload = None

        if gate_pass:
            qe_screening_csv = run_track_screening(
                backend_root / "mode_pairs_qe_aligned",
                qe_aligned_pairs,
                relaxed_scf,
                backend_spec,
                mode_source="qe_aligned",
                comparison_key_map=None,
            )
            self_screening_csv = run_track_screening(
                backend_root / "mode_pairs_ml_self_consistent",
                self_pairs,
                relaxed_scf,
                backend_spec,
                mode_source="ml_self_consistent",
                comparison_key_map=self_mapping,
            )

            baseline_csv = Path(backend_spec["baseline_screening_csv"]).expanduser().resolve()
            if baseline_csv.exists():
                qe_comparison = compare_rankings(
                    baseline_csv,
                    qe_screening_csv,
                    golden_compare_key=qe_aligned_golden["pair_code"],
                    key_field="comparison_key",
                )
                self_comparison = compare_rankings(
                    baseline_csv,
                    self_screening_csv,
                    golden_compare_key=self_mapping.get(self_golden["pair_code"]),
                    key_field="comparison_key",
                )
                dump_json(backend_root / "mode_pairs_qe_aligned" / "comparison_to_existing_baseline.json", qe_comparison)
                dump_json(backend_root / "mode_pairs_ml_self_consistent" / "comparison_to_existing_baseline.json", self_comparison)
        else:
            skip_payload = {
                "status": "skipped",
                "reason": "qe_aligned_golden_gate_failed",
                "gate_reasons": gate_reasons,
                "golden_pes_compare": qe_aligned_benchmark["golden_pes_compare"],
            }
            dump_json(backend_root / "mode_pairs_qe_aligned" / "screening_skipped_reason.json", skip_payload)
            dump_json(backend_root / "mode_pairs_ml_self_consistent" / "screening_skipped_reason.json", skip_payload)

        summary = {
            "backend_tag": backend_tag,
            "backend_spec": {k: str(v) for k, v in backend_spec.items()},
            "relax_summary": relax_summary,
            "qpoint_diagnostics_path": str(backend_root / "qpoint_diagnostics" / "qpoint_diagnostics.json"),
            "qpoint_highlights": diagnostics["highlights"],
            "gate_pass": gate_pass,
            "gate_reasons": gate_reasons,
            "mode_pairs_qe_aligned": {
                "track_root": str(backend_root / "mode_pairs_qe_aligned"),
                "golden_pair_code": qe_aligned_golden["pair_code"],
                "mode_pair_frequency_compare": qe_aligned_benchmark["mode_pair_frequency_compare"],
                "qe_mode_reference_compare": qe_aligned_benchmark["qe_mode_reference_compare"],
                "golden_pes_compare": qe_aligned_benchmark["golden_pes_compare"],
                "golden_compare": qe_aligned_benchmark["golden_pes_compare"],
                "screening_csv": None if qe_screening_csv is None else str(qe_screening_csv),
                "screening_skipped_reason": skip_payload,
                "comparison_to_existing_baseline": qe_comparison,
            },
            "mode_pairs_ml_self_consistent": {
                "track_root": str(backend_root / "mode_pairs_ml_self_consistent"),
                "golden_pair_code": self_golden["pair_code"],
                "mapped_qe_golden_pair_code": self_mapping.get(self_golden["pair_code"]),
                "mode_pair_frequency_compare": self_benchmark["mode_pair_frequency_compare"],
                "qe_mode_reference_compare": self_benchmark["qe_mode_reference_compare"],
                "golden_pes_compare": self_benchmark["golden_pes_compare"],
                "golden_compare": self_benchmark["golden_pes_compare"],
                "screening_csv": None if self_screening_csv is None else str(self_screening_csv),
                "screening_skipped_reason": skip_payload,
                "comparison_to_existing_baseline": self_comparison,
            },
            "baseline_benchmark": load_baseline_benchmark(Path(backend_spec["baseline_benchmark_summary"]).expanduser().resolve()),
        }
        dump_json(backend_root / "run_summary.json", summary)
        print(f"saved: {backend_root / 'run_summary.json'}")

    dump_json(output_root / "summary_overview.json", build_overview_payload(output_root))
    print(f"saved: {output_root / 'summary_overview.json'}")


if __name__ == "__main__":
    main()
