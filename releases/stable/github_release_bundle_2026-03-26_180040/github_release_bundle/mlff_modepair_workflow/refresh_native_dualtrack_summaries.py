#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = ROOT / "native_dualtrack_runs"
BASE_QE_MODE_PAIRS = ROOT.parent / "hex_qgamma_qpair_workflow" / "hex_qgamma_qpair_run" / "mode_pairs" / "selected_mode_pairs.json"


def parse_args():
    p = argparse.ArgumentParser(description="Refresh native dual-track summaries with explicit reference semantics.")
    p.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))
    return p.parse_args()


def load_json(path: Path):
    return json.loads(path.read_text())


def dump_json(path: Path, payload: dict):
    path.write_text(json.dumps(payload, indent=2))


def update_track_payload(track_root: Path, track_payload: dict):
    benchmark_path = track_root / "benchmark" / "summary.json"
    if benchmark_path.exists():
        benchmark = load_json(benchmark_path)
        track_payload["mode_pair_frequency_compare"] = benchmark.get("mode_pair_frequency_compare")
        track_payload["qe_mode_reference_compare"] = benchmark.get("qe_mode_reference_compare")
        track_payload["golden_pes_compare"] = benchmark.get("golden_pes_compare", benchmark.get("golden_compare"))
        track_payload["golden_compare"] = track_payload.get("golden_pes_compare")

    skip_path = track_root / "screening_skipped_reason.json"
    if skip_path.exists():
        skip_payload = load_json(skip_path)
        if "golden_pes_compare" not in skip_payload and "golden_compare" in skip_payload:
            skip_payload["golden_pes_compare"] = skip_payload["golden_compare"]
        track_payload["screening_skipped_reason"] = skip_payload
        dump_json(skip_path, skip_payload)
    return track_payload


def build_overview_payload(output_root: Path):
    backends = []
    for backend_dir in sorted(output_root.iterdir()):
        if not backend_dir.is_dir():
            continue
        summary_path = backend_dir / "run_summary.json"
        if not summary_path.exists():
            continue
        payload = load_json(summary_path)
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


def refresh_benchmark_summary(path: Path):
    payload = load_json(path)
    changed = False
    qe_pairs = load_json(BASE_QE_MODE_PAIRS)["pairs"]
    qe_golden = next(
        pair
        for pair in qe_pairs
        if int(pair["gamma_mode"]["mode_number_one_based"]) == 8
        and pair["target_mode"]["point_label"] == "M"
        and int(pair["target_mode"]["mode_number_one_based"]) == 3
    )
    qe_ref = {
        "reference_kind": "qe_mode_frequency",
        "reference_label": "qe_golden_pair_frequency",
        "gamma_freq_thz": float(qe_golden["gamma_mode"]["freq_thz"]),
        "target_freq_thz": float(qe_golden["target_mode"]["freq_thz"]),
    }

    analysis_ref = payload.get("analysis", {}).get("mode_pair_reference", payload.get("analysis", {}).get("reference"))
    if analysis_ref is not None and "mode_pair_reference" not in payload:
        payload["mode_pair_reference"] = analysis_ref
        changed = True

    if "mode_pair_frequency_compare" not in payload and analysis_ref is not None:
        axis = payload.get("analysis", {}).get("axis_checks", {})
        gamma_fit = axis.get("mode1_axis_fit", {}).get("freq", {})
        target_fit = axis.get("mode2_axis_fit", {}).get("freq", {})
        payload["mode_pair_frequency_compare"] = {
            "reference_kind": analysis_ref.get("reference_kind", "mode_pair_frequency"),
            "reference_label": analysis_ref.get("reference_label", "selected_mode_pair_frequency"),
            "gamma_freq_ref_thz": analysis_ref.get("gamma_freq_thz"),
            "gamma_freq_fit_thz": gamma_fit.get("thz"),
            "gamma_freq_abs_error_thz": None if gamma_fit.get("thz") is None else abs(float(gamma_fit["thz"]) - float(analysis_ref["gamma_freq_thz"])),
            "target_freq_ref_thz": analysis_ref.get("target_freq_thz"),
            "target_freq_fit_thz": target_fit.get("thz"),
            "target_freq_abs_error_thz": None if target_fit.get("thz") is None else abs(float(target_fit["thz"]) - float(analysis_ref["target_freq_thz"])),
        }
        changed = True

    if "golden_pes_reference" not in payload and "golden_reference" in payload:
        payload["golden_pes_reference"] = payload["golden_reference"]
        changed = True

    if "golden_pes_compare" not in payload and "golden_compare" in payload:
        payload["golden_pes_compare"] = payload["golden_compare"]
        changed = True

    if "qe_mode_reference" not in payload and "mode_pairs_" in str(path):
        payload["qe_mode_reference"] = qe_ref
        changed = True

    if "qe_mode_reference_compare" not in payload and "mode_pairs_" in str(path):
        axis = payload.get("analysis", {}).get("axis_checks", {})
        gamma_fit = axis.get("mode1_axis_fit", {}).get("freq", {})
        target_fit = axis.get("mode2_axis_fit", {}).get("freq", {})
        payload["qe_mode_reference_compare"] = {
            "reference_kind": qe_ref["reference_kind"],
            "reference_label": qe_ref["reference_label"],
            "gamma_freq_ref_thz": qe_ref["gamma_freq_thz"],
            "gamma_freq_fit_thz": gamma_fit.get("thz"),
            "gamma_freq_abs_error_thz": None if gamma_fit.get("thz") is None else abs(float(gamma_fit["thz"]) - qe_ref["gamma_freq_thz"]),
            "target_freq_ref_thz": qe_ref["target_freq_thz"],
            "target_freq_fit_thz": target_fit.get("thz"),
            "target_freq_abs_error_thz": None if target_fit.get("thz") is None else abs(float(target_fit["thz"]) - qe_ref["target_freq_thz"]),
        }
        changed = True

    desired_semantics = {
        "mode_pair_reference": (
            "Mode frequencies carried by the current track's selected_mode_pairs.json."
            if "mode_pairs_" in str(path)
            else "Raw Gamma/M mode frequencies from selected_mode_pairs.json (QE mode labeling reference)."
        ),
        "qe_mode_reference": "Raw Gamma/M mode frequencies from the original QE selected_mode_pairs.json.",
        "golden_pes_reference": "Frequencies and phi122 extracted from the fitted n7 PES reference.",
        "golden_compare_alias": "Deprecated compatibility alias for golden_pes_compare.",
    }
    if payload.get("reference_semantics") != desired_semantics:
        payload["reference_semantics"] = desired_semantics
        changed = True

    if changed:
        dump_json(path, payload)


def main():
    args = parse_args()
    output_root = Path(args.output_root).expanduser().resolve()

    for backend_dir in sorted(output_root.iterdir()):
        if not backend_dir.is_dir():
            continue
        summary_path = backend_dir / "run_summary.json"
        if not summary_path.exists():
            continue

        for benchmark_path in backend_dir.glob("mode_pairs_*/benchmark/summary.json"):
            refresh_benchmark_summary(benchmark_path)
        qe_benchmark = backend_dir / "benchmark_qe_structure" / "summary.json"
        if qe_benchmark.exists():
            refresh_benchmark_summary(qe_benchmark)

        payload = load_json(summary_path)
        payload["mode_pairs_qe_aligned"] = update_track_payload(backend_dir / "mode_pairs_qe_aligned", payload.get("mode_pairs_qe_aligned", {}))
        payload["mode_pairs_ml_self_consistent"] = update_track_payload(backend_dir / "mode_pairs_ml_self_consistent", payload.get("mode_pairs_ml_self_consistent", {}))
        dump_json(summary_path, payload)

    dump_json(output_root / "summary_overview.json", build_overview_payload(output_root))


if __name__ == "__main__":
    main()
