"""Reproducible v3 MLFF/archived-QE phonon and Phonopy-ASR audit.

The QE mode dataset must retain its independent geometry-verification flag.
An unverified archival match is descriptive, even if the copied structure
hash in the converted dataset equals the model's structure hash.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from mlff_modepair_workflow.compare_stage1_models_v3 import compare
from mlff_modepair_workflow.core import load_atoms_from_qe
from mlff_modepair_workflow.phonopy_bridge import (
    apply_phonopy_asr,
    legacy_to_phonopy_force_constants,
    make_phonopy,
    phonons_from_phonopy,
)
from mlff_modepair_workflow.prophet_backend import sha256_file


def _metrics(comparison: dict) -> dict:
    errors = np.array([r["matched_frequency_abs_error_thz"] for r in comparison["q_records"]])
    slices = {
        "gamma_acoustic": errors[0, :3],
        "gamma_optical": errors[0, 3:],
        "finite_q_acoustic": errors[1:, :3].ravel(),
        "finite_q_optical": errors[1:, 3:].ravel(),
        "all": errors.ravel(),
    }
    categories = {
        name: {
            "count": int(len(values)),
            "mae_thz": float(np.mean(values)),
            "rmse_thz": float(np.sqrt(np.mean(values**2))),
            "p95_abs_error_thz": float(np.percentile(values, 95)),
            "max_abs_error_thz": float(np.max(values)),
        }
        for name, values in slices.items()
    }
    isolated = []
    degenerate = []
    for row in comparison["q_records"]:
        for group in row["reference_degenerate_subspaces"]:
            if len(group["reference_modes_one_based"]) == 1:
                isolated.append(group["subspace_overlap"])
            else:
                degenerate.append(group["subspace_overlap"])
    selected = {}
    for row in comparison["q_records"]:
        key = tuple(row["q_index"])
        if key in {(0, 0), (3, 0), (2, 2)}:
            selected["%d,%d" % key] = row
    return {
        "frequency_error_by_group": categories,
        "isolated_mode_count": len(isolated),
        "isolated_mode_median_overlap_squared": float(np.median(isolated)) if isolated else None,
        "isolated_mode_fraction_overlap_above_0_9": float(np.mean(np.array(isolated) > 0.9)) if isolated else None,
        "degenerate_group_count": len(degenerate),
        "degenerate_group_median_subspace_overlap": float(np.median(degenerate)) if degenerate else None,
        "selected_q_records": selected,
    }


def audit_model(qe: dict, model: dict, model_dir: Path, qe_asr: str) -> dict:
    source = model["source"]
    structure = Path(source["structure"])
    if not structure.is_file():
        structure = Path(qe["source"]["structure"])
    if sha256_file(structure) != source["structure_sha256"]:
        raise ValueError(f"Model structure changed: {structure}")
    if source["q_grid"] != qe["source"]["q_grid"]:
        raise ValueError("MLFF and QE q grids differ")
    if source["structure_sha256"] != qe["source"]["structure_sha256"]:
        raise ValueError("MLFF and converted QE structure hashes differ")
    if source["symbols"] != qe["source"]["symbols"]:
        raise ValueError("MLFF and QE atom order differs")
    primitive = load_atoms_from_qe(structure)
    mesh_n = source["q_grid"][0]
    with np.load(model_dir / "force_constants.npz") as arrays:
        saved_is_asr = "force_constants_raw_ev_per_A2" in arrays
        raw_phi = arrays["force_constants_raw_ev_per_A2"] if saved_is_asr else arrays["force_constants_ev_per_A2"]
        saved_phi = arrays["force_constants_ev_per_A2"]
    phonon = make_phonopy(primitive, mesh_n)
    phonon.force_constants = legacy_to_phonopy_force_constants(phonon, raw_phi)
    off_grid_q = np.array([[0, 0, 0], [1/6, 0, 0], [1/12, 0, 0], [1/24, 0, 0], [1/48, 0, 0]])
    phonon.run_qpoints(off_grid_q)
    raw_low_q = np.asarray(phonon.get_qpoints_dict()["frequencies"])[:, :3].copy()
    corrected_phi, asr = apply_phonopy_asr(phonon, mesh_n)
    phonon.run_qpoints(off_grid_q)
    fixed_low_q = np.asarray(phonon.get_qpoints_dict()["frequencies"])[:, :3].copy()
    raw_records, _ = phonons_from_phonopy(primitive, raw_phi, mesh_n)
    fixed_records, _ = phonons_from_phonopy(primitive, corrected_phi, mesh_n)
    saved_frequencies = np.array([r["freqs_thz"] for r in model["q_points"]])
    reconstructed_raw = np.array([r["freqs_thz"] for r in raw_records])
    reconstructed_fixed = np.array([r["freqs_thz"] for r in fixed_records])
    reconstructed_saved = reconstructed_fixed if saved_is_asr else reconstructed_raw
    if np.max(np.abs(saved_frequencies - reconstructed_saved)) > 1e-5:
        raise ValueError("Saved Stage1 frequencies disagree with the saved force constants")
    def with_records(records):
        return {**model, "q_points": records}
    raw_comparison = compare(qe, with_records(raw_records))
    fixed_comparison = compare(qe, with_records(fixed_records))
    return {
        "backend": source["backend"],
        "structure_sha256": source["structure_sha256"],
        "geometry_source": source["geometry_source"],
        "qe_geometry_verified": bool(qe["source"].get("qe_geometry_verified", False)),
        "qe_asr": qe_asr,
        "mlff_saved_asr": saved_is_asr,
        "comparison_status": "quantitative_same_geometry" if qe["source"].get("qe_geometry_verified") else "historical_qe_geometry_unverified",
        "raw": _metrics(raw_comparison),
        "phonopy_asr": _metrics(fixed_comparison),
        "q_records_raw": raw_comparison["q_records"],
        "q_records_phonopy_asr": fixed_comparison["q_records"],
        "asr_diagnostic": {
            **asr,
            "gamma_raw_thz": raw_low_q[0].tolist(),
            "gamma_asr_thz": fixed_low_q[0].tolist(),
            "max_finite_6x6_frequency_change_thz": float(np.max(np.abs(reconstructed_raw[1:] - reconstructed_fixed[1:]))),
            "max_gamma_optical_frequency_change_thz": float(np.max(np.abs(reconstructed_raw[0, 3:] - reconstructed_fixed[0, 3:]))),
            "off_grid_q_frac": off_grid_q.tolist(),
            "off_grid_lowest_three_raw_thz": raw_low_q.tolist(),
            "off_grid_lowest_three_asr_thz": fixed_low_q.tolist(),
            "saved_vs_reconstructed_max_frequency_difference_thz": float(np.max(np.abs(saved_frequencies - reconstructed_saved))),
            "saved_vs_asr_max_force_constant_difference_ev_per_A2": float(np.max(np.abs(saved_phi - corrected_phi))) if saved_is_asr else None,
        },
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qe-dataset", type=Path, required=True)
    parser.add_argument("--qe-asr", choices=["simple", "no", "crystal", "unknown"], required=True)
    parser.add_argument("--model-dir", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    qe = json.loads(args.qe_dataset.read_text())
    result = {
        "kind": "v3_mlff_dft_phonopy_asr_audit",
        "qe_dataset": str(args.qe_dataset.resolve()),
        "qe_source": qe["source"],
        "qe_asr": args.qe_asr,
        "models": {},
    }
    for model_dir in args.model_dir:
        model = json.loads((model_dir / "phonon_dataset.json").read_text())
        key = model["source"]["backend"]
        if key in result["models"]:
            raise ValueError(f"Repeated backend {key}; use one geometry line per report")
        result["models"][key] = audit_model(qe, model, model_dir, args.qe_asr)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    for name, row in result["models"].items():
        print(name, row["comparison_status"], "raw MAE", row["raw"]["frequency_error_by_group"]["all"]["mae_thz"],
              "ASR MAE", row["phonopy_asr"]["frequency_error_by_group"]["all"]["mae_thz"],
              "raw drift", row["asr_diagnostic"]["raw_max_translational_drift_ev_per_A2"])
    print(args.output)


if __name__ == "__main__":
    main()
