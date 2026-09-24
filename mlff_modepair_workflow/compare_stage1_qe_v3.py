"""Compare any v3 MLFF Stage1 mesh against a v3 QE mesh on one structure."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from .phonon_eigenvectors import compare_modes
from .units import CONTRACT_VERSION, NORMALIZATION_VERSION


def _vectors(record: dict) -> np.ndarray:
    return np.column_stack([
        np.asarray([[complex(*component) for component in atom] for atom in mode],
                   dtype=complex).reshape(-1)
        for mode in record["eigenvectors"]
    ])


def compare(model_dataset: dict, qe_dataset: dict, *,
            allow_unverified_historical: bool = False) -> dict:
    for name, dataset in (("model", model_dataset), ("qe", qe_dataset)):
        if (dataset.get("version") != CONTRACT_VERSION
                or dataset["source"].get("normalization_version") != NORMALIZATION_VERSION):
            raise ValueError(f"{name} Stage1 is not v3 with unit-mass real modes")
    model_source = model_dataset["source"]
    qe_source = qe_dataset["source"]
    if qe_source["backend"] != "qe":
        raise ValueError("QE reference dataset must come from QE v3 Stage1")
    verified = bool(qe_source.get("qe_geometry_verified"))
    if not verified and not allow_unverified_historical:
        raise ValueError("QE reference geometry is unverified; use an explicit historical-only comparison")
    if (model_source["structure_sha256"] != qe_source["structure_sha256"]
            or model_source["q_grid"] != qe_source["q_grid"]
            or model_source["natoms_primitive"] != qe_source["natoms_primitive"]):
        raise ValueError("Stage1 comparison needs one structure, atom count and q mesh")
    qe = {tuple(row["q_index"]): row for row in qe_dataset["q_points"]}
    model = {tuple(row["q_index"]): row for row in model_dataset["q_points"]}
    if len(qe) != len(qe_dataset["q_points"]) or len(model) != len(model_dataset["q_points"]) or set(qe) != set(model):
        raise ValueError("Stage1 q points differ or are duplicated")
    rows = []
    for key in sorted(qe):
        qe_freq = np.asarray(qe[key]["freqs_thz"], dtype=float)
        ml_freq = np.asarray(model[key]["freqs_thz"], dtype=float)
        assignment, overlaps, groups = compare_modes(
            qe_freq, _vectors(qe[key]), ml_freq, _vectors(model[key]),
            gamma_acoustic=key == (0, 0))
        rows.append({
            "q_index": list(key), "q_frac": qe[key]["q_frac"],
            "qe_frequency_thz": qe_freq.tolist(), "model_frequency_thz": ml_freq.tolist(),
            "qe_to_model_mode_one_based": (assignment + 1).tolist(),
            "matched_frequency_abs_error_thz": np.abs(qe_freq - ml_freq[assignment]).tolist(),
            "single_mode_overlap_squared": overlaps.tolist(),
            "qe_degenerate_subspaces": groups,
        })
    errors = np.asarray([value for row in rows for value in row["matched_frequency_abs_error_thz"]])
    isolated = [group["subspace_overlap"] for row in rows for group in row["qe_degenerate_subspaces"]
                if len(group["qe_modes"]) == 1]
    degenerate = [group["subspace_overlap"] for row in rows for group in row["qe_degenerate_subspaces"]
                  if len(group["qe_modes"]) > 1]
    return {"kind": "v3_mlff_qe_phonon_mesh_comparison", "model_backend": model_source["backend"],
            "qe_geometry_verified": verified,
            "structure_sha256": qe_source["structure_sha256"], "q_count": len(rows),
            "matched_frequency_mae_thz": float(np.mean(errors)),
            "matched_frequency_rmse_thz": float(np.sqrt(np.mean(errors**2))),
            "matched_frequency_p95_abs_error_thz": float(np.percentile(errors, 95)),
            "isolated_mode_count": len(isolated),
            "isolated_mode_median_overlap_squared": float(np.median(isolated)) if isolated else None,
            "degenerate_group_count": len(degenerate),
            "degenerate_group_median_subspace_overlap": float(np.median(degenerate)) if degenerate else None,
            "q_records": rows,
            "note": ("Single branches within near-degenerate QE subspaces are basis dependent. "
                     + ("QE geometry was independently verified. " if verified else
                        "Historical QE geometry was not independently verified; frequency and overlap values are diagnostic only. "))}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-dataset", type=Path, required=True)
    parser.add_argument("--qe-dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-unverified-historical", action="store_true")
    args = parser.parse_args(argv)
    result = compare(json.loads(args.model_dataset.read_text()), json.loads(args.qe_dataset.read_text()),
                     allow_unverified_historical=args.allow_unverified_historical)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
