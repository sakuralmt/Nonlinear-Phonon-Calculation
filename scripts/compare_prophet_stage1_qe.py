"""Read-only Stage1 comparison with archived QE modes on the same q mesh."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mlff_modepair_workflow.phonon_eigenvectors import compare_modes, read_matdyn_q_mesh, read_qe_eigenvectors
from mlff_modepair_workflow.prophet_backend import sha256_file


def compare(dataset: dict, matdyn_input: Path, qe_eig: Path, old_result: dict | None = None):
    q_list = read_matdyn_q_mesh(matdyn_input)
    natoms = dataset["source"]["natoms_primitive"]
    qe_freq, qe_vec = read_qe_eigenvectors(qe_eig, natoms, q_list)
    mesh_n = dataset["source"]["q_grid"][0]
    by_q = {tuple(row["q_index"]): row for row in dataset["q_points"]}
    old_by_q = {} if old_result is None else {
        tuple(int(round(value * mesh_n)) % mesh_n for value in row["q_frac"][:2]): row
        for row in old_result["q_records"]
    }
    records = []
    for iq, q in enumerate(q_list):
        key = tuple(int(round(value * mesh_n)) % mesh_n for value in q[:2])
        if key not in by_q:
            raise ValueError(f"QE q point is missing from Stage1: {q}")
        row = by_q[key]
        ml_freq = np.asarray(row["freqs_thz"], dtype=float)
        ml_vec = np.column_stack([
            np.array([[complex(*axis) for axis in atom] for atom in mode], dtype=complex).reshape(-1)
            for mode in row["eigenvectors"]
        ])
        assignment, overlaps, groups = compare_modes(
            qe_freq[iq], qe_vec[iq], ml_freq, ml_vec, gamma_acoustic=key == (0, 0)
        )
        old_deviation = None
        if key in old_by_q:
            old_deviation = float(np.max(np.abs(ml_freq - np.asarray(old_by_q[key]["ml_freq_thz"]))))
        records.append({
            "q_index": list(key), "q_frac": q.tolist(),
            "qe_frequency_thz": qe_freq[iq].tolist(),
            "prophet_frequency_thz": ml_freq.tolist(),
            "qe_to_prophet_mode_one_based": (assignment + 1).tolist(),
            "matched_frequency_abs_error_thz": np.abs(qe_freq[iq] - ml_freq[assignment]).tolist(),
            "isolated_overlap_squared": overlaps.tolist(),
            "degenerate_subspaces": groups,
            "max_old_prophet_frequency_change_thz": old_deviation,
        })
    if len(records) != mesh_n * mesh_n:
        raise ValueError("QE comparison must cover the complete q mesh")
    isolated = [group["subspace_overlap"] for row in records for group in row["degenerate_subspaces"] if len(group["qe_modes"]) == 1]
    subspaces = [group["subspace_overlap"] for row in records for group in row["degenerate_subspaces"] if len(group["qe_modes"]) > 1]
    deltas = [row["max_old_prophet_frequency_change_thz"] for row in records if row["max_old_prophet_frequency_change_thz"] is not None]
    frequency_errors = [error for row in records for error in row["matched_frequency_abs_error_thz"]]
    return {
        "kind": "prophet_stage1_archived_qe_comparison",
        "structure_sha256": dataset["source"]["structure_sha256"],
        "matdyn_input_sha256": sha256_file(matdyn_input),
        "qe_eig_sha256": sha256_file(qe_eig),
        "q_count": len(records),
        "isolated_mode_count": len(isolated),
        "isolated_mode_median_overlap_squared": float(np.median(isolated)),
        "degenerate_group_count": len(subspaces),
        "degenerate_group_median_overlap_squared": float(np.median(subspaces)) if subspaces else None,
        "matched_frequency_mae_thz": float(np.mean(frequency_errors)),
        "matched_frequency_median_abs_error_thz": float(np.median(frequency_errors)),
        "matched_frequency_p95_abs_error_thz": float(np.percentile(frequency_errors, 95)),
        "max_old_prophet_frequency_change_thz": max(deltas) if deltas else None,
        "q_records": records,
        "note": "QE modes are an offline reference; the Stage1 output modes remain Prophet-derived."
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phonon-dataset", type=Path, required=True)
    parser.add_argument("--matdyn-input", type=Path, required=True)
    parser.add_argument("--qe-eig", type=Path, required=True)
    parser.add_argument("--old-prophet-result", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    dataset = json.loads(args.phonon_dataset.read_text())
    prior = None if args.old_prophet_result is None else json.loads(args.old_prophet_result.read_text())
    result = compare(dataset, args.matdyn_input, args.qe_eig, prior)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
