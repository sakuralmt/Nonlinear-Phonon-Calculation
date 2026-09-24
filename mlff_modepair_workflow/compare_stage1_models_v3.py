"""Compare two v3 MLFF phonon meshes, with explicit geometry matching."""

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


def compare(reference: dict, candidate: dict, *,
            allow_different_structures: bool = False) -> dict:
    for name, dataset in (("reference", reference), ("candidate", candidate)):
        if (dataset.get("version") != CONTRACT_VERSION
                or dataset["source"].get("normalization_version") != NORMALIZATION_VERSION):
            raise ValueError(f"{name} Stage1 is not v3 with unit-mass real modes")
    left, right = reference["source"], candidate["source"]
    if (left["q_grid"] != right["q_grid"]
            or left["natoms_primitive"] != right["natoms_primitive"]
            or left["symbols"] != right["symbols"]
            or not np.allclose(left["masses_amu"], right["masses_amu"], atol=1e-8)):
        raise ValueError("Model Stage1 comparison needs identical q mesh, atom order and masses")
    same_structure = left["structure_sha256"] == right["structure_sha256"]
    if not same_structure and not allow_different_structures:
        raise ValueError("Stage1 structures differ; opt in only for a descriptive geometry-sensitive comparison")
    refs = {tuple(row["q_index"]): row for row in reference["q_points"]}
    cands = {tuple(row["q_index"]): row for row in candidate["q_points"]}
    if (len(refs) != len(reference["q_points"]) or len(cands) != len(candidate["q_points"])
            or set(refs) != set(cands)):
        raise ValueError("Stage1 q points differ or are duplicated")
    rows = []
    for key in sorted(refs):
        a = np.asarray(refs[key]["freqs_thz"], dtype=float)
        b = np.asarray(cands[key]["freqs_thz"], dtype=float)
        assignment, overlaps, groups = compare_modes(
            a, _vectors(refs[key]), b, _vectors(cands[key]), gamma_acoustic=key == (0, 0)
        )
        groups = [{"reference_modes_one_based": group["qe_modes"],
                   "candidate_modes_one_based": group["ml_modes"],
                   "subspace_overlap": group["subspace_overlap"]} for group in groups]
        rows.append({
            "q_index": list(key), "reference_frequencies_thz": a.tolist(),
            "candidate_frequencies_thz": b.tolist(),
            "reference_to_candidate_modes_one_based": (assignment + 1).tolist(),
            "matched_frequency_abs_error_thz": np.abs(a - b[assignment]).tolist(),
            "single_mode_overlap_squared": overlaps.tolist(),
            "reference_degenerate_subspaces": groups,
        })
    errors = np.asarray([value for row in rows for value in row["matched_frequency_abs_error_thz"]])
    isolated = [group["subspace_overlap"] for row in rows for group in row["reference_degenerate_subspaces"]
                if len(group["reference_modes_one_based"]) == 1]
    degenerate = [group["subspace_overlap"] for row in rows for group in row["reference_degenerate_subspaces"]
                  if len(group["reference_modes_one_based"]) > 1]
    return {
        "kind": "v3_mlff_stage1_model_comparison",
        "reference_backend": left["backend"], "candidate_backend": right["backend"],
        "reference_structure_sha256": left["structure_sha256"],
        "candidate_structure_sha256": right["structure_sha256"],
        "same_structure": same_structure,
        "comparison_status": "fixed_geometry_model_difference" if same_structure else
        "descriptive_model_plus_geometry_difference",
        "q_count": len(rows),
        "matched_frequency_mae_thz": float(np.mean(errors)),
        "matched_frequency_rmse_thz": float(np.sqrt(np.mean(errors**2))),
        "isolated_mode_median_overlap_squared": float(np.median(isolated)) if isolated else None,
        "degenerate_group_median_subspace_overlap": float(np.median(degenerate)) if degenerate else None,
        "q_records": rows,
        "note": ("Mode labels within near-degenerate groups depend on basis choice. "
                 + ("Both models use the same geometry. " if same_structure else
                    "Both the model and relaxed geometry differ; these metrics do not isolate model accuracy. ")),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-dataset", type=Path, required=True)
    parser.add_argument("--candidate-dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-different-structures", action="store_true")
    args = parser.parse_args(argv)
    result = compare(json.loads(args.reference_dataset.read_text()),
                     json.loads(args.candidate_dataset.read_text()),
                     allow_different_structures=args.allow_different_structures)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
