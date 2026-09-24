#!/usr/bin/env python3
"""Compare stage2 couplings after quotienting mode phase and Gamma degeneracy."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np


def _complex_mode(record: dict, number: int) -> np.ndarray:
    return np.asarray([[complex(*component) for component in atom]
                       for atom in record["eigenvectors"][number - 1]], dtype=complex).ravel()


def _stats(values: list[float]) -> dict:
    array = np.asarray(values, dtype=float)
    return {"median": float(np.median(array)), "p95": float(np.percentile(array, 95)),
            "max": float(np.max(array))}


def compare(stage1: dict, old_dataset: dict, new_dataset: dict, stage2: dict) -> dict:
    if not stage1["same_structure"] or not stage2["all_486_complete"]:
        raise ValueError("Equivalence audit requires identical geometry and two complete 486-pair PES sets")
    if (old_dataset["source"]["structure_sha256"] != stage1["reference_structure_sha256"]
            or new_dataset["source"]["structure_sha256"] != stage1["candidate_structure_sha256"]):
        raise ValueError("Stage1 comparison does not describe these phonon datasets")
    old_q = {tuple(row["q_index"]): row for row in old_dataset["q_points"]}
    new_q = {tuple(row["q_index"]): row for row in new_dataset["q_points"]}
    if set(old_q) != set(new_q) or len(old_q) != 36:
        raise ValueError("Expected the same complete 6x6 phonon mesh")
    complex_overlaps = []
    for record in stage1["q_records"]:
        index = tuple(record["q_index"])
        if index == (0, 0):
            continue
        if record["reference_to_candidate_modes_one_based"] != list(range(1, 10)):
            raise ValueError(f"Finite-q branch permutation needs explicit pair-code remapping at {index}")
        for number in range(1, 10):
            u, v = _complex_mode(old_q[index], number), _complex_mode(new_q[index], number)
            complex_overlaps.append(float(abs(np.vdot(u, v)) / np.linalg.norm(u) / np.linalg.norm(v)))
    if min(complex_overlaps) < 0.99:
        raise ValueError("At least one finite-q mode differs by more than a phase")
    groups = stage1["q_records"][0]["reference_degenerate_subspaces"]
    if sorted(number for group in groups for number in group["reference_modes_one_based"]) != list(range(1, 10)):
        raise ValueError("Gamma subspaces do not partition all nine modes")
    if any(group["subspace_overlap"] < 0.99 for group in groups):
        raise ValueError("Gamma subspace mapping is not reliable")
    pairs = {}
    for row in stage2["pairs"]:
        gamma, target = row["pair_code"].split("__", 1)
        number = int(gamma.rsplit("_m", 1)[1])
        pairs[(number, target)] = row
    targets = sorted({target for _, target in pairs})
    if len(targets) != 54:
        raise ValueError("Expected 54 q-mode channels across six orbit representatives")
    channels = []
    for target in targets:
        for group in groups:
            old_numbers = group["reference_modes_one_based"]
            new_numbers = group["candidate_modes_one_based"]
            old_rows = [pairs[(number, target)] for number in old_numbers]
            new_rows = [pairs[(number, target)] for number in new_numbers]
            old_norm = float(np.linalg.norm([row["phi122_old"] for row in old_rows]))
            new_norm = float(np.linalg.norm([row["phi122_new_aligned"] for row in new_rows]))
            old_trace = float(sum(row["phi1122_old"] for row in old_rows))
            new_trace = float(sum(row["phi1122_new_aligned"] for row in new_rows))
            channels.append({
                "gamma_modes_old": old_numbers, "gamma_modes_new": new_numbers,
                "q_mode_code": target,
                "phi122_vector_norm_old": old_norm, "phi122_vector_norm_new": new_norm,
                "phi122_vector_norm_abs_difference": abs(old_norm - new_norm),
                "phi1122_gamma_trace_old": old_trace, "phi1122_gamma_trace_new": new_trace,
                "phi1122_gamma_trace_abs_difference": abs(old_trace - new_trace),
            })
    old_rank = sorted(channels, key=lambda row: (-row["phi122_vector_norm_old"],
                                                  row["q_mode_code"], row["gamma_modes_old"]))
    new_rank = sorted(channels, key=lambda row: (-row["phi122_vector_norm_new"],
                                                  row["q_mode_code"], row["gamma_modes_old"]))
    key = lambda row: (tuple(row["gamma_modes_old"]), row["q_mode_code"])
    return {
        "kind": "equivalence_aware_prophet_phonopy_stage2_comparison",
        "q_mode_complex_overlap_min": min(complex_overlaps),
        "gamma_subspace_overlap_min": min(group["subspace_overlap"] for group in groups),
        "physical_channels": len(channels),
        "phi122_vector_norm_abs_difference": _stats([row["phi122_vector_norm_abs_difference"] for row in channels]),
        "phi1122_gamma_trace_abs_difference": _stats([row["phi1122_gamma_trace_abs_difference"] for row in channels]),
        "top_k_overlap": {str(k): len({key(row) for row in old_rank[:k]} &
                                    {key(row) for row in new_rank[:k]}) for k in (5, 10, 20, 30)},
        "channels": channels,
        "note": "At finite q, the complex eigenvector is compared up to global phase. "
                "At Gamma, near-degenerate modes are compared as subspaces: the Euclidean norm of "
                "Phi_iqq and the trace of Phi_iiqq are invariant to orthogonal basis rotation. "
                "These are MLFF-to-MLFF workflow comparisons, not DFT accuracy estimates.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage1-comparison", type=Path, required=True)
    parser.add_argument("--old-dataset", type=Path, required=True)
    parser.add_argument("--new-dataset", type=Path, required=True)
    parser.add_argument("--stage2-comparison", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compare(json.loads(args.stage1_comparison.read_text()),
                     json.loads(args.old_dataset.read_text()),
                     json.loads(args.new_dataset.read_text()),
                     json.loads(args.stage2_comparison.read_text()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "channels"}, indent=2))


if __name__ == "__main__":
    main()
