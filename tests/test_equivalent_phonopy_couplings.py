"""Phase-equivalent phonons need not have matching real standing waves."""

import numpy as np
import pytest

from scripts.compare_equivalent_phonopy_couplings import compare


def _mode_vectors(phase: complex) -> list:
    vectors = []
    for column in np.eye(9, dtype=complex).T:
        vector = (phase * column).reshape(3, 3)
        vectors.append([[[float(value.real), float(value.imag)] for value in atom]
                        for atom in vector])
    return vectors


def test_complex_phase_equivalence_without_real_displacement_overlap():
    groups = [[1, 2, 3], [4, 5], [6, 7], [8], [9]]
    stage1 = {
        "same_structure": True,
        "reference_structure_sha256": "same",
        "candidate_structure_sha256": "same",
        "q_records": [
            {"q_index": [i, j],
             "reference_to_candidate_modes_one_based": list(range(1, 10)),
             "reference_degenerate_subspaces": [
                 {"reference_modes_one_based": group,
                  "candidate_modes_one_based": group,
                  "subspace_overlap": 1.0} for group in groups]}
            for i in range(6) for j in range(6)
        ],
    }
    source = {"structure_sha256": "same"}
    old = {"source": source, "q_points": [
        {"q_index": [i, j], "eigenvectors": _mode_vectors(1.0)}
        for i in range(6) for j in range(6)
    ]}
    new = {"source": source, "q_points": [
        {"q_index": [i, j],
         "eigenvectors": _mode_vectors(1.0 if (i, j) == (0, 0) else 1j)}
        for i in range(6) for j in range(6)
    ]}
    pairs = []
    for target in range(54):
        for gamma in range(1, 10):
            value = float(target + gamma / 10)
            pairs.append({"pair_code": f"Gamma_p0_m{gamma}__target_{target}",
                          "phi122_old": value, "phi122_new_aligned": value,
                          "phi1122_old": value, "phi1122_new_aligned": value})
    result = compare(stage1, old, new, {"all_486_complete": True, "pairs": pairs})
    assert result["q_mode_complex_overlap_min"] == pytest.approx(1)
    assert result["physical_channels"] == 270
    assert result["top_k_overlap"]["30"] == 30
    assert result["phi122_vector_norm_abs_difference"]["max"] == pytest.approx(0)
