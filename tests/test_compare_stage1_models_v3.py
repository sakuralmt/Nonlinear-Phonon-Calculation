import copy

import numpy as np
import pytest

from mlff_modepair_workflow.compare_stage1_models_v3 import compare
from mlff_modepair_workflow.prophet_stage1 import phonons_from_force_constants
from mlff_modepair_workflow.units import NORMALIZATION_VERSION


def _dataset(backend):
    phi = np.zeros((2, 2, 1, 3, 1, 3))
    phi[0, 0, 0, :, 0, :] = np.diag([1.0, 2.0, 3.0])
    return {
        "version": 3,
        "source": {"backend": backend, "normalization_version": NORMALIZATION_VERSION,
                   "structure_sha256": "same", "q_grid": [2, 2, 1],
                   "natoms_primitive": 1, "symbols": ["H"], "masses_amu": [1.008]},
        "q_points": phonons_from_force_constants(phi, np.array([1.008]), 2),
    }


def test_fixed_structure_model_mesh_comparison():
    result = compare(_dataset("prophet"), _dataset("equiformer-v3-oam"))
    assert result["same_structure"] is True
    assert result["q_count"] == 4
    assert result["matched_frequency_mae_thz"] == pytest.approx(0)
    assert result["isolated_mode_median_overlap_squared"] == pytest.approx(1)


def test_different_structures_require_descriptive_opt_in():
    candidate = copy.deepcopy(_dataset("equiformer-v3-oam"))
    candidate["source"]["structure_sha256"] = "different"
    with pytest.raises(ValueError, match="structures differ"):
        compare(_dataset("prophet"), candidate)
    result = compare(_dataset("prophet"), candidate, allow_different_structures=True)
    assert result["same_structure"] is False
    assert result["comparison_status"] == "descriptive_model_plus_geometry_difference"
