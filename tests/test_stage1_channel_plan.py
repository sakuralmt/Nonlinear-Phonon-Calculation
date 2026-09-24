"""Stage1 channel plans must reproduce ranking without discarding multiplet members."""

import copy

import numpy as np
import pytest

from mlff_modepair_workflow.prophet_stage1 import (
    equivalent_pair_channels, finite_q_orbits, mode_pairs_from_phonons,
    phonons_from_force_constants,
)
from scripts.select_ranked_mode_pairs import select


def _fixture():
    phi = np.zeros((6, 6, 3, 3, 3, 3))
    for atom in range(3):
        for axis in range(3):
            phi[0, 0, atom, axis, atom, axis] = 1 + 0.1 * atom + axis
    records = phonons_from_force_constants(phi, np.ones(3), 6)
    gamma = next(row for row in records if row["q_index"] == [0, 0])
    gamma["freqs_thz"] = [0, 0, 0, 5, 5.01, 8, 9, 10, 11]
    orbits = finite_q_orbits(6)
    pairs = mode_pairs_from_phonons(records, orbits, 3)
    source = {"structure_sha256": "structure", "symbols": ["Mo", "S", "S"]}
    mode_pairs = {"source": source, "finite_q_orbits": orbits, "pairs": pairs,
                  "equivalent_pair_channels": equivalent_pair_channels(records, orbits, pairs, 3)}
    dataset = {"source": source, "q_points": records}
    ranking = {"signature": {"mode_pairs_sha256": "pairs", "structure_sha256": "structure"},
               "backend": {"backend": "mattersim"},
               "pairs": [{"pair_code": pair["pair_code"],
                          "phi122_mev": (100.0 if pair["gamma_mode"]["mode_number_one_based"] == 5
                                         and pair["q_orbit_number"] == 1
                                         and pair["target_mode"]["mode_number_one_based"] == 1 else 1.0)}
                         for pair in pairs]}
    return mode_pairs, dataset, ranking


def test_selector_consumes_precomputed_channels_and_keeps_strong_second_component():
    mode_pairs, dataset, ranking = _fixture()
    selected = select(mode_pairs, ranking, dataset, pair_sha256="pairs", ranking_sha256="ranking",
                      top_k_channels=1)
    assert selected["selection_meta"]["stage1_channels_precomputed"]
    assert selected["selection_meta"]["available_physical_channels"] == 6 * 9 * 6
    assert selected["selection_meta"]["selected_channels"][0]["gamma_modes_one_based"] == [4, 5]
    assert len(selected["pairs"]) == 2
    assert "equivalent_pair_channels" not in selected


def test_legacy_v3_without_channel_plan_has_identical_selection():
    mode_pairs, dataset, ranking = _fixture()
    full = select(mode_pairs, ranking, dataset, pair_sha256="pairs", ranking_sha256="ranking",
                  top_k_channels=20)
    legacy = copy.deepcopy(mode_pairs)
    del legacy["equivalent_pair_channels"]
    fallback = select(legacy, ranking, dataset, pair_sha256="pairs", ranking_sha256="ranking",
                      top_k_channels=20)
    assert [row["pair_code"] for row in full["pairs"]] == [row["pair_code"] for row in fallback["pairs"]]
    assert not fallback["selection_meta"]["stage1_channels_precomputed"]


def test_tolerance_override_recomputes_without_rerunning_stage2_but_corruption_fails():
    mode_pairs, dataset, ranking = _fixture()
    override = select(mode_pairs, ranking, dataset, pair_sha256="pairs", ranking_sha256="ranking",
                      top_k_channels=1, degeneracy_thz=0.001)
    assert not override["selection_meta"]["stage1_channels_precomputed"]
    assert override["selection_meta"]["gamma_degeneracy_threshold_thz"] == 0.001
    mode_pairs["equivalent_pair_channels"]["channels"][0]["pair_codes"].pop()
    with pytest.raises(ValueError, match="precomputed physical channels"):
        select(mode_pairs, ranking, dataset, pair_sha256="pairs", ranking_sha256="ranking",
               top_k_channels=1)


def test_selector_accepts_one_atom_primitive_without_hardcoded_486():
    phi = np.zeros((6, 6, 1, 3, 1, 3))
    records = phonons_from_force_constants(phi, np.ones(1), 6)
    orbits = finite_q_orbits(6)
    pairs = mode_pairs_from_phonons(records, orbits, 1)
    source = {"structure_sha256": "one-atom", "symbols": ["B"]}
    payload = {"source": source, "finite_q_orbits": orbits, "pairs": pairs,
               "equivalent_pair_channels": equivalent_pair_channels(records, orbits, pairs, 1)}
    dataset = {"source": source, "q_points": records}
    ranking = {"signature": {"mode_pairs_sha256": "pairs", "structure_sha256": "one-atom"},
               "backend": {"backend": "mattersim"},
               "pairs": [{"pair_code": pair["pair_code"], "phi122_mev": 1.0} for pair in pairs]}
    chosen = select(payload, ranking, dataset, pair_sha256="pairs", ranking_sha256="rank",
                    top_k_channels=1)
    assert len(pairs) == 54
    assert chosen["selection_meta"]["available_physical_channels"] == 18
    assert len(chosen["pairs"]) == 3
