"""Independent structure-symmetry and staged PES regression tests."""

import json

import numpy as np
import pytest
import spglib
from ase import Atoms
from ase.calculators.calculator import Calculator, all_changes

from mlff_modepair_workflow.prophet_stage1 import (
    equivalent_pair_channels,
    gamma_mode_partition,
    mode_pairs_from_phonons,
    phonons_from_force_constants,
)
from mlff_modepair_workflow.screening_stage2 import (
    CENTER,
    FULL,
    SIX,
    _calculate,
    _key,
    _proxy,
    finalize,
    validate_relaxed_stage1,
)
from mlff_modepair_workflow.prophet_backend import sha256_file
from mlff_modepair_workflow.structure_symmetry import (
    Operation,
    _mapping,
    classify_hex_qpoint,
    structure_q_orbits,
)


def test_symmetry_coordinate_signs_share_one_zero_checkpoint():
    points = {_key(x, y): x + y * y for x, y in SIX}
    for x, y in SIX:
        assert points[_key(-x, -y)] == -x + y * y


def test_public_stage1_forwards_gamma_threshold(monkeypatch, tmp_path):
    from nonlinear_phonon_calculation.cli import main
    from mlff_modepair_workflow import advanced_stage1

    received = {}

    def fake_stage1(*args, **kwargs):
        received.update(kwargs)
        return (tmp_path / "modes.json",)

    monkeypatch.setattr(advanced_stage1, "run_advanced_stage1", fake_stage1)
    assert (
        main(
            [
                "stage1",
                "--structure",
                "input.in",
                "--checkpoint",
                "weights.pt",
                "--source-root",
                "source",
                "--output-dir",
                str(tmp_path),
                "--gamma-degeneracy-thz",
                "0.02",
            ]
        )
        == 0
    )
    assert received["gamma_degeneracy_thz"] == 0.02


def test_completed_stage1_cannot_be_overwritten(tmp_path):
    from mlff_modepair_workflow.advanced_stage1 import run_advanced_stage1

    (tmp_path / "phonon_dataset.json").write_text("{}")
    with pytest.raises(ValueError, match="fresh output directory"):
        run_advanced_stage1(
            tmp_path / "input.in",
            tmp_path / "weights.pt",
            tmp_path / "source",
            "tece-oam-rra-1.0",
            tmp_path,
            gamma_degeneracy_thz=0.02,
        )


def test_mode_mapping_requires_frequency_agreement_after_assignment():
    vectors = np.eye(3).reshape(3, 1, 3)
    encoded = np.stack([vectors, np.zeros_like(vectors)], axis=-1).tolist()
    left = {
        "q_frac": [0, 0, 0],
        "freqs_thz": [1, 2, 3],
        "eigenvectors": encoded,
        "degenerate_groups_one_based": [[1], [2], [3]],
    }
    right = {**left, "eigenvectors": [encoded[1], encoded[0], encoded[2]]}
    identity = Operation(
        np.eye(3, dtype=int), np.zeros(3), np.array([0]), np.zeros((1, 3)), np.eye(3)
    )
    assert _mapping(left, right, identity, False, 0.9, 0.01) is None


@pytest.mark.parametrize("model", ["tece-oam-rra-1.0", "equiformer-v3-oam"])
def test_advanced_weight_hash_is_checked_before_model_import(tmp_path, model):
    from mlff_modepair_workflow.advanced_stage1 import make_advanced_calculator

    checkpoint = tmp_path / "incorrect.pt"
    checkpoint.write_bytes(b"not the pinned model")
    with pytest.raises(ValueError, match="checkpoint SHA256 mismatch"):
        make_advanced_calculator(model, checkpoint, "cpu", tmp_path / "missing-source")


def _hex_atoms():
    a = 3.2
    cell = [[a, 0, 0], [-a / 2, a * np.sqrt(3) / 2, 0], [0, 0, 18]]
    return Atoms(
        "MoS2",
        scaled_positions=[[0, 0, 0.5], [1 / 3, 2 / 3, 0.58], [1 / 3, 2 / 3, 0.42]],
        cell=cell,
        pbc=True,
    )


def _records(mesh, nat):
    phi = np.zeros((mesh, mesh, nat, 3, nat, 3))
    for atom in range(nat):
        phi[0, 0, atom, :, atom, :] = np.eye(3)
    return phonons_from_force_constants(phi, np.ones(nat), mesh)


def _translation_separated_records(mesh, nat):
    phi = np.zeros((mesh, mesh, nat, 3, nat, 3))
    for atom in range(nat):
        for other in range(nat):
            phi[0, 0, atom, :, other, :] = (
                (nat - 1) if atom == other else -1
            ) * np.eye(3)
    return phonons_from_force_constants(phi, np.ones(nat), mesh)


def test_actual_atomic_motif_controls_hexagonal_orbit_reduction():
    atoms = _hex_atoms()
    records = _records(6, len(atoms))
    orbits, meta = structure_q_orbits(atoms, records, 6)
    assert meta["q_orbit_count"] == 6
    assert sum(orbit["size"] for orbit in orbits) == 35
    physical = _translation_separated_records(6, len(atoms))
    assert len(mode_pairs_from_phonons(physical, orbits, np.ones(len(atoms)))) == 324
    for orbit in orbits:
        assert set(map(tuple, orbit["members_index"])) == {
            tuple(map(int, key.split(","))) for key in orbit["mode_maps"]
        }
    broken = atoms.copy()
    broken.positions[1, 0] += 0.1
    low, diagnostic = structure_q_orbits(broken, records, 6)
    assert diagnostic["q_orbit_count"] == 19
    assert sum(row["size"] for row in low) == 35
    assert len(mode_pairs_from_phonons(physical, low, np.ones(len(atoms)))) == 19 * 54
    for structure, expected in ((atoms, 6), (broken, 19)):
        cell = (
            structure.cell.array,
            structure.get_scaled_positions(),
            structure.get_atomic_numbers(),
        )
        mapping, addresses = spglib.get_ir_reciprocal_mesh(
            [6, 6, 1], cell, is_time_reversal=True, symprec=1e-3
        )
        assert len(addresses) == 36
        assert len(set(mapping)) - 1 == expected


def test_sixty_degree_cell_representation_preserves_candidate_coverage():
    atoms = _hex_atoms()
    alternative = atoms.copy()
    cell = alternative.cell.array.copy()
    cell[1] += cell[0]
    alternative.set_cell(cell, scale_atoms=False)
    assert classify_hex_qpoint(np.array([1 / 3, 1 / 3, 0]), atoms.cell.array) == "K"
    assert classify_hex_qpoint(np.array([1 / 3, 2 / 3, 0]), atoms.cell.array) == "line"
    assert (
        classify_hex_qpoint(np.array([1 / 3, 2 / 3, 0]), alternative.cell.array) == "K"
    )
    assert (
        classify_hex_qpoint(np.array([1 / 3, 1 / 3, 0]), alternative.cell.array)
        == "line"
    )
    records = _records(6, len(atoms))
    for structure in (atoms, alternative):
        orbits, _ = structure_q_orbits(structure, records, 6)
        assert len(orbits) == 6
        assert sorted(
            point for orbit in orbits for point in map(tuple, orbit["members_index"])
        ) == [(i, j) for i in range(6) for j in range(6) if (i, j) != (0, 0)]


@pytest.mark.parametrize("formula", ["H", "BN", "MoSSe"])
def test_one_two_three_atom_hexagonal_motifs_match_spglib_oracle(formula):
    reference = _hex_atoms()
    positions = {
        "H": [[0, 0, 0.5]],
        "BN": [[0, 0, 0.5], [1 / 3, 2 / 3, 0.5]],
        "MoSSe": reference.get_scaled_positions(),
    }[formula]
    atoms = Atoms(formula, scaled_positions=positions, cell=reference.cell, pbc=True)
    records = _records(6, len(atoms))
    orbits, diagnostic = structure_q_orbits(atoms, records, 6)
    mapping, _ = spglib.get_ir_reciprocal_mesh(
        [6, 6, 1],
        (atoms.cell.array, atoms.get_scaled_positions(), atoms.numbers),
        is_time_reversal=True,
        symprec=1e-3,
    )
    assert diagnostic["q_orbit_count"] == len(set(mapping)) - 1
    assert sum(row["size"] for row in orbits) == 35
    physical = _translation_separated_records(6, len(atoms))
    assert len(mode_pairs_from_phonons(physical, orbits, np.ones(len(atoms)))) == len(
        orbits
    ) * (3 * len(atoms)) * (3 * len(atoms) - 3)


def test_gamma_acoustic_modes_are_identified_by_translation_not_index():
    records = _translation_separated_records(2, 2)
    gamma = records[0]
    permutation = [3, 0, 1, 2, 4, 5]
    gamma["freqs_thz"] = [gamma["freqs_thz"][i] for i in permutation]
    gamma["eigenvectors"] = [gamma["eigenvectors"][i] for i in permutation]
    selection = gamma_mode_partition(records, np.ones(2))
    assert selection["acoustic_modes_one_based"] == [2, 3, 4]
    assert selection["optical_modes_one_based"] == [1, 5, 6]


def test_six_point_derivative_matches_analytic_potential():
    points = {}
    for x, y in SIX:
        points[f"{x:+.1f},{y:+.1f}"] = 1.2 + 0.01 * x * y**2
    assert _proxy(points) == pytest.approx(20.0)


def test_central_fit_recovers_analytic_third_and_fourth_derivatives():
    from mlff_modepair_workflow.core import analyze_pair_grid
    from mlff_modepair_workflow.units import RY_TO_EV, energies_to_ev

    axis = np.linspace(-2, 2, 9)
    x, y = np.meshgrid(axis, axis)
    energy = (
        -321
        + 0.2 * x**2
        + 0.3 * y**2
        + 0.01 * x * y**2
        - 0.02 * x**2 * y
        + 0.007 * x**2 * y**2
        + 0.04 * x**4
        + 0.03 * y**4
    )
    pair = {"gamma_mode": {"freq_thz": 1}, "target_mode": {"freq_thz": 2}}
    fit = analyze_pair_grid(pair, energies_to_ev(energy / RY_TO_EV, "Ry"), axis, axis)
    assert fit["fit_design_rank"] == 13
    assert fit["center_fit_rmse_ev_supercell"] < 1e-12
    assert fit["physics"]["phi_122_mev_per_A3amu32"] == pytest.approx(20, abs=1e-8)
    assert fit["physics"]["phi_112_mev_per_A3amu32"] == pytest.approx(-40, abs=1e-8)
    assert fit["physics"]["phi_1122_mev_per_A4amu2"] == pytest.approx(28, abs=1e-8)
    with pytest.raises(ValueError, match="Unsupported or undeclared"):
        energies_to_ev(energy, "guess")


class HarmonicEnergy(Calculator):
    implemented_properties = ["energy"]

    def calculate(self, atoms=None, properties=("energy",), system_changes=all_changes):
        super().calculate(atoms, properties, system_changes)
        self.results["energy"] = float(np.sum(np.sin(atoms.positions) ** 2))


class InterruptedEnergy(HarmonicEnergy):
    def __init__(self):
        super().__init__()
        self.calls = 0

    def calculate(self, *args, **kwargs):
        self.calls += 1
        if self.calls == 4:
            raise RuntimeError("injected interruption")
        super().calculate(*args, **kwargs)


def test_screen_refine_audit_are_checkpointed_and_complete(tmp_path):
    atoms = Atoms(
        "BN",
        scaled_positions=[[0, 0, 0.5], [1 / 3, 2 / 3, 0.5]],
        cell=[[2, 0, 0], [-1, np.sqrt(3), 0], [0, 0, 12]],
        pbc=True,
    )
    orbits, _ = structure_q_orbits(atoms, _records(2, 2), 2)
    records = _translation_separated_records(2, 2)
    pairs = mode_pairs_from_phonons(records, orbits, np.ones(2))
    payload = {
        "pairs": pairs,
        "equivalent_pair_channels": equivalent_pair_channels(
            records, orbits, pairs, np.ones(2)
        ),
    }
    identity = {"fixture": "one"}
    root = tmp_path / "run"
    with pytest.raises(RuntimeError, match="injected interruption"):
        _calculate(pairs[0], atoms, InterruptedEnergy(), root, identity, SIX)
    assert _calculate(pairs[0], atoms, HarmonicEnergy(), root, identity, SIX) == 3
    for pair in pairs:
        expected = 0 if pair is pairs[0] else 6
        assert (
            _calculate(pair, atoms, HarmonicEnergy(), root, identity, SIX) == expected
        )
        assert _calculate(pair, atoms, HarmonicEnergy(), root, identity, SIX) == 0
    screen = finalize(root, payload, identity, "screen", 1)
    assert json.loads(screen.read_text())["pair_count"] == len(pairs)
    selected = (
        json.loads((root / "selection.json").read_text())
        if (root / "selection.json").exists()
        else None
    )
    assert selected is None
    from mlff_modepair_workflow.screening_stage2 import _selection

    selected = _selection(root, payload, identity, 1)
    selected_pairs = [
        pair for pair in pairs if pair["pair_code"] in selected["pair_codes"]
    ]
    for pair in selected_pairs:
        assert _calculate(pair, atoms, HarmonicEnergy(), root, identity, CENTER) == 19
    refined = json.loads(finalize(root, payload, identity, "refine", 1).read_text())
    assert refined["pair_count"] == len(selected_pairs)
    assert all(row["analysis"]["fit_design_rank"] == 13 for row in refined["pairs"])
    for pair in selected_pairs:
        assert _calculate(pair, atoms, HarmonicEnergy(), root, identity, FULL) == 56
    audited = json.loads(finalize(root, payload, identity, "audit", 1).read_text())
    assert audited["pair_count"] == len(selected_pairs)
    assert all(
        row["analysis"]["energy_resolution"]["point_count"] == 81
        and set(row["analysis"]["window_sensitivity"]) == {"1.0", "1.5", "2.0"}
        for row in audited["pairs"]
    )


def test_stage2_rejects_shared_or_mismatched_relaxation(tmp_path):
    structure = tmp_path / "optimized_structure.scf.inp"
    structure.write_text("model structure\n")
    summary_path = tmp_path / "relax_summary.json"
    digest = sha256_file(structure)
    summary = {
        "backend": "prophet",
        "optimized_structure_sha256": digest,
        "source_structure_sha256": "initial-hash",
        "relaxation_protocol_version": "test-protocol",
    }
    summary_path.write_text(json.dumps(summary))
    payload = {
        "selection": "gamma_optical_and_momentum_conservation_only",
        "finite_q_orbits": [{}],
        "pairs": [
            {"gamma_mode": {"mode_number_one_based": gamma}}
            for gamma in (4, 5, 6)
            for _ in range(6)
        ],
        "source": {
            "backend": "prophet",
            "geometry_source": "model_relaxed",
            "symmetry": {"covariance_contract": "gamma_and_finite_q_v2"},
            "structure_sha256": digest,
            "natoms_primitive": 2,
            "gamma_mode_selection": {
                "acoustic_modes_one_based": [1, 2, 3],
                "optical_modes_one_based": [4, 5, 6],
            },
            "relaxation": {
                "optimized_structure_sha256": digest,
                "source_structure_sha256": "initial-hash",
                "protocol_version": "test-protocol",
                "summary": str(summary_path),
                "summary_sha256": sha256_file(summary_path),
            },
        },
    }
    validate_relaxed_stage1(payload, structure)
    payload["source"]["symmetry"].pop("covariance_contract")
    with pytest.raises(ValueError, match="Gamma and finite-q"):
        validate_relaxed_stage1(payload, structure)
    payload["source"]["symmetry"]["covariance_contract"] = "gamma_and_finite_q_v2"
    payload["pairs"][0]["gamma_mode"]["mode_number_one_based"] = 1
    with pytest.raises(ValueError, match="Gamma optical"):
        validate_relaxed_stage1(payload, structure)
    payload["pairs"][0]["gamma_mode"]["mode_number_one_based"] = 4
    moved = tmp_path / "moved"
    moved.mkdir()
    (moved / structure.name).write_bytes(structure.read_bytes())
    (moved / summary_path.name).write_bytes(summary_path.read_bytes())
    summary_path.unlink()
    validate_relaxed_stage1(payload, moved / structure.name)
    summary_path.write_text(json.dumps(summary))
    payload["source"]["geometry_source"] = "shared_dft"
    with pytest.raises(ValueError, match="model-relaxed"):
        validate_relaxed_stage1(payload, structure)
    payload["source"]["geometry_source"] = "model_relaxed"
    summary_path.write_text("{}")
    with pytest.raises(ValueError, match="relaxation summary"):
        validate_relaxed_stage1(payload, structure)
