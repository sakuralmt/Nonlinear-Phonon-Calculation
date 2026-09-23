import numpy as np
import pytest
from ase import Atoms
from ase.calculators.calculator import Calculator, all_changes

from mlff_modepair_workflow.core import ModePairFrozenPhononBuilder
from mlff_modepair_workflow.phonon_eigenvectors import (
    compare_modes,
    dynamical_matrix,
    real_space_force_constants,
)


def _mode_pair(q, natoms=1):
    vec = [[[1.0 / np.sqrt(natoms), 0.0], [0.0, 0.0], [0.0, 0.0]] for _ in range(natoms)]
    return {
        "pair_code": "test",
        "gamma_mode": {"eigenvector": vec},
        "target_mode": {"eigenvector_q": vec, "q_frac": q},
    }


@pytest.mark.parametrize("symbols", ["H", "BN", "MoS2"])
def test_real_frozen_mode_is_unit_mass_weighted_at_gamma_m_and_k(symbols):
    primitive = Atoms(symbols, positions=[[i * 0.5, 0, 0] for i in range(len(Atoms(symbols)))], cell=np.diag([3.0, 3.0, 15.0]), pbc=True)
    for q, expected_factor in [([0, 0, 0], 1.0), ([0.5, 0, 0], 1.0), ([1 / 3, 1 / 3, 0], np.sqrt(2))]:
        builder = ModePairFrozenPhononBuilder(_mode_pair(q, len(primitive)), primitive)
        displacement = builder.displacement_cart(0, 1)
        mass_weighted_norm = np.linalg.norm(displacement * builder.mass_sqrt)
        assert np.isclose(mass_weighted_norm, 1.0)
        assert np.isclose(builder.q_amplitude_factor, expected_factor)


def test_purely_imaginary_self_conjugate_mode_is_rephased():
    primitive = Atoms("BN", positions=[[0, 0, 0], [0.5, 0, 0]], cell=np.diag([3.0, 3.0, 15.0]), pbc=True)
    pair = _mode_pair([0.5, 0, 0], 2)
    for vector in pair["target_mode"]["eigenvector_q"]:
        vector[0] = [0.0, 1 / np.sqrt(2)]
    builder = ModePairFrozenPhononBuilder(pair, primitive)
    assert np.isclose(np.linalg.norm(builder.displacement_cart(0, 1) * builder.mass_sqrt), 1.0)


class _ToySprings(Calculator):
    implemented_properties = ["forces"]

    def __init__(self, equilibrium, mesh_n, k=4.0):
        super().__init__()
        self.equilibrium = equilibrium
        self.mesh_n = mesh_n
        self.k = k

    def calculate(self, atoms=None, properties=("forces",), system_changes=all_changes):
        super().calculate(atoms, properties, system_changes)
        u = (atoms.get_positions() - self.equilibrium).reshape(self.mesh_n, self.mesh_n, 3)
        forces = -self.k * u
        forces[:, :, 0] = -self.k * (2 * u[:, :, 0] - np.roll(u[:, :, 0], 1, axis=0) - np.roll(u[:, :, 0], -1, axis=0))
        self.results["forces"] = forces.reshape(-1, 3)


def test_force_constants_fourier_transform_on_whole_mesh():
    primitive = Atoms("H", positions=[[0, 0, 0]], cell=np.diag([3.0, 3.0, 15.0]), pbc=True)
    mesh_n = 6
    from ase.build import make_supercell

    equilibrium = make_supercell(primitive, np.diag([mesh_n, mesh_n, 1])).get_positions()
    phi = real_space_force_constants(primitive, _ToySprings(equilibrium, mesh_n), mesh_n, 0.01)
    for iq in range(mesh_n):
        matrix, error = dynamical_matrix(phi, primitive.get_masses(), np.array([iq / mesh_n, 0, 0]))
        assert error < 1e-12
        expected = np.diag([2 * 4 * (1 - np.cos(2 * np.pi * iq / mesh_n)), 4, 4]) / primitive.get_masses()[0]
        assert np.allclose(matrix, expected, atol=1e-9)


def test_degenerate_modes_are_compared_as_subspaces():
    qe = np.eye(3)
    ml = np.eye(3)
    ml[:, :2] = np.array([[1, 1], [1, -1], [0, 0]]) / np.sqrt(2)
    assignment, individual, groups = compare_modes(np.array([1, 1, 5]), qe, np.array([1, 1, 5]), ml)
    assert np.allclose(individual[:2], 0.5)
    assert groups[0]["qe_modes"] == [1, 2]
    assert np.isclose(groups[0]["subspace_overlap"], 1)


def test_gamma_acoustic_triplet_is_grouped_despite_numerical_splitting():
    qe = np.eye(4)
    ml = np.eye(4)
    ml[:, :2] = np.array([[1, 1], [1, -1], [0, 0], [0, 0]]) / np.sqrt(2)
    _, _, groups = compare_modes(
        np.array([-0.44, 0.49, 0.52, 5.2]), qe,
        np.array([0.0, 0.0, 0.0, 5.2]), ml, gamma_acoustic=True
    )
    assert groups[0]["qe_modes"] == [1, 2, 3]
    assert np.isclose(groups[0]["subspace_overlap"], 1.0)
