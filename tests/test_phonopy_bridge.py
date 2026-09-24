import numpy as np
import pytest
from ase import Atoms
from ase.build import make_supercell
from ase.calculators.calculator import Calculator, all_changes

pytest.importorskip("phonopy")

from mlff_modepair_workflow.phonon_eigenvectors import real_space_force_constants
from mlff_modepair_workflow.phonopy_bridge import (
    _stable_phase_fix,
    apply_phonopy_asr,
    force_constants_from_calculator,
    legacy_to_phonopy_force_constants,
    make_phonopy,
    phonons_from_phonopy,
    phonopy_to_legacy_force_constants,
)
from mlff_modepair_workflow.prophet_stage1 import phonons_from_force_constants


class OnsiteSprings(Calculator):
    implemented_properties = ["forces"]

    def __init__(self, primitive, mesh_n):
        super().__init__()
        reference = make_supercell(primitive, np.diag([mesh_n, mesh_n, 1]))
        self.positions = reference.positions
        self.symbols = reference.get_chemical_symbols()
        self.cell = reference.cell.array
        self.k = {symbol: 2.0 + i for i, symbol in enumerate(primitive.get_chemical_symbols())}

    def calculate(self, atoms=None, properties=("forces",), system_changes=all_changes):
        super().calculate(atoms, properties, system_changes)
        delta = atoms.positions[:, None, :] - self.positions[None, :, :]
        fractional = delta @ np.linalg.inv(self.cell)
        fractional -= np.rint(fractional)
        delta = fractional @ self.cell
        force = np.empty((len(atoms), 3))
        for index, symbol in enumerate(atoms.get_chemical_symbols()):
            matches = [j for j, candidate in enumerate(self.symbols) if candidate == symbol]
            nearest = min(matches, key=lambda j: np.linalg.norm(delta[index, j]))
            assert np.linalg.norm(delta[index, nearest]) < 0.02
            force[index] = -self.k[symbol] * delta[index, nearest]
        self.results["forces"] = force


@pytest.mark.parametrize("symbols", ["H", "BN", "MoS2"])
def test_phonopy_generates_and_fits_all_cartesian_displacements(symbols):
    nat = len(Atoms(symbols))
    primitive = Atoms(
        symbols, positions=[[0.5 * i, 0.3 * i, 7.5 + 0.2 * i] for i in range(nat)],
        cell=[[3.0, 0, 0], [-1.5, 2.598076211, 0], [0, 0, 15]], pbc=True,
    )
    calculator = OnsiteSprings(primitive, 2)
    expected = real_space_force_constants(primitive, calculator, 2, 0.01)
    actual, phonon = force_constants_from_calculator(primitive, calculator, 2, 0.01)
    assert len(phonon.supercells_with_displacements) == 6 * nat
    assert actual.shape == expected.shape
    assert np.max(np.abs(actual - expected)) < 1e-9
    assert np.max(np.abs(phonopy_to_legacy_force_constants(phonon, 2) - actual)) < 1e-12
    assert np.max(np.abs(legacy_to_phonopy_force_constants(phonon, actual) - phonon.force_constants)) < 1e-9


def test_phonopy_frequencies_and_v3_gauge_agree_on_finite_q():
    primitive = Atoms(
        "BN", positions=[[0, 0, 5], [1.2, 0.7, 5.5]],
        cell=[[3, 0, 0], [-1.5, 2.598076211, 0], [0, 0, 10]], pbc=True,
    )
    phi = np.zeros((3, 3, 2, 3, 2, 3))
    for axis, k in enumerate([2.0, 3.0, 4.0]):
        phi[0, 0, 0, axis, 0, axis] = 5 + k
        phi[0, 0, 1, axis, 1, axis] = 6 + k
        phi[0, 0, 0, axis, 1, axis] = -k
        phi[0, 0, 1, axis, 0, axis] = -k
    before = phonons_from_force_constants(phi, primitive.get_masses(), 3)
    after, _ = phonons_from_phonopy(primitive, phi, 3)
    for old, new in zip(before, after):
        assert np.max(np.abs(np.asarray(old["freqs_thz"]) - new["freqs_thz"])) < 1e-9
        for a, b in zip(old["eigenvectors"], new["eigenvectors"]):
            va = np.array([complex(*c) for atom in a for c in atom])
            vb = np.array([complex(*c) for atom in b for c in atom])
            assert abs(np.vdot(va, vb)) ** 2 > 1 - 1e-9


def test_phonopy_bridge_rejects_bad_shape():
    primitive = Atoms("H", positions=[[0, 0, 0]], cell=np.diag([3, 3, 10]), pbc=True)
    phonon = make_phonopy(primitive, 2)
    with pytest.raises(ValueError, match="shape"):
        legacy_to_phonopy_force_constants(phonon, np.zeros((2, 2, 3)))


def test_phonopy_force_calls_preserve_ase_atom_order():
    primitive = Atoms(
        "BN", positions=[[0, 0, 5], [1.2, 0.7, 5.5]],
        cell=[[3, 0, 0], [-1.5, 2.598076211, 0], [0, 0, 10]], pbc=True,
    )

    class OrderSensitive(Calculator):
        implemented_properties = ["forces"]

        def __init__(self):
            super().__init__()
            self.reference = make_supercell(primitive, np.diag([2, 2, 1])).positions

        def calculate(self, atoms=None, properties=("forces",), system_changes=all_changes):
            super().calculate(atoms, properties, system_changes)
            self.results["forces"] = -3 * (atoms.positions - self.reference)

    calculator = OrderSensitive()
    legacy = real_space_force_constants(primitive, calculator, 2, 0.01)
    fitted, _ = force_constants_from_calculator(primitive, calculator, 2, 0.01)
    assert np.max(np.abs(fitted - legacy)) < 1e-10


def test_explicit_phonopy_asr_preserves_raw_diagnostic_and_closes_drift():
    primitive = Atoms("H", positions=[[0, 0, 0]], cell=np.diag([3, 3, 10]), pbc=True)
    phi = np.zeros((2, 2, 1, 3, 1, 3))
    phi[0, 0, 0, :, 0, :] = np.eye(3) * 2.0
    phonon = make_phonopy(primitive, 2)
    phonon.force_constants = legacy_to_phonopy_force_constants(phonon, phi)
    original = phonon.force_constants.copy()
    corrected, report = apply_phonopy_asr(phonon, 2)
    assert report["raw_max_translational_drift_ev_per_A2"] == pytest.approx(2.0)
    assert report["corrected_max_translational_drift_ev_per_A2"] < 1e-12
    assert report["rotational_sum_rule"] is False
    assert np.array_equal(original, legacy_to_phonopy_force_constants(make_phonopy(primitive, 2), phi))
    assert np.max(np.abs(corrected.sum(axis=(0, 1, 4)))) < 1e-12


def test_tied_cartesian_components_do_not_rotate_the_real_mode_phase():
    old = np.array([0.5001, 0.5j, 0.1])
    new = np.array([0.5, 0.5001j, 0.1])
    fixed_old = _stable_phase_fix(old)
    fixed_new = _stable_phase_fix(new)
    assert abs(np.angle(np.vdot(fixed_old, fixed_new))) < 1e-3
