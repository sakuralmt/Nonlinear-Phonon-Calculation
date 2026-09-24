"""Phonopy 2.38 bridge for the v3 finite-q Stage1 convention.

Phonopy owns displacement generation, force-constant fitting and q-point
diagonalization.  The v3 Stage2 contract uses a cell-periodic Bloch gauge;
Phonopy's eigenvectors include the atomic basis phase.  Conversion here is
explicit so an existing Stage2 displacement is never silently redefined.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from ase import Atoms
from ase.build import make_supercell

from .core import CONV_TO_THZ, load_atoms_from_qe
from .prophet_backend import sha256_file


def make_phonopy(primitive: Atoms, mesh_n: int):
    from phonopy import Phonopy, __version__ as phonopy_version
    from phonopy.structure.atoms import PhonopyAtoms

    if phonopy_version != "2.38.0":
        raise RuntimeError(
            f"Phonopy Stage1 is pinned to 2.38.0, found {phonopy_version}"
        )
    if mesh_n < 1 or not bool(np.all(primitive.pbc)):
        raise ValueError("Phonopy needs a positive mesh and a periodic primitive cell")
    unitcell = PhonopyAtoms(
        symbols=primitive.get_chemical_symbols(),
        cell=primitive.cell.array,
        positions=primitive.positions,
        masses=primitive.get_masses(),
    )
    # The force constants are eV/Angstrom^2 regardless of whether the input
    # geometry was read from QE, so the QE Ry/Bohr frequency factor is wrong.
    return Phonopy(
        unitcell,
        supercell_matrix=np.diag([mesh_n, mesh_n, 1]),
        primitive_matrix=np.eye(3),
        is_symmetry=False,
        factor=CONV_TO_THZ,
    )


def supercell_index(phonon, mesh_n: int) -> tuple[np.ndarray, np.ndarray]:
    """Map each Phonopy supercell atom to (primitive atom, lattice replica)."""
    primitive = phonon.unitcell
    scaled = np.asarray(phonon.supercell.scaled_positions) % 1
    basis = np.asarray(primitive.scaled_positions) % 1
    factors = np.array([mesh_n, mesh_n, 1])
    atom_index = np.empty(len(scaled), dtype=int)
    shift = np.empty((len(scaled), 3), dtype=int)
    for target, point in enumerate(scaled):
        candidates = []
        for atom, symbol in enumerate(primitive.symbols):
            if symbol != phonon.supercell.symbols[target]:
                continue
            delta = point * factors - basis[atom]
            replica = np.rint(delta).astype(int)
            if np.max(np.abs(delta - replica)) < 1e-6:
                candidates.append((atom, replica % factors))
        if len(candidates) != 1:
            raise ValueError(f"Ambiguous Phonopy supercell atom mapping at {target}")
        atom_index[target], shift[target] = candidates[0]
    identities = {(int(atom_index[i]), tuple(shift[i])) for i in range(len(scaled))}
    if len(identities) != len(scaled):
        raise ValueError("Duplicate Phonopy supercell atoms after mapping")
    return atom_index, shift


def legacy_to_phonopy_force_constants(phonon, phi: np.ndarray) -> np.ndarray:
    """Convert Phi[R,target,source] to Phonopy's full supercell FC tensor."""
    phi = np.asarray(phi, dtype=float)
    mesh_n = phi.shape[0]
    nat = len(phonon.unitcell)
    if phi.shape != (mesh_n, mesh_n, nat, 3, nat, 3):
        raise ValueError("Invalid v3 real-space force-constant shape")
    atom, shift = supercell_index(phonon, mesh_n)
    fc = np.empty((len(atom), len(atom), 3, 3), dtype=float)
    for target in range(len(atom)):
        for source in range(len(atom)):
            delta = (shift[target] - shift[source]) % [mesh_n, mesh_n, 1]
            fc[target, source] = phi[
                delta[0], delta[1], atom[target], :, atom[source], :
            ]
    return fc


def phonopy_to_legacy_force_constants(phonon, mesh_n: int) -> np.ndarray:
    """Extract primitive-source FCs in the established v3 tensor order."""
    atom, shift = supercell_index(phonon, mesh_n)
    nat = len(phonon.unitcell)
    source_indices = {}
    target_indices = {}
    for index in range(len(atom)):
        key = (int(shift[index, 0]), int(shift[index, 1]), int(atom[index]))
        target_indices[key] = index
        if key[:2] == (0, 0):
            source_indices[key[2]] = index
    if len(source_indices) != nat or len(target_indices) != mesh_n * mesh_n * nat:
        raise ValueError("Incomplete Phonopy force-constant mapping")
    fc = np.asarray(phonon.force_constants)
    if fc.shape != (len(atom), len(atom), 3, 3):
        raise ValueError("Phonopy must produce full supercell force constants")
    phi = np.empty((mesh_n, mesh_n, nat, 3, nat, 3), dtype=float)
    for i in range(mesh_n):
        for j in range(mesh_n):
            for target in range(nat):
                for source in range(nat):
                    phi[i, j, target, :, source, :] = fc[
                        target_indices[(i, j, target)], source_indices[source]
                    ]
    return phi


def apply_phonopy_asr(phonon, mesh_n: int):
    """Apply Phonopy's translational/permutation FC symmetrizer explicitly."""
    raw = np.asarray(phonon.force_constants, dtype=float).copy()
    if raw.shape != (len(phonon.supercell), len(phonon.supercell), 3, 3):
        raise ValueError("ASR requires full supercell force constants")
    raw_drift = float(np.max(np.abs(raw.sum(axis=1))))
    phonon.symmetrize_force_constants(level=1, show_drift=False)
    corrected = np.asarray(phonon.force_constants)
    corrected_drift = float(np.max(np.abs(corrected.sum(axis=1))))
    return phonopy_to_legacy_force_constants(phonon, mesh_n), {
        "method": "phonopy_2.38_symmetrize_force_constants_level_1",
        "translational_and_permutation_symmetry": True,
        "space_group_symmetry": False,
        "rotational_sum_rule": False,
        "raw_max_translational_drift_ev_per_A2": raw_drift,
        "corrected_max_translational_drift_ev_per_A2": corrected_drift,
        "max_force_constant_change_ev_per_A2": float(np.max(np.abs(corrected - raw))),
    }


def _stable_phase_fix(vector: np.ndarray) -> np.ndarray:
    """Fix a mode phase without flipping a nearly tied x/y anchor."""
    vector = np.asarray(vector, dtype=np.complex128).copy()
    magnitude = np.abs(vector)
    maximum = float(np.max(magnitude))
    if maximum:
        pivot = int(np.flatnonzero(magnitude >= maximum * (1 - 1e-3))[0])
        vector *= np.conjugate(vector[pivot]) / magnitude[pivot]
    return vector


def force_constants_from_calculator(
    primitive: Atoms, calculator, mesh_n: int, step: float
):
    """Use Phonopy to make +/- displacements and fit full force constants."""
    if step <= 0:
        raise ValueError("Displacement step must be positive")
    phonon = make_phonopy(primitive, mesh_n)
    phonon.generate_displacements(distance=step, is_plusminus=True, is_diagonal=False)
    supercells = phonon.supercells_with_displacements
    if len(supercells) != 6 * len(primitive):
        raise ValueError("Unexpected Phonopy displacement count")
    # Models should see the same atom order and perfect coordinates as the
    # existing ASE supercell path.  Phonopy groups replicas by atom type,
    # while ASE interleaves primitive atoms within each lattice replica.
    # A neural calculator can differ slightly under a permutation because of
    # floating-point summation.  Phonopy still owns the displacements and FC
    # fit; only the force-evaluation representation is canonicalized.
    atom_index, shift = supercell_index(phonon, mesh_n)
    order = np.lexsort((atom_index, shift[:, 1], shift[:, 0]))
    reference = make_supercell(primitive, np.diag([mesh_n, mesh_n, 1]))
    perfect = np.asarray(phonon.supercell.positions)[order]
    if (
        list(np.asarray(phonon.supercell.symbols)[order])
        != reference.get_chemical_symbols()
    ):
        raise ValueError("Phonopy and ASE supercell atom order cannot be aligned")
    difference = (perfect - reference.positions) @ np.linalg.inv(reference.cell.array)
    difference -= np.rint(difference)
    if np.max(np.abs(difference @ reference.cell.array)) > 1e-6:
        raise ValueError("Phonopy and ASE perfect supercells differ")
    forces = []
    for displaced in supercells:
        displacement = np.asarray(displaced.positions)[order] - perfect
        atoms = Atoms(
            symbols=reference.get_chemical_symbols(),
            cell=reference.cell.array,
            positions=reference.positions + displacement,
            pbc=True,
        )
        atoms.set_masses(reference.get_masses())
        atoms.calc = calculator
        force = np.asarray(atoms.get_forces(apply_constraint=False), dtype=float)
        if force.shape != (len(atoms), 3) or not np.isfinite(force).all():
            raise ValueError("Invalid force array from Phonopy displacement")
        phonopy_order_force = np.empty_like(force)
        phonopy_order_force[order] = force
        forces.append(phonopy_order_force)
    phonon.forces = np.asarray(forces)
    phonon.produce_force_constants(show_drift=False)
    return phonopy_to_legacy_force_constants(phonon, mesh_n), phonon


def phonons_from_phonopy(primitive: Atoms, phi: np.ndarray, mesh_n: int):
    """Return v3 q records using Phonopy's q solver and the v3 Bloch gauge."""
    from .phonon_eigenvectors import dynamical_matrix
    from .prophet_stage1 import _degenerate_groups, _encode_mode, _phase_fix

    phonon = make_phonopy(primitive, mesh_n)
    phonon.force_constants = legacy_to_phonopy_force_constants(phonon, phi)
    qpoints = np.array(
        [[i / mesh_n, j / mesh_n, 0.0] for i in range(mesh_n) for j in range(mesh_n)]
    )
    phonon.run_qpoints(qpoints, with_eigenvectors=True)
    results = phonon.get_qpoints_dict()
    basis = np.asarray(phonon.unitcell.scaled_positions) % 1
    records = []
    for iq, q in enumerate(qpoints):
        freqs = np.asarray(results["frequencies"][iq])
        _, hermitian_error = dynamical_matrix(phi, primitive.get_masses(), q)
        # Phonopy's vectors carry exp(-i q.tau); v3 Stage2 uses cell-periodic
        # vectors.  The inverse phase converts to the existing displacement.
        gauge = np.repeat(np.exp(2j * np.pi * (basis @ q)), 3)
        vectors = gauge[:, None] * results["eigenvectors"][iq]
        phase_fix = _phase_fix if iq == 0 else _stable_phase_fix
        vectors = np.column_stack(
            [phase_fix(vectors[:, mode]) for mode in range(len(freqs))]
        )
        records.append(
            {
                "q_index": [int(round(q[0] * mesh_n)), int(round(q[1] * mesh_n))],
                "q_frac": q.tolist(),
                "freqs_thz": freqs.tolist(),
                "eigenvectors": [
                    _encode_mode(vectors[:, mode], len(primitive))
                    for mode in range(len(freqs))
                ],
                "degenerate_groups_one_based": _degenerate_groups(freqs),
                "hermitian_relative_error_before_symmetrizing": hermitian_error,
                "phonon_engine": "phonopy",
            }
        )
    return records, phonon


def compare_saved_stage1(stage1_dir: Path, structure: Path | None = None) -> dict:
    """Independently audit an existing v3 phonon mesh without MLFF calls."""
    from phonopy import __version__ as phonopy_version

    stage1_dir = Path(stage1_dir)
    dataset = json.loads((stage1_dir / "phonon_dataset.json").read_text())
    source = dataset["source"]
    structure = Path(structure or source["structure"])
    if sha256_file(structure) != source["structure_sha256"]:
        raise ValueError("Structure hash does not match saved Stage1 results")
    primitive = load_atoms_from_qe(structure)
    mesh_n = source["q_grid"][0]
    with np.load(stage1_dir / "force_constants.npz") as arrays:
        phi = arrays["force_constants_ev_per_A2"]
    converted, phonon = phonons_from_phonopy(primitive, phi, mesh_n)
    old = dataset["q_points"]
    if len(old) != len(converted):
        raise ValueError("Saved q mesh is incomplete")
    freq_errors = []
    overlaps = []
    for before, after in zip(old, converted):
        if before["q_index"] != after["q_index"]:
            raise ValueError("Saved q order differs from Phonopy mesh")
        freq_errors.extend(np.abs(np.asarray(before["freqs_thz"]) - after["freqs_thz"]))
        for vec1, vec2 in zip(before["eigenvectors"], after["eigenvectors"]):
            v1 = np.array([complex(*pair) for atom in vec1 for pair in atom])
            v2 = np.array([complex(*pair) for atom in vec2 for pair in atom])
            overlaps.append(float(abs(np.vdot(v1, v2)) ** 2))
    return {
        "material_symbols": primitive.get_chemical_symbols(),
        "structure_sha256": source["structure_sha256"],
        "phonopy_version": phonopy_version,
        "q_count": len(old),
        "max_frequency_difference_thz": float(max(freq_errors)),
        "min_same_branch_overlap_squared": float(min(overlaps)),
        "mean_same_branch_overlap_squared": float(np.mean(overlaps)),
        "force_constant_shape": list(phonon.force_constants.shape),
        "force_constant_unit": "eV/Angstrom^2",
        "frequency_unit": "THz",
        "eigenvector_conversion": "multiply phonopy vector by exp(+2pi*i*q.dot(primitive_scaled_position))",
        "symmetry_reduction": False,
        "acoustic_sum_rule_applied": bool(
            source.get("phonon_engine", {}).get("acoustic_sum_rule", False)
        )
        if isinstance(source.get("phonon_engine"), dict)
        else False,
        "non_analytical_correction_applied": False,
    }


def main(argv=None):
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage1-dir", type=Path, required=True)
    parser.add_argument("--structure", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = compare_saved_stage1(args.stage1_dir, args.structure)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
