"""Finite-displacement phonon eigenvectors on a commensurate in-plane q mesh.

The real-space force constants are measured once in an N x N supercell and
Fourier transformed to every q in the same mesh. Vectors use the ASE
mass-weighted primitive-atom Cartesian convention.
"""

from __future__ import annotations

import numpy as np
from ase.build import make_supercell
from scipy.optimize import linear_sum_assignment

from .core import CONV_TO_THZ


def real_space_force_constants(
    primitive, calculator, mesh_n: int = 6, step: float = 0.01
):
    """Measure Phi[R, target, source] in eV/Angstrom^2 using 2*3*nat calls."""
    if mesh_n < 1 or step <= 0:
        raise ValueError("mesh_n and step must be positive")
    supercell = make_supercell(primitive, np.diag([mesh_n, mesh_n, 1]))
    nat = len(primitive)
    positions = supercell.get_positions()
    phi = np.empty((mesh_n, mesh_n, nat, 3, nat, 3))
    for source in range(nat):
        for axis in range(3):
            forces = []
            for sign in (+1, -1):
                atoms = supercell.copy()
                moved = positions.copy()
                moved[source, axis] += sign * step
                atoms.set_positions(moved, apply_constraint=False)
                atoms.calc = calculator
                forces.append(
                    np.asarray(atoms.get_forces(apply_constraint=False), dtype=float)
                )
            response = -(forces[0] - forces[1]) / (2 * step)
            phi[:, :, :, :, source, axis] = response.reshape(mesh_n, mesh_n, nat, 3)
    return phi


def dynamical_matrix(
    phi: np.ndarray, masses: np.ndarray, q_frac: np.ndarray
) -> tuple[np.ndarray, float]:
    """Fourier transform Phi[target R, source 0] with the Bloch +iqR convention."""
    mesh_n, _, nat = phi.shape[:3]
    replica = np.indices((mesh_n, mesh_n)).transpose(1, 2, 0)
    phase = np.exp(-2j * np.pi * np.einsum("ijk,k->ij", replica, q_frac[:2]))
    fc = np.einsum("ij,ijabcd->abcd", phase, phi)
    weight = np.sqrt(masses[:, None] * masses[None, :])
    matrix = (fc / weight[:, None, :, None]).reshape(3 * nat, 3 * nat)
    hermitian_error = float(
        np.linalg.norm(matrix - matrix.conj().T) / max(np.linalg.norm(matrix), 1e-12)
    )
    return (matrix + matrix.conj().T) / 2, hermitian_error


def frequencies_and_vectors(matrix: np.ndarray):
    eigvals, vectors = np.linalg.eigh(matrix)
    return np.sign(eigvals) * np.sqrt(np.abs(eigvals)) * CONV_TO_THZ, vectors


def compare_modes(
    qe_freq,
    qe_vectors,
    model_freq,
    model_vectors,
    degeneracy_thz=0.1,
    gamma_acoustic=False,
):
    """Assign isolated modes by overlap; score QE degenerate groups as subspaces."""
    overlap = np.abs(qe_vectors.conj().T @ model_vectors) ** 2
    rows, cols = linear_sum_assignment(-overlap)
    assignment = np.empty(len(rows), dtype=int)
    assignment[rows] = cols
    groups = []
    start = 0
    for index in range(1, len(qe_freq) + 1):
        if gamma_acoustic and index < 3:
            continue
        boundary = (
            index == len(qe_freq)
            or (gamma_acoustic and index == 3)
            or qe_freq[index] - qe_freq[index - 1] > degeneracy_thz
        )
        if boundary:
            members = list(range(start, index))
            matched = assignment[members]
            score = float(
                np.linalg.norm(
                    qe_vectors[:, members].conj().T @ model_vectors[:, matched]
                )
                ** 2
                / len(members)
            )
            groups.append(
                {
                    "qe_modes": [i + 1 for i in members],
                    "ml_modes": [int(i + 1) for i in matched],
                    "subspace_overlap": score,
                }
            )
            start = index
    return assignment, np.diag(overlap[:, assignment]), groups
