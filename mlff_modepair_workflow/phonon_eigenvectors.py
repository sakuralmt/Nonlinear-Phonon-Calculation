"""Finite-displacement phonon eigenvectors on a commensurate in-plane q mesh.

The real-space force constants are measured once in an N x N supercell and
Fourier transformed to every q in the same mesh.  Vectors use the QE/ASE
mass-weighted primitive-atom Cartesian convention.
"""

from __future__ import annotations

import re
from pathlib import Path

import numpy as np
from ase.build import make_supercell
from scipy.optimize import linear_sum_assignment

from .core import CONV_TO_THZ


_FREQ_RE = re.compile(r"freq\s*\(\s*\d+\s*\)\s*=\s*([-+\d.]+)\s*\[THz\]")
_PAIR_RE = re.compile(r"([-+]?\d+\.\d+)\s+([-+]?\d+\.\d+)")


def read_matdyn_q_mesh(input_path: Path) -> np.ndarray:
    """Read the explicit crystal-coordinate q list from a QE matdyn input."""
    lines = input_path.read_text().splitlines()
    end = next(i for i, line in enumerate(lines) if line.strip() == "/")
    namelist = re.sub(r"\s+", "", "".join(lines[:end]).lower())
    if "q_in_cryst_coord=.true." not in namelist:
        raise ValueError("matdyn input must explicitly use q_in_cryst_coord=.true.")
    count = int(lines[end + 1].strip())
    q = np.array([[float(x) for x in line.split()[:3]] for line in lines[end + 2 : end + 2 + count]])
    if q.shape != (count, 3):
        raise ValueError("Incomplete matdyn q list")
    return q


def read_qe_eigenvectors(eig_path: Path, natoms: int, expected_q: np.ndarray):
    """Return QE frequencies and normalized Cartesian mass-weighted vectors.

    The q coordinates printed by qeph.eig may be Cartesian; matdyn's input list
    is therefore the authoritative crystal-coordinate mapping by record order.
    """
    lines = eig_path.read_text().splitlines()
    blocks = [i for i, line in enumerate(lines) if "diagonalizing the dynamical matrix" in line]
    if len(blocks) != len(expected_q):
        raise ValueError(f"Expected {len(expected_q)} q blocks, found {len(blocks)}")
    nmode = 3 * natoms
    freq = np.empty((len(blocks), nmode))
    vec = np.empty((len(blocks), nmode, nmode), dtype=complex)
    for iq, start in enumerate(blocks):
        stop = blocks[iq + 1] if iq + 1 < len(blocks) else len(lines)
        cursor = start
        for mode in range(nmode):
            while cursor < stop and not (match := _FREQ_RE.search(lines[cursor])):
                cursor += 1
            if cursor >= stop:
                raise ValueError(f"Missing q {iq} mode {mode + 1}")
            freq[iq, mode] = float(match.group(1))
            for atom in range(natoms):
                cursor += 1
                parts = _PAIR_RE.findall(lines[cursor])
                if len(parts) != 3:
                    raise ValueError(f"Invalid eigenvector at q {iq}, mode {mode + 1}, atom {atom + 1}")
                vec[iq, 3 * atom : 3 * atom + 3, mode] = [complex(float(a), float(b)) for a, b in parts]
            cursor += 1
    norm = np.linalg.norm(vec, axis=1)
    if np.max(np.abs(norm - 1)) > 2e-5:
        raise ValueError("QE eigenvector normalization check failed")
    vec /= norm[:, None, :]
    gram = np.einsum("qik,qil->qkl", vec.conj(), vec)
    if np.max(np.abs(gram - np.eye(nmode))) > 2e-5:
        raise ValueError("QE eigenvector orthogonality check failed")
    return freq, vec


def real_space_force_constants(primitive, calculator, mesh_n: int = 6, step: float = 0.01):
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
                forces.append(np.asarray(atoms.get_forces(apply_constraint=False), dtype=float))
            response = -(forces[0] - forces[1]) / (2 * step)
            phi[:, :, :, :, source, axis] = response.reshape(mesh_n, mesh_n, nat, 3)
    return phi


def dynamical_matrix(phi: np.ndarray, masses: np.ndarray, q_frac: np.ndarray) -> np.ndarray:
    """Fourier transform Phi[target R, source 0] with the Bloch +iqR convention."""
    mesh_n, _, nat = phi.shape[:3]
    replica = np.indices((mesh_n, mesh_n)).transpose(1, 2, 0)
    phase = np.exp(-2j * np.pi * np.einsum("ijk,k->ij", replica, q_frac[:2]))
    fc = np.einsum("ij,ijabcd->abcd", phase, phi)
    weight = np.sqrt(masses[:, None] * masses[None, :])
    matrix = (fc / weight[:, None, :, None]).reshape(3 * nat, 3 * nat)
    hermitian_error = float(np.linalg.norm(matrix - matrix.conj().T) / max(np.linalg.norm(matrix), 1e-12))
    return (matrix + matrix.conj().T) / 2, hermitian_error


def frequencies_and_vectors(matrix: np.ndarray):
    eigvals, vectors = np.linalg.eigh(matrix)
    return np.sign(eigvals) * np.sqrt(np.abs(eigvals)) * CONV_TO_THZ, vectors


def compare_modes(qe_freq, qe_vectors, model_freq, model_vectors, degeneracy_thz=0.1, gamma_acoustic=False):
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
            score = float(np.linalg.norm(qe_vectors[:, members].conj().T @ model_vectors[:, matched]) ** 2 / len(members))
            groups.append({"qe_modes": [i + 1 for i in members], "ml_modes": [int(i + 1) for i in matched], "subspace_overlap": score})
            start = index
    return assignment, np.diag(overlap[:, assignment]), groups
