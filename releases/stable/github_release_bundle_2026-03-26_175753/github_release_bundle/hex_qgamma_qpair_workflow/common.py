#!/usr/bin/env python3
from __future__ import annotations

import math
import re
from pathlib import Path

import numpy as np
from ase.io.espresso import read_espresso_in


HEX_M_POINTS = [
    np.array([0.5, 0.0, 0.0], dtype=float),
    np.array([0.0, 0.5, 0.0], dtype=float),
    np.array([0.5, 0.5, 0.0], dtype=float),
]
HEX_K_POINTS = [
    np.array([1.0 / 3.0, 1.0 / 3.0, 0.0], dtype=float),
    np.array([2.0 / 3.0, 1.0 / 3.0, 0.0], dtype=float),
    np.array([1.0 / 3.0, 2.0 / 3.0, 0.0], dtype=float),
]

# Reciprocal-space actions on in-plane q for a hexagonal 2D lattice.
HEX_RECIPROCAL_OPERATIONS_2D = [
    np.array([[1.0, 0.0], [0.0, 1.0]], dtype=float),
    np.array([[0.0, 1.0], [-1.0, -1.0]], dtype=float),
    np.array([[-1.0, -1.0], [1.0, 0.0]], dtype=float),
    np.array([[0.0, -1.0], [-1.0, 0.0]], dtype=float),
    np.array([[1.0, 1.0], [0.0, -1.0]], dtype=float),
    np.array([[-1.0, 0.0], [1.0, 1.0]], dtype=float),
]


def canonicalize_q(q: np.ndarray, tol: float = 1.0e-8):
    q = np.array(q, dtype=float)
    q = q - np.floor(q)
    q[np.abs(q) < tol] = 0.0
    q[np.abs(q - 1.0) < tol] = 0.0
    return q


def q_equiv_delta_frac(q1: np.ndarray, q2: np.ndarray):
    d = np.array(q1, dtype=float) - np.array(q2, dtype=float)
    return d - np.round(d)


def convert_qe_cart_q_to_fractional(q_cart_2pi_over_alat: np.ndarray, prim_cell: np.ndarray):
    a_lat = float(np.linalg.norm(prim_cell[0]))
    if a_lat <= 0.0:
        raise ValueError("Invalid lattice length for q conversion.")

    q_cart = np.array(q_cart_2pi_over_alat, dtype=float) * (2.0 * np.pi / a_lat)
    b_mat = 2.0 * np.pi * np.linalg.inv(prim_cell)
    return np.linalg.solve(b_mat, q_cart)


def snap_q_to_grid(q_frac: np.ndarray, grid_n: int, tol: float = 1.0e-6):
    q = canonicalize_q(q_frac, tol=tol)
    out = q.copy()
    for i in range(3):
        target = round(out[i] * grid_n) / float(grid_n)
        if abs(out[i] - target) < tol:
            out[i] = target
    return canonicalize_q(out, tol=tol)


def snap_tuple(q: np.ndarray, grid_n: int):
    qs = snap_q_to_grid(q, grid_n)
    return tuple(float(x) for x in qs.tolist())


def load_structure_from_qe(scf_file: Path):
    with scf_file.open("r") as f:
        atoms = read_espresso_in(f)
    return atoms


def is_hexagonal_2d(cell: np.ndarray, length_tol: float, angle_tol_deg: float):
    a1 = cell[0]
    a2 = cell[1]
    la = float(np.linalg.norm(a1))
    lb = float(np.linalg.norm(a2))
    if la <= 0.0 or lb <= 0.0:
        return False, {}

    cosang = float(np.dot(a1, a2) / (la * lb))
    cosang = max(-1.0, min(1.0, cosang))
    angle_deg = math.degrees(math.acos(cosang))

    out = {
        "a_length": la,
        "b_length": lb,
        "a_b_relative_diff": abs(la - lb) / max(la, lb),
        "angle_deg": angle_deg,
    }
    ok = out["a_b_relative_diff"] <= length_tol and min(abs(angle_deg - 120.0), abs(angle_deg - 60.0)) <= angle_tol_deg
    return ok, out


def apply_hex_reciprocal_op(op2d: np.ndarray, q_frac: np.ndarray):
    q = np.array(q_frac[:2], dtype=float)
    qp = op2d @ q
    out = np.array([qp[0], qp[1], 0.0], dtype=float)
    return canonicalize_q(out)


def classify_hex_qpoint(q_frac: np.ndarray, tol: float = 5.0e-3):
    q = canonicalize_q(q_frac)
    if np.linalg.norm(q_equiv_delta_frac(q, np.zeros(3))) < tol:
        return "Gamma"
    for q_m in HEX_M_POINTS:
        if np.linalg.norm(q_equiv_delta_frac(q, q_m)) < tol:
            return "M"
    for q_k in HEX_K_POINTS:
        if np.linalg.norm(q_equiv_delta_frac(q, q_k)) < tol:
            return "K"
    return "line"


def choose_display_rep(star_unique, q_tol: float):
    star_arrays = [np.array(item, dtype=float) for item in star_unique]
    for candidate in [np.zeros(3)] + HEX_M_POINTS + HEX_K_POINTS:
        for star_q in star_arrays:
            if np.linalg.norm(q_equiv_delta_frac(star_q, candidate)) < q_tol:
                return tuple(float(x) for x in candidate.tolist())
    return star_unique[0]


def parse_multiq_eig_file(filename: Path, nat: int):
    lines = filename.read_text().splitlines()
    q_blocks = []

    current_q = None
    current_freqs = []
    current_modes = []
    collecting = False
    current_mode = []

    def flush():
        if current_q is None:
            return
        if len(current_modes) != len(current_freqs):
            raise ValueError(f"Frequency/mode count mismatch in {filename}")
        q_blocks.append(
            {
                "q_raw": np.array(current_q, dtype=float),
                "freqs_thz": np.array(current_freqs, dtype=float),
                "modes": [np.array(mode, dtype=np.complex128) for mode in current_modes],
            }
        )

    for line in lines:
        s = line.strip()

        if s.startswith("q ="):
            if collecting and len(current_mode) == nat:
                current_modes.append(np.array(current_mode, dtype=np.complex128))
            if current_q is not None:
                flush()
            nums = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
            if len(nums) < 3:
                raise ValueError(f"Could not parse q line in {filename}: {s}")
            current_q = [float(nums[0]), float(nums[1]), float(nums[2])]
            current_freqs = []
            current_modes = []
            collecting = False
            current_mode = []
            continue

        m = re.search(
            r"freq\s*\(\s*\d+\s*\)\s*=\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*\[THz\]",
            s,
        )
        if m:
            if collecting and len(current_mode) == nat:
                current_modes.append(np.array(current_mode, dtype=np.complex128))
            current_freqs.append(float(m.group(1)))
            collecting = True
            current_mode = []
            continue

        if collecting and s.startswith("("):
            nums = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
            if len(nums) >= 6:
                vec = np.array(
                    [
                        float(nums[0]) + 1j * float(nums[1]),
                        float(nums[2]) + 1j * float(nums[3]),
                        float(nums[4]) + 1j * float(nums[5]),
                    ],
                    dtype=np.complex128,
                )
                current_mode.append(vec)
                if len(current_mode) == nat:
                    current_modes.append(np.array(current_mode, dtype=np.complex128))
                    collecting = False
                    current_mode = []

    if collecting and len(current_mode) == nat:
        current_modes.append(np.array(current_mode, dtype=np.complex128))
    if current_q is not None:
        flush()

    return q_blocks
