"""Structure-derived two-dimensional reciprocal orbits and phonon checks.

The cell metric alone never authorizes a point-group reduction.  Spglib
operations must map every atom (including its species) onto the actual input
structure.  A failed phonon covariance check reduces only by time reversal.
"""

from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np
from scipy.optimize import linear_sum_assignment

from .core import decode_complex_mode


def is_hexagonal_2d(
    cell: np.ndarray, length_tol: float = 0.05, angle_tol_deg: float = 3.0
) -> tuple[bool, dict]:
    """Check the cell metric only; atomic symmetry is checked separately."""
    a, b = np.asarray(cell)[:2]
    la, lb = np.linalg.norm(a), np.linalg.norm(b)
    if min(la, lb) <= 0:
        return False, {}
    angle = math.degrees(math.acos(float(np.clip(np.dot(a, b) / (la * lb), -1, 1))))
    details = {
        "a_length": float(la),
        "b_length": float(lb),
        "a_b_relative_diff": float(abs(la - lb) / max(la, lb)),
        "angle_deg": angle,
    }
    return (
        details["a_b_relative_diff"] <= length_tol
        and min(abs(angle - 60), abs(angle - 120)) <= angle_tol_deg
    ), details


def classify_hex_qpoint(
    q: np.ndarray, cell: np.ndarray | None = None, tol: float = 5e-3
) -> str:
    q = np.asarray(q)[:2] % 1
    if np.linalg.norm(q - np.rint(q)) < tol:
        return "Gamma"
    candidates = {"M": [(0.5, 0), (0, 0.5), (0.5, 0.5)]}
    if cell is not None:
        a, b = np.asarray(cell)[:2]
        candidates["K"] = (
            [(1 / 3, 1 / 3), (2 / 3, 2 / 3)]
            if np.dot(a, b) < 0
            else [(1 / 3, 2 / 3), (2 / 3, 1 / 3)]
        )
    for label, points in candidates.items():
        if any(
            np.linalg.norm((q - point) - np.rint(q - point)) < tol for point in points
        ):
            return label
    return "line"


@dataclass(frozen=True)
class Operation:
    rotation: np.ndarray
    translation: np.ndarray
    permutation: np.ndarray
    shifts: np.ndarray
    cartesian: np.ndarray


def _operations(atoms, symprec: float) -> list[Operation]:
    import spglib

    cell = np.asarray(atoms.cell.array)
    scaled = np.asarray(atoms.get_scaled_positions()) % 1
    numbers = np.asarray(atoms.get_atomic_numbers())
    found = spglib.get_symmetry((cell, scaled, numbers), symprec=symprec)
    if found is None:
        raise ValueError("Spglib could not establish atomic symmetry")
    result = []
    seen = set()
    for rotation, translation in zip(found["rotations"], found["translations"]):
        rotation = np.asarray(rotation, dtype=int)
        # An in-plane q mesh may only use layer-preserving block rotations.
        if np.any(rotation[2, :2]) or np.any(rotation[:2, 2]):
            continue
        key = tuple(rotation.ravel())
        if key in seen:
            continue
        seen.add(key)
        cart = cell.T @ rotation @ np.linalg.inv(cell.T)
        if not np.allclose(cart.T @ cart, np.eye(3), atol=1e-5):
            continue
        permutation: np.ndarray = np.full(len(atoms), -1, dtype=int)
        shifts: np.ndarray = np.empty((len(atoms), 3), dtype=int)
        used = set()
        for source, point in enumerate(scaled):
            image = rotation @ point + translation
            choices = []
            for target, other in enumerate(scaled):
                if numbers[source] != numbers[target] or target in used:
                    continue
                delta = image - other
                shift = np.rint(delta).astype(int)
                if np.linalg.norm((delta - shift) @ cell) <= 2 * symprec:
                    choices.append((target, shift))
            if len(choices) != 1:
                raise ValueError("Atomic symmetry operation has an ambiguous atom map")
            target, shift = choices[0]
            permutation[source] = target
            shifts[source] = shift
            used.add(target)
        result.append(
            Operation(rotation, np.asarray(translation), permutation, shifts, cart)
        )
    if not any(np.array_equal(op.rotation, np.eye(3, dtype=int)) for op in result):
        raise ValueError("Atomic symmetry has no identity operation")
    return result


def _image(
    index: tuple[int, int], mesh_n: int, op: Operation, reverse: bool
) -> tuple[int, int]:
    reciprocal = np.linalg.inv(op.rotation).T
    mapped = reciprocal @ np.array([*index, 0], dtype=int)
    if reverse:
        mapped = -mapped
    if np.max(np.abs(mapped - np.rint(mapped))) > 1e-8 or abs(mapped[2]) > 1e-8:
        raise ValueError("Symmetry operation does not preserve the q mesh")
    rounded = np.rint(mapped[:2])
    return int(rounded[0]) % mesh_n, int(rounded[1]) % mesh_n


def _transform(
    vector: np.ndarray, op: Operation, q_image: np.ndarray, reverse: bool
) -> np.ndarray:
    transformed = np.empty_like(vector)
    for source, target in enumerate(op.permutation):
        phase = np.exp(-2j * np.pi * np.dot(q_image, op.shifts[source]))
        transformed[target] = phase * (op.cartesian @ vector[source])
    return transformed.conj() if reverse else transformed


def _mapping(
    left: dict,
    right: dict,
    op: Operation,
    reverse: bool,
    overlap_floor: float,
    frequency_tolerance_thz: float,
) -> dict | None:
    q_image = np.asarray(right["q_frac"])
    before = [decode_complex_mode(row) for row in left["eigenvectors"]]
    after = [decode_complex_mode(row) for row in right["eigenvectors"]]
    transformed = [
        _transform(vec, op, q_image if not reverse else -q_image, reverse).ravel()
        for vec in before
    ]
    target = [vec.ravel() for vec in after]
    overlap = np.abs(np.asarray(transformed).conj() @ np.asarray(target).T) ** 2
    rows, cols = linear_sum_assignment(-overlap)
    frequencies = np.asarray(left["freqs_thz"])
    other_frequencies = np.asarray(right["freqs_thz"])
    if (
        np.max(np.abs(np.sort(frequencies) - np.sort(other_frequencies)))
        > frequency_tolerance_thz
        or np.max(np.abs(frequencies[rows] - other_frequencies[cols]))
        > frequency_tolerance_thz
    ):
        return None
    # For a near-degenerate block, individual vectors may rotate; its singular
    # values are invariant.  Keep the branch mapping diagnostic, not a claim of
    # unique physical branch identity.
    groups = left.get("degenerate_groups_one_based", [])
    ambiguous = []
    for group in groups:
        if len(group) < 2:
            continue
        indices = [number - 1 for number in group]
        destinations = [int(cols[np.where(rows == index)[0][0]]) for index in indices]
        block = (
            np.asarray(transformed)[indices].conj() @ np.asarray(target)[destinations].T
        )
        if np.min(np.linalg.svd(block, compute_uv=False)) ** 2 < overlap_floor:
            return None
        ambiguous.extend(indices)
    isolated = [int(i) for i in rows if i not in ambiguous]
    if (
        isolated
        and min(float(overlap[i, cols[np.where(rows == i)[0][0]]]) for i in isolated)
        < overlap_floor
    ):
        return None
    return {
        "branch_map_zero_based": [
            int(cols[np.where(rows == i)[0][0]]) for i in range(len(rows))
        ],
        "min_isolated_overlap_squared": min(
            (float(overlap[i, cols[np.where(rows == i)[0][0]]]) for i in isolated),
            default=None,
        ),
        "degenerate_sources_one_based": [i + 1 for i in ambiguous],
    }


def structure_q_orbits(
    atoms,
    records: list[dict],
    mesh_n: int,
    *,
    symprec: float = 1e-3,
    overlap_floor: float = 0.9,
    frequency_tolerance_thz: float = 0.03,
) -> tuple[list[dict], dict]:
    """Build non-Gamma orbits, failing closed to q/-q if covariance is poor."""
    if mesh_n < 2 or len(records) != mesh_n**2:
        raise ValueError("A complete Gamma-centered square q mesh is required")
    by_index = {tuple(row["q_index"]): row for row in records}
    if len(by_index) != mesh_n**2:
        raise ValueError("Repeated or missing q-point record")
    reason = None
    try:
        operations = _operations(atoms, symprec)
    except ValueError as exc:
        operations = []
        reason = str(exc)
    identity = Operation(
        np.eye(3, dtype=int),
        np.zeros(3),
        np.arange(len(atoms)),
        np.zeros((len(atoms), 3), dtype=int),
        np.eye(3),
    )

    def build(ops: list[Operation]):
        remaining = set(by_index) - {(0, 0)}
        orbits: list[dict] = []
        while remaining:
            seed = min(remaining)
            candidates: dict[tuple[int, int], list[tuple[Operation, bool]]] = {}
            for op in ops:
                for reverse in (False, True):
                    image = _image(seed, mesh_n, op, reverse)
                    candidates.setdefault(image, []).append((op, reverse))
            members = set(candidates)
            if not members <= remaining:
                raise ValueError("Atomic symmetry produced overlapping q orbits")
            preferred = (
                [
                    (mesh_n // 2, 0),
                    (mesh_n // 3, mesh_n // 3),
                    (mesh_n // 3, 2 * mesh_n // 3),
                ]
                if mesh_n % 6 == 0
                else []
            )
            representative = next(
                (point for point in preferred if point in members), min(members)
            )
            # Reconstruct maps from the chosen representative, not the seed.
            maps = {}
            for member in sorted(members):
                possible = [
                    (op, reverse)
                    for op in ops
                    for reverse in (False, True)
                    if _image(representative, mesh_n, op, reverse) == member
                ]
                valid = [
                    (op, reverse, mapping)
                    for op, reverse in possible
                    if (
                        mapping := _mapping(
                            by_index[representative],
                            by_index[member],
                            op,
                            reverse,
                            overlap_floor,
                            frequency_tolerance_thz,
                        )
                    )
                    is not None
                ]
                if not valid:
                    raise ValueError(
                        f"Phonon modes at {representative} and {member} are not symmetry covariant"
                    )
                op, reverse, mapping = max(
                    valid, key=lambda row: row[2]["min_isolated_overlap_squared"] or 0
                )
                maps[f"{member[0]},{member[1]}"] = {
                    "rotation_fractional": op.rotation.tolist(),
                    "translation_fractional": op.translation.tolist(),
                    "atom_permutation": op.permutation.tolist(),
                    "lattice_shifts": op.shifts.tolist(),
                    "time_reversal": reverse,
                    **mapping,
                }
            orbits.append(
                {
                    "representative_index": list(representative),
                    "representative_q_frac": [
                        representative[0] / mesh_n,
                        representative[1] / mesh_n,
                        0.0,
                    ],
                    "representative_label": classify_hex_qpoint(
                        np.asarray([*representative, 0]) / mesh_n, atoms.cell.array
                    ),
                    "members_index": [list(member) for member in sorted(members)],
                    "members_q_frac": [
                        [i / mesh_n, j / mesh_n, 0.0] for i, j in sorted(members)
                    ],
                    "size": len(members),
                    "mode_maps": maps,
                }
            )
            remaining -= members
        if sum(row["size"] for row in orbits) != mesh_n**2 - 1:
            raise ValueError("Incomplete finite-q orbit coverage")
        orbits.sort(key=lambda row: row["representative_index"])
        return orbits

    try:
        orbits = build(operations) if operations else build([identity])
        method = (
            "atomic_spglib_plus_time_reversal"
            if len(operations) > 1
            else "time_reversal_only"
        )
    except ValueError as exc:
        reason = str(exc)
        orbits = build([identity])
        method = "time_reversal_only"
    metadata = {
        "method": method,
        "fallback_reason": reason,
        "symprec_angstrom": symprec,
        "overlap_floor_squared": overlap_floor,
        "frequency_tolerance_thz": frequency_tolerance_thz,
        "atomic_operation_count": len(operations),
        "q_orbit_count": len(orbits),
        "finite_q_count": mesh_n**2 - 1,
    }
    return orbits, metadata
