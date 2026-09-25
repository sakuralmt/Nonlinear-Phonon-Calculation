"""Symmetry acceptance using a periodic, anisotropic central-spring Hessian."""

import copy

import numpy as np
from ase import Atoms
from ase.neighborlist import neighbor_list

from mlff_modepair_workflow.phonon_eigenvectors import dynamical_matrix
from mlff_modepair_workflow.prophet_stage1 import phonons_from_force_constants
from mlff_modepair_workflow.structure_symmetry import structure_q_orbits


def spring_records(angle=120):
    a = 3.2
    atoms = Atoms(
        "BN",
        scaled_positions=[[0, 0, 0.5], [1 / 3, 2 / 3, 0.5]],
        cell=[[a, 0, 0], [-a / 2, a * np.sqrt(3) / 2, 0], [0, 0, 18]],
        pbc=True,
    )
    if angle == 60:
        cell = atoms.cell.array.copy()
        cell[1] += cell[0]
        atoms.set_cell(cell, scale_atoms=False)
    atoms.wrap()
    mesh = 6
    phi = np.zeros((mesh, mesh, 2, 3, 2, 3))
    ii, jj, shifts, vectors, lengths = neighbor_list("ijSDd", atoms, 1.15 * a)
    for i, j, shift, vector, distance in zip(ii, jj, shifts, vectors, lengths):
        unit = vector / distance
        # Isotropic transverse stiffness also avoids accidental flat ZA branches.
        block = np.exp(-distance) * (np.eye(3) + 2 * np.outer(unit, unit))
        phi[0, 0, i, :, i, :] += block
        phi[shift[0] % mesh, shift[1] % mesh, j, :, i, :] -= block
    return atoms, phi, phonons_from_force_constants(phi, atoms.get_masses(), mesh)


def test_nontrivial_bloch_modes_pass_both_cell_representations():
    for angle in (60, 120):
        atoms, phi, records = spring_records(angle)
        assert any(
            abs(np.asarray(r["eigenvectors"])[..., 1]).max() > 0.1 for r in records
        )
        for record in records:
            _, hermitian_error = dynamical_matrix(
                phi, atoms.get_masses(), record["q_frac"]
            )
            assert hermitian_error < 1e-12
        orbits, meta = structure_q_orbits(atoms, records, 6)
        assert len(orbits) == 6
        assert meta["method"] == "atomic_spglib_plus_time_reversal"


def test_random_complex_gauges_and_degenerate_rotations_preserve_orbits():
    atoms, _, records = spring_records()
    original, _ = structure_q_orbits(atoms, records, 6)
    rng = np.random.default_rng(925)
    for row in records:
        encoded = np.asarray(row["eigenvectors"])
        matrix = (encoded[..., 0] + 1j * encoded[..., 1]).reshape(6, 6).T
        for group in row["degenerate_groups_one_based"]:
            columns = np.asarray(group) - 1
            z = rng.normal(size=(len(columns), len(columns))) + 1j * rng.normal(
                size=(len(columns), len(columns))
            )
            unitary, _ = np.linalg.qr(z)
            matrix[:, columns] = matrix[:, columns] @ unitary
        vectors = matrix.T.reshape(6, 2, 3)
        row["eigenvectors"] = np.stack([vectors.real, vectors.imag], axis=-1).tolist()
    rotated, _ = structure_q_orbits(atoms, records, 6)
    assert [x["members_index"] for x in original] == [
        x["members_index"] for x in rotated
    ]
    for orbit in rotated:
        for mapping in orbit["mode_maps"].values():
            assert "gamma_mode_map" in mapping
            for block in mapping["gamma_mode_map"]["degenerate_subspace_checks"]:
                assert block["minimum_overlap_squared"] > 1 - 1e-10


def test_gamma_covariance_is_required_for_gamma_q_channel_reduction():
    atoms, _, records = spring_records()
    altered = copy.deepcopy(records)
    gamma = altered[0]
    optical_doublet = next(
        g for g in gamma["degenerate_groups_one_based"] if len(g) == 2
    )
    gamma["freqs_thz"][optical_doublet[0] - 1] += 0.2
    gamma["degenerate_groups_one_based"] = [
        part
        for group in gamma["degenerate_groups_one_based"]
        for part in ([[i] for i in group] if group == optical_doublet else [group])
    ]
    orbits, meta = structure_q_orbits(atoms, altered, 6)
    assert len(orbits) == 19
    assert meta["method"] == "time_reversal_only"
    assert "Gamma" in meta["fallback_reason"]


def test_complete_gamma_multiplet_preserves_third_norm_and_fourth_trace():
    from mlff_modepair_workflow.core import (
        ModePairFrozenPhononBuilder,
        analyze_pair_grid,
    )
    from mlff_modepair_workflow.prophet_stage1 import mode_pairs_from_phonons
    from mlff_modepair_workflow.screening_stage2 import SIX, _key, _proxy

    atoms, _, records = spring_records()
    orbits, _ = structure_q_orbits(atoms, records, 6)
    gamma = records[0]
    doublet = next(g for g in gamma["degenerate_groups_one_based"] if len(g) == 2)
    pairs = mode_pairs_from_phonons(records, orbits, atoms.get_masses())
    selected = [
        p
        for p in pairs
        if p["target_mode"]["q_frac"] == [0.5, 0, 0]
        and p["target_mode"]["mode_number_one_based"] == 6
        and p["gamma_mode"]["mode_number_one_based"] in doublet
    ]
    builders = [ModePairFrozenPhononBuilder(p, atoms) for p in selected]
    basis = [b.displacement_cart(1, 0) * b.mass_sqrt for b in builders]
    target = builders[0].displacement_cart(0, 1) * builders[0].mass_sqrt
    axis = np.arange(-1, 1.01, 0.5)
    for angle in (0, 0.31, 0.94):
        rotation = np.array(
            [[np.cos(angle), -np.sin(angle)], [np.sin(angle), np.cos(angle)]]
        )
        vectors = [np.asarray(p["gamma_mode"]["eigenvector"]) for p in selected]
        rotated_vectors = np.einsum("ij,jabc->iabc", rotation, vectors)
        thirds, fourths = [], []
        for pair, vector in zip(selected, rotated_vectors):
            changed = copy.deepcopy(pair)
            changed["gamma_mode"]["eigenvector"] = vector.tolist()
            builder = ModePairFrozenPhononBuilder(changed, atoms)

            def energy(x, y):
                displacement = builder.displacement_cart(x, y) * builder.mass_sqrt
                g1, g2 = [np.sum(displacement * b) for b in basis]
                q = np.sum(displacement * target)
                return (
                    0.01 * g1 * q * q
                    + 0.015 * g2 * q * q
                    + 0.007 * (g1 * g1 + g2 * g2) * q * q
                )

            points = {_key(x, y): energy(x, y) for x, y in SIX}
            thirds.append(_proxy(points))
            grid = np.array([[energy(x, y) for x in axis] for y in axis])
            fit = analyze_pair_grid(changed, grid, axis, axis, fit_window=1)
            fourths.append(fit["physics"]["phi_1122_mev_per_A4amu2"])
        assert np.isclose(np.linalg.norm(thirds), np.linalg.norm([20, 30]), atol=1e-8)
        assert np.isclose(sum(fourths), 56, atol=1e-8)
