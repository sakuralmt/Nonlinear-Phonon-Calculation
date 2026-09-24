"""Prophet finite-displacement Stage1 on a complete 2D q mesh.

Every non-Gamma branch is retained.  Point-group and time-reversal actions
only coalesce duplicate q points; they never select a little-group subset.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np

from .core import CONV_TO_THZ, decode_complex_mode, load_atoms_from_qe
from .phonon_eigenvectors import (
    dynamical_matrix,
    frequencies_and_vectors,
    real_space_force_constants,
)
from .prophet_backend import (
    make_prophet_calculator,
    process_resource_metrics,
    sha256_file,
    validate_atoms,
)
from .units import CONTRACT_VERSION, NORMALIZATION_VERSION, UNITS
from .structure_symmetry import classify_hex_qpoint, is_hexagonal_2d


def _phase_fix(vector: np.ndarray) -> np.ndarray:
    vector = np.asarray(vector, dtype=np.complex128).copy()
    pivot = int(np.argmax(np.abs(vector)))
    if abs(vector[pivot]) > 0:
        vector *= np.conjugate(vector[pivot]) / abs(vector[pivot])
    return vector


def _encode_mode(vector: np.ndarray, natoms: int):
    return [
        [[float(value.real), float(value.imag)] for value in row]
        for row in vector.reshape(natoms, 3)
    ]


def _degenerate_groups(freq: np.ndarray, tolerance_thz: float = 0.01):
    groups = []
    start = 0
    for end in range(1, len(freq) + 1):
        if end == len(freq) or freq[end] - freq[end - 1] > tolerance_thz:
            groups.append(list(range(start + 1, end + 1)))
            start = end
    return groups


def phonons_from_force_constants(phi: np.ndarray, masses: np.ndarray, mesh_n: int):
    records = []
    for i in range(mesh_n):
        for j in range(mesh_n):
            q = np.array([i / mesh_n, j / mesh_n, 0.0])
            matrix, hermitian_error = dynamical_matrix(phi, masses, q)
            freq, vectors = frequencies_and_vectors(matrix)
            vectors = np.column_stack(
                [_phase_fix(vectors[:, mode]) for mode in range(len(freq))]
            )
            records.append(
                {
                    "q_index": [i, j],
                    "q_frac": q.tolist(),
                    "freqs_thz": freq.tolist(),
                    "eigenvectors": [
                        _encode_mode(vectors[:, mode], len(masses))
                        for mode in range(len(freq))
                    ],
                    "degenerate_groups_one_based": _degenerate_groups(freq),
                    "hermitian_relative_error_before_symmetrizing": hermitian_error,
                }
            )
    return records


def gamma_mode_partition(records: list[dict], masses: np.ndarray) -> dict:
    """Identify Gamma translations from mass-weighted eigenvector overlap."""
    masses = np.asarray(masses, dtype=float)
    if masses.ndim != 1 or len(masses) < 1 or not np.all(masses > 0):
        raise ValueError("Positive atomic masses are required")
    gamma = next((row for row in records if row["q_index"] == [0, 0]), None)
    if gamma is None or len(gamma["eigenvectors"]) != 3 * len(masses):
        raise ValueError("A complete Gamma eigenvector basis is required")
    translations = np.zeros((3 * len(masses), 3), dtype=float)
    for atom, mass in enumerate(masses):
        translations[3 * atom : 3 * atom + 3] = np.eye(3) * np.sqrt(
            mass / np.sum(masses)
        )
    overlaps = []
    for encoded in gamma["eigenvectors"]:
        vector = decode_complex_mode(encoded).ravel()
        overlaps.append(float(np.sum(np.abs(translations.T @ vector) ** 2)))
    acoustic = sorted(np.argsort(overlaps)[-3:].tolist())
    optical = [index for index in range(3 * len(masses)) if index not in acoustic]
    if min(overlaps[index] for index in acoustic) < 0.9 or any(
        overlaps[index] > 0.1 for index in optical
    ):
        raise ValueError("Gamma acoustic and optical subspaces are not separable")
    return {
        "method": "mass_weighted_translation_subspace_overlap",
        "acoustic_modes_one_based": [index + 1 for index in acoustic],
        "optical_modes_one_based": [index + 1 for index in optical],
        "translation_overlap_squared": overlaps,
    }


def mode_pairs_from_phonons(
    records: list[dict], orbits: list[dict], masses: np.ndarray
):
    by_index = {tuple(row["q_index"]): row for row in records}
    gamma = by_index[(0, 0)]
    nmode = 3 * len(masses)
    optical = gamma_mode_partition(records, masses)["optical_modes_one_based"]
    pairs = []
    for orbit_number, orbit in enumerate(orbits, start=1):
        target = by_index[tuple(orbit["representative_index"])]
        q = target["q_frac"]
        qbar = [(-value) % 1.0 for value in q]
        self_conjugate = all(abs((2 * value) - round(2 * value)) < 1e-10 for value in q)
        label = orbit.get("representative_label", classify_hex_qpoint(np.asarray(q)))
        for gamma_index in (number - 1 for number in optical):
            for target_index in range(nmode):
                pair_code = (
                    f"Gamma_p0_m{gamma_index + 1}__{label}_q_"
                    f"{q[0]:.3f}_{q[1]:.3f}_{q[2]:.3f}_m{target_index + 1}"
                ).replace("-", "m")
                pairs.append(
                    {
                        "pair_code": pair_code,
                        "coupling_type": "Q_gamma*Q_q^2"
                        if self_conjugate
                        else "Q_gamma*Q_q*Q_-q",
                        "q_orbit_number": orbit_number,
                        "gamma_mode": {
                            "mode_code": f"Gamma_p0_m{gamma_index + 1}",
                            "point_index": 0,
                            "point_label": "Gamma",
                            "q_frac": [0.0, 0.0, 0.0],
                            "mode_index_zero_based": gamma_index,
                            "mode_number_one_based": gamma_index + 1,
                            "freq_thz": gamma["freqs_thz"][gamma_index],
                            "eigenvector": gamma["eigenvectors"][gamma_index],
                        },
                        "target_mode": {
                            "mode_code": f"{label}_p{orbit_number}_m{target_index + 1}",
                            "point_index": orbit_number,
                            "point_label": label,
                            "q_frac": q,
                            "qbar_frac": qbar,
                            "self_conjugate": self_conjugate,
                            "mode_index_zero_based": target_index,
                            "mode_number_one_based": target_index + 1,
                            "freq_thz": target["freqs_thz"][target_index],
                            "eigenvector_q": target["eigenvectors"][target_index],
                            "eigenvector_qbar_by_conjugation": [
                                [[component[0], -component[1]] for component in atom]
                                for atom in target["eigenvectors"][target_index]
                            ],
                        },
                    }
                )
    codes = [pair["pair_code"] for pair in pairs]
    if len(codes) != len(set(codes)):
        raise ValueError("Nonunique pair codes in the selected q mesh")
    return pairs


def gamma_subspaces(
    records: list[dict], masses: np.ndarray, tolerance_thz: float = 0.01
) -> list[list[int]]:
    """Group only the translationally orthogonal Gamma optical modes."""
    if tolerance_thz <= 0:
        raise ValueError("A positive degeneracy tolerance is required")
    gamma = next((row for row in records if row["q_index"] == [0, 0]), None)
    if gamma is None or len(gamma["freqs_thz"]) != 3 * len(masses):
        raise ValueError("A complete Gamma spectrum is required")
    optical = gamma_mode_partition(records, masses)["optical_modes_one_based"]
    groups = _degenerate_groups(
        np.asarray([gamma["freqs_thz"][number - 1] for number in optical]),
        tolerance_thz,
    )
    return [[optical[number - 1] for number in group] for group in groups]


def equivalent_pair_channels(
    records: list[dict],
    orbits: list[dict],
    pairs: list[dict],
    masses: np.ndarray,
    tolerance_thz: float = 0.01,
) -> dict:
    """Precompute complete Gamma-subspace channels at representative finite q points.

    Q orbits are supplied by the structure-derived symmetry check. A channel
    contains every Gamma component. Finite-q near-degeneracies are reported as
    subspaces, because one branch's coupling is basis dependent.
    """
    groups = gamma_subspaces(records, masses, tolerance_thz)
    by_q = {tuple(record["q_index"]): record for record in records}
    nmode = 3 * len(masses)
    optical = gamma_mode_partition(records, masses)["optical_modes_one_based"]
    expected = len(orbits) * nmode * len(optical)
    if len(pairs) != expected:
        raise ValueError(f"Expected {expected} complete mode pairs; got {len(pairs)}")
    by_key = {}
    for pair in pairs:
        key = (
            pair["q_orbit_number"],
            pair["target_mode"]["mode_number_one_based"],
            pair["gamma_mode"]["mode_number_one_based"],
        )
        if key in by_key:
            raise ValueError(f"Duplicate mode pair for {key}")
        by_key[key] = pair
    if set(by_key) != {
        (orbit_number, target_number, gamma_number)
        for orbit_number in range(1, len(orbits) + 1)
        for target_number in range(1, nmode + 1)
        for gamma_number in optical
    }:
        raise ValueError("Mode pairs do not cover every orbit and branch combination")
    channels = []
    covered = set()
    for orbit_number, orbit in enumerate(orbits, start=1):
        target_record = by_q[tuple(orbit["representative_index"])]
        for target_number in range(1, nmode + 1):
            target_group = next(
                group
                for group in target_record["degenerate_groups_one_based"]
                if target_number in group
            )
            for group in groups:
                members = [
                    by_key[(orbit_number, target_number, gamma_number)]
                    for gamma_number in group
                ]
                codes = [member["pair_code"] for member in members]
                if len(set(codes)) != len(codes) or covered.intersection(codes):
                    raise ValueError(
                        "A mode pair belongs to multiple physical channels"
                    )
                covered.update(codes)
                channels.append(
                    {
                        "channel_code": f"{members[0]['target_mode']['mode_code']}__Gamma_"
                        + "_".join(map(str, group)),
                        "q_orbit_number": orbit_number,
                        "representative_q_index": orbit["representative_index"],
                        "q_orbit_member_indices": orbit["members_index"],
                        "q_mode_code": members[0]["target_mode"]["mode_code"],
                        "target_mode_number_one_based": target_number,
                        "finite_q_degenerate_group_one_based": target_group,
                        "basis_dependent_target_branch": len(target_group) > 1,
                        "gamma_modes_one_based": group,
                        "pair_codes": codes,
                        "requires_all_gamma_components": len(group) > 1,
                    }
                )
    if len(covered) != len(pairs) or covered != {pair["pair_code"] for pair in pairs}:
        raise ValueError("Physical channels do not cover each pair exactly once")
    return {
        "kind": "gamma_subspace_q_orbit_channels",
        "version": 1,
        "gamma_degeneracy_threshold_thz": tolerance_thz,
        "gamma_groups_one_based": groups,
        "q_orbit_count": len(orbits),
        "finite_q_point_count": sum(orbit["size"] for orbit in orbits),
        "channel_count": len(channels),
        "pair_count": len(pairs),
        "finite_q_branch_mapping_at_orbit_members": "recorded_in_q_orbit_mode_maps",
        "score_after_stage2": "Euclidean norm of Phi122 over all Gamma components; finite-q degenerate branches remain basis dependent",
        "channels": channels,
    }


def run_prophet_stage1(
    structure: Path,
    checkpoint: str | Path,
    output_dir: Path,
    *,
    mesh_n: int = 6,
    step: float = 0.01,
    device: str = "cpu",
    convergence_step: float | None = 0.005,
    geometry_source: str = "model_relaxed",
    gamma_degeneracy_thz: float = 0.01,
    phonon_engine: str = "phonopy",
    phonopy_asr: bool = True,
):
    if not np.isfinite(gamma_degeneracy_thz) or gamma_degeneracy_thz <= 0:
        raise ValueError("Gamma degeneracy threshold must be finite and positive")
    if phonon_engine not in {"custom", "phonopy"}:
        raise ValueError("phonon_engine must be custom or phonopy")
    structure = Path(structure).resolve()
    output_dir = Path(output_dir).resolve()
    if geometry_source != "model_relaxed":
        raise ValueError("Stable Stage1 requires the Prophet-relaxed structure")
    relaxation_path = output_dir / "relax" / "relax_summary.json"
    if not relaxation_path.is_file():
        raise ValueError(f"Missing model relaxation provenance: {relaxation_path}")
    relaxation = json.loads(relaxation_path.read_text())
    if (
        relaxation.get("backend") != "prophet"
        or relaxation.get("optimized_structure_sha256") != sha256_file(structure)
        or Path(relaxation.get("optimized_structure", "")).resolve() != structure
    ):
        raise ValueError("Prophet Stage1 structure differs from its model relaxation")
    if (
        phonon_engine == "phonopy"
        and output_dir.exists()
        and any(
            (output_dir / name).exists()
            for name in (
                "phonon_dataset.json",
                "mode_pairs.selected.json",
                "force_constants.npz",
            )
        )
    ):
        raise ValueError(
            "Phonopy Stage1 needs a fresh output directory; existing results are not overwritten"
        )
    primitive = load_atoms_from_qe(structure)
    if not bool(np.all(primitive.pbc)):
        raise ValueError("Stage1 needs periodic x/y/z with explicit monolayer vacuum")
    hexagonal, geometry = is_hexagonal_2d(primitive.cell.array, 0.05, 3.0)
    if not hexagonal:
        raise ValueError(
            f"6x6 q-orbit reduction requires a hexagonal 2D cell: {geometry}"
        )
    calculator, model_meta = make_prophet_calculator(checkpoint, device, primitive)
    validate_atoms(primitive, set(model_meta["supported_atomic_numbers"]))
    start = time.perf_counter()
    if phonon_engine == "phonopy":
        from .phonopy_bridge import (
            apply_phonopy_asr,
            force_constants_from_calculator,
            phonons_from_phonopy,
        )

        raw_phi, fit_phonon = force_constants_from_calculator(
            primitive, calculator, mesh_n, step
        )
        phi, asr = (
            apply_phonopy_asr(fit_phonon, mesh_n) if phonopy_asr else (raw_phi, None)
        )
        records, _ = phonons_from_phonopy(primitive, phi, mesh_n)
    else:
        phi = real_space_force_constants(primitive, calculator, mesh_n, step)
        records = phonons_from_force_constants(phi, primitive.get_masses(), mesh_n)
    force_elapsed = time.perf_counter() - start
    from .structure_symmetry import structure_q_orbits

    orbits, symmetry = structure_q_orbits(primitive, records, mesh_n)
    pairs = mode_pairs_from_phonons(records, orbits, primitive.get_masses())
    convergence = None
    if convergence_step is not None:
        if phonon_engine == "phonopy":
            smaller, smaller_phonon = force_constants_from_calculator(
                primitive, calculator, mesh_n, convergence_step
            )
            if phonopy_asr:
                smaller, _ = apply_phonopy_asr(smaller_phonon, mesh_n)
            compare_records, _ = phonons_from_phonopy(primitive, smaller, mesh_n)
        else:
            smaller = real_space_force_constants(
                primitive, calculator, mesh_n, convergence_step
            )
            compare_records = phonons_from_force_constants(
                smaller, primitive.get_masses(), mesh_n
            )
        checks = {(0, 0), *(tuple(orbit["representative_index"]) for orbit in orbits)}
        differences = [
            abs(a - b)
            for left, right in zip(records, compare_records)
            if tuple(left["q_index"]) in checks
            for a, b in zip(left["freqs_thz"], right["freqs_thz"])
        ]
        convergence = {
            "second_step_angstrom": convergence_step,
            "checked_q_count": len(checks),
            "max_frequency_change_thz": max(differences),
            "median_frequency_change_thz": float(np.median(differences)),
            "max_acoustic_row_sum_residual_ev_per_A2": float(
                np.max(np.abs(np.sum(smaller, axis=(0, 1, 4))))
            ),
            "max_hermitian_relative_error": max(
                record["hermitian_relative_error_before_symmetrizing"]
                for record in compare_records
            ),
        }
    output_dir.mkdir(parents=True, exist_ok=True)
    if phonon_engine == "phonopy":
        fit_phonon.save(
            str(output_dir / "phonopy_params.yaml"), settings={"force_constants": True}
        )
    force_arrays = {"force_constants_ev_per_A2": phi}
    if phonon_engine == "phonopy" and phonopy_asr:
        force_arrays["force_constants_raw_ev_per_A2"] = raw_phi
    if convergence_step is not None:
        force_arrays["force_constants_convergence_ev_per_A2"] = smaller
    np.savez_compressed(output_dir / "force_constants.npz", **force_arrays)
    source = {
        "backend": "prophet",
        "model": model_meta,
        "structure": str(structure),
        "structure_sha256": sha256_file(structure),
        "natoms_primitive": len(primitive),
        "symbols": primitive.get_chemical_symbols(),
        "masses_amu": primitive.get_masses().tolist(),
        "q_grid": [mesh_n, mesh_n, 1],
        "finite_difference_step_angstrom": step,
        "geometry_source": geometry_source,
        "gamma_degeneracy_threshold_thz": gamma_degeneracy_thz,
        "relaxation": {
            "source_structure_sha256": relaxation["source_structure_sha256"],
            "optimized_structure_sha256": relaxation["optimized_structure_sha256"],
            "summary": str(relaxation_path),
            "summary_sha256": sha256_file(relaxation_path),
            "protocol_version": relaxation["relaxation_protocol_version"],
        },
        "symmetry": symmetry,
        "gamma_mode_selection": gamma_mode_partition(records, primitive.get_masses()),
        "normalization_version": NORMALIZATION_VERSION,
        "units": UNITS,
    }
    if phonon_engine == "phonopy":
        from phonopy import __version__ as phonopy_version

        source["phonon_engine"] = {
            "name": "phonopy",
            "version": phonopy_version,
            "displacements": "Cartesian +/- finite difference, no diagonal or space-group reduction",
            "acoustic_sum_rule": phonopy_asr,
            "non_analytical_correction": False,
            "frequency_factor": CONV_TO_THZ,
            "phonopy_params_sha256": sha256_file(output_dir / "phonopy_params.yaml"),
            "eigenvector_gauge": "v3_cell_periodic_phonopy_gamma_legacy_finiteq_tie_1e-3",
        }
    row_sum = float(np.max(np.abs(np.sum(phi, axis=(0, 1, 4)))))
    row_sum_ratio = row_sum / max(float(np.max(np.abs(phi))), 1e-12)
    hermitian_error = max(
        record["hermitian_relative_error_before_symmetrizing"] for record in records
    )
    quality_flags = []
    if row_sum_ratio > 1e-3:
        quality_flags.append("acoustic_row_sum_above_0.1_percent_of_max_fc")
    if hermitian_error > 1e-3:
        quality_flags.append("dynamical_matrix_hermiticity_above_0.1_percent")
    dataset = {
        "kind": "prophet_phonon_mesh",
        "version": CONTRACT_VERSION,
        "source": source,
        "q_points": records,
        "q_orbits": orbits,
        "diagnostics": {
            "force_evaluation_count": 6 * len(primitive),
            "force_elapsed_seconds": force_elapsed,
            "total_elapsed_seconds": time.perf_counter() - start,
            "resources": process_resource_metrics(device),
            "gamma_acoustic_frequencies_thz": records[0]["freqs_thz"][:3],
            "max_hermitian_relative_error": hermitian_error,
            # Translational invariance sums over source cells and source atoms
            # for each fixed target atom and Cartesian component.
            "max_acoustic_row_sum_residual_ev_per_A2": row_sum,
            "acoustic_row_sum_ratio_to_max_fc": row_sum_ratio,
            "max_net_force_column_sum_residual_ev_per_A2": float(
                np.max(np.abs(np.sum(phi, axis=(0, 1, 2))))
            ),
            "convergence": convergence,
            "phonopy_asr": asr if phonon_engine == "phonopy" else None,
            "quality_flags": quality_flags,
        },
    }
    (output_dir / "phonon_dataset.json").write_text(
        json.dumps(dataset, indent=2) + "\n"
    )
    pair_payload = {
        "kind": "mode_pairs_qgamma_qpair",
        "version": CONTRACT_VERSION,
        "source": source,
        "selection": "gamma_optical_and_momentum_conservation_only",
        "finite_q_orbits": orbits,
        "equivalent_pair_channels": equivalent_pair_channels(
            records, orbits, pairs, primitive.get_masses(), gamma_degeneracy_thz
        ),
        "pairs": pairs,
    }
    pair_file = output_dir / "mode_pairs.selected.json"
    pair_file.write_text(json.dumps(pair_payload, indent=2) + "\n")
    return (
        pair_file,
        output_dir / "phonon_dataset.json",
        output_dir / "force_constants.npz",
    )
