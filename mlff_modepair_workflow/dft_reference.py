"""Import audited DFPT modes and evaluate selected PES channels with MatterSim.

This reference path preserves the DFT geometry and basis. It does not relax
the structure or calculate DFT. Selected reference channels are evaluated
without another screening reduction; full imports use the shared symmetry pipeline.
"""

from __future__ import annotations

import io
import json
import math
import os
import re
import time
from pathlib import Path

import numpy as np
from ase.io.espresso import read_espresso_in, read_fortran_namelist

from .core import (
    ModePairFrozenPhononBuilder,
    analyze_pair_grid,
    decode_complex_mode,
    load_atoms_from_qe,
)
from .prophet_backend import sha256_file, process_resource_metrics
from .screening_stage2 import _write, _locked_identity
from .units import CONTRACT_VERSION, NORMALIZATION_VERSION, UNITS


def read_dft_structure(path: Path):
    """Read explicit cells or QE hexagonal ibrav=4 (A/C or celldm units)."""
    if path.suffix.lower() in {".xyz", ".extxyz", ".cif", ".traj", ".vasp", ".poscar"}:
        atoms = load_atoms_from_qe(path)
    else:
        text = path.read_text()
        data, _ = read_fortran_namelist(io.StringIO(text))
        system = data.get("system", {})
        if system.get("ibrav") == 4:
            from ase.units import Bohr

            if "celldm(1)" in system:
                a = float(system["celldm(1)"]) * Bohr
                c = a * float(system["celldm(3)"])
            else:
                a, c = float(system["a"]), float(system["c"])
            if (
                not np.isfinite([a, c]).all()
                or min(a, c) <= 0
                or "CELL_PARAMETERS" in text.upper()
            ):
                raise ValueError("Invalid or conflicting hexagonal QE cell")
            text = re.sub(r"\bibrav\s*=\s*4\b", "ibrav=0", text, flags=re.I)
            text += (
                f"\nCELL_PARAMETERS angstrom\n{a:.16g} 0 0\n"
                f"{-a/2:.16g} {a*math.sqrt(3)/2:.16g} 0\n0 0 {c:.16g}\n"
            )
        atoms = read_espresso_in(io.StringIO(text))
    atoms.set_constraint()
    if not atoms.pbc.all() or not np.isfinite(atoms.positions).all():
        raise ValueError("DFT reference requires a finite periodic structure")
    return atoms


def import_dft_stage1(
    structure: Path,
    dataset_path: Path,
    selection_path: Path | None,
    output: Path,
    xc: str = "PBE",
    gamma_degeneracy_thz: float = 0.01,
) -> Path:
    """Validate selected modes against an audited full DFPT export and freeze provenance."""
    dataset = json.loads(dataset_path.read_text())
    selection = json.loads(selection_path.read_text()) if selection_path else None
    provenance = dataset["source_stage1"]
    atoms = read_dft_structure(structure)
    orbits = symmetry = None
    if selection is None:
        from .prophet_stage1 import mode_pairs_from_phonons, _degenerate_groups
        from .structure_symmetry import structure_q_orbits

        records = dataset["q_points"]
        for record in records:
            record["degenerate_groups_one_based"] = _degenerate_groups(
                np.asarray(record["freqs_thz"]), gamma_degeneracy_thz
            )
        mesh = provenance["phonon_grid"]
        if mesh[0] != mesh[1] or mesh[2] != 1:
            raise ValueError("DFT screening needs a square 2D q mesh")
        orbits, symmetry = structure_q_orbits(atoms, records, mesh[0])
        all_pairs = mode_pairs_from_phonons(records, orbits, atoms.get_masses())
        selection = {
            "normalization": NORMALIZATION_VERSION,
            "units": UNITS,
            "pbe_structure_input_sha256": provenance["input_sha256"]["scf/scf.inp"],
            "pbe_phonon_eig_sha256": dataset["eig_sha256"],
            "selected": [
                {"pair": pair, "lda_channel": pair["pair_code"], "rank": i + 1}
                for i, pair in enumerate(all_pairs)
            ],
        }
    if (
        selection.get("normalization") != NORMALIZATION_VERSION
        or selection.get("units") != UNITS
    ):
        raise ValueError("DFT selection units or normalization mismatch")
    structure_sha = sha256_file(structure)
    if selection.get("pbe_structure_input_sha256") != structure_sha:
        raise ValueError("DFT structure hash mismatch")
    if (
        provenance["input_sha256"].get("scf/scf.inp") != structure_sha
        or dataset["eig_sha256"] != selection["pbe_phonon_eig_sha256"]
        or xc.upper() != "PBE"
        or "PBE" not in provenance["electronic_reference"].upper()
    ):
        raise ValueError("DFPT source, eigenvector hash or functional mismatch")
    mesh = provenance["phonon_grid"]
    points = dataset["q_points"]
    if len(points) != int(np.prod(mesh)):
        raise ValueError("Incomplete DFPT mesh")
    qkeys = [tuple(np.round(np.asarray(p["q_frac"]) % 1, 8)) for p in points]
    if len(set(qkeys)) != len(qkeys):
        raise ValueError("Duplicate DFPT q point")
    from .prophet_stage1 import gamma_mode_partition

    optical = set(
        gamma_mode_partition(points, atoms.get_masses())["optical_modes_one_based"]
    )
    if optical != set(dataset["gamma_partition"]["optical_modes_one_based"]):
        raise ValueError("DFPT Gamma acoustic/optical classification is inconsistent")
    pairs = []
    channels = []
    for item in selection["selected"]:
        pair = item["pair"]
        if pair["gamma_mode"]["mode_number_one_based"] not in optical:
            raise ValueError("Gamma acoustic modes are not reference channels")
        for mode, vector_key in (
            (pair["gamma_mode"], "eigenvector"),
            (pair["target_mode"], "eigenvector_q"),
        ):
            q = np.asarray(mode["q_frac"], float)
            matches = [
                p
                for p in points
                if np.max(abs((np.asarray(p["q_frac"]) - q + 0.5) % 1 - 0.5)) < 1e-7
            ]
            if len(matches) != 1:
                raise ValueError("Selected mode q point is missing or ambiguous")
            point = matches[0]
            index = mode["mode_number_one_based"] - 1
            vector = decode_complex_mode(mode[vector_key])
            expected = decode_complex_mode(point["eigenvectors"][index])
            if (
                vector.shape != (len(atoms), 3)
                or not np.isfinite(vector).all()
                or not np.isclose(np.linalg.norm(vector), 1, atol=1e-7)
                or abs(np.vdot(vector, expected)) ** 2 < 1 - 1e-7
                or abs(mode["freq_thz"] - point["freqs_thz"][index]) > 1e-5
            ):
                raise ValueError("Selected DFT mode differs from the DFPT export")
        q = np.asarray(pair["target_mode"]["q_frac"])
        qbar = np.asarray(pair["target_mode"]["qbar_frac"])
        if (
            np.max(abs(q - np.rint(q))) < 1e-8
            or np.max(abs(q + qbar - np.rint(q + qbar))) > 1e-7
        ):
            raise ValueError("Reference pair must be Gamma-q-minus-q with finite q")
        builder = ModePairFrozenPhononBuilder(pair, atoms)
        for g, f in ((1, 0), (0, 1)):
            norm = np.sum(
                builder.supercell.get_masses()[:, None]
                * builder.displacement_cart(g, f) ** 2
            )
            if not np.isclose(norm, 1, atol=1e-9):
                raise ValueError("DFT real-mode normalization failure")
        pairs.append(pair)
        channels.append(
            {
                "pair_code": pair["pair_code"],
                "physical_channel": item["lda_channel"],
                "reference_rank": item["rank"],
            }
        )
    if (not pairs and selection_path is not None) or len(
        {p["pair_code"] for p in pairs}
    ) != len(pairs):
        raise ValueError("Empty or duplicated DFT selection")
    payload = {
        "version": CONTRACT_VERSION,
        "scope": "selected_dft_reference_channels",
        "units": UNITS,
        "pairs": pairs,
        "channels": channels,
        "source": {
            "backend": "qe-dfpt",
            "geometry_source": "dft_relaxed",
            "xc": xc.upper(),
            "structure_sha256": structure_sha,
            "dataset_sha256": sha256_file(dataset_path),
            "selection_sha256": sha256_file(selection_path) if selection_path else None,
            "normalization_version": NORMALIZATION_VERSION,
            "dfpt_provenance": provenance,
        },
    }
    if selection_path is None:
        from .prophet_stage1 import equivalent_pair_channels, gamma_mode_partition

        payload["scope"] = "complete_dft_screening_mesh"
        payload["selection"] = "gamma_optical_and_momentum_conservation_only"
        payload["finite_q_orbits"] = orbits
        payload["equivalent_pair_channels"] = equivalent_pair_channels(
            records, orbits, pairs, atoms.get_masses(), gamma_degeneracy_thz
        )
        payload["source"].update(
            {
                "symmetry": symmetry,
                "natoms_primitive": len(atoms),
                "gamma_mode_selection": gamma_mode_partition(
                    records, atoms.get_masses()
                ),
                "gamma_degeneracy_threshold_thz": gamma_degeneracy_thz,
            }
        )
    output.mkdir(parents=True, exist_ok=True)
    path = output / (
        "mode_pairs.reference.json" if selection_path else "mode_pairs.selected.json"
    )
    if path.exists() and json.loads(path.read_text()) != payload:
        raise ValueError("DFT Stage1 output already belongs to another source")
    _write(path, payload)
    return path


def reference_identity(
    payload: dict,
    pair_path: Path,
    structure: Path,
    checkpoint: Path,
    reference_path: Path,
    device: str,
) -> dict:
    from .mattersim_backend import ENERGY_ACCUMULATION

    if (
        payload.get("version") != CONTRACT_VERSION
        or payload.get("scope") != "selected_dft_reference_channels"
        or payload.get("units") != UNITS
        or payload["source"].get("geometry_source") != "dft_relaxed"
        or payload["source"].get("normalization_version") != NORMALIZATION_VERSION
        or payload["source"].get("structure_sha256") != sha256_file(structure)
    ):
        raise ValueError("Invalid DFT reference contract or structure")
    return {
        "mode_pairs_sha256": sha256_file(pair_path),
        "structure_sha256": sha256_file(structure),
        "checkpoint_sha256": sha256_file(checkpoint),
        "reference_results_sha256": sha256_file(reference_path),
        "contract_version": CONTRACT_VERSION,
        "normalization_version": NORMALIZATION_VERSION,
        "energy_accumulation": ENERGY_ACCUMULATION,
        "device": device,
    }


def run_reference(
    pair_path: Path,
    structure: Path,
    checkpoint: Path,
    reference_path: Path,
    output: Path,
    device: str = "cpu",
    calculator=None,
) -> Path:
    """Evaluate exactly the existing QE coordinates; resume atom forces and energies by point."""
    import fcntl
    from .mattersim_backend import make_mattersim_calculator

    started = time.perf_counter()
    payload = json.loads(pair_path.read_text())
    identity = reference_identity(
        payload, pair_path, structure, checkpoint, reference_path, device
    )
    reference = json.loads(reference_path.read_text())
    if (
        reference["units"] != UNITS
        or reference["mode_selection_sha256"] != payload["source"]["selection_sha256"]
    ):
        raise ValueError("QE PES and selected DFT modes have different provenance")
    rows = {row["pbe_pair_code"]: row for row in reference["rows"]}
    if set(rows) != {p["pair_code"] for p in payload["pairs"]} or len(rows) != len(
        reference["rows"]
    ):
        raise ValueError("QE reference channel set mismatch")
    _locked_identity(output, identity)
    # One process per material. A second invocation must not overwrite checkpoints.
    with (output / ".reference.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        atoms = read_dft_structure(structure)
        load_started = time.perf_counter()
        if calculator is None:
            calculator, model = make_mattersim_calculator(checkpoint, device, atoms)
        else:
            model = {"backend": "test_calculator"}
        loading = time.perf_counter() - load_started
        results = []
        for pair in payload["pairs"]:
            builder = ModePairFrozenPhononBuilder(pair, atoms)
            ref = rows[pair["pair_code"]]
            points = ref["source_points"]
            coordinates = [(float(p["q_gamma"]), float(p["q_finite"])) for p in points]
            if (
                len(set(coordinates)) != len(points)
                or len(points) != ref["expected_base_points"]
            ):
                raise ValueError("Missing or repeated QE PES coordinates")
            xs, ys = [sorted({c[k] for c in coordinates}) for k in (0, 1)]
            if (
                set(coordinates) != {(x, y) for x in xs for y in ys}
                or (0.0, 0.0) not in coordinates
            ):
                raise ValueError(
                    "QE reference must contain a full Cartesian grid and origin"
                )
            if xs != ys or xs not in [
                np.linspace(-1, 1, 5).tolist(),
                np.linspace(-2, 2, 9).tolist(),
            ]:
                raise ValueError(
                    "Unsupported reference grid: expected central 5x5 or full 9x9"
                )
            path = output / "points" / f"{pair['pair_code']}.json"
            saved = (
                json.loads(path.read_text())
                if path.exists()
                else {
                    "identity": identity,
                    "pair_code": pair["pair_code"],
                    "points": {},
                }
            )
            if saved["identity"] != identity or saved["pair_code"] != pair["pair_code"]:
                raise ValueError("Point checkpoint source mismatch")
            for g, q in coordinates:
                key = f"{g:+.1f},{q:+.1f}"
                if key not in saved["points"]:
                    displaced = builder.build_atoms(g, q)
                    displaced.calc = calculator
                    point_started = time.perf_counter()
                    energy = float(displaced.get_potential_energy())
                    forces = displaced.get_forces(apply_constraint=False)
                    if not np.isfinite(energy) or not np.isfinite(forces).all():
                        raise ValueError("Nonfinite MatterSim energy or force")
                    saved["points"][key] = {
                        "energy_ev": energy,
                        "forces_ev_per_A": forces.tolist(),
                        "seconds": time.perf_counter() - point_started,
                    }
                    _write(path, saved)
                value = saved["points"][key]
                if (
                    not np.isfinite(value["energy_ev"])
                    or np.asarray(value["forces_ev_per_A"]).shape
                    != (builder.nat_super, 3)
                    or not np.isfinite(value["forces_ev_per_A"]).all()
                ):
                    raise ValueError("Corrupted point checkpoint")
            energies = np.array(
                [
                    [saved["points"][f"{g:+.1f},{q:+.1f}"]["energy_ev"] for g in xs]
                    for q in ys
                ]
            )
            by_coord = {(p["q_gamma"], p["q_finite"]): p for p in points}
            qe = np.array([[by_coord[g, q]["energy_ev"] for g in xs] for q in ys])
            center = xs.index(0.0)
            error = (
                (energies - energies[center, center]) - (qe - qe[center, center])
            ) * 1000
            force_error = np.array(
                [
                    np.asarray(saved["points"][f"{g:+.1f},{q:+.1f}"]["forces_ev_per_A"])
                    - by_coord[g, q]["forces_ev_per_A"]
                    for g, q in coordinates
                ]
            )
            fit = analyze_pair_grid(
                pair, energies, np.asarray(xs), np.asarray(ys), fit_window=1
            )
            qe_fit = analyze_pair_grid(
                pair, qe, np.asarray(xs), np.asarray(ys), fit_window=1
            )
            if fit["fit_design_rank"] != 13 or qe_fit["fit_design_rank"] != 13:
                raise ValueError("Rank-deficient reference fit")
            result = {
                "pair_code": pair["pair_code"],
                "points": len(points),
                "axis": xs,
                "grid_ev": energies.tolist(),
                "center_fit": fit,
                "qe_center_fit": qe_fit,
                "relative_energy_mae_mev": float(np.mean(abs(error))),
                "relative_energy_rmse_mev": float(np.sqrt(np.mean(error**2))),
                "force_mae_ev_per_A": float(np.mean(abs(force_error))),
                "force_rmse_ev_per_A": float(np.sqrt(np.mean(force_error**2))),
            }
            mask = np.asarray([abs(c[0]) <= 1 and abs(c[1]) <= 1 for c in coordinates])
            center_error = np.asarray(
                [error[ys.index(q), xs.index(g)] for g, q in coordinates]
            )[mask]
            result["central_relative_energy_mae_mev"] = float(
                np.mean(abs(center_error))
            )
            result["central_relative_energy_rmse_mev"] = float(
                np.sqrt(np.mean(center_error**2))
            )
            result["central_force_mae_ev_per_A"] = float(
                np.mean(abs(force_error[mask]))
            )
            result["central_force_rmse_ev_per_A"] = float(
                np.sqrt(np.mean(force_error[mask] ** 2))
            )
            if len(xs) == 9:
                result["wide_fit"] = analyze_pair_grid(
                    pair, energies, np.asarray(xs), np.asarray(ys), fit_window=2
                )
            results.append(result)
        result_path = output / "reference_result.json"
        _write(
            result_path,
            {
                "scope": "PBE DFT Stage1 + MatterSim Stage2, identical QE configurations",
                "identity": identity,
                "units": UNITS,
                "channels": payload["channels"],
                "model": model,
                "rows": results,
                "complete_pairs": len(results),
                "complete_points": sum(r["points"] for r in results),
                "loading_seconds": loading,
                "wall_seconds": time.perf_counter() - started,
                "resources": process_resource_metrics(device),
                "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
                "omp_num_threads": os.environ.get("OMP_NUM_THREADS"),
            },
        )
        return result_path
