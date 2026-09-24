"""Common constrained relaxation protocol for model-replacement comparisons."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import numpy as np

from .core import load_atoms_from_qe
from .prophet_backend import sha256_file


RELAXATION_PROTOCOL_VERSION = "qe_flags_bfgs_isotropic_inplane_v1"


def write_relaxed_structure_from_qe_template(
    source: Path, destination: Path, atoms
) -> None:
    """Replace geometry in a QE template while preserving its relaxation flags."""
    lines = Path(source).read_text().splitlines()
    nat = len(atoms)
    cell_header = next(
        (
            i
            for i, line in enumerate(lines)
            if line.strip().upper().startswith("CELL_PARAMETERS")
        ),
        None,
    )
    atom_header = next(
        (
            i
            for i, line in enumerate(lines)
            if line.strip().upper().startswith("ATOMIC_POSITIONS")
        ),
        None,
    )
    if cell_header is None or atom_header is None:
        raise ValueError("QE template requires CELL_PARAMETERS and ATOMIC_POSITIONS")
    for offset, row in enumerate(atoms.cell.array, start=1):
        lines[cell_header + offset] = "  " + "  ".join(f"{value:.15f}" for value in row)
    lines[cell_header] = "CELL_PARAMETERS angstrom"
    lines[atom_header] = "ATOMIC_POSITIONS crystal"
    fixed_flags = []
    for line in lines[atom_header + 1 : atom_header + 1 + nat]:
        fields = line.split()
        fixed_flags.append(fields[4:7] if len(fields) >= 7 else [])
    for offset, (symbol, position, flags) in enumerate(
        zip(atoms.get_chemical_symbols(), atoms.get_scaled_positions(), fixed_flags),
        start=1,
    ):
        lines[atom_header + offset] = (
            f"{symbol}  "
            + "  ".join(f"{value:.15f}" for value in position)
            + ("  " + " ".join(flags) if flags else "")
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", dir=destination.parent, prefix=".relaxed-", delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write("\n".join(lines) + "\n")
    try:
        recovered = load_atoms_from_qe(temporary)
        if not np.allclose(
            recovered.cell.array, atoms.cell.array, atol=1e-10
        ) or not np.allclose(recovered.positions, atoms.positions, atol=1e-9):
            raise AssertionError(
                "Relaxed QE geometry did not round-trip through the input parser"
            )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def relax_structure_with_calculator(
    source: Path, output_dir: Path, calculator, model_meta: dict, backend: str
):
    """Match Prophet's constrained BFGS plus isotropic in-plane scale search."""
    from ase.optimize import BFGS
    from scipy.optimize import minimize_scalar

    source = Path(source).resolve()
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "relax_summary.json"
    destination = output_dir / "optimized_structure.scf.inp"
    source_hash = sha256_file(source)
    if summary_path.exists():
        summary = json.loads(summary_path.read_text())
        previous_model = summary.get("model", {})
        if (
            summary.get("relaxation_protocol_version") != RELAXATION_PROTOCOL_VERSION
            or summary.get("backend") != backend
            or summary.get("source_structure_sha256") != source_hash
            or any(
                previous_model.get(key) != model_meta.get(key)
                for key in (
                    "checkpoint_sha256",
                    "verified_source_tree_sha256",
                    "actual_source_commit",
                )
            )
            or not destination.is_file()
            or summary.get("optimized_structure_sha256") != sha256_file(destination)
        ):
            raise ValueError(
                f"Existing model relaxation differs from requested inputs: {summary_path}"
            )
        return destination, summary
    if destination.exists():
        raise ValueError(
            f"Relaxed structure exists without a matching summary: {destination}"
        )
    primitive = load_atoms_from_qe(source)
    original_cell = primitive.cell.array.copy()
    trials = []
    structures = []

    def evaluate_scale(scale: float):
        atoms = primitive.copy()
        cell = original_cell.copy()
        cell[:2, :] *= scale
        atoms.set_cell(cell, scale_atoms=True)
        atoms.calc = calculator
        index = len(trials)
        optimizer = BFGS(atoms, logfile=str(output_dir / f"trial_{index:02d}.log"))
        converged = bool(optimizer.run(fmax=0.02, steps=200))
        energy = float(atoms.get_potential_energy())
        constrained_force = float(np.max(np.linalg.norm(atoms.get_forces(), axis=1)))
        raw_force = float(
            np.max(np.linalg.norm(atoms.get_forces(apply_constraint=False), axis=1))
        )
        trials.append(
            {
                "scale": float(scale),
                "energy_ev": energy,
                "max_force_ev_per_A": constrained_force,
                "max_raw_force_ev_per_A": raw_force,
                "converged": converged,
            }
        )
        if not converged or not np.isfinite(energy):
            return float("inf")
        structures.append(atoms)
        return energy

    search = minimize_scalar(
        evaluate_scale,
        bounds=(0.94, 1.06),
        method="bounded",
        options={"xatol": 1e-3, "maxiter": 14},
    )
    if not search.success or not structures:
        raise RuntimeError(
            f"{backend} structural relaxation failed; inspect trial logs in {output_dir}"
        )
    best_index = min(
        (i for i, row in enumerate(trials) if row["converged"]),
        key=lambda i: trials[i]["energy_ev"],
    )
    best_trial = trials[best_index]
    best = min(structures, key=lambda atoms: atoms.get_potential_energy())
    best_scale = best_trial["scale"]
    if best_scale <= 0.941 or best_scale >= 1.059:
        raise RuntimeError(
            f"{backend} in-plane lattice optimum reached the search bound"
        )
    write_relaxed_structure_from_qe_template(source, destination, best)
    summary = {
        "kind": f"{backend}_model_relaxation",
        "backend": backend,
        "relaxation_protocol_version": RELAXATION_PROTOCOL_VERSION,
        "source_structure": str(source),
        "source_structure_sha256": source_hash,
        "optimized_structure": str(destination),
        "optimized_structure_sha256": sha256_file(destination),
        "model": model_meta,
        "best_inplane_scale": best_scale,
        "best_energy_ev": float(best.get_potential_energy()),
        "vacuum_c_fixed": True,
        "qe_relaxation_flags_preserved": True,
        "bfgs_fmax_ev_per_A": 0.02,
        "bfgs_max_steps": 200,
        "inplane_scale_bounds": [0.94, 1.06],
        "trials": trials,
    }
    with tempfile.NamedTemporaryFile(
        mode="w", dir=output_dir, prefix=".relax-summary-", delete=False
    ) as handle:
        temporary = Path(handle.name)
        json.dump(summary, handle, indent=2)
        handle.write("\n")
    os.replace(temporary, summary_path)
    return destination, summary
