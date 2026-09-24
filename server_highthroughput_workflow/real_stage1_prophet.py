"""Stage1 Prophet entry point and isolated structure-sensitivity branch."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from mlff_modepair_workflow.core import load_atoms_from_qe
from mlff_modepair_workflow.prophet_backend import make_prophet_calculator, resolve_checkpoint, sha256_file
from mlff_modepair_workflow.prophet_stage1 import run_prophet_stage1
from server_highthroughput_workflow.stage_contracts import create_stage1_manifest, dump_json, load_json, manifest_path, STAGE1_KIND


def write_relaxed_structure_from_qe_template(source: Path, destination: Path, atoms) -> None:
    """Replace geometry in a QE template without requiring unused UPF files."""
    lines = Path(source).read_text().splitlines()
    nat = len(atoms)
    cell_header = next((i for i, line in enumerate(lines) if line.strip().upper().startswith("CELL_PARAMETERS")), None)
    atom_header = next((i for i, line in enumerate(lines) if line.strip().upper().startswith("ATOMIC_POSITIONS")), None)
    if cell_header is None or atom_header is None:
        raise ValueError("QE template requires CELL_PARAMETERS and ATOMIC_POSITIONS")
    for offset, row in enumerate(atoms.cell.array, start=1):
        lines[cell_header + offset] = "  " + "  ".join(f"{value:.15f}" for value in row)
    lines[cell_header] = "CELL_PARAMETERS angstrom"
    lines[atom_header] = "ATOMIC_POSITIONS crystal"
    fixed_flags = []
    for line in lines[atom_header + 1:atom_header + 1 + nat]:
        fields = line.split()
        fixed_flags.append(fields[4:7] if len(fields) >= 7 else [])
    for offset, (symbol, position, flags) in enumerate(zip(atoms.get_chemical_symbols(), atoms.get_scaled_positions(), fixed_flags), start=1):
        lines[atom_header + offset] = f"{symbol}  " + "  ".join(f"{value:.15f}" for value in position) + ("  " + " ".join(flags) if flags else "")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(lines) + "\n")
    recovered = load_atoms_from_qe(destination)
    if not np.allclose(recovered.cell.array, atoms.cell.array, atol=1e-10) or not np.allclose(recovered.positions, atoms.positions, atol=1e-9):
        raise AssertionError("Relaxed QE geometry did not round-trip through the input parser")


def relax_prophet_structure(source: Path, pseudo_dir: Path, output_dir: Path, checkpoint: str | Path, device: str):
    """Relax internal coordinates and one isotropic in-plane scale; keep vacuum fixed."""
    from ase.optimize import BFGS
    from scipy.optimize import minimize_scalar

    source = Path(source).resolve()
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    primitive = load_atoms_from_qe(source)
    calculator, model_meta = make_prophet_calculator(checkpoint, device, primitive)
    original_cell = primitive.cell.array.copy()
    trials = []

    def evaluate_scale(scale: float):
        atoms = primitive.copy()
        cell = original_cell.copy()
        cell[:2, :] *= scale
        atoms.set_cell(cell, scale_atoms=True)
        atoms.calc = calculator
        trial_index = len(trials)
        optimizer = BFGS(atoms, logfile=str(output_dir / f"trial_{trial_index:02d}.log"))
        converged = bool(optimizer.run(fmax=0.02, steps=200))
        energy = float(atoms.get_potential_energy())
        max_force = float(np.max(np.linalg.norm(atoms.get_forces(), axis=1)))
        trials.append({"scale": float(scale), "energy_ev": energy, "max_force_ev_per_A": max_force, "converged": converged})
        if not converged or not np.isfinite(energy):
            return float("inf")
        structures.append(atoms)
        return energy

    structures = []
    search = minimize_scalar(evaluate_scale, bounds=(0.94, 1.06), method="bounded", options={"xatol": 1e-3, "maxiter": 14})
    if not search.success or not structures:
        raise RuntimeError("Prophet structural relaxation failed; inspect trial logs")
    best = min(structures, key=lambda atoms: atoms.get_potential_energy())
    best_scale = min((trial for trial in trials if trial["converged"]), key=lambda row: row["energy_ev"])["scale"]
    if best_scale <= 0.941 or best_scale >= 1.059:
        raise RuntimeError("Prophet in-plane lattice optimum reached the search bound")
    destination = output_dir / "optimized_structure.scf.inp"
    write_relaxed_structure_from_qe_template(source, destination, best)
    summary = {
        "kind": "prophet_model_relaxation", "source_structure": str(source),
        "source_structure_sha256": sha256_file(source), "optimized_structure": str(destination),
        "optimized_structure_sha256": sha256_file(destination), "model": model_meta,
        "best_inplane_scale": best_scale, "best_energy_ev": float(best.get_potential_energy()),
        "vacuum_c_fixed": True, "trials": trials,
    }
    dump_json(output_dir / "relax_summary.json", summary)
    return destination, summary


def run_real_prophet_stage1(
    *, run_root: Path, structure: Path, pseudo_dir: Path, checkpoint: str | Path,
    device: str, mesh_n: int, step: float, geometry_source: str,
    system_id: str | None = None, system_dir: Path | None = None,
    source_cif: Path | None = None, system_meta: Path | None = None,
    provenance: str | None = None,
    phonon_engine: str = "phonopy",
    phonopy_asr: bool = True,
):
    run_root = Path(run_root).resolve()
    structure = Path(structure).resolve()
    previous_manifest = manifest_path(run_root, STAGE1_KIND)
    relax_summary = None
    if previous_manifest.exists():
        previous = load_json(previous_manifest)
        if previous.get("backend") != "prophet" or previous.get("geometry_source") != geometry_source:
            raise ValueError("Run root already belongs to another Stage1 backend or geometry source")
        checkpoint_hash = sha256_file(resolve_checkpoint(checkpoint))
        if previous.get("model", {}).get("checkpoint_sha256") != checkpoint_hash:
            raise ValueError("Run root already contains a different Prophet checkpoint")
        previous_phonon = previous.get("files", {}).get("phonon_dataset")
        if previous_phonon:
            previous_source = load_json(run_root / previous_phonon)["source"]
            if (previous_source.get("q_grid") != [mesh_n, mesh_n, 1]
                    or previous_source.get("finite_difference_step_angstrom") != step
                    or previous_source.get("phonon_engine", {}).get("name", "custom") != phonon_engine
                    or (phonon_engine == "phonopy" and
                        previous_source.get("phonon_engine", {}).get("acoustic_sum_rule") != phonopy_asr)):
                raise ValueError("Run root already contains a different q grid or finite-difference step")
        previous_source = previous.get("structure_sha256")
        if geometry_source == "shared_dft" and previous_source != sha256_file(structure):
            raise ValueError("Run root already contains a different shared-DFT structure")
        if geometry_source == "model_relaxed":
            relax_record = run_root / "stage1" / "prophet_relax" / "relax_summary.json"
            if not relax_record.exists() or load_json(relax_record).get("source_structure_sha256") != sha256_file(structure):
                raise ValueError("Run root already contains a model relaxation from another initial structure")
            relax_summary = load_json(relax_record)
            structure = Path(relax_summary["optimized_structure"]).resolve()
            if not structure.exists() or sha256_file(structure) != relax_summary["optimized_structure_sha256"]:
                raise ValueError("Stored model-relaxed structure is missing or changed")
            if previous_source != sha256_file(structure):
                raise ValueError("Stored model-relaxed structure differs from the Stage1 manifest")
        required = ("mode_pairs_json", "phonon_dataset", "force_constants", "structure")
        if any(name not in previous.get("files", {}) for name in required):
            raise ValueError("Existing Prophet Stage1 manifest is incomplete")
        for name in required:
            if not (run_root / previous["files"][name]).is_file():
                raise ValueError(f"Existing Prophet Stage1 {name} is missing")
        pair_source = load_json(run_root / previous["files"]["mode_pairs_json"])["source"]
        if (pair_source["structure_sha256"] != previous_source
                or sha256_file(run_root / previous["files"]["structure"]) != previous_source):
            raise ValueError("Existing Prophet Stage1 structure or mode pairs changed")
        return previous_manifest
    if geometry_source == "model_relaxed" and relax_summary is None:
        structure, relax_summary = relax_prophet_structure(
            structure, pseudo_dir, run_root / "stage1" / "prophet_relax", checkpoint, device
        )
    elif geometry_source != "shared_dft":
        raise ValueError(f"Unsupported geometry source: {geometry_source}")
    output_dir = run_root / "stage1" / "prophet_outputs" / geometry_source
    pair_file, phonon_file, force_file = run_prophet_stage1(
        structure, checkpoint, output_dir, mesh_n=mesh_n, step=step,
        device=device, geometry_source=geometry_source, phonon_engine=phonon_engine,
        phonopy_asr=phonopy_asr,
    )
    phonon = json.loads(phonon_file.read_text())
    manifest = create_stage1_manifest(
        run_root=run_root, mode_pairs_json=pair_file, structure=structure,
        pseudo_dir=pseudo_dir, system_id=system_id, system_dir=system_dir,
        source_cif=source_cif, system_meta=system_meta,
        backend="prophet", phonon_dataset=phonon_file,
        force_constants=force_file, geometry_source=geometry_source,
        model=phonon["source"]["model"], structure_provenance=provenance,
    )
    dump_json(run_root / "stage1" / "summary.json", {
        "kind": "prophet_stage1_summary", "stage1_manifest": str(manifest),
        "phonon_dataset": str(phonon_file), "mode_pairs_json": str(pair_file),
        "force_constants": str(force_file), "relaxation": relax_summary,
        "geometry_source": geometry_source, "structure_provenance": provenance,
    })
    return manifest
