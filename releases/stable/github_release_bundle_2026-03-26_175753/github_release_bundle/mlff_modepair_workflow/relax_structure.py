#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from ase.io import write
from ase.optimize import BFGS

from core import dump_json, load_atoms_from_qe, make_calculator


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_STRUCTURE = ROOT / "nonlocal phonon" / "scf.inp"
DEFAULT_OUT_ROOT = SCRIPT_DIR / "runs"


def parse_args():
    p = argparse.ArgumentParser(description="Relax primitive structure with a selected MLFF backend at fixed cell.")
    p.add_argument("--backend", type=str, default="chgnet")
    p.add_argument("--device", type=str, default="auto")
    p.add_argument("--model", type=str, default=None)
    p.add_argument("--run-tag", type=str, default=None, help="Optional output subdirectory tag")
    p.add_argument("--structure", type=str, default=str(DEFAULT_STRUCTURE))
    p.add_argument("--fmax", type=float, default=1.0e-3)
    p.add_argument("--steps", type=int, default=400)
    p.add_argument("--output-root", type=str, default=str(DEFAULT_OUT_ROOT))
    return p.parse_args()


def main():
    args = parse_args()
    structure = Path(args.structure).expanduser().resolve()
    run_tag = args.run_tag or args.backend
    output_dir = Path(args.output_root).expanduser().resolve() / run_tag / "relax"
    output_dir.mkdir(parents=True, exist_ok=True)

    atoms = load_atoms_from_qe(structure)
    calc, backend_meta = make_calculator(backend=args.backend, device=args.device, model=args.model)
    atoms.calc = calc

    initial_energy = float(atoms.get_potential_energy())
    initial_forces = atoms.get_forces()
    initial_max_force = float(np.max(np.linalg.norm(initial_forces, axis=1)))

    opt = BFGS(atoms, logfile=str(output_dir / "opt.log"))
    opt.run(fmax=args.fmax, steps=args.steps)

    final_energy = float(atoms.get_potential_energy())
    final_forces = atoms.get_forces()
    final_max_force = float(np.max(np.linalg.norm(final_forces, axis=1)))

    relaxed_xyz = output_dir / "relaxed_structure.extxyz"
    write(relaxed_xyz, atoms)

    summary = {
        "input_structure": str(structure),
        "backend": backend_meta,
        "run_tag": run_tag,
        "fmax_target_eV_per_A": float(args.fmax),
        "steps_limit": int(args.steps),
        "initial_energy_eV": initial_energy,
        "final_energy_eV": final_energy,
        "initial_max_force_eV_per_A": initial_max_force,
        "final_max_force_eV_per_A": final_max_force,
        "relaxed_structure": str(relaxed_xyz),
    }
    dump_json(output_dir / "relax_summary.json", summary)

    print(f"relaxed structure: {relaxed_xyz}")
    print(f"initial max force: {initial_max_force:.6e} eV/A")
    print(f"final max force: {final_max_force:.6e} eV/A")


if __name__ == "__main__":
    main()
