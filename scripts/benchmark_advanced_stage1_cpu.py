"""Time representative large-supercell energy/force calls before CPU Stage1."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
from ase.build import make_supercell

from mlff_modepair_workflow.advanced_stage1 import make_advanced_calculator
from mlff_modepair_workflow.core import load_atoms_from_qe
from mlff_modepair_workflow.prophet_backend import process_resource_metrics, sha256_file


def benchmark(model: str, checkpoint: Path, source_root: Path, structure: Path,
              output: Path, threads: list[int], mesh_n: int = 6) -> dict:
    if mesh_n < 1 or not threads or min(threads) < 1:
        raise ValueError("Positive mesh size and thread counts required")
    primitive = load_atoms_from_qe(structure)
    supercell = make_supercell(primitive, np.diag([mesh_n, mesh_n, 1]))
    supercell.set_constraint()
    calculator, model_meta = make_advanced_calculator(model, checkpoint, "cpu", source_root)
    import torch
    rows = []
    for nthreads in threads:
        torch.set_num_threads(nthreads)
        timings = []
        for repeat in range(2):
            atoms = supercell.copy()
            atoms.positions[0, 0] += 0.01 * (repeat + 1)
            atoms.calc = calculator
            started = time.perf_counter()
            energy = float(atoms.get_potential_energy())
            forces = np.asarray(atoms.get_forces(apply_constraint=False), dtype=float)
            elapsed = time.perf_counter() - started
            if forces.shape != (len(atoms), 3) or not np.isfinite(energy) or not np.isfinite(forces).all():
                raise ValueError("Nonfinite or wrong-shaped supercell model output")
            timings.append({"seconds": elapsed, "energy_eV": energy,
                            "max_abs_force_eV_per_A": float(np.max(np.abs(forces)))})
        rows.append({"torch_threads": nthreads, "calls": timings,
                     "resources": process_resource_metrics("cpu")})
    result = {"model": model_meta, "structure": str(Path(structure).resolve()),
              "structure_sha256": sha256_file(structure),
              "mesh_n": mesh_n, "natoms_supercell": len(supercell), "results": rows}
    output = Path(output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mesh-n", type=int, default=6)
    parser.add_argument("--threads", type=int, nargs="+", default=[4, 8, 16])
    args = parser.parse_args()
    result = benchmark(args.model, args.checkpoint, args.source_root, args.structure,
                       args.output, args.threads, args.mesh_n)
    print(json.dumps({"natoms_supercell": result["natoms_supercell"],
                      "results": result["results"]}, indent=2), flush=True)


if __name__ == "__main__":
    main()
