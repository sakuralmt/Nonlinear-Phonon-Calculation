"""Same-supercell CPU timing for the supported Stage1 calculators."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
from ase.build import make_supercell

from mlff_modepair_workflow.advanced_stage1 import make_advanced_calculator
from mlff_modepair_workflow.core import load_atoms_from_qe
from mlff_modepair_workflow.prophet_backend import (
    make_prophet_calculator,
    process_resource_metrics,
    sha256_file,
)


def benchmark(
    model: str,
    checkpoint: Path,
    source_root: Path | None,
    structure: Path,
    threads: list[int],
    mesh_n: int = 6,
) -> dict:
    if mesh_n < 1 or not threads or min(threads) < 1:
        raise ValueError("Positive mesh size and thread counts required")
    primitive = load_atoms_from_qe(structure)
    supercell = make_supercell(primitive, np.diag([mesh_n, mesh_n, 1]))
    supercell.set_constraint()
    started = time.perf_counter()
    if model == "prophet":
        calculator, meta = make_prophet_calculator(checkpoint, "cpu", primitive)
    else:
        if source_root is None:
            raise ValueError("Advanced model requires its pinned source root")
        calculator, meta = make_advanced_calculator(
            model, checkpoint, "cpu", source_root
        )
    load_seconds = time.perf_counter() - started
    import torch

    rows = []
    for nthreads in threads:
        torch.set_num_threads(nthreads)
        calls = []
        for step in (0.01, 0.005):
            for repeat in range(3):
                atoms = supercell.copy()
                atoms.positions[0, 0] += step * (repeat + 1)
                atoms.calc = calculator
                begun = time.perf_counter()
                energy = float(atoms.get_potential_energy())
                forces = np.asarray(atoms.get_forces(apply_constraint=False))
                elapsed = time.perf_counter() - begun
                if (
                    not np.isfinite(energy)
                    or forces.shape != (len(atoms), 3)
                    or not np.isfinite(forces).all()
                ):
                    raise ValueError("Invalid model energy or forces")
                calls.append(
                    {
                        "step_angstrom": step,
                        "repeat": repeat,
                        "seconds": elapsed,
                        "energy_ev": energy,
                    }
                )
        rows.append(
            {
                "torch_threads": nthreads,
                "calls": calls,
                "warm_median_seconds": float(
                    np.median([row["seconds"] for row in calls if row["repeat"] > 0])
                ),
                "resources": process_resource_metrics("cpu"),
            }
        )
    return {
        "model": meta,
        "model_loading_seconds": load_seconds,
        "structure_sha256": sha256_file(structure),
        "natoms_supercell": len(supercell),
        "mesh_n": mesh_n,
        "results": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mesh-n", type=int, default=6)
    parser.add_argument("--threads", type=int, nargs="+", default=[4, 8, 16])
    args = parser.parse_args()
    result = benchmark(
        args.model,
        args.checkpoint,
        args.source_root,
        args.structure,
        args.threads,
        args.mesh_n,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(
        json.dumps(
            {
                "model": args.model,
                "loading_seconds": result["model_loading_seconds"],
                "warm_median_seconds": {
                    str(row["torch_threads"]): row["warm_median_seconds"]
                    for row in result["results"]
                },
            },
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
