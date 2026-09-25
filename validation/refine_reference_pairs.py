"""Re-evaluate already matched reference pairs with the stable MLFF adapter.

This diagnostic consumes explicit structures and modes, never generates DFT
inputs, and keeps legacy contracts outside the production Stage2 entry point.
One process loads MatterSim once; independent runs can use disjoint shards.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path

import numpy as np

from mlff_modepair_workflow.core import (
    ModePairFrozenPhononBuilder,
    analyze_pair_grid,
    load_atoms_from_qe,
)
from mlff_modepair_workflow.mattersim_backend import make_mattersim_calculator
from mlff_modepair_workflow.prophet_backend import sha256_file


def save(path, value):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    tmp.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    args = parser.parse_args()
    if not 0 <= args.shard_index < args.shard_count:
        raise ValueError("Invalid shard")
    import torch

    torch.set_num_threads(int(os.environ.get("OMP_NUM_THREADS", "4")))
    torch.set_num_interop_threads(1)
    start = time.perf_counter()
    calc, provenance = make_mattersim_calculator(args.checkpoint, "cpu")
    load_seconds = time.perf_counter() - start
    manifest = json.loads(args.manifest.read_text())
    values = np.linspace(-2, 2, 9)
    for run_index, run in enumerate(manifest["runs"]):
        if run_index % args.shard_count != args.shard_index:
            continue
        structure = Path(run["structure"])
        if sha256_file(structure) != run["structure_sha256"]:
            raise ValueError(f"Structure changed: {structure}")
        atoms = load_atoms_from_qe(structure)
        out = args.output / run["material"] / run["model"]
        out.mkdir(parents=True, exist_ok=True)
        for entry in run["pairs"]:
            pair = entry["pair"]
            identity = {
                "structure_sha256": run["structure_sha256"],
                "pair": pair,
                "calculator": provenance,
                "values": values.tolist(),
            }
            digest = hashlib.sha256(
                json.dumps(identity, sort_keys=True).encode()
            ).hexdigest()
            path = out / (pair["pair_code"] + ".json")
            state = (
                json.loads(path.read_text())
                if path.exists()
                else {
                    "signature": digest,
                    "identity": identity,
                    "points": {},
                    "material": run["material"],
                    "model": run["model"],
                    "qe_pair": entry["qe_pair"],
                    "model_load_seconds": load_seconds,
                    "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
                }
            )
            if state["signature"] != digest:
                raise ValueError(f"Checkpoint identity mismatch: {path}")
            builder = ModePairFrozenPhononBuilder(pair, atoms)
            grid = np.empty((9, 9))
            for j, q2 in enumerate(values):
                for i, q1 in enumerate(values):
                    key = f"{j},{i}"
                    if key not in state["points"]:
                        a = builder.build_atoms(float(q1), float(q2))
                        a.calc = calc
                        t = time.perf_counter()
                        energy = float(a.get_potential_energy())
                        if not np.isfinite(energy):
                            raise ValueError("Non-finite MLFF energy")
                        state["points"][key] = {
                            "energy_ev": energy,
                            "seconds": time.perf_counter() - t,
                        }
                        save(path, state)
                    grid[j, i] = state["points"][key]["energy_ev"]
            state["energy_grid_ev"] = grid.tolist()
            state["n_super"] = builder.n_super
            state["central"] = analyze_pair_grid(
                pair, grid, values, values, fit_window=1.0
            )
            state["wide"] = analyze_pair_grid(
                pair, grid, values, values, fit_window=2.0
            )
            state["mass_norms"] = [
                float(np.sum(atoms_**2 * builder.supercell.get_masses()[:, None]))
                for atoms_ in [
                    builder.displacement_cart(1, 0),
                    builder.displacement_cart(0, 1),
                ]
            ]
            if not np.allclose(state["mass_norms"], 1, atol=1e-10):
                raise ValueError("Non-unit mass norm")
            if state["central"]["fit_design_rank"] != 13:
                raise ValueError("Deficient center fit")
            save(path, state)
            print(run["material"], run["model"], entry["qe_pair"], "81/81", flush=True)


if __name__ == "__main__":
    main()
