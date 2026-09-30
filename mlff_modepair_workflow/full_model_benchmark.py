"""Checkpointed matched-channel PES benchmark with pinned Stage2 models.

The manifest supplies already matched modes and relaxed geometries. No structure
relaxation or phonon calculation is performed here. Energy/force comparisons to
QE are valid only for the explicit fixed-DFT tasks, never across geometries.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import time
from pathlib import Path

import numpy as np

from .core import ModePairFrozenPhononBuilder, analyze_pair_grid
from .dft_reference import read_dft_structure
from .prophet_backend import process_resource_metrics, sha256_file
from .screening_stage2 import _locked_identity, _write
from .units import NORMALIZATION_VERSION, UNITS


def make_calculator(model, checkpoint, device, source_root):
    if model == "mattersim":
        from .mattersim_backend import make_mattersim_calculator

        return make_mattersim_calculator(checkpoint, device)
    if model == "prophet":
        from .prophet_backend import make_prophet_calculator

        return make_prophet_calculator(checkpoint, device)
    from .advanced_stage1 import make_advanced_calculator

    names = {"tece": "tece-oam-rra-1.0", "equiformer-v3": "equiformer-v3-oam"}
    if source_root is None:
        raise ValueError("Pinned source root is required")
    return make_advanced_calculator(names[model], checkpoint, device, source_root)


def run(
    manifest_path: Path,
    checkpoint: Path,
    output: Path,
    model: str,
    source_root: Path | None = None,
    device: str = "cpu",
    calculator=None,
):
    started = time.perf_counter()
    manifest = json.loads(manifest_path.read_text())
    if (
        manifest["units"] != UNITS
        or manifest["normalization_version"] != NORMALIZATION_VERSION
    ):
        raise ValueError("Benchmark units or normalization mismatch")
    if manifest["model"] != model or len(manifest["tasks"]) != len(
        {t["task_id"] for t in manifest["tasks"]}
    ):
        raise ValueError("Benchmark model or duplicate tasks mismatch")
    identity = {
        "manifest_sha256": sha256_file(manifest_path),
        "checkpoint_sha256": sha256_file(checkpoint),
        "model": model,
        "device": device,
        "normalization_version": NORMALIZATION_VERSION,
        "runner_sha256": sha256_file(Path(__file__)),
    }
    _locked_identity(output, identity)
    with (output / ".benchmark.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        load_start = time.perf_counter()
        calc, provenance = (
            make_calculator(model, checkpoint, device, source_root)
            if calculator is None
            else (calculator, {"backend": "test"})
        )
        loading = time.perf_counter() - load_start
        if calculator is None:
            from .advanced_stage1 import preflight_calculator

            probe = preflight_calculator(
                read_dft_structure(Path(manifest["tasks"][0]["structure"])),
                calc,
                device,
            )
            _write(output / "preflight.json", probe)
            if not probe["passed"]:
                raise ValueError(
                    "Benchmark force/energy or repeat-inference preflight failed"
                )
        rows = []
        for task in manifest["tasks"]:
            structure = Path(task["structure"])
            if sha256_file(structure) != task["structure_sha256"]:
                raise ValueError("Benchmark structure changed")
            atoms = read_dft_structure(structure)
            builder = ModePairFrozenPhononBuilder(task["pair"], atoms)
            norms = [
                float(
                    np.sum(
                        builder.displacement_cart(*q) ** 2
                        * builder.supercell.get_masses()[:, None]
                    )
                )
                for q in [(1, 0), (0, 1)]
            ]
            if not np.allclose(norms, 1, atol=1e-10):
                raise ValueError("Non-unit real modes")
            axis = np.asarray(task["axis"], dtype=float)
            if axis.tolist() not in [
                np.linspace(-1, 1, 5).tolist(),
                np.linspace(-2, 2, 9).tolist(),
            ]:
                raise ValueError("Unsupported benchmark grid")
            path = output / "points" / (task["task_id"] + ".json")
            state = (
                json.loads(path.read_text())
                if path.exists()
                else {"identity": identity, "task_id": task["task_id"], "points": {}}
            )
            if state["identity"] != identity or state["task_id"] != task["task_id"]:
                raise ValueError("Benchmark checkpoint mismatch")
            for j, y in enumerate(axis):
                for i, x in enumerate(axis):
                    key = f"{j},{i}"
                    if key not in state["points"]:
                        displaced = builder.build_atoms(float(x), float(y))
                        displaced.calc = calc
                        t = time.perf_counter()
                        e = float(displaced.get_potential_energy())
                        f = np.asarray(
                            displaced.get_forces(apply_constraint=False), dtype=float
                        )
                        if (
                            not np.isfinite(e)
                            or f.shape != (builder.nat_super, 3)
                            or not np.isfinite(f).all()
                        ):
                            raise ValueError("Nonfinite benchmark energy/forces")
                        state["points"][key] = {
                            "energy_ev": e,
                            "forces_ev_per_A": f.tolist(),
                            "seconds": time.perf_counter() - t,
                        }
                        _write(path, state)
                    point = state["points"][key]
                    if (
                        not np.isfinite(point["energy_ev"])
                        or np.asarray(point["forces_ev_per_A"]).shape
                        != (builder.nat_super, 3)
                        or not np.isfinite(point["forces_ev_per_A"]).all()
                    ):
                        raise ValueError("Corrupt benchmark checkpoint")
            grid = np.asarray(
                [
                    [state["points"][f"{j},{i}"]["energy_ev"] for i in range(len(axis))]
                    for j in range(len(axis))
                ]
            )
            forces = np.asarray(
                [
                    [
                        state["points"][f"{j},{i}"]["forces_ev_per_A"]
                        for i in range(len(axis))
                    ]
                    for j in range(len(axis))
                ]
            )
            central = analyze_pair_grid(task["pair"], grid, axis, axis, fit_window=1)
            if central["fit_design_rank"] != 13:
                raise ValueError("Rank-deficient central fit")
            row = {
                **task,
                "energy_grid_ev": grid.tolist(),
                "force_grid_ev_per_A": forces.tolist(),
                "central": central,
                "mass_norms": norms,
                "complete_points": grid.size,
                "inference_seconds": sum(
                    p["seconds"] for p in state["points"].values()
                ),
                "point_checkpoint_sha256": sha256_file(path),
            }
            if len(axis) == 9:
                row["wide"] = analyze_pair_grid(
                    task["pair"], grid, axis, axis, fit_window=2
                )
            rows.append(row)
            _write(
                output / "progress.json",
                {
                    "complete_tasks": len(rows),
                    "total_tasks": len(manifest["tasks"]),
                    "complete_points": sum(r["complete_points"] for r in rows),
                },
            )
            print(model, manifest["material"], task["task_id"], grid.size, flush=True)
        result = {
            "identity": identity,
            "units": UNITS,
            "material": manifest["material"],
            "model": model,
            "provenance": provenance,
            "rows": rows,
            "complete_tasks": len(rows),
            "complete_points": sum(r["complete_points"] for r in rows),
            "loading_seconds": loading,
            "wall_seconds": time.perf_counter() - started,
            "resources": process_resource_metrics(device),
            "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        }
        _write(output / "result.json", result)
        return output / "result.json"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--model",
        choices=["tece", "prophet", "equiformer-v3", "mattersim"],
        required=True,
    )
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--device", choices=["cpu", "cuda"], default="cpu")
    args = parser.parse_args()
    import torch

    torch.set_num_threads(int(os.environ.get("OMP_NUM_THREADS", "8")))
    torch.set_num_interop_threads(1)
    run(
        args.manifest,
        args.checkpoint,
        args.output,
        args.model,
        args.source_root,
        args.device,
    )


if __name__ == "__main__":
    main()
