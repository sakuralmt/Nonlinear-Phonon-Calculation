"""Evaluate MLFF forces on the already-selected QE v3 displacement grid."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import numpy as np

from .core import ModePairFrozenPhononBuilder, load_atoms_from_qe
from .prophet_backend import make_prophet_calculator, sha256_file
from .prophet_stage2 import AXES


def _save_array(path: Path, array: np.ndarray) -> None:
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temp.open("wb") as handle:
        np.save(handle, array, allow_pickle=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def evaluate_force_pair(pair: dict, primitive, calc, pair_dir: Path, signature: dict) -> int:
    pair_dir = Path(pair_dir)
    pair_dir.mkdir(parents=True, exist_ok=True)
    builder = ModePairFrozenPhononBuilder(pair, primitive)
    shape = (9, 9, builder.nat_super, 3)
    meta_path = pair_dir / "forces_meta.json"
    meta = {"pair_code": pair["pair_code"], "signature": signature,
            "shape": list(shape), "units": "eV/Angstrom"}
    if meta_path.exists() and json.loads(meta_path.read_text()) != meta:
        raise ValueError(f"Force grid signature changed: {pair_dir}")
    if not meta_path.exists():
        meta_path.write_text(json.dumps(meta, indent=2) + "\n")
    checkpoint_path = pair_dir / "forces_checkpoint.npy"
    grid = np.load(checkpoint_path, allow_pickle=False) if checkpoint_path.exists() else np.full(shape, np.nan)
    if grid.shape != shape:
        raise ValueError(f"Force checkpoint shape changed: {pair_dir}")
    evaluated = 0
    for i, a2 in enumerate(AXES):
        for j, a1 in enumerate(AXES):
            if np.isfinite(grid[i, j]).all():
                continue
            atoms = builder.build_atoms(float(a1), float(a2))
            atoms.calc = calc
            forces = np.asarray(atoms.get_forces(apply_constraint=False), dtype=float)
            if forces.shape != (builder.nat_super, 3) or not np.isfinite(forces).all():
                raise ValueError(f"Nonfinite MLFF force grid at {pair['pair_code']} ({i}, {j})")
            grid[i, j] = forces
            _save_array(checkpoint_path, grid)
            evaluated += 1
    _save_array(pair_dir / "forces_eV_per_A.npy", grid)
    return evaluated


def run(qe_root: Path, stage2_root: Path, backend: str, model: str, device: str,
        shard_index: int = 0, shard_count: int = 1) -> dict:
    qe_root, stage2_root = Path(qe_root).resolve(), Path(stage2_root).resolve()
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("Invalid force-grid shard")
    manifest = json.loads((qe_root / "run_manifest.json").read_text())
    pair_file = Path(manifest["mode_pairs_json"])
    structure = Path(manifest["structure"])
    ranking = json.loads((stage2_root / "pair_ranking.json").read_text())
    signature = ranking["signature"]
    if (signature["mode_pairs_sha256"] != sha256_file(pair_file)
            or signature["structure_sha256"] != sha256_file(structure)
            or ranking["backend"]["backend"] != backend):
        raise ValueError("Force grid Stage2 does not match QE displacement basis")
    primitive = load_atoms_from_qe(structure)
    if backend == "prophet":
        calc, model_meta = make_prophet_calculator(model, device, primitive)
    elif backend == "mattersim":
        from .mattersim_backend import make_mattersim_calculator
        calc, model_meta = make_mattersim_calculator(model, device, primitive)
    else:
        raise ValueError(f"Unsupported v3 force-grid backend: {backend}")
    if model_meta["checkpoint_sha256"] != signature["checkpoint_sha256"]:
        raise ValueError("Force grid model weights differ from Stage2")
    pairs = {pair["pair_code"]: pair for pair in json.loads(pair_file.read_text())["pairs"]}
    selected = manifest["selected_pair_codes"][shard_index::shard_count]
    evaluated = 0
    for code in selected:
        if code not in pairs:
            raise ValueError(f"QE selected mode pair absent from Stage1: {code}")
        root = stage2_root / "pairs" / code
        summary = json.loads((root / "summary.json").read_text())
        if summary["signature"] != signature or summary["completed_points"] != 81:
            raise ValueError(f"Incomplete Stage2 energy grid: {code}")
        evaluated += evaluate_force_pair(pairs[code], primitive, calc, root, signature)
    return {"backend": backend, "selected_pairs": len(selected), "new_force_points": evaluated,
            "device": device, "shard_index": shard_index, "shard_count": shard_count}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qe-root", type=Path, required=True)
    parser.add_argument("--stage2-root", type=Path, required=True)
    parser.add_argument("--backend", choices=["prophet", "mattersim"], required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--device", choices=["cpu", "cuda"], default="cpu")
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    args = parser.parse_args(argv)
    print(json.dumps(run(args.qe_root, args.stage2_root, args.backend, args.model,
                         args.device, args.shard_index, args.shard_count)))


if __name__ == "__main__":
    main()
