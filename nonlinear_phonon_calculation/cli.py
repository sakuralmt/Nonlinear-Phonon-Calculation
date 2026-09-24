"""Public two-stage command line interface for hexagonal monolayer screening."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


STAGE1_MODELS = {
    "tece": "tece-oam-rra-1.0",
    "prophet": "prophet",
    "equiformer-v3": "equiformer-v3-oam",
}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="npc", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    stage1 = commands.add_parser(
        "stage1", help="Phonopy modes and complete Gamma-q candidates"
    )
    stage1.add_argument("--model", choices=sorted(STAGE1_MODELS), default="tece")
    stage1.add_argument("--structure", type=Path, required=True)
    stage1.add_argument("--checkpoint", type=Path, required=True)
    stage1.add_argument(
        "--source-root", type=Path, help="Pinned TECE or Equiformer source checkout"
    )
    stage1.add_argument("--output-dir", type=Path, required=True)
    stage1.add_argument("--device", choices=["cpu", "cuda"], default="cpu")
    stage1.add_argument("--mesh-n", type=int, default=6)
    stage1.add_argument("--step", type=float, default=0.01)
    stage1.add_argument("--convergence-step", type=float, default=0.005)
    stage1.add_argument("--gamma-degeneracy-thz", type=float, default=0.01)
    stage2 = commands.add_parser("stage2", help="Checkpointed MatterSim PES screening")
    stage2.add_argument("phase", choices=["screen", "refine", "audit"])
    stage2.add_argument("--mode-pairs-json", type=Path, required=True)
    stage2.add_argument("--structure", type=Path, required=True)
    stage2.add_argument("--checkpoint", type=Path, required=True)
    stage2.add_argument("--output-dir", type=Path, required=True)
    stage2.add_argument("--device", choices=["cpu", "cuda"], default="cpu")
    stage2.add_argument("--top-channels", type=int, default=20)
    stage2.add_argument("--shard-index", type=int, default=0)
    stage2.add_argument("--shard-count", type=int, default=1)
    stage2.add_argument("--finalize-only", action="store_true")
    status = commands.add_parser("status", help="Inspect one Stage2 run")
    status.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "stage1":
        if not 0 < args.gamma_degeneracy_thz < float("inf"):
            parser.error("Gamma degeneracy threshold must be finite and positive")
        if args.model == "prophet":
            from mlff_modepair_workflow.prophet_stage1 import run_prophet_stage1
            from mlff_modepair_workflow.core import load_atoms_from_qe
            from mlff_modepair_workflow.model_relaxation import (
                relax_structure_with_calculator,
            )
            from mlff_modepair_workflow.prophet_backend import make_prophet_calculator

            primitive = load_atoms_from_qe(args.structure)
            calculator, meta = make_prophet_calculator(
                args.checkpoint, args.device, primitive
            )
            structure, _ = relax_structure_with_calculator(
                args.structure, args.output_dir / "relax", calculator, meta, "prophet"
            )
            output = run_prophet_stage1(
                structure,
                args.checkpoint,
                args.output_dir,
                mesh_n=args.mesh_n,
                step=args.step,
                convergence_step=args.convergence_step,
                gamma_degeneracy_thz=args.gamma_degeneracy_thz,
                device=args.device,
            )
        else:
            from mlff_modepair_workflow.advanced_stage1 import run_advanced_stage1

            if args.source_root is None:
                parser.error("TECE and EquiformerV3 need --source-root")
            output = run_advanced_stage1(
                args.structure,
                args.checkpoint,
                args.source_root,
                STAGE1_MODELS[args.model],
                args.output_dir,
                device=args.device,
                mesh_n=args.mesh_n,
                step=args.step,
                convergence_step=args.convergence_step,
                gamma_degeneracy_thz=args.gamma_degeneracy_thz,
            )
        print(json.dumps({"stage1_outputs": [str(path) for path in output]}))
        return 0
    if args.command == "stage2":
        from mlff_modepair_workflow.screening_stage2 import main as stage2_main

        forwarded = [
            args.phase,
            "--mode-pairs-json",
            str(args.mode_pairs_json),
            "--structure",
            str(args.structure),
            "--checkpoint",
            str(args.checkpoint),
            "--output-dir",
            str(args.output_dir),
            "--device",
            args.device,
            "--top-channels",
            str(args.top_channels),
            "--shard-index",
            str(args.shard_index),
            "--shard-count",
            str(args.shard_count),
        ]
        if args.finalize_only:
            forwarded.append("--finalize-only")
        return stage2_main(forwarded)
    root = args.output_dir
    result: dict = {"output_dir": str(root), "rankings": {}}
    for phase in ("screen", "refine", "audit"):
        path = root / f"{phase}_ranking.json"
        result["rankings"][phase] = (
            json.loads(path.read_text())["pair_count"] if path.is_file() else None
        )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
