#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server_highthroughput_workflow import run_server_pipeline as pipeline
from server_highthroughput_workflow.real_stage1_phonon import run_real_stage1
from server_highthroughput_workflow.scheduler import resolve_scheduler_mode, resolve_slurm_job_settings, slurm_available
from server_highthroughput_workflow.stage_contracts import (
    STAGE1_KIND,
    STAGE2_KIND,
    STAGE3_KIND,
    create_stage1_manifest,
    create_stage2_manifest,
    create_stage3_manifest,
    dump_json,
    load_json,
    manifest_path,
    resolve_relative_file,
)


DEFAULT_OUTPUT_ROOT = SCRIPT_DIR / "modular_runs"


def parse_args():
    p = argparse.ArgumentParser(description="Modular 3-stage workflow runner with file handoff support.")
    p.add_argument("--stage", choices=["stage1", "stage2", "stage3", "all"], default="all")
    p.add_argument("--run-tag", type=str, default=None)
    p.add_argument("--run-root", type=str, default=None)
    p.add_argument("--output-root", type=str, default=str(DEFAULT_OUTPUT_ROOT))

    p.add_argument("--structure", type=str, default=str(ROOT / "nonlocal phonon" / "scf.inp"))
    p.add_argument("--pseudo-dir", type=str, default=None)

    p.add_argument("--stage1-manifest", type=str, default=None)
    p.add_argument("--stage2-manifest", type=str, default=None)

    p.add_argument("--backend", type=str, default="chgnet")
    p.add_argument("--model", type=str, default="r2scan")
    p.add_argument("--runtime-config", type=str, default=None)
    p.add_argument("--runtime-profile", type=str, default=None, choices=["default", "small", "medium", "large"])

    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--fit-window", type=float, default=1.0)
    p.add_argument("--batch-size", type=int, default=None)
    p.add_argument("--num-workers", type=int, default=None)
    p.add_argument("--torch-threads", type=int, default=None)
    p.add_argument("--interop-threads", type=int, default=None)
    p.add_argument("--chunksize", type=int, default=None)
    p.add_argument("--maxtasksperchild", type=int, default=None)
    p.add_argument("--worker-affinity", type=str, default=None, choices=["off", "auto"])
    p.add_argument("--strategy", type=str, default=None, choices=["full", "coarse_to_fine"])
    p.add_argument("--coarse-grid-size", type=int, default=None)
    p.add_argument("--full-grid-size", type=int, default=None)
    p.add_argument("--refine-top-k", type=int, default=None)

    p.add_argument("--qe-mode", choices=["prepare_only", "submit_collect"], default="submit_collect")
    p.add_argument("--top-n", type=int, default=pipeline.QE_TOP_N)
    p.add_argument("--qe-partition", type=str, default=pipeline.QE_PARTITION)
    p.add_argument("--qe-walltime", type=str, default=pipeline.QE_WALLTIME)
    p.add_argument("--qe-max-running-jobs", type=int, default=pipeline.QE_MAX_RUNNING_JOBS)
    p.add_argument("--qe-poll-seconds", type=int, default=pipeline.QE_POLL_SECONDS)
    p.add_argument("--scheduler", type=str, default="auto", choices=["auto", "slurm", "local"])
    return p.parse_args()


def choose_run_root(args):
    if args.run_root:
        return Path(args.run_root).expanduser().resolve()
    if args.stage2_manifest:
        return Path(args.stage2_manifest).expanduser().resolve().parent
    if args.stage1_manifest:
        return Path(args.stage1_manifest).expanduser().resolve().parent
    run_tag = args.run_tag or "modular_run"
    return Path(args.output_root).expanduser().resolve() / run_tag


def resolve_stage1_manifest(args, run_root: Path):
    return Path(args.stage1_manifest).expanduser().resolve() if args.stage1_manifest else manifest_path(run_root, STAGE1_KIND)


def resolve_stage2_manifest(args, run_root: Path):
    return Path(args.stage2_manifest).expanduser().resolve() if args.stage2_manifest else manifest_path(run_root, STAGE2_KIND)


def run_stage1(args, run_root: Path):
    run_root.mkdir(parents=True, exist_ok=True)
    manifest = run_real_stage1(
        run_root=run_root,
        structure=Path(args.structure),
        pseudo_dir=Path(args.pseudo_dir) if args.pseudo_dir is not None else Path(args.structure).expanduser().resolve().parent,
    )
    print(f"saved: {manifest}")
    return manifest


def run_stage2(args, run_root: Path, stage1_manifest_path: Path):
    stage1 = load_json(stage1_manifest_path)
    mode_pairs_json = resolve_relative_file(run_root, stage1["files"]["mode_pairs_json"])
    structure = resolve_relative_file(run_root, stage1["files"]["structure"])
    stage2_root = run_root / "stage2_outputs"
    cmd = [
        sys.executable,
        str(ROOT / "mlff_modepair_workflow" / "run_pair_screening_optimized.py"),
        "--backend",
        args.backend,
        "--model",
        args.model,
        "--run-tag",
        args.backend,
        "--mode-pairs-json",
        str(mode_pairs_json),
        "--structure",
        str(structure),
        "--output-root",
        str(stage2_root),
    ]
    optional_pairs = [
        ("--runtime-config", args.runtime_config),
        ("--runtime-profile", args.runtime_profile),
        ("--limit", args.limit),
        ("--fit-window", args.fit_window),
        ("--batch-size", args.batch_size),
        ("--num-workers", args.num_workers),
        ("--torch-threads", args.torch_threads),
        ("--interop-threads", args.interop_threads),
        ("--chunksize", args.chunksize),
        ("--maxtasksperchild", args.maxtasksperchild),
        ("--worker-affinity", args.worker_affinity),
        ("--strategy", args.strategy),
        ("--coarse-grid-size", args.coarse_grid_size),
        ("--full-grid-size", args.full_grid_size),
        ("--refine-top-k", args.refine_top_k),
    ]
    for flag, value in optional_pairs:
        if value is not None:
            cmd.extend([flag, str(value)])
    subprocess.run(cmd, cwd=str(ROOT), check=True, text=True)

    screening_dir = stage2_root / args.backend / "screening"
    ranking_csv = screening_dir / "pair_ranking.csv"
    pair_ranking_json = screening_dir / "pair_ranking.json"
    ranking_json = pipeline.normalize_ranking_csv(ranking_csv, args.backend)
    runtime_config_used = screening_dir / "runtime_config_used.json"
    run_meta = screening_dir / "run_meta.json"
    manifest = create_stage2_manifest(
        run_root=run_root,
        stage1_manifest=stage1_manifest_path,
        ranking_csv=ranking_csv,
        ranking_json=ranking_json,
        runtime_config_used=runtime_config_used,
        run_meta=run_meta,
        pair_ranking_json=pair_ranking_json,
    )
    print(f"saved: {manifest}")
    return manifest


def prepare_qe(stage2: dict, run_root: Path, qe_root: Path, backend_tag: str, top_n: int, qe_partition: str, qe_walltime: str):
    ranking_json = resolve_relative_file(run_root, stage2["output_files"]["ranking_json"])
    mode_pairs_json = resolve_relative_file(run_root, stage2["input_files"]["mode_pairs_json"])
    structure = resolve_relative_file(run_root, stage2["input_files"]["structure"])
    pseudo_dir = resolve_relative_file(run_root, stage2["pseudo_dir"])
    slurm_settings = None
    resolved_partition = qe_partition
    resolved_walltime = qe_walltime
    resolved_qos = pipeline.QE_QOS
    if slurm_available():
        slurm_settings = resolve_slurm_job_settings(
            "qe_recheck",
            requested_partition=qe_partition,
            requested_walltime=qe_walltime,
            requested_qos=pipeline.QE_QOS,
        )
        resolved_partition = slurm_settings["partition"]
        resolved_walltime = slurm_settings["walltime"]
        resolved_qos = slurm_settings.get("qos")

    cmd = [
        sys.executable,
        str(pipeline.qe_prepare_script()),
        "--consensus-json",
        str(ranking_json),
        "--mode-pairs-json",
        str(mode_pairs_json),
        "--scf-template",
        str(structure),
        "--pseudo-dir",
        str(pseudo_dir),
        "--output-dir",
        str(qe_root),
        "--top-n",
        str(top_n),
        "--ntasks",
        str(pipeline.QE_NTASKS),
        "--partition",
        resolved_partition,
        "--walltime",
        resolved_walltime,
        "--scf-preset",
        pipeline.QE_SCF_PRESET,
        "--slurm-job-prefix",
        backend_tag,
        "--launcher-command",
        pipeline.QE_LAUNCHER_COMMAND,
    ]
    if resolved_qos:
        cmd.extend(["--qos", resolved_qos])
    for line in pipeline.QE_ENV_INIT_LINES:
        cmd.extend(["--env-init-line", line])
    pipeline.run(cmd, cwd=ROOT)
    if slurm_settings is not None:
        dump_json(qe_root / "resolved_slurm_settings.json", slurm_settings)
    return ranking_json


def run_stage3(args, run_root: Path, stage2_manifest_path: Path):
    stage2 = load_json(stage2_manifest_path)
    qe_root = run_root / "stage3_qe" / args.backend
    ranking_json = prepare_qe(stage2, run_root, qe_root, args.backend, args.top_n, args.qe_partition, args.qe_walltime)
    manifest = create_stage3_manifest(run_root, stage2_manifest_path, qe_root, qe_ranking_json=None)
    dump_json(
        qe_root / "modular_stage3_status.json",
        {
            "final_state": "prepared",
            "qe_ranking_json": None,
            "stage3_manifest": str(manifest),
        },
    )
    if args.qe_mode == "prepare_only":
        print(f"saved: {manifest}")
        return manifest

    if not slurm_available():
        raise RuntimeError("QE submit_collect requires Slurm, but sbatch/squeue are unavailable on this machine.")

    submit_cmd = [
        sys.executable,
        str(pipeline.qe_submit_script()),
        "--run-root",
        str(qe_root),
        "--max-running-jobs",
        str(args.qe_max_running_jobs),
        "--poll-seconds",
        str(args.qe_poll_seconds),
    ]
    pipeline.run(submit_cmd, cwd=ROOT)
    final_state = pipeline.wait_for_qe_completion(qe_root)

    collect_cmd = [
        sys.executable,
        str(pipeline.qe_collect_script()),
        "--run-root",
        str(qe_root),
    ]
    pipeline.run(collect_cmd, cwd=ROOT)
    qe_ranking_json = qe_root / "results" / "qe_ranking.json"
    manifest = create_stage3_manifest(run_root, stage2_manifest_path, qe_root, qe_ranking_json=qe_ranking_json)
    status_path = qe_root / "modular_stage3_status.json"
    dump_json(
        status_path,
        {
            "final_state": final_state,
            "qe_ranking_json": str(qe_ranking_json),
            "stage3_manifest": str(manifest),
        },
    )
    print(f"saved: {manifest}")
    return manifest


def main():
    args = parse_args()
    scheduler_mode = resolve_scheduler_mode(args.scheduler)
    run_root = choose_run_root(args)

    stage1_manifest_path = resolve_stage1_manifest(args, run_root)
    stage2_manifest_path = resolve_stage2_manifest(args, run_root)

    if args.stage in {"stage1", "all"}:
        stage1_manifest_path = run_stage1(args, run_root)
        if args.stage == "stage1":
            return

    if args.stage in {"stage2", "all"}:
        if not stage1_manifest_path.exists():
            raise FileNotFoundError(f"Missing stage1 manifest: {stage1_manifest_path}")
        stage2_manifest_path = run_stage2(args, run_root, stage1_manifest_path)
        if args.stage == "stage2":
            return

    if args.stage in {"stage3", "all"}:
        if not stage2_manifest_path.exists():
            raise FileNotFoundError(f"Missing stage2 manifest: {stage2_manifest_path}")
        if args.qe_mode == "submit_collect" and scheduler_mode == "local":
            raise RuntimeError("Stage3 submit_collect cannot run with --scheduler local. Use --qe-mode prepare_only or a machine with Slurm.")
        run_stage3(args, run_root, stage2_manifest_path)


if __name__ == "__main__":
    main()
