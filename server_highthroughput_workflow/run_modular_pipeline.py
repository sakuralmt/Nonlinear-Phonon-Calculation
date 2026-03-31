#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from nonlinear_phonon_calculation.system_inputs import (
    DEFAULT_INPUT_ROOT,
    build_run_tag,
    default_runs_root,
    latest_run_root,
    load_system_spec,
    resolve_system_dir,
)
from server_highthroughput_workflow.qe_relax_preflight import run_qe_relax
from server_highthroughput_workflow.real_stage1_phonon import run_real_stage1, run_stage1_tuning
from server_highthroughput_workflow.scheduler import resolve_scheduler_mode, resolve_slurm_job_settings, slurm_available
from server_highthroughput_workflow.stage_contracts import (
    STAGE1_KIND,
    STAGE2_KIND,
    STAGE3_KIND,
    create_stage2_manifest,
    create_stage3_manifest,
    dump_json,
    load_json,
    manifest_path,
    resolve_relative_file,
)
from server_highthroughput_workflow.system_runtime import prepare_runtime_system

DEFAULT_QE_TOP_N = 5
DEFAULT_QE_PARTITION = "debug"
DEFAULT_QE_WALLTIME = "72:00:00"
DEFAULT_QE_MAX_RUNNING_JOBS = 5
DEFAULT_QE_POLL_SECONDS = 20
DEFAULT_STAGE2_BACKEND = "gptff"
DEFAULT_STAGE2_MODEL = "auto"
DEFAULT_STAGE2_MODEL_PRESET = "gptff_v2"
STAGE2_MODEL_PRESETS = {
    "gptff_v1": {"backend": "gptff", "model": "gptff_v1"},
    "gptff_v2": {"backend": "gptff", "model": "gptff_v2"},
    "chgnet": {"backend": "chgnet", "model": "0.3.0"},
}


def _pipeline():
    from server_highthroughput_workflow import stage23_pipeline as pipeline

    return pipeline


def parse_args():
    p = argparse.ArgumentParser(description="Modular 3-stage workflow runner driven by system directories.")
    p.add_argument("--stage", choices=["tune", "stage1", "stage2", "stage3", "all"], default="all")
    p.add_argument("--run-tag", type=str, default=None)
    p.add_argument("--run-root", type=str, default=None)
    p.add_argument("--input-root", type=str, default=str(DEFAULT_INPUT_ROOT))
    p.add_argument("--system", type=str, default=None)
    p.add_argument("--system-dir", type=str, default=None)
    p.add_argument("--qe-relax", choices=["yes", "no"], default="yes")

    p.add_argument(
        "--stage2-model",
        choices=sorted(STAGE2_MODEL_PRESETS),
        default=DEFAULT_STAGE2_MODEL_PRESET,
        help="Stage2 ML model preset.",
    )
    p.add_argument("--backend", type=str, default=DEFAULT_STAGE2_BACKEND)
    p.add_argument("--model", type=str, default=DEFAULT_STAGE2_MODEL)
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
    p.add_argument("--top-n", type=int, default=DEFAULT_QE_TOP_N)
    p.add_argument("--qe-partition", type=str, default=DEFAULT_QE_PARTITION)
    p.add_argument("--qe-walltime", type=str, default=DEFAULT_QE_WALLTIME)
    p.add_argument("--qe-max-running-jobs", type=int, default=DEFAULT_QE_MAX_RUNNING_JOBS)
    p.add_argument("--qe-poll-seconds", type=int, default=DEFAULT_QE_POLL_SECONDS)
    p.add_argument("--scheduler", type=str, default="auto", choices=["auto", "slurm", "local"])
    args = p.parse_args()
    preset = STAGE2_MODEL_PRESETS[args.stage2_model]
    if args.backend == DEFAULT_STAGE2_BACKEND and args.model == DEFAULT_STAGE2_MODEL:
        args.backend = preset["backend"]
        args.model = preset["model"]
    return args


def _resolve_system_spec(args):
    if args.system_dir:
        return load_system_spec(Path(args.system_dir).expanduser().resolve())
    if not args.system:
        raise ValueError("--system is required unless --system-dir is given")
    return load_system_spec(resolve_system_dir(Path(args.input_root), args.system))


def _resolve_system_id(args) -> str:
    if args.system:
        return args.system
    if args.system_dir:
        return Path(args.system_dir).expanduser().resolve().name
    if args.run_root:
        run_root = Path(args.run_root).expanduser().resolve()
        return run_root.parent.name
    raise ValueError("--system is required unless --system-dir or --run-root is given")


def choose_run_root(args, system_id: str):
    if args.run_root:
        return Path(args.run_root).expanduser().resolve()
    runs_root = default_runs_root(Path(args.input_root))
    if args.stage in {"stage2", "stage3"}:
        existing = latest_run_root(runs_root, system_id)
        if existing is not None:
            return existing
    run_tag = args.run_tag or build_run_tag(system_id)
    return runs_root / system_id / run_tag


def resolve_stage1_manifest(run_root: Path):
    return manifest_path(run_root, STAGE1_KIND)


def resolve_stage2_manifest(run_root: Path):
    return manifest_path(run_root, STAGE2_KIND)


def _runtime_root(run_root: Path) -> Path:
    return run_root / "stage1" / "source_system"


def _prepare_system_runtime(spec, run_root: Path):
    runtime_root = _runtime_root(run_root)
    runtime_root.mkdir(parents=True, exist_ok=True)
    return prepare_runtime_system(
        system_dir=spec.system_dir,
        runtime_root=runtime_root,
        preferred_pseudos=spec.preferred_pseudos,
    )


def _stage1_structure_path(run_root: Path) -> Path:
    return _runtime_root(run_root) / "inputs" / "system.scf.inp"


def _stage1_pseudo_dir(run_root: Path) -> Path:
    return _runtime_root(run_root) / "inputs" / "pseudos"


def _write_stage_runtime_summary(run_root: Path, payload: dict):
    dump_json(run_root / "contracts" / "run_context.json", payload)


def run_stage1(args, run_root: Path, spec):
    run_root.mkdir(parents=True, exist_ok=True)
    system_summary = _prepare_system_runtime(spec, run_root)
    structure_path = _stage1_structure_path(run_root)
    pseudo_dir = _stage1_pseudo_dir(run_root)

    structure_for_stage1 = structure_path
    relax_summary = None
    if args.qe_relax == "yes" and not spec.already_relaxed:
        relax_summary = run_qe_relax(
            run_root=run_root,
            structure_path=structure_path,
            pseudo_dir=pseudo_dir,
            scheduler=args.scheduler,
        )
        structure_for_stage1 = Path(relax_summary["optimized_structure"]).expanduser().resolve()

    manifest = run_real_stage1(
        run_root=run_root,
        structure=structure_for_stage1,
        pseudo_dir=pseudo_dir,
        system_id=spec.system_id,
        system_dir=spec.system_dir,
        source_cif=spec.structure_cif,
        system_meta=spec.metadata_path,
        workflow_family=spec.workflow_family,
    )
    _write_stage_runtime_summary(
        run_root,
        {
            "system_id": spec.system_id,
            "system_dir": str(spec.system_dir),
            "workflow_family": spec.workflow_family,
            "input_root": str(Path(args.input_root).expanduser().resolve()),
            "scheduler_mode": resolve_scheduler_mode(args.scheduler),
            "qe_relax": args.qe_relax,
            "prepared_system": system_summary,
            "relax_summary": relax_summary,
        },
    )
    print(f"saved: {manifest}")
    return manifest


def run_tuning(args, run_root: Path, spec):
    run_root.mkdir(parents=True, exist_ok=True)
    _prepare_system_runtime(spec, run_root)
    summary = run_stage1_tuning(
        run_root=run_root,
        structure=_stage1_structure_path(run_root),
        pseudo_dir=_stage1_pseudo_dir(run_root),
        workflow_family=spec.workflow_family,
    )
    print(f"saved: {run_root / 'stage1' / 'convergence_summary.json'}")
    return summary


def run_stage2(args, run_root: Path, stage1_manifest_path: Path):
    pipeline = _pipeline()
    stage1 = load_json(stage1_manifest_path)
    mode_pairs_json = resolve_relative_file(run_root, stage1["files"]["mode_pairs_json"])
    structure = resolve_relative_file(run_root, stage1["files"]["structure"])
    stage2_root = run_root / "stage2" / "outputs"
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
    pipeline = _pipeline()
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
    subprocess.run(cmd, cwd=str(ROOT), check=True, text=True)
    if slurm_settings is not None:
        dump_json(qe_root / "resolved_slurm_settings.json", slurm_settings)
    return ranking_json


def run_stage3(args, run_root: Path, stage2_manifest_path: Path):
    pipeline = _pipeline()
    stage2 = load_json(stage2_manifest_path)
    qe_root = run_root / "stage3" / "qe" / args.backend
    qe_root.mkdir(parents=True, exist_ok=True)
    stage3_status_path = qe_root / "modular_stage3_status.json"
    qe_manifest_path = qe_root / "run_manifest.json"
    qe_ranking_json = qe_root / "results" / "qe_ranking.json"
    submission_log = qe_root / "submission_log.json"
    submission_state = load_json(submission_log) if submission_log.exists() else None
    completed_jobs = None if submission_state is None else submission_state.get("completed_count")
    active_jobs = None if submission_state is None else submission_state.get("active_count")
    total_jobs = None if submission_state is None else submission_state.get("total_jobs")
    stage3_complete = (
        qe_ranking_json.exists()
        and completed_jobs is not None
        and active_jobs is not None
        and total_jobs is not None
        and int(completed_jobs) >= int(total_jobs)
        and int(active_jobs) == 0
    )

    if stage3_complete:
        manifest = create_stage3_manifest(run_root, stage2_manifest_path, qe_root, qe_ranking_json=qe_ranking_json)
        dump_json(
            stage3_status_path,
            {
                "mode": args.qe_mode,
                "final_state": "all_completed",
                "qe_root": str(qe_root),
                "qe_ranking_json": str(qe_ranking_json),
                "stage3_manifest": str(manifest),
                "resume_mode": "reuse_completed",
            },
        )
        print(f"[stage3] reusing completed QE batch: {qe_root}")
        print(f"saved: {manifest}")
        return manifest

    if qe_manifest_path.exists():
        print(f"[stage3] reusing prepared QE batch: {qe_root}")
        manifest = create_stage3_manifest(run_root, stage2_manifest_path, qe_root, qe_ranking_json=None)
        dump_json(
            stage3_status_path,
            {
                "mode": args.qe_mode,
                "final_state": "prepared",
                "qe_root": str(qe_root),
                "qe_ranking_json": None,
                "stage3_manifest": str(manifest),
                "resume_mode": "resume_existing_prepare",
            },
        )
    else:
        prepare_qe(
            stage2=stage2,
            run_root=run_root,
            qe_root=qe_root,
            backend_tag=f"{args.backend}_r03",
            top_n=args.top_n,
            qe_partition=args.qe_partition,
            qe_walltime=args.qe_walltime,
        )
        manifest = create_stage3_manifest(run_root, stage2_manifest_path, qe_root, qe_ranking_json=None)
        dump_json(
            stage3_status_path,
            {
                "mode": args.qe_mode,
                "final_state": "prepared",
                "qe_root": str(qe_root),
                "qe_ranking_json": None,
                "stage3_manifest": str(manifest),
                "resume_mode": "fresh_prepare",
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
    subprocess.run(submit_cmd, cwd=str(ROOT), check=True, text=True)
    final_state = pipeline.wait_for_qe_completion(qe_root)
    collect_cmd = [
        sys.executable,
        str(pipeline.qe_collect_script()),
        "--run-root",
        str(qe_root),
    ]
    subprocess.run(collect_cmd, cwd=str(ROOT), check=True, text=True)
    manifest = create_stage3_manifest(run_root, stage2_manifest_path, qe_root, qe_ranking_json=qe_ranking_json)
    dump_json(
        stage3_status_path,
        {
            "mode": args.qe_mode,
            "final_state": final_state,
            "qe_root": str(qe_root),
            "qe_ranking_json": str(qe_ranking_json) if qe_ranking_json.exists() else None,
            "stage3_manifest": str(manifest),
            "resume_mode": "submit_collect",
        },
    )
    print(f"saved: {manifest}")
    return manifest


def main():
    args = parse_args()
    system_id = _resolve_system_id(args)
    scheduler_mode = resolve_scheduler_mode(args.scheduler)
    spec = _resolve_system_spec(args) if args.stage in {"tune", "stage1", "all"} else None
    run_root = choose_run_root(args, system_id)
    run_root.mkdir(parents=True, exist_ok=True)
    stage1_manifest_path = resolve_stage1_manifest(run_root)
    stage2_manifest_path = resolve_stage2_manifest(run_root)

    if args.stage in {"tune", "stage1", "all"}:
        assert spec is not None
        if args.stage == "tune":
            run_tuning(args, run_root, spec)
            return
        stage1_manifest_path = run_stage1(args, run_root, spec)

    if args.stage in {"stage2", "all"}:
        if not stage1_manifest_path.exists():
            raise FileNotFoundError(f"Missing stage1 manifest: {stage1_manifest_path}")
        stage2_manifest_path = run_stage2(args, run_root, stage1_manifest_path)

    if args.stage in {"stage3", "all"}:
        if not stage2_manifest_path.exists():
            raise FileNotFoundError(f"Missing stage2 manifest: {stage2_manifest_path}")
        if args.qe_mode == "submit_collect" and scheduler_mode == "local":
            raise RuntimeError("Stage3 submit_collect cannot run with --scheduler local. Use --qe-mode prepare_only or a machine with Slurm.")
        run_stage3(args, run_root, stage2_manifest_path)


if __name__ == "__main__":
    raise SystemExit(main())
