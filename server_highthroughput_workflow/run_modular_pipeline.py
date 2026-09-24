#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
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
from server_highthroughput_workflow.real_stage1_phonon import run_real_stage1_v3, run_stage1_tuning
from server_highthroughput_workflow.real_stage1_prophet import run_real_prophet_stage1
from server_highthroughput_workflow.scheduler import resolve_scheduler_mode, resolve_slurm_job_settings, slurm_available
from server_highthroughput_workflow.stage_contracts import (
    STAGE1_KIND,
    STAGE2_KIND,
    STAGE3_KIND,
    create_stage2_manifest,
    create_stage1_manifest,
    create_stage3_manifest,
    dump_json,
    load_json,
    manifest_path,
    resolve_relative_file,
)
from server_highthroughput_workflow.system_runtime import prepare_runtime_system

DEFAULT_QE_TOP_N = 30
DEFAULT_QE_PARTITION = "regular"
DEFAULT_QE_WALLTIME = "72:00:00"
DEFAULT_QE_MAX_RUNNING_JOBS = 30
DEFAULT_QE_POLL_SECONDS = 20
DEFAULT_STAGE2_BACKEND = "mattersim"
DEFAULT_STAGE2_MODEL = "auto"
DEFAULT_STAGE2_MODEL_PRESET = "mattersim_v1_5m"
DEFAULT_QE_SCF_PROFILE_LEVEL = "balanced"
DEFAULT_QE_STATIC_PRESET = "static_balanced"
STAGE2_MODEL_PRESETS = {
    "gptff_v1": {"backend": "gptff", "model": "gptff_v1"},
    "gptff_v2": {"backend": "gptff", "model": "gptff_v2"},
    "chgnet": {"backend": "chgnet", "model": "0.3.0"},
    "prophet_oame_mbd": {"backend": "prophet", "model": "prophet_oame_mbd"},
    "mattersim_v1_5m": {"backend": "mattersim", "model": "mattersim_v1_5m"},
}


def resolve_prophet_device(hint: str) -> str:
    if hint != "auto":
        return hint
    try:
        import torch
    except ImportError:
        return "cpu"
    return "cuda" if torch.cuda.is_available() else "cpu"


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
    p.add_argument("--stage1-backend", choices=["qe", "prophet", "tece-oam-rra-1.0",
                                               "equflashv2-45m-oam", "equiformer-v3-oam"], default="prophet")
    p.add_argument("--geometry-source", choices=["shared_dft", "model_relaxed"], default="shared_dft")
    p.add_argument("--stage1-structure", type=str, default=None, help="Explicit shared or initial QE-format structure")
    p.add_argument("--qe-matdyn-input", type=str, default=None, help="Import a complete QE matdyn q-mesh input")
    p.add_argument("--qe-eig", type=str, default=None, help="Import matching QE matdyn eigenvectors")
    p.add_argument("--qe-source-structure", type=str, default=None,
                   help="Actual structure input for an imported QE phonon calculation")
    p.add_argument("--structure-provenance", type=str, default=None)
    p.add_argument("--prophet-checkpoint", type=str, default=None)
    p.add_argument("--stage1-checkpoint", type=str, default=None,
                   help="Pinned checkpoint file for a Matbench Discovery Stage1 model")
    p.add_argument("--stage1-source-root", type=str, default=None,
                   help="Git checkout at the Stage1 model's locked source commit")
    p.add_argument("--stage1-device", choices=["auto", "cpu", "cuda"], default="auto")
    p.add_argument("--stage2-device", choices=["auto", "cpu", "cuda"], default="auto")
    p.add_argument("--q-grid-n", type=int, default=6)
    p.add_argument("--fd-step", type=float, default=0.01)
    p.add_argument("--phonon-engine", choices=["custom", "phonopy"], default="phonopy",
                   help="Phonopy 2.38.0 is the MLFF Stage1 default; custom is historical")
    p.add_argument("--no-phonopy-asr", action="store_false", dest="phonopy_asr",
                   help="Diagnostic only: retain raw Phonopy force constants")

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

    p.add_argument("--qe-mode", choices=["prepare_only", "submit_collect", "collect_only"], default="submit_collect")
    p.add_argument("--top-n", type=int, default=DEFAULT_QE_TOP_N)
    p.add_argument("--dft-joint-phase", type=int, choices=[20, 30],
                   help="Joint Prophet-top20 plus unique MatterSim selection; omit for single Stage2 top-N")
    p.add_argument("--stage3-prophet-ranking", type=str, default=None)
    p.add_argument("--stage3-mattersim-ranking", type=str, default=None)
    p.add_argument("--stage3-state-root", type=str, default=None,
                   help="Shared Slurm Stage3 controller state for all active materials")
    p.add_argument("--qe-submit-batch", type=int, default=5)
    p.add_argument("--qe-pilot-pair-limit", type=int, default=None)
    p.add_argument("--qe-partition", type=str, default=DEFAULT_QE_PARTITION)
    p.add_argument("--qe-walltime", type=str, default=DEFAULT_QE_WALLTIME)
    p.add_argument("--qe-max-running-jobs", type=int, default=DEFAULT_QE_MAX_RUNNING_JOBS)
    p.add_argument("--qe-poll-seconds", type=int, default=DEFAULT_QE_POLL_SECONDS)
    p.add_argument("--qe-scf-profile-level", type=str, default=DEFAULT_QE_SCF_PROFILE_LEVEL, choices=["balanced", "fast"])
    p.add_argument("--qe-static-preset", type=str, default=DEFAULT_QE_STATIC_PRESET)
    p.add_argument("--qe-scf-preset", type=str, default=None)
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


def resolve_stage2_manifest(run_root: Path, backend: str | None = None):
    if backend in {"prophet", "mattersim"}:
        return run_root / "contracts" / f"stage2.{backend}.manifest.json"
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

    structure_for_stage1 = Path(args.stage1_structure).expanduser().resolve() if args.stage1_structure else structure_path
    relax_summary = None
    advanced_stage1 = args.stage1_backend not in {"qe", "prophet"}
    if advanced_stage1 and args.stage1_structure is None and not spec.already_relaxed:
        raise ValueError("Advanced Stage1 needs a proven shared DFT structure or --stage1-structure")
    needs_qe_relax = (
        not advanced_stage1
        and
        args.stage1_structure is None
        and not (args.stage1_backend == "prophet" and args.geometry_source == "model_relaxed")
        and args.qe_relax == "yes" and not spec.already_relaxed
    )
    if needs_qe_relax:
        relax_summary = run_qe_relax(
            run_root=run_root,
            structure_path=structure_path,
            pseudo_dir=pseudo_dir,
            scheduler=args.scheduler,
        )
        structure_for_stage1 = Path(relax_summary["optimized_structure"]).expanduser().resolve()

    if args.stage1_backend == "prophet":
        manifest = run_real_prophet_stage1(
            run_root=run_root, structure=structure_for_stage1, pseudo_dir=pseudo_dir,
            checkpoint=args.prophet_checkpoint or "prophet_oame_mbd",
            device=resolve_prophet_device(args.stage1_device), mesh_n=args.q_grid_n,
            step=args.fd_step, geometry_source=args.geometry_source,
            phonon_engine=getattr(args, "phonon_engine", "phonopy"),
            phonopy_asr=getattr(args, "phonopy_asr", True),
            system_id=spec.system_id, system_dir=spec.system_dir,
            source_cif=spec.structure_cif, system_meta=spec.metadata_path,
            provenance=args.structure_provenance or (
                "qe_relax_this_run" if relax_summary else
                "system_json_already_relaxed" if spec.already_relaxed else "unverified_input"
            ),
        )
    elif advanced_stage1:
        if not args.stage1_checkpoint or not args.stage1_source_root:
            raise ValueError("Advanced Stage1 requires --stage1-checkpoint and --stage1-source-root")
        from mlff_modepair_workflow.advanced_stage1 import run_advanced_stage1
        from mlff_modepair_workflow.prophet_backend import sha256_file
        previous_manifest = manifest_path(run_root, STAGE1_KIND)
        if previous_manifest.exists():
            previous = load_json(previous_manifest)
            if (previous.get("backend") != args.stage1_backend
                    or previous.get("geometry_source") != args.geometry_source
                    or previous.get("model", {}).get("checkpoint_sha256")
                    != sha256_file(Path(args.stage1_checkpoint).expanduser().resolve())):
                raise ValueError("Run root already belongs to another Stage1 model or geometry source")
            previous_phonon = previous.get("files", {}).get("phonon_dataset")
            if previous_phonon:
                engine = load_json(run_root / previous_phonon)["source"].get("phonon_engine", {}).get("name", "custom")
                if engine != getattr(args, "phonon_engine", "phonopy"):
                    raise ValueError("Run root already contains a different phonon engine")
                if engine == "phonopy":
                    previous_asr = load_json(run_root / previous_phonon)["source"]["phonon_engine"].get("acoustic_sum_rule")
                    if previous_asr != getattr(args, "phonopy_asr", True):
                        raise ValueError("Run root already contains a different Phonopy ASR policy")
            if (args.geometry_source == "shared_dft"
                    and previous.get("structure_sha256") != sha256_file(structure_for_stage1)):
                raise ValueError("Run root already contains a different shared-DFT structure")
        advanced_output = run_root / "stage1" / args.stage1_backend / args.geometry_source
        pair_file, phonon_file, force_constants = run_advanced_stage1(
            structure=structure_for_stage1, checkpoint=Path(args.stage1_checkpoint),
            source_root=Path(args.stage1_source_root), model_name=args.stage1_backend,
            output_dir=advanced_output,
            device=resolve_prophet_device(args.stage1_device), mesh_n=args.q_grid_n,
            step=args.fd_step, geometry_source=args.geometry_source,
            phonon_engine=getattr(args, "phonon_engine", "phonopy"),
            phonopy_asr=getattr(args, "phonopy_asr", True),
        )
        advanced_source = load_json(phonon_file)["source"]
        model_meta = advanced_source["model"]
        structure_for_stage1 = Path(advanced_source["structure"])
        if args.geometry_source == "model_relaxed":
            relax_summary = load_json(advanced_output / "relax" / "relax_summary.json")
        manifest = create_stage1_manifest(
            run_root=run_root, mode_pairs_json=pair_file, structure=structure_for_stage1,
            pseudo_dir=pseudo_dir, system_id=spec.system_id, system_dir=spec.system_dir,
            source_cif=spec.structure_cif, system_meta=spec.metadata_path,
            backend=args.stage1_backend, phonon_dataset=phonon_file,
            force_constants=force_constants, geometry_source=args.geometry_source,
            structure_provenance=args.structure_provenance or (
                f"{args.stage1_backend}_relax_from_shared_dft"
                if args.geometry_source == "model_relaxed" else "shared_dft_input"
            ),
            model=model_meta, contract_version=3,
        )
    else:
        manifest = run_real_stage1_v3(
            run_root=run_root, structure=structure_for_stage1, pseudo_dir=pseudo_dir,
            system_id=spec.system_id, system_dir=spec.system_dir,
            source_cif=spec.structure_cif, system_meta=spec.metadata_path,
            mesh_n=args.q_grid_n, matdyn_input=args.qe_matdyn_input, qe_eig=args.qe_eig,
            qe_source_structure=args.qe_source_structure,
            structure_provenance=args.structure_provenance,
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
            "stage1_backend": args.stage1_backend,
            "geometry_source": args.geometry_source,
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
    if args.backend in {"prophet", "mattersim"}:
        if args.limit is not None:
            raise ValueError("v3 stable Stage2 computes every pair; use the direct diagnostic CLI for partial runs")
        model = (args.prophet_checkpoint or ("prophet_oame_mbd" if args.model == "auto" else args.model)) if args.backend == "prophet" else args.model
        if args.backend == "mattersim" and model == "mattersim_v1_5m":
            model = os.environ.get("NPC_MATTERSIM_CHECKPOINT")
            if not model:
                raise ValueError("Set NPC_MATTERSIM_CHECKPOINT or pass --model /path/to/mattersim-v1.0.0-5M.pth")
        cmd = [
            sys.executable, "-m", "mlff_modepair_workflow.prophet_stage2",
            "--backend", args.backend, "--model", model,
            "--device", resolve_prophet_device(args.stage2_device),
            "--run-tag", args.backend, "--mode-pairs-json", str(mode_pairs_json),
            "--structure", str(structure), "--output-root", str(stage2_root),
        ]
        subprocess.run(cmd, cwd=str(ROOT), check=True, text=True)
        screening_dir = stage2_root / args.backend / "screening"
        ranking_csv = screening_dir / "pair_ranking.csv"
        ranking_json = pipeline.normalize_ranking_csv(ranking_csv, args.backend)
        manifest = create_stage2_manifest(
            run_root=run_root, stage1_manifest=stage1_manifest_path,
            ranking_csv=ranking_csv, ranking_json=ranking_json,
            runtime_config_used=screening_dir / "runtime_config_used.json",
            run_meta=screening_dir / "run_meta.json",
            pair_ranking_json=screening_dir / "pair_ranking.json",
            raw_pairs_dir=screening_dir / "pairs",
            backend=args.backend,
        )
        print(f"saved: {manifest}")
        return manifest
    cmd = [
        sys.executable,
        str(ROOT / "mlff_modepair_workflow" / "run_pair_screening_optimized.py"),
        "--backend",
        args.backend,
        "--model",
        args.model,
        "--device",
        args.stage2_device,
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


def prepare_qe(stage2: dict, run_root: Path, qe_root: Path, backend_tag: str, top_n: int, qe_partition: str, qe_walltime: str, args):
    pipeline = _pipeline()
    ranking_json = resolve_relative_file(run_root, stage2["output_files"]["ranking_json"])
    mode_pairs_json = resolve_relative_file(run_root, stage2["input_files"]["mode_pairs_json"])
    structure = resolve_relative_file(run_root, stage2["input_files"]["structure"])
    pseudo_dir = resolve_relative_file(run_root, stage2["pseudo_dir"])
    convergence_summary = run_root / "stage1" / "convergence_summary.json"
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
        "--convergence-summary",
        str(convergence_summary),
        "--top-n",
        str(top_n),
        "--ntasks",
        str(pipeline.QE_NTASKS),
        "--partition",
        resolved_partition,
        "--walltime",
        resolved_walltime,
        "--qe-scf-profile-level",
        args.qe_scf_profile_level,
        "--qe-static-preset",
        args.qe_static_preset,
        "--slurm-job-prefix",
        backend_tag,
        "--launcher-command",
        pipeline.QE_LAUNCHER_COMMAND,
    ]
    if args.qe_scf_preset:
        cmd.extend(["--scf-preset", args.qe_scf_preset])
    if resolved_qos:
        cmd.extend(["--qos", resolved_qos])
    for line in pipeline.QE_ENV_INIT_LINES:
        cmd.extend(["--env-init-line", line])
    subprocess.run(cmd, cwd=str(ROOT), check=True, text=True)
    if slurm_settings is not None:
        dump_json(qe_root / "resolved_slurm_settings.json", slurm_settings)
    return ranking_json


def _stage3_profile_fields(qe_root: Path):
    manifest_path = qe_root / "run_manifest.json"
    if not manifest_path.exists():
        return {}
    payload = load_json(manifest_path)
    keys = (
        "scf_profile_source",
        "scf_profile_branch",
        "scf_profile_level",
        "scf_static_preset",
        "selected_profiles_json",
        "resolved_from_legacy_alias",
        "scf_settings_summary",
        "extra_k_mesh_scale_after_supercell_reduction",
    )
    return {key: payload.get(key) for key in keys if payload.get(key) is not None}


def run_stage3_v3(args, run_root: Path, stage2_manifest_path: Path):
    from mlff_modepair_workflow.stage3_scheduler import launch, register
    from mlff_modepair_workflow.stage3_v3 import collect, prepare, select_pairs, select_single_top_n

    stage2 = load_json(stage2_manifest_path)
    mode_pairs = resolve_relative_file(run_root, stage2["input_files"]["mode_pairs_json"])
    structure = resolve_relative_file(run_root, stage2["input_files"]["structure"])
    pseudo_dir = resolve_relative_file(run_root, stage2["pseudo_dir"])
    tag = "joint" if args.dft_joint_phase else args.backend
    qe_root = run_root / "stage3" / "qe_v3" / tag
    ranking_path = stage2.get("runtime_files", {}).get("pair_ranking_json")
    if args.dft_joint_phase:
        prophet_ranking = Path(args.stage3_prophet_ranking).resolve() if args.stage3_prophet_ranking else run_root / "stage2/outputs/prophet/screening/pair_ranking.json"
        mattersim_ranking = Path(args.stage3_mattersim_ranking).resolve() if args.stage3_mattersim_ranking else run_root / "stage2/outputs/mattersim/screening/pair_ranking.json"
        selection = select_pairs(mode_pairs, structure, prophet_ranking,
                                 mattersim_ranking if args.dft_joint_phase == 30 else None,
                                 args.dft_joint_phase)
    else:
        if ranking_path is None:
            raise ValueError("v3 Stage3 requires a complete Stage2 pair_ranking.json")
        selection = select_single_top_n(mode_pairs, structure,
                                        resolve_relative_file(run_root, ranking_path), args.top_n)
    if args.qe_mode != "collect_only":
        if args.qe_partition != "regular":
            raise ValueError("v3 Stage3 QE jobs must use the regular partition")
        prepare(selection, mode_pairs, structure, pseudo_dir, qe_root,
                ntasks=_pipeline().QE_NTASKS, walltime=args.qe_walltime,
                launcher=_pipeline().QE_LAUNCHER_COMMAND,
                env_init=_pipeline().QE_ENV_INIT_LINES)
    elif not (qe_root / "run_manifest.json").is_file():
        raise FileNotFoundError(f"No prepared v3 Stage3 run: {qe_root}")
    qe_ranking = qe_root / "results" / "qe_v3_ranking.json"
    controller_id = None
    if args.qe_mode == "submit_collect":
        state_root = Path(args.stage3_state_root).expanduser().resolve() if args.stage3_state_root else Path.home() / "qiyan_shared" / "stage3_qe_global"
        register(state_root, qe_root)
        controller_id = launch(state_root, args.qe_max_running_jobs, args.qe_submit_batch,
                               args.qe_poll_seconds, args.qe_pilot_pair_limit)
    if args.qe_mode == "collect_only":
        result = collect(qe_root)
        if result["complete_pairs"] != result["expected_pairs"]:
            raise RuntimeError(f"v3 Stage3 has {result['complete_pairs']}/{result['expected_pairs']} complete pairs")
    manifest = create_stage3_manifest(run_root, stage2_manifest_path, qe_root,
                                      qe_ranking_json=qe_ranking if qe_ranking.is_file() else None,
                                      tag=f"qe_v3_{tag}")
    dump_json(qe_root / "modular_stage3_status.json", {
        "mode": args.qe_mode, "qe_root": str(qe_root), "stage3_manifest": str(manifest),
        "controller_job_id": controller_id, "global_state_root": None if controller_id is None else str(state_root),
        "phase": selection["phase"], "selected_pair_codes": [row["pair_code"] for row in selection["selected_pairs"]],
    })
    print(f"saved: {manifest}")
    return manifest


def run_stage3(args, run_root: Path, stage2_manifest_path: Path):
    pipeline = _pipeline()
    stage2 = load_json(stage2_manifest_path)
    if stage2["version"] >= 3:
        return run_stage3_v3(args, run_root, stage2_manifest_path)
    if args.qe_mode == "collect_only":
        raise ValueError("collect_only is available only for the v3 Stage3 path")
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
                **_stage3_profile_fields(qe_root),
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
                **_stage3_profile_fields(qe_root),
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
            args=args,
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
                **_stage3_profile_fields(qe_root),
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
            **_stage3_profile_fields(qe_root),
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
    stage2_manifest_path = resolve_stage2_manifest(run_root, args.backend)

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
