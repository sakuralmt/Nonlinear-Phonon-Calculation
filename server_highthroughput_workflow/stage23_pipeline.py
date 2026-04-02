#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mlff_modepair_workflow.core import resolve_chgnet_runtime_config, select_runtime_config_path
from server_highthroughput_workflow.scheduler import (
    resolve_scheduler_mode,
    resolve_slurm_job_settings,
    scheduler_capabilities,
    slurm_available,
)


# ============================
# User configuration
# ============================
WORK_DIR = Path.cwd()
MODE_PAIRS_JSON = WORK_DIR / "stage1" / "outputs" / "mode_pairs.selected.json"
STRUCTURE = WORK_DIR / "stage1" / "inputs" / "system.scf.inp"
PSEUDO_DIR = WORK_DIR / "stage1" / "inputs" / "pseudos"
OUTPUT_ROOT = SCRIPT_DIR / "runs"

GOLDEN_GATE = {
    "gamma_abs_error_thz_max": 1.0,
    "target_abs_error_thz_max": 1.5,
    "phi122_abs_error_mev_max": 2.5,
}

BACKEND_SPECS = [
    {
        "tag": "chgnet_r2scan",
        "backend": "chgnet",
        "model": "r2scan",
        "probe": "from chgnet.model import CHGNet",
    },
]

MLFF_RUN_BENCHMARK = False
MLFF_MP_START = "spawn"
MLFF_DEFAULT_SCHEDULER = "auto"
HT_PYTHON_BIN = "/home/server/miniconda3/envs/qiyan-ht/bin/python"
MLFF_PARTITION = "debug"
MLFF_WALLTIME = "24:00:00"
MLFF_POLL_SECONDS = 30

QE_TOP_N = 5
QE_SCF_PROFILE_LEVEL = "balanced"
QE_STATIC_PRESET = "static_balanced"
QE_NTASKS = 24
QE_PARTITION = "debug"
QE_QOS = None
QE_WALLTIME = "72:00:00"
QE_MAX_RUNNING_JOBS = 20
QE_POLL_SECONDS = 20
QE_ENV_INIT_LINES = [
    "set +u",
    "source /opt/intel/oneapi/setvars.sh >/dev/null 2>&1 || true",
    "set -u",
]
QE_LAUNCHER_COMMAND = "mpirun -np {ntasks} pw.x < scf.inp > scf.out"
_MLFF_RUNTIME_CACHE = None


def resolve_mlff_slurm_settings():
    return resolve_slurm_job_settings(
        "mlff_screening",
        requested_partition=MLFF_PARTITION,
        requested_walltime=MLFF_WALLTIME,
        requested_qos=None,
    )


def resolve_stage3_continuation_settings():
    return resolve_slurm_job_settings(
        "stage3_continuation",
        requested_partition=MLFF_PARTITION,
        requested_walltime=QE_WALLTIME,
        requested_qos=None,
    )


def resolve_qe_slurm_settings():
    return resolve_slurm_job_settings(
        "qe_recheck",
        requested_partition=QE_PARTITION,
        requested_walltime=QE_WALLTIME,
        requested_qos=QE_QOS,
    )


def parse_args():
    p = argparse.ArgumentParser(description="Server-side MLFF -> QE recheck controller.")
    p.add_argument("--run-tag", type=str, default=None)
    p.add_argument("--output-root", type=str, default=str(OUTPUT_ROOT))
    p.add_argument("--runtime-config", type=str, default=None, help="Optional runtime config JSON for the CHGNet screening stage.")
    p.add_argument("--runtime-profile", type=str, default=None, choices=["default", "small", "medium", "large"], help="Optional portable CPU profile name.")
    p.add_argument("--scheduler", type=str, default=MLFF_DEFAULT_SCHEDULER, choices=["auto", "slurm", "local"], help="Scheduler mode for the MLFF screening stage.")
    p.add_argument("--force", action="store_true")
    return p.parse_args()


def dump_json(path: Path, payload: dict):
    path.write_text(json.dumps(payload, indent=2))


def run(cmd: list[str], cwd: Path | None = None):
    print("+", " ".join(str(x) for x in cmd), flush=True)
    subprocess.run(cmd, cwd=None if cwd is None else str(cwd), check=True, text=True)


def probe_backend(spec: dict):
    model = spec.get("model")
    if isinstance(model, Path) and not model.exists():
        raise FileNotFoundError(f"Missing model file: {model}")
    subprocess.run([sys.executable, "-c", spec["probe"]], check=True, text=True)


def benchmark_script():
    return ROOT / "mlff_modepair_workflow" / "ops" / "benchmark_golden_pair.py"


def screening_script():
    return ROOT / "mlff_modepair_workflow" / "run_pair_screening_optimized.py"


def qe_prepare_script():
    return ROOT / "qe_modepair_handoff_workflow" / "prepare_top_pairs.py"


def qe_submit_script():
    return ROOT / "qe_modepair_handoff_workflow" / "submit_top_pairs.py"


def qe_collect_script():
    return ROOT / "qe_modepair_handoff_workflow" / "collect_top_pairs.py"


def _model_arg(spec: dict):
    model = spec.get("model")
    if model is None:
        return []
    if isinstance(model, Path):
        return ["--model", str(model.resolve())]
    return ["--model", str(model)]


def resolve_mlff_runtime(args):
    global _MLFF_RUNTIME_CACHE
    if _MLFF_RUNTIME_CACHE is not None:
        return _MLFF_RUNTIME_CACHE
    if args.runtime_config:
        config_path = Path(args.runtime_config).expanduser().resolve()
    else:
        config_path = select_runtime_config_path(WORK_DIR, profile_name=args.runtime_profile)
    runtime, meta = resolve_chgnet_runtime_config(config_path=config_path)
    _MLFF_RUNTIME_CACHE = (runtime, meta, config_path)
    return _MLFF_RUNTIME_CACHE


def extract_energy_ry(scf_out: Path):
    if not scf_out.exists():
        return None
    lines = scf_out.read_text(errors="ignore").splitlines()
    for line in reversed(lines):
        if "total energy" in line and line.lstrip().startswith("!"):
            m = re.search(r"=\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)", line)
            if m:
                return float(m.group(1))
    return None


def squeue_existing_job_ids(job_ids: list[str]):
    if not job_ids:
        return set()
    arg = ",".join(str(job_id) for job_id in job_ids)
    try:
        result = subprocess.run(["squeue", "-h", "-j", arg, "-o", "%i"], capture_output=True, text=True, check=True)
        txt = result.stdout.strip()
        return set() if not txt else {line.strip() for line in txt.splitlines() if line.strip()}
    except Exception:
        return set()


def parse_sbatch_job_id(stdout: str):
    m = re.search(r"Submitted batch job\s+(\d+)", stdout)
    return None if m is None else m.group(1)


def squeue_job_state(job_id: str):
    try:
        result = subprocess.run(["squeue", "-h", "-j", str(job_id), "-o", "%T"], capture_output=True, text=True, check=True)
    except Exception:
        return None
    txt = result.stdout.strip()
    return None if not txt else txt.splitlines()[0].strip()


def passes_golden_gate(compare: dict):
    reasons = []
    gamma_err = compare.get("gamma_freq_abs_error_thz")
    target_err = compare.get("target_freq_abs_error_thz")
    phi_err = compare.get("phi122_abs_error_mev_per_A3amu32")

    if gamma_err is None or gamma_err > GOLDEN_GATE["gamma_abs_error_thz_max"]:
        reasons.append(f"gamma_abs_error={gamma_err}")
    if target_err is None or target_err > GOLDEN_GATE["target_abs_error_thz_max"]:
        reasons.append(f"target_abs_error={target_err}")
    if phi_err is None or phi_err > GOLDEN_GATE["phi122_abs_error_mev_max"]:
        reasons.append(f"phi122_abs_error={phi_err}")
    return len(reasons) == 0, reasons


def run_benchmark(spec: dict, mlff_root: Path):
    cmd = [
        sys.executable,
        str(benchmark_script()),
        "--backend",
        spec["backend"],
        "--run-tag",
        spec["tag"],
        "--mode-pairs-json",
        str(MODE_PAIRS_JSON),
        "--structure",
        str(STRUCTURE),
        "--output-root",
        str(mlff_root),
    ]
    cmd.extend(_model_arg(spec))
    run(cmd, cwd=ROOT)
    return mlff_root / spec["tag"] / "benchmark" / "summary.json"


def run_screening(spec: dict, mlff_root: Path, args, scheduler_mode: str):
    if scheduler_mode == "slurm":
        return run_screening_via_slurm(spec, mlff_root, args)

    runtime, _meta, config_path = resolve_mlff_runtime(args)
    cmd = [
        sys.executable,
        str(screening_script()),
        "--backend",
        spec["backend"],
        "--run-tag",
        spec["tag"],
        "--mode-pairs-json",
        str(MODE_PAIRS_JSON),
        "--structure",
        str(STRUCTURE),
        "--output-root",
        str(mlff_root),
    ]
    if config_path is not None:
        cmd.extend(["--runtime-config", str(config_path)])
    cmd.extend(
        [
        "--strategy",
        runtime["strategy"],
        "--coarse-grid-size",
        str(runtime["coarse_grid_size"]),
        "--full-grid-size",
        str(runtime["full_grid_size"]),
        "--refine-top-k",
        str(runtime["refine_top_k"]),
        "--batch-size",
        str(runtime["batch_size"]),
        "--num-workers",
        str(runtime["num_workers"]),
        "--torch-threads",
        str(runtime["torch_threads"]),
        "--interop-threads",
        str(runtime["interop_threads"]),
        "--worker-affinity",
        runtime["worker_affinity"],
        "--mp-start",
        MLFF_MP_START,
        "--chunksize",
        str(runtime["chunksize"]),
        "--maxtasksperchild",
        str(runtime["maxtasksperchild"]),
    ]
    )
    cmd.extend(_model_arg(spec))
    run(cmd, cwd=ROOT)
    return mlff_root / spec["tag"] / "screening" / "pair_ranking.csv"


def _screening_cmd(spec: dict, mlff_root: Path, args):
    runtime, _meta, config_path = resolve_mlff_runtime(args)
    cmd = [
        HT_PYTHON_BIN,
        "-u",
        str(screening_script()),
        "--backend",
        spec["backend"],
        "--run-tag",
        spec["tag"],
        "--mode-pairs-json",
        str(MODE_PAIRS_JSON),
        "--structure",
        str(STRUCTURE),
        "--output-root",
        str(mlff_root),
    ]
    if config_path is not None:
        cmd.extend(["--runtime-config", str(config_path)])
    cmd.extend(
        [
        "--strategy",
        runtime["strategy"],
        "--coarse-grid-size",
        str(runtime["coarse_grid_size"]),
        "--full-grid-size",
        str(runtime["full_grid_size"]),
        "--refine-top-k",
        str(runtime["refine_top_k"]),
        "--batch-size",
        str(runtime["batch_size"]),
        "--num-workers",
        str(runtime["num_workers"]),
        "--torch-threads",
        str(runtime["torch_threads"]),
        "--interop-threads",
        str(runtime["interop_threads"]),
        "--worker-affinity",
        runtime["worker_affinity"],
        "--mp-start",
        MLFF_MP_START,
        "--chunksize",
        str(runtime["chunksize"]),
        "--maxtasksperchild",
        str(runtime["maxtasksperchild"]),
    ]
    )
    cmd.extend(_model_arg(spec))
    return cmd


def _write_mlff_submit_script(spec: dict, mlff_root: Path, args):
    runtime, meta, _config_path = resolve_mlff_runtime(args)
    cpus_per_task = max(1, int(runtime["num_workers"]) * int(runtime["torch_threads"]))
    slurm_settings = resolve_mlff_slurm_settings()
    job_root = mlff_root / spec["tag"] / "screening_job"
    job_root.mkdir(parents=True, exist_ok=True)
    submit_path = job_root / "submit.sh"
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={spec['tag'][:12]}_mlff",
        f"#SBATCH --partition={slurm_settings['partition']}",
        "#SBATCH --nodes=1",
        "#SBATCH --ntasks=1",
        f"#SBATCH --cpus-per-task={cpus_per_task}",
        f"#SBATCH --time={slurm_settings['walltime']}",
        f"#SBATCH --output={job_root / 'slurm-%j.out'}",
        f"#SBATCH --error={job_root / 'slurm-%j.err'}",
        "set -euo pipefail",
        f"cd {ROOT}",
        f"export OMP_NUM_THREADS={runtime['torch_threads']}",
        f"export MKL_NUM_THREADS={runtime['torch_threads']}",
        f"export OPENBLAS_NUM_THREADS={runtime['torch_threads']}",
        f"export NUMEXPR_NUM_THREADS={runtime['torch_threads']}",
        "export LD_LIBRARY_PATH=$HOME/miniconda3/envs/qiyan-ht/lib:${LD_LIBRARY_PATH:-}",
        " ".join(shlex.quote(str(x)) for x in _screening_cmd(spec, mlff_root, args)),
    ]
    if slurm_settings.get("qos"):
        lines.insert(7, f"#SBATCH --qos={slurm_settings['qos']}")
    submit_path.write_text("\n".join(lines) + "\n")
    submit_path.chmod(0o755)
    dump_json(
        job_root / "runtime_config_used.json",
        {
            "runtime": runtime,
            "meta": meta,
            "cpus_per_task": cpus_per_task,
            "slurm_settings": slurm_settings,
        },
    )
    return job_root, submit_path


def run_screening_via_slurm(spec: dict, mlff_root: Path, args):
    ranking_csv = mlff_root / spec["tag"] / "screening" / "pair_ranking.csv"
    job_root, submit_path = _write_mlff_submit_script(spec, mlff_root, args)
    result = subprocess.run(["sbatch", str(submit_path)], capture_output=True, text=True)
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if result.returncode != 0:
        raise RuntimeError(f"MLFF sbatch failed:\n{stdout}\n{stderr}")
    job_id = parse_sbatch_job_id(stdout)
    if not job_id:
        raise RuntimeError(f"Could not parse MLFF sbatch job id: {stdout}")
    dump_json(
        job_root / "job_status.json",
        {
            "state": "submitted",
            "job_id": job_id,
            "sbatch_stdout": stdout,
            "sbatch_stderr": stderr,
            "submit_time_epoch": time.time(),
            "ranking_csv": str(ranking_csv),
        },
    )
    print(f"[controller] submitted MLFF screening job {job_id}", flush=True)
    return ranking_csv, job_root, job_id


def submit_continuation_job(run_root: Path, spec: dict, ranking_csv: Path, mlff_job_root: Path, mlff_job_id: str):
    slurm_settings = resolve_stage3_continuation_settings()
    submit_path = mlff_job_root / "continue_after_screening.sh"
    lines = [
        "#!/usr/bin/env bash",
        f"#SBATCH --job-name={spec['tag'][:12]}_cont",
        f"#SBATCH --partition={slurm_settings['partition']}",
        "#SBATCH --nodes=1",
        "#SBATCH --ntasks=1",
        "#SBATCH --cpus-per-task=1",
        f"#SBATCH --time={slurm_settings['walltime']}",
        f"#SBATCH --output={mlff_job_root / 'continue-%j.out'}",
        f"#SBATCH --error={mlff_job_root / 'continue-%j.err'}",
        "set -euo pipefail",
        f"cd {ROOT}",
        "export LD_LIBRARY_PATH=$HOME/miniconda3/envs/qiyan-ht/lib:${LD_LIBRARY_PATH:-}",
        " ".join(
            [
                HT_PYTHON_BIN,
                shlex.quote("-u"),
                shlex.quote(str(SCRIPT_DIR / "ops" / "continue_after_screening.py")),
                shlex.quote("--run-root"),
                shlex.quote(str(run_root)),
                shlex.quote("--backend-tag"),
                shlex.quote(spec["tag"]),
                shlex.quote("--ranking-csv"),
                shlex.quote(str(ranking_csv)),
            ]
        ),
    ]
    if slurm_settings.get("qos"):
        lines.insert(7, f"#SBATCH --qos={slurm_settings['qos']}")
    submit_path.write_text("\n".join(lines) + "\n")
    submit_path.chmod(0o755)
    result = subprocess.run(
        ["sbatch", f"--dependency=afterok:{mlff_job_id}", str(submit_path)],
        capture_output=True,
        text=True,
    )
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if result.returncode != 0:
        raise RuntimeError(f"Continuation sbatch failed:\n{stdout}\n{stderr}")
    cont_job_id = parse_sbatch_job_id(stdout)
    if not cont_job_id:
        raise RuntimeError(f"Could not parse continuation sbatch job id: {stdout}")
    dump_json(
        mlff_job_root / "continuation_job.json",
        {
            "state": "submitted",
            "job_id": cont_job_id,
            "dependency": f"afterok:{mlff_job_id}",
            "ranking_csv": str(ranking_csv),
            "sbatch_stdout": stdout,
            "sbatch_stderr": stderr,
            "submit_time_epoch": time.time(),
            "slurm_settings": slurm_settings,
        },
    )
    print(f"[controller] submitted continuation job {cont_job_id} after MLFF job {mlff_job_id}", flush=True)
    return cont_job_id


def wait_for_mlff_job(run_root: Path, job_root: Path, job_id: str, ranking_csv: Path):
    status_path = run_root / "mlff_gate_status.json"
    while True:
        state = squeue_job_state(job_id)
        payload = {
            "timestamp_epoch": time.time(),
            "job_id": str(job_id),
            "queue_state": state,
            "ranking_csv": str(ranking_csv),
        }
        if state is None:
            payload["status"] = "finished"
            dump_json(status_path, payload)
            if ranking_csv.exists():
                dump_json(job_root / "job_status.json", {"state": "completed", "job_id": str(job_id), "ranking_csv": str(ranking_csv)})
                return
            slurm_out = job_root / f"slurm-{job_id}.out"
            slurm_err = job_root / f"slurm-{job_id}.err"
            raise RuntimeError(
                "MLFF screening job finished without ranking output. "
                f"Check {slurm_out} and {slurm_err}"
            )
        payload["status"] = "queued_or_running"
        dump_json(status_path, payload)
        print(f"[controller] MLFF job {job_id} state: {state}", flush=True)
        time.sleep(MLFF_POLL_SECONDS)


def normalize_ranking_csv(ranking_csv: Path, backend_tag: str):
    raw_rows = []
    with ranking_csv.open() as f:
        reader = csv.DictReader(f)
        raw_rows.extend(reader)

    total = max(1, len(raw_rows))
    rows = []
    for idx, row in enumerate(raw_rows, start=1):
        phi122 = float(row["phi122_mev"])
        gamma_fit = float(row["gamma_freq_fit_thz"]) if row.get("gamma_freq_fit_thz") else None
        target_fit = float(row["target_freq_fit_thz"]) if row.get("target_freq_fit_thz") else None
        gamma_err = float(row["gamma_freq_abs_err_thz"]) if row.get("gamma_freq_abs_err_thz") else None
        target_err = float(row["target_freq_abs_err_thz"]) if row.get("target_freq_abs_err_thz") else None
        rows.append(
            {
                "pair_code": row["pair_code"],
                "coupling_type": row["coupling_type"],
                "point_label": row["point_label"],
                "qx": float(row["qx"]),
                "qy": float(row["qy"]),
                "qz": float(row["qz"]),
                "gamma_mode_code": row["gamma_mode_code"],
                "target_mode_code": row["target_mode_code"],
                "gamma_freq_ref_thz": float(row["gamma_freq_ref_thz"]) if row.get("gamma_freq_ref_thz") else None,
                "gamma_freq_fit_thz": gamma_fit,
                "gamma_freq_abs_err_thz": gamma_err,
                "target_freq_ref_thz": float(row["target_freq_ref_thz"]) if row.get("target_freq_ref_thz") else None,
                "target_freq_fit_thz": target_fit,
                "target_freq_abs_err_thz": target_err,
                "rmse_ev_supercell": float(row["rmse_ev_supercell"]) if row.get("rmse_ev_supercell") else None,
                "mean_norm_rank": float(idx) / float(total),
                "max_norm_rank": float(idx) / float(total),
                "phi122_mean_mev": phi122,
                "phi122_min_mev": phi122,
                "phi122_max_mev": phi122,
                "per_run": [
                    {
                        "run_tag": backend_tag,
                        "rank": idx,
                        "phi122_mev": phi122,
                        "gamma_freq_fit_thz": gamma_fit,
                        "gamma_freq_abs_err_thz": gamma_err,
                        "target_freq_fit_thz": target_fit,
                        "target_freq_abs_err_thz": target_err,
                    }
                ],
            }
        )
    payload = {"run_tags": [backend_tag], "rows": rows}
    out_json = ranking_csv.with_name("single_backend_ranking.json")
    dump_json(out_json, payload)
    return out_json


def _job_dirs_from_manifest(manifest_path: Path):
    manifest = json.loads(manifest_path.read_text())
    job_dirs = []
    for pair_dir_str in manifest["pair_dirs"]:
        pair_dir = Path(pair_dir_str)
        with (pair_dir / "amplitude_grid.csv").open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                job_dirs.append(pair_dir / row["job_name"])
    return job_dirs


def _read_job_status(job_dir: Path):
    path = job_dir / "job_status.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def wait_for_qe_completion(qe_run_root: Path):
    job_dirs = _job_dirs_from_manifest(qe_run_root / "run_manifest.json")
    total = len(job_dirs)
    while True:
        completed = 0
        active_ids = []
        for job_dir in job_dirs:
            if extract_energy_ry(job_dir / "scf.out") is not None:
                completed += 1
                continue
            status = _read_job_status(job_dir)
            if status and status.get("state") in {"submitted", "running"} and status.get("job_id"):
                active_ids.append(str(status["job_id"]))

        active = len(squeue_existing_job_ids(active_ids))
        payload = {
            "timestamp_epoch": time.time(),
            "total_jobs": total,
            "completed_jobs": completed,
            "active_workflow_jobs": active,
        }
        dump_json(qe_run_root / "controller_status.json", payload)
        print(f"[controller] completed {completed}/{total}, active workflow jobs {active}", flush=True)

        if completed >= total:
            return "all_completed"
        if active == 0:
            return "stalled_incomplete"
        time.sleep(QE_POLL_SECONDS)


def run_qe_recheck(spec: dict, ranking_json: Path, qe_root: Path):
    slurm_settings = resolve_qe_slurm_settings()
    prepare_cmd = [
        sys.executable,
        str(qe_prepare_script()),
        "--consensus-json",
        str(ranking_json),
        "--mode-pairs-json",
        str(MODE_PAIRS_JSON),
        "--scf-template",
        str(STRUCTURE),
        "--pseudo-dir",
        str(PSEUDO_DIR),
        "--output-dir",
        str(qe_root),
        "--top-n",
        str(QE_TOP_N),
        "--ntasks",
        str(QE_NTASKS),
        "--partition",
        slurm_settings["partition"],
        "--walltime",
        slurm_settings["walltime"],
        "--qe-scf-profile-level",
        QE_SCF_PROFILE_LEVEL,
        "--qe-static-preset",
        QE_STATIC_PRESET,
        "--slurm-job-prefix",
        spec["tag"],
        "--launcher-command",
        QE_LAUNCHER_COMMAND,
    ]
    if slurm_settings.get("qos"):
        prepare_cmd.extend(["--qos", slurm_settings["qos"]])
    for line in QE_ENV_INIT_LINES:
        prepare_cmd.extend(["--env-init-line", line])
    run(prepare_cmd, cwd=ROOT)

    submit_cmd = [
        sys.executable,
        str(qe_submit_script()),
        "--run-root",
        str(qe_root),
        "--max-running-jobs",
        str(QE_MAX_RUNNING_JOBS),
        "--poll-seconds",
        str(QE_POLL_SECONDS),
    ]
    run(submit_cmd, cwd=ROOT)
    final_state = wait_for_qe_completion(qe_root)

    collect_cmd = [
        sys.executable,
        str(qe_collect_script()),
        "--run-root",
        str(qe_root),
    ]
    run(collect_cmd, cwd=ROOT)
    return final_state, qe_root / "results" / "qe_ranking.json"


def main():
    args = parse_args()
    scheduler_mode = resolve_scheduler_mode(args.scheduler)
    scheduler_info = scheduler_capabilities(scheduler_mode)
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    run_tag = args.run_tag or time.strftime("run_%Y%m%d_%H%M%S")
    run_root = output_root / run_tag
    if run_root.exists():
        if not args.force:
            raise RuntimeError(f"Run root already exists: {run_root}. Use --force to overwrite.")
        shutil.rmtree(run_root)
    run_root.mkdir(parents=True, exist_ok=True)

    workflow_summary = {
        "run_root": str(run_root),
        "mode_pairs_json": str(MODE_PAIRS_JSON),
        "structure": str(STRUCTURE),
        "scheduler": scheduler_info,
        "attempts": [],
        "selected_backend": None,
        "status": "started",
    }
    dump_json(run_root / "workflow_summary.json", workflow_summary)

    mlff_root = run_root / "mlff"
    mlff_root.mkdir(parents=True, exist_ok=True)

    for spec in BACKEND_SPECS:
        attempt = {
            "backend_tag": spec["tag"],
            "backend": spec["backend"],
            "model": str(spec["model"]),
            "scheduler": scheduler_info,
        }
        try:
            probe_backend(spec)
            runtime, runtime_meta, runtime_config_path = resolve_mlff_runtime(args)
            attempt["runtime"] = runtime
            attempt["runtime_meta"] = runtime_meta
            attempt["runtime_config_path"] = None if runtime_config_path is None else str(runtime_config_path)
            gate_pass = True
            gate_reasons = []
            if MLFF_RUN_BENCHMARK:
                benchmark_path = run_benchmark(spec, mlff_root)
                benchmark = json.loads(benchmark_path.read_text())
                gate_pass, gate_reasons = passes_golden_gate(benchmark["golden_compare"])
                attempt["benchmark_summary"] = str(benchmark_path)
                attempt["golden_compare"] = benchmark["golden_compare"]
                attempt["gate_pass"] = gate_pass
                attempt["gate_reasons"] = gate_reasons
            else:
                attempt["benchmark_skipped"] = True

            if not gate_pass:
                attempt["status"] = "benchmark_only"
                workflow_summary["attempts"].append(attempt)
                dump_json(run_root / "workflow_summary.json", workflow_summary)
                continue

            if scheduler_mode == "slurm":
                workflow_summary["status"] = "mlff_queued"
                dump_json(run_root / "workflow_summary.json", workflow_summary)
                ranking_csv, mlff_job_root, mlff_job_id = run_screening(spec, mlff_root, args, scheduler_mode)
                attempt["screening_job_root"] = str(mlff_job_root)
                attempt["screening_job_id"] = str(mlff_job_id)
                continuation_job_id = submit_continuation_job(run_root, spec, ranking_csv, mlff_job_root, mlff_job_id)
                attempt["continuation_job_id"] = str(continuation_job_id)
                attempt["status"] = "submitted"
                workflow_summary["selected_backend"] = spec["tag"]
                workflow_summary["status"] = "submitted_mlff_chain"
                dump_json(run_root / "workflow_summary.json", workflow_summary)
                print(f"workflow submitted with backend: {spec['tag']}", flush=True)
                workflow_summary["attempts"].append(attempt)
                dump_json(run_root / "workflow_summary.json", workflow_summary)
                return
            else:
                workflow_summary["status"] = "mlff_running"
                dump_json(run_root / "workflow_summary.json", workflow_summary)
                ranking_csv = run_screening(spec, mlff_root, args, scheduler_mode)

            workflow_summary["status"] = "mlff_finished"
            dump_json(run_root / "workflow_summary.json", workflow_summary)
            ranking_json = normalize_ranking_csv(ranking_csv, spec["tag"])
            attempt["screening_csv"] = str(ranking_csv)
            attempt["screening_ranking_json"] = str(ranking_json)

            if scheduler_mode == "local" or not slurm_available():
                attempt["status"] = "screened_only_no_slurm"
                workflow_summary["selected_backend"] = spec["tag"]
                workflow_summary["status"] = "screened_only"
                workflow_summary["attempts"].append(attempt)
                dump_json(run_root / "workflow_summary.json", workflow_summary)
                print("screening complete; QE recheck skipped because scheduler mode is local or Slurm is unavailable", flush=True)
                return

            qe_root = run_root / "qe_recheck" / spec["tag"]
            final_state, qe_ranking_json = run_qe_recheck(spec, ranking_json, qe_root)
            attempt["qe_run_root"] = str(qe_root)
            attempt["qe_final_state"] = final_state
            attempt["qe_ranking_json"] = str(qe_ranking_json)
            workflow_summary["selected_backend"] = spec["tag"]
            workflow_summary["status"] = "qe_completed"
            workflow_summary["attempts"].append(attempt)
            dump_json(run_root / "workflow_summary.json", workflow_summary)
            print(f"workflow complete with backend: {spec['tag']}", flush=True)
            return
        except Exception as exc:
            attempt["status"] = "failed"
            attempt["error"] = f"{type(exc).__name__}: {exc}"

        workflow_summary["attempts"].append(attempt)
        dump_json(run_root / "workflow_summary.json", workflow_summary)

    workflow_summary["status"] = "no_backend_reached_qe"
    dump_json(run_root / "workflow_summary.json", workflow_summary)
    print(f"no backend reached QE recheck; summary: {run_root / 'workflow_summary.json'}", flush=True)


if __name__ == "__main__":
    main()
