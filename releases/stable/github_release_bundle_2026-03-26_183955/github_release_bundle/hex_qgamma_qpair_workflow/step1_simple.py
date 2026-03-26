#!/usr/bin/env python3
"""
Simple step-1 workflow for hexagonal Q_gamma Q_q Q_-q screening.

Usage:
    cd <calculation directory>
    python step1_simple.py
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path


# ============================
# User configuration
# ============================
WORK_DIR = Path.cwd()
SCF_TEMPLATE = "scf.inp"
FORCE_CONSTANTS = None
GRID_N = 6

RUN_ROOT_NAME = "hex_qgamma_qpair_run"
SCREEN_DIR_NAME = "screening"
JOB_DIR_NAME = "matdyn_job"
EXTRACT_DIR_NAME = "extracted"

FLFRQ = "screened_hex_6x6.freq"
FLEIG = "screened_hex_6x6.eig"

JOB_NAME = "ph_hex_qpair"
NODES = 1
NTASKS_PER_NODE = 50
PARTITION = "long"
WALLTIME = "3-00:00:00"
LOAD_MODULES = False
ENV_INIT_LINES = []
MATDYN_LAUNCHER_COMMAND = "mpirun -np {ntasks} matdyn.x < matdyn.inp > dynmat.out"

AUTO_SUBMIT = False
FORCE_RESUBMIT = False


SCRIPT_DIR = Path(__file__).resolve().parent
SCREEN_SCRIPT = SCRIPT_DIR / "screen_hex_qgamma_qpair_points.py"
PREPARE_SCRIPT = SCRIPT_DIR / "prepare_matdyn_hex_qgamma_qpair.py"
EXTRACT_SCRIPT = SCRIPT_DIR / "extract_screened_eigs.py"


def run_python(script: Path, *args: str):
    cmd = [sys.executable, str(script), *args]
    return subprocess.run(cmd, check=True, text=True)


def ensure_file(path: Path, label: str):
    if not path.exists():
        raise FileNotFoundError(f"Missing {label}: {path}")


def resolve_force_constants(work_dir: Path):
    if FORCE_CONSTANTS is not None:
        fc_path = work_dir / FORCE_CONSTANTS
        ensure_file(fc_path, "force constants")
        return fc_path

    preferred = ["MM.fc", "WSe2.fc"]
    for name in preferred:
        fc_path = work_dir / name
        if fc_path.exists():
            return fc_path

    candidates = sorted(work_dir.glob("*.fc"))
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        raise RuntimeError(f"Multiple .fc files found, set FORCE_CONSTANTS explicitly: {[p.name for p in candidates]}")
    raise FileNotFoundError(f"No force constants file found in {work_dir}")


def main():
    work_dir = WORK_DIR.expanduser().resolve()
    run_root = work_dir / RUN_ROOT_NAME
    screen_dir = run_root / SCREEN_DIR_NAME
    job_dir = run_root / JOB_DIR_NAME
    extract_dir = run_root / EXTRACT_DIR_NAME
    screening_json = screen_dir / "screening_summary.json"
    eig_file = job_dir / FLEIG
    submission_info = job_dir / "submission_info.json"

    ensure_file(work_dir / SCF_TEMPLATE, "scf template")
    fc_path = resolve_force_constants(work_dir)

    run_root.mkdir(parents=True, exist_ok=True)

    run_python(
        SCREEN_SCRIPT,
        "--work-dir",
        str(work_dir),
        "--scf-template",
        SCF_TEMPLATE,
        "--grid-n",
        str(GRID_N),
        "--output-dir",
        str(screen_dir),
    )

    prepare_args = [
        "--screening-json",
        str(screening_json),
        "--job-dir",
        str(job_dir),
        "--flfrc",
        fc_path.name,
        "--flfrq",
        FLFRQ,
        "--fleig",
        FLEIG,
        "--job-name",
        JOB_NAME,
        "--nodes",
        str(NODES),
        "--ntasks-per-node",
        str(NTASKS_PER_NODE),
        "--partition",
        PARTITION,
        "--walltime",
        WALLTIME,
    ]
    if LOAD_MODULES:
        prepare_args.append("--load-modules")
    prepare_args.extend(["--launcher-command", MATDYN_LAUNCHER_COMMAND])
    for line in ENV_INIT_LINES:
        prepare_args.extend(["--env-init-line", line])
    run_python(PREPARE_SCRIPT, *prepare_args)

    shutil.copy2(fc_path, job_dir / fc_path.name)

    def extract_now():
        run_python(
            EXTRACT_SCRIPT,
            "--eig-file",
            str(eig_file),
            "--screening-json",
            str(screening_json),
            "--scf-template",
            str(work_dir / SCF_TEMPLATE),
            "--q-format",
            "auto",
            "--grid-n",
            str(GRID_N),
            "--output-dir",
            str(extract_dir),
        )

    submitted = submission_info.exists()
    if eig_file.exists():
        extract_now()
        status = "extracted"
    elif AUTO_SUBMIT:
        if submitted and not FORCE_RESUBMIT:
            if eig_file.exists():
                extract_now()
                status = "extracted"
            else:
                status = "already_submitted_waiting_eig"
        else:
            result = subprocess.run(["sbatch", "run.sh"], cwd=job_dir, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"sbatch failed: {result.stderr.strip()}")
            submission_info.write_text(
                json.dumps(
                    {
                        "command": ["sbatch", "run.sh"],
                        "stdout": result.stdout.strip(),
                        "stderr": result.stderr.strip(),
                    },
                    indent=2,
                )
            )
            if eig_file.exists():
                extract_now()
                status = "submitted_and_extracted"
            else:
                status = "submitted_waiting_eig"
    else:
        status = "prepared_only"

    manifest = {
        "kind": "hex_qgamma_qpair_step1_simple",
        "work_dir": str(work_dir),
        "run_root": str(run_root),
        "screening_json": str(screening_json),
        "job_dir": str(job_dir),
        "eig_file": str(eig_file),
        "extract_dir": str(extract_dir),
        "auto_submit": AUTO_SUBMIT,
        "status": status,
    }
    (run_root / "step1_manifest.json").write_text(json.dumps(manifest, indent=2))

    print(f"run root: {run_root}")
    print(f"status: {status}")
    print(f"screening: {screening_json}")
    print(f"job dir: {job_dir}")
    if eig_file.exists():
        print(f"extracted: {extract_dir}")


if __name__ == "__main__":
    main()
