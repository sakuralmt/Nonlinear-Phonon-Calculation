#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import shutil
import subprocess
import time
from pathlib import Path

from common import (
    dump_json,
    ensure_dir,
    extract_energy_ry,
    file_contains_job_done,
    parse_sbatch_job_id,
    squeue_existing_job_ids,
)
from config import (
    FILDYN_PREFIX,
    FORCE_CONSTANT_FILE,
    MATDYN_EIG_FILE,
    MATDYN_ROOT,
    POLL_SECONDS,
    PSEUDO_DIR,
    RAW_SCF_TEMPLATE,
    REQUESTS_JSON,
    RESET_IF_RUN_ROOT_EXISTS,
    RESULTS_ROOT,
    RUN_ROOT,
)
from stage1_env import (
    STAGE1_ENV_ASSESSMENT_JSON,
    STAGE1_RUNTIME_CONFIG_JSON,
    build_runtime_signature,
    ensure_stage1_environment_assessed,
)
from step1_frontend import prepare_frontend

FAILED_STAGE_STATES = {
    "BOOT_FAIL": "failed",
    "CANCELLED": "cancelled",
    "DEADLINE": "failed",
    "FAILED": "failed",
    "NODE_FAIL": "node_fail",
    "OUT_OF_MEMORY": "out_of_memory",
    "PREEMPTED": "preempted",
    "REVOKED": "failed",
    "TIMEOUT": "timeout",
}


def _require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise RuntimeError(f"Missing {label}: {path}")


def preflight() -> None:
    _require_file(REQUESTS_JSON, "requested-pair payload")
    _require_file(RAW_SCF_TEMPLATE, "scf template")
    _require_file(PSEUDO_DIR / "W.pz-spn-rrkjus_psl.1.0.0.UPF", "W pseudopotential")
    _require_file(PSEUDO_DIR / "Se.pz-n-rrkjus_psl.0.2.UPF", "Se pseudopotential")
    for exe in ["pw.x", "ph.x", "q2r.x", "matdyn.x", "sbatch", "squeue"]:
        if shutil.which(exe) is None:
            raise RuntimeError(f"Required executable not found in PATH: {exe}")


def _load_frontend_manifest() -> dict:
    return json.loads((RUN_ROOT / "frontend_manifest.json").read_text())


def _frontend_manifest_matches_runtime(assessment: dict) -> bool:
    frontend_manifest = RUN_ROOT / "frontend_manifest.json"
    if not frontend_manifest.exists():
        return False
    try:
        manifest = _load_frontend_manifest()
    except Exception:
        return False
    return manifest.get("runtime_signature") == build_runtime_signature(assessment)


def _stage_complete(stage_name: str, stage_dir: Path) -> bool:
    if stage_name == "pw":
        return extract_energy_ry(stage_dir / "scf.out") is not None
    if stage_name == "ph":
        return file_contains_job_done(stage_dir / "ph.out") and (stage_dir / f"{FILDYN_PREFIX}0").exists()
    if stage_name == "q2r":
        return file_contains_job_done(stage_dir / "q2r.out") and (stage_dir / FORCE_CONSTANT_FILE).exists()
    if stage_name == "matdyn":
        return (stage_dir / MATDYN_EIG_FILE).exists()
    raise KeyError(f"Unknown stage: {stage_name}")


def _stage_status_path(stage_dir: Path) -> Path:
    return stage_dir / "job_status.json"


def _read_stage_status(stage_dir: Path):
    path = _stage_status_path(stage_dir)
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _write_stage_status(stage_dir: Path, payload: dict) -> None:
    dump_json(_stage_status_path(stage_dir), payload)


def _write_controller_status(payload: dict) -> None:
    ensure_dir(RESULTS_ROOT)
    dump_json(RESULTS_ROOT / "controller_status.json", payload)


def _normalize_slurm_state(raw_state: str | None) -> str | None:
    if raw_state is None:
        return None
    match = re.match(r"[A-Z_]+", str(raw_state).strip().upper())
    return None if match is None else match.group(0)


def _query_sacct_status(job_id: str) -> dict | None:
    result = subprocess.run(
        ["sacct", "-j", str(job_id), "--format=State,ExitCode,DerivedExitCode", "-n", "-P", "-X"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        row = line.strip()
        if not row:
            continue
        parts = row.split("|")
        raw_state = parts[0].strip() if len(parts) >= 1 else ""
        normalized_state = _normalize_slurm_state(raw_state)
        if normalized_state is None:
            continue
        return {
            "raw_state": raw_state,
            "normalized_state": normalized_state,
            "exit_code": parts[1].strip() if len(parts) >= 2 else None,
            "derived_exit_code": parts[2].strip() if len(parts) >= 3 else None,
            "reason": None,
            "source": "sacct",
        }
    return None


def _query_scontrol_status(job_id: str) -> dict | None:
    result = subprocess.run(
        ["scontrol", "show", "job", str(job_id)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    raw_state_match = re.search(r"\bJobState=([^\s]+)", result.stdout)
    if raw_state_match is None:
        return None
    raw_state = raw_state_match.group(1).strip()
    normalized_state = _normalize_slurm_state(raw_state)
    if normalized_state is None:
        return None
    exit_code_match = re.search(r"\bExitCode=([^\s]+)", result.stdout)
    reason_match = re.search(r"\bReason=([^\s]+)", result.stdout)
    return {
        "raw_state": raw_state,
        "normalized_state": normalized_state,
        "exit_code": None if exit_code_match is None else exit_code_match.group(1).strip(),
        "derived_exit_code": None,
        "reason": None if reason_match is None else reason_match.group(1).strip(),
        "source": "scontrol",
    }


def _query_terminal_job_status(job_id: str | None) -> dict | None:
    if not job_id:
        return None
    status = _query_sacct_status(str(job_id))
    if status is not None:
        return status
    return _query_scontrol_status(str(job_id))


def _inactive_stage_payload(stage_name: str, stage_dir: Path, job_id: str | None) -> dict:
    timestamp = time.time()
    if _stage_complete(stage_name, stage_dir):
        return {
            "state": "completed",
            "job_id": job_id,
            "update_time_epoch": timestamp,
        }

    terminal = _query_terminal_job_status(job_id)
    if terminal is None:
        return {
            "state": "idle",
            "job_id": job_id,
            "update_time_epoch": timestamp,
        }

    normalized_state = terminal.get("normalized_state")
    stage_state = FAILED_STAGE_STATES.get(normalized_state, "completed_without_outputs" if normalized_state == "COMPLETED" else "idle")
    return {
        "state": stage_state,
        "job_id": job_id,
        "slurm_state": terminal.get("raw_state"),
        "slurm_state_normalized": normalized_state,
        "slurm_reason": terminal.get("reason"),
        "slurm_exit_code": terminal.get("exit_code"),
        "slurm_derived_exit_code": terminal.get("derived_exit_code"),
        "slurm_query_source": terminal.get("source"),
        "update_time_epoch": timestamp,
    }


def _submit_stage(stage_name: str, stage_dir: Path, submit_script: Path) -> str:
    result = subprocess.run(["sbatch", str(submit_script)], capture_output=True, text=True)
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if result.returncode != 0:
        raise RuntimeError(f"sbatch failed for stage {stage_name}:\n{stdout}\n{stderr}")
    job_id = parse_sbatch_job_id(stdout)
    if not job_id:
        raise RuntimeError(f"Could not parse sbatch job id for stage {stage_name}: {stdout}")
    _write_stage_status(
        stage_dir,
        {
            "state": "submitted",
            "job_id": job_id,
            "sbatch_stdout": stdout,
            "sbatch_stderr": stderr,
            "submit_time_epoch": time.time(),
        },
    )
    return job_id


def _wait_for_stage(stage_name: str, stage_dir: Path) -> bool:
    while True:
        if _stage_complete(stage_name, stage_dir):
            completed_payload = {"state": "completed", "update_time_epoch": time.time()}
            _write_stage_status(stage_dir, completed_payload)
            _write_controller_status(
                {
                    "phase": "frontend",
                    "frontend_stage": stage_name,
                    **completed_payload,
                    "timestamp_epoch": completed_payload["update_time_epoch"],
                }
            )
            return True
        status = _read_stage_status(stage_dir)
        job_id = None if not status else status.get("job_id")
        active = bool(job_id and str(job_id) in squeue_existing_job_ids([str(job_id)]))
        if active:
            _write_controller_status(
                {
                    "phase": "frontend",
                    "frontend_stage": stage_name,
                    "state": "running",
                    "job_id": job_id,
                    "timestamp_epoch": time.time(),
                }
            )
        else:
            inactive_payload = _inactive_stage_payload(stage_name, stage_dir, job_id)
            _write_stage_status(stage_dir, inactive_payload)
            _write_controller_status(
                {
                    "phase": "frontend",
                    "frontend_stage": stage_name,
                    **inactive_payload,
                    "timestamp_epoch": inactive_payload["update_time_epoch"],
                }
            )
            return inactive_payload["state"] == "completed"
        time.sleep(POLL_SECONDS)


def _ensure_frontend_completed() -> None:
    manifest = _load_frontend_manifest()
    for stage in manifest["stages"]:
        stage_name = stage["name"]
        stage_dir = Path(stage["stage_dir"])
        submit_script = Path(stage["submit_script"])
        if _stage_complete(stage_name, stage_dir):
            _write_stage_status(stage_dir, {"state": "completed"})
            continue
        status = _read_stage_status(stage_dir)
        queued = False
        if status and status.get("job_id"):
            queued = str(status["job_id"]) in squeue_existing_job_ids([str(status["job_id"])])
        if queued:
            if _wait_for_stage(stage_name, stage_dir):
                continue
        _submit_stage(stage_name, stage_dir, submit_script)
        if not _wait_for_stage(stage_name, stage_dir):
            raise RuntimeError(f"Frontend stage stalled without completion: {stage_name}")


def _write_stage1_summary() -> Path:
    manifest = _load_frontend_manifest()
    payload = {
        "bundle_scope": "stage1_only",
        "status": "completed",
        "frontend_manifest": str(RUN_ROOT / "frontend_manifest.json"),
        "runtime_assessment_json": str(STAGE1_ENV_ASSESSMENT_JSON),
        "runtime_config_json": str(STAGE1_RUNTIME_CONFIG_JSON),
        "matdyn_root": manifest["matdyn_root"],
        "requests_json": manifest["requests_json"],
        "request_count": len(manifest.get("requested_points", [])),
        "matdyn_q_point_count": len(manifest.get("matdyn_q_points", [])),
        "generated_files": {
            "eig": str(MATDYN_ROOT / "qeph.eig"),
            "freq": str(MATDYN_ROOT / "qeph.freq")
        },
        "completed_at_epoch": time.time()
    }
    out = RESULTS_ROOT / "stage1_summary.json"
    dump_json(out, payload)
    return out


def main() -> None:
    preflight()
    ensure_dir(RESULTS_ROOT)
    print("[stage1] assess runtime")
    runtime_assessment = ensure_stage1_environment_assessed()

    if RUN_ROOT.exists() and RESET_IF_RUN_ROOT_EXISTS:
        shutil.rmtree(RUN_ROOT)
    ensure_dir(RUN_ROOT)

    if not _frontend_manifest_matches_runtime(runtime_assessment):
        print("[stage1] prepare frontend")
        prepare_frontend()

    print("[stage1] submit and wait frontend")
    _ensure_frontend_completed()
    summary_path = _write_stage1_summary()
    _write_controller_status(
        {
            "phase": "frontend",
            "state": "all_completed",
            "summary": str(summary_path),
            "timestamp_epoch": time.time()
        }
    )
    print(f"[stage1] done: {summary_path}")


if __name__ == "__main__":
    main()
