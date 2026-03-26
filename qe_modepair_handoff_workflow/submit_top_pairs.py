#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import time
from pathlib import Path

from common import dump_json, extract_energy_ry, parse_sbatch_job_id, squeue_existing_job_ids


def parse_args():
    p = argparse.ArgumentParser(description="Submit QE top-pair jobs with concurrency control")
    p.add_argument("--run-root", type=str, required=True)
    p.add_argument("--max-running-jobs", type=int, default=20)
    p.add_argument("--poll-seconds", type=int, default=20)
    p.add_argument("--max-submit-attempts", type=int, default=2)
    p.add_argument("--stop-on-pair-failure", action="store_true")
    return p.parse_args()


def _load_manifest(run_root: Path):
    return json.loads((run_root / "run_manifest.json").read_text())


def _all_job_dirs(manifest: dict):
    out = []
    for pair_dir_str in manifest["pair_dirs"]:
        pair_dir = Path(pair_dir_str)
        with (pair_dir / "amplitude_grid.csv").open() as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("job_name"):
                    out.append(pair_dir / row["job_name"])
                elif row.get("job_dir"):
                    candidate = Path(row["job_dir"])
                    out.append(candidate if candidate.is_absolute() else pair_dir / candidate)
                else:
                    raise KeyError(f"Unsupported amplitude_grid.csv schema in {pair_dir}: {reader.fieldnames}")
    return out


def _read_job_status(job_dir: Path):
    path = job_dir / "job_status.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _write_job_status(job_dir: Path, payload: dict):
    dump_json(job_dir / "job_status.json", payload)


def _current_active_ids(job_dirs: list[Path]):
    ids = []
    for job_dir in job_dirs:
        status = _read_job_status(job_dir)
        if not status:
            continue
        if status.get("state") in {"submitted", "running"} and status.get("job_id"):
            ids.append(str(status["job_id"]))
    return ids


def _pair_dir_for_job(job_dir: Path):
    return job_dir.parent


def _job_failed(job_dir: Path):
    if any(job_dir.glob("qe_error_*.err")):
        return True
    slurm_outs = sorted(job_dir.glob("slurm-*.out"))
    if slurm_outs:
        for path in slurm_outs[-3:]:
            text = path.read_text(errors="ignore")
            if "JOB DONE" not in text:
                return True
    scf_out = job_dir / "scf.out"
    if scf_out.exists():
        text = scf_out.read_text(errors="ignore")
        if "convergence NOT achieved" in text:
            return True
    return False


def main():
    args = parse_args()
    run_root = Path(args.run_root).expanduser().resolve()
    manifest = _load_manifest(run_root)
    job_dirs = _all_job_dirs(manifest)

    submission_rows = []

    while True:
        submitted_ids = _current_active_ids(job_dirs)
        queued_ids = squeue_existing_job_ids(submitted_ids)
        running_count = len(queued_ids)

        pending = []
        completed_count = 0
        failed_pairs = set()

        for job_dir in job_dirs:
            if extract_energy_ry(job_dir / "scf.out") is not None:
                completed_count += 1
                _write_job_status(job_dir, {"state": "completed"})
                continue

            status = _read_job_status(job_dir)
            attempts = 0 if not status else int(status.get("submit_attempts", 1 if status.get("job_id") else 0))
            if status and status.get("job_id") and str(status["job_id"]) in queued_ids:
                if status.get("state") != "running":
                    status["state"] = "running"
                    _write_job_status(job_dir, status)
                continue

            if status and attempts >= args.max_submit_attempts and _job_failed(job_dir):
                status["state"] = "exhausted"
                _write_job_status(job_dir, status)
                failed_pairs.add(_pair_dir_for_job(job_dir))
                continue

            if status and _job_failed(job_dir):
                status["state"] = "failed"
                _write_job_status(job_dir, status)
                if args.stop_on_pair_failure:
                    failed_pairs.add(_pair_dir_for_job(job_dir))
                continue

            pending.append(job_dir)

        if args.stop_on_pair_failure:
            pending = [job_dir for job_dir in pending if _pair_dir_for_job(job_dir) not in failed_pairs]

        if not pending:
            dump_json(
                run_root / "submission_log.json",
                {
                    "submitted_rows": submission_rows,
                    "active_count": running_count,
                    "completed_count": completed_count,
                    "total_jobs": len(job_dirs),
                },
            )
            print(f"submission phase finished: {completed_count} completed, {running_count} still active")
            return

        available = args.max_running_jobs - running_count
        if available <= 0:
            time.sleep(args.poll_seconds)
            continue

        sent_now = 0
        for job_dir in pending:
            if sent_now >= available:
                break

            result = subprocess.run(["sbatch", str(job_dir / "submit.sh")], capture_output=True, text=True)
            stdout = result.stdout.strip()
            stderr = result.stderr.strip()
            if result.returncode != 0:
                raise RuntimeError(f"sbatch failed for {job_dir}:\n{stdout}\n{stderr}")

            job_id = parse_sbatch_job_id(stdout)
            if not job_id:
                raise RuntimeError(f"Could not parse sbatch job id for {job_dir}: {stdout}")

            previous_status = _read_job_status(job_dir)
            previous_attempts = 0 if previous_status is None else int(
                previous_status.get("submit_attempts", 1 if previous_status.get("job_id") else 0)
            )
            payload = {
                "state": "submitted",
                "job_id": job_id,
                "sbatch_stdout": stdout,
                "sbatch_stderr": stderr,
                "submit_time_epoch": time.time(),
                "submit_attempts": previous_attempts + 1,
            }
            _write_job_status(job_dir, payload)
            submission_rows.append({"job_dir": str(job_dir), "job_id": job_id, "stdout": stdout})
            sent_now += 1

        dump_json(
            run_root / "submission_log.json",
            {
                "submitted_rows": submission_rows,
                "active_count": running_count + sent_now,
                "completed_count": completed_count,
                "total_jobs": len(job_dirs),
            },
        )

        if sent_now == 0:
            time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()
