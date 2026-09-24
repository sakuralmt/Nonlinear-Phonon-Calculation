"""One bounded Slurm controller for every registered v3 QE recheck run."""

from __future__ import annotations

import argparse
import fcntl
import functools
import itertools
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

from .stage3_v3 import parse_qe_output

JOB_ID_RE = re.compile(r"Submitted batch job\s+(\d+)")


def _atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(payload, indent=2) + "\n")
    os.replace(temp, path)


def register(state_root: Path, run_root: Path) -> dict:
    state_root, run_root = Path(state_root).resolve(), Path(run_root).resolve()
    manifest = json.loads((run_root / "run_manifest.json").read_text())
    if manifest.get("kind") != "qe_v3_stage3_run":
        raise ValueError("Only v3 QE Stage3 run roots can be registered")
    state_root.mkdir(parents=True, exist_ok=True)
    with (state_root / ".registry.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        path = state_root / "registry.json"
        payload = json.loads(path.read_text()) if path.exists() else {"run_roots": []}
        if str(run_root) not in payload["run_roots"]:
            payload["run_roots"].append(str(run_root))
            _atomic(path, payload)
    return payload


def _jobs(run_root: Path, pair_limit: int | None = None):
    manifest = json.loads((run_root / "run_manifest.json").read_text())
    codes = manifest["selected_pair_codes"]
    if pair_limit is not None:
        codes = codes[:pair_limit]
    for code in codes:
        root = run_root / "pairs" / code
        meta = json.loads((root / "pair_meta.json").read_text())
        natoms = meta["builder"]["nat_super"]
        for i in range(9):
            for j in range(9):
                yield root / f"grid_{i:02d}_{j:02d}", natoms


def _queue() -> dict[str, tuple[str, str]]:
    result = subprocess.run(["squeue", "-h", "-u", os.environ["USER"],
                             "-o", "%i|%j|%T"], capture_output=True, text=True, check=True)
    jobs = {}
    for line in result.stdout.splitlines():
        fields = line.strip().split("|", 2)
        if len(fields) == 3:
            jobs[fields[0]] = (fields[1], fields[2])
    return jobs


def _count_stage3_active(queue: dict[str, tuple[str, str]]) -> int:
    # Include older QE rechecks to keep the user-wide Stage3 concurrency safe.
    return sum((name.startswith(("qv3_", "qe_")) or re.fullmatch(r".+_r\d{2}_\d{4}", name))
               and state not in {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"}
               for name, state in queue.values())


def _status(job_dir: Path) -> dict:
    path = job_dir / "job_status.json"
    return json.loads(path.read_text()) if path.exists() else {"state": "unsubmitted", "attempts": 0}


@functools.lru_cache(maxsize=None)
def _job_name(job_dir: Path) -> str:
    script = job_dir / "submit.sh"
    if not script.is_file() or not (job_dir / "scf.inp").is_file():
        raise FileNotFoundError(f"Stage3 point was not prepared: {job_dir}")
    match = re.search(r"^#SBATCH --job-name=(\S+)$", script.read_text(), re.M)
    if match is None:
        raise ValueError(f"Stage3 Slurm script has no job name: {script}")
    return match.group(1)


def _snapshot(state_root: Path, queue: dict | None = None, pair_limit: int | None = None) -> dict:
    registry = json.loads((state_root / "registry.json").read_text())
    queue = _queue() if queue is None else queue
    queued_by_name = {}
    for job_id, (name, _) in queue.items():
        queued_by_name.setdefault(name, []).append(job_id)
    rows = []
    candidates_by_run = []
    uncertain_active = 0
    for root_string in registry["run_roots"]:
        root = Path(root_string)
        counts = {key: 0 for key in ("complete", "active", "pending", "exhausted")}
        run_candidates = []
        for job_dir, natoms in _jobs(root, pair_limit):
            status = _status(job_dir)
            output = job_dir / "scf.out"
            if status["state"] == "complete" and output.is_file():
                stat = output.stat()
                if (status.get("output_size") == stat.st_size
                        and status.get("output_mtime_ns") == stat.st_mtime_ns):
                    counts["complete"] += 1
                    continue
            name = _job_name(job_dir)
            job_id = str(status.get("job_id") or "")
            if job_id not in queue and name in queued_by_name:
                matches = queued_by_name[name]
                if len(matches) != 1:
                    raise RuntimeError(f"Multiple active QE jobs with the same name: {name}")
                job_id = matches[0]
                _atomic(job_dir / "job_status.json", {"state": "submitted", "job_id": job_id,
                                                        "attempts": max(1, int(status.get("attempts", 0))),
                                                        "submitted_at": time.time()})
            if job_id in queue:
                counts["active"] += 1
                continue
            if (status.get("state") == "submitted" and status.get("submitted_at") is not None
                    and time.time() - float(status["submitted_at"]) < 120):
                counts["active"] += 1
                uncertain_active += 1
                continue
            parsed = parse_qe_output(output, natoms)
            if parsed is not None:
                stat = output.stat()
                _atomic(job_dir / "job_status.json", {**status, "state": "complete",
                                                        "output_size": stat.st_size,
                                                        "output_mtime_ns": stat.st_mtime_ns})
                counts["complete"] += 1
                continue
            if int(status.get("attempts", 0)) >= 2:
                _atomic(job_dir / "job_status.json", {**status, "state": "exhausted"})
                counts["exhausted"] += 1
                continue
            counts["pending"] += 1
            run_candidates.append(job_dir)
        rows.append({"run_root": str(root), "counts": counts})
        candidates_by_run.append(run_candidates)
    # Alternate materials so a small submission batch does not starve later runs.
    candidates = [job_dir for group in itertools.zip_longest(*candidates_by_run)
                  for job_dir in group if job_dir is not None]
    return {"runs": rows, "active_stage3_jobs_userwide": _count_stage3_active(queue) + uncertain_active,
            "pending_job_dirs": candidates}


def run(state_root: Path, max_active: int = 30, submit_batch: int = 5,
        poll_seconds: int = 30, once: bool = False,
        pair_limit: int | None = None) -> None:
    state_root = Path(state_root).resolve()
    if not 1 <= max_active <= 30 or not 1 <= submit_batch <= max_active:
        raise ValueError("Stage3 requires 1 <= submit_batch <= max_active <= 30")
    if pair_limit is not None and pair_limit < 1:
        raise ValueError("pair_limit must be positive")
    if poll_seconds < 1:
        raise ValueError("poll_seconds must be positive")
    if not (state_root / "registry.json").exists():
        raise FileNotFoundError(f"No registered Stage3 runs in {state_root}")
    global_lock = Path.home() / ".cache/mlff_modepair_workflow/stage3_controller.lock"
    global_lock.parent.mkdir(parents=True, exist_ok=True)
    with global_lock.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        while True:
            queue = _queue()  # A failed Slurm query raises; never submit blind.
            snapshot = _snapshot(state_root, queue, pair_limit)
            slots = min(max_active - snapshot["active_stage3_jobs_userwide"], submit_batch)
            submitted = 0
            for job_dir in snapshot["pending_job_dirs"][:max(0, slots)]:
                completed = parse_qe_output(job_dir / "scf.out", _natoms(job_dir))
                if completed is not None:
                    continue
                result = subprocess.run(["sbatch", str(job_dir / "submit.sh")],
                                        capture_output=True, text=True, check=True)
                match = JOB_ID_RE.search(result.stdout)
                if match is None:
                    raise RuntimeError(f"Unrecognized sbatch response: {result.stdout!r}")
                previous = _status(job_dir)
                _atomic(job_dir / "job_status.json", {"state": "submitted", "job_id": match.group(1),
                                                       "attempts": int(previous.get("attempts", 0)) + 1,
                                                       "submitted_at": time.time()})
                submitted += 1
            total_pending = sum(row["counts"]["pending"] for row in snapshot["runs"])
            total_active = sum(row["counts"]["active"] for row in snapshot["runs"])
            total_exhausted = sum(row["counts"]["exhausted"] for row in snapshot["runs"])
            print(json.dumps({"runs": snapshot["runs"], "userwide_active": snapshot["active_stage3_jobs_userwide"],
                              "submitted_this_poll": submitted, "exhausted": total_exhausted,
                              "pilot_pair_limit": pair_limit}), flush=True)
            if total_pending == 0 and total_active == 0:
                if total_exhausted:
                    raise RuntimeError(f"Stage3 finished with {total_exhausted} exhausted QE points")
                return
            if once:
                return
            time.sleep(poll_seconds)


def _natoms(job_dir: Path) -> int:
    return int(json.loads((job_dir.parent / "pair_meta.json").read_text())["builder"]["nat_super"])


def launch(state_root: Path, max_active: int = 30, submit_batch: int = 5,
           poll_seconds: int = 30, pair_limit: int | None = None) -> str:
    if not 1 <= max_active <= 30 or not 1 <= submit_batch <= max_active:
        raise ValueError("Stage3 requires 1 <= submit_batch <= max_active <= 30")
    if pair_limit is not None and pair_limit < 1:
        raise ValueError("pair_limit must be positive")
    if poll_seconds < 1:
        raise ValueError("poll_seconds must be positive")
    state_root = Path(state_root).resolve()
    if not (state_root / "registry.json").is_file():
        raise FileNotFoundError(f"No registered Stage3 runs in {state_root}")
    state_root.mkdir(parents=True, exist_ok=True)
    with (state_root / ".launch.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        queue = _queue()
        controllers = [job_id for job_id, (name, _) in queue.items() if name == "qe3_global_controller"]
        if controllers:
            detail = subprocess.run(["scontrol", "show", "job", controllers[0]],
                                    capture_output=True, text=True, check=True).stdout
            if f"Command={state_root / 'controller.sbatch'}" not in detail:
                raise RuntimeError("A Stage3 controller already uses a different global state root")
            return controllers[0]
        repository = Path(__file__).resolve().parent.parent
        command = [sys.executable, "-m", "mlff_modepair_workflow.stage3_scheduler", "run",
                   "--state-root", str(state_root), "--max-active", str(max_active),
                   "--submit-batch", str(submit_batch), "--poll-seconds", str(poll_seconds)]
        if pair_limit is not None:
            command.extend(["--pair-limit", str(pair_limit)])
        script = state_root / "controller.sbatch"
        script.write_text("\n".join([
            "#!/bin/bash", "#SBATCH --job-name=qe3_global_controller",
            "#SBATCH --partition=regular", "#SBATCH --nodes=1", "#SBATCH --ntasks=1",
            "#SBATCH --time=72:00:00", f"#SBATCH --chdir={repository}",
            f"#SBATCH --output={state_root / 'controller-%j.out'}",
            f"#SBATCH --error={state_root / 'controller-%j.err'}", "",
            " ".join(shlex.quote(part) for part in command), "",
        ]))
        result = subprocess.run(["sbatch", str(script)], capture_output=True, text=True, check=True)
        match = JOB_ID_RE.search(result.stdout)
        if match is None:
            raise RuntimeError(f"Unrecognized controller sbatch response: {result.stdout!r}")
        return match.group(1)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    reg = sub.add_parser("register")
    reg.add_argument("--state-root", type=Path, required=True)
    reg.add_argument("--run-root", type=Path, required=True)
    controller = sub.add_parser("run")
    controller.add_argument("--state-root", type=Path, required=True)
    controller.add_argument("--max-active", type=int, default=30)
    controller.add_argument("--submit-batch", type=int, default=5)
    controller.add_argument("--poll-seconds", type=int, default=30)
    controller.add_argument("--once", action="store_true")
    controller.add_argument("--pair-limit", type=int, help="Pilot only: run the first N selected pairs per material")
    launch_parser = sub.add_parser("launch")
    launch_parser.add_argument("--state-root", type=Path, required=True)
    launch_parser.add_argument("--max-active", type=int, default=30)
    launch_parser.add_argument("--submit-batch", type=int, default=5)
    launch_parser.add_argument("--poll-seconds", type=int, default=30)
    launch_parser.add_argument("--pair-limit", type=int)
    status = sub.add_parser("status")
    status.add_argument("--state-root", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "register":
        print(json.dumps(register(args.state_root, args.run_root)))
    elif args.command == "run":
        run(args.state_root, args.max_active, args.submit_batch, args.poll_seconds, args.once,
            args.pair_limit)
    elif args.command == "launch":
        print(launch(args.state_root, args.max_active, args.submit_batch, args.poll_seconds,
                     args.pair_limit))
    else:
        snapshot = _snapshot(args.state_root)
        snapshot.pop("pending_job_dirs")
        print(json.dumps(snapshot, indent=2))


if __name__ == "__main__":
    main()
