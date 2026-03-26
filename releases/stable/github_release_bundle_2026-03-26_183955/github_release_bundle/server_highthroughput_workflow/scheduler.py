#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path


VALID_SCHEDULERS = {"auto", "slurm", "local"}
SLURM_RUNTIME_CONFIG_NAME = "slurm_runtime_config.json"
SLURM_CLUSTER_REPORT_NAME = "slurm_cluster_assessment.json"
SLURM_EXPORT_SCRIPT_NAME = "slurm_submit_defaults.sh"

INFINITE_TIME_TOKENS = {
    "",
    "infinite",
    "unlimited",
    "none",
    "partition_limit",
}

JOB_ENV_PREFIX = {
    "qe_relax": "QE_RELAX",
    "mlff_screening": "MLFF",
    "stage3_continuation": "STAGE3_CONTINUATION",
    "qe_recheck": "QE_RECHECK",
}


def slurm_available():
    return shutil.which("sbatch") is not None and shutil.which("squeue") is not None


def resolve_scheduler_mode(requested: str):
    requested = str(requested).strip().lower()
    if requested not in VALID_SCHEDULERS:
        raise ValueError(f"Unsupported scheduler mode: {requested}")
    if requested == "auto":
        return "slurm" if slurm_available() else "local"
    if requested == "slurm" and not slurm_available():
        raise RuntimeError("Scheduler mode 'slurm' was requested, but sbatch/squeue are not available.")
    return requested


def scheduler_capabilities(mode: str):
    mode = resolve_scheduler_mode(mode)
    return {
        "mode": mode,
        "queue_submission": mode == "slurm",
        "job_dependencies": mode == "slurm",
        "local_fallback": mode == "local",
    }


def _run_capture(cmd: list[str]):
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return {
        "cmd": cmd,
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _tokenize_scontrol_line(line: str):
    return dict(re.findall(r"(\w+)=([^\s]+)", line))


def parse_slurm_time_to_seconds(raw: str | None):
    if raw is None:
        return None
    text = str(raw).strip()
    if text.lower() in INFINITE_TIME_TOKENS:
        return None

    days = 0
    if "-" in text:
        day_text, text = text.split("-", 1)
        days = int(day_text)

    parts = text.split(":")
    if len(parts) == 3:
        hours, minutes, seconds = [int(x) for x in parts]
    elif len(parts) == 2:
        hours = 0
        minutes, seconds = [int(x) for x in parts]
    elif len(parts) == 1:
        hours = 0
        minutes = int(parts[0])
        seconds = 0
    else:
        raise ValueError(f"Unsupported Slurm time token: {raw}")
    return int((((days * 24) + hours) * 60 + minutes) * 60 + seconds)


def format_slurm_time(seconds: int | None):
    if seconds is None:
        return "UNLIMITED"
    total = int(seconds)
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}-{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _normalize_partition_name(name: str):
    return str(name).rstrip("*")


def _partition_state_is_usable(row_states: list[str]):
    if not row_states:
        return True
    for state in row_states:
        lowered = state.lower().strip("*")
        if lowered.startswith(("down", "drain", "drng", "inval", "inact")):
            continue
        return True
    return False


def probe_slurm_cluster():
    if not slurm_available():
        return {
            "available": False,
            "detected_at_epoch": time.time(),
            "partitions": [],
            "default_partition": None,
            "source_commands": [],
        }

    scontrol = _run_capture(["scontrol", "show", "partition", "-o"])
    if scontrol["returncode"] != 0:
        raise RuntimeError(f"Failed to inspect Slurm partitions:\n{scontrol['stderr'].strip()}")

    sinfo = _run_capture(["sinfo", "-h", "-o", "%P|%a|%l|%D|%t|%N"])
    sinfo_rows = []
    if sinfo["returncode"] == 0:
        for line in sinfo["stdout"].splitlines():
            parts = line.strip().split("|")
            if len(parts) != 6:
                continue
            partition_name = _normalize_partition_name(parts[0])
            sinfo_rows.append(
                {
                    "partition": partition_name,
                    "partition_display": parts[0],
                    "availability": parts[1],
                    "time_limit": parts[2],
                    "nodes": parts[3],
                    "state": parts[4],
                    "nodelist": parts[5],
                }
            )

    by_partition = {}
    for line in scontrol["stdout"].splitlines():
        if not line.strip():
            continue
        tokens = _tokenize_scontrol_line(line)
        name = _normalize_partition_name(tokens["PartitionName"])
        part = {
            "name": name,
            "display_name": tokens["PartitionName"],
            "is_default": str(tokens.get("Default", "NO")).upper() == "YES",
            "max_time_raw": tokens.get("MaxTime"),
            "default_time_raw": tokens.get("DefaultTime"),
            "nodes_raw": tokens.get("Nodes"),
            "state_raw": tokens.get("State"),
            "allow_qos": tokens.get("AllowQos"),
            "qos": tokens.get("QoS"),
            "max_time_seconds": parse_slurm_time_to_seconds(tokens.get("MaxTime")),
            "default_time_seconds": parse_slurm_time_to_seconds(tokens.get("DefaultTime")),
            "sinfo_rows": [],
        }
        by_partition[name] = part

    for row in sinfo_rows:
        part = by_partition.setdefault(
            row["partition"],
            {
                "name": row["partition"],
                "display_name": row["partition_display"],
                "is_default": row["partition_display"].endswith("*"),
                "max_time_raw": row["time_limit"],
                "default_time_raw": None,
                "nodes_raw": row["nodes"],
                "state_raw": row["state"],
                "allow_qos": None,
                "qos": None,
                "max_time_seconds": parse_slurm_time_to_seconds(row["time_limit"]),
                "default_time_seconds": None,
                "sinfo_rows": [],
            },
        )
        part["sinfo_rows"].append(row)

    partitions = []
    default_partition = None
    for name in sorted(by_partition):
        part = by_partition[name]
        row_states = [row["state"] for row in part["sinfo_rows"]]
        availability_values = {row["availability"] for row in part["sinfo_rows"] if row.get("availability")}
        part["availability"] = next(iter(availability_values), "unknown")
        part["row_states"] = row_states
        part["usable"] = part["availability"] == "up" and _partition_state_is_usable(row_states)
        if part["is_default"]:
            default_partition = part["name"]
        partitions.append(part)

    if default_partition is None and partitions:
        for part in partitions:
            if part["display_name"].endswith("*"):
                default_partition = part["name"]
                part["is_default"] = True
                break

    return {
        "available": True,
        "detected_at_epoch": time.time(),
        "default_partition": default_partition,
        "partitions": partitions,
        "source_commands": [
            ["scontrol", "show", "partition", "-o"],
            ["sinfo", "-h", "-o", "%P|%a|%l|%D|%t|%N"],
        ],
    }


def _select_partition(cluster: dict, requested_partition: str | None, requested_seconds: int | None):
    usable = [part for part in cluster.get("partitions", []) if part.get("usable")]
    if not usable:
        raise RuntimeError("No usable Slurm partition was detected on this machine.")

    notes = []
    by_name = {part["name"]: part for part in usable}

    if requested_partition:
        requested_name = _normalize_partition_name(requested_partition)
        requested = by_name.get(requested_name)
        if requested is not None:
            max_seconds = requested.get("max_time_seconds")
            if requested_seconds is None or max_seconds is None or max_seconds >= requested_seconds:
                notes.append(f"requested partition '{requested_partition}' is usable")
                return requested, notes
            notes.append(
                f"requested partition '{requested_partition}' max_time={requested.get('max_time_raw')} is shorter than requested walltime"
            )
        else:
            notes.append(f"requested partition '{requested_partition}' is unavailable on this machine")

    default_name = cluster.get("default_partition")
    default_partition = by_name.get(default_name) if default_name else None
    if default_partition is not None:
        max_seconds = default_partition.get("max_time_seconds")
        if requested_seconds is None or max_seconds is None or max_seconds >= requested_seconds:
            notes.append(f"falling back to default partition '{default_partition['name']}'")
            return default_partition, notes

    fitting = []
    for part in usable:
        max_seconds = part.get("max_time_seconds")
        if requested_seconds is None or max_seconds is None or max_seconds >= requested_seconds:
            fitting.append(part)
    candidates = fitting if fitting else usable
    candidates.sort(
        key=lambda part: (
            0 if part["name"] == default_name else 1,
            0 if any(state.lower().startswith("idle") for state in part.get("row_states", [])) else 1,
            0 if part.get("max_time_seconds") is None else 1,
            -(part.get("max_time_seconds") or 0),
            part["name"],
        )
    )
    chosen = candidates[0]
    if fitting:
        notes.append(f"falling back to usable partition '{chosen['name']}'")
    else:
        notes.append(
            f"no partition can satisfy requested walltime; falling back to '{chosen['name']}' with max_time={chosen.get('max_time_raw')}"
        )
    return chosen, notes


def _resolve_walltime(chosen_partition: dict, requested_walltime: str | None):
    notes = []
    if requested_walltime:
        requested_seconds = parse_slurm_time_to_seconds(requested_walltime)
        max_seconds = chosen_partition.get("max_time_seconds")
        if requested_seconds is None:
            return requested_walltime, notes
        if max_seconds is None or max_seconds >= requested_seconds:
            return requested_walltime, notes
        notes.append(
            f"requested walltime {requested_walltime} exceeds partition max_time {chosen_partition.get('max_time_raw')}; clamping to partition limit"
        )
        return chosen_partition.get("max_time_raw") or format_slurm_time(max_seconds), notes

    default_time = chosen_partition.get("default_time_raw")
    if default_time and default_time.lower() not in INFINITE_TIME_TOKENS:
        notes.append(f"using partition default_time {default_time}")
        return default_time, notes
    max_time = chosen_partition.get("max_time_raw")
    if max_time and max_time.lower() not in INFINITE_TIME_TOKENS:
        notes.append(f"using partition max_time {max_time}")
        return max_time, notes
    notes.append("using generic fallback walltime 24:00:00")
    return "24:00:00", notes


def resolve_slurm_job_settings(
    job_kind: str,
    requested_partition: str | None = None,
    requested_walltime: str | None = None,
    requested_qos: str | None = None,
):
    if not slurm_available():
        raise RuntimeError("Cannot resolve Slurm job settings because sbatch/squeue are unavailable.")

    env_prefix = JOB_ENV_PREFIX.get(job_kind, job_kind.upper())
    env_partition = os.environ.get(f"QIYAN_SLURM_{env_prefix}_PARTITION")
    env_walltime = os.environ.get(f"QIYAN_SLURM_{env_prefix}_WALLTIME")
    env_qos = os.environ.get(f"QIYAN_SLURM_{env_prefix}_QOS")

    effective_partition = env_partition or requested_partition
    effective_walltime = env_walltime or requested_walltime
    effective_qos = env_qos if env_qos is not None else requested_qos

    cluster = probe_slurm_cluster()
    requested_seconds = parse_slurm_time_to_seconds(effective_walltime)
    chosen_partition, partition_notes = _select_partition(cluster, effective_partition, requested_seconds)
    walltime, walltime_notes = _resolve_walltime(chosen_partition, effective_walltime)

    notes = []
    if env_partition:
        notes.append(f"partition overridden by env QIYAN_SLURM_{env_prefix}_PARTITION={env_partition}")
    if env_walltime:
        notes.append(f"walltime overridden by env QIYAN_SLURM_{env_prefix}_WALLTIME={env_walltime}")
    if env_qos:
        notes.append(f"qos overridden by env QIYAN_SLURM_{env_prefix}_QOS={env_qos}")
    notes.extend(partition_notes)
    notes.extend(walltime_notes)

    return {
        "job_kind": job_kind,
        "partition": chosen_partition["name"],
        "walltime": walltime,
        "qos": effective_qos,
        "requested_partition": requested_partition,
        "requested_walltime": requested_walltime,
        "requested_qos": requested_qos,
        "cluster_default_partition": cluster.get("default_partition"),
        "notes": notes,
        "cluster": cluster,
    }


def render_slurm_export_script(job_settings_by_kind: dict[str, dict]):
    lines = [
        "#!/usr/bin/env bash",
        "# Generated by assess_chgnet_env.py",
        "# Source this file to pin launcher/runtime Slurm defaults for this machine.",
        "set -euo pipefail",
        "",
    ]
    for job_kind, payload in job_settings_by_kind.items():
        env_prefix = JOB_ENV_PREFIX.get(job_kind, job_kind.upper())
        lines.append(f"# {job_kind}")
        lines.append(f"export QIYAN_SLURM_{env_prefix}_PARTITION={json.dumps(payload['partition'])}")
        lines.append(f"export QIYAN_SLURM_{env_prefix}_WALLTIME={json.dumps(payload['walltime'])}")
        if payload.get("qos"):
            lines.append(f"export QIYAN_SLURM_{env_prefix}_QOS={json.dumps(payload['qos'])}")
        lines.append("")
    return "\n".join(lines) + "\n"
