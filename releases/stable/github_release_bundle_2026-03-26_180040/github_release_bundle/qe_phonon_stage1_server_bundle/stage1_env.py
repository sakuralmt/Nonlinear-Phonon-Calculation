#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import time

from common import dump_json, ensure_dir, resolve_frontend_slurm_settings, slurm_available
from config import (
    MATDYN_MPI_TASKS,
    MATDYN_NODES,
    MODULE_LINES,
    PH_MPI_TASKS,
    PH_NODES,
    PHONON_PARTITION,
    PHONON_QOS,
    PHONON_TIME,
    PW_MPI_TASKS,
    PW_NODES,
    Q2R_MPI_TASKS,
    Q2R_NODES,
    RESULTS_ROOT,
)


STAGE1_ENV_ASSESSMENT_JSON = RESULTS_ROOT / "stage1_env_assessment.json"
STAGE1_ENV_ASSESSMENT_MD = RESULTS_ROOT / "stage1_env_assessment.md"
STAGE1_RUNTIME_CONFIG_JSON = RESULTS_ROOT / "stage1_runtime_config.json"

GLOBAL_STAGE_ENV_PREFIX = "QIYAN_STAGE1"
STAGE_ENV_PREFIX = {
    "pw": "QIYAN_STAGE1_PW",
    "ph": "QIYAN_STAGE1_PH",
    "q2r": "QIYAN_STAGE1_Q2R",
    "matdyn": "QIYAN_STAGE1_MATDYN",
}


def _run_capture(cmd: list[str], timeout: int = 20) -> dict:
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=timeout)
        return {
            "cmd": cmd,
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
    except Exception as exc:
        return {"cmd": cmd, "error": repr(exc)}


def _run_shell_capture(script: str, timeout: int = 20, cwd: str | None = None) -> dict:
    try:
        result = subprocess.run(
            ["bash", "-lc", script],
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
            cwd=cwd,
        )
        return {
            "cmd": ["bash", "-lc", script],
            "returncode": result.returncode,
            "stdout": result.stdout.strip(),
            "stderr": result.stderr.strip(),
        }
    except Exception as exc:
        return {"cmd": ["bash", "-lc", script], "error": repr(exc)}


def _text_snippet(text: str, limit: int = 200) -> str:
    compact = " ".join(str(text).split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


def _env_int(*names: str) -> int | None:
    value = _env_first(*names)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer in environment for {names[0]}: {value}") from exc


def _probe_executable(name: str, version_commands: list[list[str]]) -> dict:
    path = shutil.which(name)
    payload = {"available": path is not None, "path": path, "probe": None}
    if path is None:
        return payload
    for args in version_commands:
        probe = _run_capture([name, *args])
        if probe.get("stdout") or probe.get("stderr") or probe.get("returncode") == 0:
            payload["probe"] = {
                "args": args,
                "returncode": probe.get("returncode"),
                "stdout_snippet": _text_snippet(probe.get("stdout", "")),
                "stderr_snippet": _text_snippet(probe.get("stderr", "")),
            }
            break
    return payload


def _build_qe_probe_script(binary: str, args: list[str]) -> str:
    quoted_args = " ".join(subprocess.list2cmdline([arg]) for arg in args)
    lines = ["set -e"]
    lines.extend(MODULE_LINES)
    if quoted_args:
        lines.append(f"{binary} {quoted_args}")
    else:
        lines.append(binary)
    return "\n".join(lines)


def _probe_qe_executable(name: str, version_commands: list[list[str]]) -> dict:
    path = shutil.which(name)
    if path is None:
        env_path = _env_first("PATH")
        if env_path and name not in env_path:
            shell_probe = _run_shell_capture("\n".join(MODULE_LINES + [f"command -v {name} || true"]))
            detected = shell_probe.get("stdout", "").strip().splitlines()
            path = detected[-1].strip() if detected else None
    payload = {"available": path is not None, "path": path, "probe": None}
    if path is None:
        return payload
    for args in version_commands:
        with tempfile.TemporaryDirectory(prefix=f"qe_stage1_probe_{name.replace('.', '_')}_") as tmpdir:
            probe = _run_shell_capture(_build_qe_probe_script(name, args), cwd=tmpdir)
        if probe.get("stdout") or probe.get("stderr") or probe.get("returncode") == 0:
            payload["probe"] = {
                "args": args,
                "returncode": probe.get("returncode"),
                "stdout_snippet": _text_snippet(probe.get("stdout", "")),
                "stderr_snippet": _text_snippet(probe.get("stderr", "")),
            }
            break
    return payload


def _detect_launcher(commands: dict[str, dict]) -> dict:
    notes: list[str] = []
    override = _env_first(f"{GLOBAL_STAGE_ENV_PREFIX}_MPI_LAUNCHER")
    order = []
    if override is not None:
        order.append(str(override).lower())
        notes.append(f"launcher overridden by env {GLOBAL_STAGE_ENV_PREFIX}_MPI_LAUNCHER={override}")
    order.extend(["mpirun", "srun", "mpiexec"])

    seen = set()
    for candidate in order:
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate == "mpirun" and commands["mpirun"]["available"]:
            return {
                "kind": "mpirun",
                "supports_parallel": True,
                "path": commands["mpirun"]["path"],
                "template": "mpirun -np {ntasks} {binary}",
                "notes": notes,
            }
        if candidate == "srun" and commands["srun"]["available"]:
            return {
                "kind": "srun",
                "supports_parallel": True,
                "path": commands["srun"]["path"],
                "template": "srun -n {ntasks} {binary}",
                "notes": notes,
            }
        if candidate == "mpiexec" and commands["mpiexec"]["available"]:
            return {
                "kind": "mpiexec",
                "supports_parallel": True,
                "path": commands["mpiexec"]["path"],
                "template": "mpiexec -n {ntasks} {binary}",
                "notes": notes,
            }

    notes.append("no MPI launcher detected; forcing single-rank direct execution as a safety fallback")
    return {
        "kind": "direct",
        "supports_parallel": False,
        "path": None,
        "template": "{binary}",
        "notes": notes,
    }


def _build_stage_specs() -> dict[str, dict]:
    return {
        "pw": {
            "label": "pw.x primitive SCF",
            "binary": "pw.x",
            "default_nodes": PW_NODES,
            "default_tasks_per_node": PW_MPI_TASKS,
            "task_cap": max(1, int(PW_MPI_TASKS)),
            "allow_multi_node": True,
        },
        "ph": {
            "label": "ph.x DFPT phonon",
            "binary": "ph.x",
            "default_nodes": PH_NODES,
            "default_tasks_per_node": PH_MPI_TASKS,
            "task_cap": max(1, int(PH_MPI_TASKS)),
            "allow_multi_node": True,
        },
        "q2r": {
            "label": "q2r.x IFC transform",
            "binary": "q2r.x",
            "default_nodes": Q2R_NODES,
            "default_tasks_per_node": Q2R_MPI_TASKS,
            "task_cap": 1,
            "allow_multi_node": False,
        },
        "matdyn": {
            "label": "matdyn.x eig/freq extraction",
            "binary": "matdyn.x",
            "default_nodes": MATDYN_NODES,
            "default_tasks_per_node": MATDYN_MPI_TASKS,
            "task_cap": max(1, int(MATDYN_MPI_TASKS)),
            "allow_multi_node": True,
        },
    }


def _fallback_stage_settings(
    requested_partition: str,
    requested_walltime: str,
    requested_qos: str | None,
    requested_nodes: int,
    requested_ntasks_per_node: int,
    reason: str,
) -> dict:
    cpu_count = max(1, int(os.cpu_count() or 1))
    nodes = 1
    ntasks_per_node = max(1, min(int(requested_ntasks_per_node), cpu_count))
    return {
        "partition": requested_partition,
        "walltime": requested_walltime,
        "qos": requested_qos,
        "nodes": nodes,
        "ntasks_per_node": ntasks_per_node,
        "total_tasks": nodes * ntasks_per_node,
        "notes": [
            reason,
            f"fell back to local cpu_count={cpu_count}",
        ],
    }


def _resolve_stage_settings(
    requested_partition: str,
    requested_walltime: str,
    requested_qos: str | None,
    requested_nodes: int,
    requested_ntasks_per_node: int,
) -> dict:
    if not slurm_available():
        return _fallback_stage_settings(
            requested_partition,
            requested_walltime,
            requested_qos,
            requested_nodes,
            requested_ntasks_per_node,
            "Slurm is unavailable on this machine",
        )
    try:
        return resolve_frontend_slurm_settings(
            requested_partition=requested_partition,
            requested_walltime=requested_walltime,
            requested_qos=requested_qos,
            requested_nodes=requested_nodes,
            requested_ntasks_per_node=requested_ntasks_per_node,
        )
    except Exception as exc:
        return _fallback_stage_settings(
            requested_partition,
            requested_walltime,
            requested_qos,
            requested_nodes,
            requested_ntasks_per_node,
            f"Slurm probe failed: {exc}",
        )


def _build_command(launcher: dict, binary: str, total_tasks: int) -> str:
    if launcher["kind"] == "direct":
        return binary
    return launcher["template"].format(ntasks=int(total_tasks), binary=binary)


def _resolve_stage_runtime(stage_name: str, spec: dict, launcher: dict) -> dict:
    notes: list[str] = []
    stage_env = STAGE_ENV_PREFIX[stage_name]

    requested_partition = _env_first(
        f"{stage_env}_PARTITION",
        f"{GLOBAL_STAGE_ENV_PREFIX}_PARTITION",
    ) or PHONON_PARTITION
    requested_walltime = _env_first(
        f"{stage_env}_WALLTIME",
        f"{GLOBAL_STAGE_ENV_PREFIX}_WALLTIME",
    ) or PHONON_TIME
    requested_qos = _env_first(
        f"{stage_env}_QOS",
        f"{GLOBAL_STAGE_ENV_PREFIX}_QOS",
    )
    if requested_qos is None:
        requested_qos = PHONON_QOS

    requested_nodes = _env_int(
        f"{stage_env}_NODES",
        f"{GLOBAL_STAGE_ENV_PREFIX}_{stage_name.upper()}_NODES",
    )
    if requested_nodes is None:
        requested_nodes = int(spec["default_nodes"])

    requested_tasks = _env_int(
        f"{stage_env}_NTASKS_PER_NODE",
        f"{stage_env}_TASKS",
        f"{GLOBAL_STAGE_ENV_PREFIX}_{stage_name.upper()}_NTASKS_PER_NODE",
        f"{GLOBAL_STAGE_ENV_PREFIX}_{stage_name.upper()}_TASKS",
    )
    if requested_tasks is None:
        requested_tasks = int(spec["default_tasks_per_node"])

    if not spec.get("allow_multi_node", True) and requested_nodes != 1:
        notes.append(f"forcing single-node mode for stage '{stage_name}'")
        requested_nodes = 1

    task_cap = int(spec.get("task_cap", requested_tasks))
    if requested_tasks > task_cap:
        notes.append(f"capping ntasks-per-node from {requested_tasks} to {task_cap} for stage '{stage_name}'")
        requested_tasks = task_cap

    if not launcher["supports_parallel"]:
        if requested_nodes != 1 or requested_tasks != 1:
            notes.append("parallel launcher unavailable; forcing single-rank direct execution")
        requested_nodes = 1
        requested_tasks = 1

    slurm_settings = _resolve_stage_settings(
        requested_partition=requested_partition,
        requested_walltime=requested_walltime,
        requested_qos=requested_qos,
        requested_nodes=requested_nodes,
        requested_ntasks_per_node=requested_tasks,
    )
    stage_notes = list(notes)
    stage_notes.extend(slurm_settings.get("notes", []))

    if not launcher["supports_parallel"] and slurm_settings["total_tasks"] != 1:
        slurm_settings["nodes"] = 1
        slurm_settings["ntasks_per_node"] = 1
        slurm_settings["total_tasks"] = 1

    return {
        "label": spec["label"],
        "binary": spec["binary"],
        "requested": {
            "partition": requested_partition,
            "walltime": requested_walltime,
            "qos": requested_qos,
            "nodes": requested_nodes,
            "ntasks_per_node": requested_tasks,
        },
        "slurm_settings": slurm_settings,
        "command": _build_command(launcher, spec["binary"], int(slurm_settings["total_tasks"])),
        "notes": stage_notes,
    }


def build_runtime_signature(assessment: dict) -> dict:
    return {
        "launcher": {
            "kind": assessment["launcher"]["kind"],
            "template": assessment["launcher"]["template"],
            "supports_parallel": assessment["launcher"]["supports_parallel"],
        },
        "stages": {
            stage_name: {
                "command": payload["command"],
                "slurm_settings": payload["slurm_settings"],
            }
            for stage_name, payload in assessment["stages"].items()
        },
    }


def build_runtime_config(assessment: dict) -> dict:
    return {
        "kind": "qe_stage1_runtime_config",
        "generated_at_epoch": assessment["generated_at_epoch"],
        "launcher": assessment["launcher"],
        "stages": assessment["stages"],
    }


def build_markdown(assessment: dict) -> str:
    lines = []
    lines.append("# Stage1 QE Runtime Assessment")
    lines.append("")
    lines.append(f"- generated at epoch: `{assessment['generated_at_epoch']:.0f}`")
    lines.append(f"- Slurm available: `{assessment['slurm_available']}`")
    lines.append(f"- MPI launcher: `{assessment['launcher']['kind']}`")
    for note in assessment["launcher"].get("notes", []):
        lines.append(f"- launcher note: `{note}`")
    lines.append("")
    lines.append("## Commands")
    for name, payload in assessment["commands"].items():
        lines.append(f"- `{name}`: available=`{payload['available']}`, path=`{payload.get('path')}`")
        probe = payload.get("probe")
        if probe and (probe.get("stdout_snippet") or probe.get("stderr_snippet")):
            snippet = probe.get("stdout_snippet") or probe.get("stderr_snippet")
            lines.append(f"  - probe `{probe.get('args')}` => `{snippet}`")
    lines.append("")
    lines.append("## Stage Settings")
    for stage_name, payload in assessment["stages"].items():
        slurm_settings = payload["slurm_settings"]
        lines.append(
            f"- `{stage_name}`: command=`{payload['command']}`, partition=`{slurm_settings['partition']}`, "
            f"nodes=`{slurm_settings['nodes']}`, ntasks-per-node=`{slurm_settings['ntasks_per_node']}`, "
            f"total=`{slurm_settings['total_tasks']}`"
        )
        for note in payload.get("notes", []):
            lines.append(f"  - {note}")
    lines.append("")
    lines.append(f"- runtime config: `{STAGE1_RUNTIME_CONFIG_JSON}`")
    return "\n".join(lines) + "\n"


def assess_stage1_environment(force_refresh: bool = False) -> dict:
    ensure_dir(RESULTS_ROOT)
    if not force_refresh and STAGE1_ENV_ASSESSMENT_JSON.exists():
        return json.loads(STAGE1_ENV_ASSESSMENT_JSON.read_text())

    commands = {
        "pw.x": _probe_qe_executable("pw.x", [["-h"]]),
        "ph.x": _probe_qe_executable("ph.x", [["-h"]]),
        "q2r.x": _probe_qe_executable("q2r.x", [["-h"]]),
        "matdyn.x": _probe_qe_executable("matdyn.x", [["-h"]]),
        "mpirun": _probe_qe_executable("mpirun", [["--version"], ["-V"]]),
        "mpiexec": _probe_qe_executable("mpiexec", [["--version"], ["-V"]]),
        "srun": _probe_executable("srun", [["--version"]]),
        "sbatch": _probe_executable("sbatch", [["--version"]]),
        "squeue": _probe_executable("squeue", [["--version"]]),
        "scontrol": _probe_executable("scontrol", [["--version"]]),
        "sinfo": _probe_executable("sinfo", [["--version"]]),
    }

    launcher = _detect_launcher(commands)
    specs = _build_stage_specs()
    stages = {
        stage_name: _resolve_stage_runtime(stage_name, spec, launcher)
        for stage_name, spec in specs.items()
    }

    assessment = {
        "kind": "qe_stage1_env_assessment",
        "generated_at_epoch": time.time(),
        "slurm_available": slurm_available(),
        "launcher": launcher,
        "commands": commands,
        "stages": stages,
        "runtime_signature": build_runtime_signature({"launcher": launcher, "stages": stages}),
    }

    dump_json(STAGE1_ENV_ASSESSMENT_JSON, assessment)
    dump_json(STAGE1_RUNTIME_CONFIG_JSON, build_runtime_config(assessment))
    STAGE1_ENV_ASSESSMENT_MD.write_text(build_markdown(assessment))
    return assessment


def ensure_stage1_environment_assessed(force_refresh: bool = False) -> dict:
    return assess_stage1_environment(force_refresh=force_refresh)
