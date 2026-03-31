#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import select
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent
LOG_FILE_NAME = "launcher.log"
DEFAULT_QE_RELAX = True
DEFAULT_STAGE = "all"
VALID_STAGES = ("all", "tune", "stage1", "stage2", "stage3")
DRIVER_HEARTBEAT_SECONDS = 60
STAGE_LABELS = {
    "all": "Full workflow",
    "tune": "Convergence tuning",
    "stage1": "Stage 1",
    "stage2": "Stage 2",
    "stage3": "Stage 3",
}
STAGE_DESCRIPTIONS = {
    "all": "Read one system directory and run stage1 -> stage2 -> stage3.",
    "tune": "Run family-aware QE convergence tuning for the selected system and store reusable stage1 profiles.",
    "stage1": "Read CIF and pseudopotentials from a system directory and generate the phonon handoff.",
    "stage2": "Continue from an existing run root and execute MLFF screening.",
    "stage3": "Continue from an existing run root and prepare or submit QE top5 recheck jobs.",
}


if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from nonlinear_phonon_calculation.system_inputs import (
    DEFAULT_INPUT_ROOT,
    build_run_tag,
    default_runs_root,
    latest_run_root,
    list_system_ids,
)
from server_highthroughput_workflow.handoff_bundle import export_handoff_bundle, import_handoff_bundle
from server_highthroughput_workflow.stage_contracts import manifest_path

MODULAR_RUNNER = ROOT / "server_highthroughput_workflow" / "run_modular_pipeline.py"


def parse_args():
    parser = argparse.ArgumentParser(
        prog="npc",
        description="Interactive TUI launcher for the staged nonlinear phonon workflow.",
    )
    parser.add_argument("--input-root", type=str, default=str(DEFAULT_INPUT_ROOT))
    parser.add_argument("--system", type=str, default=None)
    parser.add_argument("--stage", choices=VALID_STAGES, default=None)
    parser.add_argument("--run-root", type=str, default=None)
    parser.add_argument("--qe-relax", choices=["yes", "no"], default=None)
    parser.add_argument("--qe-mode", choices=["prepare_only", "submit_collect"], default="submit_collect")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--handoff-export", choices=["stage1", "stage2"], default=None)
    parser.add_argument("--handoff-import", action="store_true")
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--bundle", type=str, default=None)
    return parser.parse_args()


def derive_system_id_from_run_root(run_root: Path) -> str:
    run_root = Path(run_root).expanduser().resolve()
    return run_root.parent.name


def _append_log(log_path: Path, line: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as handle:
        handle.write(line)


def log_line(log_path: Path, message: str) -> None:
    print(message, flush=True)
    _append_log(log_path, f"{message}\n")


def log_section(log_path: Path, title: str) -> None:
    log_line(log_path, "")
    log_line(log_path, f"[{title}]")


def load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text())


def emit_stdout(_: Path | None, message: str) -> None:
    print(message, flush=True)


def prompt_yes_no(question: str, default: bool) -> bool:
    suffix = "[Y/n]" if default else "[y/N]"
    while True:
        answer = input(f"{question} {suffix}: ").strip().lower()
        if not answer:
            return default
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer yes or no.", flush=True)


def prompt_text(question: str, default: str | None = None) -> str:
    suffix = "" if default is None else f" (default: {default})"
    while True:
        answer = input(f"{question}{suffix}: ").strip()
        if answer:
            return answer
        if default is not None:
            return default


def prompt_stage(default: str) -> str:
    options = "/".join(VALID_STAGES)
    while True:
        answer = input(f"Which stage to run? [{options}] (default: {default}): ").strip().lower()
        if not answer:
            return default
        if answer in VALID_STAGES:
            return answer
        print(f"Please choose one of: {options}.", flush=True)


def prompt_system(input_root: Path, default: str | None = None) -> str:
    systems = list_system_ids(input_root)
    if systems:
        print("Available systems:", flush=True)
        for system_id in systems:
            print(f"- {system_id}", flush=True)
    return prompt_text("Choose a system", default=default or (systems[0] if systems else None))


def show_stage_choices(log_path: Path) -> None:
    log_section(log_path, "Available stages")
    for stage in VALID_STAGES:
        log_line(log_path, f"- {STAGE_LABELS[stage]} ({stage}): {STAGE_DESCRIPTIONS[stage]}")


def choose_run_root(input_root: Path, system_id: str, run_root: str | None, stage: str) -> Path:
    if run_root:
        return Path(run_root).expanduser().resolve()
    runs_root = default_runs_root(input_root)
    if stage in {"stage2", "stage3"}:
        existing = latest_run_root(runs_root, system_id)
        if existing is not None:
            return existing
    return runs_root / system_id / build_run_tag(system_id)


def latest_run_root_any_system(input_root: Path) -> Path | None:
    runs_root = default_runs_root(input_root)
    latest: Path | None = None
    latest_mtime = -1.0
    for system_id in list_system_ids(input_root):
        candidate = latest_run_root(runs_root, system_id)
        if candidate is None or not candidate.exists():
            continue
        mtime = candidate.stat().st_mtime
        if mtime > latest_mtime:
            latest = candidate
            latest_mtime = mtime
    return latest


def stage_manifest_path(run_root: Path, stage: str) -> Path:
    if stage == "stage1":
        return manifest_path(run_root, "stage1_manifest")
    if stage == "stage2":
        return manifest_path(run_root, "stage2_manifest")
    if stage == "stage3":
        return manifest_path(run_root, "stage3_manifest")
    raise ValueError(f"Unsupported stage name: {stage}")


def ensure_stage_prerequisites(run_root: Path, stage: str, log_path: Path) -> None:
    required = None
    hint = None
    if stage == "stage2":
        required = stage_manifest_path(run_root, "stage1")
        hint = "Run npc and choose stage1 first to create the missing stage1 contract."
    elif stage == "stage3":
        required = stage_manifest_path(run_root, "stage2")
        hint = "Run npc and choose stage2 first to create the missing stage2 contract."
    if required is None:
        return
    if not required.exists():
        message = f"Missing required file for {stage}: {required}"
        if hint:
            message = f"{message} {hint}"
        raise RuntimeError(message)
    log_line(log_path, f"Found prerequisite for {stage}: {required}")


def build_modular_command(
    stage: str,
    run_root: Path,
    input_root: Path,
    system_id: str,
    qe_relax: bool,
    qe_mode: str,
) -> list[str]:
    command = [
        sys.executable,
        "-u",
        str(MODULAR_RUNNER),
        "--stage",
        stage,
        "--run-root",
        str(run_root),
        "--input-root",
        str(input_root),
        "--system",
        system_id,
        "--qe-relax",
        "yes" if qe_relax else "no",
    ]
    if stage in {"stage3", "all"}:
        command.extend(["--qe-mode", qe_mode])
    return command


def _is_low_value_output_line(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    noisy_prefixes = (
        "bash: warning: setlocale:",
        "/bin/sh: warning: setlocale:",
        "sh: warning: setlocale:",
        "Loading mkl version",
        "Loading intel_ipp_intel64 version",
        "Loading compiler-rt version",
        "Loading mpi version",
        "Loading compiler version",
        "For a full Intel(R) Integrated Performance Primitives functionality",
    )
    return stripped.startswith(noisy_prefixes)


def _format_elapsed(seconds: float) -> str:
    total = max(0, int(seconds))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def run_streaming_command(command: list[str], cwd: Path, log_path: Path, label: str) -> None:
    rendered = subprocess.list2cmdline(command)
    _append_log(log_path, f"$ {rendered}\n")
    log_line(log_path, f"Launching {label} driver...")

    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )

    assert process.stdout is not None
    last_visible_output = time.monotonic()
    start_time = last_visible_output
    while True:
        ready, _, _ = select.select([process.stdout], [], [], 1.0)
        if ready:
            line = process.stdout.readline()
            if line == "":
                if process.poll() is not None:
                    break
                continue
            _append_log(log_path, line)
            if _is_low_value_output_line(line):
                continue
            print(line, end="", flush=True)
            last_visible_output = time.monotonic()
            continue

        if process.poll() is not None:
            break

        now = time.monotonic()
        if now - last_visible_output >= DRIVER_HEARTBEAT_SECONDS:
            log_line(log_path, f"{label} driver still running ({_format_elapsed(now - start_time)} elapsed).")
            last_visible_output = now

    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)
    log_line(log_path, f"{label} driver finished.")


def print_artifact_summary(run_root: Path, log_path: Path) -> None:
    log_line(log_path, "")
    log_line(log_path, "Artifacts:")
    log_line(log_path, f"- Run root: {run_root}")
    for stage in ("stage1", "stage2", "stage3"):
        manifest = stage_manifest_path(run_root, stage)
        if manifest.exists():
            log_line(log_path, f"- {stage} contract: {manifest}")

def _preview_codes(rows: list[dict], key: str, limit: int = 3) -> str:
    preview = [str(row.get(key)) for row in rows[:limit] if row.get(key)]
    return ", ".join(preview) if preview else "(none)"


def _format_float(value) -> str:
    try:
        return f"{float(value):.3f}"
    except Exception:
        return str(value)


def _print_top_rows(emit, log_path: Path | None, title: str, rows: list[dict], formatter, limit: int = 3) -> None:
    if not rows:
        return
    emit(log_path, f"- {title}:")
    for index, row in enumerate(rows[:limit], start=1):
        emit(log_path, f"  {index}. {formatter(row)}")


def _summarize_stage2_row(row: dict) -> str:
    parts = [str(row.get("pair_code", "(unknown)"))]
    if row.get("point_label"):
        parts.append(f"point={row['point_label']}")
    phi_value = row.get("phi122_mev", row.get("phi122_mean_mev"))
    if phi_value is not None:
        parts.append(f"|phi122|={abs(float(phi_value)):.3f} meV")
    if row.get("gamma_freq_abs_err_thz") is not None:
        parts.append(f"gamma_err={_format_float(row['gamma_freq_abs_err_thz'])} THz")
    if row.get("target_freq_abs_err_thz") is not None:
        parts.append(f"target_err={_format_float(row['target_freq_abs_err_thz'])} THz")
    if row.get("rmse_ev_supercell") is not None:
        parts.append(f"rmse={_format_float(row['rmse_ev_supercell'])} eV")
    return ", ".join(parts)


def _summarize_stage3_row(row: dict) -> str:
    parts = [str(row.get("pair_code", "(unknown)"))]
    for key, label in (
        ("qe_phi122_mev", "qe_phi122"),
        ("qe_r2", "qe_r2"),
        ("qe_gamma_axis_freq_thz", "qe_gamma_thz"),
        ("qe_target_axis_freq_thz", "qe_target_thz"),
        ("phi122_mev", "screen_phi122"),
        ("rmse_ev_supercell", "rmse_eV"),
    ):
        if row.get(key) is not None:
            parts.append(f"{label}={_format_float(row[key])}")
    return ", ".join(parts)


def _format_final_qe_state(state: str | None) -> str:
    if state == "all_completed":
        return "All prepared QE jobs completed and were collected."
    if state == "stalled_incomplete":
        return "QE submission stopped with unfinished jobs still missing outputs."
    if state:
        return state
    return "unknown"


def _stage3_job_state_counts(qe_run_root: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for path in qe_run_root.glob("*/*/job_status.json"):
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        state = str(payload.get("state", "unknown"))
        counts[state] = counts.get(state, 0) + 1
    return counts


def _format_job_state_counts(counts: dict[str, int]) -> str:
    ordered_keys = ("completed", "running", "submitted", "failed", "exhausted")
    parts = [f"{key}={counts[key]}" for key in ordered_keys if counts.get(key)]
    extras = [f"{key}={value}" for key, value in sorted(counts.items()) if key not in ordered_keys and value]
    rendered = parts + extras
    return ", ".join(rendered) if rendered else "(none)"


def summarize_stage1(run_root: Path, log_path: Path | None, emit=log_line) -> None:
    manifest = load_json(stage_manifest_path(run_root, "stage1"))
    if not manifest:
        return
    if emit is log_line:
        log_section(log_path, "Stage 1 summary")
    else:
        emit(log_path, "\n[Stage 1 summary]")
    emit(log_path, "Stage 1 prepared the handoff files for downstream stages.")
    files = manifest.get("files", {})
    if files.get("mode_pairs_json"):
        emit(log_path, f"- Mode pairs: {run_root / files['mode_pairs_json']}")
    if files.get("structure"):
        emit(log_path, f"- Structure snapshot: {run_root / files['structure']}")
    emit(log_path, f"- Pseudopotentials copied: {len(manifest.get('pseudo_files', []))}")
    emit(log_path, "- Next recommended action: run npc --stage stage2 on this run root.")


def summarize_stage2(run_root: Path, log_path: Path | None, emit=log_line) -> None:
    manifest = load_json(stage_manifest_path(run_root, "stage2"))
    if not manifest:
        return
    if emit is log_line:
        log_section(log_path, "Stage 2 summary")
    else:
        emit(log_path, "\n[Stage 2 summary]")
    emit(log_path, "Stage 2 completed MLFF screening and produced QE follow-up ranking artifacts.")
    output_files = manifest.get("output_files", {})
    runtime_files = manifest.get("runtime_files", {})
    ranking_json_path = run_root / output_files["ranking_json"] if output_files.get("ranking_json") else None
    ranking_csv_path = run_root / output_files["ranking_csv"] if output_files.get("ranking_csv") else None
    pair_ranking_path = run_root / runtime_files["pair_ranking_json"] if runtime_files.get("pair_ranking_json") else None
    rows_payload = load_json(ranking_json_path) if ranking_json_path else None
    pair_payload = load_json(pair_ranking_path) if pair_ranking_path else None
    rows = [] if rows_payload is None else list(rows_payload.get("rows", []))
    pair_rows = [] if pair_payload is None else list(pair_payload.get("pairs", []))

    emit(log_path, f"- Ranked pairs: {len(rows) if rows else len(pair_rows)}")
    if rows:
        emit(log_path, f"- Top candidates: {_preview_codes(rows, 'pair_code')}")
        _print_top_rows(emit, log_path, "Top ranking details", rows, _summarize_stage2_row)
    elif pair_rows:
        emit(log_path, f"- Top candidates: {_preview_codes(pair_rows, 'pair_code')}")
        _print_top_rows(emit, log_path, "Top ranking details", pair_rows, _summarize_stage2_row)
    if ranking_csv_path and ranking_csv_path.exists():
        emit(log_path, f"- Ranking CSV: {ranking_csv_path}")
    if ranking_json_path and ranking_json_path.exists():
        emit(log_path, f"- Ranking JSON: {ranking_json_path}")
    emit(log_path, "- Next recommended action: run npc --stage stage3 on this run root.")


def summarize_stage3(run_root: Path, log_path: Path | None, emit=log_line) -> None:
    manifest = load_json(stage_manifest_path(run_root, "stage3"))
    if not manifest:
        return
    if emit is log_line:
        log_section(log_path, "Stage 3 summary")
    else:
        emit(log_path, "\n[Stage 3 summary]")
    qe_files = manifest.get("qe_files", {})
    qe_run_root = run_root / qe_files["qe_run_root"] if qe_files.get("qe_run_root") else None
    qe_run_manifest = load_json(qe_run_root / "run_manifest.json") if qe_run_root and (qe_run_root / "run_manifest.json").exists() else None
    submission_payload = load_json(qe_run_root / "submission_log.json") if qe_run_root and (qe_run_root / "submission_log.json").exists() else None
    status_payload = load_json(qe_run_root / "modular_stage3_status.json") if qe_run_root and (qe_run_root / "modular_stage3_status.json").exists() else None
    qe_ranking_path = run_root / qe_files["qe_ranking_json"] if qe_files.get("qe_ranking_json") else None
    if qe_ranking_path is None and status_payload and status_payload.get("qe_ranking_json"):
        qe_ranking_path = Path(status_payload["qe_ranking_json"])
    ranking_payload = load_json(qe_ranking_path) if qe_ranking_path and qe_ranking_path.exists() else None
    ranking_rows = [] if ranking_payload is None else list(ranking_payload.get("rows", []))
    job_state_counts = {} if qe_run_root is None else _stage3_job_state_counts(qe_run_root)

    if qe_run_root:
        emit(log_path, f"- QE run root: {qe_run_root}")
    if qe_run_manifest and qe_run_manifest.get("job_count") is not None:
        emit(log_path, f"- Prepared QE jobs: {qe_run_manifest['job_count']}")
    if submission_payload:
        completed = submission_payload.get("completed_count")
        total = submission_payload.get("total_jobs")
        active = submission_payload.get("active_count")
        if completed is not None and total is not None:
            emit(log_path, f"- Submission progress: {completed}/{total} completed, {active or 0} active")
    if job_state_counts:
        emit(log_path, f"- Job states: {_format_job_state_counts(job_state_counts)}")
    if status_payload and status_payload.get("final_state"):
        emit(log_path, f"- Final QE state: {_format_final_qe_state(status_payload['final_state'])}")
    if status_payload and status_payload.get("resume_mode"):
        emit(log_path, f"- Resume mode: {status_payload['resume_mode']}")
    if qe_ranking_path and qe_ranking_path.exists():
        emit(log_path, f"- QE ranking rows: {len(ranking_rows)}")
        if ranking_rows:
            emit(log_path, f"- Top QE candidates: {_preview_codes(ranking_rows, 'pair_code')}")
            _print_top_rows(emit, log_path, "Top QE result details", ranking_rows, _summarize_stage3_row)
        emit(log_path, f"- QE ranking JSON: {qe_ranking_path}")
    elif qe_run_manifest and not submission_payload:
        emit(log_path, "- QE jobs were prepared but not submitted yet.")
        emit(log_path, "- Next recommended action: rerun stage3 on a Slurm machine to continue submit/collect.")
    else:
        emit(log_path, "- QE jobs are in progress or partially collected. No final QE ranking is available yet.")
        if qe_run_root:
            emit(log_path, f"- Live job directory: {qe_run_root}")


def print_result_summary(stage: str, run_root: Path, log_path: Path) -> None:
    if stage in {"stage1", "all"}:
        summarize_stage1(run_root, log_path, emit=log_line)
    if stage in {"stage2", "all"}:
        summarize_stage2(run_root, log_path, emit=log_line)
    if stage in {"stage3", "all"}:
        summarize_stage3(run_root, log_path, emit=log_line)


def print_status_report(run_root: Path | None, run_hint: str | None = None) -> int:
    print("\n[Status]", flush=True)
    if run_root is None:
        if run_hint:
            print(run_hint, flush=True)
        else:
            print("No run root could be resolved yet.", flush=True)
        return 0
    print(f"Run root: {run_root}", flush=True)
    if not run_root.exists():
        print("No run root directory exists yet.", flush=True)
        return 0

    discovered = []
    for stage in ("stage1", "stage2", "stage3"):
        manifest = stage_manifest_path(run_root, stage)
        if manifest.exists():
            discovered.append(stage)
            print(f"- Found {stage} manifest: {manifest}", flush=True)
    if not discovered:
        print("No stage manifests found yet.", flush=True)
        return 0
    summarize_stage1(run_root, None, emit=emit_stdout)
    summarize_stage2(run_root, None, emit=emit_stdout)
    summarize_stage3(run_root, None, emit=emit_stdout)
    return 0


def main() -> int:
    args = parse_args()
    input_root = Path(args.input_root).expanduser().resolve()
    runs_root = default_runs_root(input_root)

    if args.handoff_import:
        if not args.bundle or not args.run_root:
            raise SystemExit("--handoff-import requires --bundle and --run-root.")
        imported = import_handoff_bundle(Path(args.bundle), Path(args.run_root))
        print("Imported handoff bundle.", flush=True)
        print(f"Run root: {Path(args.run_root).expanduser().resolve()}", flush=True)
        for stage_name, manifest in sorted(imported.items()):
            print(f"- {stage_name} manifest: {manifest}", flush=True)
        return 0

    if args.handoff_export:
        if not args.output or not args.run_root:
            raise SystemExit("--handoff-export requires --run-root and --output.")
        bundle_path = export_handoff_bundle(Path(args.run_root), args.handoff_export, Path(args.output))
        print(f"Exported {args.handoff_export} handoff bundle: {bundle_path}", flush=True)
        return 0

    if args.status:
        status_run_root: Path | None
        hint: str | None = None
        if args.run_root:
            status_run_root = Path(args.run_root).expanduser().resolve()
        elif args.system:
            status_run_root = latest_run_root(runs_root, args.system)
            if status_run_root is None:
                hint = f"No run root found yet for system '{args.system}'."
        else:
            status_run_root = latest_run_root_any_system(input_root)
            if status_run_root is None:
                hint = f"No run roots found under {runs_root}."
        return print_status_report(status_run_root, hint)

    stage = args.stage or prompt_stage(DEFAULT_STAGE)
    if stage in {"stage2", "stage3"} and args.run_root and not args.system:
        system_id = derive_system_id_from_run_root(Path(args.run_root))
    else:
        system_id = args.system or prompt_system(input_root)
    qe_relax = DEFAULT_QE_RELAX if args.qe_relax is None else args.qe_relax == "yes"
    if args.qe_relax is None and stage in {"stage1", "all"}:
        qe_relax = prompt_yes_no("Run QE structure relaxation first?", default=DEFAULT_QE_RELAX)
    run_root = choose_run_root(input_root, system_id, args.run_root, stage)
    run_root.mkdir(parents=True, exist_ok=True)
    log_path = run_root / "logs" / LOG_FILE_NAME
    _append_log(log_path, f"\n[{datetime.now().astimezone().isoformat(timespec='seconds')}] start_release.py\n")

    try:
        show_stage_choices(log_path)
        log_section(log_path, "Release launcher")
        log_line(log_path, "Launcher started.")
        log_line(log_path, f"Input root: {input_root}")
        log_line(log_path, f"System: {system_id}")
        log_line(log_path, f"Run root: {run_root}")
        log_line(log_path, f"Selected stage: {STAGE_LABELS[stage]} ({stage})")
        log_line(log_path, f"QE pre-relax: {'yes' if qe_relax else 'no'}")

        ensure_stage_prerequisites(run_root, stage, log_path)

        command = build_modular_command(stage, run_root, input_root, system_id, qe_relax, args.qe_mode)
        run_streaming_command(command, cwd=ROOT, log_path=log_path, label=STAGE_LABELS[stage])
        print_result_summary(stage, run_root, log_path)
        log_section(log_path, "Complete")
        log_line(log_path, "Workflow finished successfully.")
        print_artifact_summary(run_root, log_path)
        return 0
    except Exception as exc:
        _append_log(log_path, traceback.format_exc())
        log_line(log_path, "")
        log_line(log_path, f"ERROR: {type(exc).__name__}: {exc}")
        log_line(log_path, f"Saved log: {log_path}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
