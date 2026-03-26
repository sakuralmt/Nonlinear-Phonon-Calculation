#!/usr/bin/env python3
from __future__ import annotations

import os
import json
import select
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RUN_DIR_NAME = "release_run"
LOG_FILE_NAME = "start_release.log"
DEFAULT_QE_RELAX = True
DEFAULT_STAGE = "all"
VALID_STAGES = ("all", "stage1", "stage2", "stage3")
DRIVER_HEARTBEAT_SECONDS = 60
STAGE_LABELS = {
    "all": "Full workflow",
    "stage1": "Stage 1",
    "stage2": "Stage 2",
    "stage3": "Stage 3",
}
STAGE_DESCRIPTIONS = {
    "all": "Run the full release path: optional QE relax, then stage1 -> stage2 -> stage3.",
    "stage1": "Run the real stage1 phonon frontend and generate mode-pair handoff inputs.",
    "stage2": "Run MLFF screening from an existing real stage1 manifest.",
    "stage3": "Prepare or submit QE top5 recheck jobs from an existing stage2 manifest.",
}


if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from nonlinear_phonon_calculation.resources import bundle_path
from server_highthroughput_workflow.qe_relax_preflight import run_qe_relax
from server_highthroughput_workflow.scheduler import resolve_scheduler_mode

FIXED_SCHEDULER = "auto"
FIXED_BACKEND = "chgnet"
FIXED_MODEL = "r2scan"
FIXED_STRUCTURE = bundle_path("nonlocal phonon/scf.inp")
FIXED_PSEUDO_DIR = bundle_path("nonlocal phonon")
MODULAR_RUNNER = ROOT / "server_highthroughput_workflow" / "run_modular_pipeline.py"


def _append_log(log_path: Path, line: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as handle:
        handle.write(line)


def log_line(log_path: Path, message: str) -> None:
    text = f"{message}\n"
    print(message, flush=True)
    _append_log(log_path, text)


def log_section(log_path: Path, title: str) -> None:
    log_line(log_path, "")
    log_line(log_path, f"[{title}]")


def load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text())


def show_stage_choices(log_path: Path) -> None:
    log_section(log_path, "Available stages")
    for stage in VALID_STAGES:
        log_line(log_path, f"- {STAGE_LABELS[stage]} ({stage}): {STAGE_DESCRIPTIONS[stage]}")


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


def prompt_stage(question: str, default: str) -> str:
    options = "/".join(VALID_STAGES)
    while True:
        answer = input(f"{question} [{options}] (default: {default}): ").strip().lower()
        if not answer:
            return default
        if answer in VALID_STAGES:
            return answer
        print(f"Please choose one of: {options}.", flush=True)


def stage_manifest_path(run_root: Path, stage: str) -> Path:
    if stage == "stage1":
        return run_root / "stage1_manifest.json"
    if stage == "stage2":
        return run_root / "stage2_manifest.json"
    if stage == "stage3":
        return run_root / "stage3_manifest.json"
    raise ValueError(f"Unsupported stage name: {stage}")


def ensure_stage_prerequisites(run_root: Path, stage: str, log_path: Path) -> None:
    required = None
    hint = None
    if stage == "stage2":
        required = run_root / "stage1_manifest.json"
        hint = "Run npc (or ./tui) and choose stage1 first to create the missing stage1 manifest."
    elif stage == "stage3":
        required = run_root / "stage2_manifest.json"
        hint = "Run npc (or ./tui) and choose stage2 first to create the missing stage2 manifest."
    if required is None:
        return
    if not required.exists():
        message = f"Missing required file for {stage}: {required}"
        if hint:
            message = f"{message} {hint}"
        raise RuntimeError(message)
    log_line(log_path, f"Found prerequisite for {stage}: {required}")


def choose_qe_mode() -> str:
    scheduler_mode = resolve_scheduler_mode(FIXED_SCHEDULER)
    return "submit_collect" if scheduler_mode == "slurm" else "prepare_only"


def build_modular_command(stage: str, run_root: Path, structure_path: Path | None) -> list[str]:
    command = [
        sys.executable,
        "-u",
        str(MODULAR_RUNNER),
        "--stage",
        stage,
        "--run-root",
        str(run_root),
        "--scheduler",
        FIXED_SCHEDULER,
        "--backend",
        FIXED_BACKEND,
        "--model",
        FIXED_MODEL,
    ]

    if stage in {"all", "stage1"} and structure_path is not None:
        command.extend(
            [
                "--structure",
                str(structure_path),
                "--pseudo-dir",
                str(FIXED_PSEUDO_DIR),
            ]
        )

    if stage in {"all", "stage3"}:
        command.extend(["--qe-mode", choose_qe_mode()])

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
            log_line(
                log_path,
                f"{label} driver still running ({_format_elapsed(now - start_time)} elapsed).",
            )
            last_visible_output = now

    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)
    log_line(log_path, f"{label} driver finished.")


def print_artifact_summary(run_root: Path, log_path: Path) -> None:
    log_line(log_path, "")
    log_line(log_path, "Artifacts:")
    log_line(log_path, f"- Run root: {run_root}")
    pre_relax = run_root / "pre_relax" / "optimized_structure.scf.inp"
    if pre_relax.exists():
        log_line(log_path, f"- Relaxed structure: {pre_relax}")
    for stage in ("stage1", "stage2", "stage3"):
        manifest = stage_manifest_path(run_root, stage)
        if manifest.exists():
            log_line(log_path, f"- {stage} manifest: {manifest}")
    stage3_results = run_root / "stage3_qe" / FIXED_BACKEND / "results" / "qe_ranking.json"
    if stage3_results.exists():
        log_line(log_path, f"- QE ranking: {stage3_results}")


def _preview_codes(rows: list[dict], key: str, limit: int = 3) -> str:
    preview = [str(row.get(key)) for row in rows[:limit] if row.get(key)]
    return ", ".join(preview) if preview else "(none)"


def _format_float(value) -> str:
    try:
        return f"{float(value):.3f}"
    except Exception:
        return str(value)


def _summarize_stage2_row(row: dict) -> str:
    parts = [str(row.get("pair_code", "(unknown)"))]
    if row.get("point_label"):
        parts.append(f"point={row['point_label']}")
    if row.get("phi122_mev") is not None:
        parts.append(f"|phi122|={abs(float(row['phi122_mev'])):.3f} meV")
    if row.get("rmse_ev_supercell") is not None:
        parts.append(f"rmse={_format_float(row['rmse_ev_supercell'])} eV")
    return ", ".join(parts)


def _summarize_stage3_row(row: dict) -> str:
    parts = [str(row.get("pair_code", "(unknown)"))]
    priority_keys = [
        ("rank", "rank"),
        ("phi122_mev", "phi122"),
        ("phi122_fit_mev", "phi122_fit"),
        ("phi122_abs_error_mev", "phi122_err"),
        ("gamma_freq_thz", "gamma_thz"),
        ("target_freq_thz", "target_thz"),
        ("rmse_mev", "rmse_meV"),
        ("rmse_ev_supercell", "rmse_eV"),
    ]
    for key, label in priority_keys:
        if row.get(key) is not None:
            parts.append(f"{label}={_format_float(row[key])}")
    return ", ".join(parts)


def _print_top_rows(log_path: Path, title: str, rows: list[dict], formatter, limit: int = 3) -> None:
    if not rows:
        return
    log_line(log_path, f"- {title}:")
    for index, row in enumerate(rows[:limit], start=1):
        log_line(log_path, f"  {index}. {formatter(row)}")


def _format_final_qe_state(state: str | None) -> str:
    if state == "all_completed":
        return "All prepared QE jobs completed and were collected."
    if state == "stalled_incomplete":
        return "QE submission stopped with unfinished jobs still missing outputs."
    if state:
        return state
    return "unknown"


def summarize_stage1(run_root: Path, log_path: Path) -> None:
    manifest = load_json(run_root / "stage1_manifest.json")
    if not manifest:
        return
    log_section(log_path, "Stage 1 summary")
    log_line(log_path, "Stage 1 packaged the input handoff for downstream stages.")
    files = manifest.get("files", {})
    pseudo_files = manifest.get("pseudo_files", [])
    if files.get("mode_pairs_json"):
        log_line(log_path, f"- Mode pairs snapshot: {run_root / files['mode_pairs_json']}")
    if files.get("structure"):
        log_line(log_path, f"- Structure snapshot: {run_root / files['structure']}")
    log_line(log_path, f"- Pseudopotentials copied: {len(pseudo_files)}")
    log_line(log_path, "- Next recommended action: rerun npc (or ./tui) and choose stage2.")


def summarize_stage2(run_root: Path, log_path: Path) -> None:
    manifest = load_json(run_root / "stage2_manifest.json")
    if not manifest:
        return
    log_section(log_path, "Stage 2 summary")
    log_line(log_path, "Stage 2 completed MLFF screening and produced ranking artifacts for QE follow-up.")
    output_files = manifest.get("output_files", {})
    runtime_files = manifest.get("runtime_files", {})
    ranking_json_path = run_root / output_files["ranking_json"] if output_files.get("ranking_json") else None
    ranking_csv_path = run_root / output_files["ranking_csv"] if output_files.get("ranking_csv") else None
    pair_ranking_path = run_root / runtime_files["pair_ranking_json"] if runtime_files.get("pair_ranking_json") else None
    run_meta_path = run_root / runtime_files["run_meta"] if runtime_files.get("run_meta") else None
    runtime_cfg_path = run_root / runtime_files["runtime_config_used"] if runtime_files.get("runtime_config_used") else None

    rows_payload = load_json(ranking_json_path) if ranking_json_path else None
    pair_payload = load_json(pair_ranking_path) if pair_ranking_path else None
    run_meta = load_json(run_meta_path) if run_meta_path else None
    runtime_cfg = load_json(runtime_cfg_path) if runtime_cfg_path else None

    rows = [] if rows_payload is None else list(rows_payload.get("rows", []))
    pair_rows = [] if pair_payload is None else list(pair_payload.get("pairs", []))
    log_line(log_path, f"- Ranked pairs: {len(rows) if rows else len(pair_rows)}")
    if rows:
        log_line(log_path, f"- Top candidates: {_preview_codes(rows, 'pair_code')}")
        _print_top_rows(log_path, "Top ranking details", rows, _summarize_stage2_row)
    elif pair_rows:
        log_line(log_path, f"- Top candidates: {_preview_codes(pair_rows, 'pair_code')}")
        _print_top_rows(log_path, "Top ranking details", pair_rows, _summarize_stage2_row)
    if run_meta and run_meta.get("backend"):
        backend = run_meta["backend"]
        log_line(log_path, f"- Backend used: {backend.get('backend')} / model={backend.get('model')}")
    if run_meta and run_meta.get("n_pairs") is not None:
        log_line(log_path, f"- Screened pairs: {run_meta['n_pairs']}")
    if run_meta and run_meta.get("structure"):
        log_line(log_path, f"- Structure used: {run_meta['structure']}")
    if runtime_cfg and runtime_cfg.get("runtime"):
        runtime = runtime_cfg["runtime"]
        log_line(
            log_path,
            f"- Runtime profile: strategy={runtime.get('strategy')}, workers={runtime.get('num_workers')}, torch_threads={runtime.get('torch_threads')}",
        )
    screening_dir = None if ranking_json_path is None else ranking_json_path.parent
    if ranking_csv_path and ranking_csv_path.exists():
        log_line(log_path, f"- Ranking CSV: {ranking_csv_path}")
    if ranking_json_path and ranking_json_path.exists():
        log_line(log_path, f"- Ranking JSON: {ranking_json_path}")
    if screening_dir:
        plot_3d = screening_dir / "pair_screening_3d.png"
        plot_top = screening_dir / "pair_ranking_top15.png"
        if plot_3d.exists():
            log_line(log_path, f"- 3D screening plot: {plot_3d}")
        if plot_top.exists():
            log_line(log_path, f"- Top15 ranking plot: {plot_top}")
    log_line(log_path, "- Next recommended action: rerun npc (or ./tui) and choose stage3.")


def summarize_stage3(run_root: Path, log_path: Path) -> None:
    manifest = load_json(run_root / "stage3_manifest.json")
    if not manifest:
        return
    log_section(log_path, "Stage 3 summary")
    qe_files = manifest.get("qe_files", {})
    qe_run_root = run_root / qe_files["qe_run_root"] if qe_files.get("qe_run_root") else None
    status_path = None if qe_run_root is None else qe_run_root / "modular_stage3_status.json"
    status_payload = load_json(status_path) if status_path else None
    qe_run_manifest_path = None if qe_run_root is None else qe_run_root / "run_manifest.json"
    qe_run_manifest = load_json(qe_run_manifest_path) if qe_run_manifest_path else None
    qe_ranking_path = None
    if qe_files.get("qe_ranking_json"):
        qe_ranking_path = run_root / qe_files["qe_ranking_json"]
    elif status_payload and status_payload.get("qe_ranking_json"):
        qe_ranking_path = Path(status_payload["qe_ranking_json"])
    ranking_payload = load_json(qe_ranking_path) if qe_ranking_path else None
    ranking_rows = [] if ranking_payload is None else list(ranking_payload.get("rows", []))

    if qe_run_root:
        log_line(log_path, f"- QE run root: {qe_run_root}")
    if qe_run_manifest and qe_run_manifest.get("job_count") is not None:
        log_line(log_path, f"- Prepared QE jobs: {qe_run_manifest['job_count']}")
    if status_payload and status_payload.get("final_state"):
        log_line(log_path, f"- Final QE state: {_format_final_qe_state(status_payload['final_state'])}")
    if qe_ranking_path and qe_ranking_path.exists():
        log_line(log_path, f"- QE ranking rows: {len(ranking_rows)}")
        if ranking_rows:
            log_line(log_path, f"- Top QE candidates: {_preview_codes(ranking_rows, 'pair_code')}")
            _print_top_rows(log_path, "Top QE result details", ranking_rows, _summarize_stage3_row)
        log_line(log_path, f"- QE ranking JSON: {qe_ranking_path}")
        log_line(log_path, "- Stage 3 is complete. You can now inspect the QE ranking and downstream pair results.")
    elif qe_run_manifest and not status_payload:
        log_line(log_path, "- QE jobs were prepared but not submitted/collected by this run.")
        log_line(log_path, "- Next recommended action: run stage3 on a Slurm machine or submit from the QE run root.")
    else:
        log_line(log_path, "- QE jobs were prepared, but no collected QE ranking is available yet.")
        if qe_run_root:
            log_line(log_path, f"- Check this directory for live job outputs: {qe_run_root}")


def print_result_summary(stage: str, run_root: Path, log_path: Path) -> None:
    if stage == "stage1":
        summarize_stage1(run_root, log_path)
        return
    if stage == "stage2":
        summarize_stage2(run_root, log_path)
        return
    if stage == "stage3":
        summarize_stage3(run_root, log_path)
        return
    summarize_stage1(run_root, log_path)
    summarize_stage2(run_root, log_path)
    summarize_stage3(run_root, log_path)


def main() -> int:
    run_root = Path.cwd() / RUN_DIR_NAME
    run_root.mkdir(parents=True, exist_ok=True)
    log_path = run_root / LOG_FILE_NAME
    _append_log(log_path, f"\n[{datetime.now().astimezone().isoformat(timespec='seconds')}] start_release.py\n")

    try:
        show_stage_choices(log_path)
        qe_relax = prompt_yes_no("Run QE structure relaxation first?", default=DEFAULT_QE_RELAX)
        stage = prompt_stage("Which stage to run?", default=DEFAULT_STAGE)
        stage_label = STAGE_LABELS[stage]

        log_section(log_path, "Release launcher")
        log_line(log_path, "Release launcher started.")
        log_line(log_path, f"Run root: {run_root}")
        log_line(log_path, f"Selected stage: {stage_label} ({stage})")
        log_line(log_path, f"QE pre-relax: {'yes' if qe_relax else 'no'}")
        log_line(log_path, f"Fixed scheduler setting: {FIXED_SCHEDULER} -> {resolve_scheduler_mode(FIXED_SCHEDULER)}")

        ensure_stage_prerequisites(run_root, stage, log_path)

        structure_path = None
        if stage in {"all", "stage1"}:
            structure_path = FIXED_STRUCTURE
            if qe_relax:
                log_section(log_path, "QE pre-relax")
                log_line(log_path, "Starting QE structure relaxation...")
                relax_summary = run_qe_relax(
                    run_root=run_root,
                    structure_path=FIXED_STRUCTURE,
                    pseudo_dir=FIXED_PSEUDO_DIR,
                    scheduler=FIXED_SCHEDULER,
                    emit=lambda message: log_line(log_path, message),
                )
                structure_path = Path(relax_summary["optimized_structure"]).expanduser().resolve()
                log_line(log_path, f"Using relaxed structure: {structure_path}")
            else:
                log_section(log_path, stage_label)
                log_line(log_path, f"Using checked-in structure: {FIXED_STRUCTURE}")
        elif qe_relax:
            log_line(log_path, "QE pre-relax is ignored when starting from stage2 or stage3.")
            log_section(log_path, stage_label)
        else:
            log_section(log_path, stage_label)

        qe_mode = choose_qe_mode()
        if stage in {"all", "stage3"} and qe_mode == "prepare_only":
            log_line(log_path, "No Slurm detected under the fixed scheduler setting. Stage3 will run in prepare_only mode.")

        log_line(log_path, f"Starting workflow stage: {stage_label}.")
        command = build_modular_command(stage, run_root, structure_path)
        run_streaming_command(command, cwd=ROOT, log_path=log_path, label=stage_label)

        print_result_summary(stage, run_root, log_path)
        log_section(log_path, "Complete")
        log_line(log_path, "Workflow finished successfully.")
        print_artifact_summary(run_root, log_path)
        return 0
    except Exception as exc:
        _append_log(log_path, traceback.format_exc())
        log_line(log_path, "")
        log_line(log_path, f"ERROR: {type(exc).__name__}: {exc}")
        if (run_root / "pre_relax" / "job_status.json").exists():
            log_line(log_path, f"Relevant status file: {run_root / 'pre_relax' / 'job_status.json'}")
        for candidate in (
            run_root / "stage1_manifest.json",
            run_root / "stage2_manifest.json",
            run_root / "stage3_manifest.json",
        ):
            if candidate.exists():
                log_line(log_path, f"Relevant artifact: {candidate}")
        log_line(log_path, f"Saved log: {log_path}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
