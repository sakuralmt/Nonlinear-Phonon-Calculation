#!/usr/bin/env python3
from __future__ import annotations

import getpass
import json
import subprocess
import time
from pathlib import Path

from common import (
    dump_json,
    ensure_dir,
    extract_energy_ry,
    extract_final_relaxed_structure,
    extract_max_atomic_force_ry_bohr,
    extract_total_force_ry_bohr,
    extract_wall_sec,
    file_contains_job_done,
    load_qe_template,
    max_cell_delta_A,
    max_position_delta_A,
    parse_sbatch_job_id,
    primitive_k_mesh_from_settings,
    relpath,
    resolve_structure_template,
    squeue_existing_job_ids,
    write_qe_input,
)
from config import (
    COMMON_TUNING_AXES,
    COMMON_TUNING_BASE_OVERRIDES,
    MAX_RUNNING_AUTOTUNE_JOBS,
    PARAM_TUNING_ROOT,
    PHONON_ACTIVE_PROFILE_LEVEL,
    PHONON_BALANCED_THRESHOLDS,
    PHONON_BALANCED_RELAXED_SCALE,
    PHONON_STRICT_PRESET_NAME,
    PHONON_TUNING_ROOT,
    PES_ACTIVE_PROFILE_LEVEL,
    PES_BALANCED_THRESHOLDS,
    PES_BALANCED_RELAXED_SCALE,
    PES_FAST_THRESHOLDS,
    PES_FAST_RELAXED_SCALE,
    PES_STRICT_PRESET_NAME,
    PES_TUNING_ROOT,
    PSEUDO_DIR,
    RAW_SCF_TEMPLATE,
    RELAX_STRICT_PRESET_NAME,
    SCF_TEMPLATE,
)
from scf_settings import compact_settings_summary, resolve_scf_settings


def _candidate_signature(settings: dict) -> tuple:
    return (
        settings.get("ecutwfc"),
        settings.get("ecutrho"),
        settings.get("conv_thr"),
        settings.get("degauss"),
        tuple(settings.get("primitive_k_mesh", [])),
        settings.get("mixing_beta"),
        settings.get("electron_maxstep"),
    )


def _submit_script(job_dir: Path, job_name: str, ntasks: int = 24) -> Path:
    lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name={job_name[:48]}",
        "#SBATCH --nodes=1",
        f"#SBATCH --ntasks={ntasks}",
        "#SBATCH --time=24:00:00",
        "#SBATCH --partition=long",
        f"#SBATCH --chdir={job_dir}",
        "#SBATCH --output=slurm-%j.out",
        "#SBATCH --error=slurm-%j.err",
        "",
        "ulimit -s unlimited",
        "ulimit -c unlimited",
        "",
        f"cd {job_dir}",
        "mkdir -p tmp",
        f"mpirun -np {ntasks} pw.x < vc_relax.inp > vc_relax.out",
        "",
        "exit",
        "",
    ]
    path = job_dir / "submit.sh"
    path.write_text("\n".join(lines))
    path.chmod(0o755)
    return path


def _build_candidates(strict_preset_name: str, base_overrides: dict, axes: dict) -> tuple[dict, list[dict]]:
    strict_settings = resolve_scf_settings(strict_preset_name)
    base_settings = resolve_scf_settings(strict_preset_name, overrides=base_overrides)
    candidates = []
    seen = set()

    def _append(name: str, axis: str, level: str, settings: dict):
        sig = _candidate_signature(settings)
        if sig in seen:
            return
        seen.add(sig)
        candidates.append(
            {
                "name": name,
                "axis": axis,
                "level": level,
                "settings": settings,
            }
        )

    _append("strict_reference", "strict", "reference", strict_settings)
    _append("base_mid", "base", "mid", base_settings)
    for axis_name, points in axes.items():
        for point in points:
            settings = dict(base_settings)
            settings.update(point.get("overrides", {}))
            if point.get("primitive_k_mesh"):
                settings["primitive_k_mesh"] = list(point["primitive_k_mesh"])
            _append(f"{axis_name}_{point['label']}", axis_name, point["label"], settings)

    return strict_settings, candidates


def _profile_settings_from_candidate(strict_output_settings: dict, candidate_settings: dict | None) -> dict:
    profile = dict(strict_output_settings)
    if candidate_settings is None:
        return profile
    for key in [
        "ecutwfc",
        "ecutrho",
        "conv_thr",
        "degauss",
        "primitive_k_mesh",
        "mixing_beta",
        "electron_maxstep",
        "occupations",
        "smearing",
        "k_scale",
    ]:
        if key in candidate_settings:
            profile[key] = candidate_settings[key]
    return profile


def _write_candidate_jobs(run_root: Path, structure_template: Path, strict_settings: dict, candidates: list[dict]) -> dict:
    ensure_dir(run_root)
    template = load_qe_template(structure_template)
    rows = []
    for item in candidates:
        job_dir = run_root / item["name"]
        ensure_dir(job_dir)
        k_mesh = primitive_k_mesh_from_settings(template, item["settings"])
        write_qe_input(
            out_file=job_dir / "vc_relax.inp",
            cell=template["cell"],
            symbols=template["symbols"],
            frac_positions=template["frac"],
            constraints=template["constraints"],
            k_mesh=k_mesh,
            pseudo_dir_rel=relpath(job_dir, PSEUDO_DIR.resolve()),
            scf_settings=item["settings"],
        )
        _submit_script(job_dir, f"autotune_{run_root.name}_{item['name']}")
        dump_json(
            job_dir / "job_meta.json",
            {
                "candidate": item["name"],
                "axis": item["axis"],
                "level": item["level"],
                "settings": item["settings"],
            },
        )
        rows.append(item)

    manifest = {
        "structure_template": str(structure_template.resolve()),
        "strict_settings": strict_settings,
        "strict_settings_summary": compact_settings_summary(strict_settings),
        "candidates": rows,
    }
    dump_json(run_root / "run_manifest.json", manifest)
    return manifest


def _read_status(job_dir: Path):
    path = job_dir / "job_status.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _write_status(job_dir: Path, payload: dict):
    dump_json(job_dir / "job_status.json", payload)


def _current_user_running_count() -> int:
    try:
        user = getpass.getuser()
        result = subprocess.run(["squeue", "-u", user, "-h", "-o", "%i"], capture_output=True, text=True, check=True)
        txt = result.stdout.strip()
        return 0 if not txt else len([line for line in txt.splitlines() if line.strip()])
    except Exception:
        return 0


def _submit_pending_jobs(run_root: Path):
    manifest = json.loads((run_root / "run_manifest.json").read_text())
    job_dirs = [run_root / item["name"] for item in manifest["candidates"]]
    while True:
        active_ids = []
        pending = []
        for job_dir in job_dirs:
            status = _read_status(job_dir)
            if file_contains_job_done(job_dir / "vc_relax.out"):
                _write_status(job_dir, {"state": "completed"})
                continue
            if status and status.get("job_id"):
                active_ids.append(str(status["job_id"]))
                continue
            pending.append(job_dir)

        queued = squeue_existing_job_ids(active_ids)
        user_running_count = _current_user_running_count()
        available = max(MAX_RUNNING_AUTOTUNE_JOBS - user_running_count, 0)
        if not pending:
            return
        if available <= 0:
            time.sleep(20)
            continue
        sent = 0
        for job_dir in pending:
            if sent >= available:
                break
            result = subprocess.run(["sbatch", str(job_dir / "submit.sh")], capture_output=True, text=True)
            stdout = result.stdout.strip()
            stderr = result.stderr.strip()
            if result.returncode != 0:
                raise RuntimeError(f"sbatch failed for autotune job {job_dir}:\n{stdout}\n{stderr}")
            job_id = parse_sbatch_job_id(stdout)
            if not job_id:
                raise RuntimeError(f"Could not parse autotune job id for {job_dir}: {stdout}")
            _write_status(
                job_dir,
                {
                    "state": "submitted",
                    "job_id": job_id,
                    "sbatch_stdout": stdout,
                    "sbatch_stderr": stderr,
                    "submit_time_epoch": time.time(),
                },
            )
            sent += 1
        if sent == 0:
            time.sleep(20)


def _wait_for_jobs(run_root: Path):
    while True:
        _submit_pending_jobs(run_root)
        manifest = json.loads((run_root / "run_manifest.json").read_text())
        job_dirs = [run_root / item["name"] for item in manifest["candidates"]]
        active_ids = []
        completed = 0
        for job_dir in job_dirs:
            if file_contains_job_done(job_dir / "vc_relax.out"):
                completed += 1
                _write_status(job_dir, {"state": "completed"})
                continue
            status = _read_status(job_dir)
            if status and status.get("job_id"):
                active_ids.append(str(status["job_id"]))
        if completed >= len(job_dirs):
            return
        queued = squeue_existing_job_ids(active_ids)
        for job_dir in job_dirs:
            status = _read_status(job_dir)
            if not status or not status.get("job_id"):
                continue
            if str(status["job_id"]) in queued:
                status["state"] = "running"
            elif status.get("state") not in {"completed", "exhausted"}:
                status["state"] = "exhausted"
            _write_status(job_dir, status)
        if not queued:
            return
        time.sleep(20)


def _candidate_metrics(job_dir: Path, reference_structure: dict):
    out_path = job_dir / "vc_relax.out"
    final_structure = extract_final_relaxed_structure(out_path, len(reference_structure["symbols"]))
    energy_ry = extract_energy_ry(out_path)
    total_force = extract_total_force_ry_bohr(out_path)
    max_force = extract_max_atomic_force_ry_bohr(out_path)
    wall_sec = extract_wall_sec(out_path)
    job_done = file_contains_job_done(out_path)
    metrics = {
        "job_done": job_done,
        "energy_ry": energy_ry,
        "final_total_force_ry_bohr": total_force,
        "final_max_atomic_force_ry_bohr": max_force,
        "wall_sec": wall_sec,
        "final_structure": final_structure,
        "energy_abs_diff_mev": None,
        "max_position_delta_A": None,
        "max_cell_delta_A": None,
    }
    if energy_ry is not None and reference_structure.get("energy_ry") is not None:
        metrics["energy_abs_diff_mev"] = abs(energy_ry - reference_structure["energy_ry"]) * 13.605693009 * 1000.0
    if final_structure is not None:
        metrics["max_position_delta_A"] = max_position_delta_A(
            reference_structure["cell"],
            reference_structure["frac"],
            final_structure["cell"],
            final_structure["frac"],
        )
        metrics["max_cell_delta_A"] = max_cell_delta_A(reference_structure["cell"], final_structure["cell"])
    return metrics


def _eligible(metrics: dict, thresholds: dict, require_force: bool) -> bool:
    if not metrics.get("job_done"):
        return False
    if metrics.get("energy_abs_diff_mev") is None or metrics["energy_abs_diff_mev"] > thresholds["energy_abs_diff_mev"]:
        return False
    if metrics.get("max_position_delta_A") is None or metrics["max_position_delta_A"] > thresholds["max_position_delta_A"]:
        return False
    if metrics.get("max_cell_delta_A") is None or metrics["max_cell_delta_A"] > thresholds["max_cell_delta_A"]:
        return False
    if require_force:
        force = metrics.get("final_max_atomic_force_ry_bohr")
        if force is None or force > thresholds["max_atomic_force_ry_bohr"]:
            return False
    return True


def _scaled_thresholds(thresholds: dict, scale: float) -> dict:
    return {
        key: (value * scale if isinstance(value, (int, float)) else value)
        for key, value in thresholds.items()
    }


def _select_fastest(rows: list[dict], thresholds: dict, require_force: bool):
    candidates = [
        row
        for row in rows
        if row["name"] != "strict_reference" and _eligible(row["metrics"], thresholds, require_force=require_force)
    ]
    candidates.sort(key=lambda row: (1.0e18 if row["metrics"]["wall_sec"] is None else row["metrics"]["wall_sec"], row["name"]))
    return candidates[0] if candidates else None


def _combined_cache_paths():
    return (
        PARAM_TUNING_ROOT / "combined_curve_summary.json",
        PARAM_TUNING_ROOT / "combined_selected_profiles.json",
    )


def _build_branch_selection(
    branch_name: str,
    rows: list[dict],
    strict_output_settings: dict,
    balanced_thresholds: dict,
    balanced_relaxed_scale: float,
    fast_thresholds: dict | None = None,
    fast_relaxed_scale: float | None = None,
) -> tuple[dict, list[str], dict, dict | None]:
    warnings = []
    require_force = branch_name == "phonon"
    balanced = _select_fastest(rows, balanced_thresholds, require_force=require_force)
    balanced_thresholds_used = dict(balanced_thresholds)
    if balanced is None:
        balanced_thresholds_used = _scaled_thresholds(balanced_thresholds, balanced_relaxed_scale)
        msg = f"{branch_name}: no candidate met the default balanced thresholds; retried with relaxed scale x{balanced_relaxed_scale:.2f}."
        warnings.append(msg)
        print(f"[warning] {msg}")
        balanced = _select_fastest(rows, balanced_thresholds_used, require_force=require_force)
    if balanced is None:
        raise RuntimeError(f"{branch_name} autotune failed: no candidate met the balanced convergence criteria, even after relaxing thresholds.")

    fast = None
    fast_thresholds_used = None
    if fast_thresholds is not None:
        fast_thresholds_used = dict(fast_thresholds)
        fast = _select_fastest(rows, fast_thresholds, require_force=False)
        if fast is None and fast_relaxed_scale is not None:
            fast_thresholds_used = _scaled_thresholds(fast_thresholds, fast_relaxed_scale)
            msg = f"{branch_name}: no candidate met the default fast thresholds; retried with relaxed scale x{fast_relaxed_scale:.2f}."
            warnings.append(msg)
            print(f"[warning] {msg}")
            fast = _select_fastest(rows, fast_thresholds_used, require_force=False)

    selected = {
        "strict": {
            "preset_name": "shared_strict_reference",
            "settings": strict_output_settings,
            "settings_summary": compact_settings_summary(strict_output_settings),
        },
        "balanced": {
            "source_candidate": balanced["name"],
            "settings": _profile_settings_from_candidate(strict_output_settings, balanced["settings"]),
            "settings_summary": compact_settings_summary(_profile_settings_from_candidate(strict_output_settings, balanced["settings"])),
        },
    }
    if fast_thresholds is not None:
        fast_settings = _profile_settings_from_candidate(
            selected["balanced"]["settings"] if fast is None else strict_output_settings,
            None if fast is None else fast["settings"],
        )
        selected["fast"] = {
            "source_candidate": None if fast is None else fast["name"],
            "settings": fast_settings,
            "settings_summary": compact_settings_summary(fast_settings),
        }

    return selected, warnings, balanced_thresholds_used, fast_thresholds_used


def _ensure_combined_tuning() -> tuple[list[dict], dict]:
    summary_path, selected_path = _combined_cache_paths()
    if summary_path.exists() and selected_path.exists():
        payload = json.loads(summary_path.read_text())
        selected = json.loads(selected_path.read_text())
        return payload["rows"], selected

    structure_template = resolve_structure_template(SCF_TEMPLATE, RAW_SCF_TEMPLATE)
    strict_settings, candidates = _build_candidates(
        strict_preset_name=RELAX_STRICT_PRESET_NAME,
        base_overrides=COMMON_TUNING_BASE_OVERRIDES,
        axes=COMMON_TUNING_AXES,
    )
    _write_candidate_jobs(PARAM_TUNING_ROOT, structure_template, strict_settings, candidates)
    _submit_pending_jobs(PARAM_TUNING_ROOT)
    _wait_for_jobs(PARAM_TUNING_ROOT)

    reference_job_dir = PARAM_TUNING_ROOT / "strict_reference"
    template = load_qe_template(structure_template)
    reference_final = extract_final_relaxed_structure(reference_job_dir / "vc_relax.out", template["nat"])
    if reference_final is None:
        raise RuntimeError(f"Combined autotune failed: strict reference did not produce a parseable final structure: {reference_job_dir}")
    reference_structure = dict(reference_final)
    reference_structure["energy_ry"] = extract_energy_ry(reference_job_dir / "vc_relax.out")

    rows = []
    for item in candidates:
        job_dir = PARAM_TUNING_ROOT / item["name"]
        metrics = _candidate_metrics(job_dir, reference_structure)
        rows.append(
            {
                "name": item["name"],
                "axis": item["axis"],
                "level": item["level"],
                "settings": item["settings"],
                "settings_summary": compact_settings_summary(item["settings"]),
                "metrics": metrics,
            }
        )

    phonon_strict_output = resolve_scf_settings(PHONON_STRICT_PRESET_NAME)
    pes_strict_output = resolve_scf_settings(PES_STRICT_PRESET_NAME)
    phonon_selected, phonon_warnings, phonon_balanced_used, _ = _build_branch_selection(
        branch_name="phonon",
        rows=rows,
        strict_output_settings=phonon_strict_output,
        balanced_thresholds=PHONON_BALANCED_THRESHOLDS,
        balanced_relaxed_scale=PHONON_BALANCED_RELAXED_SCALE,
        fast_thresholds=None,
        fast_relaxed_scale=None,
    )
    pes_selected, pes_warnings, pes_balanced_used, pes_fast_used = _build_branch_selection(
        branch_name="pes",
        rows=rows,
        strict_output_settings=pes_strict_output,
        balanced_thresholds=PES_BALANCED_THRESHOLDS,
        balanced_relaxed_scale=PES_BALANCED_RELAXED_SCALE,
        fast_thresholds=PES_FAST_THRESHOLDS,
        fast_relaxed_scale=PES_FAST_RELAXED_SCALE,
    )

    combined_summary = {
        "structure_template": str(structure_template),
        "reference_source": str(structure_template),
        "rows": rows,
        "branches": {
            "phonon": {
                "warnings": phonon_warnings,
                "balanced_thresholds_used": phonon_balanced_used,
                "selected": phonon_selected,
            },
            "pes": {
                "warnings": pes_warnings,
                "balanced_thresholds_used": pes_balanced_used,
                "fast_thresholds_used": pes_fast_used,
                "selected": pes_selected,
            },
        },
    }
    combined_selected = {"phonon": phonon_selected, "pes": pes_selected}
    ensure_dir(PHONON_TUNING_ROOT)
    ensure_dir(PES_TUNING_ROOT)
    dump_json(summary_path, combined_summary)
    dump_json(selected_path, combined_selected)
    dump_json(PHONON_TUNING_ROOT / "phonon_curve_summary.json", {
        "branch": "phonon",
        "structure_template": str(structure_template),
        "reference_source": str(structure_template),
        "rows": rows,
        "warnings": phonon_warnings,
        "balanced_thresholds_used": phonon_balanced_used,
        "selected": phonon_selected,
    })
    dump_json(PHONON_TUNING_ROOT / "phonon_selected_profiles.json", {"phonon": phonon_selected})
    dump_json(PES_TUNING_ROOT / "pes_curve_summary.json", {
        "branch": "pes",
        "structure_template": str(structure_template),
        "reference_source": str(structure_template),
        "rows": rows,
        "warnings": pes_warnings,
        "balanced_thresholds_used": pes_balanced_used,
        "fast_thresholds_used": pes_fast_used,
        "selected": pes_selected,
    })
    dump_json(PES_TUNING_ROOT / "pes_selected_profiles.json", {"pes": pes_selected})
    return rows, combined_selected


def run_phonon_tuning() -> dict:
    _rows, _selected = _ensure_combined_tuning()
    return json.loads((PHONON_TUNING_ROOT / "phonon_curve_summary.json").read_text())


def run_pes_tuning() -> dict:
    _rows, _selected = _ensure_combined_tuning()
    return json.loads((PES_TUNING_ROOT / "pes_curve_summary.json").read_text())


if __name__ == "__main__":
    print(json.dumps(run_phonon_tuning(), indent=2))
