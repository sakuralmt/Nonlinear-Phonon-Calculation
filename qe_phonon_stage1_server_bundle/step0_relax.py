#!/usr/bin/env python3
from __future__ import annotations

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
    parse_sbatch_job_id,
    prepare_primitive_qe_input,
    relpath,
    squeue_existing_job_ids,
    write_qe_input,
)
from config import (
    MODULE_LINES,
    OPTIMIZED_SCF_TEMPLATE,
    PSEUDO_DIR,
    RAW_SCF_TEMPLATE,
    RELAX_JOB_PREFIX,
    RELAX_NODES,
    RELAX_NTASKS,
    RELAX_PARTITION,
    RELAX_QOS,
    RELAX_ROOT,
    RELAX_STRICT_PRESET_NAME,
    RELAX_TIME,
)
from scf_settings import compact_settings_summary, resolve_scf_settings


def _status_path() -> Path:
    return RELAX_ROOT / "job_status.json"


def _summary_path() -> Path:
    return RELAX_ROOT / "relax_summary.json"


def _read_status():
    path = _status_path()
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _write_status(payload: dict):
    dump_json(_status_path(), payload)


def _write_submit(stage_dir: Path) -> Path:
    lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name={RELAX_JOB_PREFIX}",
        f"#SBATCH --nodes={RELAX_NODES}",
        f"#SBATCH --ntasks={RELAX_NTASKS}",
        f"#SBATCH --time={RELAX_TIME}",
        f"#SBATCH --partition={RELAX_PARTITION}",
        f"#SBATCH --chdir={stage_dir}",
        "#SBATCH --output=slurm-%j.out",
        "#SBATCH --error=slurm-%j.err",
    ]
    if RELAX_QOS:
        lines.append(f"#SBATCH --qos={RELAX_QOS}")
    lines.extend(["", "ulimit -s unlimited", "ulimit -c unlimited"])
    lines.extend(MODULE_LINES)
    lines.extend(
        [
            "",
            f"cd {stage_dir}",
            "mkdir -p tmp",
            f"mpirun -np {RELAX_NTASKS} pw.x < vc_relax.inp > vc_relax.out",
            "",
            "exit",
            "",
        ]
    )
    path = stage_dir / "submit.sh"
    path.write_text("\n".join(lines))
    path.chmod(0o755)
    return path


def prepare_relax() -> Path:
    ensure_dir(RELAX_ROOT)
    scf_settings = resolve_scf_settings(RELAX_STRICT_PRESET_NAME)
    template = load_qe_template(RAW_SCF_TEMPLATE)
    primitive_k_mesh = list(scf_settings.get("primitive_k_mesh") or template["k_points"])
    prepare_primitive_qe_input(
        template_path=RAW_SCF_TEMPLATE,
        out_file=RELAX_ROOT / "vc_relax.inp",
        pseudo_dir_rel=relpath(RELAX_ROOT, PSEUDO_DIR.resolve()),
        scf_settings=scf_settings,
        k_mesh=primitive_k_mesh,
    )
    submit_path = _write_submit(RELAX_ROOT)
    manifest = {
        "kind": "qe_bundle_vc_relax",
        "input_structure": str(RAW_SCF_TEMPLATE.resolve()),
        "output_root": str(RELAX_ROOT.resolve()),
        "strict_preset": RELAX_STRICT_PRESET_NAME,
        "strict_settings_summary": compact_settings_summary(scf_settings),
        "strict_settings": scf_settings,
        "submit_script": str(submit_path.resolve()),
    }
    dump_json(RELAX_ROOT / "relax_manifest.json", manifest)
    return RELAX_ROOT / "relax_manifest.json"


def _submit_if_needed() -> str | None:
    status = _read_status()
    if status and status.get("job_id") and str(status["job_id"]) in squeue_existing_job_ids([str(status["job_id"])]):
        return str(status["job_id"])
    result = subprocess.run(["sbatch", str(RELAX_ROOT / "submit.sh")], capture_output=True, text=True)
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if result.returncode != 0:
        raise RuntimeError(f"sbatch failed for vc-relax:\n{stdout}\n{stderr}")
    job_id = parse_sbatch_job_id(stdout)
    if not job_id:
        raise RuntimeError(f"Could not parse vc-relax job id: {stdout}")
    _write_status(
        {
            "state": "submitted",
            "job_id": job_id,
            "sbatch_stdout": stdout,
            "sbatch_stderr": stderr,
            "submit_time_epoch": time.time(),
        }
    )
    return job_id


def _collect_result() -> dict:
    template = load_qe_template(RAW_SCF_TEMPLATE)
    final_structure = extract_final_relaxed_structure(RELAX_ROOT / "vc_relax.out", template["nat"])
    if final_structure is None:
        raise RuntimeError("vc-relax finished but final structure could not be parsed")

    strict_scf_settings = resolve_scf_settings("phonon_strict")
    write_qe_input(
        out_file=OPTIMIZED_SCF_TEMPLATE,
        cell=final_structure["cell"],
        symbols=final_structure["symbols"],
        frac_positions=final_structure["frac"],
        constraints=final_structure["constraints"],
        k_mesh=list(strict_scf_settings.get("primitive_k_mesh") or template["k_points"]),
        pseudo_dir_rel=relpath(RELAX_ROOT, PSEUDO_DIR.resolve()),
        scf_settings=strict_scf_settings,
    )

    summary = {
        "input_structure": str(RAW_SCF_TEMPLATE.resolve()),
        "optimized_structure": str(OPTIMIZED_SCF_TEMPLATE.resolve()),
        "vc_relax_output": str((RELAX_ROOT / "vc_relax.out").resolve()),
        "strict_preset": RELAX_STRICT_PRESET_NAME,
        "final_energy_ry": extract_energy_ry(RELAX_ROOT / "vc_relax.out"),
        "final_total_force_ry_bohr": extract_total_force_ry_bohr(RELAX_ROOT / "vc_relax.out"),
        "final_max_atomic_force_ry_bohr": extract_max_atomic_force_ry_bohr(RELAX_ROOT / "vc_relax.out"),
        "wall_sec": extract_wall_sec(RELAX_ROOT / "vc_relax.out"),
        "job_done": file_contains_job_done(RELAX_ROOT / "vc_relax.out"),
        "final_structure": final_structure,
    }
    dump_json(_summary_path(), summary)
    _write_status({"state": "completed"})
    return summary


def run_relax() -> dict:
    if _summary_path().exists() and OPTIMIZED_SCF_TEMPLATE.exists():
        return json.loads(_summary_path().read_text())

    prepare_relax()
    job_id = _submit_if_needed()
    while True:
        if file_contains_job_done(RELAX_ROOT / "vc_relax.out"):
            return _collect_result()
        active = bool(job_id and str(job_id) in squeue_existing_job_ids([str(job_id)]))
        _write_status({"state": "running" if active else "submitted", "job_id": job_id})
        if not active:
            raise RuntimeError("vc-relax stalled without JOB DONE")
        time.sleep(20)


if __name__ == "__main__":
    prepare_relax()
    summary = run_relax()
    print(json.dumps(summary, indent=2))
