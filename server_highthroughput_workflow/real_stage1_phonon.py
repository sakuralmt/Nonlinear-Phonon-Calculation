#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from nonlinear_phonon_calculation.workflow_families import resolve_workflow_family
from server_highthroughput_workflow.stage_contracts import create_stage1_manifest, dump_json


STAGE1_SOURCE = ROOT / "qe_phonon_stage1_server_bundle"
QPAIR_TOOLS = STAGE1_SOURCE / "qpair_tools"
QPAIR_ROOT_NAME = "stage1/runtime/qgamma_qpair"
PHONON_RUNTIME_NAME = "stage1/runtime/phonon_bundle"
def _run_python(script: Path, *args: str, cwd: Path | None = None):
    cmd = [sys.executable, str(script), *args]
    subprocess.run(cmd, cwd=None if cwd is None else str(cwd), check=True, text=True)


def _copytree_clean(src: Path, dst: Path):
    shutil.copytree(
        src,
        dst,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "slurm-*.out", "slurm-*.err"),
    )


def _sync_stage1_inputs(runtime_root: Path, structure: Path, pseudo_dir: Path):
    inputs = runtime_root / "inputs"
    pseudos_dst = inputs / "pseudos"
    pseudos_dst.mkdir(parents=True, exist_ok=True)
    shutil.copy2(structure, inputs / "scf.inp")
    for pseudo in sorted(pseudo_dir.glob("*.UPF")):
        shutil.copy2(pseudo, pseudos_dst / pseudo.name)
    (inputs / "structure_meta.json").write_text(json.dumps({"already_relaxed": True}, indent=2) + "\n")


def _write_requested_pairs(screening_json: Path, out_json: Path, out_csv: Path):
    summary = json.loads(screening_json.read_text())
    requests = []
    for idx, item in enumerate(summary["selected_points"], start=1):
        q = [float(x) for x in item["rep_q_frac"]]
        label = item["label"]
        request_id = f"{label}_q_{q[0]:.3f}_{q[1]:.3f}_{q[2]:.3f}_seed_{idx:02d}".replace("-", "m")
        requests.append(
            {
                "request_id": request_id,
                "source_pair_code": request_id,
                "gamma_mode_number": 1,
                "target_q_frac": q,
                "target_mode_number": 1,
                "point_label": label,
            }
        )
    out_json.write_text(json.dumps({"requests": requests}, indent=2) + "\n")
    with out_csv.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "request_id",
                "source_pair_code",
                "gamma_mode_number",
                "target_qx",
                "target_qy",
                "target_qz",
                "target_mode_number",
                "point_label",
            ]
        )
        for row in requests:
            q = row["target_q_frac"]
            writer.writerow(
                [
                    row["request_id"],
                    row["source_pair_code"],
                    row["gamma_mode_number"],
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    row["target_mode_number"],
                    row["point_label"],
                ]
            )
    return requests


def run_real_stage1(
    run_root: Path,
    structure: Path,
    pseudo_dir: Path,
    *,
    system_id: str | None = None,
    system_dir: Path | None = None,
    source_cif: Path | None = None,
    system_meta: Path | None = None,
    workflow_family: str = "tmd_monolayer_hex",
):
    run_root = Path(run_root).expanduser().resolve()
    structure = Path(structure).expanduser().resolve()
    pseudo_dir = Path(pseudo_dir).expanduser().resolve()

    phonon_runtime = run_root / PHONON_RUNTIME_NAME / "qe_phonon_stage1_server_bundle"
    qpair_root = run_root / QPAIR_ROOT_NAME
    screening_root = qpair_root / "screening"
    extracted_root = qpair_root / "extracted"
    mode_selection_root = qpair_root / "mode_selection"
    mode_pairs_root = qpair_root / "mode_pairs"

    family = resolve_workflow_family(workflow_family)
    if phonon_runtime.exists():
        shutil.rmtree(phonon_runtime)
    _copytree_clean(STAGE1_SOURCE, phonon_runtime)
    _sync_stage1_inputs(phonon_runtime, structure, pseudo_dir)

    screen_script = QPAIR_TOOLS / "screen_hex_qgamma_qpair_points.py"
    extract_script = QPAIR_TOOLS / "extract_screened_eigs.py"
    select_script = QPAIR_TOOLS / "select_modes_qgamma_qpair.py"
    pair_script = QPAIR_TOOLS / "generate_mode_pairs_qgamma_qpair.py"

    screening_root.mkdir(parents=True, exist_ok=True)
    _run_python(
        screen_script,
        "--work-dir",
        str(phonon_runtime / "inputs"),
        "--scf-template",
        "scf.inp",
        "--grid-n",
        str(family.stage1_screen_grid_n),
        "--output-dir",
        str(screening_root),
    )

    requests = _write_requested_pairs(
        screening_root / "screening_summary.json",
        phonon_runtime / "inputs" / "requested_pairs.json",
        phonon_runtime / "inputs" / "requested_pairs.csv",
    )

    _run_python(phonon_runtime / "run_all.py", cwd=phonon_runtime)

    extracted_root.mkdir(parents=True, exist_ok=True)
    _run_python(
        extract_script,
        "--eig-file",
        str(phonon_runtime / "qe_phonon_pes_run" / "matdyn" / "qeph.eig"),
        "--screening-json",
        str(screening_root / "screening_summary.json"),
        "--scf-template",
        str(phonon_runtime / "inputs" / "scf.inp"),
        "--q-format",
        "auto",
        "--grid-n",
        str(family.stage1_screen_grid_n),
        "--output-dir",
        str(extracted_root),
    )

    _run_python(
        select_script,
        "--run-root",
        str(qpair_root),
        "--scf-template",
        str(phonon_runtime / "inputs" / "scf.inp"),
        "--output-dir",
        str(mode_selection_root),
        "--apply-selection-rules",
        "--gamma-optical-only",
    )
    _run_python(
        pair_script,
        "--run-root",
        str(qpair_root),
        "--output-dir",
        str(mode_pairs_root),
    )

    manifest = create_stage1_manifest(
        run_root=run_root,
        mode_pairs_json=mode_pairs_root / "selected_mode_pairs.json",
        structure=structure,
        pseudo_dir=pseudo_dir,
        system_id=system_id,
        system_dir=system_dir,
        source_cif=source_cif,
        system_meta=system_meta,
    )

    summary = {
        "kind": "real_stage1_phonon_summary",
        "phonon_runtime_root": str(phonon_runtime),
        "qpair_root": str(qpair_root),
        "screening_summary": str(screening_root / "screening_summary.json"),
        "frontend_manifest": str(phonon_runtime / "qe_phonon_pes_run" / "frontend_manifest.json"),
        "stage1_summary": str(phonon_runtime / "qe_phonon_pes_run" / "results" / "stage1_summary.json"),
        "eig_file": str(phonon_runtime / "qe_phonon_pes_run" / "matdyn" / "qeph.eig"),
        "freq_file": str(phonon_runtime / "qe_phonon_pes_run" / "matdyn" / "qeph.freq"),
        "selected_mode_pairs": str(mode_pairs_root / "selected_mode_pairs.json"),
        "request_count": len(requests),
        "selected_qpoint_count": len(requests),
        "stage1_manifest": str(manifest),
        "workflow_family": family.name,
    }
    dump_json(run_root / "stage1" / "summary.json", summary)
    return manifest


def run_stage1_tuning(
    run_root: Path,
    structure: Path,
    pseudo_dir: Path,
    *,
    workflow_family: str,
):
    run_root = Path(run_root).expanduser().resolve()
    structure = Path(structure).expanduser().resolve()
    pseudo_dir = Path(pseudo_dir).expanduser().resolve()
    phonon_runtime = run_root / PHONON_RUNTIME_NAME / "qe_phonon_stage1_server_bundle"
    if phonon_runtime.exists():
        shutil.rmtree(phonon_runtime)
    _copytree_clean(STAGE1_SOURCE, phonon_runtime)
    _sync_stage1_inputs(phonon_runtime, structure, pseudo_dir)
    tuning_script = phonon_runtime / "convergence" / "autotune.py"
    _run_python(
        tuning_script,
        "--workflow-family",
        workflow_family,
        "--branch",
        "all",
        cwd=phonon_runtime,
    )
    summary = {
        "kind": "stage1_convergence_summary",
        "workflow_family": workflow_family,
        "phonon_runtime_root": str(phonon_runtime),
        "combined_curve_summary": str(phonon_runtime / "qe_phonon_pes_run" / "param_tuning" / "combined_curve_summary.json"),
        "selected_profiles_json": str(phonon_runtime / "qe_phonon_pes_run" / "results" / "selected_profiles.json"),
    }
    dump_json(run_root / "stage1" / "convergence_summary.json", summary)
    return summary
