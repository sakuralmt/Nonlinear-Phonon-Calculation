#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path

from common import (
    canonicalize_q,
    dump_json,
    ensure_dir,
    guess_point_label,
    load_qe_template,
    prepare_primitive_qe_input,
    q_key,
    relpath,
    resolve_active_profile,
    resolve_structure_template,
    snap_q_to_grid,
)
from config import (
    FILDYN_PREFIX,
    FORCE_CONSTANT_FILE,
    FRONTEND_ROOT,
    MATDYN_COMMAND,
    MATDYN_EIG_FILE,
    MATDYN_FREQ_FILE,
    MATDYN_ROOT,
    MODULE_LINES,
    PHONON_JOB_PREFIX,
    PHONON_Q_GRID,
    PHONON_ACTIVE_PROFILE_LEVEL,
    PHONON_STRICT_PRESET_NAME,
    OPTIMIZED_SCF_TEMPLATE,
    PSEUDO_DIR,
    REQUESTS_JSON,
    RAW_SCF_TEMPLATE,
    RUN_ROOT,
    SCF_TEMPLATE,
    SELECTED_PROFILES_JSON,
)
from scf_settings import resolve_scf_settings
from stage1_env import (
    STAGE1_ENV_ASSESSMENT_JSON,
    STAGE1_RUNTIME_CONFIG_JSON,
    build_runtime_signature,
    ensure_stage1_environment_assessed,
)


def _load_requests() -> list[dict]:
    payload = json.loads(REQUESTS_JSON.read_text())
    requests = list(payload.get('requests', []))
    if not requests:
        raise RuntimeError(f'No requests found in {REQUESTS_JSON}')
    return requests


def _validate_requested_q(q: list[float]) -> list[float]:
    snapped = snap_q_to_grid(q, PHONON_Q_GRID[0])
    for value in snapped:
        target = round(value * PHONON_Q_GRID[0]) / float(PHONON_Q_GRID[0])
        if abs(value - target) > 1.0e-8:
            raise ValueError(f'q={q} is not on the fixed {PHONON_Q_GRID[0]}x{PHONON_Q_GRID[1]}x{PHONON_Q_GRID[2]} grid')
    if abs(snapped[2]) > 1.0e-8:
        raise ValueError(f'q={q} has non-zero z component, not supported in this bundle')
    return snapped


def _requested_q_points(requests: list[dict]) -> list[list[float]]:
    out = [[0.0, 0.0, 0.0]]
    seen = {q_key(out[0])}
    for item in requests:
        q = _validate_requested_q(item['target_q_frac'])
        key = q_key(q)
        if key in seen:
            continue
        seen.add(key)
        out.append(q)
    return out


def _write_stage_submit(stage_dir: Path, job_name: str, command: str, stdin_name: str, stdout_name: str, slurm_settings: dict):
    lines = [
        '#!/bin/bash',
        f'#SBATCH --job-name={job_name}',
        f'#SBATCH -N {slurm_settings["nodes"]}',
        f'#SBATCH --ntasks-per-node={slurm_settings["ntasks_per_node"]}',
        f'#SBATCH --time={slurm_settings["walltime"]}',
        f'#SBATCH -p {slurm_settings["partition"]}',
        f'#SBATCH --chdir={stage_dir}',
        '#SBATCH --output=slurm-%j.out',
        '#SBATCH --error=slurm-%j.err',
    ]
    if slurm_settings.get("qos"):
        lines.append(f'#SBATCH --qos={slurm_settings["qos"]}')
    lines.extend([
        '',
        'ulimit -s unlimited',
        'ulimit -c unlimited',
    ])
    lines.extend(MODULE_LINES)
    lines.extend([
        '',
        f'cd {stage_dir}',
        'export OMP_NUM_THREADS=1',
        'export MKL_NUM_THREADS=1',
        'export OPENBLAS_NUM_THREADS=1',
        'export NUMEXPR_NUM_THREADS=1',
        'rm -f TDPWSTOP',
        f'{command} < {stdin_name} > {stdout_name}',
        '',
        'exit',
        '',
    ])
    path = stage_dir / 'submit.sh'
    path.write_text('\n'.join(lines))
    path.chmod(0o755)


def _write_ph_input(stage_dir: Path, prefix: str):
    recover_raw = str(os.environ.get("QIYAN_STAGE1_PH_RECOVER", "true")).strip().lower()
    recover_value = ".false." if recover_raw in {"0", "false", "no", "off"} else ".true."
    path = stage_dir / 'ph.in'
    path.write_text(
        "phonons of WSe2 on 6x6x1 grid\n"
        "&INPUTPH\n"
        f"prefix='{prefix}'\n"
        "outdir='../pw_stage/tmp'\n"
        "amass(1)=183.84\n"
        "amass(2)=78.960\n"
        "tr2_ph=1.0d-15\n"
        "alpha_mix(1)=0.3\n"
        "ldisp=.true.\n"
        f"nq1={PHONON_Q_GRID[0]}, nq2={PHONON_Q_GRID[1]}, nq3={PHONON_Q_GRID[2]}\n"
        f"fildyn='{FILDYN_PREFIX}'\n"
        f"recover={recover_value}\n"
        "/\n"
    )
    return path


def _write_q2r_input(stage_dir: Path):
    path = stage_dir / 'q2r.in'
    path.write_text(
        "&input\n"
        f"fildyn='../ph_stage/{FILDYN_PREFIX}'\n"
        f"flfrc='{FORCE_CONSTANT_FILE}'\n"
        "zasr='simple'\n"
        "/\n"
    )
    return path


def _write_matdyn_input(stage_dir: Path, q_points: list[list[float]]):
    path = stage_dir / 'matdyn.inp'
    lines = [
        '&input',
        "  asr = 'simple'",
        f"  flfrc='../phonon_frontend/q2r_stage/{FORCE_CONSTANT_FILE}'",
        f"  flfrq = '{MATDYN_FREQ_FILE}'",
        f"  fleig = '{MATDYN_EIG_FILE}'",
        '  q_in_cryst_coord=.true.',
        '/',
        str(len(q_points)),
    ]
    for q in q_points:
        lines.append(f'{q[0]:.10f}  {q[1]:.10f}  {q[2]:.10f}')
    path.write_text('\n'.join(lines) + '\n')
    return path


def prepare_frontend() -> Path:
    requests = _load_requests()
    q_points = _requested_q_points(requests)
    runtime_assessment = ensure_stage1_environment_assessed()
    stage_runtime = runtime_assessment["stages"]
    pw_runtime = stage_runtime["pw"]
    ph_runtime = stage_runtime["ph"]
    q2r_runtime = stage_runtime["q2r"]
    matdyn_runtime = stage_runtime["matdyn"]

    ensure_dir(FRONTEND_ROOT)
    ensure_dir(MATDYN_ROOT)

    pw_stage = FRONTEND_ROOT / 'pw_stage'
    ph_stage = FRONTEND_ROOT / 'ph_stage'
    q2r_stage = FRONTEND_ROOT / 'q2r_stage'
    for path in [pw_stage, ph_stage, q2r_stage, MATDYN_ROOT]:
        ensure_dir(path)

    structure_template = resolve_structure_template(OPTIMIZED_SCF_TEMPLATE, RAW_SCF_TEMPLATE)
    fallback_settings = resolve_scf_settings(PHONON_STRICT_PRESET_NAME)
    phonon_settings = resolve_active_profile(
        selected_profiles_path=SELECTED_PROFILES_JSON,
        branch='phonon',
        level=PHONON_ACTIVE_PROFILE_LEVEL,
        fallback_settings=fallback_settings,
    )
    template_meta = load_qe_template(structure_template)
    primitive_k_mesh = phonon_settings.get('primitive_k_mesh') or template_meta['k_points']

    primitive_meta = prepare_primitive_qe_input(
        structure_template,
        pw_stage / 'scf.inp',
        relpath(pw_stage, PSEUDO_DIR.resolve()),
        phonon_settings,
        k_mesh=list(primitive_k_mesh),
    )
    prefix = primitive_meta['prefix']

    _write_stage_submit(
        pw_stage,
        f'{PHONON_JOB_PREFIX}_pw',
        pw_runtime["command"],
        'scf.inp',
        'scf.out',
        pw_runtime["slurm_settings"],
    )
    _write_ph_input(ph_stage, prefix)
    _write_stage_submit(
        ph_stage,
        f'{PHONON_JOB_PREFIX}_ph',
        ph_runtime["command"],
        'ph.in',
        'ph.out',
        ph_runtime["slurm_settings"],
    )
    _write_q2r_input(q2r_stage)
    _write_stage_submit(
        q2r_stage,
        f'{PHONON_JOB_PREFIX}_q2r',
        q2r_runtime["command"],
        'q2r.in',
        'q2r.out',
        q2r_runtime["slurm_settings"],
    )
    _write_matdyn_input(MATDYN_ROOT, q_points)
    _write_stage_submit(
        MATDYN_ROOT,
        f'{PHONON_JOB_PREFIX}_matdyn',
        matdyn_runtime["command"],
        'matdyn.inp',
        'matdyn.out',
        matdyn_runtime["slurm_settings"],
    )

    manifest = {
        'kind': 'qe_phonon_frontend',
        'root': str(FRONTEND_ROOT),
        'matdyn_root': str(MATDYN_ROOT),
        'requests_json': str(REQUESTS_JSON),
        'prefix': prefix,
        'source_structure_template': str(structure_template),
        'phonon_profile_level': PHONON_ACTIVE_PROFILE_LEVEL,
        'phonon_profile_settings': phonon_settings,
        'runtime_signature': build_runtime_signature(runtime_assessment),
        'runtime_assessment_json': str(STAGE1_ENV_ASSESSMENT_JSON),
        'runtime_config_json': str(STAGE1_RUNTIME_CONFIG_JSON),
        'slurm_settings': {
            'pw': pw_runtime["slurm_settings"],
            'ph': ph_runtime["slurm_settings"],
            'q2r': q2r_runtime["slurm_settings"],
            'matdyn': matdyn_runtime["slurm_settings"],
        },
        'q_grid': list(PHONON_Q_GRID),
        'matdyn_q_points': q_points,
        'requested_points': [
            {
                'request_id': item['request_id'],
                'point_label': item.get('point_label') or guess_point_label(item['target_q_frac']),
                'target_q_frac': canonicalize_q(item['target_q_frac']),
            }
            for item in requests
        ],
        'stages': [
            {'name': 'pw', 'stage_dir': str(pw_stage), 'submit_script': str(pw_stage / 'submit.sh')},
            {'name': 'ph', 'stage_dir': str(ph_stage), 'submit_script': str(ph_stage / 'submit.sh')},
            {'name': 'q2r', 'stage_dir': str(q2r_stage), 'submit_script': str(q2r_stage / 'submit.sh')},
            {'name': 'matdyn', 'stage_dir': str(MATDYN_ROOT), 'submit_script': str(MATDYN_ROOT / 'submit.sh')},
        ],
    }
    dump_json(RUN_ROOT / 'frontend_manifest.json', manifest)
    print(f'prepared frontend: {RUN_ROOT / "frontend_manifest.json"}')
    return RUN_ROOT / 'frontend_manifest.json'


if __name__ == '__main__':
    from config import RUN_ROOT
    ensure_dir(RUN_ROOT)
    prepare_frontend()
