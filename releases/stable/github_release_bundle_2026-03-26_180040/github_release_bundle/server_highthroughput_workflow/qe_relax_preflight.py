#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
import re
import subprocess
import time
from pathlib import Path

try:
    from .scheduler import resolve_scheduler_mode, resolve_slurm_job_settings
except ImportError:
    from scheduler import resolve_scheduler_mode, resolve_slurm_job_settings


RELAX_SETTINGS = {
    "disk_io": "low",
    "verbosity": "low",
    "tprnfor": True,
    "tstress": True,
    "calculation": "vc-relax",
    "include_ions": True,
    "include_cell": True,
    "ion_dynamics": "bfgs",
    "cell_dynamics": "bfgs",
    "press_conv_thr": "0.1",
    "forc_conv_thr": "1.0d-5",
    "etot_conv_thr": "1.0d-10",
    "occupations": "smearing",
    "smearing": "gauss",
    "degauss": "1.0d-10",
    "ecutwfc": 120,
    "ecutrho": 1200,
    "electron_maxstep": 10000,
    "conv_thr": "1.0d-12",
    "mixing_mode": "plain",
    "mixing_beta": "0.3d0",
    "diagonalization": "david",
    "primitive_k_mesh": [12, 12, 1],
}

OPTIMIZED_SCF_SETTINGS = {
    "disk_io": "low",
    "verbosity": "low",
    "tprnfor": False,
    "tstress": False,
    "calculation": "scf",
    "include_ions": False,
    "include_cell": False,
    "forc_conv_thr": "1.0d-8",
    "etot_conv_thr": "1.0d-10",
    "occupations": "smearing",
    "smearing": "gauss",
    "degauss": "1.0d-10",
    "ecutwfc": 120,
    "ecutrho": 1200,
    "electron_maxstep": 10000,
    "conv_thr": "1.0d-12",
    "mixing_mode": "plain",
    "mixing_beta": "0.3d0",
    "diagonalization": "david",
    "primitive_k_mesh": [12, 12, 1],
}

RELAX_JOB_PREFIX = "qerelax"
RELAX_NTASKS = 24
RELAX_PARTITION = "long"
RELAX_QOS = None
RELAX_TIME = "24:00:00"
RELAX_POLL_SECONDS = 20
RELAX_HEARTBEAT_SECONDS = 60
RELAX_ENV_INIT_LINES = [
    "set +u",
    "source /opt/intel/oneapi/setvars.sh >/dev/null 2>&1 || true",
    "set -u",
]
LOCAL_RELAX_COMMAND = "mpirun -np {ntasks} pw.x < vc_relax.inp > vc_relax.out 2> vc_relax.err"


def parse_args():
    parser = argparse.ArgumentParser(description="Run the fixed QE vc-relax preflight used by the minimal release launcher.")
    parser.add_argument("--run-root", required=True, type=str)
    parser.add_argument("--structure", required=True, type=str)
    parser.add_argument("--pseudo-dir", required=True, type=str)
    parser.add_argument("--scheduler", default="auto", choices=["auto", "slurm", "local"])
    return parser.parse_args()


def dump_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def load_qe_template(scf_file: Path) -> dict:
    lines = scf_file.read_text().splitlines()

    nat = None
    nat_re = re.compile(r"\bnat\s*=\s*(\d+)\b", re.I)
    for line in lines:
        match = nat_re.search(line)
        if match:
            nat = int(match.group(1))
            break
    if nat is None:
        raise ValueError(f"Could not parse nat= from template: {scf_file}")

    cell_header = None
    for index, line in enumerate(lines):
        if line.strip().upper().startswith("CELL_PARAMETERS"):
            cell_header = index
            break
    if cell_header is None:
        raise ValueError(f"CELL_PARAMETERS not found in template: {scf_file}")

    cell = []
    for offset in range(1, 4):
        parts = lines[cell_header + offset].split()
        if len(parts) < 3:
            raise ValueError(f"Invalid CELL_PARAMETERS line in template: {scf_file}")
        cell.append([float(parts[0]), float(parts[1]), float(parts[2])])

    atom_header = None
    atom_crystal = False
    for index, line in enumerate(lines):
        upper = line.strip().upper()
        if upper.startswith("ATOMIC_POSITIONS"):
            atom_header = index
            atom_crystal = "CRYSTAL" in upper
            break
    if atom_header is None:
        raise ValueError(f"ATOMIC_POSITIONS not found in template: {scf_file}")
    if not atom_crystal:
        raise ValueError(f"Template ATOMIC_POSITIONS must be (crystal): {scf_file}")

    symbols = []
    frac = []
    constraints = []
    for index in range(atom_header + 1, atom_header + 1 + nat):
        parts = lines[index].split()
        if len(parts) < 4:
            raise ValueError(f"Invalid ATOMIC_POSITIONS line in template: {scf_file}")
        symbols.append(parts[0])
        frac.append([float(parts[1]), float(parts[2]), float(parts[3])])
        if len(parts) >= 7:
            constraints.append(f"{parts[4]}   {parts[5]}   {parts[6]}")
        else:
            constraints.append("0   0   0")

    k_points = None
    for index, line in enumerate(lines):
        upper = line.strip().upper()
        if upper.startswith("K_POINTS") and "AUTOMATIC" in upper:
            parts = lines[index + 1].split()
            if len(parts) < 3:
                raise ValueError(f"Invalid K_POINTS automatic line in template: {scf_file}")
            k_points = [int(parts[0]), int(parts[1]), int(parts[2])]
            break
    if k_points is None:
        raise ValueError(f"K_POINTS {{automatic}} not found in template: {scf_file}")

    return {
        "nat": nat,
        "cell": cell,
        "symbols": symbols,
        "frac": frac,
        "constraints": constraints,
        "k_points": k_points,
    }


def write_qe_input(
    out_file: Path,
    cell: list[list[float]],
    symbols: list[str],
    frac_positions: list[list[float]],
    constraints: list[str],
    k_mesh: list[int],
    pseudo_dir_rel: str,
    scf_settings: dict,
) -> None:
    nat = len(symbols)
    calculation = scf_settings.get("calculation", "scf")
    disk_io = scf_settings.get("disk_io", "low")
    verbosity = scf_settings.get("verbosity", "high")
    tprnfor = ".true." if scf_settings.get("tprnfor", True) else ".false."
    tstress = ".true." if scf_settings.get("tstress", True) else ".false."
    include_ions = bool(scf_settings.get("include_ions", False))
    include_cell = bool(scf_settings.get("include_cell", False))
    ion_dynamics = scf_settings.get("ion_dynamics", "bfgs")
    cell_dynamics = scf_settings.get("cell_dynamics", "bfgs")
    press_conv_thr = scf_settings.get("press_conv_thr", "0.1")

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w") as handle:
        handle.write("&CONTROL\n")
        handle.write(f"  calculation = '{calculation}'\n")
        handle.write(f"  disk_io = '{disk_io}'\n")
        handle.write("  prefix = 'pwscf'\n")
        handle.write(f"  pseudo_dir = '{pseudo_dir_rel}'\n")
        handle.write("  outdir = './tmp'\n")
        handle.write(f"  verbosity = '{verbosity}'\n")
        handle.write(f"  tprnfor = {tprnfor}\n")
        handle.write(f"  tstress = {tstress}\n")
        handle.write(f"  forc_conv_thr = {scf_settings['forc_conv_thr']}\n")
        if scf_settings.get("etot_conv_thr"):
            handle.write(f"  etot_conv_thr = {scf_settings['etot_conv_thr']}\n")
        handle.write("/\n\n")

        handle.write("&SYSTEM\n")
        handle.write("  ibrav = 0\n")
        handle.write(f"  nat = {nat}, ntyp = 2\n")
        if scf_settings.get("occupations") == "smearing":
            handle.write(
                "  occupations = 'smearing', "
                f"smearing = '{scf_settings['smearing']}', degauss = {scf_settings['degauss']}\n"
            )
        else:
            handle.write(f"  occupations = '{scf_settings['occupations']}'\n")
        handle.write(f"  ecutwfc = {scf_settings['ecutwfc']}, ecutrho = {scf_settings['ecutrho']}\n")
        handle.write("/\n\n")

        handle.write("&ELECTRONS\n")
        handle.write(f"  electron_maxstep = {scf_settings['electron_maxstep']}\n")
        handle.write(f"  conv_thr = {scf_settings['conv_thr']}\n")
        handle.write(f"  mixing_mode = '{scf_settings['mixing_mode']}'\n")
        handle.write(f"  mixing_beta = {scf_settings['mixing_beta']}\n")
        handle.write(f"  diagonalization = '{scf_settings['diagonalization']}'\n")
        handle.write("/\n\n")

        if include_ions:
            handle.write("&IONS\n")
            handle.write(f"  ion_dynamics = '{ion_dynamics}'\n")
            handle.write("/\n\n")

        if include_cell:
            handle.write("&CELL\n")
            handle.write(f"  cell_dynamics = '{cell_dynamics}'\n")
            handle.write(f"  press_conv_thr = {press_conv_thr}\n")
            handle.write("/\n\n")

        handle.write("ATOMIC_SPECIES\n")
        handle.write("W  183.84 W.pz-spn-rrkjus_psl.1.0.0.UPF\n")
        handle.write("Se 78.960 Se.pz-n-rrkjus_psl.0.2.UPF\n\n")

        handle.write("CELL_PARAMETERS (angstrom)\n")
        for row in cell:
            handle.write(f"   {row[0]:.9f}   {row[1]:.9f}   {row[2]:.9f}\n")

        handle.write("\nATOMIC_POSITIONS (crystal)\n")
        for index, symbol in enumerate(symbols):
            position = frac_positions[index]
            constraint = constraints[index] if constraints else "0   0   0"
            handle.write(f"{symbol:<4}   {position[0]:.10f}   {position[1]:.10f}   {position[2]:.10f}   {constraint}\n")

        handle.write("\nK_POINTS {automatic}\n")
        handle.write(f"{k_mesh[0]} {k_mesh[1]} {k_mesh[2]} 0 0 0\n")


def relpath(from_dir: Path, to_path: Path) -> str:
    return os.path.relpath(str(to_path), start=str(from_dir))


def file_contains_job_done(path: Path) -> bool:
    if not path.exists():
        return False
    return "JOB DONE" in path.read_text(errors="ignore")


def extract_energy_ry(scf_out: Path) -> float | None:
    if not scf_out.exists():
        return None
    for line in reversed(scf_out.read_text(errors="ignore").splitlines()):
        if "total energy" in line and line.lstrip().startswith("!"):
            match = re.search(r"=\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)", line)
            if match:
                return float(match.group(1))
    return None


def extract_total_force_ry_bohr(scf_out: Path) -> float | None:
    if not scf_out.exists():
        return None
    result = None
    for line in scf_out.read_text(errors="ignore").splitlines():
        match = re.search(r"Total force\s*=\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)", line)
        if match:
            result = float(match.group(1))
    return result


def extract_max_atomic_force_ry_bohr(scf_out: Path) -> float | None:
    if not scf_out.exists():
        return None
    current = []
    last_complete = None
    for line in scf_out.read_text(errors="ignore").splitlines():
        match = re.search(
            r"force\s*=\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)",
            line,
        )
        if match:
            fx, fy, fz = float(match.group(1)), float(match.group(2)), float(match.group(3))
            current.append(math.sqrt(fx * fx + fy * fy + fz * fz))
        elif current:
            last_complete = current[:]
            current = []
    if current:
        last_complete = current[:]
    if not last_complete:
        return None
    return max(last_complete)


def parse_time_to_seconds(text: str | None) -> float | None:
    if text is None:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    match = re.fullmatch(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:([0-9]+(?:\.[0-9]*)?)s)?", cleaned)
    if not match:
        return None
    days = 0 if match.group(1) is None else int(match.group(1))
    hours = 0 if match.group(2) is None else int(match.group(2))
    minutes = 0 if match.group(3) is None else int(match.group(3))
    seconds = 0.0 if match.group(4) is None else float(match.group(4))
    return float((((days * 24) + hours) * 60 + minutes) * 60 + seconds)


def extract_wall_sec(scf_out: Path) -> float | None:
    if not scf_out.exists():
        return None
    result = None
    for line in scf_out.read_text(errors="ignore").splitlines():
        match = re.search(r"PWSCF\s*:\s*.*?CPU\s+(.*?)\s+WALL", line)
        if match:
            result = parse_time_to_seconds(match.group(1))
    return result


def extract_final_relaxed_structure(scf_out: Path, nat: int) -> dict | None:
    if not scf_out.exists():
        return None
    lines = scf_out.read_text(errors="ignore").splitlines()
    last_cell = None
    last_frac = None
    for index, line in enumerate(lines):
        upper = line.strip().upper()
        if upper.startswith("CELL_PARAMETERS"):
            try:
                cell = []
                for offset in range(1, 4):
                    parts = lines[index + offset].split()
                    cell.append([float(parts[0]), float(parts[1]), float(parts[2])])
                last_cell = cell
            except Exception:
                pass
        if upper.startswith("ATOMIC_POSITIONS") and "CRYSTAL" in upper:
            try:
                symbols = []
                frac = []
                constraints = []
                for offset in range(1, nat + 1):
                    parts = lines[index + offset].split()
                    symbols.append(parts[0])
                    frac.append([float(parts[1]), float(parts[2]), float(parts[3])])
                    if len(parts) >= 7:
                        constraints.append(f"{parts[4]}   {parts[5]}   {parts[6]}")
                    else:
                        constraints.append("0   0   0")
                last_frac = {
                    "symbols": symbols,
                    "frac": frac,
                    "constraints": constraints,
                }
            except Exception:
                pass
    if last_cell is None or last_frac is None:
        return None
    return {
        "cell": last_cell,
        "symbols": last_frac["symbols"],
        "frac": last_frac["frac"],
        "constraints": last_frac["constraints"],
    }


def parse_sbatch_job_id(stdout: str) -> str | None:
    match = re.search(r"Submitted batch job\s+(\d+)", stdout)
    return None if match is None else match.group(1)


def squeue_existing_job_ids(job_ids: list[str]) -> set[str]:
    if not job_ids:
        return set()
    job_arg = ",".join(job_ids)
    try:
        result = subprocess.run(["squeue", "-h", "-j", job_arg, "-o", "%i"], capture_output=True, text=True, check=True)
    except Exception:
        return set()
    text = result.stdout.strip()
    return set() if not text else {line.strip() for line in text.splitlines() if line.strip()}


def _status_path(relax_root: Path) -> Path:
    return relax_root / "job_status.json"


def _summary_path(relax_root: Path) -> Path:
    return relax_root / "relax_summary.json"


def _optimized_structure_path(relax_root: Path) -> Path:
    return relax_root / "optimized_structure.scf.inp"


def _write_status(relax_root: Path, payload: dict) -> None:
    dump_json(_status_path(relax_root), payload)


def _prepare_relax_input(relax_root: Path, structure_path: Path, pseudo_dir: Path) -> dict:
    template = load_qe_template(structure_path)
    primitive_k_mesh = list(RELAX_SETTINGS.get("primitive_k_mesh") or template["k_points"])
    write_qe_input(
        out_file=relax_root / "vc_relax.inp",
        cell=template["cell"],
        symbols=template["symbols"],
        frac_positions=template["frac"],
        constraints=template["constraints"],
        k_mesh=primitive_k_mesh,
        pseudo_dir_rel=relpath(relax_root, pseudo_dir.resolve()),
        scf_settings=RELAX_SETTINGS,
    )
    manifest = {
        "kind": "minimal_release_qe_relax",
        "input_structure": str(structure_path.resolve()),
        "pseudo_dir": str(pseudo_dir.resolve()),
        "output_root": str(relax_root.resolve()),
        "scheduler_mode": None,
        "requested_slurm": {
            "partition": RELAX_PARTITION,
            "walltime": RELAX_TIME,
            "qos": RELAX_QOS,
        },
        "relax_settings": RELAX_SETTINGS,
    }
    dump_json(relax_root / "relax_manifest.json", manifest)
    return template


def _write_submit_script(relax_root: Path, slurm_settings: dict) -> Path:
    submit_path = relax_root / "submit.sh"
    lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name={RELAX_JOB_PREFIX}",
        "#SBATCH --nodes=1",
        f"#SBATCH --ntasks={RELAX_NTASKS}",
        f"#SBATCH --time={slurm_settings['walltime']}",
        f"#SBATCH --partition={slurm_settings['partition']}",
        f"#SBATCH --chdir={relax_root}",
        "#SBATCH --output=slurm-%j.out",
        "#SBATCH --error=slurm-%j.err",
        "",
        "set -euo pipefail",
    ]
    if slurm_settings.get("qos"):
        lines.insert(6, f"#SBATCH --qos={slurm_settings['qos']}")
    lines.extend(RELAX_ENV_INIT_LINES)
    lines.extend(
        [
            f"cd {relax_root}",
            "mkdir -p tmp",
            LOCAL_RELAX_COMMAND.format(ntasks=RELAX_NTASKS),
            "",
        ]
    )
    submit_path.write_text("\n".join(lines))
    submit_path.chmod(0o755)
    return submit_path


def _collect_summary(relax_root: Path, template: dict, pseudo_dir: Path, structure_path: Path, scheduler_mode: str) -> dict:
    final_structure = extract_final_relaxed_structure(relax_root / "vc_relax.out", template["nat"])
    if final_structure is None:
        raise RuntimeError("QE relax finished but the final relaxed structure could not be parsed from vc_relax.out.")

    optimized_structure = _optimized_structure_path(relax_root)
    write_qe_input(
        out_file=optimized_structure,
        cell=final_structure["cell"],
        symbols=final_structure["symbols"],
        frac_positions=final_structure["frac"],
        constraints=final_structure["constraints"],
        k_mesh=list(OPTIMIZED_SCF_SETTINGS.get("primitive_k_mesh") or template["k_points"]),
        pseudo_dir_rel=relpath(relax_root, pseudo_dir.resolve()),
        scf_settings=OPTIMIZED_SCF_SETTINGS,
    )

    summary = {
        "scheduler_mode": scheduler_mode,
        "input_structure": str(structure_path.resolve()),
        "optimized_structure": str(optimized_structure.resolve()),
        "vc_relax_output": str((relax_root / "vc_relax.out").resolve()),
        "final_energy_ry": extract_energy_ry(relax_root / "vc_relax.out"),
        "final_total_force_ry_bohr": extract_total_force_ry_bohr(relax_root / "vc_relax.out"),
        "final_max_atomic_force_ry_bohr": extract_max_atomic_force_ry_bohr(relax_root / "vc_relax.out"),
        "wall_sec": extract_wall_sec(relax_root / "vc_relax.out"),
        "job_done": file_contains_job_done(relax_root / "vc_relax.out"),
        "final_structure": final_structure,
    }
    dump_json(_summary_path(relax_root), summary)
    _write_status(relax_root, {"state": "completed"})
    return summary


def _run_local(relax_root: Path, emit) -> None:
    emit("Running QE pre-relax locally.")
    _write_status(relax_root, {"state": "running", "mode": "local", "start_time_epoch": time.time()})
    command = LOCAL_RELAX_COMMAND.format(ntasks=RELAX_NTASKS)
    start_epoch = time.time()
    last_heartbeat_epoch = start_epoch
    process = subprocess.Popen(command, cwd=str(relax_root), shell=True, text=True)
    while True:
        return_code = process.poll()
        if return_code is not None:
            if return_code != 0:
                raise RuntimeError(
                    "Local QE relax failed. Check "
                    f"{relax_root / 'vc_relax.out'} and {relax_root / 'vc_relax.err'}."
                )
            break
        now = time.time()
        if now - last_heartbeat_epoch >= RELAX_HEARTBEAT_SECONDS:
            emit(f"QE pre-relax still running locally ({int(now - start_epoch)}s elapsed).")
            last_heartbeat_epoch = now
        time.sleep(min(5, RELAX_HEARTBEAT_SECONDS))
    if process.returncode != 0:
        raise RuntimeError(
            "Local QE relax failed. Check "
            f"{relax_root / 'vc_relax.out'} and {relax_root / 'vc_relax.err'}."
        )
    emit("Local QE pre-relax finished.")


def _run_slurm(relax_root: Path, emit) -> str:
    slurm_settings = resolve_slurm_job_settings(
        "qe_relax",
        requested_partition=RELAX_PARTITION,
        requested_walltime=RELAX_TIME,
        requested_qos=RELAX_QOS,
    )
    emit(
        "QE pre-relax Slurm settings: "
        f"partition={slurm_settings['partition']}, walltime={slurm_settings['walltime']}, qos={slurm_settings.get('qos')}."
    )
    for note in slurm_settings.get("notes", []):
        emit(f"QE pre-relax note: {note}")
    submit_path = _write_submit_script(relax_root, slurm_settings)
    result = subprocess.run(["sbatch", str(submit_path)], capture_output=True, text=True)
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if result.returncode != 0:
        raise RuntimeError(f"sbatch failed for QE relax:\n{stdout}\n{stderr}")
    job_id = parse_sbatch_job_id(stdout)
    if not job_id:
        raise RuntimeError(f"Could not parse QE relax job id: {stdout}")
    _write_status(
        relax_root,
        {
            "state": "submitted",
            "mode": "slurm",
            "job_id": job_id,
            "sbatch_stdout": stdout,
            "sbatch_stderr": stderr,
            "submit_time_epoch": time.time(),
            "slurm_settings": slurm_settings,
        },
    )
    emit(f"Submitted QE pre-relax job {job_id}.")
    last_state = None
    last_heartbeat_epoch = 0.0
    start_epoch = time.time()
    while True:
        if file_contains_job_done(relax_root / "vc_relax.out"):
            emit("QE pre-relax finished.")
            return job_id
        active = job_id in squeue_existing_job_ids([job_id])
        state = "running" if active else "submitted"
        _write_status(relax_root, {"state": state, "mode": "slurm", "job_id": job_id})
        if state != last_state:
            emit(f"QE pre-relax state: {state}.")
            last_state = state
            last_heartbeat_epoch = time.time()
        now = time.time()
        if active and now - last_heartbeat_epoch >= RELAX_HEARTBEAT_SECONDS:
            elapsed_sec = int(now - start_epoch)
            emit(f"QE pre-relax still running (job {job_id}, elapsed {elapsed_sec}s).")
            last_heartbeat_epoch = now
        if not active:
            raise RuntimeError("QE relax stalled without JOB DONE in vc_relax.out.")
        time.sleep(RELAX_POLL_SECONDS)


def run_qe_relax(run_root: Path, structure_path: Path, pseudo_dir: Path, scheduler: str = "auto", emit=None) -> dict:
    run_root = Path(run_root).expanduser().resolve()
    structure_path = Path(structure_path).expanduser().resolve()
    pseudo_dir = Path(pseudo_dir).expanduser().resolve()
    relax_root = run_root / "pre_relax"
    relax_root.mkdir(parents=True, exist_ok=True)
    emit = (lambda _message: None) if emit is None else emit

    summary_path = _summary_path(relax_root)
    optimized_structure = _optimized_structure_path(relax_root)
    if summary_path.exists() and optimized_structure.exists():
        emit("Reusing existing QE pre-relax outputs.")
        return json.loads(summary_path.read_text())

    scheduler_mode = resolve_scheduler_mode(scheduler)
    emit(f"QE pre-relax scheduler mode: {scheduler_mode}.")
    template = _prepare_relax_input(relax_root, structure_path, pseudo_dir)

    manifest_path = relax_root / "relax_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["scheduler_mode"] = scheduler_mode
    dump_json(manifest_path, manifest)

    if scheduler_mode == "slurm":
        _run_slurm(relax_root, emit)
    else:
        _run_local(relax_root, emit)
        if not file_contains_job_done(relax_root / "vc_relax.out"):
            raise RuntimeError("Local QE relax finished without JOB DONE in vc_relax.out.")

    return _collect_summary(relax_root, template, pseudo_dir, structure_path, scheduler_mode)


def main() -> None:
    args = parse_args()
    summary = run_qe_relax(
        run_root=Path(args.run_root),
        structure_path=Path(args.structure),
        pseudo_dir=Path(args.pseudo_dir),
        scheduler=args.scheduler,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
