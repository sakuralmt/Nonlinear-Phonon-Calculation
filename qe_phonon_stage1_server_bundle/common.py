#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import re
import subprocess
import shutil
from pathlib import Path


# Atomic masses (amu) for the WSe2 template used in this workflow.
MASS_DICT = {"W": 183.84, "Se": 78.960}

RY_TO_EV = 13.605693009

# Unit conversion for frequencies when Q is in Angstrom*sqrt(amu) and E in eV.
CONV_TO_THZ = 15.63330423985619
CONV_TO_CM1 = 521.4708983725064


def dump_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2))


def slurm_available() -> bool:
    return shutil.which("sbatch") is not None and shutil.which("squeue") is not None


def _run_capture(cmd: list[str]):
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return {"returncode": result.returncode, "stdout": result.stdout, "stderr": result.stderr}


def _tokenize_partition_line(line: str):
    return dict(re.findall(r"(\w+)=([^\s]+)", line))


def _normalize_partition_name(name: str) -> str:
    return str(name).rstrip("*")


def _parse_time_to_seconds(raw: str | None):
    if raw is None:
        return None
    text = str(raw).strip()
    if text.lower() in {"", "infinite", "unlimited", "none", "partition_limit"}:
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
    else:
        hours = 0
        minutes = int(parts[0])
        seconds = 0
    return int((((days * 24) + hours) * 60 + minutes) * 60 + seconds)


def _usable_state(state: str) -> bool:
    lowered = str(state).lower().strip("*")
    return not lowered.startswith(("down", "drain", "drng", "inval", "inact"))


def resolve_frontend_slurm_settings(
    requested_partition: str,
    requested_walltime: str,
    requested_qos: str | None,
    requested_nodes: int,
    requested_ntasks_per_node: int,
):
    if not slurm_available():
        raise RuntimeError("Slurm is unavailable on this machine.")

    scontrol = _run_capture(["scontrol", "show", "partition", "-o"])
    if scontrol["returncode"] != 0:
        raise RuntimeError(f"Failed to inspect Slurm partitions:\n{scontrol['stderr'].strip()}")

    partitions = {}
    default_partition = None
    for line in scontrol["stdout"].splitlines():
        if not line.strip():
            continue
        tokens = _tokenize_partition_line(line)
        name = _normalize_partition_name(tokens["PartitionName"])
        partitions[name] = {
            "name": name,
            "is_default": str(tokens.get("Default", "NO")).upper() == "YES",
            "max_time_raw": tokens.get("MaxTime"),
            "max_time_seconds": _parse_time_to_seconds(tokens.get("MaxTime")),
        }
        if partitions[name]["is_default"]:
            default_partition = name

    sinfo_nodes = _run_capture(["scontrol", "show", "node", "-o"])
    node_rows = []
    if sinfo_nodes["returncode"] == 0:
        for line in sinfo_nodes["stdout"].splitlines():
            if not line.strip():
                continue
            node_name_match = re.search(r"\bNodeName=([^\s]+)", line)
            partitions_match = re.search(r"\bPartitions=([^\s]+)", line)
            cpu_total_match = re.search(r"\bCPUTot=(\d+)", line)
            cpu_alloc_match = re.search(r"\bCPUAlloc=(\d+)", line)
            state_match = re.search(r"\bState=([^\s]+)", line)
            if None in {node_name_match, partitions_match, cpu_total_match, cpu_alloc_match, state_match}:
                continue
            node_name = node_name_match.group(1)
            cpu_total = int(cpu_total_match.group(1))
            cpu_alloc = int(cpu_alloc_match.group(1))
            state = state_match.group(1)
            for partition_name in partitions_match.group(1).split(","):
                node_rows.append(
                    {
                        "partition": _normalize_partition_name(partition_name),
                        "node": node_name,
                        "cpus": cpu_total,
                        "idle_cpus": max(0, cpu_total - cpu_alloc),
                        "state": state,
                    }
                )

    chosen = partitions.get(_normalize_partition_name(requested_partition))
    notes: list[str] = []
    requested_seconds = _parse_time_to_seconds(requested_walltime)
    if chosen is None:
        notes.append(f"requested partition '{requested_partition}' is unavailable")
        chosen = partitions.get(default_partition) if default_partition else None
        if chosen is not None:
            notes.append(f"falling back to default partition '{chosen['name']}'")
    if chosen is None:
        chosen = next(iter(partitions.values()))
        notes.append(f"falling back to first detected partition '{chosen['name']}'")

    max_seconds = chosen.get("max_time_seconds")
    walltime = requested_walltime
    if requested_seconds is not None and max_seconds is not None and requested_seconds > max_seconds:
        walltime = chosen["max_time_raw"]
        notes.append(f"clamped walltime to partition max_time {walltime}")

    usable_nodes = [
        row for row in node_rows
        if row["partition"] == chosen["name"] and _usable_state(row["state"])
    ]
    available_node_count = max(1, len(usable_nodes))
    cpus_per_node = min((row["cpus"] for row in usable_nodes), default=max(1, requested_ntasks_per_node))
    idle_caps = [int(row.get("idle_cpus", 0)) for row in usable_nodes if int(row.get("idle_cpus", 0)) > 0]
    if idle_caps:
        current_idle_cap = max(1, min(idle_caps))
        if current_idle_cap < cpus_per_node:
            cpus_per_node = current_idle_cap
            notes.append(f"capped ntasks-per-node to current idle cpus {current_idle_cap}")
    nodes = max(1, min(int(requested_nodes), available_node_count))
    ntasks_per_node = max(1, min(int(requested_ntasks_per_node), int(cpus_per_node)))
    total_tasks = nodes * ntasks_per_node
    if nodes != requested_nodes:
        notes.append(f"reduced nodes from {requested_nodes} to {nodes}")
    if ntasks_per_node != requested_ntasks_per_node:
        notes.append(f"reduced ntasks-per-node from {requested_ntasks_per_node} to {ntasks_per_node}")

    return {
        "partition": chosen["name"],
        "walltime": walltime,
        "qos": requested_qos,
        "nodes": nodes,
        "ntasks_per_node": ntasks_per_node,
        "total_tasks": total_tasks,
        "notes": notes,
    }


def canonicalize_q(q, tol: float = 1.0e-8) -> list[float]:
    out = [float(v) - math.floor(float(v)) for v in q]
    for i, value in enumerate(out):
        if abs(value) < tol or abs(value - 1.0) < tol:
            out[i] = 0.0
    return out


def q_equiv_delta_frac(q1, q2) -> list[float]:
    out = []
    for a, b in zip(q1, q2):
        d = float(a) - float(b)
        out.append(d - round(d))
    return out


def q_distance_frac(q1, q2) -> float:
    d = q_equiv_delta_frac(q1, q2)
    return math.sqrt(sum(v * v for v in d))


def snap_q_to_grid(q, grid_n: int, tol: float = 1.0e-8) -> list[float]:
    out = canonicalize_q(q, tol=tol)
    for i, value in enumerate(out):
        snapped = round(value * grid_n) / float(grid_n)
        if abs(value - snapped) < tol:
            out[i] = snapped
    return canonicalize_q(out, tol=tol)


def q_key(q, digits: int = 10) -> tuple[float, float, float]:
    qq = canonicalize_q(q)
    return tuple(round(float(v), digits) for v in qq)


def minus_q(q) -> list[float]:
    return canonicalize_q([-float(v) for v in q])


def is_self_conjugate_q(q, tol: float = 1.0e-8) -> bool:
    return q_distance_frac(q, minus_q(q)) < tol


def guess_point_label(q, tol: float = 1.0e-6) -> str:
    qq = canonicalize_q(q, tol=tol)
    if q_distance_frac(qq, [0.0, 0.0, 0.0]) < tol:
        return "Gamma"
    if q_distance_frac(qq, [0.5, 0.0, 0.0]) < tol or q_distance_frac(qq, [0.0, 0.5, 0.0]) < tol or q_distance_frac(qq, [0.5, 0.5, 0.0]) < tol:
        return "M"
    if q_distance_frac(qq, [1.0 / 3.0, 1.0 / 3.0, 0.0]) < tol or q_distance_frac(qq, [2.0 / 3.0, 1.0 / 3.0, 0.0]) < tol or q_distance_frac(qq, [1.0 / 3.0, 2.0 / 3.0, 0.0]) < tol:
        return "K"
    return "line"


def decode_complex_mode(mode) -> list[list[complex]]:
    """Decode eigenvector from our JSON into per-atom complex cart vectors."""
    out: list[list[complex]] = []
    for vec in mode:
        if isinstance(vec, dict):
            out.append(
                [
                    float(vec["x"]["re"]) + 1j * float(vec["x"]["im"]),
                    float(vec["y"]["re"]) + 1j * float(vec["y"]["im"]),
                    float(vec["z"]["re"]) + 1j * float(vec["z"]["im"]),
                ]
            )
        else:
            # Legacy encoding: [[[re, im], [re, im], [re, im]], ...]
            out.append([float(c[0]) + 1j * float(c[1]) for c in vec])
    return out


def encode_complex_mode(mode) -> list[dict]:
    out = []
    for vec in mode:
        out.append(
            {
                "x": {"re": float(vec[0].real), "im": float(vec[0].imag)},
                "y": {"re": float(vec[1].real), "im": float(vec[1].imag)},
                "z": {"re": float(vec[2].real), "im": float(vec[2].imag)},
            }
        )
    return out


def infer_commensurate_supercell_n(q_frac, n_max: int = 12, tol: float = 1.0e-8) -> int:
    """Find smallest n such that n*q is integer (2D only, z must be 0)."""
    q = [float(v) for v in q_frac]
    q = [v - math.floor(v) for v in q]

    def _snap(x: float) -> float:
        if abs(x) < tol:
            return 0.0
        if abs(x - 1.0) < tol:
            return 0.0
        return x

    q = [_snap(v) for v in q]
    if abs(q[2]) > tol:
        raise ValueError(f"q has non-zero z component (not supported): q={q_frac}")

    for n in range(1, n_max + 1):
        ok0 = abs(q[0] * n - round(q[0] * n)) < tol
        ok1 = abs(q[1] * n - round(q[1] * n)) < tol
        if ok0 and ok1:
            return n
    raise ValueError(f"Could not find commensurate nxnx1 supercell for q={q_frac}")


def _mat_inv_3x3(m: list[list[float]]) -> list[list[float]]:
    a, b, c = m[0]
    d, e, f = m[1]
    g, h, i = m[2]
    det = a * (e * i - f * h) - b * (d * i - f * g) + c * (d * h - e * g)
    if abs(det) < 1.0e-14:
        raise ValueError("Singular cell matrix (det ~ 0)")
    inv_det = 1.0 / det
    return [
        [(e * i - f * h) * inv_det, (c * h - b * i) * inv_det, (b * f - c * e) * inv_det],
        [(f * g - d * i) * inv_det, (a * i - c * g) * inv_det, (c * d - a * f) * inv_det],
        [(d * h - e * g) * inv_det, (b * g - a * h) * inv_det, (a * e - b * d) * inv_det],
    ]


def _vec_mat_mul(v: list[float], m: list[list[float]]) -> list[float]:
    # row-vector v times matrix m
    return [
        v[0] * m[0][0] + v[1] * m[1][0] + v[2] * m[2][0],
        v[0] * m[0][1] + v[1] * m[1][1] + v[2] * m[2][1],
        v[0] * m[0][2] + v[1] * m[1][2] + v[2] * m[2][2],
    ]


def load_qe_template(scf_file: Path) -> dict:
    """Parse a minimal subset of a QE pw.x input (ibrav=0, crystal positions)."""
    lines = scf_file.read_text().splitlines()

    nat = None
    nat_re = re.compile(r"\bnat\s*=\s*(\d+)\b", re.I)
    for line in lines:
        m = nat_re.search(line)
        if m:
            nat = int(m.group(1))
            break
    if nat is None:
        raise ValueError("Could not parse nat= from template")

    cell_header = None
    for i, line in enumerate(lines):
        if line.strip().upper().startswith("CELL_PARAMETERS"):
            cell_header = i
            break
    if cell_header is None:
        raise ValueError("CELL_PARAMETERS not found in template")
    cell = []
    for j in range(1, 4):
        parts = lines[cell_header + j].split()
        if len(parts) < 3:
            raise ValueError("Invalid CELL_PARAMETERS line")
        cell.append([float(parts[0]), float(parts[1]), float(parts[2])])

    atom_header = None
    atom_crystal = False
    for i, line in enumerate(lines):
        up = line.strip().upper()
        if up.startswith("ATOMIC_POSITIONS"):
            atom_header = i
            atom_crystal = "CRYSTAL" in up
            break
    if atom_header is None:
        raise ValueError("ATOMIC_POSITIONS not found in template")
    if not atom_crystal:
        raise ValueError("Template ATOMIC_POSITIONS must be (crystal)")

    symbols: list[str] = []
    frac: list[list[float]] = []
    constraints: list[str] = []
    for i in range(atom_header + 1, atom_header + 1 + nat):
        parts = lines[i].split()
        if len(parts) < 4:
            raise ValueError("Invalid ATOMIC_POSITIONS line")
        symbols.append(parts[0])
        frac.append([float(parts[1]), float(parts[2]), float(parts[3])])
        if len(parts) >= 7:
            constraints.append(f"{parts[4]}   {parts[5]}   {parts[6]}")
        else:
            constraints.append("0   0   0")

    k_points = None
    for i, line in enumerate(lines):
        up = line.strip().upper()
        if up.startswith("K_POINTS") and "AUTOMATIC" in up:
            parts = lines[i + 1].split()
            if len(parts) < 3:
                raise ValueError("Invalid K_POINTS automatic line")
            k_points = [int(parts[0]), int(parts[1]), int(parts[2])]
            break
    if k_points is None:
        raise ValueError("K_POINTS {automatic} not found in template")

    return {
        "nat": nat,
        "cell": cell,  # rows: a,b,c in angstrom
        "symbols": symbols,
        "frac": frac,
        "constraints": constraints,
        "k_points": k_points,
    }


def extract_string_value(text: str, key: str, default: str | None = None) -> str | None:
    pattern = re.compile(rf"\b{re.escape(key)}\s*=\s*['\"]([^'\"]+)['\"]", re.I)
    match = pattern.search(text)
    if match:
        return match.group(1)
    return default


def _replace_or_insert_control_key(text: str, key: str, value_literal: str) -> str:
    lines = text.splitlines()
    in_control = False
    inserted = False
    output: list[str] = []

    key_pattern = re.compile(rf"^\s*{re.escape(key)}\s*=", re.I)
    for line in lines:
        stripped = line.strip()
        if stripped.upper().startswith("&CONTROL"):
            in_control = True
            output.append(line)
            continue
        if in_control and stripped == "/":
            if not inserted:
                output.append(f"  {key} = {value_literal}")
                inserted = True
            in_control = False
            output.append(line)
            continue
        if in_control and key_pattern.match(stripped):
            output.append(f"  {key} = {value_literal}")
            inserted = True
            continue
        output.append(line)

    if not inserted:
        raise ValueError(f"Could not update {key} in &CONTROL")
    return "\n".join(output) + ("\n" if text.endswith("\n") else "")


def prepare_primitive_scf_input(template_path: Path, out_file: Path, pseudo_dir_rel: str) -> dict:
    text = template_path.read_text()
    prefix = extract_string_value(text, "prefix", default="pwscf") or "pwscf"
    text = _replace_or_insert_control_key(text, "pseudo_dir", f"'{pseudo_dir_rel}'")
    text = _replace_or_insert_control_key(text, "outdir", "'./tmp'")
    out_file.write_text(text)
    return {"prefix": prefix}


def parse_time_to_seconds(text: str | None) -> float | None:
    if text is None:
        return None
    s = text.strip()
    if not s:
        return None
    m = re.fullmatch(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:([0-9]+(?:\.[0-9]*)?)s)?", s)
    if not m:
        return None
    days = 0 if m.group(1) is None else int(m.group(1))
    hours = 0 if m.group(2) is None else int(m.group(2))
    minutes = 0 if m.group(3) is None else int(m.group(3))
    seconds = 0.0 if m.group(4) is None else float(m.group(4))
    return float((((days * 24) + hours) * 60 + minutes) * 60 + seconds)


def load_selected_profiles(path: Path) -> dict | None:
    if not path.exists():
        return None
    return json.loads(path.read_text())


def resolve_active_profile(
    selected_profiles_path: Path,
    branch: str,
    level: str,
    fallback_settings: dict,
) -> dict:
    payload = load_selected_profiles(selected_profiles_path)
    if payload is None:
        return dict(fallback_settings)
    branch_payload = payload.get(branch, {})
    profile = branch_payload.get(level)
    if not profile:
        return dict(fallback_settings)
    return dict(profile.get("settings", fallback_settings))


def resolve_structure_template(optimized_template: Path, raw_template: Path) -> Path:
    return optimized_template if optimized_template.exists() else raw_template


def file_contains_job_done(path: Path) -> bool:
    if not path.exists():
        return False
    text = path.read_text(errors="ignore")
    return "JOB DONE" in text


def parse_multiq_eig_file(filename: Path, nat: int) -> list[dict]:
    lines = filename.read_text().splitlines()
    q_blocks = []

    current_q = None
    current_freqs: list[float] = []
    current_modes: list[list[list[complex]]] = []
    collecting = False
    current_mode: list[list[complex]] = []

    def flush():
        if current_q is None:
            return
        if len(current_modes) != len(current_freqs):
            raise ValueError(f"Frequency/mode count mismatch in {filename}")
        q_blocks.append(
            {
                "q_frac": canonicalize_q(current_q),
                "freqs_thz": [float(v) for v in current_freqs],
                "modes": current_modes[:],
            }
        )

    for line in lines:
        s = line.strip()
        if s.startswith("q ="):
            if collecting and len(current_mode) == nat:
                current_modes.append(current_mode[:])
            if current_q is not None:
                flush()
            nums = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
            if len(nums) < 3:
                raise ValueError(f"Could not parse q line: {s}")
            current_q = [float(nums[0]), float(nums[1]), float(nums[2])]
            current_freqs = []
            current_modes = []
            collecting = False
            current_mode = []
            continue

        m = re.search(r"freq\s*\(\s*\d+\s*\)\s*=\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*\[THz\]", s)
        if m:
            if collecting and len(current_mode) == nat:
                current_modes.append(current_mode[:])
            current_freqs.append(float(m.group(1)))
            collecting = True
            current_mode = []
            continue

        if collecting and s.startswith("("):
            nums = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
            if len(nums) >= 6:
                vec = [
                    float(nums[0]) + 1j * float(nums[1]),
                    float(nums[2]) + 1j * float(nums[3]),
                    float(nums[4]) + 1j * float(nums[5]),
                ]
                current_mode.append(vec)
                if len(current_mode) == nat:
                    current_modes.append(current_mode[:])
                    collecting = False
                    current_mode = []

    if collecting and len(current_mode) == nat:
        current_modes.append(current_mode[:])
    if current_q is not None:
        flush()
    return q_blocks


def make_supercell(template: dict, n_super: int) -> dict:
    nat_prim = int(template["nat"])
    cell_prim = template["cell"]
    cell_super = [
        [cell_prim[0][0] * n_super, cell_prim[0][1] * n_super, cell_prim[0][2] * n_super],
        [cell_prim[1][0] * n_super, cell_prim[1][1] * n_super, cell_prim[1][2] * n_super],
        [cell_prim[2][0], cell_prim[2][1], cell_prim[2][2]],
    ]

    symbols_prim = template["symbols"]
    frac_prim = template["frac"]
    cons_prim = template["constraints"]

    symbols: list[str] = []
    frac: list[list[float]] = []
    constraints: list[str] = []
    prim_indices: list[int] = []
    replica_r: list[list[int]] = []

    for i in range(n_super):
        for j in range(n_super):
            for s in range(nat_prim):
                symbols.append(symbols_prim[s])
                constraints.append(cons_prim[s])
                prim_indices.append(s)
                replica_r.append([i, j, 0])
                frac.append(
                    [
                        (frac_prim[s][0] + float(i)) / float(n_super),
                        (frac_prim[s][1] + float(j)) / float(n_super),
                        frac_prim[s][2],
                    ]
                )

    return {
        "n_super": n_super,
        "n_cells": n_super * n_super,
        "cell": cell_super,
        "symbols": symbols,
        "frac": frac,
        "constraints": constraints,
        "prim_indices": prim_indices,
        "replica_r": replica_r,
        "nat_prim": nat_prim,
    }


def _exp_i(theta: float) -> complex:
    return math.cos(theta) + 1j * math.sin(theta)


def build_pair_structure_generator(pair_record: dict, scf_template: Path, primitive_k_mesh: tuple[int, int, int]):
    template = load_qe_template(scf_template)
    q_frac = [float(v) for v in pair_record["target_mode"]["q_frac"]]
    n_super = infer_commensurate_supercell_n(q_frac)

    supercell = make_supercell(template, n_super)
    cell = supercell["cell"]
    cell_inv = _mat_inv_3x3(cell)

    symbols = supercell["symbols"]
    masses = [float(MASS_DICT[s]) for s in symbols]

    nat_prim = supercell["nat_prim"]
    prim_indices = supercell["prim_indices"]
    replica_r = supercell["replica_r"]
    n_cells = supercell["n_cells"]

    gamma_mode = decode_complex_mode(pair_record["gamma_mode"]["eigenvector"])
    q_mode = decode_complex_mode(pair_record["target_mode"]["eigenvector_q"])

    # Expand eigenvectors onto the supercell atom list.
    gamma_super = [gamma_mode[idx] for idx in prim_indices]
    q_super = [q_mode[idx] for idx in prim_indices]

    # Phase for each replica in lattice units.
    phase_q: list[complex] = []
    for r in replica_r:
        theta = 2.0 * math.pi * (q_frac[0] * r[0] + q_frac[1] * r[1] + q_frac[2] * r[2])
        phase_q.append(_exp_i(theta))

    k_super = [
        max(1, int(math.ceil(float(primitive_k_mesh[0]) / float(n_super)))),
        max(1, int(math.ceil(float(primitive_k_mesh[1]) / float(n_super)))),
        max(1, int(math.ceil(float(primitive_k_mesh[2]) / 1.0))),
    ]

    base_frac = supercell["frac"]

    def displaced_frac_positions(a1: float, a2: float) -> list[list[float]]:
        out_frac: list[list[float]] = []
        scale = 1.0 / math.sqrt(float(n_cells))
        for i in range(len(symbols)):
            m_sqrt = math.sqrt(masses[i])
            ph = phase_q[i]
            # u_complex is cartesian in Angstrom*sqrt(amu) (because a1,a2 are Q)
            # u_phys (Angstrom) divides by sqrt(mass).
            ux = (a1 * gamma_super[i][0] + a2 * q_super[i][0] * ph) * scale
            uy = (a1 * gamma_super[i][1] + a2 * q_super[i][1] * ph) * scale
            uz = (a1 * gamma_super[i][2] + a2 * q_super[i][2] * ph) * scale
            u_cart = [ux.real / m_sqrt, uy.real / m_sqrt, uz.real / m_sqrt]
            u_frac = _vec_mat_mul(u_cart, cell_inv)
            f0 = base_frac[i][0] + u_frac[0]
            f1 = base_frac[i][1] + u_frac[1]
            f2 = base_frac[i][2] + u_frac[2]
            # Wrap in-plane only, to avoid boundary-induced fitting artifacts.
            f0 = f0 - math.floor(f0)
            f1 = f1 - math.floor(f1)
            out_frac.append([f0, f1, f2])
        return out_frac

    return {
        "n_super": n_super,
        "n_cells": n_cells,
        "cell": cell,
        "symbols": symbols,
        "constraints": supercell["constraints"],
        "k_super": k_super,
        "displaced_frac_positions": displaced_frac_positions,
    }


def primitive_k_mesh_from_settings(template: dict, scf_settings: dict) -> list[int]:
    if scf_settings.get("primitive_k_mesh"):
        return [int(v) for v in scf_settings["primitive_k_mesh"]]
    return [int(v) for v in template["k_points"]]


def supercell_k_mesh_from_primitive(primitive_k_mesh: list[int], n_super: int) -> list[int]:
    return [
        max(1, int(math.ceil(float(primitive_k_mesh[0]) / float(n_super)))),
        max(1, int(math.ceil(float(primitive_k_mesh[1]) / float(n_super)))),
        max(1, int(math.ceil(float(primitive_k_mesh[2])))),
    ]


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

    with out_file.open("w") as f:
        f.write("&CONTROL\n")
        f.write(f"  calculation = '{calculation}'\n")
        f.write(f"  disk_io = '{disk_io}'\n")
        f.write("  prefix = 'pwscf'\n")
        f.write(f"  pseudo_dir = '{pseudo_dir_rel}'\n")
        f.write("  outdir = './tmp'\n")
        f.write(f"  verbosity = '{verbosity}'\n")
        f.write(f"  tprnfor = {tprnfor}\n")
        f.write(f"  tstress = {tstress}\n")
        f.write(f"  forc_conv_thr = {scf_settings['forc_conv_thr']}\n")
        if scf_settings.get("etot_conv_thr"):
            f.write(f"  etot_conv_thr = {scf_settings['etot_conv_thr']}\n")
        f.write("/\n\n")

        f.write("&SYSTEM\n")
        f.write("  ibrav = 0\n")
        f.write(f"  nat = {nat}, ntyp = 2\n")
        if scf_settings.get("occupations") == "smearing":
            f.write(
                "  occupations = 'smearing', "
                f"smearing = '{scf_settings['smearing']}', degauss = {scf_settings['degauss']}\n"
            )
        else:
            f.write(f"  occupations = '{scf_settings['occupations']}'\n")
        f.write(f"  ecutwfc = {scf_settings['ecutwfc']}, ecutrho = {scf_settings['ecutrho']}\n")
        f.write("/\n\n")

        f.write("&ELECTRONS\n")
        f.write(f"  electron_maxstep = {scf_settings['electron_maxstep']}\n")
        f.write(f"  conv_thr = {scf_settings['conv_thr']}\n")
        f.write(f"  mixing_mode = '{scf_settings['mixing_mode']}'\n")
        f.write(f"  mixing_beta = {scf_settings['mixing_beta']}\n")
        f.write(f"  diagonalization = '{scf_settings['diagonalization']}'\n")
        f.write("/\n\n")

        if include_ions:
            f.write("&IONS\n")
            f.write(f"  ion_dynamics = '{ion_dynamics}'\n")
            f.write("/\n\n")

        if include_cell:
            f.write("&CELL\n")
            f.write(f"  cell_dynamics = '{cell_dynamics}'\n")
            f.write(f"  press_conv_thr = {press_conv_thr}\n")
            f.write("/\n\n")

        f.write("ATOMIC_SPECIES\n")
        f.write("W  183.84 W.pz-spn-rrkjus_psl.1.0.0.UPF\n")
        f.write("Se 78.960 Se.pz-n-rrkjus_psl.0.2.UPF\n\n")

        f.write("CELL_PARAMETERS (angstrom)\n")
        for i in range(3):
            f.write(f"   {cell[i][0]:.9f}   {cell[i][1]:.9f}   {cell[i][2]:.9f}\n")

        f.write("\nATOMIC_POSITIONS (crystal)\n")
        for i in range(nat):
            sym = symbols[i]
            pos = frac_positions[i]
            cons = constraints[i] if constraints else "0   0   0"
            f.write(f"{sym:<4}   {pos[0]:.10f}   {pos[1]:.10f}   {pos[2]:.10f}   {cons}\n")

        f.write("\nK_POINTS {automatic}\n")
        f.write(f"{k_mesh[0]} {k_mesh[1]} {k_mesh[2]} 0 0 0\n")


def write_scf_input(
    out_file: Path,
    cell: list[list[float]],
    symbols: list[str],
    frac_positions: list[list[float]],
    constraints: list[str],
    k_mesh: list[int],
    pseudo_dir_rel: str,
    scf_settings: dict,
) -> None:
    write_qe_input(
        out_file=out_file,
        cell=cell,
        symbols=symbols,
        frac_positions=frac_positions,
        constraints=constraints,
        k_mesh=k_mesh,
        pseudo_dir_rel=pseudo_dir_rel,
        scf_settings=scf_settings,
    )


def prepare_primitive_qe_input(
    template_path: Path,
    out_file: Path,
    pseudo_dir_rel: str,
    scf_settings: dict,
    k_mesh: list[int] | None = None,
) -> dict:
    template = load_qe_template(template_path)
    prefix = extract_string_value(template_path.read_text(), "prefix", default="pwscf") or "pwscf"
    write_qe_input(
        out_file=out_file,
        cell=template["cell"],
        symbols=template["symbols"],
        frac_positions=template["frac"],
        constraints=template["constraints"],
        k_mesh=template["k_points"] if k_mesh is None else list(k_mesh),
        pseudo_dir_rel=pseudo_dir_rel,
        scf_settings=scf_settings,
    )
    return {"prefix": prefix}


def extract_energy_ry(scf_out: Path) -> float | None:
    if not scf_out.exists():
        return None
    lines = scf_out.read_text(errors="ignore").splitlines()
    for line in reversed(lines):
        if "total energy" in line and line.lstrip().startswith("!"):
            m = re.search(r"=\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)", line)
            if m:
                return float(m.group(1))
    return None


def extract_total_force_ry_bohr(scf_out: Path) -> float | None:
    if not scf_out.exists():
        return None
    result = None
    for line in scf_out.read_text(errors="ignore").splitlines():
        m = re.search(r"Total force\s*=\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)", line)
        if m:
            result = float(m.group(1))
    return result


def extract_max_atomic_force_ry_bohr(scf_out: Path) -> float | None:
    if not scf_out.exists():
        return None
    current = []
    last_complete = None
    for line in scf_out.read_text(errors="ignore").splitlines():
        m = re.search(
            r"force\s*=\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)",
            line,
        )
        if m:
            fx, fy, fz = float(m.group(1)), float(m.group(2)), float(m.group(3))
            current.append(math.sqrt(fx * fx + fy * fy + fz * fz))
        elif current:
            last_complete = current[:]
            current = []
    if current:
        last_complete = current[:]
    if not last_complete:
        return None
    return max(last_complete)


def extract_wall_sec(scf_out: Path) -> float | None:
    if not scf_out.exists():
        return None
    result = None
    for line in scf_out.read_text(errors="ignore").splitlines():
        m = re.search(r"PWSCF\s*:\s*.*?CPU\s+(.*?)\s+WALL", line)
        if m:
            result = parse_time_to_seconds(m.group(1))
    return result


def extract_final_relaxed_structure(scf_out: Path, nat: int) -> dict | None:
    if not scf_out.exists():
        return None
    lines = scf_out.read_text(errors="ignore").splitlines()
    last_cell = None
    last_frac = None
    for i, line in enumerate(lines):
        up = line.strip().upper()
        if up.startswith("CELL_PARAMETERS"):
            try:
                cell = []
                for j in range(1, 4):
                    parts = lines[i + j].split()
                    cell.append([float(parts[0]), float(parts[1]), float(parts[2])])
                last_cell = cell
            except Exception:
                pass
        if up.startswith("ATOMIC_POSITIONS") and "CRYSTAL" in up:
            try:
                frac = []
                symbols = []
                constraints = []
                for j in range(1, nat + 1):
                    parts = lines[i + j].split()
                    symbols.append(parts[0])
                    frac.append([float(parts[1]), float(parts[2]), float(parts[3])])
                    if len(parts) >= 7:
                        constraints.append(f"{parts[4]}   {parts[5]}   {parts[6]}")
                    else:
                        constraints.append("0   0   0")
                last_frac = {"symbols": symbols, "frac": frac, "constraints": constraints}
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


def fractional_to_cartesian(cell: list[list[float]], frac_positions: list[list[float]]) -> list[list[float]]:
    out = []
    for frac in frac_positions:
        x = frac[0] * cell[0][0] + frac[1] * cell[1][0] + frac[2] * cell[2][0]
        y = frac[0] * cell[0][1] + frac[1] * cell[1][1] + frac[2] * cell[2][1]
        z = frac[0] * cell[0][2] + frac[1] * cell[1][2] + frac[2] * cell[2][2]
        out.append([x, y, z])
    return out


def max_position_delta_A(cell_ref: list[list[float]], frac_ref: list[list[float]], cell_new: list[list[float]], frac_new: list[list[float]]) -> float:
    cart_ref = fractional_to_cartesian(cell_ref, frac_ref)
    cart_new = fractional_to_cartesian(cell_new, frac_new)
    max_delta = 0.0
    for r, n in zip(cart_ref, cart_new):
        dx = n[0] - r[0]
        dy = n[1] - r[1]
        dz = n[2] - r[2]
        max_delta = max(max_delta, math.sqrt(dx * dx + dy * dy + dz * dz))
    return max_delta


def max_cell_delta_A(cell_ref: list[list[float]], cell_new: list[list[float]]) -> float:
    max_delta = 0.0
    for row_ref, row_new in zip(cell_ref, cell_new):
        for a, b in zip(row_ref, row_new):
            max_delta = max(max_delta, abs(float(a) - float(b)))
    return max_delta


def freq_from_c2(c2: float) -> dict:
    if c2 > 0:
        root = math.sqrt(2.0 * c2)
        return {"stable": True, "thz": float(root * CONV_TO_THZ), "cm1": float(root * CONV_TO_CM1)}
    root = math.sqrt(2.0 * abs(c2))
    return {"stable": False, "imag_thz": float(root * CONV_TO_THZ), "imag_cm1": float(root * CONV_TO_CM1)}


def _solve_linear_system(a: list[list[float]], b: list[float]) -> list[float]:
    n = len(b)
    m = [row[:] + [b_i] for row, b_i in zip(a, b)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(m[r][col]))
        if abs(m[pivot][col]) < 1.0e-14:
            raise ValueError("Singular normal equation matrix")
        m[col], m[pivot] = m[pivot], m[col]

        piv = m[col][col]
        for j in range(col, n + 1):
            m[col][j] /= piv

        for r in range(n):
            if r == col:
                continue
            factor = m[r][col]
            if abs(factor) < 1.0e-18:
                continue
            for j in range(col, n + 1):
                m[r][j] -= factor * m[col][j]
    return [m[i][n] for i in range(n)]


def _lstsq_normal(features: list[list[float]], y: list[float]) -> list[float]:
    p = len(features[0])
    a = [[0.0 for _ in range(p)] for _ in range(p)]
    b = [0.0 for _ in range(p)]
    for f, yi in zip(features, y):
        for i in range(p):
            b[i] += f[i] * yi
            fi = f[i]
            for j in range(p):
                a[i][j] += fi * f[j]
    return _solve_linear_system(a, b)


def fit_pair_grid(
    a1_vals: list[float],
    a2_vals: list[float],
    e_grid_ry: list[list[float]],
    fit_window: float = 1.0,
) -> dict:
    # Shift energies by min to improve conditioning (only affects constant term).
    e_flat_ev = [v * RY_TO_EV for row in e_grid_ry for v in row]
    e0 = min(e_flat_ev)

    xs: list[float] = []
    ys: list[float] = []
    zs: list[float] = []

    for i2, a2 in enumerate(a2_vals):
        for i1, a1 in enumerate(a1_vals):
            xs.append(float(a1))
            ys.append(float(a2))
            zs.append(float(e_grid_ry[i2][i1] * RY_TO_EV - e0))

    def feat(x: float, y: float) -> list[float]:
        x2 = x * x
        y2 = y * y
        return [
            x2,  # c20
            y2,  # c02
            x * y2,  # c12
            x2 * y,  # c21
            x2 * x,  # c30
            y2 * y,  # c03
            x * y,  # c11
            x2 * x2,  # c40
            y2 * y2,  # c04
            x2 * y2,  # c22
            x,  # c10
            y,  # c01
            1.0,  # c00
        ]

    features_fit: list[list[float]] = []
    z_fit: list[float] = []
    for x, y, z in zip(xs, ys, zs):
        if abs(x) <= fit_window and abs(y) <= fit_window:
            features_fit.append(feat(x, y))
            z_fit.append(z)
    params = _lstsq_normal(features_fit, z_fit)

    def model(x: float, y: float) -> float:
        f = feat(x, y)
        return sum(p * fi for p, fi in zip(params, f))

    preds = [model(x, y) for x, y in zip(xs, ys)]
    resid = [p - z for p, z in zip(preds, zs)]
    sse = sum(r * r for r in resid)
    z_mean = sum(zs) / float(len(zs))
    sst = sum((z - z_mean) * (z - z_mean) for z in zs)
    r2 = 1.0 - sse / sst if sst > 0 else 0.0
    rmse = math.sqrt(sse / float(len(zs)))
    max_abs_resid = max(abs(r) for r in resid)

    c20, c02, c12, c21, c30, c03, c11, c40, c04, c22, c10, c01, c00 = params

    # Axis checks (quadratic fit on y=0 and x=0 cuts).
    def quad_fit(x_list: list[float], z_list: list[float]) -> float:
        feats = [[x * x, x, 1.0] for x in x_list]
        p2, _p1, _p0 = _lstsq_normal(feats, z_list)
        return float(p2)

    # Find the index closest to 0.0 (should exist in our symmetric grids).
    idx_a2_0 = min(range(len(a2_vals)), key=lambda i: abs(a2_vals[i]))
    idx_a1_0 = min(range(len(a1_vals)), key=lambda i: abs(a1_vals[i]))

    a1_cut = [a1 for a1 in a1_vals if abs(a1) <= fit_window]
    a2_cut = [a2 for a2 in a2_vals if abs(a2) <= fit_window]
    e_a1 = [e_grid_ry[idx_a2_0][min(range(len(a1_vals)), key=lambda i: abs(a1_vals[i] - a1))] * RY_TO_EV - e0 for a1 in a1_cut]
    e_a2 = [e_grid_ry[min(range(len(a2_vals)), key=lambda i: abs(a2_vals[i] - a2))][idx_a1_0] * RY_TO_EV - e0 for a2 in a2_cut]

    axis1_c2 = quad_fit(a1_cut, e_a1)
    axis2_c2 = quad_fit(a2_cut, e_a2)

    return {
        "fit_window": float(fit_window),
        "r2": float(r2),
        "rmse_ev_supercell": float(rmse),
        "max_abs_residual_ev_supercell": float(max_abs_resid),
        "physics": {
            "freq_mode1": freq_from_c2(float(c20)),
            "freq_mode2": freq_from_c2(float(c02)),
            # Our convention: E has a c12 * A1 * A2^2 term -> phi122 = 2*c12 in eV,
            # report in meV.
            "phi_122_mev": float(2.0 * c12 * 1000.0),
            "phi_112_mev": float(2.0 * c21 * 1000.0),
            "phi_111_mev": float(6.0 * c30 * 1000.0),
            "phi_222_mev": float(6.0 * c03 * 1000.0),
            "coefficients_ev": {
                "c20": float(c20),
                "c02": float(c02),
                "c12": float(c12),
                "c21": float(c21),
                "c30": float(c30),
                "c03": float(c03),
                "c11": float(c11),
                "c40": float(c40),
                "c04": float(c04),
                "c22": float(c22),
                "c10": float(c10),
                "c01": float(c01),
                "c00": float(c00),
            },
        },
        "axis_checks": {
            "mode1_axis_fit": {"c2": float(axis1_c2), "freq": freq_from_c2(float(axis1_c2))},
            "mode2_axis_fit": {"c2": float(axis2_c2), "freq": freq_from_c2(float(axis2_c2))},
        },
    }


def parse_sbatch_job_id(stdout: str) -> str | None:
    m = re.search(r"Submitted batch job\s+(\d+)", stdout)
    return m.group(1) if m else None


def squeue_count_jobs(job_ids: list[str]) -> int:
    return len(squeue_existing_job_ids(job_ids))


def squeue_existing_job_ids(job_ids: list[str]) -> set[str]:
    if not job_ids:
        return set()
    # Slurm accepts comma-separated job id list.
    arg = ",".join(job_ids)
    try:
        result = subprocess.run(["squeue", "-h", "-j", arg, "-o", "%i"], capture_output=True, text=True, check=True)
        txt = result.stdout.strip()
        return set() if not txt else {line.strip() for line in txt.splitlines() if line.strip()}
    except Exception:
        return set()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def relpath(from_dir: Path, to_path: Path) -> str:
    return os.path.relpath(str(to_path), start=str(from_dir))
