#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path

import numpy as np
from ase.build import make_supercell
from ase.io.espresso import read_espresso_in

try:
    from .scf_settings import DEFAULT_PRESET_NAME, resolve_scf_settings
except ImportError:
    from scf_settings import DEFAULT_PRESET_NAME, resolve_scf_settings


MASS_DICT = {"W": 183.84, "Se": 78.960}
RY_TO_EV = 13.605693009
CONV_TO_THZ = 15.63330423985619
CONV_TO_CM1 = 521.4708983725064


def decode_complex_mode(mode):
    arr = []
    for vec in mode:
        if isinstance(vec, dict):
            arr.append(
                [
                    float(vec["x"]["re"]) + 1j * float(vec["x"]["im"]),
                    float(vec["y"]["re"]) + 1j * float(vec["y"]["im"]),
                    float(vec["z"]["re"]) + 1j * float(vec["z"]["im"]),
                ]
            )
        else:
            arr.append([c[0] + 1j * c[1] for c in vec])
    return np.array(arr, dtype=np.complex128)


def infer_commensurate_supercell_n(q_frac, n_max: int = 12, tol: float = 1.0e-8):
    q = np.array(q_frac, dtype=float)
    q = q - np.floor(q)
    q[np.abs(q) < tol] = 0.0
    q[np.abs(q - 1.0) < tol] = 0.0
    for n in range(1, n_max + 1):
        if abs(q[2]) > tol:
            continue
        if abs(q[0] * n - round(q[0] * n)) < tol and abs(q[1] * n - round(q[1] * n)) < tol:
            return n
    raise ValueError(f"Could not find commensurate nxnx1 supercell for q={q_frac}")


def load_qe_template(scf_file: Path):
    lines = scf_file.read_text().splitlines()
    with scf_file.open("r") as f:
        prim = read_espresso_in(f)

    nat = len(prim)
    atom_header = None
    for i, line in enumerate(lines):
        if line.strip().upper().startswith("ATOMIC_POSITIONS"):
            atom_header = i
            break
    if atom_header is None:
        raise ValueError("ATOMIC_POSITIONS not found in template")

    constraints = []
    for i in range(atom_header + 1, atom_header + 1 + nat):
        parts = lines[i].split()
        if len(parts) >= 7:
            constraints.append(f"{parts[4]}   {parts[5]}   {parts[6]}")
        else:
            constraints.append("0   0   0")

    k_points = None
    for i, line in enumerate(lines):
        up = line.strip().upper()
        if up.startswith("K_POINTS") and "AUTOMATIC" in up:
            vals = lines[i + 1].split()
            k_points = [int(vals[0]), int(vals[1]), int(vals[2])]
            break
    if k_points is None:
        raise ValueError("Could not parse K_POINTS automatic")

    return prim, constraints, k_points


def write_scf_input(
    out_file: Path,
    base_cell,
    symbols,
    frac_positions,
    constraints,
    k_super,
    scf_settings: dict | None = None,
):
    settings = resolve_scf_settings(DEFAULT_PRESET_NAME) if scf_settings is None else dict(scf_settings)
    disk_io = settings.get("disk_io", "low")
    verbosity = settings.get("verbosity", "high")
    tprnfor = ".true." if settings.get("tprnfor", True) else ".false."
    tstress = ".true." if settings.get("tstress", True) else ".false."
    include_ions = bool(settings.get("include_ions", True))
    include_cell = bool(settings.get("include_cell", False))

    with out_file.open("w") as f:
        f.write("&CONTROL\n")
        f.write("  calculation = 'scf'\n")
        f.write(f"  disk_io = '{disk_io}'\n")
        f.write("  prefix = 'pwscf'\n")
        f.write("  pseudo_dir = './'\n")
        f.write("  outdir = './tmp'\n")
        f.write(f"  verbosity = '{verbosity}'\n")
        f.write(f"  tprnfor = {tprnfor}\n")
        f.write(f"  tstress = {tstress}\n")
        f.write(f"  forc_conv_thr = {settings['forc_conv_thr']}\n")
        if settings.get("etot_conv_thr"):
            f.write(f"  etot_conv_thr = {settings['etot_conv_thr']}\n")
        f.write("/\n\n")

        f.write("&SYSTEM\n")
        f.write("  ibrav = 0\n")
        f.write(f"  nat = {len(symbols)}, ntyp = 2\n")
        f.write("  nosym = .true.\n")
        f.write("  noinv = .true.\n")
        if settings["occupations"] == "smearing":
            f.write(
                "  occupations = 'smearing', "
                f"smearing = '{settings['smearing']}', degauss = {settings['degauss']}\n"
            )
        else:
            f.write(f"  occupations = '{settings['occupations']}'\n")
        f.write(f"  ecutwfc = {settings['ecutwfc']}, ecutrho = {settings['ecutrho']}\n")
        f.write("/\n\n")

        f.write("&ELECTRONS\n")
        f.write(f"  electron_maxstep = {settings['electron_maxstep']}\n")
        f.write(f"  conv_thr = {settings['conv_thr']}\n")
        f.write(f"  mixing_mode = '{settings['mixing_mode']}'\n")
        f.write(f"  mixing_beta = {settings['mixing_beta']}\n")
        f.write(f"  diagonalization = '{settings['diagonalization']}'\n")
        f.write("/\n\n")

        if include_ions:
            f.write("&IONS\n")
            f.write("  ion_dynamics = 'bfgs'\n")
            f.write("/\n\n")

        if include_cell:
            f.write("&CELL\n")
            f.write("  press_conv_thr = 0.1\n")
            f.write("/\n\n")

        f.write("ATOMIC_SPECIES\n")
        f.write("W  183.84 W.pz-spn-rrkjus_psl.1.0.0.UPF\n")
        f.write("Se 78.960 Se.pz-n-rrkjus_psl.0.2.UPF\n\n")

        f.write("CELL_PARAMETERS (angstrom)\n")
        for i in range(3):
            f.write(f"   {base_cell[i,0]:.9f}   {base_cell[i,1]:.9f}   {base_cell[i,2]:.9f}\n")

        f.write("\nATOMIC_POSITIONS (crystal)\n")
        nat_prim = len(constraints)
        for i, (sym, pos) in enumerate(zip(symbols, frac_positions)):
            cons = constraints[i % nat_prim]
            f.write(f"{sym:<4}   {pos[0]:.10f}   {pos[1]:.10f}   {pos[2]:.10f}   {cons}\n")

        f.write("\nK_POINTS {automatic}\n")
        f.write(f"{k_super[0]} {k_super[1]} {k_super[2]} 0 0 0\n")


def build_pair_structure_generator(pair_record: dict, scf_template: Path):
    prim, constraints_prim, k_prim = load_qe_template(scf_template)
    q_frac = np.array(pair_record["target_mode"]["q_frac"], dtype=float)
    n_super = infer_commensurate_supercell_n(q_frac)
    nat_prim = len(prim)
    supercell = make_supercell(prim, [[n_super, 0, 0], [0, n_super, 0], [0, 0, 1]])
    base_cell = supercell.get_cell().array.copy()
    base_frac = supercell.get_scaled_positions().copy()
    cell_inv = np.linalg.inv(base_cell)
    symbols = supercell.get_chemical_symbols()
    masses = np.array([MASS_DICT[s] for s in symbols], dtype=float)[:, None]
    prim_indices = np.arange(len(supercell), dtype=int) % nat_prim
    replica_r = np.array([[i, j, 0] for i in range(n_super) for j in range(n_super) for _ in range(nat_prim)], dtype=float)

    gamma_mode = decode_complex_mode(pair_record["gamma_mode"]["eigenvector"])
    q_mode = decode_complex_mode(pair_record["target_mode"]["eigenvector_q"])
    phase_q = np.exp(2j * np.pi * np.dot(replica_r, q_frac))
    gamma_super = gamma_mode[prim_indices]
    q_super = q_mode[prim_indices]
    n_cells = n_super * n_super
    k_super = [max(1, int(np.ceil(k / n_super))) for k in k_prim]

    def fractional_positions(a1: float, a2: float):
        u_complex = (a1 * gamma_super + a2 * q_super * phase_q[:, None]) / np.sqrt(n_cells)
        u_phys = np.real(u_complex) / np.sqrt(masses)
        frac = base_frac + u_phys @ cell_inv
        frac[:, :2] %= 1.0
        return frac

    return {
        "prim": prim,
        "n_super": n_super,
        "n_cells": n_cells,
        "base_cell": base_cell,
        "symbols": symbols,
        "constraints_prim": constraints_prim,
        "k_super": k_super,
        "fractional_positions": fractional_positions,
    }


def extract_energy_ry(scf_out: Path):
    if not scf_out.exists():
        return None
    lines = scf_out.read_text(errors="ignore").splitlines()
    for line in reversed(lines):
        if "total energy" in line and line.lstrip().startswith("!"):
            m = re.search(r"=\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)", line)
            if m:
                return float(m.group(1))
    return None


def fit_func(xy, c20, c02, c12, c21, c30, c03, c11, c40, c04, c22, c10, c01, c00):
    x, y = xy
    return (
        c20 * x**2
        + c02 * y**2
        + c12 * x * y**2
        + c21 * x**2 * y
        + c30 * x**3
        + c03 * y**3
        + c11 * x * y
        + c40 * x**4
        + c04 * y**4
        + c22 * x**2 * y**2
        + c10 * x
        + c01 * y
        + c00
    )


def freq_from_c2(c2: float):
    if c2 > 0:
        root = np.sqrt(2.0 * c2)
        return {"stable": True, "thz": float(root * CONV_TO_THZ), "cm1": float(root * CONV_TO_CM1)}
    return {"stable": False, "imag_thz": float(np.sqrt(2.0 * abs(c2)) * CONV_TO_THZ), "imag_cm1": float(np.sqrt(2.0 * abs(c2)) * CONV_TO_CM1)}


def fit_pair_grid(a1_vals: np.ndarray, a2_vals: np.ndarray, e_grid_ry: np.ndarray, fit_window: float = 1.0):
    e_grid_ev = e_grid_ry * RY_TO_EV
    e_shift = e_grid_ev - np.min(e_grid_ev)
    x = np.repeat(a1_vals, len(a2_vals))
    y = np.tile(a2_vals, len(a1_vals))
    z = e_shift.T.reshape(-1)

    mask = (np.abs(x) <= fit_window) & (np.abs(y) <= fit_window)
    x_fit = x[mask]
    y_fit = y[mask]
    z_fit = z[mask]

    design = np.column_stack(
        [
            x_fit**2,
            y_fit**2,
            x_fit * y_fit**2,
            x_fit**2 * y_fit,
            x_fit**3,
            y_fit**3,
            x_fit * y_fit,
            x_fit**4,
            y_fit**4,
            x_fit**2 * y_fit**2,
            x_fit,
            y_fit,
            np.ones_like(x_fit),
        ]
    )
    params, _, _, _ = np.linalg.lstsq(design, z_fit, rcond=None)
    model_all = fit_func(np.vstack([x, y]), *params)
    residuals = model_all - z
    sse = float(np.sum(residuals**2))
    sst = float(np.sum((z - np.mean(z)) ** 2))
    r2 = float(1.0 - sse / sst)
    rmse = float(np.sqrt(np.mean(residuals**2)))

    c20, c02, c12, c21, c30, c03, c11, c40, c04, c22, c10, c01, c00 = params
    idx_a2_0 = int(np.argmin(np.abs(a2_vals)))
    idx_a1_0 = int(np.argmin(np.abs(a1_vals)))
    e_a1 = e_shift[idx_a2_0, :]
    e_a2 = e_shift[:, idx_a1_0]
    axis1 = np.polyfit(a1_vals[np.abs(a1_vals) <= fit_window], e_a1[np.abs(a1_vals) <= fit_window], 2)
    axis2 = np.polyfit(a2_vals[np.abs(a2_vals) <= fit_window], e_a2[np.abs(a2_vals) <= fit_window], 2)

    return {
        "fit_window": fit_window,
        "r2": r2,
        "rmse_ev_supercell": rmse,
        "max_abs_residual_ev_supercell": float(np.max(np.abs(residuals))),
        "physics": {
            "freq_mode1": freq_from_c2(float(c20)),
            "freq_mode2": freq_from_c2(float(c02)),
            "phi_122_mev_per_A3amu32": float(2.0 * c12 * 1000.0),
            "phi_112_mev_per_A3amu32": float(2.0 * c21 * 1000.0),
            "phi_111_mev_per_A3amu32": float(6.0 * c30 * 1000.0),
            "phi_222_mev_per_A3amu32": float(6.0 * c03 * 1000.0),
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
            "mode1_axis_fit": {"c2": float(axis1[0]), "freq": freq_from_c2(float(axis1[0]))},
            "mode2_axis_fit": {"c2": float(axis2[0]), "freq": freq_from_c2(float(axis2[0]))},
        },
    }


def count_running_jobs(user: str):
    try:
        result = subprocess.run(["squeue", "-u", user, "-h", "-o", "%i"], capture_output=True, text=True, check=True)
        txt = result.stdout.strip()
        return 0 if not txt else len(txt.splitlines())
    except Exception:
        return 0


def parse_sbatch_job_id(stdout: str):
    m = re.search(r"Submitted batch job\s+(\d+)", stdout)
    return None if m is None else m.group(1)


def squeue_existing_job_ids(job_ids: list[str]):
    if not job_ids:
        return set()
    arg = ",".join(str(job_id) for job_id in job_ids)
    try:
        result = subprocess.run(["squeue", "-h", "-j", arg, "-o", "%i"], capture_output=True, text=True, check=True)
        txt = result.stdout.strip()
        return set() if not txt else {line.strip() for line in txt.splitlines() if line.strip()}
    except Exception:
        return set()


def dump_json(path: Path, payload: dict):
    path.write_text(json.dumps(payload, indent=2))
