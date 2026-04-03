#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import importlib.util
from pathlib import Path

import numpy as np
from ase.data import atomic_masses, atomic_numbers
from ase.build import make_supercell
from ase.io import read
from ase.io.espresso import read_espresso_in


CONV_TO_THZ = 15.63330423985619
CONV_TO_CM1 = 521.4708983725064
MASS_DICT = {"W": 183.84, "Se": 78.960}
RY_TO_EV = 13.605693009
DEFAULT_GPTFF_MODEL_NAME = "gptff_v2.pth"
DEFAULT_MATTERSIM_MODEL = "mattersim-v1.0.0-5M"
GPTFF_MODEL_ALIASES = {
    "gptff": "gptff_v2.pth",
    "gptff_v1": "gptff_v1.pth",
    "gptff_v2": "gptff_v2.pth",
}

RUNTIME_CONFIG_KEYS = {
    "strategy",
    "coarse_grid_size",
    "full_grid_size",
    "refine_top_k",
    "batch_size",
    "num_workers",
    "torch_threads",
    "interop_threads",
    "worker_affinity",
    "chunksize",
    "maxtasksperchild",
}


def decode_complex_mode(mode):
    if not mode:
        return np.zeros((0, 3), dtype=np.complex128)

    first = mode[0]
    if isinstance(first, dict):
        rows = []
        for vec in mode:
            rows.append(
                [
                    float(vec["x"]["re"]) + 1j * float(vec["x"]["im"]),
                    float(vec["y"]["re"]) + 1j * float(vec["y"]["im"]),
                    float(vec["z"]["re"]) + 1j * float(vec["z"]["im"]),
                ]
            )
        return np.array(rows, dtype=np.complex128)

    return np.array([[c[0] + 1j * c[1] for c in vec] for vec in mode], dtype=np.complex128)


def canonicalize_q(q, tol: float = 1.0e-8):
    q = np.array(q, dtype=float)
    q = q - np.floor(q)
    q[np.abs(q) < tol] = 0.0
    q[np.abs(q - 1.0) < tol] = 0.0
    return q


def infer_commensurate_supercell_n(q_frac, n_max: int = 12, tol: float = 1.0e-8):
    q = canonicalize_q(q_frac)
    for n in range(1, n_max + 1):
        if abs(q[2]) > tol:
            continue
        cond_x = abs(q[0] * n - round(q[0] * n)) < tol
        cond_y = abs(q[1] * n - round(q[1] * n)) < tol
        if cond_x and cond_y:
            return n
    raise ValueError(f"Could not find commensurate nxnx1 supercell for q={q_frac} up to n={n_max}")


def load_atoms_from_qe(scf_file: Path):
    suffix = scf_file.suffix.lower()
    if suffix in {".xyz", ".extxyz", ".traj", ".cif", ".vasp", ".poscar"}:
        return read(scf_file)
    try:
        with scf_file.open("r") as f:
            return read_espresso_in(f)
    except Exception:
        return read(scf_file)


def atomic_mass_from_symbol(symbol: str) -> float:
    if symbol in MASS_DICT:
        return float(MASS_DICT[symbol])
    try:
        return float(atomic_masses[atomic_numbers[symbol]])
    except Exception as exc:
        raise KeyError(f"Unsupported element symbol for mass lookup: {symbol}") from exc


def _ensure_mattersim_ase_compat() -> None:
    import ase.constraints

    if hasattr(ase.constraints, "full_3x3_to_voigt_6_stress"):
        return
    from ase.stress import full_3x3_to_voigt_6_stress

    ase.constraints.full_3x3_to_voigt_6_stress = full_3x3_to_voigt_6_stress


def configure_torch_runtime(torch_threads: int | None = None, interop_threads: int | None = 1):
    if torch_threads is not None:
        threads = max(1, int(torch_threads))
        os.environ["OMP_NUM_THREADS"] = str(threads)
        os.environ["MKL_NUM_THREADS"] = str(threads)
        os.environ["OPENBLAS_NUM_THREADS"] = str(threads)
        os.environ["NUMEXPR_NUM_THREADS"] = str(threads)
    else:
        threads = None

    try:
        import torch
    except Exception:
        return

    if threads is not None:
        torch.set_num_threads(threads)
    if interop_threads is not None and hasattr(torch, "set_num_interop_threads"):
        try:
            torch.set_num_interop_threads(max(1, int(interop_threads)))
        except RuntimeError:
            pass


def _parse_lscpu_topology():
    try:
        result = subprocess.run(
            ["lscpu", "-p=cpu,core,socket"],
            capture_output=True,
            text=True,
            check=True,
        )
    except Exception:
        return []

    rows = []
    for line in result.stdout.splitlines():
        if not line or line.startswith("#"):
            continue
        cpu_str, core_str, socket_str = line.split(",")
        rows.append(
            {
                "cpu": int(cpu_str),
                "core": int(core_str),
                "socket": int(socket_str),
            }
        )
    return rows


def available_cpu_ids():
    if hasattr(os, "sched_getaffinity"):
        return sorted(int(cpu) for cpu in os.sched_getaffinity(0))
    count = os.cpu_count() or 1
    return list(range(count))


def suggest_worker_cpu_sets(num_workers: int, threads_per_worker: int):
    num_workers = max(1, int(num_workers))
    threads_per_worker = max(1, int(threads_per_worker))
    allowed = set(available_cpu_ids())
    topology = [row for row in _parse_lscpu_topology() if row["cpu"] in allowed]
    if not topology:
        allowed_list = sorted(allowed)
        return [allowed_list[i * threads_per_worker : (i + 1) * threads_per_worker] for i in range(num_workers)]

    by_socket_primary: dict[int, list[int]] = {}
    by_socket_extra: dict[int, list[int]] = {}
    seen_cores: set[tuple[int, int]] = set()
    for row in sorted(topology, key=lambda item: (item["socket"], item["core"], item["cpu"])):
        socket = int(row["socket"])
        cpu = int(row["cpu"])
        core_key = (socket, int(row["core"]))
        if core_key not in seen_cores:
            by_socket_primary.setdefault(socket, []).append(cpu)
            seen_cores.add(core_key)
        else:
            by_socket_extra.setdefault(socket, []).append(cpu)

    sockets = sorted(by_socket_primary) or [0]
    cpu_sets: list[list[int]] = []
    for worker_idx in range(num_workers):
        socket = sockets[worker_idx % len(sockets)]
        primary_pool = by_socket_primary.get(socket, [])
        extra_pool = by_socket_extra.get(socket, [])
        start = (worker_idx // len(sockets)) * threads_per_worker
        selected = primary_pool[start : start + threads_per_worker]
        if len(selected) < threads_per_worker:
            need = threads_per_worker - len(selected)
            selected.extend(extra_pool[:need])
        if len(selected) < threads_per_worker:
            global_extra = [cpu for cpu in sorted(allowed) if cpu not in selected]
            need = threads_per_worker - len(selected)
            selected.extend(global_extra[:need])
        cpu_sets.append(sorted(selected))
    return cpu_sets


def set_process_cpu_affinity(cpu_ids: list[int] | tuple[int, ...] | None):
    if not cpu_ids or not hasattr(os, "sched_setaffinity"):
        return
    os.sched_setaffinity(0, {int(cpu) for cpu in cpu_ids})


def cpu_topology_summary():
    allowed = available_cpu_ids()
    topology = [row for row in _parse_lscpu_topology() if row["cpu"] in set(allowed)]
    summary = {
        "logical_cpus": len(allowed),
        "affinity_cpus": allowed,
        "socket_count": 1,
        "physical_cores": len(allowed),
        "cores_per_socket": len(allowed),
        "threads_per_core": 1,
    }
    if not topology:
        return summary

    sockets = sorted({int(row["socket"]) for row in topology})
    cores = {(int(row["socket"]), int(row["core"])) for row in topology}
    summary["socket_count"] = max(1, len(sockets))
    summary["physical_cores"] = max(1, len(cores))
    summary["cores_per_socket"] = max(1, summary["physical_cores"] // summary["socket_count"])
    summary["threads_per_core"] = max(1, int(round(len(topology) / float(summary["physical_cores"]))))
    return summary


def default_portable_cpu_runtime_config():
    return {
        "kind": "chgnet_runtime_config",
        "mode": "auto_cpu",
        "profile_name": "portable_cpu",
        "strategy": "coarse_to_fine",
        "coarse_grid_size": 5,
        "full_grid_size": 9,
        "refine_top_k": 24,
        "batch_size": "auto",
        "num_workers": "auto",
        "torch_threads": "auto",
        "interop_threads": 1,
        "worker_affinity": "auto",
        "chunksize": 1,
        "maxtasksperchild": 25,
        "limits": {
            "max_workers": 2,
            "max_threads_per_worker": 16,
            "max_batch_size": 16,
            "min_threads_per_worker": 1,
        },
    }


def _runtime_is_auto(value):
    return value is None or (isinstance(value, str) and value.strip().lower() == "auto")


def _coerce_runtime_value(key: str, value):
    if key == "worker_affinity":
        return str(value)
    if key == "strategy":
        return str(value)
    return int(value)


def _auto_cpu_runtime_from_profile(profile: dict, cpu_summary: dict | None = None):
    cpu_summary = cpu_summary or cpu_topology_summary()
    logical = max(1, int(cpu_summary.get("logical_cpus", 1)))
    physical = max(1, int(cpu_summary.get("physical_cores", logical)))
    sockets = max(1, int(cpu_summary.get("socket_count", 1)))
    limits = dict(profile.get("limits", {}))

    max_workers = max(1, int(limits.get("max_workers", 2)))
    max_threads = max(1, int(limits.get("max_threads_per_worker", 16)))
    min_threads = max(1, int(limits.get("min_threads_per_worker", 1)))
    max_batch = max(1, int(limits.get("max_batch_size", 16)))

    auto_workers = 1
    if sockets >= 2 and physical >= 16 and logical >= 24:
        auto_workers = 2
    auto_workers = min(max_workers, auto_workers)

    if physical >= 64:
        auto_threads = 16
    elif physical >= 32:
        auto_threads = 12
    elif physical >= 16:
        auto_threads = 8
    elif physical >= 8:
        auto_threads = 4
    else:
        auto_threads = physical

    auto_threads = min(max_threads, max(min_threads, auto_threads))
    auto_threads = min(auto_threads, max(1, physical // auto_workers))
    auto_threads = min(auto_threads, max(1, logical // auto_workers))
    if auto_threads < min_threads and auto_workers > 1:
        auto_workers = 1
        auto_threads = min(max_threads, max(min_threads, logical))
    auto_threads = max(min_threads, auto_threads)

    if logical < 12:
        auto_batch = 4
    elif logical < 32:
        auto_batch = 8
    else:
        auto_batch = 16
    auto_batch = min(max_batch, auto_batch)

    runtime = {
        "strategy": profile.get("strategy", "coarse_to_fine"),
        "coarse_grid_size": int(profile.get("coarse_grid_size", 5)),
        "full_grid_size": int(profile.get("full_grid_size", 9)),
        "refine_top_k": int(profile.get("refine_top_k", 24)),
        "batch_size": auto_batch if _runtime_is_auto(profile.get("batch_size", "auto")) else int(profile["batch_size"]),
        "num_workers": auto_workers if _runtime_is_auto(profile.get("num_workers", "auto")) else int(profile["num_workers"]),
        "torch_threads": auto_threads if _runtime_is_auto(profile.get("torch_threads", "auto")) else int(profile["torch_threads"]),
        "interop_threads": int(profile.get("interop_threads", 1)),
        "worker_affinity": "auto" if _runtime_is_auto(profile.get("worker_affinity", "auto")) else str(profile["worker_affinity"]),
        "chunksize": int(profile.get("chunksize", 1)),
        "maxtasksperchild": int(profile.get("maxtasksperchild", 25)),
    }
    if runtime["worker_affinity"] == "auto" and runtime["num_workers"] <= 1:
        runtime["worker_affinity"] = "off"
    return runtime


def _fixed_runtime_from_profile(profile: dict):
    runtime = {
        "strategy": profile.get("strategy", "coarse_to_fine"),
        "coarse_grid_size": int(profile.get("coarse_grid_size", 5)),
        "full_grid_size": int(profile.get("full_grid_size", 9)),
        "refine_top_k": int(profile.get("refine_top_k", 24)),
        "batch_size": int(profile.get("batch_size", 16)),
        "num_workers": int(profile.get("num_workers", 1)),
        "torch_threads": int(profile.get("torch_threads", 8)),
        "interop_threads": int(profile.get("interop_threads", 1)),
        "worker_affinity": str(profile.get("worker_affinity", "off")),
        "chunksize": int(profile.get("chunksize", 1)),
        "maxtasksperchild": int(profile.get("maxtasksperchild", 25)),
    }
    if runtime["worker_affinity"] == "auto" and runtime["num_workers"] <= 1:
        runtime["worker_affinity"] = "off"
    return runtime


def resolve_chgnet_runtime_config(config_path: Path | str | None = None, overrides: dict | None = None, cpu_summary: dict | None = None):
    payload = default_portable_cpu_runtime_config()
    source = "builtin_portable_auto"
    if config_path is not None:
        config_path = Path(config_path).expanduser().resolve()
        payload = json.loads(config_path.read_text())
        source = str(config_path)

    cpu_summary = cpu_summary or cpu_topology_summary()
    mode = str(payload.get("mode", "fixed")).lower()
    if mode == "auto_cpu":
        runtime = _auto_cpu_runtime_from_profile(payload, cpu_summary=cpu_summary)
    else:
        runtime = _fixed_runtime_from_profile(payload)

    for key, value in (overrides or {}).items():
        if key in RUNTIME_CONFIG_KEYS and value is not None:
            runtime[key] = _coerce_runtime_value(key, value)

    runtime["num_workers"] = max(1, int(runtime["num_workers"]))
    runtime["torch_threads"] = max(1, int(runtime["torch_threads"]))
    runtime["interop_threads"] = max(1, int(runtime["interop_threads"]))
    runtime["batch_size"] = max(1, int(runtime["batch_size"]))
    runtime["chunksize"] = max(1, int(runtime["chunksize"]))
    runtime["maxtasksperchild"] = max(1, int(runtime["maxtasksperchild"]))
    if runtime["worker_affinity"] not in {"off", "auto"}:
        runtime["worker_affinity"] = "off"

    meta = {
        "source": source,
        "mode": mode,
        "cpu_summary": cpu_summary,
    }
    return runtime, meta


def default_runtime_config_path(repo_root: Path):
    repo_root = Path(repo_root).expanduser().resolve()
    candidates = [
        repo_root / "server_highthroughput_workflow" / "env_reports" / "chgnet_runtime_config.json",
        repo_root / "server_highthroughput_workflow" / "portable_cpu_config.json",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def portable_profile_config_path(repo_root: Path, profile_name: str | None):
    repo_root = Path(repo_root).expanduser().resolve()
    profile = "default" if not profile_name else str(profile_name).strip().lower()
    mapping = {
        "default": repo_root / "server_highthroughput_workflow" / "portable_cpu_config.json",
        "small": repo_root / "server_highthroughput_workflow" / "portable_cpu_small.json",
        "medium": repo_root / "server_highthroughput_workflow" / "portable_cpu_medium.json",
        "large": repo_root / "server_highthroughput_workflow" / "portable_cpu_large.json",
    }
    path = mapping.get(profile)
    if path is None or not path.exists():
        return None
    return path


def select_runtime_config_path(repo_root: Path, profile_name: str | None = None):
    repo_root = Path(repo_root).expanduser().resolve()
    if profile_name is not None:
        return portable_profile_config_path(repo_root, profile_name)
    assessed = repo_root / "server_highthroughput_workflow" / "env_reports" / "chgnet_runtime_config.json"
    if assessed.exists():
        return assessed
    return portable_profile_config_path(repo_root, profile_name)


class BatchedCHGNetCalculator:
    def __init__(self, model, use_device: str = "cpu"):
        from chgnet.model.dynamics import CHGNetCalculator

        self._base = CHGNetCalculator(model=model, use_device=use_device)
        self.model = model
        self.use_device = use_device

    def __getattr__(self, item):
        return getattr(self._base, item)

    def predict_energies(self, atoms_list, batch_size: int = 16):
        from pymatgen.io.ase import AseAtomsAdaptor

        structures = [AseAtomsAdaptor.get_structure(atoms) for atoms in atoms_list]
        predictions = self.model.predict_structure(structures, task="e", batch_size=max(1, int(batch_size)))
        energies = []
        for idx, item in enumerate(predictions):
            extensive_factor = len(structures[idx]) if self.model.is_intensive else 1
            energies.append(float(item["e"]) * extensive_factor)
        return energies


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
        return {
            "stable": True,
            "thz": float(root * CONV_TO_THZ),
            "cm1": float(root * CONV_TO_CM1),
        }
    return {
        "stable": False,
        "imag_thz": float(np.sqrt(2.0 * abs(c2)) * CONV_TO_THZ),
        "imag_cm1": float(np.sqrt(2.0 * abs(c2)) * CONV_TO_CM1),
    }


def fit_1d_axis_quartic(a: np.ndarray, e: np.ndarray):
    design = np.column_stack([a**2, a**3, a**4, a, np.ones_like(a)])
    coeff, _, _, _ = np.linalg.lstsq(design, e, rcond=None)
    c2, c3, c4, c1, c0 = coeff
    pred = design @ coeff
    rmse = float(np.sqrt(np.mean((pred - e) ** 2)))
    return {
        "c2": float(c2),
        "c3": float(c3),
        "c4": float(c4),
        "c1": float(c1),
        "c0": float(c0),
        "rmse": rmse,
        "freq": freq_from_c2(float(c2)),
    }


def fit_polynomial(a1_vals: np.ndarray, a2_vals: np.ndarray, energies: np.ndarray, fit_window: float | None = None):
    x = np.repeat(a1_vals, len(a2_vals))
    y = np.tile(a2_vals, len(a1_vals))

    if fit_window is not None:
        mask = (np.abs(x) <= fit_window) & (np.abs(y) <= fit_window)
        x_fit = x[mask]
        y_fit = y[mask]
        e_fit = energies[mask]
    else:
        x_fit, y_fit, e_fit = x, y, energies

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

    params, _, _, _ = np.linalg.lstsq(design, e_fit, rcond=None)
    e_model_all = fit_func(np.vstack([x, y]), *params)
    residuals = e_model_all - energies
    sse = np.sum(residuals**2)
    sst = np.sum((energies - np.mean(energies)) ** 2)
    r2 = float(1.0 - sse / sst)
    rmse = float(np.sqrt(np.mean(residuals**2)))
    return params, residuals, r2, rmse


def extract_physics(params: np.ndarray):
    c20, c02, c12, c21, c30, c03, c11, c40, c04, c22, c10, c01, c00 = params
    return {
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
    }


def axis_frequency_checks(a1_vals: np.ndarray, a2_vals: np.ndarray, e_grid: np.ndarray):
    idx_a2_0 = int(np.argmin(np.abs(a2_vals)))
    idx_a1_0 = int(np.argmin(np.abs(a1_vals)))
    e_a1 = e_grid[idx_a2_0, :]
    e_a2 = e_grid[:, idx_a1_0]
    return {
        "mode1_axis_fit": fit_1d_axis_quartic(a1_vals, e_a1),
        "mode2_axis_fit": fit_1d_axis_quartic(a2_vals, e_a2),
    }


def compare_with_reference_grid(ref_grid_file: Path, ml_grid_ev_supercell: np.ndarray):
    if not ref_grid_file.exists():
        return None

    ref = np.loadtxt(ref_grid_file)
    if ref.ndim == 1:
        if ref.size != ml_grid_ev_supercell.size:
            return None
        ref = ref.reshape(ml_grid_ev_supercell.shape)
    elif ref.shape != ml_grid_ev_supercell.shape:
        return None

    if np.max(np.abs(ref)) < 1.0:
        ref_ev = ref * RY_TO_EV
    else:
        ref_ev = ref

    ref_rel = ref_ev - np.min(ref_ev)
    ml_rel = ml_grid_ev_supercell - np.min(ml_grid_ev_supercell)
    diff = ml_rel - ref_rel
    rmse = float(np.sqrt(np.mean(diff**2)))
    mae = float(np.mean(np.abs(diff)))
    ref_flat = ref_rel.reshape(-1)
    ml_flat = ml_rel.reshape(-1)
    sst = float(np.sum((ref_flat - np.mean(ref_flat)) ** 2))
    sse = float(np.sum((ml_flat - ref_flat) ** 2))
    r2 = float(1.0 - sse / sst)
    return {
        "rmse_ev_supercell": rmse,
        "mae_ev_supercell": mae,
        "r2_against_ref_shape": r2,
        "max_abs_diff_ev_supercell": float(np.max(np.abs(diff))),
    }


class ModePairFrozenPhononBuilder:
    def __init__(self, pair_record: dict, prim_atoms):
        self.pair_record = pair_record
        self.prim_atoms = prim_atoms
        self.gamma_mode = decode_complex_mode(pair_record["gamma_mode"]["eigenvector"])
        self.q_mode = decode_complex_mode(pair_record["target_mode"]["eigenvector_q"])
        self.q_frac = np.array(pair_record["target_mode"]["q_frac"], dtype=float)
        self.n_super = infer_commensurate_supercell_n(self.q_frac)
        self.nat_prim = len(self.prim_atoms)

        self.supercell = make_supercell(self.prim_atoms, [[self.n_super, 0, 0], [0, self.n_super, 0], [0, 0, 1]])
        self.n_cells = self.n_super * self.n_super
        self.base_cell = self.supercell.get_cell().array.copy()
        self.base_frac = self.supercell.get_scaled_positions().copy()
        self.cell_inv = np.linalg.inv(self.base_cell)
        self.prim_indices = np.arange(len(self.supercell), dtype=int) % self.nat_prim
        self.replica_r = np.array(
            [[i, j, 0] for i in range(self.n_super) for j in range(self.n_super) for _ in range(self.nat_prim)],
            dtype=float,
        )
        self.phase_q = np.exp(2j * np.pi * np.dot(self.replica_r, self.q_frac))
        self.gamma_super = self.gamma_mode[self.prim_indices]
        self.q_super = self.q_mode[self.prim_indices]
        masses = np.array([atomic_mass_from_symbol(s) for s in self.supercell.get_chemical_symbols()], dtype=float)
        self.mass_sqrt = np.sqrt(masses)[:, None]

    @property
    def nat_super(self):
        return len(self.supercell)

    def displacement_cart(self, a1: float, a2: float):
        u_complex = a1 * self.gamma_super + a2 * self.q_super * self.phase_q[:, None]
        u_complex = u_complex / np.sqrt(self.n_cells)
        return np.real(u_complex) / self.mass_sqrt

    def fractional_positions(self, a1: float, a2: float):
        frac = self.base_frac + self.displacement_cart(a1, a2) @ self.cell_inv
        frac[:, :2] %= 1.0
        return frac

    def displacement_scale(self, a1: float, a2: float):
        norms = np.linalg.norm(self.displacement_cart(a1, a2), axis=1)
        return float(np.max(norms)), float(np.mean(norms))

    def build_atoms(self, a1: float, a2: float):
        atoms = self.supercell.copy()
        atoms.set_scaled_positions(self.fractional_positions(a1, a2))
        return atoms

    def build_atoms_list(self, a1_vals: np.ndarray, a2_vals: np.ndarray):
        index_map = []
        atoms_list = []
        for i_a2, a2 in enumerate(a2_vals):
            for j_a1, a1 in enumerate(a1_vals):
                index_map.append((i_a2, j_a1, float(a1), float(a2)))
                atoms_list.append(self.build_atoms(float(a1), float(a2)))
        return index_map, atoms_list

    def evaluate_grid(self, calc, a1_vals: np.ndarray, a2_vals: np.ndarray, row_callback=None, batch_size: int = 1):
        grid = np.zeros((len(a2_vals), len(a1_vals)), dtype=float)
        if batch_size > 1 and hasattr(calc, "predict_energies"):
            index_map, atoms_list = self.build_atoms_list(a1_vals, a2_vals)
            energies = calc.predict_energies(atoms_list, batch_size=batch_size)
            for (i_a2, j_a1, _a1, _a2), energy in zip(index_map, energies):
                grid[i_a2, j_a1] = float(energy)
            if row_callback is not None:
                for i_a2, a2 in enumerate(a2_vals):
                    row_callback(i_a2, float(a2))
            return grid

        for i_a2, a2 in enumerate(a2_vals):
            for j_a1, a1 in enumerate(a1_vals):
                atoms = self.build_atoms(float(a1), float(a2))
                atoms.calc = calc
                grid[i_a2, j_a1] = float(atoms.get_potential_energy())
            if row_callback is not None:
                row_callback(i_a2, float(a2))
        return grid

    def metadata(self):
        return {
            "pair_code": self.pair_record["pair_code"],
            "n_super": self.n_super,
            "n_cells": self.n_cells,
            "nat_prim": self.nat_prim,
            "nat_super": self.nat_super,
            "q_frac": self.q_frac.tolist(),
            "normalization": "u = Re[(A1 e_Gamma + A2 e_q exp(i qR))/sqrt(N_cells)]/sqrt(M)",
        }


def evaluate_pair_grid(
    pair_record: dict,
    structure_path: Path | None,
    calc,
    a1_vals: np.ndarray,
    a2_vals: np.ndarray,
    row_callback=None,
    prim_atoms=None,
    batch_size: int = 1,
):
    if prim_atoms is None:
        if structure_path is None:
            raise ValueError("structure_path is required when prim_atoms is not provided.")
        prim_atoms = load_atoms_from_qe(structure_path)
    builder = ModePairFrozenPhononBuilder(pair_record, prim_atoms)
    grid = builder.evaluate_grid(calc, a1_vals, a2_vals, row_callback=row_callback, batch_size=batch_size)
    return grid, builder


def analyze_pair_grid(pair_record: dict, e_grid_ev_supercell: np.ndarray, a1_vals: np.ndarray, a2_vals: np.ndarray, fit_window: float | None = 1.0):
    e_shift = e_grid_ev_supercell - np.min(e_grid_ev_supercell)
    params, residuals, r2, rmse = fit_polynomial(a1_vals, a2_vals, e_shift.T.reshape(-1), fit_window=fit_window)
    physics = extract_physics(params)
    axis = axis_frequency_checks(a1_vals, a2_vals, e_shift)
    mode_pair_reference = {
        "reference_kind": "mode_pair_frequency",
        "reference_label": "selected_mode_pair_frequency",
        "gamma_freq_thz": float(pair_record["gamma_mode"]["freq_thz"]),
        "target_freq_thz": float(pair_record["target_mode"]["freq_thz"]),
    }
    return {
        "fit_window": fit_window,
        "r2": r2,
        "rmse_ev_supercell": rmse,
        "max_abs_residual_ev_supercell": float(np.max(np.abs(residuals))),
        "physics": physics,
        "axis_checks": axis,
        "mode_pair_reference": mode_pair_reference,
        "reference": mode_pair_reference,
    }


def save_pair_plot(path: Path, e_grid: np.ndarray, a1_vals: np.ndarray, a2_vals: np.ndarray, title: str):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(6.0, 5.0))
    c = ax.contourf(a1_vals, a2_vals, e_grid - np.min(e_grid), levels=30, cmap="viridis")
    fig.colorbar(c, ax=ax)
    ax.set_xlabel("A1")
    ax.set_ylabel("A2")
    ax.set_aspect("equal")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def dump_json(path: Path, payload: dict):
    path.write_text(json.dumps(payload, indent=2))


def choose_device(device_hint: str = "auto"):
    if device_hint != "auto":
        return device_hint
    try:
        import torch
    except Exception:
        return "cpu"

    if torch.cuda.is_available():
        return "cuda"
    if getattr(torch.backends, "mps", None) is not None and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def resolve_gptff_model_path(model: str | Path | None = None) -> Path:
    if model not in {None, "", "auto"}:
        alias = GPTFF_MODEL_ALIASES.get(str(model).strip().lower())
        if alias is not None:
            model = alias
        else:
            path = Path(model).expanduser().resolve()
            if not path.exists():
                raise FileNotFoundError(f"GPTFF model file not found: {path}")
            return path

    env_model = os.environ.get("GPTFF_MODEL_PATH")
    if env_model:
        path = Path(env_model).expanduser().resolve()
        if path.exists():
            return path

    spec = importlib.util.find_spec("gptff")
    if spec is not None:
        for location in spec.submodule_search_locations or []:
            candidate_root = Path(str(location)).expanduser().resolve().parent
            candidate_name = str(model).strip() if model not in {None, "", "auto"} else DEFAULT_GPTFF_MODEL_NAME
            candidate = candidate_root / "pretrained" / candidate_name
            if candidate.exists():
                return candidate

    raise FileNotFoundError(
        "Could not resolve a GPTFF model path. Provide --model or set GPTFF_MODEL_PATH."
    )


def gptff_backend_meta(model_path: Path, device: str) -> dict:
    return {
        "backend": "gptff",
        "device": device,
        "model": str(model_path),
        "model_version": model_path.stem,
        "published_error_metrics": {
            "energy_mae_mev_per_atom": 32.0,
            "force_mae_mev_per_angstrom": 71.0,
            "stress_mae_gpa": 0.365,
        },
        "error_source_urls": [
            "https://github.com/atomly-materials-research-lab/GPTFF",
            "https://doi.org/10.1016/j.scib.2024.08.039",
        ],
    }


def make_calculator(backend: str, device: str = "auto", model: str | None = None, default_dtype: str | None = None):
    backend = backend.lower()
    chosen_device = choose_device(device)

    if backend == "chgnet":
        from chgnet.model import CHGNet

        model_name = model or "0.3.0"
        chgnet_model = CHGNet.load(model_name=model_name, use_device=chosen_device, verbose=True)
        calc = BatchedCHGNetCalculator(model=chgnet_model, use_device=chosen_device)
        return calc, {"backend": backend, "device": chosen_device, "model": model_name}

    if backend == "mace":
        from mace.calculators import MACECalculator, mace_mp

        dtype = "float64" if default_dtype is None else default_dtype
        if dtype == "float64" and chosen_device == "mps":
            chosen_device = "cpu"
        if model is None:
            calc = mace_mp(device=chosen_device, default_dtype=dtype)
            model_desc = "mace_mp_default"
        else:
            calc = MACECalculator(model_paths=str(Path(model).expanduser().resolve()), device=chosen_device, default_dtype=dtype)
            model_desc = str(Path(model).expanduser().resolve())
        return calc, {"backend": backend, "device": chosen_device, "model": model_desc, "default_dtype": dtype}

    if backend == "gptff":
        from gptff.model.mpredict import ASECalculator

        model_path = resolve_gptff_model_path(model)
        calc = ASECalculator(str(model_path), chosen_device)
        return calc, gptff_backend_meta(model_path, chosen_device)

    if backend == "mattersim":
        _ensure_mattersim_ase_compat()
        from mattersim.forcefield import MatterSimCalculator

        model_name = DEFAULT_MATTERSIM_MODEL if model in {None, "", "auto"} else str(model)
        if chosen_device == "mps":
            chosen_device = "cpu"
        calc = MatterSimCalculator.from_checkpoint(load_path=model_name, device=chosen_device)
        return calc, {
            "backend": "mattersim",
            "device": chosen_device,
            "model": model_name,
            "model_version": "mattersim_v1_5m",
            "source": "official_mattersim_1.0",
        }

    raise ValueError(f"Unsupported backend: {backend}")


def load_pairs(mode_pairs_json: Path):
    return json.loads(mode_pairs_json.read_text())["pairs"]


def find_golden_pair(pairs):
    for pair in pairs:
        if pair["gamma_mode"]["mode_number_one_based"] == 8 and pair["target_mode"]["point_label"] == "M" and pair["target_mode"]["mode_number_one_based"] == 3:
            return pair
    raise RuntimeError("Golden pair Gamma mode 8 + M mode 3 not found")


def load_mode_pair_reference(pair_record: dict):
    return {
        "reference_kind": "mode_pair_frequency",
        "reference_label": "selected_mode_pair_frequency",
        "pair_code": pair_record["pair_code"],
        "gamma_freq_thz": float(pair_record["gamma_mode"]["freq_thz"]),
        "target_freq_thz": float(pair_record["target_mode"]["freq_thz"]),
    }


def load_golden_reference(fit_json: Path):
    payload = json.loads(fit_json.read_text())
    physics = payload["physics"]
    return {
        "reference_kind": "golden_pes_fit",
        "reference_label": "n7_golden_pes_fit",
        "gamma_freq_thz": float(physics["freq_mode1"]["thz"]),
        "target_freq_thz": float(physics["freq_mode2"]["thz"]),
        "phi122_mev_per_A3amu32": float(physics["phi_122_mev_per_A3amu32"]),
        "fit_json": str(fit_json),
    }


def compare_mode_frequency_metrics(analysis: dict, mode_pair_reference: dict):
    gamma_fit = analysis["axis_checks"]["mode1_axis_fit"]["freq"]
    target_fit = analysis["axis_checks"]["mode2_axis_fit"]["freq"]
    return {
        "reference_kind": mode_pair_reference["reference_kind"],
        "reference_label": mode_pair_reference["reference_label"],
        "gamma_freq_ref_thz": mode_pair_reference["gamma_freq_thz"],
        "gamma_freq_fit_thz": gamma_fit.get("thz"),
        "gamma_freq_abs_error_thz": None if not gamma_fit["stable"] else abs(float(gamma_fit["thz"]) - mode_pair_reference["gamma_freq_thz"]),
        "target_freq_ref_thz": mode_pair_reference["target_freq_thz"],
        "target_freq_fit_thz": target_fit.get("thz"),
        "target_freq_abs_error_thz": None if not target_fit["stable"] else abs(float(target_fit["thz"]) - mode_pair_reference["target_freq_thz"]),
    }


def compare_golden_metrics(analysis: dict, golden_reference: dict):
    gamma_fit = analysis["axis_checks"]["mode1_axis_fit"]["freq"]
    target_fit = analysis["axis_checks"]["mode2_axis_fit"]["freq"]
    phi_fit = float(analysis["physics"]["phi_122_mev_per_A3amu32"])

    return {
        "reference_kind": golden_reference.get("reference_kind", "golden_pes_fit"),
        "reference_label": golden_reference.get("reference_label", "golden_pes_fit"),
        "gamma_freq_ref_thz": golden_reference["gamma_freq_thz"],
        "gamma_freq_fit_thz": gamma_fit.get("thz"),
        "gamma_freq_abs_error_thz": None if not gamma_fit["stable"] else abs(float(gamma_fit["thz"]) - golden_reference["gamma_freq_thz"]),
        "target_freq_ref_thz": golden_reference["target_freq_thz"],
        "target_freq_fit_thz": target_fit.get("thz"),
        "target_freq_abs_error_thz": None if not target_fit["stable"] else abs(float(target_fit["thz"]) - golden_reference["target_freq_thz"]),
        "phi122_ref_mev_per_A3amu32": golden_reference["phi122_mev_per_A3amu32"],
        "phi122_fit_mev_per_A3amu32": phi_fit,
        "phi122_abs_error_mev_per_A3amu32": abs(phi_fit - golden_reference["phi122_mev_per_A3amu32"]),
    }
