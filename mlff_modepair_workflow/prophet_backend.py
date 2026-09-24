"""Pinned and validated Prophet OAME-MBD ASE calculator adapter."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import resource
import sys
from pathlib import Path

import numpy as np


PROPHET_COMMIT = "c4fda8251d8a7c90c7cb7842aea4d2f57e5fc3bd"
OAME_MBD_SHA256 = "28b21122f4c6c1a7c5fe9bac8a0182edf9d24a7a65a72a9ca80b8d12d4620514"
SOURCE_SHA256 = {
    "__init__.py": "d66208b3d4dcb83ad690078142d14041f1f47865d414d29f992015943e1f4437",
    "calculator.py": "9f63cd6140492d3fda241875ffa0c9f22078a0698509bc0124a138577ccda8af",
    "model.py": "7a13b59e17f7c94bc597a4c07f564845ddf730464d6d3ca8cd44c7c34c375364",
    "graph.py": "5b826319d8ef87bacb7f8ecc4e250935b7581c4f672e13f31c8e0cc07898f3ef",
    "layer_norm.py": "1bd3c57a6c8f9f189412a466b769866cf75c681f56e45b4e0f0ab48789f34c52",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def process_resource_metrics(device: str) -> dict:
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rss_mb = maxrss / (1024 * 1024) if sys.platform == "darwin" else maxrss / 1024
    metrics = {
        "peak_process_rss_mb": float(rss_mb),
        "peak_gpu_allocated_mb": None,
        "peak_gpu_reserved_mb": None,
    }
    if device.startswith("cuda"):
        import torch

        metrics["peak_gpu_allocated_mb"] = float(
            torch.cuda.max_memory_allocated() / 2**20
        )
        metrics["peak_gpu_reserved_mb"] = float(
            torch.cuda.max_memory_reserved() / 2**20
        )
    return metrics


def resolve_checkpoint(model: str | Path | None) -> Path:
    spec = None if model in {None, "", "auto", "prophet_oame_mbd"} else model
    if spec is None:
        spec = os.environ.get("NPC_PROPHET_CHECKPOINT")
    if not spec:
        raise ValueError(
            "Prophet requires --model/--prophet-checkpoint or NPC_PROPHET_CHECKPOINT"
        )
    path = Path(spec).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Prophet checkpoint not found: {path}")
    found = sha256_file(path)
    if found != OAME_MBD_SHA256:
        raise ValueError(
            f"Unverified Prophet checkpoint {path}: sha256={found}; expected {OAME_MBD_SHA256}"
        )
    return path


def checkpoint_config(path: Path) -> dict:
    with path.open("rb") as handle:
        config = json.loads(handle.readline().decode("utf-8"))
    if not isinstance(config.get("atomic_numbers"), list):
        raise ValueError("Prophet checkpoint has no atomic_numbers list")
    return config


def validate_source() -> Path:
    explicit_source = os.environ.get("NPC_PROPHET_SOURCE")
    if explicit_source:
        source = Path(explicit_source).expanduser().resolve()
        if not (source / "prophet" / "__init__.py").is_file():
            raise FileNotFoundError(
                f"NPC_PROPHET_SOURCE does not contain the prophet package: {source}"
            )
        if str(source) not in sys.path:
            sys.path.insert(0, str(source))
    spec = importlib.util.find_spec("prophet")
    if spec is None or spec.origin is None:
        raise ImportError(
            "Pinned prophet-mlip is not installed; install source commit "
            + PROPHET_COMMIT
        )
    root = Path(spec.origin).resolve().parent
    if explicit_source and root != source / "prophet":
        raise ValueError(f"Imported Prophet from {root}, expected {source / 'prophet'}")
    for name, expected in SOURCE_SHA256.items():
        path = root / name
        if not path.is_file() or sha256_file(path) != expected:
            raise ValueError(
                f"Prophet source differs from validated commit {PROPHET_COMMIT}: {path}"
            )
    return root


def validate_atoms(atoms, allowed_atomic_numbers: set[int]) -> None:
    numbers = set(map(int, atoms.get_atomic_numbers()))
    missing = sorted(numbers - allowed_atomic_numbers)
    if missing:
        raise ValueError(
            f"Prophet checkpoint does not support atomic numbers {missing}"
        )
    masses = np.asarray(atoms.get_masses(), dtype=float)
    if np.any(~np.isfinite(masses)) or np.any(masses <= 0):
        raise ValueError("All atomic masses must be finite and positive")
    if not bool(np.all(atoms.pbc)):
        raise ValueError(
            "Prophet 2D phonon workflow requires three periodic flags and explicit vacuum along c"
        )
    if abs(float(np.linalg.det(atoms.cell.array))) < 1e-8:
        raise ValueError("Prophet requires a nonsingular cell")


def make_prophet_calculator(model: str | Path | None, device: str, atoms=None):
    path = resolve_checkpoint(model)
    source_root = validate_source()
    config = checkpoint_config(path)
    allowed = set(map(int, config["atomic_numbers"]))
    if atoms is not None:
        validate_atoms(atoms, allowed)
    if device not in {"cpu", "cuda"} and not device.startswith("cuda:"):
        raise ValueError(f"Validated Prophet devices are cpu and cuda, got {device!r}")
    if device.startswith("cuda"):
        import torch

        if not torch.cuda.is_available():
            raise RuntimeError("CUDA was requested but is unavailable")
    from ase.calculators.calculator import Calculator
    from ase.stress import full_3x3_to_voigt_6_stress
    import torch
    from prophet import KairosCalculator
    from prophet.graph import dict_to_pytorch_geometric, preprocess_graph

    class PreciseSumKairosCalculator(KairosCalculator):
        """Keep the pinned model, but sum its per-atom float32 energies in float64."""

        def calculate(self, atoms=None, properties=None, system_changes=None):
            Calculator.calculate(self, atoms)
            graph = dict_to_pytorch_geometric(
                preprocess_graph(atoms, self.atom_indices, self.cutoff)
            )
            graph.n_graph = torch.zeros(graph.x.shape[0], dtype=torch.int64).to(
                self.device
            )
            graph = graph.to(self.device)
            if not self.compile_state:
                raise RuntimeError(
                    "The validated Prophet adapter does not enable torch.compile"
                )
            per_atom, forces, stress = self.model(
                graph.x,
                graph.positions,
                graph.edge_attr,
                graph.edge_index,
                getattr(graph, "cell", None),
                graph.n_node,
                graph.n_edge,
                graph.n_graph,
            )
            energy = float(per_atom.detach().to(torch.float64).sum().cpu())
            self.results["energy"] = energy
            self.results["free_energy"] = energy
            self.results["forces"] = np.asarray(forces.detach().cpu())
            self.results["stress"] = (
                full_3x3_to_voigt_6_stress(np.asarray(stress.detach().cpu()[0]))
                if stress is not None
                else None
            )

    calc = PreciseSumKairosCalculator(
        model_path=path, use_kernel=False, use_compile=False, device=device
    )
    meta = {
        "backend": "prophet",
        "checkpoint": str(path),
        "checkpoint_sha256": OAME_MBD_SHA256,
        "source_commit": PROPHET_COMMIT,
        "source_root": str(source_root),
        "model_name": config.get("model_name"),
        "supported_atomic_numbers": sorted(allowed),
        "device": device,
        "use_kernel": False,
        "use_compile": False,
        "effective_batch_size": 1,
        "energy_accumulation": "float64_sum_of_float32_atomic_energies_v1",
    }
    return calc, meta
