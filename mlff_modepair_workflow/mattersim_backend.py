"""Pinned MatterSim calculator for the v3 Stage2 comparison."""

from __future__ import annotations

from importlib.metadata import version
from pathlib import Path

from .prophet_backend import sha256_file


MATTERSIM_VERSION = "1.2.1"
MATTERSIM_5M_SHA256 = "e3df9fa708725e3d453140646c7d1838324b347a3d1214cf1440522146f872b5"


def make_mattersim_calculator(model: str | Path, device: str, atoms=None):
    if device not in {"cpu", "cuda"}:
        raise ValueError(f"MatterSim device must be cpu or cuda, got {device!r}")
    path = Path(model).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"MatterSim checkpoint is missing: {path}")
    digest = sha256_file(path)
    if digest != MATTERSIM_5M_SHA256:
        raise ValueError(f"MatterSim checkpoint SHA256 mismatch: {digest}")
    installed = version("mattersim")
    if installed != MATTERSIM_VERSION:
        raise RuntimeError(f"MatterSim {MATTERSIM_VERSION} is required; found {installed}")
    if atoms is not None and (not all(atoms.pbc) or len(atoms) == 0):
        raise ValueError("MatterSim Stage2 requires a nonempty periodic structure")
    # MatterSim 1.2.1 still imports this name from ase.constraints; recent ASE
    # releases moved it to ase.stress. Restore the same function before import.
    import ase.constraints
    from ase.stress import full_3x3_to_voigt_6_stress

    ase.constraints.full_3x3_to_voigt_6_stress = full_3x3_to_voigt_6_stress
    from mattersim.forcefield import MatterSimCalculator

    calculator = MatterSimCalculator.from_checkpoint(load_path=str(path), device=device)
    return calculator, {
        "backend": "mattersim",
        "checkpoint": str(path),
        "checkpoint_sha256": digest,
        "source_commit": f"mattersim-pypi-{installed}",
        "model_name": "mattersim-v1.0.0-5M",
        "device": device,
        "effective_batch_size": 1,
        "energy_accumulation": "MatterSimCalculator_total_energy_eV_v1",
    }
