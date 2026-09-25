"""Pinned MatterSim calculator for the stable Stage2 workflow."""

from __future__ import annotations

from importlib.metadata import version
from pathlib import Path

from .prophet_backend import sha256_file


MATTERSIM_VERSION = "1.2.1"
MATTERSIM_5M_SHA256 = "e3df9fa708725e3d453140646c7d1838324b347a3d1214cf1440522146f872b5"
ENERGY_ACCUMULATION = "atomic_float32_sum_float64_v2"


def _atomic_energy_to_double(module, inputs, output):
    """Promote atomic energies before the pinned M3GNet total-energy scatter.

    The network still uses its original float32 weights and features. Promoting
    before summation avoids order-dependent rounding of large supercell totals;
    casting the final ASE scalar to double would be too late. Autograd propagates
    through this cast. This instance-local hook does not patch upstream globals.
    """
    return output.double()


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
        raise RuntimeError(
            f"MatterSim {MATTERSIM_VERSION} is required; found {installed}"
        )
    if atoms is not None and (not all(atoms.pbc) or len(atoms) == 0):
        raise ValueError("MatterSim Stage2 requires a nonempty periodic structure")
    # MatterSim 1.2.1 still imports this name from ase.constraints; recent ASE
    # releases moved it to ase.stress. Restore the same function before import.
    import ase.constraints
    from ase.stress import full_3x3_to_voigt_6_stress

    ase.constraints.full_3x3_to_voigt_6_stress = full_3x3_to_voigt_6_stress
    from mattersim.forcefield import MatterSimCalculator

    calculator = MatterSimCalculator.from_checkpoint(load_path=str(path), device=device)
    if calculator.potential.model_name != "m3gnet":
        raise RuntimeError("The pinned accumulation adapter requires M3GNet")
    calculator.potential.model.normalizer.register_forward_hook(
        _atomic_energy_to_double
    )
    return calculator, {
        "backend": "mattersim",
        "checkpoint": str(path),
        "checkpoint_sha256": digest,
        "source_commit": f"mattersim-pypi-{installed}",
        "model_name": "mattersim-v1.0.0-5M",
        "device": device,
        "effective_batch_size": 1,
        "energy_accumulation": ENERGY_ACCUMULATION,
    }
