"""Isolated ASE Stage1 adapter for the three pinned Matbench Discovery models."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import subprocess
import sys
import time
from pathlib import Path

import numpy as np

from .core import load_atoms_from_qe
from .model_relaxation import relax_structure_with_calculator
from .phonon_eigenvectors import real_space_force_constants
from .prophet_backend import process_resource_metrics, sha256_file
from .prophet_stage1 import finite_q_orbits, mode_pairs_from_phonons, phonons_from_force_constants
from .units import CONTRACT_VERSION, NORMALIZATION_VERSION, UNITS
from qe_phonon_stage1_server_bundle.qpair_tools.common import is_hexagonal_2d


MODEL_SOURCES = {
    "tece-oam-rra-1.0": {
        "repository": "https://github.com/xvzemin/tace",
        "source_commit": "81f65a4c188bd09cec8d1419388f7afdcc1b6fd0",
        "source_tree_sha256": "cb7f58c76a07072d1e61949055c930c61f061ce84f274e150cb3bb5d1e7968b8",
        "checkpoint_url": "https://huggingface.co/xvzemin/tace-foundations/resolve/main/TECE-OAM-RRA-1.0.pt",
        "python": "3.13", "torch": "2.13.0", "precision": "float32",
    },
    "equflashv2-45m-oam": {
        "repository": "https://github.com/SamsungDS/GGNN",
        "source_commit": "16b5cae474370977b59120e8bc57e4bcc19cd093",
        "source_tree_sha256": "f025884b75bb1b16b653101caf84390888ed00896cfc8a74308a58241c218164",
        "checkpoint_url": "https://figshare.com/ndownloader/files/65435007",
        "python": "3.12", "torch": "2.9.1+cu126", "precision": "model_default",
    },
    "equiformer-v3-oam": {
        "repository": "https://github.com/atomicarchitects/equiformer_v3",
        "source_commit": "a7300c58df683dc99cb48027d5bfd4c887486c48",
        "source_tree_sha256": "3e15a029e8e1ea534e979f5548293eb1e10841d4c86317fd4576ee1d7acc922e",
        "checkpoint_url": "https://huggingface.co/mirror-physics/equiformer_v3/tree/main/checkpoint",
        "python": "3.12", "torch": "2.4.0", "precision": "model_default",
    },
}


def _source_tree_sha256(source_root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(Path(source_root).rglob("*")):
        relative = path.relative_to(source_root)
        if (not path.is_file() or ".git" in relative.parts or "__pycache__" in relative.parts
                or path.suffix == ".pyc"):
            continue
        name = relative.as_posix().encode()
        data = path.read_bytes()
        digest.update(len(name).to_bytes(8, "big"))
        digest.update(name)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def _check_source(model_name: str, source_root: Path) -> dict:
    spec = MODEL_SOURCES[model_name]
    source_root = Path(source_root).resolve()
    tree_hash = _source_tree_sha256(source_root)
    if tree_hash != spec["source_tree_sha256"]:
        raise ValueError(f"{model_name} source files differ from pinned commit export: {tree_hash}")
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=source_root,
                                capture_output=True, text=True)
    except FileNotFoundError:
        # Compute nodes may not provide Git; the pinned full source-tree hash
        # above still verifies the exact code used for inference.
        head = None
    else:
        head = result.stdout.strip() if result.returncode == 0 else None
    if head is not None and head != spec["source_commit"]:
        raise ValueError(f"{model_name} source checkout differs from pinned commit: {head}")
    return {**spec, "source_root": str(source_root), "actual_source_commit": head or spec["source_commit"],
            "verified_source_tree_sha256": tree_hash}


def make_advanced_calculator(model_name: str, checkpoint: Path, device: str, source_root: Path):
    if model_name not in MODEL_SOURCES:
        raise ValueError(f"Unknown advanced Stage1 model: {model_name}")
    if device not in {"cpu", "cuda"}:
        raise ValueError("Stage1 device must be cpu or cuda")
    checkpoint = Path(checkpoint).resolve()
    if not checkpoint.is_file():
        raise FileNotFoundError(checkpoint)
    source = _check_source(model_name, source_root)
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))
    if model_name == "equiformer-v3-oam" and str(Path(source_root) / "src") not in sys.path:
        sys.path.insert(0, str(Path(source_root) / "src"))
    if model_name == "tece-oam-rra-1.0":
        from tace.interface.ase import TACEAseCalc
        calculator = TACEAseCalc(str(checkpoint), device=device, dtype="float32")
    elif model_name == "equflashv2-45m-oam":
        from GGNN.common.calculator import UCalculator
        calculator = UCalculator(checkpoint_path=str(checkpoint), cpu=device == "cpu")
    else:
        # Register the official EquiformerV3/DeNS architecture before OCPCalculator
        # reconstructs the model from the checkpoint's stored configuration.
        importlib.import_module("experimental.models.equiformer_v3.equiformer_v3")
        importlib.import_module("experimental.models.equiformer_v3.equiformer_v3_dens")
        from fairchem.core.common.relaxation.ase_utils import OCPCalculator
        calculator = OCPCalculator(checkpoint_path=str(checkpoint), cpu=device == "cpu")
    metadata = {"backend": model_name, "checkpoint": str(checkpoint),
                "checkpoint_sha256": sha256_file(checkpoint), **source,
                "device": device, "units": {"energy": "eV", "force": "eV/Angstrom"}}
    return calculator, metadata


def preflight_calculator(primitive, calculator, device: str, step: float = 0.005) -> dict:
    if not bool(np.all(primitive.pbc)) or step <= 0:
        raise ValueError("Advanced Stage1 needs a periodic structure and positive difference step")
    atoms = primitive.copy()
    atoms.calc = calculator
    start = time.perf_counter()
    energy = float(atoms.get_potential_energy())
    forces = np.asarray(atoms.get_forces(apply_constraint=False), dtype=float)
    if forces.shape != (len(atoms), 3) or not np.isfinite(energy) or not np.isfinite(forces).all():
        raise ValueError("Advanced model returned nonfinite or wrong-shaped energy/forces")
    repeated = atoms.copy()
    repeated.calc = calculator
    repeat_energy = float(repeated.get_potential_energy())
    repeat_forces = np.asarray(repeated.get_forces(apply_constraint=False), dtype=float)
    # A relaxed high-symmetry atom can have both F_x and dE/dx exactly zero.
    # Probe a displaced configuration so the conservative-force check is not
    # passed vacuously by a model with a flat or inconsistent response.
    probe_displacement = max(0.03, 6 * step)
    probe = primitive.copy()
    probe.positions[0, 0] += probe_displacement
    probe.calc = calculator
    probe_forces = np.asarray(probe.get_forces(apply_constraint=False), dtype=float)
    if probe_forces.shape != forces.shape or not np.isfinite(probe_forces).all():
        raise ValueError("Advanced model returned invalid displaced forces")
    plus, minus = probe.copy(), probe.copy()
    plus.positions[0, 0] += step
    minus.positions[0, 0] -= step
    plus.calc = minus.calc = calculator
    energy_plus = float(plus.get_potential_energy())
    energy_minus = float(minus.get_potential_energy())
    slope = (energy_plus - energy_minus) / (2 * step)
    repeat_e = abs(repeat_energy - energy)
    repeat_f = float(np.max(np.abs(repeat_forces - forces)))
    probe_force = float(probe_forces[0, 0])
    conservative_error = abs(slope + probe_force)
    conservative_limit = max(0.02, 0.2 * abs(probe_force))
    return {
        "natoms": len(primitive), "symbols": primitive.get_chemical_symbols(),
        "energy_eV": energy, "force_shape": list(forces.shape),
        "repeat_energy_difference_eV": repeat_e,
        "repeat_force_max_difference_eV_per_A": repeat_f,
        "force_energy_difference_eV_per_A": conservative_error,
        "force_energy_difference_step_A": step,
        "force_energy_probe_displacement_A": probe_displacement,
        "force_energy_probe_force_eV_per_A": probe_force,
        "force_energy_probe_slope_eV_per_A": slope,
        "force_energy_probe_energy_span_eV": energy_plus - energy_minus,
        "preflight_limits": {"repeat_energy_eV": 1e-3,
                             "repeat_force_eV_per_A": 1e-3,
                             "force_energy_eV_per_A": conservative_limit},
        "passed": bool(repeat_e <= 1e-3 and repeat_f <= 1e-3
                       and abs(probe_force) > 1e-4
                       and conservative_error <= conservative_limit),
        "elapsed_seconds": time.perf_counter() - start,
        "resources": process_resource_metrics(device),
    }


def run_advanced_stage1(structure: Path, checkpoint: Path, source_root: Path,
                        model_name: str, output_dir: Path, *, device: str = "cuda",
                        mesh_n: int = 6, step: float = 0.01,
                        convergence_step: float = 0.005,
                        geometry_source: str = "shared_dft",
                        preflight_only: bool = False,
                        relax_only: bool = False) -> tuple[Path, Path, Path] | Path:
    if geometry_source not in {"shared_dft", "model_relaxed"}:
        raise ValueError(f"Unsupported advanced Stage1 geometry source: {geometry_source}")
    if preflight_only and relax_only:
        raise ValueError("Choose preflight-only or relax-only, not both")
    if relax_only and geometry_source != "model_relaxed":
        raise ValueError("Relax-only requires model_relaxed geometry source")
    structure = Path(structure).resolve()
    output_dir = Path(output_dir).resolve()
    primitive = load_atoms_from_qe(structure)
    hexagonal, details = is_hexagonal_2d(primitive.cell.array, 0.05, 3.0)
    if not hexagonal:
        raise ValueError(f"Advanced Stage1 requires a hexagonal in-plane cell: {details}")
    calculator, model_meta = make_advanced_calculator(model_name, checkpoint, device, source_root)
    preflight = preflight_calculator(primitive, calculator, device)
    output_dir.mkdir(parents=True, exist_ok=True)
    initial_preflight_path = output_dir / (
        "preflight.initial.json" if geometry_source == "model_relaxed" else "preflight.json"
    )
    initial_preflight_path.write_text(json.dumps({
        "model": model_meta, "structure": str(structure),
        "structure_sha256": sha256_file(structure), "preflight": preflight,
    }, indent=2) + "\n")
    if not preflight["passed"]:
        raise ValueError(f"Advanced Stage1 preflight failed; inspect {initial_preflight_path}")
    if preflight_only:
        return initial_preflight_path
    relaxation = None
    if geometry_source == "model_relaxed":
        structure, relaxation = relax_structure_with_calculator(
            structure, output_dir / "relax", calculator, model_meta, model_name
        )
        primitive = load_atoms_from_qe(structure)
        preflight = preflight_calculator(primitive, calculator, device)
        (output_dir / "preflight.json").write_text(json.dumps({
            "model": model_meta, "structure": str(structure),
            "structure_sha256": sha256_file(structure), "preflight": preflight,
        }, indent=2) + "\n")
        if not preflight["passed"]:
            raise ValueError(f"Relaxed advanced Stage1 preflight failed; inspect {output_dir / 'preflight.json'}")
        if relax_only:
            return output_dir / "relax" / "relax_summary.json"
    start = time.perf_counter()
    phi = real_space_force_constants(primitive, calculator, mesh_n, step)
    records = phonons_from_force_constants(phi, primitive.get_masses(), mesh_n)
    convergence = None
    if convergence_step is not None:
        smaller = real_space_force_constants(primitive, calculator, mesh_n, convergence_step)
        other = phonons_from_force_constants(smaller, primitive.get_masses(), mesh_n)
        differences = np.asarray([abs(a - b) for left, right in zip(records, other)
                                  for a, b in zip(left["freqs_thz"], right["freqs_thz"])])
        convergence = {"step_A": convergence_step,
                       "max_frequency_change_THz": float(np.max(differences)),
                       "median_frequency_change_THz": float(np.median(differences))}
    orbits = finite_q_orbits(mesh_n)
    pairs = mode_pairs_from_phonons(records, orbits, len(primitive))
    source = {
        "backend": model_name, "model": model_meta, "structure": str(structure),
        "structure_sha256": sha256_file(structure), "natoms_primitive": len(primitive),
        "symbols": primitive.get_chemical_symbols(), "masses_amu": primitive.get_masses().tolist(),
        "q_grid": [mesh_n, mesh_n, 1], "finite_difference_step_angstrom": step,
        "geometry_source": geometry_source, "normalization_version": NORMALIZATION_VERSION,
        "units": UNITS,
    }
    if relaxation is not None:
        relax_path = output_dir / "relax" / "relax_summary.json"
        source["relaxation"] = {
            "source_structure_sha256": relaxation["source_structure_sha256"],
            "optimized_structure_sha256": relaxation["optimized_structure_sha256"],
            "summary": str(relax_path), "summary_sha256": sha256_file(relax_path),
            "protocol_version": relaxation["relaxation_protocol_version"],
        }
    output_dir.mkdir(parents=True, exist_ok=True)
    arrays = {"force_constants_ev_per_A2": phi}
    if convergence_step is not None:
        arrays["force_constants_convergence_ev_per_A2"] = smaller
    force_constants = output_dir / "force_constants.npz"
    np.savez_compressed(force_constants, **arrays)
    phonon = output_dir / "phonon_dataset.json"
    phonon.write_text(json.dumps({
        "kind": "advanced_mlff_phonon_mesh", "version": CONTRACT_VERSION,
        "source": source, "q_points": records, "q_orbits": orbits,
        "diagnostics": {"preflight": preflight, "convergence": convergence,
                        "elapsed_seconds": time.perf_counter() - start,
                        "resources": process_resource_metrics(device)},
    }, indent=2) + "\n")
    pair_file = output_dir / "mode_pairs.selected.json"
    pair_file.write_text(json.dumps({"kind": "mode_pairs_qgamma_qpair", "version": CONTRACT_VERSION,
                                     "source": source, "selection": "momentum_conservation_only",
                                     "finite_q_orbits": orbits, "pairs": pairs}, indent=2) + "\n")
    return pair_file, phonon, force_constants


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--model", choices=sorted(MODEL_SOURCES), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--device", choices=["cpu", "cuda"], default="cuda")
    parser.add_argument("--mesh-n", type=int, default=6)
    parser.add_argument("--step", type=float, default=0.01)
    parser.add_argument("--convergence-step", type=float, default=0.005)
    parser.add_argument("--geometry-source", choices=["shared_dft", "model_relaxed"], default="shared_dft",
                        help="Use the given DFT geometry or relax it with the selected Stage1 model first")
    parser.add_argument("--preflight-only", action="store_true",
                        help="Check periodic energy, forces, repeatability and force-energy consistency without a phonon run")
    parser.add_argument("--relax-only", action="store_true",
                        help="Preflight and relax this model's own geometry, then stop before the phonon mesh")
    args = parser.parse_args(argv)
    print(run_advanced_stage1(args.structure, args.checkpoint, args.source_root,
                              args.model, args.output_dir, device=args.device,
                              mesh_n=args.mesh_n, step=args.step,
                              convergence_step=args.convergence_step,
                              geometry_source=args.geometry_source,
                              preflight_only=args.preflight_only,
                              relax_only=args.relax_only))


if __name__ == "__main__":
    main()
