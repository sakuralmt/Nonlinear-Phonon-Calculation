"""Compare MLFF and QE eigenvectors across an explicit commensurate q mesh."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import numpy as np

from mlff_modepair_workflow.core import load_atoms_from_qe
from mlff_modepair_workflow.phonon_eigenvectors import (
    compare_modes,
    dynamical_matrix,
    frequencies_and_vectors,
    read_matdyn_q_mesh,
    read_qe_eigenvectors,
    real_space_force_constants,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _calculator(backend: str, checkpoint: Path, device: str):
    if backend == "gptff":
        from gptff.model.mpredict import ASECalculator

        return ASECalculator(str(checkpoint), device)
    if backend == "prophet":
        from prophet import KairosCalculator

        return KairosCalculator(model_path=checkpoint, use_kernel=False, use_compile=False, device=device)
    if backend == "mattersim":
        import ase.constraints

        if not hasattr(ase.constraints, "full_3x3_to_voigt_6_stress"):
            from ase.stress import full_3x3_to_voigt_6_stress

            ase.constraints.full_3x3_to_voigt_6_stress = full_3x3_to_voigt_6_stress
        from mattersim.forcefield import MatterSimCalculator

        return MatterSimCalculator.from_checkpoint(
            load_path=str(checkpoint), device=device, load_training_state=False
        )
    raise ValueError(backend)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["gptff", "prophet", "mattersim"], required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--matdyn-input", type=Path, required=True)
    parser.add_argument("--qe-eig", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mesh", type=int, default=6)
    parser.add_argument("--step", type=float, default=0.01)
    parser.add_argument("--device", default="cpu")
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args(argv)
    import torch

    torch.set_num_threads(args.threads)
    try:
        torch.set_num_interop_threads(1)
    except RuntimeError:
        pass
    primitive = load_atoms_from_qe(args.structure)
    q = read_matdyn_q_mesh(args.matdyn_input)
    mesh_points = np.rint(q[:, :2] * args.mesh).astype(int) % args.mesh
    if (len(q) != args.mesh**2
            or not np.allclose(q[:, :2] * args.mesh, np.rint(q[:, :2] * args.mesh), atol=1e-7)
            or not np.allclose(q[:, 2], 0)
            or len({tuple(point) for point in mesh_points}) != args.mesh**2):
        raise ValueError("matdyn q list is not the specified complete commensurate in-plane mesh")
    qe_freq, qe_vec = read_qe_eigenvectors(args.qe_eig, len(primitive), q)
    calculator = _calculator(args.backend, args.checkpoint, args.device)
    start = time.perf_counter()
    phi = real_space_force_constants(primitive, calculator, args.mesh, args.step)
    force_time = time.perf_counter() - start
    print(f"{args.backend}: measured {phi.shape[-2] * 3 * 2} displaced structures in {force_time:.1f} s", flush=True)
    records = []
    all_freq = []
    all_vec = []
    masses = primitive.get_masses()
    for iq, point in enumerate(q):
        matrix, hermitian_error = dynamical_matrix(phi, masses, point)
        freq, vec = frequencies_and_vectors(matrix)
        assignment, overlaps, groups = compare_modes(
            qe_freq[iq], qe_vec[iq], freq, vec, gamma_acoustic=bool(np.allclose(point, 0))
        )
        all_freq.append(freq)
        all_vec.append(vec)
        records.append({
            "q_index": iq,
            "q_frac": point.tolist(),
            "qe_freq_thz": qe_freq[iq].tolist(),
            "ml_freq_thz": freq.tolist(),
            "qe_to_ml_mode_one_based": (assignment + 1).tolist(),
            "individual_overlap_squared": overlaps.tolist(),
            "matched_frequency_error_thz": (freq[assignment] - qe_freq[iq]).tolist(),
            "degeneracy_groups": groups,
            "hermitian_relative_error_before_symmetrizing": hermitian_error,
        })
    isolated = [g["subspace_overlap"] for row in records for g in row["degeneracy_groups"] if len(g["qe_modes"]) == 1]
    subspaces = [g["subspace_overlap"] for row in records for g in row["degeneracy_groups"] if len(g["qe_modes"]) > 1]
    payload = {
        "backend": args.backend,
        "device": args.device,
        "threads": args.threads,
        "mesh": args.mesh,
        "step_angstrom": args.step,
        "force_evaluation_count": 2 * 3 * len(primitive),
        "force_evaluation_elapsed_seconds": force_time,
        "structure": str(args.structure.resolve()),
        "structure_sha256": _sha256(args.structure),
        "matdyn_input": str(args.matdyn_input.resolve()),
        "matdyn_input_sha256": _sha256(args.matdyn_input),
        "qe_eig": str(args.qe_eig.resolve()),
        "qe_eig_sha256": _sha256(args.qe_eig),
        "checkpoint": str(args.checkpoint.resolve()),
        "checkpoint_sha256": _sha256(args.checkpoint),
        "isolated_mode_count": len(isolated),
        "isolated_mode_median_overlap_squared": float(np.median(isolated)),
        "degenerate_group_count": len(subspaces),
        "degenerate_group_median_subspace_overlap": float(np.median(subspaces)) if subspaces else None,
        "q_records": records,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    np.savez_compressed(args.output.with_suffix(".npz"), phi=phi, q=q, qe_freq=qe_freq, qe_vectors=qe_vec, ml_freq=np.array(all_freq), ml_vectors=np.array(all_vec))
    print(json.dumps({key: payload[key] for key in ("isolated_mode_count", "isolated_mode_median_overlap_squared", "degenerate_group_count", "degenerate_group_median_subspace_overlap")}), flush=True)


if __name__ == "__main__":
    sys.exit(main())
