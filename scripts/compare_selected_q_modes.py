"""Compare archived selected QE modes where a complete qeph.eig is unavailable."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import numpy as np
import torch

from compare_full_grid_eigenvectors import _calculator
from mlff_modepair_workflow.core import decode_complex_mode, load_atoms_from_qe
from mlff_modepair_workflow.phonon_eigenvectors import (
    dynamical_matrix,
    frequencies_and_vectors,
    real_space_force_constants,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=["gptff", "mattersim", "prophet"], required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--pairs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mesh", type=int, default=6)
    parser.add_argument("--step", type=float, default=0.01)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--device", default="cpu")
    args = parser.parse_args()
    torch.set_num_threads(args.threads)
    try:
        torch.set_num_interop_threads(1)
    except RuntimeError:
        pass
    atoms = load_atoms_from_qe(args.structure)
    pairs = json.loads(args.pairs.read_text())["pairs"]
    calc = _calculator(args.backend, args.checkpoint, args.device)
    start = time.perf_counter()
    phi = real_space_force_constants(atoms, calc, args.mesh, args.step)
    elapsed = time.perf_counter() - start
    q_results = {}
    records = []
    for pair in pairs:
        target = pair["target_mode"]
        q = np.asarray(target["q_frac"], dtype=float)
        key = tuple(np.round(q, 8))
        if key not in q_results:
            matrix, hermitian_error = dynamical_matrix(phi, atoms.get_masses(), q)
            frequencies, vectors = frequencies_and_vectors(matrix)
            q_results[key] = (frequencies, vectors, hermitian_error)
        frequencies, vectors, hermitian_error = q_results[key]
        qe_vector = decode_complex_mode(target["eigenvector_q"]).reshape(-1)
        qe_vector /= np.linalg.norm(qe_vector)
        overlap = np.abs(qe_vector.conj() @ vectors) ** 2
        match = int(np.argmax(overlap))
        records.append({
            "pair_code": pair["pair_code"],
            "q_frac": q.tolist(),
            "qe_mode_one_based": target["mode_number_one_based"],
            "qe_freq_thz": target["freq_thz"],
            "ml_mode_one_based": match + 1,
            "ml_freq_thz": float(frequencies[match]),
            "freq_error_thz": float(frequencies[match] - target["freq_thz"]),
            "overlap_squared": float(overlap[match]),
            "hermitian_relative_error_before_symmetrizing": hermitian_error,
        })
    result = {
        "backend": args.backend,
        "mesh": args.mesh,
        "step_angstrom": args.step,
        "force_evaluation_count": 2 * 3 * len(atoms),
        "force_evaluation_elapsed_seconds": elapsed,
        "structure": str(args.structure.resolve()),
        "structure_sha256": hashlib.sha256(args.structure.read_bytes()).hexdigest(),
        "pairs": str(args.pairs.resolve()),
        "pairs_sha256": hashlib.sha256(args.pairs.read_bytes()).hexdigest(),
        "checkpoint": str(args.checkpoint.resolve()),
        "checkpoint_sha256": hashlib.sha256(args.checkpoint.read_bytes()).hexdigest(),
        "records": records,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2))
    np.savez_compressed(args.output.with_suffix(".npz"), phi=phi)
    print(args.backend, f"{len(records)} selected modes, {elapsed:.1f} s", flush=True)


if __name__ == "__main__":
    main()
