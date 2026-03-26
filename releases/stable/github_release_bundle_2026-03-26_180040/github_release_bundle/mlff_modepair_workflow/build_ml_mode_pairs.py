#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import csv
import json
from fractions import Fraction
from pathlib import Path

import numpy as np
from ase import Atoms
from phonopy import Phonopy
from phonopy.structure.atoms import PhonopyAtoms

from core import load_atoms_from_qe, make_calculator


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_QE_EXTRACTED = ROOT / "hex_qgamma_qpair_workflow" / "hex_qgamma_qpair_run" / "extracted" / "screened_eigenvectors.json"
DEFAULT_QE_MODE_PAIRS = ROOT / "hex_qgamma_qpair_workflow" / "hex_qgamma_qpair_run" / "mode_pairs" / "selected_mode_pairs.json"
DEFAULT_OUT_ROOT = SCRIPT_DIR / "runs"
MASS_DICT = {"W": 183.84, "Se": 78.960}


RUN_CONFIGS = {
    "chgnet_qe": {
        "backend": "chgnet",
        "model": None,
        "structure": ROOT / "nonlocal phonon" / "scf.inp",
        "device": "auto",
    },
    "chgnet_r2scan_relaxed": {
        "backend": "chgnet",
        "model": "r2scan",
        "structure": ROOT / "mlff_modepair_workflow" / "runs" / "chgnet_r2scan_relaxed" / "relax" / "relaxed_structure.extxyz",
        "device": "auto",
    },
    "chgnet_0_2_relaxed": {
        "backend": "chgnet",
        "model": "0.2.0",
        "structure": ROOT / "mlff_modepair_workflow" / "runs" / "chgnet_0_2_relaxed" / "relax" / "relaxed_structure.extxyz",
        "device": "auto",
    },
    "mace_20231210_l0_relaxed": {
        "backend": "mace",
        "model": str(Path.home() / ".cache" / "mace" / "20231210mace128L0_energy_epoch249model"),
        "structure": ROOT / "mlff_modepair_workflow" / "runs" / "mace_20231210_l0_relaxed" / "relax" / "relaxed_structure.extxyz",
        "device": "cpu",
    },
}


def atoms_to_phonopy(atoms):
    return PhonopyAtoms(
        symbols=atoms.get_chemical_symbols(),
        cell=atoms.get_cell().array,
        scaled_positions=atoms.get_scaled_positions(),
    )


def phonopy_supercell_to_ase(supercell):
    return Atoms(
        symbols=supercell.symbols,
        cell=np.array(supercell.cell, dtype=float),
        scaled_positions=np.array(supercell.scaled_positions, dtype=float),
        pbc=[True, True, True],
    )


def decode_qe_mode(mode):
    if not mode:
        return np.zeros((0, 3), dtype=np.complex128)
    first = mode[0]
    if isinstance(first, dict):
        return np.array(
            [
                [
                    float(vec["x"]["re"]) + 1j * float(vec["x"]["im"]),
                    float(vec["y"]["re"]) + 1j * float(vec["y"]["im"]),
                    float(vec["z"]["re"]) + 1j * float(vec["z"]["im"]),
                ]
                for vec in mode
            ],
            dtype=np.complex128,
        )
    return np.array([[c[0] + 1j * c[1] for c in vec] for vec in mode], dtype=np.complex128)


def encode_complex_mode(mode):
    return [
        {
            "x": {"re": float(vec[0].real), "im": float(vec[0].imag)},
            "y": {"re": float(vec[1].real), "im": float(vec[1].imag)},
            "z": {"re": float(vec[2].real), "im": float(vec[2].imag)},
        }
        for vec in np.array(mode, dtype=np.complex128)
    ]


def infer_shared_supercell(points: list[dict], max_denominator: int = 12):
    denominators = []
    for point in points:
        q = point["q_target_frac"]
        for value in q[:2]:
            frac = Fraction(str(float(value))).limit_denominator(max_denominator)
            denominators.append(frac.denominator)
    n = 1
    for d in denominators:
        n = (n * d) // np.gcd(n, d)
    return [[int(n), 0, 0], [0, int(n), 0], [0, 0, 1]]


def extract_phonopy_mode(prim_atoms, q_frac, eig_matrix, mode_idx, add_basis_phase: bool):
    vec = np.array(eig_matrix[:, mode_idx], dtype=np.complex128).reshape(len(prim_atoms), 3)
    if add_basis_phase:
        tau = np.array(prim_atoms.get_scaled_positions(), dtype=float)
        vec = vec * np.exp(2j * np.pi * np.dot(tau, np.array(q_frac, dtype=float)))[:, None]
    ref = vec.reshape(-1)[np.argmax(np.abs(vec.reshape(-1)))]
    vec = vec * np.exp(-1j * np.angle(ref))
    return vec


def mass_weighted_overlap(qe_vec, mlff_vec, masses):
    a = (qe_vec * np.sqrt(masses)[:, None]).reshape(-1)
    b = (mlff_vec * np.sqrt(masses)[:, None]).reshape(-1)
    num = np.vdot(a, b)
    den = np.linalg.norm(a) * np.linalg.norm(b)
    return 0.0 if den == 0 else float(abs(num) / den)


def align_mode_phase_to_reference(ref_vec, test_vec, masses):
    a = (np.array(ref_vec, dtype=np.complex128) * np.sqrt(masses)[:, None]).reshape(-1)
    b = (np.array(test_vec, dtype=np.complex128) * np.sqrt(masses)[:, None]).reshape(-1)
    num = np.vdot(a, b)
    if abs(num) == 0.0:
        return np.array(test_vec, dtype=np.complex128), 0.0
    aligned = np.array(test_vec, dtype=np.complex128) * np.exp(-1j * np.angle(num))
    return aligned, float(abs(num) / (np.linalg.norm(a) * np.linalg.norm(b)))


def solve_assignment(overlap_matrix: np.ndarray):
    try:
        from scipy.optimize import linear_sum_assignment

        row_ind, col_ind = linear_sum_assignment(-overlap_matrix)
        return {int(i): int(j) for i, j in zip(row_ind.tolist(), col_ind.tolist())}
    except Exception:
        assignment = {}
        used = set()
        for i in range(overlap_matrix.shape[0]):
            order = np.argsort(overlap_matrix[i])[::-1]
            for j in order:
                jj = int(j)
                if jj not in used:
                    assignment[i] = jj
                    used.add(jj)
                    break
        return assignment


def run_phonopy(prim_atoms, calc, supercell_matrix, qpoints):
    phonon = Phonopy(atoms_to_phonopy(prim_atoms), supercell_matrix, primitive_matrix=None, is_symmetry=True, symprec=1e-5)
    phonon.generate_displacements(distance=0.01)
    forces = []
    for sc in phonon.supercells_with_displacements:
        atoms = phonopy_supercell_to_ase(sc)
        atoms.calc = calc
        forces.append(atoms.get_forces())
    phonon.forces = forces
    phonon.produce_force_constants()
    phonon.run_qpoints(qpoints, with_eigenvectors=True)
    qd = phonon.get_qpoints_dict()
    return np.array(qd["frequencies"], dtype=float), np.array(qd["eigenvectors"], dtype=np.complex128)


def build_mode_alignment(run_tag: str, qe_extracted_json: Path):
    qe_points = json.loads(qe_extracted_json.read_text())["points"]
    qe_points = sorted(qe_points, key=lambda item: int(item["target_index"]))
    config = RUN_CONFIGS[run_tag]
    prim_atoms = load_atoms_from_qe(Path(config["structure"]))
    calc, backend_meta = make_calculator(config["backend"], device=config["device"], model=config["model"])

    qpoints = [point["q_target_frac"] for point in qe_points]
    supercell_matrix = infer_shared_supercell(qe_points)
    freqs, eigs = run_phonopy(prim_atoms, calc, supercell_matrix, qpoints)

    masses = np.array([MASS_DICT[s] for s in prim_atoms.get_chemical_symbols()], dtype=float)
    aligned_points = []
    for iq, qe_point in enumerate(qe_points):
        qe_modes = [decode_qe_mode(mode) for mode in qe_point["modes"]]
        best_phase_mode = None
        best_phase_score = -1.0
        best_overlap = None
        best_vectors = None
        phase_trials = []
        for add_phase in (False, True):
            overlap_matrix = np.zeros((len(qe_modes), len(qe_modes)))
            ml_vectors = []
            for j in range(len(qe_modes)):
                ml_mode = extract_phonopy_mode(prim_atoms, qe_point["q_target_frac"], eigs[iq], j, add_basis_phase=add_phase)
                ml_vectors.append(ml_mode)
            for i, qe_mode in enumerate(qe_modes):
                for j, ml_mode in enumerate(ml_vectors):
                    overlap_matrix[i, j] = mass_weighted_overlap(qe_mode, ml_mode, masses)
            score = float(np.mean(np.max(overlap_matrix, axis=1)))
            phase_trials.append(
                {
                    "add_basis_phase": add_phase,
                    "mean_best_overlap": score,
                    "overlap_matrix": overlap_matrix.tolist(),
                }
            )
            if score > best_phase_score:
                best_phase_score = score
                best_phase_mode = add_phase
                best_overlap = overlap_matrix
                best_vectors = ml_vectors

        assignment = solve_assignment(best_overlap)
        matched_modes = []
        for qe_idx in range(len(qe_modes)):
            ml_idx = assignment[qe_idx]
            ml_vec, aligned_overlap = align_mode_phase_to_reference(qe_modes[qe_idx], best_vectors[ml_idx], masses)
            matched_modes.append(
                {
                    "qe_mode_index_zero_based": qe_idx,
                    "qe_mode_number_one_based": qe_idx + 1,
                    "ml_mode_index_zero_based": ml_idx,
                    "ml_mode_number_one_based": ml_idx + 1,
                    "freq_thz": float(freqs[iq, ml_idx]),
                    "overlap": aligned_overlap,
                    "vector": ml_vec,
                }
            )

        aligned_points.append(
            {
                "target_index": int(qe_point["target_index"]),
                "label": qe_point["label"],
                "q_target_frac": qe_point["q_target_frac"],
                "supercell_matrix": supercell_matrix,
                "best_basis_phase_mode": best_phase_mode,
                "best_phase_score": best_phase_score,
                "phase_trials": phase_trials,
                "matched_modes": matched_modes,
            }
        )

    return {
        "run_tag": run_tag,
        "backend": backend_meta,
        "structure": str(Path(config["structure"]).resolve()),
        "supercell_matrix": supercell_matrix,
        "aligned_points": aligned_points,
    }


def build_mode_pairs_from_alignment(qe_mode_pairs_json: Path, alignment: dict):
    qe_pairs = json.loads(qe_mode_pairs_json.read_text())["pairs"]
    point_map = {int(point["target_index"]): point for point in alignment["aligned_points"]}
    out_pairs = []
    for pair in qe_pairs:
        new_pair = copy.deepcopy(pair)

        gamma_idx = int(pair["gamma_mode"]["mode_index_zero_based"])
        gamma_point = point_map[int(pair["gamma_mode"]["point_index"])]
        gamma_match = gamma_point["matched_modes"][gamma_idx]
        new_pair["gamma_mode"]["freq_thz"] = gamma_match["freq_thz"]
        new_pair["gamma_mode"]["eigenvector"] = encode_complex_mode(gamma_match["vector"])
        new_pair["gamma_mode"]["source"] = "ml_phonopy_matched_to_qe"
        new_pair["gamma_mode"]["source_ml_mode_index_zero_based"] = gamma_match["ml_mode_index_zero_based"]
        new_pair["gamma_mode"]["source_ml_mode_number_one_based"] = gamma_match["ml_mode_number_one_based"]
        new_pair["gamma_mode"]["overlap_to_qe"] = gamma_match["overlap"]

        target_idx = int(pair["target_mode"]["mode_index_zero_based"])
        target_point = point_map[int(pair["target_mode"]["point_index"])]
        target_match = target_point["matched_modes"][target_idx]
        new_pair["target_mode"]["freq_thz"] = target_match["freq_thz"]
        new_pair["target_mode"]["eigenvector_q"] = encode_complex_mode(target_match["vector"])
        new_pair["target_mode"]["eigenvector_qbar_by_conjugation"] = encode_complex_mode(np.conjugate(target_match["vector"]))
        new_pair["target_mode"]["source"] = "ml_phonopy_matched_to_qe"
        new_pair["target_mode"]["source_ml_mode_index_zero_based"] = target_match["ml_mode_index_zero_based"]
        new_pair["target_mode"]["source_ml_mode_number_one_based"] = target_match["ml_mode_number_one_based"]
        new_pair["target_mode"]["overlap_to_qe"] = target_match["overlap"]

        out_pairs.append(new_pair)

    return {"kind": "mode_pairs_qgamma_qpair_ml_aligned", "pairs": out_pairs}


def write_outputs(output_dir: Path, alignment: dict, mode_pairs_payload: dict):
    output_dir.mkdir(parents=True, exist_ok=True)

    aligned_json = output_dir / "aligned_qpoints.json"
    serializable_alignment = copy.deepcopy(alignment)
    for point in serializable_alignment["aligned_points"]:
        for mode in point["matched_modes"]:
            mode["vector"] = encode_complex_mode(mode["vector"])
    aligned_json.write_text(json.dumps(serializable_alignment, indent=2))

    mode_pairs_json = output_dir / "selected_mode_pairs.json"
    mode_pairs_json.write_text(json.dumps(mode_pairs_payload, indent=2))

    summary_csv = output_dir / "qpoint_mode_alignment_summary.csv"
    with summary_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "target_index",
                "label",
                "qx",
                "qy",
                "qz",
                "qe_mode_number",
                "ml_mode_number",
                "ml_freq_thz",
                "overlap_to_qe",
                "best_phase_score",
            ]
        )
        for point in alignment["aligned_points"]:
            q = point["q_target_frac"]
            for mode in point["matched_modes"]:
                writer.writerow(
                    [
                        point["target_index"],
                        point["label"],
                        f"{q[0]:.6f}",
                        f"{q[1]:.6f}",
                        f"{q[2]:.6f}",
                        mode["qe_mode_number_one_based"],
                        mode["ml_mode_number_one_based"],
                        f"{mode['freq_thz']:.6f}",
                        f"{mode['overlap']:.6f}",
                        f"{point['best_phase_score']:.6f}",
                    ]
                )

    pair_csv = output_dir / "selected_mode_pairs.csv"
    with pair_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "pair_code",
                "point_label",
                "qx",
                "qy",
                "qz",
                "gamma_mode_number",
                "gamma_freq_thz_ml",
                "gamma_overlap_to_qe",
                "target_mode_number",
                "target_freq_thz_ml",
                "target_overlap_to_qe",
            ]
        )
        for pair in mode_pairs_payload["pairs"]:
            q = pair["target_mode"]["q_frac"]
            writer.writerow(
                [
                    pair["pair_code"],
                    pair["target_mode"]["point_label"],
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    pair["gamma_mode"]["mode_number_one_based"],
                    f"{pair['gamma_mode']['freq_thz']:.6f}",
                    f"{pair['gamma_mode']['overlap_to_qe']:.6f}",
                    pair["target_mode"]["mode_number_one_based"],
                    f"{pair['target_mode']['freq_thz']:.6f}",
                    f"{pair['target_mode']['overlap_to_qe']:.6f}",
                ]
            )

    return {
        "aligned_json": aligned_json,
        "mode_pairs_json": mode_pairs_json,
        "summary_csv": summary_csv,
        "pair_csv": pair_csv,
    }


def parse_args():
    p = argparse.ArgumentParser(description="Build ML-derived mode pairs by matching phonopy+ML modes to existing QE labels.")
    p.add_argument("--run-tag", type=str, default="chgnet_r2scan_relaxed", choices=sorted(RUN_CONFIGS))
    p.add_argument("--qe-extracted-json", type=str, default=str(DEFAULT_QE_EXTRACTED))
    p.add_argument("--qe-mode-pairs-json", type=str, default=str(DEFAULT_QE_MODE_PAIRS))
    p.add_argument("--output-dir", type=str, default=None)
    return p.parse_args()


def main():
    args = parse_args()
    qe_extracted_json = Path(args.qe_extracted_json).expanduser().resolve()
    qe_mode_pairs_json = Path(args.qe_mode_pairs_json).expanduser().resolve()
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir is not None
        else (DEFAULT_OUT_ROOT / f"{args.run_tag}_ml_modes" / "ml_modes")
    )

    alignment = build_mode_alignment(args.run_tag, qe_extracted_json)
    mode_pairs_payload = build_mode_pairs_from_alignment(qe_mode_pairs_json, alignment)
    outputs = write_outputs(output_dir, alignment, mode_pairs_payload)

    print(f"run_tag: {args.run_tag}")
    print(f"saved: {outputs['aligned_json']}")
    print(f"saved: {outputs['mode_pairs_json']}")
    print(f"saved: {outputs['summary_csv']}")
    print(f"saved: {outputs['pair_csv']}")


if __name__ == "__main__":
    main()
