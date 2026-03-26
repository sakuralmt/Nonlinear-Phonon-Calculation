#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from ase import Atoms
from phonopy import Phonopy
from phonopy.structure.atoms import PhonopyAtoms

from core import make_calculator, load_atoms_from_qe


ROOT = Path(__file__).resolve().parent.parent
QE_EXTRACTED = ROOT / "hex_qgamma_qpair_workflow" / "hex_qgamma_qpair_run" / "extracted" / "screened_eigenvectors.json"
QE_STRUCTURE = ROOT / "nonlocal phonon" / "scf.inp"
OUT_DIR = Path(__file__).resolve().parent / "eigenvector_checks"
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
    "mace_20231210_l0_relaxed": {
        "backend": "mace",
        "model": str(Path.home() / ".cache" / "mace" / "20231210mace128L0_energy_epoch249model"),
        "structure": ROOT / "mlff_modepair_workflow" / "runs" / "mace_20231210_l0_relaxed" / "relax" / "relaxed_structure.extxyz",
        "device": "cpu",
    },
    "gptff_best": {
        "backend": "gptff",
        "model": str(ROOT / "GPTFF" / "pretrained" / "gptff_v2.pth"),
        "structure": ROOT / "nonlocal phonon" / "scf.inp",
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
    if isinstance(mode[0], dict):
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
    return np.array(
        [
            [
                float(vec[0][0]) + 1j * float(vec[0][1]),
                float(vec[1][0]) + 1j * float(vec[1][1]),
                float(vec[2][0]) + 1j * float(vec[2][1]),
            ]
            for vec in mode
        ],
        dtype=np.complex128,
    )


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


def make_calc(config):
    if config["backend"] == "gptff":
        from gptff.model.mpredict import ASECalculator

        return ASECalculator(str(config["model"]), config["device"])
    calc, _meta = make_calculator(config["backend"], device=config["device"], model=config["model"])
    return calc


def run_phonopy(prim_atoms, calc, supercell_matrix, qpoints):
    phonon = Phonopy(atoms_to_phonopy(prim_atoms), supercell_matrix, primitive_matrix=None, is_symmetry=True, symprec=1e-5)
    phonon.generate_displacements(distance=0.01)
    forces = []
    for sc in phonon.supercells_with_displacements:
        at = phonopy_supercell_to_ase(sc)
        at.calc = calc
        forces.append(at.get_forces())
    phonon.forces = forces
    phonon.produce_force_constants()
    phonon.run_qpoints(qpoints, with_eigenvectors=True)
    qd = phonon.get_qpoints_dict()
    return np.array(qd["frequencies"], dtype=float), np.array(qd["eigenvectors"], dtype=np.complex128)


def compare_run(run_tag: str, points_to_check: list[str]):
    config = RUN_CONFIGS[run_tag]
    prim_atoms = load_atoms_from_qe(Path(config["structure"]))
    calc = make_calc(config)

    qe_points = json.loads(QE_EXTRACTED.read_text())["points"]
    qe_map = {item["label"]: item for item in qe_points if item["label"] in points_to_check}
    qpoints = [qe_map[label]["q_target_frac"] for label in points_to_check]
    freqs, eigs = run_phonopy(prim_atoms, calc, [[6, 0, 0], [0, 6, 0], [0, 0, 1]], qpoints)

    masses = np.array([MASS_DICT[s] for s in prim_atoms.get_chemical_symbols()], dtype=float)

    point_reports = []
    for iq, label in enumerate(points_to_check):
        qe_point = qe_map[label]
        qe_freqs = np.array(qe_point["freqs_thz"], dtype=float)
        qe_modes = [decode_qe_mode(mode) for mode in qe_point["modes"]]

        best_phase_mode = None
        best_phase_score = -1.0
        overlap_rows = []
        for add_phase in (False, True):
            overlap_matrix = np.zeros((len(qe_modes), len(qe_modes)))
            for i in range(len(qe_modes)):
                for j in range(len(qe_modes)):
                    mlff_mode = extract_phonopy_mode(prim_atoms, qpoints[iq], eigs[iq], j, add_basis_phase=add_phase)
                    overlap_matrix[i, j] = mass_weighted_overlap(qe_modes[i], mlff_mode, masses)
            score = float(np.mean(np.max(overlap_matrix, axis=1)))
            overlap_rows.append({"add_basis_phase": add_phase, "mean_best_overlap": score, "overlap_matrix": overlap_matrix.tolist()})
            if score > best_phase_score:
                best_phase_score = score
                best_phase_mode = add_phase
                best_overlap = overlap_matrix

        matches = []
        used_mlff = set()
        for i in range(best_overlap.shape[0]):
            order = np.argsort(best_overlap[i])[::-1]
            chosen = None
            for j in order:
                if int(j) not in used_mlff:
                    chosen = int(j)
                    used_mlff.add(chosen)
                    break
            if chosen is None:
                chosen = int(order[0])
            matches.append(
                {
                    "qe_mode_zero_based": i,
                    "qe_mode_number_one_based": i + 1,
                    "qe_freq_thz": float(qe_freqs[i]),
                    "mlff_mode_zero_based": chosen,
                    "mlff_mode_number_one_based": chosen + 1,
                    "mlff_freq_thz": float(freqs[iq, chosen]),
                    "overlap": float(best_overlap[i, chosen]),
                    "freq_diff_thz": float(freqs[iq, chosen] - qe_freqs[i]),
                }
            )

        point_reports.append(
            {
                "label": label,
                "q_frac": qe_point["q_target_frac"],
                "best_basis_phase_mode": best_phase_mode,
                "best_phase_score": best_phase_score,
                "phase_trials": overlap_rows,
                "matches": matches,
            }
        )

    return {
        "run_tag": run_tag,
        "config": {k: (str(v) if isinstance(v, Path) else v) for k, v in config.items()},
        "point_reports": point_reports,
    }


def parse_args():
    p = argparse.ArgumentParser(description="Compare MLFF phonon eigenvectors against QE eigenvectors at selected q points")
    p.add_argument("--run-tags", nargs="+", default=["gptff_best", "chgnet_r2scan_relaxed", "mace_20231210_l0_relaxed"])
    p.add_argument("--points", nargs="+", default=["Gamma", "M", "K"])
    p.add_argument("--output-dir", type=str, default=str(OUT_DIR))
    return p.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    reports = []
    for tag in args.run_tags:
        rep = compare_run(tag, args.points)
        reports.append(rep)
        out = output_dir / f"{tag}_qpoint_comparison.json"
        out.write_text(json.dumps(rep, indent=2))
        print(f"saved: {out}")
        for point in rep["point_reports"]:
            overlaps = [m["overlap"] for m in point["matches"]]
            freq_errs = [abs(m["freq_diff_thz"]) for m in point["matches"]]
            print(
                f"{tag} | {point['label']}: "
                f"mean overlap={np.mean(overlaps):.3f}, "
                f"min overlap={np.min(overlaps):.3f}, "
                f"mean |domega|={np.mean(freq_errs):.3f} THz"
            )


if __name__ == "__main__":
    main()
