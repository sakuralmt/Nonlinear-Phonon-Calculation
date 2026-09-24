"""Refit a legacy QE energy grid on its actual real mass-weighted displacement coordinates."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import numpy as np
from ase.build import make_supercell

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mlff_modepair_workflow.core import analyze_pair_grid, decode_complex_mode, infer_commensurate_supercell_n, load_atoms_from_qe
from mlff_modepair_workflow.prophet_backend import sha256_file
from mlff_modepair_workflow.units import NORMALIZATION_VERSION, UNITS, energies_to_ev


def legacy_real_norms(pair: dict, primitive) -> dict:
    q = np.asarray(pair["target_mode"]["q_frac"], dtype=float)
    n = infer_commensurate_supercell_n(q)
    supercell = make_supercell(primitive, np.diag([n, n, 1]))
    nat = len(primitive)
    cell_indices = np.array([[i, j, 0] for i in range(n) for j in range(n) for _ in range(nat)])
    atom_indices = np.arange(len(supercell)) % nat
    phase = np.exp(2j * np.pi * (cell_indices @ q))[:, None]
    gamma = decode_complex_mode(pair["gamma_mode"]["eigenvector"])[atom_indices]
    target = decode_complex_mode(pair["target_mode"]["eigenvector_q"])[atom_indices] * phase
    # This reproduces the pre-v3 builder exactly: Re[e/sqrt(Ncells)]/sqrt(M).
    gamma_norm = float(np.linalg.norm(np.real(gamma)) / n)
    target_norm = float(np.linalg.norm(np.real(target)) / n)
    if gamma_norm < 1e-8 or target_norm < 1e-8:
        raise ValueError("Legacy real displacement nearly vanished; its phase cannot be reconstructed reliably")
    return {"gamma_mass_weighted_norm_per_legacy_amplitude": gamma_norm,
            "target_mass_weighted_norm_per_legacy_amplitude": target_norm,
            "n_super": n, "n_cells": n * n}


def read_grid(amplitude_csv: Path, energy_grid: Path, source_unit: str):
    with amplitude_csv.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    x = sorted({float(row["a1"]) for row in rows})
    y = sorted({float(row["a2"]) for row in rows})
    grid = energies_to_ev(np.loadtxt(energy_grid), source_unit)
    if grid.shape != (len(y), len(x)) or len(rows) != len(x) * len(y):
        raise ValueError("Legacy energy grid and amplitude table have different shapes")
    for row in rows:
        i, j = int(row["a1_index"]), int(row["a2_index"])
        if not np.isclose(float(row["a1"]), x[i]) or not np.isclose(float(row["a2"]), y[j]):
            raise ValueError("Legacy amplitude table index/order is inconsistent")
    # Stage3 writes energy_grid_ry.dat as rows of a2 and columns of a1,
    # independent of the job-table iteration order.
    return np.asarray(x), np.asarray(y), np.asarray(grid)


def _normalized(vector):
    value = decode_complex_mode(vector).reshape(-1)
    norm = np.linalg.norm(value)
    if norm < 1e-12:
        raise ValueError("Zero legacy mode vector")
    return value / norm


def map_to_prophet(old_pair: dict, new_pairs: dict, phonon: dict, structure_sha256: str):
    if new_pairs["source"]["structure_sha256"] != structure_sha256:
        return {"status": "not_directly_comparable", "reason": "different_structure_hash"}
    q = np.asarray(old_pair["target_mode"]["q_frac"], dtype=float) % 1
    n = new_pairs["source"]["q_grid"][0]
    q_index = tuple(int(round(value * n)) % n for value in q[:2])
    by_q = {tuple(row["q_index"]): row for row in phonon["q_points"]}
    if q_index not in by_q:
        return {"status": "not_directly_comparable", "reason": "old_q_not_in_new_mesh"}
    rows = [by_q[(0, 0)], by_q[q_index]]
    old_vectors = [old_pair["gamma_mode"]["eigenvector"], old_pair["target_mode"]["eigenvector_q"]]
    matches = []
    for old_vector, row in zip(old_vectors, rows):
        source = _normalized(old_vector)
        overlaps = [float(abs(np.vdot(source, _normalized(vector))) ** 2) for vector in row["eigenvectors"]]
        index = int(np.argmax(overlaps))
        group = next(group for group in row["degenerate_groups_one_based"] if index + 1 in group)
        matches.append({"matched_mode_one_based": index + 1,
                        "overlap_squared": overlaps[index], "group_one_based": group})
    if any(match["overlap_squared"] < 0.95 for match in matches):
        return {"status": "not_directly_comparable", "reason": "mode_overlap_below_0.95", "matches": matches}
    if any(len(match["group_one_based"]) > 1 for match in matches):
        return {"status": "subspace_only", "reason": "near_degenerate_mode_basis", "matches": matches}
    new_pair = next((pair for pair in new_pairs["pairs"]
                     if np.allclose(np.asarray(pair["target_mode"]["q_frac"]) % 1, q)
                     and pair["gamma_mode"]["mode_number_one_based"] == matches[0]["matched_mode_one_based"]
                     and pair["target_mode"]["mode_number_one_based"] == matches[1]["matched_mode_one_based"]), None)
    if new_pair is None:
        return {"status": "not_directly_comparable", "reason": "mapped_q_is_not_an_orbit_representative", "matches": matches}
    return {"status": "magnitude_only", "reason": "independent_mode_phases_have_arbitrary_sign",
            "matches": matches, "new_pair_code": new_pair["pair_code"]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-mode-pairs-json", type=Path, required=True)
    parser.add_argument("--pair-code", required=True)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--amplitude-grid", type=Path, required=True)
    parser.add_argument("--energy-grid", type=Path, required=True)
    parser.add_argument("--source-energy-unit", choices=["Ry", "eV"], required=True)
    parser.add_argument("--grid-layout", choices=["a2_rows_a1_cols"], required=True)
    parser.add_argument("--new-mode-pairs-json", type=Path)
    parser.add_argument("--new-phonon-dataset", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    old_payload = json.loads(args.old_mode_pairs_json.read_text())
    pair = next((row for row in old_payload["pairs"] if row["pair_code"] == args.pair_code), None)
    if pair is None:
        raise ValueError(f"Old pair not found: {args.pair_code}")
    primitive = load_atoms_from_qe(args.structure)
    norm = legacy_real_norms(pair, primitive)
    a1, a2, energy = read_grid(args.amplitude_grid, args.energy_grid, args.source_energy_unit)
    q1 = a1 * norm["gamma_mass_weighted_norm_per_legacy_amplitude"]
    q2 = a2 * norm["target_mass_weighted_norm_per_legacy_amplitude"]
    # Printed QE eigenvectors have finite decimal precision.  A nominal |Q|=1
    # point can become 1.0000002 after recovering its real supercell norm.
    analysis = analyze_pair_grid(pair, energy, q1, q2, fit_window=1.001)
    mapping = {"status": "not_directly_comparable", "reason": "new_mode_basis_not_supplied"}
    if args.new_mode_pairs_json and args.new_phonon_dataset:
        new_pairs = json.loads(args.new_mode_pairs_json.read_text())
        phonon = json.loads(args.new_phonon_dataset.read_text())
        mapping = map_to_prophet(pair, new_pairs, phonon, sha256_file(args.structure))
    result = {
        "kind": "legacy_dft_grid_refit_on_actual_real_coordinates",
        "pair_code": args.pair_code, "structure_sha256": sha256_file(args.structure),
        "old_mode_pairs_sha256": sha256_file(args.old_mode_pairs_json),
        "amplitude_grid_sha256": sha256_file(args.amplitude_grid),
        "energy_grid_sha256": sha256_file(args.energy_grid),
        "source_energy_unit": args.source_energy_unit,
        "source_grid_layout": args.grid_layout,
        "fit_window_nominal_Q": 1.0,
        "fit_selection_tolerance_Q": 0.001,
        "normalization_version": NORMALIZATION_VERSION, "units": UNITS,
        "legacy_norms": norm, "q1_values": q1.tolist(), "q2_values": q2.tolist(),
        "analysis": analysis, "mode_mapping": mapping,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
