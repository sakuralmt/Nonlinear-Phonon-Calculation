#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
from ase.io import write
from ase.optimize import BFGS

try:
    from benchmark_golden_pair import A1_VALS, A2_VALS
    from build_ml_mode_pairs import (
        align_mode_phase_to_reference,
        decode_qe_mode,
        encode_complex_mode,
        extract_phonopy_mode,
        infer_shared_supercell,
        mass_weighted_overlap,
        run_phonopy,
        solve_assignment,
    )
    from core import (
        analyze_pair_grid,
        compare_golden_metrics,
        compare_mode_frequency_metrics,
        compare_with_reference_grid,
        dump_json,
        evaluate_pair_grid,
        load_atoms_from_qe,
        load_golden_reference,
        load_mode_pair_reference,
        make_calculator,
        save_pair_plot,
    )
except ModuleNotFoundError:
    from .benchmark_golden_pair import A1_VALS, A2_VALS
    from .build_ml_mode_pairs import (
        align_mode_phase_to_reference,
        decode_qe_mode,
        encode_complex_mode,
        extract_phonopy_mode,
        infer_shared_supercell,
        mass_weighted_overlap,
        run_phonopy,
        solve_assignment,
    )
    from .core import (
        analyze_pair_grid,
        compare_golden_metrics,
        compare_mode_frequency_metrics,
        compare_with_reference_grid,
        dump_json,
        evaluate_pair_grid,
        load_atoms_from_qe,
        load_golden_reference,
        load_mode_pair_reference,
        make_calculator,
        save_pair_plot,
    )


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
HEX_WORKFLOW_DIR = ROOT / "hex_qgamma_qpair_workflow"
SELECT_SCRIPT = HEX_WORKFLOW_DIR / "select_modes_qgamma_qpair.py"
PAIR_SCRIPT = HEX_WORKFLOW_DIR / "generate_mode_pairs_qgamma_qpair.py"

BASE_STRUCTURE = ROOT / "nonlocal phonon" / "scf.inp"
BASE_SCREENING_JSON = HEX_WORKFLOW_DIR / "hex_qgamma_qpair_run" / "screening" / "screening_summary.json"
BASE_SELECTED_QPOINTS = HEX_WORKFLOW_DIR / "hex_qgamma_qpair_run" / "screening" / "selected_qpoints.csv"
BASE_IRRED_QPOINTS = HEX_WORKFLOW_DIR / "hex_qgamma_qpair_run" / "screening" / "irreducible_qpoints.csv"
BASE_QE_EXTRACTED = HEX_WORKFLOW_DIR / "hex_qgamma_qpair_run" / "extracted" / "screened_eigenvectors.json"
BASE_QE_MODE_PAIRS = HEX_WORKFLOW_DIR / "hex_qgamma_qpair_run" / "mode_pairs" / "selected_mode_pairs.json"
BASE_GOLDEN_FIT = ROOT / "n7" / "fit_outputs_n7" / "fit_results.json"
BASE_REF_GRID = ROOT / "n7" / "E_tot_ph72_new.dat"
MASS_DICT = {"W": 183.84, "Se": 78.960}

BACKEND_SPECS = {
    "chgnet_r2scan": {
        "backend": "chgnet",
        "model": "r2scan",
        "device": "auto",
        "relax_fmax": 1.0e-3,
        "relax_steps": 400,
        "baseline_screening_csv": SCRIPT_DIR / "runs" / "chgnet_r2scan_relaxed" / "screening" / "pair_ranking.csv",
        "baseline_benchmark_summary": SCRIPT_DIR / "runs" / "chgnet_r2scan_relaxed" / "benchmark" / "summary.json",
    },
    "gptff_v2": {
        "backend": "gptff",
        "model": ROOT / "GPTFF" / "pretrained" / "gptff_v2.pth",
        "device": "auto",
        "relax_fmax": 5.0e-3,
        "relax_steps": 400,
        "baseline_screening_csv": ROOT / "gptff_modepair_workflow" / "screening" / "pair_ranking.csv",
        "baseline_benchmark_summary": ROOT / "gptff_modepair_workflow" / "benchmark" / "qe_structure_gptff_v2" / "summary.json",
    },
}

GOLDEN_GATE = {
    "gamma_abs_error_thz_max": 1.0,
    "target_abs_error_thz_max": 1.5,
    "phi122_abs_error_mev_max": 2.5,
}


def canonicalize_mode_by_max_component(mode: np.ndarray, masses: np.ndarray):
    vec = np.array(mode, dtype=np.complex128)
    weighted = (vec * np.sqrt(masses)[:, None]).reshape(-1)
    idx = int(np.argmax(np.abs(weighted)))
    ref = weighted[idx]
    if abs(ref) == 0.0:
        return vec
    return vec * np.exp(-1j * np.angle(ref))


def encode_complex_mode_list(mode: np.ndarray):
    return [
        [
            [float(vec[0].real), float(vec[0].imag)],
            [float(vec[1].real), float(vec[1].imag)],
            [float(vec[2].real), float(vec[2].imag)],
        ]
        for vec in np.array(mode, dtype=np.complex128)
    ]


def write_minimal_qe_input(atoms, out_file: Path):
    out_file.parent.mkdir(parents=True, exist_ok=True)
    cell = atoms.get_cell().array
    frac = atoms.get_scaled_positions()
    symbols = atoms.get_chemical_symbols()

    with out_file.open("w") as f:
        f.write("&CONTROL\n")
        f.write("  calculation = 'scf'\n")
        f.write("  prefix = 'pwscf'\n")
        f.write("  pseudo_dir = './'\n")
        f.write("  outdir = './tmp'\n")
        f.write("/\n\n")
        f.write("&SYSTEM\n")
        f.write("  ibrav = 0\n")
        f.write(f"  nat = {len(symbols)}, ntyp = 2\n")
        f.write("  ecutwfc = 100\n")
        f.write("  ecutrho = 1000\n")
        f.write("/\n\n")
        f.write("&ELECTRONS\n")
        f.write("  conv_thr = 1.0d-10\n")
        f.write("/\n\n")
        f.write("ATOMIC_SPECIES\n")
        f.write("W  183.84 W.pz-spn-rrkjus_psl.1.0.0.UPF\n")
        f.write("Se 78.960 Se.pz-n-rrkjus_psl.0.2.UPF\n\n")
        f.write("CELL_PARAMETERS (angstrom)\n")
        for row in cell:
            f.write(f"  {row[0]:.10f}  {row[1]:.10f}  {row[2]:.10f}\n")
        f.write("\nATOMIC_POSITIONS (crystal)\n")
        for symbol, pos in zip(symbols, frac):
            f.write(f"{symbol:<2}  {pos[0]:.10f}  {pos[1]:.10f}  {pos[2]:.10f}\n")
        f.write("\nK_POINTS {automatic}\n")
        f.write("1 1 1 0 0 0\n")


def run_fixed_cell_relax(structure_path: Path, backend_spec: dict, output_dir: Path, force: bool = False):
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "relax_summary.json"
    relaxed_xyz = output_dir / "relaxed_structure.extxyz"
    relaxed_scf = output_dir / "relaxed_structure.scf.inp"

    if not force and summary_path.exists() and relaxed_xyz.exists() and relaxed_scf.exists():
        summary = json.loads(summary_path.read_text())
        final_force = summary.get("final_max_force_eV_per_A")
        if final_force is not None and final_force <= float(backend_spec["relax_fmax"]):
            summary["reused_existing"] = True
            return summary

    atoms = load_atoms_from_qe(structure_path)
    calc, backend_meta = make_calculator(
        backend=backend_spec["backend"],
        device=backend_spec["device"],
        model=None if backend_spec["model"] is None else str(backend_spec["model"]),
    )
    atoms.calc = calc

    initial_energy = float(atoms.get_potential_energy())
    initial_forces = np.array(atoms.get_forces(), dtype=float)
    initial_max_force = float(np.max(np.linalg.norm(initial_forces, axis=1)))

    opt = BFGS(atoms, logfile=str(output_dir / "opt.log"))
    converged = bool(opt.run(fmax=float(backend_spec["relax_fmax"]), steps=int(backend_spec["relax_steps"])))

    final_energy = float(atoms.get_potential_energy())
    final_forces = np.array(atoms.get_forces(), dtype=float)
    final_max_force = float(np.max(np.linalg.norm(final_forces, axis=1)))

    write(relaxed_xyz, atoms)
    write_minimal_qe_input(atoms, relaxed_scf)

    summary = {
        "input_structure": str(structure_path),
        "backend": backend_meta,
        "converged": converged,
        "fmax_target_eV_per_A": float(backend_spec["relax_fmax"]),
        "steps_limit": int(backend_spec["relax_steps"]),
        "initial_energy_eV": initial_energy,
        "final_energy_eV": final_energy,
        "initial_max_force_eV_per_A": initial_max_force,
        "final_max_force_eV_per_A": final_max_force,
        "relaxed_structure_extxyz": str(relaxed_xyz),
        "relaxed_structure_scf": str(relaxed_scf),
        "reused_existing": False,
    }
    dump_json(summary_path, summary)
    return summary


def build_phonon_bundle(backend_spec: dict, structure_path: Path, qe_extracted_json: Path):
    qe_points = json.loads(qe_extracted_json.read_text())["points"]
    qe_points = sorted(qe_points, key=lambda item: int(item["target_index"]))
    qpoints = [point["q_target_frac"] for point in qe_points]
    supercell_matrix = infer_shared_supercell(qe_points)

    prim_atoms = load_atoms_from_qe(structure_path)
    calc, backend_meta = make_calculator(
        backend=backend_spec["backend"],
        device=backend_spec["device"],
        model=None if backend_spec["model"] is None else str(backend_spec["model"]),
    )
    freqs, eigs = run_phonopy(prim_atoms, calc, supercell_matrix, qpoints)
    masses = np.array([MASS_DICT[s] for s in prim_atoms.get_chemical_symbols()], dtype=float)

    return {
        "backend": backend_meta,
        "structure": str(structure_path),
        "qe_points": qe_points,
        "qpoints": qpoints,
        "supercell_matrix": supercell_matrix,
        "prim_atoms": prim_atoms,
        "masses": masses,
        "freqs": freqs,
        "eigs": eigs,
    }


def build_mode_alignment_from_bundle(bundle: dict):
    qe_points = bundle["qe_points"]
    prim_atoms = bundle["prim_atoms"]
    masses = bundle["masses"]
    freqs = bundle["freqs"]
    eigs = bundle["eigs"]

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
        ml_to_qe = {}
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
            ml_to_qe[int(ml_idx)] = int(qe_idx)

        aligned_points.append(
            {
                "target_index": int(qe_point["target_index"]),
                "label": qe_point["label"],
                "q_target_frac": qe_point["q_target_frac"],
                "best_basis_phase_mode": best_phase_mode,
                "best_phase_score": best_phase_score,
                "phase_trials": phase_trials,
                "matched_modes": matched_modes,
                "ml_to_qe_zero_based": ml_to_qe,
            }
        )

    return {
        "backend": bundle["backend"],
        "structure": bundle["structure"],
        "supercell_matrix": bundle["supercell_matrix"],
        "aligned_points": aligned_points,
    }


def build_self_consistent_screened_payload(bundle: dict):
    payload_points = []
    prim_atoms = bundle["prim_atoms"]
    masses = bundle["masses"]

    for iq, point in enumerate(bundle["qe_points"]):
        q_frac = point["q_target_frac"]
        add_basis_phase = not np.allclose(np.array(q_frac, dtype=float), 0.0)
        modes = []
        for mode_idx in range(bundle["freqs"].shape[1]):
            vec = extract_phonopy_mode(prim_atoms, q_frac, bundle["eigs"][iq], mode_idx, add_basis_phase=add_basis_phase)
            vec = canonicalize_mode_by_max_component(vec, masses)
            modes.append(encode_complex_mode(vec))
        payload_points.append(
            {
                "target_index": int(point["target_index"]),
                "label": point["label"],
                "q_target_frac": q_frac,
                "q_raw": q_frac,
                "q_match_method": "ml-self-consistent",
                "freqs_thz": [float(x) for x in bundle["freqs"][iq].tolist()],
                "modes": [encode_complex_mode_list(np.array([[c["x"]["re"] + 1j * c["x"]["im"], c["y"]["re"] + 1j * c["y"]["im"], c["z"]["re"] + 1j * c["z"]["im"]] for c in mode], dtype=np.complex128)) for mode in modes],
                "mode_phase_convention": "max_mass_weighted_component_real_positive",
                "include_basis_phase": bool(add_basis_phase),
            }
        )
    return {"kind": "screened_eigenvectors_qpair_ml_self_consistent", "points": payload_points}


def build_qe_aligned_screened_payload(alignment: dict):
    payload_points = []
    for point in alignment["aligned_points"]:
        matched = sorted(point["matched_modes"], key=lambda item: item["qe_mode_index_zero_based"])
        payload_points.append(
            {
                "target_index": point["target_index"],
                "label": point["label"],
                "q_target_frac": point["q_target_frac"],
                "q_raw": point["q_target_frac"],
                "q_match_method": "ml-qe-aligned",
                "freqs_thz": [float(item["freq_thz"]) for item in matched],
                "modes": [encode_complex_mode_list(item["vector"]) for item in matched],
                "mode_phase_convention": "aligned_to_qe_inner_product_real_positive",
                "include_basis_phase": bool(point["best_basis_phase_mode"]),
            }
        )
    return {"kind": "screened_eigenvectors_qpair_ml_qe_aligned", "points": payload_points}


def write_screened_payload(output_dir: Path, payload: dict):
    output_dir.mkdir(parents=True, exist_ok=True)
    out_json = output_dir / "screened_eigenvectors.json"
    out_json.write_text(json.dumps(payload, indent=2))

    out_csv = output_dir / "screened_eigenvectors_summary.csv"
    with out_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["target_index", "label", "qx", "qy", "qz", "n_modes", "match_method"])
        for item in payload["points"]:
            q = item["q_target_frac"]
            writer.writerow(
                [
                    item["target_index"],
                    item["label"],
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    len(item["freqs_thz"]),
                    item.get("q_match_method", ""),
                ]
            )
    return out_json, out_csv


def prepare_track_run_root(track_root: Path, screened_payload: dict, relaxed_scf_path: Path, mode_source: str):
    if track_root.exists():
        shutil.rmtree(track_root)
    track_root.mkdir(parents=True, exist_ok=True)

    screening_dir = track_root / "screening"
    screening_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(BASE_SCREENING_JSON, screening_dir / "screening_summary.json")
    if BASE_SELECTED_QPOINTS.exists():
        shutil.copy2(BASE_SELECTED_QPOINTS, screening_dir / "selected_qpoints.csv")
    if BASE_IRRED_QPOINTS.exists():
        shutil.copy2(BASE_IRRED_QPOINTS, screening_dir / "irreducible_qpoints.csv")

    extracted_dir = track_root / "extracted"
    write_screened_payload(extracted_dir, screened_payload)
    dump_json(
        track_root / "track_meta.json",
        {
            "mode_source": mode_source,
            "relaxed_scf_template": str(relaxed_scf_path),
            "base_screening_json": str(BASE_SCREENING_JSON),
        },
    )

    select_cmd = [
        sys.executable,
        str(SELECT_SCRIPT),
        "--run-root",
        str(track_root),
        "--scf-template",
        str(relaxed_scf_path),
        "--output-dir",
        str(track_root / "mode_selection"),
        "--apply-selection-rules",
        "--gamma-optical-only",
    ]
    subprocess.run(select_cmd, check=True, text=True)

    pair_cmd = [
        sys.executable,
        str(PAIR_SCRIPT),
        "--run-root",
        str(track_root),
        "--output-dir",
        str(track_root / "mode_pairs"),
    ]
    subprocess.run(pair_cmd, check=True, text=True)

    payload = json.loads((track_root / "mode_pairs" / "selected_mode_pairs.json").read_text())
    return payload


def build_qe_pair_lookup(qe_mode_pairs_json: Path):
    pairs = json.loads(qe_mode_pairs_json.read_text())["pairs"]
    lookup = {}
    for pair in pairs:
        lookup[
            (
                int(pair["gamma_mode"]["mode_number_one_based"]),
                int(pair["target_mode"]["point_index"]),
                int(pair["target_mode"]["mode_number_one_based"]),
            )
        ] = pair["pair_code"]
    return lookup


def build_ml_to_qe_maps(alignment: dict):
    gamma_map = None
    target_maps = {}
    for point in alignment["aligned_points"]:
        mapping = {int(k): int(v) for k, v in point["ml_to_qe_zero_based"].items()}
        if point["label"] == "Gamma":
            gamma_map = mapping
        target_maps[int(point["target_index"])] = mapping
    return gamma_map, target_maps


def find_pair_by_numbers(mode_pairs_payload: dict, gamma_mode_number: int, point_label: str, target_mode_number: int):
    for pair in mode_pairs_payload["pairs"]:
        if (
            int(pair["gamma_mode"]["mode_number_one_based"]) == int(gamma_mode_number)
            and pair["target_mode"]["point_label"] == point_label
            and int(pair["target_mode"]["mode_number_one_based"]) == int(target_mode_number)
        ):
            return pair
    raise RuntimeError(f"Pair not found for gamma={gamma_mode_number}, point={point_label}, target={target_mode_number}")


def find_golden_pair_for_track(mode_pairs_payload: dict, alignment: dict, mode_source: str):
    if mode_source == "qe_aligned":
        return find_pair_by_numbers(mode_pairs_payload, 8, "M", 3)

    gamma_point = next(point for point in alignment["aligned_points"] if point["label"] == "Gamma")
    m_point = next(point for point in alignment["aligned_points"] if point["label"] == "M")
    gamma_ml = next(item["ml_mode_number_one_based"] for item in gamma_point["matched_modes"] if item["qe_mode_number_one_based"] == 8)
    target_ml = next(item["ml_mode_number_one_based"] for item in m_point["matched_modes"] if item["qe_mode_number_one_based"] == 3)
    return find_pair_by_numbers(mode_pairs_payload, gamma_ml, "M", target_ml)


def map_self_pair_codes_to_qe(mode_pairs_payload: dict, alignment: dict, qe_mode_pairs_json: Path):
    qe_lookup = build_qe_pair_lookup(qe_mode_pairs_json)
    gamma_ml_to_qe, target_ml_to_qe = build_ml_to_qe_maps(alignment)

    mapping = {}
    for pair in mode_pairs_payload["pairs"]:
        gamma_ml = int(pair["gamma_mode"]["mode_number_one_based"]) - 1
        point_index = int(pair["target_mode"]["point_index"])
        target_ml = int(pair["target_mode"]["mode_number_one_based"]) - 1
        gamma_qe = gamma_ml_to_qe.get(gamma_ml)
        target_qe = target_ml_to_qe.get(point_index, {}).get(target_ml)
        mapped_code = None
        if gamma_qe is not None and target_qe is not None:
            mapped_code = qe_lookup.get((gamma_qe + 1, point_index, target_qe + 1))
        mapping[pair["pair_code"]] = mapped_code
    return mapping


def build_qpoint_diagnostics(alignment: dict, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    highlights = {}
    for point in alignment["aligned_points"]:
        overlaps = [float(item["overlap"]) for item in point["matched_modes"]]
        row = {
            "target_index": int(point["target_index"]),
            "label": point["label"],
            "q_frac": point["q_target_frac"],
            "best_phase_score": float(point["best_phase_score"]),
            "mean_overlap": float(np.mean(overlaps)),
            "min_overlap": float(np.min(overlaps)),
            "matches": [
                {
                    "qe_mode_number_one_based": int(item["qe_mode_number_one_based"]),
                    "ml_mode_number_one_based": int(item["ml_mode_number_one_based"]),
                    "ml_freq_thz": float(item["freq_thz"]),
                    "overlap": float(item["overlap"]),
                }
                for item in point["matched_modes"]
            ],
        }
        rows.append(row)

        if point["label"] == "Gamma":
            highlights["Gamma_mode8"] = next(item for item in row["matches"] if item["qe_mode_number_one_based"] == 8)
        if point["label"] == "M":
            highlights["M_mode3"] = next(item for item in row["matches"] if item["qe_mode_number_one_based"] == 3)
        if point["label"] == "K":
            highlights["K_mode3"] = next(item for item in row["matches"] if item["qe_mode_number_one_based"] == 3)

    payload = {"points": rows, "highlights": highlights}
    dump_json(output_dir / "qpoint_diagnostics.json", payload)

    with (output_dir / "qpoint_diagnostics.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["target_index", "label", "qx", "qy", "qz", "best_phase_score", "mean_overlap", "min_overlap"])
        for row in rows:
            q = row["q_frac"]
            writer.writerow(
                [
                    row["target_index"],
                    row["label"],
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    f"{row['best_phase_score']:.6f}",
                    f"{row['mean_overlap']:.6f}",
                    f"{row['min_overlap']:.6f}",
                ]
            )
    return payload


def load_ranking_csv(path: Path, key_field: str = "pair_code"):
    rows = list(csv.DictReader(path.open()))
    out = {}
    for row in rows:
        key = row[key_field]
        if not key:
            continue
        out[key] = {
            "rank": int(row["rank"]),
            "phi122_mev": float(row["phi122_mev"]),
            "gamma_freq_fit_thz": float(row["gamma_freq_fit_thz"]) if row["gamma_freq_fit_thz"] else None,
            "target_freq_fit_thz": float(row["target_freq_fit_thz"]) if row["target_freq_fit_thz"] else None,
        }
    return rows, out


def rankdata_desc(values: np.ndarray):
    order = np.argsort(-values)
    ranks = np.empty(len(values), dtype=float)
    i = 0
    while i < len(values):
        j = i + 1
        while j < len(values) and values[order[j]] == values[order[i]]:
            j += 1
        avg_rank = 0.5 * (i + 1 + j)
        ranks[order[i:j]] = avg_rank
        i = j
    return ranks


def spearman_abs_phi(base_map: dict, new_map: dict):
    common = sorted(set(base_map) & set(new_map))
    if len(common) < 2:
        return None
    a_vals = np.array([abs(base_map[key]["phi122_mev"]) for key in common], dtype=float)
    b_vals = np.array([abs(new_map[key]["phi122_mev"]) for key in common], dtype=float)
    return float(np.corrcoef(rankdata_desc(a_vals), rankdata_desc(b_vals))[0, 1])


def compare_rankings(baseline_ranking_csv: Path, new_ranking_csv: Path, golden_compare_key: str, key_field: str = "comparison_key", top_n: int = 10):
    base_rows, base_map = load_ranking_csv(baseline_ranking_csv, key_field="pair_code")
    new_rows, new_map = load_ranking_csv(new_ranking_csv, key_field=key_field)

    if golden_compare_key not in base_map or golden_compare_key not in new_map:
        return {
            "status": "unavailable",
            "reason": f"golden comparison key missing: {golden_compare_key}",
        }

    base_top = [row["pair_code"] for row in base_rows[:top_n]]
    new_top = [row[key_field] for row in new_rows[:top_n] if row.get(key_field)]
    common = sorted(set(base_top) & set(new_top))

    rank_differences = []
    for row in new_rows:
        key = row.get(key_field)
        if not key or key not in base_map or key not in new_map:
            continue
        rank_differences.append(
            {
                "pair_code": row["pair_code"],
                "comparison_key": key,
                "baseline_rank": base_map[key]["rank"],
                "new_rank": new_map[key]["rank"],
                "rank_shift": new_map[key]["rank"] - base_map[key]["rank"],
                "baseline_phi122_mev": base_map[key]["phi122_mev"],
                "new_phi122_mev": new_map[key]["phi122_mev"],
            }
        )

    return {
        "status": "ok",
        "golden_pair": {
            "comparison_key": golden_compare_key,
            "baseline_rank": base_map[golden_compare_key]["rank"],
            "new_rank": new_map[golden_compare_key]["rank"],
            "baseline_phi122_mev": base_map[golden_compare_key]["phi122_mev"],
            "new_phi122_mev": new_map[golden_compare_key]["phi122_mev"],
        },
        "top_overlap": {
            "top_n": top_n,
            "baseline_top_pairs": base_top,
            "new_top_comparison_keys": new_top,
            "common_pairs": common,
            "overlap_fraction": len(common) / float(top_n),
        },
        "spearman_abs_phi": spearman_abs_phi(base_map, new_map),
        "rank_differences": rank_differences,
    }


def run_track_benchmark(
    track_root: Path,
    mode_pairs_payload: dict,
    golden_pair: dict,
    structure_path: Path,
    backend_spec: dict,
    mode_source: str,
    qe_reference_pair: dict | None = None,
):
    out_dir = track_root / "benchmark"
    out_dir.mkdir(parents=True, exist_ok=True)

    calc, backend_meta = make_calculator(
        backend=backend_spec["backend"],
        device=backend_spec["device"],
        model=None if backend_spec["model"] is None else str(backend_spec["model"]),
    )
    mode_pair_reference = load_mode_pair_reference(golden_pair)
    qe_mode_reference = None if qe_reference_pair is None else load_mode_pair_reference(qe_reference_pair)
    golden_reference = load_golden_reference(BASE_GOLDEN_FIT)

    e_grid, builder = evaluate_pair_grid(
        golden_pair,
        structure_path=structure_path,
        calc=calc,
        a1_vals=A1_VALS,
        a2_vals=A2_VALS,
        row_callback=lambda i_a2, _a2: print(f"[{track_root.parent.name}:{mode_source}:benchmark] row {i_a2 + 1}/{len(A2_VALS)}"),
    )
    analysis = analyze_pair_grid(golden_pair, e_grid, A1_VALS, A2_VALS, fit_window=1.0)
    mode_pair_compare = compare_mode_frequency_metrics(analysis, mode_pair_reference)
    qe_mode_compare = None if qe_mode_reference is None else compare_mode_frequency_metrics(analysis, qe_mode_reference)
    golden_compare = compare_golden_metrics(analysis, golden_reference)
    ref_compare = compare_with_reference_grid(BASE_REF_GRID, e_grid)

    summary = {
        "pair_code": golden_pair["pair_code"],
        "mode_source": mode_source,
        "structure": str(structure_path),
        "backend": backend_meta,
        "builder": builder.metadata(),
        "analysis": analysis,
        "mode_pair_reference": mode_pair_reference,
        "mode_pair_frequency_compare": mode_pair_compare,
        "qe_mode_reference": qe_mode_reference,
        "qe_mode_reference_compare": qe_mode_compare,
        "golden_pes_reference": golden_reference,
        "golden_pes_compare": golden_compare,
        "golden_reference": golden_reference,
        "golden_compare": golden_compare,
        "reference_grid_compare": ref_compare,
        "reference_semantics": {
            "mode_pair_reference": "Mode frequencies carried by the current track's selected_mode_pairs.json.",
            "qe_mode_reference": "Raw Gamma/M mode frequencies from the original QE selected_mode_pairs.json.",
            "golden_pes_reference": "Frequencies and phi122 extracted from the fitted n7 PES reference.",
            "golden_compare_alias": "Deprecated compatibility alias for golden_pes_compare.",
        },
    }
    np.savetxt(out_dir / "energy_grid_eV.dat", e_grid, fmt="%.10f")
    dump_json(out_dir / "summary.json", summary)
    save_pair_plot(out_dir / "pes_map.png", e_grid, A1_VALS, A2_VALS, title=f"{mode_source}: {golden_pair['pair_code']}")
    return summary


def passes_golden_gate(golden_compare: dict):
    reasons = []
    if golden_compare.get("gamma_freq_fit_thz") is None:
        reasons.append("gamma_axis_unstable")
    if golden_compare.get("target_freq_fit_thz") is None:
        reasons.append("target_axis_unstable")
    gamma_err = golden_compare.get("gamma_freq_abs_error_thz")
    target_err = golden_compare.get("target_freq_abs_error_thz")
    phi_err = golden_compare.get("phi122_abs_error_mev_per_A3amu32")
    if gamma_err is None or gamma_err >= GOLDEN_GATE["gamma_abs_error_thz_max"]:
        reasons.append(f"gamma_abs_error={gamma_err}")
    if target_err is None or target_err >= GOLDEN_GATE["target_abs_error_thz_max"]:
        reasons.append(f"target_abs_error={target_err}")
    if phi_err is None or phi_err >= GOLDEN_GATE["phi122_abs_error_mev_max"]:
        reasons.append(f"phi122_abs_error={phi_err}")
    return len(reasons) == 0, reasons


def run_track_screening(track_root: Path, mode_pairs_payload: dict, structure_path: Path, backend_spec: dict, mode_source: str, comparison_key_map: dict | None):
    out_dir = track_root / "pair_screening"
    out_dir.mkdir(parents=True, exist_ok=True)

    calc, backend_meta = make_calculator(
        backend=backend_spec["backend"],
        device=backend_spec["device"],
        model=None if backend_spec["model"] is None else str(backend_spec["model"]),
    )

    ranking = []
    for idx, pair in enumerate(mode_pairs_payload["pairs"], start=1):
        pair_dir = out_dir / pair["pair_code"]
        pair_dir.mkdir(parents=True, exist_ok=True)
        e_grid, builder = evaluate_pair_grid(pair, structure_path=structure_path, calc=calc, a1_vals=A1_VALS, a2_vals=A2_VALS, row_callback=None)
        analysis = analyze_pair_grid(pair, e_grid, A1_VALS, A2_VALS, fit_window=1.0)

        summary = {
            "pair_code": pair["pair_code"],
            "mode_source": mode_source,
            "structure": str(structure_path),
            "backend": backend_meta,
            "builder": builder.metadata(),
            "analysis": analysis,
        }
        np.savetxt(pair_dir / "energy_grid_eV.dat", e_grid, fmt="%.10f")
        dump_json(pair_dir / "summary.json", summary)
        save_pair_plot(pair_dir / "pes_map.png", e_grid, A1_VALS, A2_VALS, title=pair["pair_code"])

        gamma_axis = analysis["axis_checks"]["mode1_axis_fit"]["freq"]
        target_axis = analysis["axis_checks"]["mode2_axis_fit"]["freq"]
        comparison_key = pair["pair_code"] if comparison_key_map is None else comparison_key_map.get(pair["pair_code"])
        ranking.append(
            {
                "pair_code": pair["pair_code"],
                "comparison_key": comparison_key,
                "coupling_type": pair["coupling_type"],
                "point_label": pair["target_mode"]["point_label"],
                "q_frac": pair["target_mode"]["q_frac"],
                "n_super": builder.n_super,
                "gamma_mode_code": pair["gamma_mode"]["mode_code"],
                "gamma_mode_number": pair["gamma_mode"]["mode_number_one_based"],
                "gamma_freq_ref_thz": pair["gamma_mode"]["freq_thz"],
                "gamma_freq_fit_thz": gamma_axis.get("thz"),
                "target_mode_code": pair["target_mode"]["mode_code"],
                "target_mode_number": pair["target_mode"]["mode_number_one_based"],
                "target_freq_ref_thz": pair["target_mode"]["freq_thz"],
                "target_freq_fit_thz": target_axis.get("thz"),
                "phi122_mev": analysis["physics"]["phi_122_mev_per_A3amu32"],
                "phi112_mev": analysis["physics"]["phi_112_mev_per_A3amu32"],
                "r2": analysis["r2"],
                "rmse_ev_supercell": analysis["rmse_ev_supercell"],
            }
        )
        print(f"[{track_root.parent.name}:{mode_source}:screening] {idx}/{len(mode_pairs_payload['pairs'])} {pair['pair_code']}")

    ranking.sort(key=lambda item: abs(item["phi122_mev"]), reverse=True)

    ranking_csv = out_dir / "pair_ranking.csv"
    with ranking_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rank",
                "pair_code",
                "comparison_key",
                "coupling_type",
                "point_label",
                "qx",
                "qy",
                "qz",
                "n_super",
                "gamma_mode_code",
                "gamma_mode_number",
                "gamma_freq_ref_thz",
                "gamma_freq_fit_thz",
                "target_mode_code",
                "target_mode_number",
                "target_freq_ref_thz",
                "target_freq_fit_thz",
                "phi122_mev",
                "phi112_mev",
                "r2",
                "rmse_ev_supercell",
            ]
        )
        for rank, item in enumerate(ranking, start=1):
            q = item["q_frac"]
            writer.writerow(
                [
                    rank,
                    item["pair_code"],
                    "" if item["comparison_key"] is None else item["comparison_key"],
                    item["coupling_type"],
                    item["point_label"],
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    item["n_super"],
                    item["gamma_mode_code"],
                    item["gamma_mode_number"],
                    f"{item['gamma_freq_ref_thz']:.6f}",
                    f"{item['gamma_freq_fit_thz']:.6f}" if item["gamma_freq_fit_thz"] is not None else "",
                    item["target_mode_code"],
                    item["target_mode_number"],
                    f"{item['target_freq_ref_thz']:.6f}",
                    f"{item['target_freq_fit_thz']:.6f}" if item["target_freq_fit_thz"] is not None else "",
                    f"{item['phi122_mev']:.6f}",
                    f"{item['phi112_mev']:.6f}",
                    f"{item['r2']:.6f}",
                    f"{item['rmse_ev_supercell']:.6f}",
                ]
            )

    dump_json(out_dir / "pair_ranking.json", {"mode_source": mode_source, "backend": backend_meta, "pairs": ranking})
    return ranking_csv


def load_baseline_benchmark(summary_path: Path):
    if not summary_path.exists():
        return None
    payload = json.loads(summary_path.read_text())
    return {
        "path": str(summary_path),
        "mode_pair_frequency_compare": payload.get("mode_pair_frequency_compare"),
        "golden_pes_compare": payload.get("golden_pes_compare", payload.get("golden_compare")),
        "analysis_reference": payload.get("analysis", {}).get("mode_pair_reference", payload.get("analysis", {}).get("reference")),
    }
