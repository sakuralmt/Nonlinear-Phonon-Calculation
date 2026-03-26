#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

import numpy as np
from ase.io import write
from ase.optimize import BFGS

from core import (
    analyze_pair_grid,
    compare_golden_metrics,
    compare_mode_frequency_metrics,
    compare_with_reference_grid,
    dump_json,
    evaluate_pair_grid,
    find_golden_pair,
    load_atoms_from_qe,
    load_golden_reference,
    load_mode_pair_reference,
    load_pairs,
    make_calculator,
)


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
RUNS_DIR = SCRIPT_DIR / "runs"

DEFAULT_MODE_PAIRS = ROOT / "hex_qgamma_qpair_workflow" / "hex_qgamma_qpair_run" / "mode_pairs" / "selected_mode_pairs.json"
DEFAULT_STRUCTURE = ROOT / "nonlocal phonon" / "scf.inp"
DEFAULT_REF_GRID = ROOT / "n7" / "E_tot_ph72_new.dat"
DEFAULT_GOLDEN_FIT = ROOT / "n7" / "fit_outputs_n7" / "fit_results.json"

A1_VALS = np.linspace(-2.0, 2.0, 9)
A2_VALS = np.linspace(-2.0, 2.0, 9)
GAMMA_REF_THZ = 7.546373765478082
TARGET_REF_THZ = 3.883932051986477
PHI122_REF_MEV = 3.70576269691557


def parse_args():
    p = argparse.ArgumentParser(description="Scan installed MLFF candidates on the golden pair after fixed-cell relaxation.")
    p.add_argument("--device", type=str, default="auto")
    p.add_argument("--structure", type=str, default=str(DEFAULT_STRUCTURE))
    p.add_argument("--mode-pairs-json", type=str, default=str(DEFAULT_MODE_PAIRS))
    p.add_argument("--golden-fit-json", type=str, default=str(DEFAULT_GOLDEN_FIT))
    p.add_argument("--golden-ref-grid", type=str, default=str(DEFAULT_REF_GRID))
    p.add_argument("--output-root", type=str, default=str(RUNS_DIR))
    p.add_argument("--fmax", type=float, default=1.0e-3)
    p.add_argument("--steps", type=int, default=400)
    return p.parse_args()


def candidate_list():
    mace_cache = Path.home() / ".cache" / "mace"
    candidates = [
        {"tag": "chgnet_relaxed", "backend": "chgnet", "model": None},
        {"tag": "chgnet_0_2_relaxed", "backend": "chgnet", "model": "0.2.0"},
        {"tag": "chgnet_r2scan_relaxed", "backend": "chgnet", "model": "r2scan"},
        {"tag": "mace_mpa0_medium_relaxed", "backend": "mace", "model": None},
        {"tag": "mace_mptrj_20229_relaxed", "backend": "mace", "model": str(mace_cache / "MACE_MPtrj_20229model")},
        {"tag": "mace_20231203_l1_relaxed", "backend": "mace", "model": str(mace_cache / "20231203mace128L1_epoch199model")},
        {"tag": "mace_20231210_l0_relaxed", "backend": "mace", "model": str(mace_cache / "20231210mace128L0_energy_epoch249model")},
    ]
    return candidates


def main():
    args = parse_args()
    structure = Path(args.structure).expanduser().resolve()
    mode_pairs_json = Path(args.mode_pairs_json).expanduser().resolve()
    golden_fit_json = Path(args.golden_fit_json).expanduser().resolve()
    golden_ref_grid = Path(args.golden_ref_grid).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    pairs = load_pairs(mode_pairs_json)
    golden_pair = find_golden_pair(pairs)
    mode_pair_reference = load_mode_pair_reference(golden_pair)
    golden_reference = load_golden_reference(golden_fit_json)

    ranking = []
    for candidate in candidate_list():
        tag = candidate["tag"]
        print("=" * 72)
        print(f"[scan] {tag}")
        print("=" * 72)

        calc, backend_meta = make_calculator(candidate["backend"], device=args.device, model=candidate["model"])
        run_root = output_root / tag
        relax_dir = run_root / "relax"
        benchmark_dir = run_root / "benchmark"
        relax_dir.mkdir(parents=True, exist_ok=True)
        benchmark_dir.mkdir(parents=True, exist_ok=True)

        atoms = load_atoms_from_qe(structure)
        atoms.calc = calc
        initial_energy = float(atoms.get_potential_energy())
        initial_max_force = float(np.max(np.linalg.norm(atoms.get_forces(), axis=1)))

        opt = BFGS(atoms, logfile=str(relax_dir / "opt.log"))
        opt.run(fmax=args.fmax, steps=args.steps)

        final_energy = float(atoms.get_potential_energy())
        final_forces = atoms.get_forces()
        final_max_force = float(np.max(np.linalg.norm(final_forces, axis=1)))

        relaxed_structure = relax_dir / "relaxed_structure.extxyz"
        write(relaxed_structure, atoms)
        dump_json(
            relax_dir / "relax_summary.json",
            {
                "run_tag": tag,
                "input_structure": str(structure),
                "backend": backend_meta,
                "initial_energy_eV": initial_energy,
                "final_energy_eV": final_energy,
                "initial_max_force_eV_per_A": initial_max_force,
                "final_max_force_eV_per_A": final_max_force,
                "relaxed_structure": str(relaxed_structure),
            },
        )

        e_grid, builder = evaluate_pair_grid(
            golden_pair,
            structure_path=relaxed_structure,
            calc=calc,
            a1_vals=A1_VALS,
            a2_vals=A2_VALS,
            row_callback=lambda i_a2, _a2: print(f"[{tag}] row {i_a2 + 1}/{len(A2_VALS)}"),
        )
        analysis = analyze_pair_grid(golden_pair, e_grid, A1_VALS, A2_VALS, fit_window=1.0)
        mode_pair_compare = compare_mode_frequency_metrics(analysis, mode_pair_reference)
        golden_compare = compare_golden_metrics(analysis, golden_reference)
        ref_compare = compare_with_reference_grid(golden_ref_grid, e_grid)

        dump_json(
            benchmark_dir / "summary.json",
            {
                "run_tag": tag,
                "backend": backend_meta,
                "structure": str(relaxed_structure),
                "builder": builder.metadata(),
                "analysis": analysis,
                "mode_pair_reference": mode_pair_reference,
                "mode_pair_frequency_compare": mode_pair_compare,
                "golden_pes_reference": golden_reference,
                "golden_pes_compare": golden_compare,
                "golden_reference": golden_reference,
                "golden_compare": golden_compare,
                "reference_grid_compare": ref_compare,
            },
        )
        np.savetxt(benchmark_dir / "energy_grid_eV.dat", e_grid, fmt="%.10f")

        ranking.append(
            {
                "run_tag": tag,
                "backend": backend_meta["backend"],
                "model": backend_meta["model"],
                "device": backend_meta["device"],
                "initial_max_force": initial_max_force,
                "final_max_force": final_max_force,
                "gamma_fit_thz": golden_compare["gamma_freq_fit_thz"],
                "gamma_abs_err_thz": golden_compare["gamma_freq_abs_error_thz"],
                "target_fit_thz": golden_compare["target_freq_fit_thz"],
                "target_abs_err_thz": golden_compare["target_freq_abs_error_thz"],
                "phi122_fit_mev": golden_compare["phi122_fit_mev_per_A3amu32"],
                "phi122_abs_err_mev": golden_compare["phi122_abs_error_mev_per_A3amu32"],
                "grid_rmse_ev_supercell": None if ref_compare is None else ref_compare["rmse_ev_supercell"],
            }
        )

    for row in ranking:
        gamma_term = 1.0e9 if row["gamma_abs_err_thz"] is None else row["gamma_abs_err_thz"] / GAMMA_REF_THZ
        target_term = 1.0e9 if row["target_abs_err_thz"] is None else row["target_abs_err_thz"] / TARGET_REF_THZ
        phi_term = row["phi122_abs_err_mev"] / PHI122_REF_MEV
        row["balanced_score"] = gamma_term + target_term + phi_term

    ranking.sort(key=lambda item: item["balanced_score"])

    ranking_csv = output_root / "candidate_scan_ranking.csv"
    with ranking_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rank",
                "run_tag",
                "backend",
                "model",
                "device",
                "initial_max_force",
                "final_max_force",
                "gamma_fit_thz",
                "gamma_abs_err_thz",
                "target_fit_thz",
                "target_abs_err_thz",
                "phi122_fit_mev",
                "phi122_abs_err_mev",
                "balanced_score",
                "grid_rmse_ev_supercell",
            ]
        )
        for rank, row in enumerate(ranking, start=1):
            writer.writerow(
                [
                    rank,
                    row["run_tag"],
                    row["backend"],
                    row["model"],
                    row["device"],
                    f"{row['initial_max_force']:.6e}",
                    f"{row['final_max_force']:.6e}",
                    "" if row["gamma_fit_thz"] is None else f"{row['gamma_fit_thz']:.6f}",
                    "" if row["gamma_abs_err_thz"] is None else f"{row['gamma_abs_err_thz']:.6f}",
                    "" if row["target_fit_thz"] is None else f"{row['target_fit_thz']:.6f}",
                    "" if row["target_abs_err_thz"] is None else f"{row['target_abs_err_thz']:.6f}",
                    f"{row['phi122_fit_mev']:.6f}",
                    f"{row['phi122_abs_err_mev']:.6f}",
                    f"{row['balanced_score']:.6f}",
                    "" if row["grid_rmse_ev_supercell"] is None else f"{row['grid_rmse_ev_supercell']:.6f}",
                ]
            )

    dump_json(output_root / "candidate_scan_ranking.json", {"rows": ranking})
    print(f"saved: {ranking_csv}")


if __name__ == "__main__":
    main()
