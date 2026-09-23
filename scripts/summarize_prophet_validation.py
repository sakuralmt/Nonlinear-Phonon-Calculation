"""Summarize local Prophet v3 physical and performance validation without promoting it."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import numpy as np


def load(path: Path):
    return json.loads(path.read_text()) if path.exists() else None


def summarize(root: Path):
    tracks = {}
    pilots = []
    for material in ("mos2", "wse2"):
        shared_dir = root / material / "shared_dft_final" / "stage1"
        if not shared_dir.exists():
            shared_dir = root / material / "stage1"
        geometry_dirs = {
            "shared_dft": shared_dir,
            "model_relaxed": root / material / "model_relaxed" / "stage1",
        }
        datasets = {}
        for geometry, directory in geometry_dirs.items():
            dataset = load(directory / "phonon_dataset.json")
            pairs = load(directory / "mode_pairs.selected.json")
            if dataset is None or pairs is None:
                tracks[f"{material}/{geometry}"] = {"complete_stage1": False}
                continue
            datasets[geometry] = dataset
            diagnostics = dataset["diagnostics"]
            tracks[f"{material}/{geometry}"] = {
                "complete_stage1": len(dataset["q_points"]) == 36 and len(dataset["q_orbits"]) == 6 and len(pairs["pairs"]) == 486,
                "q_count": len(dataset["q_points"]),
                "q_orbits": len(dataset["q_orbits"]),
                "pair_count": len(pairs["pairs"]),
                "structure_sha256": dataset["source"]["structure_sha256"],
                "source_structure": dataset["source"]["structure"],
                "mode_pairs_json": str((directory / "mode_pairs.selected.json").resolve()),
                "phonon_dataset_json": str((directory / "phonon_dataset.json").resolve()),
                "force_constants_npz": str((directory / "force_constants.npz").resolve()),
                "checkpoint_sha256": dataset["source"]["model"]["checkpoint_sha256"],
                "checkpoint_path": dataset["source"]["model"]["checkpoint"],
                "source_commit": dataset["source"]["model"]["source_commit"],
                "source_root": dataset["source"]["model"]["source_root"],
                "q_grid": dataset["source"]["q_grid"],
                "finite_difference_step_angstrom": dataset["source"]["finite_difference_step_angstrom"],
                "gamma_acoustic_thz": diagnostics["gamma_acoustic_frequencies_thz"],
                "max_hermitian_relative_error": diagnostics["max_hermitian_relative_error"],
                "max_acoustic_row_sum_residual_ev_per_A2": diagnostics.get("max_acoustic_row_sum_residual_ev_per_A2"),
                "max_fd_step_frequency_change_thz": diagnostics["convergence"]["max_frequency_change_thz"],
                "force_elapsed_seconds": diagnostics["force_elapsed_seconds"],
                "resources": diagnostics.get("resources"),
            }
            force_path = directory / "force_constants.npz"
            if force_path.exists():
                with np.load(force_path) as force_data:
                    if "force_constants_convergence_ev_per_A2" in force_data:
                        large = force_data["force_constants_ev_per_A2"]
                        small = force_data["force_constants_convergence_ev_per_A2"]
                        richardson = (4 * small - large) / 3
                        tracks[f"{material}/{geometry}"]["fd_row_sum_diagnostic_ev_per_A2"] = {
                            "step_0p01": float(np.max(np.abs(large.sum(axis=(0, 1, 4))))),
                            "step_0p005": float(np.max(np.abs(small.sum(axis=(0, 1, 4))))),
                            "richardson_diagnostic_only": float(np.max(np.abs(richardson.sum(axis=(0, 1, 4))))),
                        }
        comparison = load(shared_dir / "qe_comparison.json") or load(root / material / "stage1" / "qe_comparison.json")
        if comparison:
            tracks[f"{material}/shared_dft"]["qe_comparison"] = {
                key: comparison[key] for key in (
                    "q_count", "matched_frequency_mae_thz", "matched_frequency_p95_abs_error_thz",
                    "isolated_mode_median_overlap_squared", "degenerate_group_median_overlap_squared",
                ) if key in comparison
            }
        if len(datasets) == 2:
            shared = np.array([record["freqs_thz"] for record in datasets["shared_dft"]["q_points"]])
            relaxed = np.array([record["freqs_thz"] for record in datasets["model_relaxed"]["q_points"]])
            shift = np.abs(shared - relaxed)
            record = load(root / material / "model_relaxed" / "relax_summary.json")
            selected_modes = []
            for q_index, mode_number in (([0, 0], 8), ([3, 0], 9)):
                record_a = next(row for row in datasets["shared_dft"]["q_points"] if row["q_index"] == q_index)
                record_b = next(row for row in datasets["model_relaxed"]["q_points"] if row["q_index"] == q_index)
                def complex_mode(record):
                    values = np.asarray(record["eigenvectors"][mode_number - 1])
                    return (values[..., 0] + 1j * values[..., 1]).reshape(-1)
                overlap = float(abs(np.vdot(complex_mode(record_a), complex_mode(record_b))) ** 2)
                groups = [next(group for group in item["degenerate_groups_one_based"] if mode_number in group)
                          for item in (record_a, record_b)]
                selected_modes.append({"q_index": q_index, "mode_one_based": mode_number,
                                       "same_index_overlap_squared": overlap,
                                       "degenerate_groups_one_based": groups})
            tracks[f"{material}/model_relaxed"]["relative_to_shared_dft"] = {
                "inplane_scale": None if record is None else record["best_inplane_scale"],
                "frequency_median_abs_shift_thz": float(np.median(shift)),
                "frequency_mae_thz": float(np.mean(shift)),
                "frequency_p95_abs_shift_thz": float(np.percentile(shift, 95)),
                "optical_pilot_mode_mapping": selected_modes,
            }
        for summary_path in (root / material).glob("**/stage2/*/screening/pairs/*/summary.json"):
            summary = load(summary_path)
            pilots.append({
                "material": material,
                "geometry_source": summary["signature"]["geometry_source"],
                "run_tag": summary_path.parts[-5],
                "pair_code": summary["pair_code"],
                "n_super": summary["builder"]["n_super"],
                "natoms_supercell": summary["builder"]["nat_super"],
                "points": summary["completed_points"],
                "elapsed_seconds": summary["elapsed_seconds"],
                "peak_rss_mb": None if not summary.get("resources") else summary["resources"]["peak_process_rss_mb"],
                "fit_rank": summary["analysis"]["fit_design_rank"],
                "center_fit_rmse_ev": summary["analysis"].get("center_fit_rmse_ev_supercell"),
                "phi122_mev_per_A3amu32": summary["analysis"]["physics"]["phi_122_mev_per_A3amu32"],
                "quality_flags": summary["quality_flags"],
            })
    full_counts = {}
    for material in ("mos2", "wse2"):
        for geometry in ("shared_dft", "model_relaxed"):
            screening = root / material / geometry / "stage2" / "prophet" / "screening"
            count = len(list((screening / "pairs").glob("*/summary.json"))) if screening.exists() else 0
            full_counts[f"{material}/{geometry}"] = count
    legacy = load(root / "wse2" / "legacy_dft_refit_m9.json")
    stress = load(root / "mos2" / "stress_12x12.json")
    precision = {}
    for label, filename in (("original_float32_total", "mode_pilot.json"),
                            ("float64_atomic_sum", "mode_pilot_precise.json")):
        pilot = load(root / "mos2" / filename)
        if pilot:
            precision[label] = {
                "pair_code": pilot["check"]["pair_code"],
                "mass_weighted_norms": pilot["check"]["mass_weighted_norms"],
                "slope_absolute_differences_ev_per_Q": [row["absolute_difference_ev_per_Q"] for row in pilot["check"]["slope_comparisons"]]
            }
    return {
        "kind": "prophet_stage12_v3_validation_report",
        "tracks": tracks,
        "pilots": sorted(pilots, key=lambda row: (row["material"], row["geometry_source"], row["pair_code"])),
        "full_stage2_completed_pairs": full_counts,
        "legacy_wse2_dft_refit": None if legacy is None else {
            "fit_rank": legacy["analysis"]["fit_design_rank"],
            "phi122_mev_per_A3amu32": legacy["analysis"]["physics"]["phi_122_mev_per_A3amu32"],
            "mode_mapping": legacy["mode_mapping"],
        },
        "stress_12x12": None if stress is None else {
            "natoms": stress["natoms"], "elapsed_seconds": stress["elapsed_seconds"],
            "resources": stress["resources"],
        },
        "precision_pilot": precision,
        "stable_gate_passed": all(row.get("complete_stage1") for row in tracks.values()) and all(count == 486 for count in full_counts.values()),
        "scientific_limit": "Historical QE/DFT coupling coefficients use a different mode basis and sometimes different normalization; no direct third-order superiority claim is made.",
    }


def markdown(report: dict) -> str:
    def fmt(value, digits=4):
        return "—" if value is None else f"{value:.{digits}f}"

    lines = ["# Prophet Stage1/Stage2 v3 local validation", "",
             f"Stable promotion gate: **{'passed' if report['stable_gate_passed'] else 'not yet passed'}**.", "",
             "| Material / geometry | q points | pairs | FD max Δν (THz) | acoustic row sum (eV/Å²) | QE ν MAE (THz) | RSS peak (MB) |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for name, row in sorted(report["tracks"].items()):
        qe = row.get("qe_comparison", {})
        resources = row.get("resources") or {}
        lines.append(f"| {name} | {row.get('q_count', 0)} | {row.get('pair_count', 0)} | "
                     f"{fmt(row.get('max_fd_step_frequency_change_thz'))} | "
                     f"{fmt(row.get('max_acoustic_row_sum_residual_ev_per_A2'))} | "
                     f"{fmt(qe.get('matched_frequency_mae_thz'))} | "
                     f"{fmt(resources.get('peak_process_rss_mb'), 0)} |")
    for name, row in sorted(report["tracks"].items()):
        diagnostic = row.get("fd_row_sum_diagnostic_ev_per_A2")
        if diagnostic:
            lines.extend(["", f"{name} finite-difference row-sum check: "
                          f"0.01 Å → {diagnostic['step_0p01']:.4f}, "
                          f"0.005 Å → {diagnostic['step_0p005']:.4f}, "
                          f"Richardson diagnostic → {diagnostic['richardson_diagnostic_only']:.4f} eV/Å². "
                          "The production mode pairs still use the 0.01 Å force constants."])
    lines.extend(["", "| Material / model-relaxed | lattice scale | median frequency shift (THz) |",
                  "|---|---:|---:|"])
    for name, row in sorted(report["tracks"].items()):
        delta = row.get("relative_to_shared_dft")
        if delta:
            lines.append(f"| {name} | {delta['inplane_scale']:.5f} | {delta['frequency_median_abs_shift_thz']:.4f} |")
    lines.extend(["", "| Pilot | pair | 81-point time (s) | fit rank | Φ122 (meV/(Å³·amu³ᐟ²)) |",
                  "|---|---|---:|---:|---:|"])
    for row in report["pilots"]:
        lines.append(f"| {row['material']}/{row['geometry_source']}/{row['run_tag']} | {row['pair_code']} | "
                     f"{row['elapsed_seconds']:.1f} | {row['fit_rank']} | {row['phi122_mev_per_A3amu32']:.4f} |")
    legacy = report.get("legacy_wse2_dft_refit")
    if legacy:
        lines.extend(["", f"Archived WSe₂ DFT grid refit: Φ122={legacy['phi122_mev_per_A3amu32']:.4f} "
                      f"meV/(Å³·amu³ᐟ²), rank {legacy['fit_rank']}. "
                      f"Mode mapping: **{legacy['mode_mapping']['status']}** "
                      f"({legacy['mode_mapping']['reason']})."])
    stress = report.get("stress_12x12")
    if stress:
        lines.extend(["", f"Local 12×12 stress point: {stress['natoms']} atoms, "
                      f"{stress['elapsed_seconds']:.1f} s, "
                      f"{stress['resources']['peak_process_rss_mb']:.0f} MB peak RSS."])
    lines.extend(["", "Full Stage2 completed pairs: " + ", ".join(f"{key}={value}/486" for key, value in sorted(report["full_stage2_completed_pairs"].items())) + ".",
                  "", report["scientific_limit"], ""])
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    args = parser.parse_args(argv)
    root = args.root.resolve()
    report = summarize(root)
    repository = Path(__file__).resolve().parent.parent
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    manifest = {
        "kind": "prophet_stage12_v3_local_run_manifest",
        "testing_repository": str(repository), "testing_git_commit": commit,
        "testing_worktree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=repository)),
        "python_version": sys.version.split()[0], "cpu_threads_per_local_process": 4,
        "tracks": report["tracks"],
        "pilot_summaries": report["pilots"],
    }
    (root / "validation_report.json").write_text(json.dumps(report, indent=2) + "\n")
    (root / "validation_report.md").write_text(markdown(report))
    (root / "run_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    physical_lines = ["# Prophet v3 physical comparison", "",
                      "The two structure lines are independent. Signed mode-pair couplings depend on eigenvector phase; near-degenerate single-branch coefficients depend on the basis.", "",
                      *markdown(report).split("| Pilot |")[0].splitlines()[4:], "",
                      report["scientific_limit"], ""]
    if report["legacy_wse2_dft_refit"]:
        legacy = report["legacy_wse2_dft_refit"]
        physical_lines.extend(["", f"Archived WSe₂ DFT Γ₈–M₉ refit: |Φ122|={abs(legacy['phi122_mev_per_A3amu32']):.4f} meV/(Å³·amu³ᐟ²). "
                               f"Mode mapping is {legacy['mode_mapping']['status']}; this is not a direct branchwise comparison."])
    physical_lines.extend(["", "| Γ₈–M₉ optical pilot | signed Φ122 | magnitude |",
                           "|---|---:|---:|"])
    for pilot in report["pilots"]:
        if pilot["pair_code"] == "Gamma_p0_m8__M_q_0.500_0.000_0.000_m9":
            coupling = pilot["phi122_mev_per_A3amu32"]
            physical_lines.append(f"| {pilot['material']}/{pilot['geometry_source']} | {coupling:.4f} | {abs(coupling):.4f} |")
    physical_lines.append("")
    for name, row in sorted(report["tracks"].items()):
        mapping = row.get("relative_to_shared_dft", {}).get("optical_pilot_mode_mapping")
        if mapping:
            physical_lines.append(f"{name} same-index overlaps: Γ₈={mapping[0]['same_index_overlap_squared']:.6f}, "
                                  f"M₉={mapping[1]['same_index_overlap_squared']:.6f}; "
                                  f"groups Γ₈={mapping[0]['degenerate_groups_one_based']}, "
                                  f"M₉={mapping[1]['degenerate_groups_one_based']}.")
    physical_lines.append("")
    (root / "physical_comparison_report.md").write_text("\n".join(physical_lines))
    performance_lines = ["# Prophet v3 local performance", "",
                         "Measured CPU timings include concurrent work and are not a GPU throughput estimate.", "",
                         "| Pilot | Atoms | 81-point time (s) | Peak RSS (MB) |",
                         "|---|---:|---:|---:|"]
    for pilot in report["pilots"]:
        peak = "—" if pilot["peak_rss_mb"] is None else f"{pilot['peak_rss_mb']:.0f}"
        performance_lines.append(f"| {pilot['material']}/{pilot['geometry_source']}/{pilot['run_tag']} | "
                                 f"{pilot['natoms_supercell']} | {pilot['elapsed_seconds']:.1f} | {peak} |")
    if report["stress_12x12"]:
        stress = report["stress_12x12"]
        performance_lines.extend(["", f"12×12 stress: {stress['natoms']} atoms, "
                                  f"{stress['elapsed_seconds']:.1f} s, "
                                  f"{stress['resources']['peak_process_rss_mb']:.0f} MB peak RSS."])
    performance_lines.extend(["", "GPU production throughput remains unmeasured; do not extrapolate the CPU figures to a full 157,464-point campaign.", ""])
    (root / "performance_report.md").write_text("\n".join(performance_lines))
    print(root / "validation_report.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
