"""Read-only QE references: full-grid modes and recovered legacy PES derivatives.

Never generates QE inputs or submits calculations. Different relaxed geometries
and imperfect historical provenance make these directional reference differences,
not same-displacement DFT label errors.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np
from mlff_modepair_workflow.core import (
    analyze_pair_grid,
    decode_complex_mode,
    infer_commensurate_supercell_n,
)
from mlff_modepair_workflow.phonon_eigenvectors import compare_modes
from mlff_modepair_workflow.prophet_backend import sha256_file
from mlff_modepair_workflow.units import energies_to_ev

MODELS = ("tece", "prophet", "equiformer-v3")


def read(path):
    return json.loads(path.read_text())


def matrix(record):
    values = np.column_stack(
        [decode_complex_mode(v).ravel() for v in record["eigenvectors"]]
    )
    return values / np.linalg.norm(values, axis=0)


def paths(root, model, material, geometry):
    if model == "prophet":
        base = root / "prophet-phonopy" / material / geometry
        return base / "stage1", base / "stage2/pair_ranking.json"
    stage = "phonopy_stage1_asr_stablephase" if model == "equiformer-v3" else "stage1"
    return (
        root / model / material / geometry / stage,
        root / "mattersim-stage2" / model / material / geometry / "pair_ranking.json",
    )


def legacy_fit(pair, reference, material):
    code = pair["pair_code"]
    base = (
        reference / "mos2-qe-raw"
        if material == "mos2"
        else reference / "wse2_stage2_stage3_core_20260402/stage3_real_top1"
    )
    meta = (
        base / code / "pair_meta.json"
        if material == "mos2"
        else base / "pair_meta.json"
    )
    grid = (
        base / "pair_results" / code / "energy_grid_ry.dat"
        if material == "mos2"
        else base / "energy_grid_ry.dat"
    )
    if not grid.is_file() or not meta.is_file() or read(meta)["pair_code"] != code:
        return {"status": "raw_grid_unavailable", "pair_code": code}
    q = np.asarray(pair["target_mode"]["q_frac"])
    n = infer_commensurate_supercell_n(q)
    phase = np.exp(
        2j * np.pi * (np.array([[i, j, 0] for i in range(n) for j in range(n)]) @ q)
    )
    g = decode_complex_mode(pair["gamma_mode"]["eigenvector"])
    v = decode_complex_mode(pair["target_mode"]["eigenvector_q"])
    gnorm = np.linalg.norm(g.real)
    qnorm = np.linalg.norm((phase[:, None, None] * v).real) / n
    if min(gnorm, qnorm) < 1e-8:
        raise ValueError("Cannot recover a vanishing legacy real quadrature")
    metadata = read(meta)
    x = np.asarray(metadata["a1_vals"]) * gnorm
    y = np.asarray(metadata["a2_vals"]) * qnorm
    energy = energies_to_ev(np.loadtxt(grid), "Ry")
    if energy.shape != (len(y), len(x)) or not np.isfinite(energy).all():
        raise ValueError("Invalid archived QE grid")
    fit = analyze_pair_grid(pair, energy, x, y, fit_window=1.001)
    assert fit["fit_design_rank"] == 13
    return {
        "status": "recovered_real_coordinate_fit",
        "pair_code": code,
        "energy_unit_explicit": "Ry",
        "grid_layout": "a2_rows_a1_columns",
        "grid_sha256": sha256_file(grid),
        "metadata_sha256": sha256_file(meta),
        "gamma_real_norm": float(gnorm),
        "finite_q_real_norm": float(qnorm),
        "fit_x_values": x[abs(x) <= 1.001].tolist(),
        "fit_y_values": y[abs(y) <= 1.001].tolist(),
        "analysis": fit,
    }


def match_vector(encoded, record):
    v = decode_complex_mode(encoded).ravel()
    v /= np.linalg.norm(v)
    overlaps = abs(v.conj() @ matrix(record)) ** 2
    index = int(np.argmax(overlaps))
    group = next(g for g in record["degenerate_groups_one_based"] if index + 1 in g)
    return index + 1, float(overlaps[index]), group


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--qe-reference", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    results = {
        "scope": "historical_directional_reference_not_same_displacement_DFT_error",
        "runs": {},
        "legacy_fits": {},
    }
    mode_rows = []
    coupling_rows = []
    for material in ("mos2", "wse2"):
        qe_path = args.archive / "historical_qe_v3" / material / "phonon_dataset.json"
        qe = read(qe_path)
        qe_by_q = {tuple(r["q_index"]): r for r in qe["q_points"]}
        assert len(qe_by_q) == 36
        oldpath = (
            args.qe_reference
            / "mos2_stage2_stage3_core_20260402/baseline_local/mos2_gptff_v1_stage3_run/stage1/outputs/mode_pairs.selected.json"
            if material == "mos2"
            else args.qe_reference
            / "wse2_stage2_stage3_core_20260402/baseline_local/wse2_stage3_run/stage1/outputs/mode_pairs.selected.json"
        )
        oldpairs = read(oldpath)["pairs"]
        # The historical five-pair list is kept explicitly in the prior audit.
        prior = read(Path(__file__).resolve().parents[1] / "docs/validation_v5.json")[
            "archived_dft"
        ][material]["tece"]["five_pair_proxy"]["rows"]
        codes = [r["archived_qe_pair"] for r in prior]
        pairs = [next(p for p in oldpairs if p["pair_code"] == code) for code in codes]
        fits = {
            p["pair_code"]: legacy_fit(p, args.qe_reference, material) for p in pairs
        }
        results["legacy_fits"][material] = fits
        for geometry in ("model_relaxed", "shared_dft"):
            for model in MODELS:
                stage, ranking_path = paths(args.archive, model, material, geometry)
                phonon_path = stage / "phonon_dataset.json"
                pair_path = stage / "mode_pairs.selected.json"
                phonon = read(phonon_path)
                ranking = read(ranking_path)
                sig = ranking["signature"]
                if (
                    sig["mode_pairs_sha256"] != sha256_file(pair_path)
                    or sig["structure_sha256"] != phonon["source"]["structure_sha256"]
                    or sig["geometry_source"] != geometry
                ):
                    raise ValueError(f"Broken Stage1/Stage2 provenance: {stage}")
                by_q = {tuple(r["q_index"]): r for r in phonon["q_points"]}
                assert set(by_q) == set(qe_by_q)
                rows = []
                for q, ref in qe_by_q.items():
                    cand = by_q[q]
                    assignment, overlap, groups = compare_modes(
                        np.asarray(ref["freqs_thz"]),
                        matrix(ref),
                        np.asarray(cand["freqs_thz"]),
                        matrix(cand),
                        degeneracy_thz=0.01,
                        gamma_acoustic=False,
                    )
                    for i, j in enumerate(assignment):
                        group = next(g for g in groups if i + 1 in g["qe_modes"])
                        block = (
                            matrix(ref)[:, np.asarray(group["qe_modes"]) - 1].conj().T
                            @ matrix(cand)[:, np.asarray(group["ml_modes"]) - 1]
                        )
                        singular = np.linalg.svd(block, compute_uv=False) ** 2
                        rows.append(
                            {
                                "material": material,
                                "model": model,
                                "geometry": geometry,
                                "qx": q[0] / 6,
                                "qy": q[1] / 6,
                                "qe_mode": i + 1,
                                "ml_mode": int(j) + 1,
                                "qe_frequency_thz": ref["freqs_thz"][i],
                                "ml_frequency_thz": cand["freqs_thz"][j],
                                "frequency_difference_thz": cand["freqs_thz"][j]
                                - ref["freqs_thz"][i],
                                "single_overlap_squared": float(overlap[i]),
                                "qe_subspace_dimension": len(group["qe_modes"]),
                                "subspace_minimum_overlap_squared": float(
                                    min(singular)
                                ),
                                "gamma_point": q == (0, 0),
                            }
                        )
                mode_rows.extend(rows)
                error = np.array([r["frequency_difference_thz"] for r in rows])
                finite = np.array(
                    [
                        r["frequency_difference_thz"]
                        for r in rows
                        if not r["gamma_point"]
                    ]
                )
                isolated = [
                    r["single_overlap_squared"]
                    for r in rows
                    if r["qe_subspace_dimension"] == 1 and not r["gamma_point"]
                ]
                key = f"{material}/{geometry}/{model}"
                results["runs"][key] = {
                    "source_hashes": {
                        "qe": sha256_file(qe_path),
                        "ml_phonon": sha256_file(phonon_path),
                        "ranking": sha256_file(ranking_path),
                        "old_pairs": sha256_file(oldpath),
                    },
                    "qe_geometry_verified": qe["source"].get(
                        "qe_geometry_verified", False
                    ),
                    "all_frequency_mae_thz": float(np.mean(abs(error))),
                    "all_frequency_rmse_thz": float(np.sqrt(np.mean(error**2))),
                    "finite_q_frequency_mae_thz": float(np.mean(abs(finite))),
                    "finite_q_frequency_rmse_thz": float(np.sqrt(np.mean(finite**2))),
                    "isolated_finite_q_median_overlap_squared": float(
                        np.median(isolated)
                    ),
                    "mode_count": len(rows),
                }
                ranked = {
                    (
                        round(r["qx"] * 6) % 6,
                        round(r["qy"] * 6) % 6,
                        r["gamma_mode_number"],
                        r["target_mode_number"],
                    ): r
                    for r in ranking["pairs"]
                }
                for pair in pairs:
                    q = tuple(
                        round(v * 6) % 6 for v in pair["target_mode"]["q_frac"][:2]
                    )
                    gm, go, gg = match_vector(
                        pair["gamma_mode"]["eigenvector"], by_q[(0, 0)]
                    )
                    tm, to, tg = match_vector(
                        pair["target_mode"]["eigenvector_q"], by_q[q]
                    )
                    matched_row = ranked.get((*q, gm, tm))
                    fit = fits[pair["pair_code"]]
                    row = {
                        "material": material,
                        "geometry": geometry,
                        "model": model,
                        "qe_pair": pair["pair_code"],
                        "ml_pair": matched_row["pair_code"] if matched_row else None,
                        "gamma_overlap_squared": go,
                        "q_overlap_squared": to,
                        "gamma_subspace_dimension": len(gg),
                        "q_subspace_dimension": len(tg),
                        "comparison_level": "directional_reference"
                        if matched_row and min(go, to) >= 0.9
                        else "unreliable_mapping",
                        "raw_qe_status": fit["status"],
                        "qe_abs_phi122": None,
                        "qe_phi1122": None,
                        "ml_abs_phi122": abs(matched_row["phi122_mev"])
                        if matched_row
                        else None,
                        "ml_phi1122": matched_row.get("phi1122_mev_per_A4amu2")
                        if matched_row
                        else None,
                    }
                    if fit["status"] == "recovered_real_coordinate_fit":
                        physics = fit["analysis"]["physics"]
                        row["qe_abs_phi122"] = abs(physics["phi_122_mev_per_A3amu32"])
                        row["qe_phi1122"] = physics["phi_1122_mev_per_A4amu2"]
                    else:
                        old = next(
                            r
                            for r in prior
                            if r["archived_qe_pair"] == pair["pair_code"]
                        )
                        row["qe_abs_phi122"] = old["qe_abs_phi122_mev"]
                        row["raw_qe_status"] = (
                            "archived_third_order_only_raw_grid_unavailable"
                        )
                    coupling_rows.append(row)
    for name, rows in [("phonon_modes", mode_rows), ("couplings", coupling_rows)]:
        with (args.output / f"archived_dft_{name}.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(
                stream, fieldnames=list(rows[0]), lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(rows)
    results["limitations"] = [
        "No new DFT labels. Historical QE structure provenance is not fully verified.",
        "The WSe2 full-grid reference is the older 65/650 Ry 12x12 raw-QE subset re-exported with dynmat asr='no'; its nonzero Gamma acoustic frequencies must not be scored against ASR-corrected MLFF modes. This does not diagnose the separate 80/800 Ry selected-mode/PES archive as defective.",
        "Old finite-q real-coordinate amplitudes are recovered before refitting; K fit samples span a different actual Q window.",
        "Raw fourth-order QE grids available for five MoS2 pairs and one WSe2 pair; missing four WSe2 raw grids are not imputed.",
        "Finite-q degenerate branches remain directional and basis dependent.",
    ]
    (args.output / "archived_dft_comparison.json").write_text(
        json.dumps(results, indent=2, allow_nan=False) + "\n"
    )
    print(json.dumps({k: v for k, v in results["runs"].items()}, indent=2))


if __name__ == "__main__":
    main()
