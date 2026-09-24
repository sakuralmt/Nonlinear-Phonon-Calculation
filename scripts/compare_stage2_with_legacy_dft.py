"""Compare complete v3 MLFF rankings with the archived five-pair QE rechecks.

The old QE rank field is the Stage2 selection rank, not a QE coupling rank.
Re-rank the five QE values after recovering their actual real-mode units.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

import numpy as np

from mlff_modepair_workflow.core import analyze_pair_grid, load_atoms_from_qe
from mlff_modepair_workflow.units import energies_to_ev
from scripts.refit_legacy_dft_grid import legacy_real_norms, map_to_prophet


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _refit_raw(pair: dict, norm: dict, raw_root: Path):
    code = pair["pair_code"]
    meta_path = raw_root / code / "pair_meta.json"
    grid_path = raw_root / "pair_results" / code / "energy_grid_ry.dat"
    if not (meta_path.is_file() and grid_path.is_file()):
        return None
    meta = json.loads(meta_path.read_text())
    if meta["pair_code"] != code:
        raise ValueError(f"Raw QE pair metadata does not match {code}")
    q1 = np.asarray(meta["a1_vals"], dtype=float) * norm["gamma_mass_weighted_norm_per_legacy_amplitude"]
    q2 = np.asarray(meta["a2_vals"], dtype=float) * norm["target_mass_weighted_norm_per_legacy_amplitude"]
    grid = energies_to_ev(np.loadtxt(grid_path), "Ry")
    if grid.shape != (len(q2), len(q1)):
        raise ValueError(f"Raw QE energy-grid shape does not match amplitudes: {code}")
    analysis = analyze_pair_grid(pair, grid, q1, q2, fit_window=1.001)
    if analysis["fit_design_rank"] != 13:
        raise ValueError(f"Raw QE refit is rank deficient: {code}")
    return analysis["physics"]["phi_122_mev_per_A3amu32"], {
        "method": "raw_ry_grid_refit", "grid_sha256": _sha256(grid_path),
        "fit_design_rank": analysis["fit_design_rank"],
    }


def _subset_rank(values: dict[str, float]) -> dict[str, int]:
    ordered = sorted(values, key=lambda code: (-abs(values[code]), code))
    return {code: i for i, code in enumerate(ordered, start=1)}


def _spearman(a: list[int], b: list[int]) -> float:
    n = len(a)
    if n < 2:
        return float("nan")
    return 1 - 6 * sum((x - y) ** 2 for x, y in zip(a, b)) / (n * (n * n - 1))


def compare(args) -> dict:
    old_structure_hash = _sha256(args.old_structure)
    new_structure_hash = _sha256(args.new_structure)
    if old_structure_hash != new_structure_hash:
        raise ValueError("Old QE and v3 MLFF structures are not byte-identical")
    old = json.loads(args.old_mode_pairs_json.read_text())
    new = json.loads(args.new_mode_pairs_json.read_text())
    phonon = json.loads(args.new_phonon_dataset.read_text())
    qe_rows = json.loads(args.qe_ranking_json.read_text())["rows"]
    if new["source"]["structure_sha256"] != new_structure_hash:
        raise ValueError("v3 Stage1 structure hash differs from the supplied structure")
    if len(qe_rows) != 5 or len({row["pair_code"] for row in qe_rows}) != 5:
        raise ValueError("Expected exactly five unique historical QE rechecks")
    old_pairs = {row["pair_code"]: row for row in old["pairs"]}
    primitive = load_atoms_from_qe(args.old_structure)
    archived_refit = {}
    for path in args.precomputed_refit:
        payload = json.loads(path.read_text())
        if payload["structure_sha256"] != old_structure_hash:
            raise ValueError(f"Precomputed QE refit has a different structure: {path}")
        if payload["old_mode_pairs_sha256"] != _sha256(args.old_mode_pairs_json):
            raise ValueError(f"Precomputed QE refit uses different old modes: {path}")
        archived_refit[payload["pair_code"]] = payload

    dft = []
    for row in qe_rows:
        code = row["pair_code"]
        if code not in old_pairs:
            raise ValueError(f"QE recheck missing from old mode pairs: {code}")
        pair = old_pairs[code]
        norm = legacy_real_norms(pair, primitive)
        factor = 1 / (norm["gamma_mass_weighted_norm_per_legacy_amplitude"]
                      * norm["target_mass_weighted_norm_per_legacy_amplitude"] ** 2)
        raw = _refit_raw(pair, norm, args.raw_grid_root) if args.raw_grid_root else None
        if raw:
            phi, evidence = raw
        elif code in archived_refit:
            refit = archived_refit[code]
            phi = refit["analysis"]["physics"]["phi_122_mev_per_A3amu32"]
            evidence = {"method": "verified_archived_raw_grid_refit",
                        "grid_sha256": refit["energy_grid_sha256"],
                        "fit_design_rank": refit["analysis"]["fit_design_rank"]}
        else:
            phi = row["qe_phi122_mev"] * factor
            evidence = {"method": "archived_qe_fit_scaled_by_measured_real_mode_norm"}
        mapping = map_to_prophet(pair, new, phonon, new_structure_hash)
        matches = mapping.get("matches", [])
        q = np.asarray(pair["target_mode"]["q_frac"], dtype=float) % 1
        mapped_pair = None
        if len(matches) == 2 and min(x["overlap_squared"] for x in matches) >= 0.99:
            mapped_pair = next((candidate for candidate in new["pairs"]
                if candidate["gamma_mode"]["mode_number_one_based"] == matches[0]["matched_mode_one_based"]
                and candidate["target_mode"]["mode_number_one_based"] == matches[1]["matched_mode_one_based"]
                and np.allclose(np.asarray(candidate["target_mode"]["q_frac"]) % 1, q, atol=1e-6)), None)
        comparison_level = (
            "single_mode_magnitude" if mapping["status"] == "magnitude_only" and mapped_pair else
            "high_overlap_subspace_proxy" if mapping["status"] == "subspace_only" and mapped_pair else
            "unmapped"
        )
        dft.append({
            "old_pair_code": code, "new_pair_code": mapped_pair["pair_code"] if mapped_pair else None,
            "qe_phi122_archived_mev": row["qe_phi122_mev"],
            "qe_phi122_normalized_mev": float(phi),
            "qe_abs_phi122_normalized_mev": abs(float(phi)),
            "legacy_coordinate_factor": factor,
            "qe_evidence": evidence,
            "mode_mapping_status": mapping["status"],
            "comparison_level": comparison_level,
            "gamma_overlap_squared": matches[0]["overlap_squared"] if matches else None,
            "target_overlap_squared": matches[1]["overlap_squared"] if matches else None,
            "gamma_near_degenerate_group": matches[0]["group_one_based"] if matches else None,
            "target_near_degenerate_group": matches[1]["group_one_based"] if matches else None,
        })
    dft_rank = _subset_rank({row["old_pair_code"]: row["qe_phi122_normalized_mev"] for row in dft})
    for row in dft:
        row["qe_subset_rank"] = dft_rank[row["old_pair_code"]]

    comparisons = {}
    for value in args.mlff_screening:
        backend, sep, path_string = value.partition("=")
        if not sep or not backend or not path_string or backend in comparisons:
            raise ValueError("--mlff-screening must be unique backend=/absolute/screening/path")
        directory = Path(path_string)
        meta = json.loads((directory / "run_meta.json").read_text())
        ranking = json.loads((directory / "pair_ranking.json").read_text())
        if meta["backend"]["backend"] != backend or ranking["backend"]["backend"] != backend:
            raise ValueError(f"Backend identity mismatch in {directory}")
        if meta["signature"]["structure_sha256"] != new_structure_hash:
            raise ValueError(f"MLFF structure differs from QE: {directory}")
        if meta["signature"]["mode_pairs_sha256"] != _sha256(args.new_mode_pairs_json):
            raise ValueError(f"MLFF mode-pair input differs from v3 Stage1: {directory}")
        if meta["n_pairs"] != 486 or meta["n_energy_evaluations"] != 486 * 81:
            raise ValueError(f"Incomplete 486-pair MLFF run: {directory}")
        by_code = {row["pair_code"]: row for row in ranking["pairs"]}
        if len(by_code) != 486:
            raise ValueError(f"Duplicate or missing MLFF ranking entries: {directory}")
        rows = []
        selected_phi = {}
        for reference in dft:
            code = reference["new_pair_code"]
            if code is None or code not in by_code:
                continue
            model = by_code[code]
            phi = float(model["phi122_mev_per_A3amu32"])
            selected_phi[reference["old_pair_code"]] = phi
            rows.append({
                **reference, "backend": backend,
                "mlff_phi122_mev": phi, "mlff_abs_phi122_mev": abs(phi),
                "mlff_full_rank_of_486": int(model["rank"]),
                "abs_magnitude_error_mev": abs(abs(phi) - reference["qe_abs_phi122_normalized_mev"]),
                "relative_magnitude_error": abs(abs(phi) - reference["qe_abs_phi122_normalized_mev"])
                    / reference["qe_abs_phi122_normalized_mev"],
                "quality_flags": model.get("quality_flags", ""),
            })
        selected_rank = _subset_rank(selected_phi)
        for row in rows:
            row["mlff_subset_rank"] = selected_rank[row["old_pair_code"]]
        rows.sort(key=lambda row: row["qe_subset_rank"])
        if len(rows) != 5:
            raise ValueError(f"Only {len(rows)} of five QE pairs could be mapped for {backend}")
        comparisons[backend] = {
            "rows": rows,
            "metrics": {
                "qe_recheck_count": 5,
                "single_mode_count": sum(row["comparison_level"] == "single_mode_magnitude" for row in rows),
                "subspace_proxy_count": sum(row["comparison_level"] == "high_overlap_subspace_proxy" for row in rows),
                "top3_overlap_count": len({row["old_pair_code"] for row in rows if row["qe_subset_rank"] <= 3}
                    & {row["old_pair_code"] for row in rows if row["mlff_subset_rank"] <= 3}),
                "spearman_within_five": _spearman(
                    [row["qe_subset_rank"] for row in rows],
                    [row["mlff_subset_rank"] for row in rows]),
                "max_mlff_full_rank_among_five": max(row["mlff_full_rank_of_486"] for row in rows),
                "median_relative_magnitude_error": float(np.median([row["relative_magnitude_error"] for row in rows])),
            },
        }
    return {
        "kind": "v3_mlff_vs_legacy_five_qe_rechecks",
        "material": args.material,
        "caveat": "Only five selected QE pairs were rechecked; this is not a full 486-pair QE ranking. "
                  "Subspace proxies are high-overlap branch choices, not basis-invariant couplings.",
        "provenance": {
            "old_structure_sha256": old_structure_hash,
            "new_structure_sha256": new_structure_hash,
            "old_mode_pairs_sha256": _sha256(args.old_mode_pairs_json),
            "new_mode_pairs_sha256": _sha256(args.new_mode_pairs_json),
            "qe_ranking_sha256": _sha256(args.qe_ranking_json),
        },
        "comparisons": comparisons,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--material", required=True)
    parser.add_argument("--old-mode-pairs-json", type=Path, required=True)
    parser.add_argument("--old-structure", type=Path, required=True)
    parser.add_argument("--qe-ranking-json", type=Path, required=True)
    parser.add_argument("--new-mode-pairs-json", type=Path, required=True)
    parser.add_argument("--new-phonon-dataset", type=Path, required=True)
    parser.add_argument("--new-structure", type=Path, required=True)
    parser.add_argument("--raw-grid-root", type=Path)
    parser.add_argument("--precomputed-refit", type=Path, action="append", default=[])
    parser.add_argument("--mlff-screening", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compare(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    for backend, comparison in result["comparisons"].items():
        csv_path = args.output.with_name(f"{args.output.stem}_{backend}.csv")
        rows = comparison["rows"]
        with csv_path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    print(args.output)


if __name__ == "__main__":
    main()
