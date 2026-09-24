"""Audit the fixed-geometry MLFF Stage1 + MatterSim Stage2 campaign against archived QE."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import numpy as np

from mlff_modepair_workflow.compare_stage1_models_v3 import compare as compare_meshes


MODELS = ("prophet", "equiformer-v3", "tece", "equflashv2")
EXPECTED_MATTERSIM_SHA256 = "e3df9fa708725e3d453140646c7d1838324b347a3d1214cf1440522146f872b5"
EXPECTED_STAGE1 = {
    "prophet": ("c4fda8251d8a7c90c7cb7842aea4d2f57e5fc3bd",
                "28b21122f4c6c1a7c5fe9bac8a0182edf9d24a7a65a72a9ca80b8d12d4620514"),
    "equiformer-v3": ("a7300c58df683dc99cb48027d5bfd4c887486c48",
                       "429ccded98163122e7ba588d78e2441653f37f3e091e106c432807fe373c8f98"),
    "tece": ("81f65a4c188bd09cec8d1419388f7afdcc1b6fd0",
             "9f36562582d931347c3904f763e820edcaf5f27c3f13beb6776e49fcc7de38bb"),
    "equflashv2": ("16b5cae474370977b59120e8bc57e4bcc19cd093",
                   "068ce25767d5064fae41ffac352484ae59a8544ee671607a09dffdc6d62402a2"),
}


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read(path: Path) -> dict:
    return json.loads(path.read_text())


def _validate_stage1_source(source: dict, model: str) -> None:
    expected_commit, expected_weight = EXPECTED_STAGE1[model]
    actual = source["model"]
    if (actual["source_commit"] != expected_commit
            or actual["checkpoint_sha256"] != expected_weight):
        raise ValueError(f"Unexpected {model} source or weight")


def _paths(root: Path, material: str, model: str, geometry: str) -> tuple[Path, Path, Path]:
    if model == "prophet":
        stage1 = root / "prophet-phonopy" / material / geometry / "stage1"
        ranking = root / "prophet-phonopy" / material / geometry / "stage2" / "pair_ranking.json"
    elif model == "equiformer-v3":
        stage1 = root / model / material / geometry / "phonopy_stage1_asr_stablephase"
        ranking = root / "mattersim-stage2" / model / material / geometry / "pair_ranking.json"
    elif model == "tece":
        stage1 = root / model / material / geometry / "stage1"
        ranking = root / "mattersim-stage2" / model / material / geometry / "pair_ranking.json"
    else:
        stage1 = root / model / material / geometry / "phonopy_stage1_asr_cpu"
        ranking = root / "mattersim-stage2" / model / material / geometry / "pair_ranking.json"
    return stage1 / "phonon_dataset.json", stage1 / "mode_pairs.selected.json", ranking


def _key(pair: dict, mesh_n: int) -> tuple[int, int, int, int]:
    q = pair["target_mode"]["q_frac"]
    return (round(q[0] * mesh_n) % mesh_n, round(q[1] * mesh_n) % mesh_n,
            pair["gamma_mode"]["mode_number_one_based"],
            pair["target_mode"]["mode_number_one_based"])


def _validated_run(dataset_file: Path, pairs_file: Path, ranking_file: Path):
    dataset, pairs, ranking = map(_read, (dataset_file, pairs_file, ranking_file))
    source = dataset["source"]
    signature = ranking["signature"]
    if (source["structure_sha256"] != pairs["source"]["structure_sha256"]
            or source["structure_sha256"] != signature["structure_sha256"]
            or _sha(pairs_file) != signature["mode_pairs_sha256"]
            or source["geometry_source"] != signature["geometry_source"]
            or source["normalization_version"] != signature["normalization_version"]):
        raise ValueError(f"Stage1/Stage2 source hash mismatch: {ranking_file}")
    if (ranking["backend"]["backend"] != "mattersim"
            or ranking["backend"]["checkpoint_sha256"] != EXPECTED_MATTERSIM_SHA256
            or signature["checkpoint_sha256"] != EXPECTED_MATTERSIM_SHA256):
        raise ValueError(f"Stage2 is not MatterSim: {ranking_file}")
    if (source["phonon_engine"]["name"] != "phonopy"
            or source["phonon_engine"]["version"] != "2.38.0"
            or not source["phonon_engine"]["acoustic_sum_rule"]):
        raise ValueError(f"Not the pinned Phonopy+ASR route: {dataset_file}")
    if (len(signature["a1_values"]) != 9 or len(signature["a2_values"]) != 9
            or signature["fit_window"] != 1.0
            or len(dataset["q_points"]) != 36 or len(dataset["q_orbits"]) != 6
            or sum(orbit["size"] for orbit in dataset["q_orbits"]) != 35
            or len(pairs["pairs"]) != 486
            or len(ranking["pairs"]) != 486 or
            len({p["pair_code"] for p in ranking["pairs"]}) != 486 or
            {r["pair_code"] for r in ranking["pairs"]} != {p["pair_code"] for p in pairs["pairs"]}
            or any(r["fit_design_rank"] != 13 or not math.isfinite(r["phi122_mev"])
                   for r in ranking["pairs"])):
        raise ValueError(f"Incomplete or invalid 6x6/486-pair result: {ranking_file}")
    return dataset, pairs, ranking


def _frequency_metrics(qe: dict, dataset: dict) -> dict:
    compared = compare_meshes(qe, dataset)
    groups = {"gamma_optical": [], "finite_q_acoustic": [], "finite_q_optical": []}
    for row in compared["q_records"]:
        error = row["matched_frequency_abs_error_thz"]
        if row["q_index"] == [0, 0]:
            groups["gamma_optical"].extend(error[3:])
        else:
            groups["finite_q_acoustic"].extend(error[:3])
            groups["finite_q_optical"].extend(error[3:])
    return {
        "all_mae_thz": compared["matched_frequency_mae_thz"],
        "all_rmse_thz": compared["matched_frequency_rmse_thz"],
        **{f"{name}_mae_thz": float(np.mean(values)) for name, values in groups.items()},
        "isolated_mode_median_overlap_squared": compared["isolated_mode_median_overlap_squared"],
        "degenerate_group_median_subspace_overlap": compared["degenerate_group_median_subspace_overlap"],
    }


def _physical_channels(reference: tuple[dict, dict, dict], candidate: tuple[dict, dict, dict],
                       *, allow_different_structures: bool = False,
                       degeneracy_thz: float = 0.1) -> dict:
    ref_dataset, ref_pairs, ref_rank = reference
    cand_dataset, cand_pairs, cand_rank = candidate
    mapping = compare_meshes(ref_dataset, cand_dataset,
                             allow_different_structures=allow_different_structures,
                             degeneracy_thz=degeneracy_thz)
    rec = {tuple(row["q_index"]): row for row in mapping["q_records"]}
    n = ref_dataset["source"]["q_grid"][0]
    ref_codes = {_key(pair, n): pair["pair_code"] for pair in ref_pairs["pairs"]}
    cand_codes = {_key(pair, n): pair["pair_code"] for pair in cand_pairs["pairs"]}
    ref_values = {row["pair_code"]: row["phi122_mev"] for row in ref_rank["pairs"]}
    cand_values = {row["pair_code"]: row["phi122_mev"] for row in cand_rank["pairs"]}
    gamma_groups = rec[(0, 0)]["reference_degenerate_subspaces"]
    targets = sorted({(i, j, mode) for i, j, _, mode in ref_codes})
    channels = []
    for i, j, mode in targets:
        q_match = rec[(i, j)]
        mapped_mode = q_match["reference_to_candidate_modes_one_based"][mode - 1]
        overlap = q_match["single_mode_overlap_squared"][mode - 1]
        for group in gamma_groups:
            ref_gamma = group["reference_modes_one_based"]
            cand_gamma = group["candidate_modes_one_based"]
            if len(ref_gamma) != len(cand_gamma):
                raise ValueError("Gamma multiplet dimensions differ")
            ref_score = math.sqrt(sum(ref_values[ref_codes[(i, j, gamma, mode)]] ** 2
                                      for gamma in ref_gamma))
            cand_score = math.sqrt(sum(cand_values[cand_codes[(i, j, gamma, mapped_mode)]] ** 2
                                       for gamma in cand_gamma))
            channels.append({"q_index": [i, j], "q_mode_reference": mode,
                             "q_mode_candidate": mapped_mode,
                             "gamma_modes_reference": ref_gamma,
                             "gamma_modes_candidate": cand_gamma,
                             "q_single_mode_overlap_squared": overlap,
                             "gamma_subspace_overlap": group["subspace_overlap"],
                             "reference_norm_mev": ref_score,
                             "candidate_norm_mev": cand_score})
    identity = lambda row: (tuple(row["q_index"]), row["q_mode_reference"],
                            tuple(row["gamma_modes_reference"]))
    left = sorted(channels, key=lambda x: -x["reference_norm_mev"])
    right = sorted(channels, key=lambda x: -x["candidate_norm_mev"])
    reliable = [row for row in channels if row["q_single_mode_overlap_squared"] >= 0.9]
    ref_ranks = np.argsort(np.argsort([row["reference_norm_mev"] for row in reliable]))
    cand_ranks = np.argsort(np.argsort([row["candidate_norm_mev"] for row in reliable]))
    top_20 = left[:20]
    return {
        "channel_count": len(channels),
        "comparison_status": mapping["comparison_status"],
        "q_mode_overlap_squared_min": min(row["q_single_mode_overlap_squared"] for row in channels),
        "channels_below_0_9_q_overlap": sum(row["q_single_mode_overlap_squared"] < 0.9 for row in channels),
        "reliable_channel_count": len(reliable),
        "spearman_strength_rank_reliable_channels": float(np.corrcoef(ref_ranks, cand_ranks)[0, 1]),
        "top_20_reference_median_absolute_strength_difference_mev": float(np.median([
            abs(row["reference_norm_mev"] - row["candidate_norm_mev"]) for row in top_20
        ])),
        "top_20_reference_median_relative_strength_difference": float(np.median([
            abs(row["reference_norm_mev"] - row["candidate_norm_mev"]) / row["reference_norm_mev"]
            for row in top_20
        ])),
        "top_k": {str(k): {
            "overlap": len({identity(x) for x in left[:k]} & {identity(x) for x in right[:k]}),
            "unreliable_q_modes_in_either_top_k": sum(
                row["q_single_mode_overlap_squared"] < 0.9
                for row in {identity(x): x for x in left[:k] + right[:k]}.values()
            ),
        } for k in (5, 10, 20, 30)},
        "top_20_reference": left[:20], "top_20_candidate": right[:20],
        "note": "Gamma multiplets use ||Phi_iqq||. A finite-q branch overlap below 0.9 makes "
                "that branch's comparison basis-dependent; its top-K overlap is diagnostic only.",
    }


def _legacy_five(root: Path, material: str, candidate: tuple[dict, dict, dict]) -> dict:
    cand_dataset, cand_pairs, cand_rank = candidate
    old_root = root / material / "shared_dft_final" / "stage1"
    old_dataset = _read(old_root / "phonon_dataset.json")
    old_pairs_path = old_root / "mode_pairs.selected.json"
    old_pairs = _read(old_pairs_path)
    dft = _read(root / "server_results" / material / "qe_vs_mattersim_five.json")
    if (_sha(old_pairs_path) != dft["provenance"]["new_mode_pairs_sha256"]
            or cand_dataset["source"]["structure_sha256"] != dft["provenance"]["new_structure_sha256"]):
        raise ValueError("Archived QE five-pair source does not match candidate geometry or old Stage1")
    mapping = compare_meshes(old_dataset, cand_dataset)
    mapped_q = {tuple(row["q_index"]): row for row in mapping["q_records"]}
    n = old_dataset["source"]["q_grid"][0]
    old_by_code = {row["pair_code"]: row for row in old_pairs["pairs"]}
    cand_codes = {_key(pair, n): pair["pair_code"] for pair in cand_pairs["pairs"]}
    cand_values = {row["pair_code"]: row for row in cand_rank["pairs"]}
    rows = []
    for prior in dft["comparisons"]["mattersim"]["rows"]:
        old_pair = old_by_code[prior["new_pair_code"]]
        i, j, gamma, target = _key(old_pair, n)
        gamma_rec, target_rec = mapped_q[(0, 0)], mapped_q[(i, j)]
        new_gamma = gamma_rec["reference_to_candidate_modes_one_based"][gamma - 1]
        new_target = target_rec["reference_to_candidate_modes_one_based"][target - 1]
        new_code = cand_codes[(i, j, new_gamma, new_target)]
        model_row = cand_values[new_code]
        gamma_overlap = gamma_rec["single_mode_overlap_squared"][gamma - 1]
        target_overlap = target_rec["single_mode_overlap_squared"][target - 1]
        dft_value = prior["qe_abs_phi122_normalized_mev"]
        model_value = abs(model_row["phi122_mev"])
        reliable_branch = min(gamma_overlap, target_overlap) >= 0.95
        rows.append({
            "archived_qe_pair": prior["old_pair_code"], "old_prophet_pair": prior["new_pair_code"],
            "candidate_pair": new_code, "qe_abs_phi122_mev": dft_value,
            "candidate_abs_phi122_mev": model_value,
            "candidate_full_rank_of_486": model_row["rank"],
            "magnitude_abs_difference_mev": abs(dft_value - model_value) if reliable_branch else None,
            "relative_magnitude_difference": abs(dft_value - model_value) / dft_value if reliable_branch else None,
            "qe_to_old_prophet_gamma_overlap_squared": prior["gamma_overlap_squared"],
            "qe_to_old_prophet_q_overlap_squared": prior["target_overlap_squared"],
            "old_to_candidate_gamma_overlap_squared": gamma_overlap,
            "old_to_candidate_q_overlap_squared": target_overlap,
            "archived_qe_mapping_level": prior["comparison_level"],
            "comparison_level": "high_overlap_directional_proxy" if reliable_branch else "subspace_mapping_only",
            "qe_evidence": prior["qe_evidence"],
            "legacy_coordinate_factor": prior["legacy_coordinate_factor"],
        })
    valid = [row for row in rows if row["relative_magnitude_difference"] is not None]
    return {"count": len(rows), "directional_proxy_count": len(valid),
            "median_relative_magnitude_difference": float(np.median(
                [row["relative_magnitude_difference"] for row in valid])) if valid else None,
            "rows": rows,
            "note": "Only five previously selected QE mode-pair grids exist. Their modes and v2 real "
                    "coordinate scales were mapped first. These are directional proxies, not a "
                    "basis-invariant five-pair DFT MAE or a 486-pair QE ranking."}


def analyze(root: Path, *, physical_channel_degeneracy_thz: float = 0.1) -> dict:
    if physical_channel_degeneracy_thz <= 0:
        raise ValueError("physical_channel_degeneracy_thz must be positive")
    result = {"kind": "phonopy_stage1_mattersim_stage2_model_campaign", "materials": {},
              "qe_stage3_new_jobs": 0,
              "physical_channel_degeneracy_threshold_thz": physical_channel_degeneracy_thz,
              "note": "Archived QE geometry provenance is not independently verified; no new DFT jobs were run."}
    for material in ("mos2", "wse2"):
        qe = _read(root / "historical_qe_v3" / material / "phonon_dataset.json")
        if qe["source"].get("qe_geometry_verified") is not False:
            raise ValueError("Expected explicitly unverified historical QE geometry")
        material_result = {"qe_source": qe["source"], "models": {}, "vs_prophet": {},
                           "own_geometry": {}, "own_vs_prophet": {}}
        runs = {}
        mattersim_checkpoint_sha256 = None
        own_runs = {}
        for model in MODELS:
            paths = _paths(root, material, model, "shared_dft")
            if not all(path.is_file() for path in paths):
                partial = {"status": "incomplete",
                           "missing": [str(path) for path in paths if not path.is_file()]}
                if paths[0].is_file() and paths[1].is_file():
                    dataset, pairs = _read(paths[0]), _read(paths[1])
                    _validate_stage1_source(dataset["source"], model)
                    if (len(dataset["q_points"]) != 36 or len(pairs["pairs"]) != 486
                            or dataset["source"]["structure_sha256"] != pairs["source"]["structure_sha256"]
                            or dataset["source"]["phonon_engine"]["name"] != "phonopy"
                            or dataset["source"]["phonon_engine"]["version"] != "2.38.0"
                            or not dataset["source"]["phonon_engine"]["acoustic_sum_rule"]):
                        raise ValueError(f"Invalid partial Stage1: {paths[0]}")
                    partial["stage1_frequency_vs_archived_qe"] = _frequency_metrics(qe, dataset)
                    partial["stage1_dataset_sha256"] = _sha(paths[0])
                material_result["models"][model] = partial
                continue
            run = _validated_run(*paths)
            _validate_stage1_source(run[0]["source"], model)
            current_checkpoint = run[2]["backend"]["checkpoint_sha256"]
            if mattersim_checkpoint_sha256 is not None and current_checkpoint != mattersim_checkpoint_sha256:
                raise ValueError("Different MatterSim weights across Stage1 replacement models")
            mattersim_checkpoint_sha256 = current_checkpoint
            if qe["source"]["structure_sha256"] != run[0]["source"]["structure_sha256"]:
                raise ValueError("QE comparison was declared against another input structure")
            runs[model] = run
            ranking = run[2]["pairs"]
            material_result["models"][model] = {
                "status": "complete", "phonon_dataset_sha256": _sha(paths[0]),
                "mode_pairs_sha256": _sha(paths[1]), "ranking_sha256": _sha(paths[2]),
                "source_structure_sha256": run[0]["source"]["structure_sha256"],
                "mattersim_checkpoint_sha256": current_checkpoint,
                "frequency_vs_archived_qe": _frequency_metrics(qe, run[0]),
                "stage2": {"pair_count": len(ranking),
                           "fit_rank_13_count": sum(r["fit_design_rank"] == 13 for r in ranking),
                           "quality_flag_count": sum(bool(r["quality_flags"]) for r in ranking),
                           "sum_pair_seconds": sum(r["elapsed_sec"] for r in ranking),
                           "target_frequency_fit_vs_stage1_median_abs_difference_thz": float(np.median([
                               r["target_freq_abs_err_thz"] for r in ranking
                               if r["target_freq_abs_err_thz"] is not None
                           ])),
                           "top20_target_frequency_fit_vs_stage1_median_abs_difference_thz": float(np.median([
                               r["target_freq_abs_err_thz"] for r in ranking[:20]
                               if r["target_freq_abs_err_thz"] is not None
                           ])),
                           "top_pair": ranking[0]["pair_code"],
                           "top_abs_phi122_mev": abs(ranking[0]["phi122_mev"])},
                "archived_qe_five": _legacy_five(root, material, run),
            }
        for model, run in runs.items():
            if model != "prophet":
                material_result["vs_prophet"][model] = _physical_channels(
                    runs["prophet"], run,
                    degeneracy_thz=physical_channel_degeneracy_thz)
        for model in MODELS:
            paths = _paths(root, material, model, "model_relaxed")
            if not all(path.is_file() for path in paths):
                partial = {
                    "status": "incomplete",
                    "missing": [str(path) for path in paths if not path.is_file()],
                }
                if paths[0].is_file() and paths[1].is_file():
                    dataset, pairs = _read(paths[0]), _read(paths[1])
                    _validate_stage1_source(dataset["source"], model)
                    if (len(dataset["q_points"]) != 36 or len(pairs["pairs"]) != 486
                            or dataset["source"]["structure_sha256"] != pairs["source"]["structure_sha256"]
                            or dataset["source"]["geometry_source"] != "model_relaxed"):
                        raise ValueError(f"Invalid relaxed Stage1: {paths[0]}")
                    partial["stage1_structure_sha256"] = dataset["source"]["structure_sha256"]
                    partial["stage1_dataset_sha256"] = _sha(paths[0])
                    shared_path = _paths(root, material, model, "shared_dft")[0]
                    if shared_path.is_file():
                        comparison = compare_meshes(_read(shared_path), dataset,
                                                    allow_different_structures=True)
                        partial["phonons_vs_same_model_shared_structure"] = {
                            "frequency_mae_thz": comparison["matched_frequency_mae_thz"],
                            "isolated_mode_median_overlap_squared": comparison[
                                "isolated_mode_median_overlap_squared"],
                            "comparison_status": comparison["comparison_status"],
                        }
                material_result["own_geometry"][model] = partial
                continue
            dataset, pairs, ranking = _validated_run(*paths)
            _validate_stage1_source(dataset["source"], model)
            own_runs[model] = (dataset, pairs, ranking)
            comparison = (compare_meshes(runs[model][0], dataset,
                                         allow_different_structures=True)
                          if model in runs else None)
            material_result["own_geometry"][model] = {
                "status": "complete", "structure_sha256": dataset["source"]["structure_sha256"],
                "dataset_sha256": _sha(paths[0]), "pairs_sha256": _sha(paths[1]),
                "ranking_sha256": _sha(paths[2]), "pair_count": len(ranking["pairs"]),
                "top_pair": ranking["pairs"][0]["pair_code"],
                "top_abs_phi122_mev": abs(ranking["pairs"][0]["phi122_mev"]),
                "phonons_vs_same_model_shared_structure": {
                    "frequency_mae_thz": comparison["matched_frequency_mae_thz"],
                    "isolated_mode_median_overlap_squared": comparison[
                        "isolated_mode_median_overlap_squared"],
                    "comparison_status": comparison["comparison_status"],
                } if comparison is not None else None,
            }
        for model, run in own_runs.items():
            if model != "prophet":
                material_result["own_vs_prophet"][model] = _physical_channels(
                    own_runs["prophet"], run, allow_different_structures=True,
                    degeneracy_thz=physical_channel_degeneracy_thz
                )
        result["materials"][material] = material_result
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--physical-channel-degeneracy-thz", type=float, default=0.1)
    args = parser.parse_args()
    report = analyze(args.data_root,
                     physical_channel_degeneracy_thz=args.physical_channel_degeneracy_thz)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
