#!/usr/bin/env python3
"""Replay low-cost coupling screens from completed 9x9 MatterSim energy grids."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

from mlff_modepair_workflow.core import polynomial_design
from mlff_modepair_workflow.prophet_stage1 import equivalent_pair_channels
from scripts.analyze_phonopy_model_campaign import MODELS, _paths, _sha, _validated_run


AXIS = np.arange(-2.0, 2.01, 0.5)
FIT_WINDOWS = {}
for _window in (1.0, 1.5, 2.0):
    _design, _mask = polynomial_design(AXIS, AXIS, fit_window=_window)
    FIT_WINDOWS[_window] = (np.linalg.pinv(_design), _mask, int(np.linalg.matrix_rank(_design)))
STEPS = (0.5, 1.0, 1.5, 2.0)
RANK_CUTOFFS = (5, 10, 20, 30)
RECALL_CUTOFFS = (20, 25, 30, 40, 50, 60)


def _grid_index(value: float) -> int:
    index = int(round((value + 2.0) / 0.5))
    if abs(AXIS[index] - value) > 1e-12:
        raise ValueError(f"Coordinate {value} is absent from the archived grid")
    return index


def _stencil122(grid: np.ndarray, step: float) -> float:
    """Six-energy central difference for d^3 E / dx dy^2, in meV."""
    xplus, xminus = _grid_index(step), _grid_index(-step)
    yplus, yminus, zero = _grid_index(step), _grid_index(-step), _grid_index(0)
    curvature_plus = (grid[yplus, xplus] - 2 * grid[zero, xplus]
                      + grid[yminus, xplus]) / step**2
    curvature_minus = (grid[yplus, xminus] - 2 * grid[zero, xminus]
                       + grid[yminus, xminus]) / step**2
    return float(1000 * (curvature_plus - curvature_minus) / (2 * step))


def _stencil1122(grid: np.ndarray, step: float) -> float:
    """Nine-energy central difference for d^4 E / dx^2 dy^2, in meV."""
    plus, minus, zero = _grid_index(step), _grid_index(-step), _grid_index(0)
    curvature = lambda row: (grid[row, plus] - 2 * grid[row, zero]
                             + grid[row, minus]) / step**2
    return float(1000 * (curvature(plus) - 2 * curvature(zero) + curvature(minus)) / step**2)


def _fit_window(grid: np.ndarray, window: float) -> tuple[float, float]:
    inverse, mask, rank = FIT_WINDOWS[window]
    if rank != 13:
        raise AssertionError(f"The {window:g} Å√amu fit is not full rank")
    params = inverse @ grid.T.reshape(-1)[mask]
    return float(2000 * params[2]), float(4000 * params[9])


def _rank(codes: list[str], values: dict[str, float]) -> list[str]:
    return sorted(codes, key=lambda code: (-abs(values[code]), code))


def _channel_rank(channels: list[dict], values: dict[str, float]) -> list[tuple[str, float, int]]:
    ranked = []
    for channel in channels:
        score = float(np.linalg.norm([values[code] for code in channel["pair_codes"]]))
        ranked.append((channel["channel_code"], score, len(channel["pair_codes"])))
    return sorted(ranked, key=lambda row: (-row[1], row[0]))


def _metrics(reference: dict[str, float], proxy: dict[str, float], channels: list[dict]) -> dict:
    codes = sorted(reference)
    ref_values = np.asarray([reference[code] for code in codes])
    proxy_values = np.asarray([proxy[code] for code in codes])
    ref_order, proxy_order = _rank(codes, reference), _rank(codes, proxy)
    ref_channels, proxy_channels = _channel_rank(channels, reference), _channel_rank(channels, proxy)
    ref_ids = [row[0] for row in ref_channels]
    proxy_ids = [row[0] for row in proxy_channels]
    ref_by_id = {row[0]: row[1] for row in ref_channels}
    proxy_by_id = {row[0]: row[1] for row in proxy_channels}
    ids = sorted(ref_by_id)
    if set(ids) != set(proxy_by_id):
        raise ValueError("Proxy and reference physical channels differ")
    channel_error = np.asarray([proxy_by_id[code] - ref_by_id[code] for code in ids])
    by_channel = {channel["channel_code"]: channel for channel in channels}
    return {
        "pair_phi122_mae_mev": float(np.mean(np.abs(proxy_values - ref_values))),
        "pair_phi122_rmse_mev": float(np.sqrt(np.mean((proxy_values - ref_values)**2))),
        "pair_abs_phi122_spearman": float(spearmanr(abs(ref_values), abs(proxy_values)).statistic),
        "pair_top_k_overlap": {str(k): len(set(ref_order[:k]) & set(proxy_order[:k]))
                               for k in RANK_CUTOFFS},
        "channel_count": len(channels),
        "channel_score_mae_mev": float(np.mean(np.abs(channel_error))),
        "channel_score_rmse_mev": float(np.sqrt(np.mean(channel_error**2))),
        "channel_score_spearman": float(spearmanr(
            [ref_by_id[code] for code in ids], [proxy_by_id[code] for code in ids]).statistic),
        "channel_top_k_overlap": {str(k): len(set(ref_ids[:k]) & set(proxy_ids[:k]))
                                  for k in RANK_CUTOFFS},
        "reference_top20_recall_by_proxy_top_n": {
            str(n): len(set(ref_ids[:20]) & set(proxy_ids[:n])) for n in RECALL_CUTOFFS
        },
        "expanded_pairs_by_proxy_top_n": {
            str(n): sum(len(by_channel[code]["pair_codes"]) for code in proxy_ids[:n])
            for n in RECALL_CUTOFFS
        },
        "missed_reference_top20_at_proxy_top30": [
            {"channel_code": code, "reference_rank": ref_ids.index(code) + 1,
             "proxy_rank": proxy_ids.index(code) + 1,
             "reference_score_mev": ref_by_id[code], "proxy_score_mev": proxy_by_id[code]}
            for code in ref_ids[:20] if code not in set(proxy_ids[:30])
        ],
    }


def analyze(validation_root: Path, grid_root: Path) -> dict:
    if any(rank != 13 for _, _, rank in FIT_WINDOWS.values()):
        raise AssertionError("A polynomial fit is not full rank")
    result = {"kind": "coarse_screening_replay", "reference": "archived_center_5x5_13_parameter_fit",
              "grid_axis": AXIS.tolist(), "gamma_grouping_threshold_thz": 0.01,
              "methods": {f"six_point_h{step:g}": {"energy_points_per_pair": 6, "step": step}
                          for step in STEPS}, "materials": {}}
    for material in ("mos2", "wse2"):
        result["materials"][material] = {}
        for geometry in ("shared_dft", "model_relaxed"):
            result["materials"][material][geometry] = {}
            for model in MODELS:
                dataset_path, pairs_path, ranking_path = _paths(validation_root, material, model, geometry)
                dataset, pair_payload, ranking = _validated_run(dataset_path, pairs_path, ranking_path)
                pair_codes = [pair["pair_code"] for pair in pair_payload["pairs"]]
                ranked = {row["pair_code"]: row for row in ranking["pairs"]}
                if len(pair_codes) != 486 or set(pair_codes) != set(ranked):
                    raise ValueError(f"Incomplete ranking: {ranking_path}")
                channels = equivalent_pair_channels(dataset["q_points"],
                                                    pair_payload["finite_q_orbits"],
                                                    pair_payload["pairs"], 3, 0.01)["channels"]
                base = (grid_root / "mattersim-phonopy-stage2" / model / material / geometry
                        / "mattersim-v1-5m" / "screening" / "pairs")
                grids = {}
                for code in pair_codes:
                    path = base / code / "energy_grid_eV.npy"
                    if not path.is_file():
                        raise FileNotFoundError(path)
                    grid = np.load(path, allow_pickle=False)
                    if grid.shape != (9, 9) or not np.isfinite(grid).all():
                        raise ValueError(f"Incomplete or nonfinite grid: {path}")
                    grids[code] = grid
                if len(list(base.glob("*/energy_grid_eV.npy"))) != 486:
                    raise ValueError(f"Unexpected extra or duplicated grids: {base}")
                reference = {}
                fourth = {}
                window_third = {1.5: {}, 2.0: {}}
                window_fourth = {1.5: {}, 2.0: {}}
                proxy = {f"six_point_h{step:g}": {} for step in STEPS}
                fourth_proxy = {f"nine_point_h{step:g}": {} for step in STEPS}
                for code, grid in grids.items():
                    reference[code], fourth[code] = _fit_window(grid, 1.0)
                    for window in window_third:
                        window_third[window][code], window_fourth[window][code] = _fit_window(grid, window)
                    for step in STEPS:
                        proxy[f"six_point_h{step:g}"][code] = _stencil122(grid, step)
                        fourth_proxy[f"nine_point_h{step:g}"][code] = _stencil1122(grid, step)
                phi_discrepancy = max(abs(reference[code] - ranked[code]["phi122_mev"])
                                      for code in pair_codes)
                fourth_discrepancy = max(abs(fourth[code] - ranked[code]["phi1122_mev_per_A4amu2"])
                                         for code in pair_codes)
                if phi_discrepancy > 1e-5 or fourth_discrepancy > 1e-4:
                    raise ValueError(f"Grid/archived fit mismatch: {ranking_path}: "
                                     f"Phi122={phi_discrepancy}, Phi1122={fourth_discrepancy}")
                result["materials"][material][geometry][model] = {
                    "status": "verified", "pair_count": 486, "grid_count": len(grids),
                    "source": {
                        "phonon_dataset_sha256": _sha(dataset_path),
                        "mode_pairs_sha256": _sha(pairs_path),
                        "pair_ranking_sha256": _sha(ranking_path),
                        "structure_sha256": dataset["source"]["structure_sha256"],
                        "mattersim_checkpoint_sha256": ranking["backend"]["checkpoint_sha256"],
                    },
                    "reference_fit_max_abs_difference_from_ranking_mev": phi_discrepancy,
                    "fourth_fit_max_abs_difference_from_ranking_mev": fourth_discrepancy,
                    "gamma_groups_0p01_thz": equivalent_pair_channels(
                        dataset["q_points"], pair_payload["finite_q_orbits"],
                        pair_payload["pairs"], 3, 0.01)["gamma_groups_one_based"],
                    "screening": {name: _metrics(reference, values, channels)
                                  for name, values in proxy.items()},
                    "window_sensitivity": {
                        f"window_{window:g}": {
                            "energy_points_per_pair": (int(2 * window / 0.5) + 1) ** 2,
                            "third_order_vs_center25": _metrics(reference, window_third[window], channels),
                            "fourth_order_mae_mev": float(np.mean([
                                abs(window_fourth[window][code] - fourth[code]) for code in pair_codes])),
                            "fourth_order_abs_spearman": float(spearmanr(
                                [abs(fourth[code]) for code in pair_codes],
                                [abs(window_fourth[window][code]) for code in pair_codes]).statistic),
                        } for window in window_third},
                    "fourth_order_nine_point": {
                        name: {"mae_mev": float(np.mean([abs(values[code] - fourth[code])
                                                         for code in pair_codes])),
                               "rmse_mev": float(np.sqrt(np.mean([
                                   (values[code] - fourth[code])**2 for code in pair_codes]))),
                               "abs_spearman": float(spearmanr(
                                   [abs(fourth[code]) for code in pair_codes],
                                   [abs(values[code]) for code in pair_codes]).statistic)}
                        for name, values in fourth_proxy.items()},
                }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validation-root", type=Path, required=True)
    parser.add_argument("--grid-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = analyze(args.validation_root, args.grid_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(args.output)


if __name__ == "__main__":
    main()
