#!/usr/bin/env python3
"""Audit whether a Prophet coupling ranking retains MatterSim's top candidates."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from scipy.stats import spearmanr

from scripts.select_ranked_mode_pairs import _gamma_groups


def compare(prophet: dict, mattersim: dict, dataset: dict | None = None) -> dict:
    for key in ("mode_pairs_sha256", "structure_sha256", "geometry_source",
                "normalization_version", "a1_values", "a2_values", "fit_window"):
        if prophet["signature"][key] != mattersim["signature"][key]:
            raise ValueError(f"Different mode basis or fit protocol: {key}")
    if len(prophet["pairs"]) != 486 or len(mattersim["pairs"]) != 486:
        raise ValueError("Final screening comparison requires both complete 486-pair rankings")
    p = {row["pair_code"]: row for row in prophet["pairs"]}
    m = {row["pair_code"]: row for row in mattersim["pairs"]}
    if len(p) != 486 or len(m) != 486 or set(p) != set(m):
        raise ValueError("Rankings have duplicate or unmatched pairs")
    codes = sorted(p)
    p_values = np.asarray([abs(p[c]["phi122_mev"]) for c in codes])
    m_values = np.asarray([abs(m[c]["phi122_mev"]) for c in codes])
    p_order = sorted(codes, key=lambda c: (-abs(p[c]["phi122_mev"]), c))
    m_order = sorted(codes, key=lambda c: (-abs(m[c]["phi122_mev"]), c))
    p_rank = {code: i + 1 for i, code in enumerate(p_order)}
    m_rank = {code: i + 1 for i, code in enumerate(m_order)}
    top_overlap = {str(k): len(set(p_order[:k]) & set(m_order[:k])) for k in (5, 10, 20, 30, 50)}
    top20_recall = {str(k): len(set(p_order[:k]) & set(m_order[:20])) / 20 for k in (20, 30, 50, 100)}
    result = {
        "kind": "prophet_screening_vs_mattersim_ranking_audit",
        "material_structure_sha256": prophet["signature"]["structure_sha256"],
        "mode_pairs_sha256": prophet["signature"]["mode_pairs_sha256"],
        "pair_count": 486,
        "abs_phi122_spearman": float(spearmanr(p_values, m_values).statistic),
        "top_k_overlap": top_overlap,
        "mattersim_top20_recall_by_prophet_top_k": top20_recall,
        "prophet_sum_pair_seconds": float(sum(row["elapsed_sec"] for row in p.values())),
        "mattersim_sum_pair_seconds": float(sum(row["elapsed_sec"] for row in m.values())),
        "mattersim_top20_in_prophet_order": [
            {"pair_code": code, "mattersim_rank": m_rank[code], "prophet_rank": p_rank[code],
             "mattersim_abs_phi122_mev": abs(m[code]["phi122_mev"]),
             "prophet_abs_phi122_mev": abs(p[code]["phi122_mev"])}
            for code in m_order[:20]
        ],
        "note": "MatterSim here is an alternative MLFF ranking, not DFT ground truth. "
                "Scores use the same v3 modes, structure, 9x9 grid and central 5x5 fit.",
    }
    if dataset is not None:
        if dataset["source"]["structure_sha256"] != prophet["signature"]["structure_sha256"]:
            raise ValueError("Phonon dataset does not use the screened structure")
        groups = _gamma_groups(dataset, 0.1)
        targets = sorted({code.split("__", 1)[1] for code in codes})
        if len(targets) != 54:
            raise ValueError("Expected 54 target q modes")
        def channel_scores(rows):
            scores = {}
            for target in targets:
                for group in groups:
                    key = (tuple(group), target)
                    scores[key] = float(np.linalg.norm([
                        rows[f"Gamma_p0_m{number}__{target}"]["phi122_mev"] for number in group
                    ]))
            return scores
        p_channels = channel_scores(p)
        m_channels = channel_scores(m)
        p_channel_order = sorted(p_channels, key=lambda key: (-p_channels[key], key))
        m_channel_order = sorted(m_channels, key=lambda key: (-m_channels[key], key))
        result["equivalence_aware_channels"] = {
            "count": len(p_channels), "gamma_groups_one_based": groups,
            "score": "norm of Phi_iqq over each Gamma near-degenerate subspace",
            "spearman": float(spearmanr([p_channels[key] for key in sorted(p_channels)],
                                       [m_channels[key] for key in sorted(m_channels)]).statistic),
            "top_k_overlap": {str(k): len(set(p_channel_order[:k]) & set(m_channel_order[:k]))
                              for k in (5, 10, 20, 30, 50)},
            "mattersim_top20_recall_by_prophet_top_k": {
                str(k): len(set(p_channel_order[:k]) & set(m_channel_order[:20])) / 20
                for k in (20, 30, 50, 100)
            },
            "prophet_top20_expanded_pair_count": sum(len(key[0]) for key in p_channel_order[:20]),
            "mattersim_top20_in_prophet_order": [
                {"gamma_modes_one_based": list(key[0]), "q_mode_code": key[1],
                 "mattersim_rank": rank, "prophet_rank": p_channel_order.index(key) + 1,
                 "mattersim_score": m_channels[key], "prophet_score": p_channels[key]}
                for rank, key in enumerate(m_channel_order[:20], start=1)
            ],
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prophet-ranking", type=Path, required=True)
    parser.add_argument("--mattersim-ranking", type=Path, required=True)
    parser.add_argument("--phonon-dataset", type=Path, help="Add Gamma-subspace-invariant channel ranking")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compare(json.loads(args.prophet_ranking.read_text()),
                     json.loads(args.mattersim_ranking.read_text()),
                     json.loads(args.phonon_dataset.read_text()) if args.phonon_dataset else None)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
