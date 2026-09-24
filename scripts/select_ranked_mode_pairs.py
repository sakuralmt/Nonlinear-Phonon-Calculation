#!/usr/bin/env python3
"""Select top Gamma-subspace/q channels from complete Stage2 rankings for later DFT."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

from mlff_modepair_workflow.prophet_stage1 import equivalent_pair_channels, gamma_subspaces


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _gamma_groups(dataset: dict, threshold_thz: float) -> list[list[int]]:
    return gamma_subspaces(dataset["q_points"], len(dataset["source"]["symbols"]), threshold_thz)


def select(mode_pairs: dict, ranking: dict, dataset: dict, *,
           pair_sha256: str, ranking_sha256: str, top_k_channels: int,
           degeneracy_thz: float = 0.1) -> dict:
    if top_k_channels < 1 or degeneracy_thz <= 0:
        raise ValueError("Positive top-k and degeneracy threshold required")
    if (ranking["signature"]["mode_pairs_sha256"] != pair_sha256
            or ranking["signature"]["structure_sha256"] != mode_pairs["source"]["structure_sha256"]
            or dataset["source"]["structure_sha256"] != mode_pairs["source"]["structure_sha256"]):
        raise ValueError("Screening ranking, phonons and mode pairs do not share one Stage1 source")
    pairs = mode_pairs["pairs"]
    source_codes = {row["pair_code"] for row in pairs}
    ranked = {row["pair_code"]: row for row in ranking["pairs"]}
    natoms = len(dataset["source"]["symbols"])
    orbits = mode_pairs["finite_q_orbits"]
    expected = len(orbits) * (3 * natoms) ** 2
    if len(source_codes) != expected or len(pairs) != expected or len(ranked) != expected or set(ranked) != source_codes:
        raise ValueError(f"Selection requires a complete, unique {expected}-pair screening ranking")
    precomputed = mode_pairs.get("equivalent_pair_channels")
    if precomputed is not None:
        canonical = equivalent_pair_channels(
            dataset["q_points"], orbits, pairs, natoms,
            precomputed["gamma_degeneracy_threshold_thz"])
        if precomputed != canonical:
            raise ValueError("Stage1 precomputed physical channels differ from the pair basis")
    use_precomputed = (precomputed is not None
                       and degeneracy_thz == precomputed["gamma_degeneracy_threshold_thz"])
    planned = (precomputed if use_precomputed else equivalent_pair_channels(
        dataset["q_points"], orbits, pairs, natoms, degeneracy_thz))
    groups = planned["gamma_groups_one_based"]
    channels = []
    for channel in planned["channels"]:
        codes = channel["pair_codes"]
        score = math.sqrt(sum(float(ranked[code]["phi122_mev"]) ** 2 for code in codes))
        channels.append({"channel_code": channel["channel_code"],
                         "q_mode_code": channel["q_mode_code"],
                         "gamma_modes_one_based": channel["gamma_modes_one_based"],
                         "screening_phi122_norm_mev": score, "pair_codes": codes})
    channels.sort(key=lambda row: (-row["screening_phi122_norm_mev"],
                                   row["q_mode_code"], row["gamma_modes_one_based"]))
    if top_k_channels > len(channels):
        raise ValueError("top-k exceeds available physical channels")
    chosen = channels[:top_k_channels]
    ordered_codes = [code for channel in chosen for code in channel["pair_codes"]]
    by_code = {row["pair_code"]: row for row in pairs}
    return {
        **{key: value for key, value in mode_pairs.items() if key not in {"pairs", "equivalent_pair_channels"}},
        "selection": "ranked_gamma_subspace_channels",
        "selection_meta": {
            "screening_backend": ranking["backend"]["backend"],
            "screening_ranking_sha256": ranking_sha256,
            "source_mode_pairs_sha256": pair_sha256,
            "score": "Euclidean norm of Phi122 over each near-degenerate Gamma subspace",
            "gamma_degeneracy_threshold_thz": degeneracy_thz,
            "gamma_groups_one_based": groups,
            "available_physical_channels": planned["channel_count"],
            "stage1_channels_precomputed": use_precomputed,
            "top_k_physical_channels": top_k_channels,
            "selected_pair_count": len(ordered_codes),
            "selected_channels": chosen,
        },
        "pairs": [by_code[code] for code in ordered_codes],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode-pairs-json", type=Path, required=True)
    parser.add_argument("--screening-ranking", type=Path, required=True)
    parser.add_argument("--phonon-dataset", type=Path, required=True)
    parser.add_argument("--top-k-channels", type=int, default=20)
    parser.add_argument("--gamma-degeneracy-thz", type=float, default=0.1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    pair_file, rank_file, dataset_file = args.mode_pairs_json, args.screening_ranking, args.phonon_dataset
    result = select(json.loads(pair_file.read_text()), json.loads(rank_file.read_text()),
                    json.loads(dataset_file.read_text()), pair_sha256=_sha(pair_file),
                    ranking_sha256=_sha(rank_file), top_k_channels=args.top_k_channels,
                    degeneracy_thz=args.gamma_degeneracy_thz)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(result, indent=2, allow_nan=False) + "\n"
    if args.output.exists() and args.output.read_text() != content:
        raise ValueError("Refusing to replace a different candidate selection")
    args.output.write_text(content)
    print(json.dumps({"output": str(args.output),
                      "physical_channels": args.top_k_channels,
                      "mode_pairs": len(result["pairs"])}))


if __name__ == "__main__":
    main()
