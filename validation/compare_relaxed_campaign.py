"""Offline comparison of complete, self-relaxed Stage1/MatterSim campaigns.

No inference or DFT submission. The fourth-order channel statistic is the trace
across a complete Gamma multiplet, not the norm of its diagonal entries.
"""

from __future__ import annotations
import argparse
import csv
import itertools
import json
from pathlib import Path
import numpy as np
from scipy.stats import spearmanr
from mlff_modepair_workflow.core import decode_complex_mode
from mlff_modepair_workflow.phonon_eigenvectors import compare_modes
from mlff_modepair_workflow.prophet_backend import sha256_file

MODELS = ("tece", "prophet", "equiformer-v3")


def matrix(record):
    return np.column_stack(
        [decode_complex_mode(v).ravel() for v in record["eigenvectors"]]
    )


def error_metrics(a, b):
    a, b = np.asarray(a), np.asarray(b)
    if not a.size:
        return {"count": 0, "mae": None, "rmse": None, "max_absolute_difference": None}
    delta = b - a
    return {
        "count": int(a.size),
        "mae": float(np.mean(abs(delta))),
        "rmse": float(np.sqrt(np.mean(delta**2))),
        "max_absolute_difference": float(np.max(abs(delta))),
    }


def load(root):
    paths = {
        "phonon": "stage1/phonon_dataset.json",
        "pairs": "stage1/mode_pairs.selected.json",
        "screen": "stage2/screen_ranking.json",
        "refine": "stage2/refine_ranking.json",
        "selection": "stage2/selection.json",
    }
    data = {k: json.loads((root / v).read_text()) for k, v in paths.items()}
    source = data["phonon"]["source"]
    if source["geometry_source"] != "model_relaxed":
        raise ValueError("This comparison requires model-relaxed production runs")
    if source["structure_sha256"] != data["screen"]["identity"]["structure_sha256"]:
        raise ValueError("Stage1 and Stage2 use different structures")
    if (
        sha256_file(root / paths["pairs"])
        != data["screen"]["identity"]["mode_pairs_sha256"]
    ):
        raise ValueError("Stage2 does not match its Stage1 mode file")
    data["hashes"] = {k: sha256_file(root / v) for k, v in paths.items()}
    data["by_q"] = {tuple(r["q_index"]): r for r in data["phonon"]["q_points"]}
    data["fit"] = {r["pair_code"]: r["analysis"] for r in data["refine"]["pairs"]}
    data["proxy"] = {
        r["pair_code"]: r["phi122_proxy_mev"] for r in data["screen"]["pairs"]
    }
    data["selected"] = {r["channel_code"] for r in data["selection"]["channels"]}
    return data


def channel_values(data, channel):
    codes = channel["pair_codes"]
    value = {
        "screen_phi122_norm": float(np.linalg.norm([data["proxy"][c] for c in codes])),
        "refined_phi122_norm": None,
        "refined_phi1122_trace": None,
    }
    if all(c in data["fit"] for c in codes):
        physics = [data["fit"][c]["physics"] for c in codes]
        value.update(
            refined_phi122_norm=float(
                np.linalg.norm([p["phi_122_mev_per_A3amu32"] for p in physics])
            ),
            refined_phi1122_trace=float(
                sum(p["phi_1122_mev_per_A4amu2"] for p in physics)
            ),
        )
    return value


def compare(left, right):
    if set(left["by_q"]) != set(right["by_q"]):
        raise ValueError("Different q meshes")
    if left["phonon"]["source"]["symbols"] != right["phonon"]["source"]["symbols"]:
        raise ValueError(
            "Atom mapping must be established before comparing different atom orders"
        )
    maps, qrows = {}, []
    for q, ref in left["by_q"].items():
        cand = right["by_q"][q]
        assignment, overlap, groups = compare_modes(
            np.asarray(ref["freqs_thz"]),
            matrix(ref),
            np.asarray(cand["freqs_thz"]),
            matrix(cand),
            degeneracy_thz=0.01,
            gamma_acoustic=(q == (0, 0)),
        )
        maps[q] = assignment, overlap
        for i, j in enumerate(assignment):
            group = next(g for g in groups if i + 1 in g["qe_modes"])
            cross = (
                matrix(ref)[:, np.asarray(group["qe_modes"]) - 1].conj().T
                @ matrix(cand)[:, np.asarray(group["ml_modes"]) - 1]
            )
            singular = np.linalg.svd(cross, compute_uv=False) ** 2
            qrows.append(
                {
                    "qx": q[0] / 6,
                    "qy": q[1] / 6,
                    "reference_mode": i + 1,
                    "candidate_mode": int(j) + 1,
                    "reference_frequency_thz": ref["freqs_thz"][i],
                    "candidate_frequency_thz": cand["freqs_thz"][j],
                    "single_overlap_squared": float(overlap[i]),
                    "subspace_dimension": len(group["qe_modes"]),
                    "subspace_minimum_overlap_squared": float(min(singular)),
                    "basis_dependent": len(group["qe_modes"]) > 1,
                }
            )
    refchannels = left["pairs"]["equivalent_pair_channels"]["channels"]
    candidates = {
        (
            tuple(c["representative_q_index"]),
            tuple(c["gamma_modes_one_based"]),
            c["target_mode_number_one_based"],
        ): c
        for c in right["pairs"]["equivalent_pair_channels"]["channels"]
    }
    ga, gb = matrix(left["by_q"][(0, 0)]), matrix(right["by_q"][(0, 0)])
    rows, excluded = [], []
    for channel in refchannels:
        q = tuple(channel["representative_q_index"])
        gamma = np.asarray(channel["gamma_modes_one_based"]) - 1
        target = channel["target_mode_number_one_based"] - 1
        mapped_gamma = sorted(int(maps[(0, 0)][0][g]) + 1 for g in gamma)
        mapped_target = int(maps[q][0][target]) + 1
        candidate = candidates.get((q, tuple(mapped_gamma), mapped_target))
        reason = None
        if candidate is None:
            reason = "Gamma multiplets cannot be matched completely"
        elif (
            channel["basis_dependent_target_branch"]
            or candidate["basis_dependent_target_branch"]
        ):
            reason = "finite-q degenerate branch"
        singular = (
            np.linalg.svd(
                ga[:, gamma].conj().T @ gb[:, np.asarray(mapped_gamma) - 1],
                compute_uv=False,
            )
            ** 2
        )
        if reason is None and min(singular) < 0.9:
            reason = "Gamma subspace overlap below 0.9"
        if reason is None and maps[q][1][target] < 0.9:
            reason = "finite-q isolated overlap below 0.9"
        if reason:
            excluded.append(
                {"reference_channel": channel["channel_code"], "reason": reason}
            )
            continue
        lval, rval = channel_values(left, channel), channel_values(right, candidate)
        rows.append(
            {
                "reference_channel": channel["channel_code"],
                "candidate_channel": candidate["channel_code"],
                "qx": q[0] / 6,
                "qy": q[1] / 6,
                "gamma_dimension": len(gamma),
                "target_overlap_squared": float(maps[q][1][target]),
                "gamma_minimum_overlap_squared": float(min(singular)),
                "reference_selected": channel["channel_code"] in left["selected"],
                "candidate_selected": candidate["channel_code"] in right["selected"],
                **{f"reference_{k}": v for k, v in lval.items()},
                **{f"candidate_{k}": v for k, v in rval.items()},
            }
        )
    metrics = {}
    for name in ["screen_phi122_norm", "refined_phi122_norm", "refined_phi1122_trace"]:
        subset = [
            r
            for r in rows
            if r["reference_" + name] is not None and r["candidate_" + name] is not None
        ]
        metrics[name] = error_metrics(
            [r["reference_" + name] for r in subset],
            [r["candidate_" + name] for r in subset],
        )
    metrics["frequency_thz"] = error_metrics(
        [r["reference_frequency_thz"] for r in qrows],
        [r["candidate_frequency_thz"] for r in qrows],
    )
    metrics["screen_spearman"] = float(
        spearmanr(
            [r["reference_screen_phi122_norm"] for r in rows],
            [r["candidate_screen_phi122_norm"] for r in rows],
        ).statistic
    )
    return {
        "metrics": metrics,
        "channels": rows,
        "excluded_channels": excluded,
        "phonon_modes": qrows,
        "same_structure": False,
        "reference_hashes": left["hashes"],
        "candidate_hashes": right["hashes"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    results = {}
    for material in ["mose2", "ws2"]:
        loaded = {model: load(args.root / model / material) for model in MODELS}
        for a, b in itertools.combinations(MODELS, 2):
            key = f"{material}_{a}_vs_{b}"
            r = compare(loaded[a], loaded[b])
            results[key] = r
            for field in ["channels", "phonon_modes", "excluded_channels"]:
                with (args.output / f"{key}_{field}.csv").open(
                    "w", newline=""
                ) as stream:
                    if r[field]:
                        writer = csv.DictWriter(
                            stream, fieldnames=list(r[field][0]), lineterminator="\n"
                        )
                        writer.writeheader()
                        writer.writerows(r[field])
            print(key, json.dumps(r["metrics"]))
    (args.output / "matched_comparisons.json").write_text(
        json.dumps(results, indent=2, allow_nan=False) + "\n"
    )


if __name__ == "__main__":
    main()
