#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np

PHI_UNIT = "meV/(Å amu)^(3/2)"
DEFAULT_RESULT_ROOT = Path("/Users/lmtsakura/qiyan_shared/result")
SUPPORTED_MATERIALS = ("mos2", "mose2", "ws2")


def parse_args():
    root = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser(
        description="Extract stage2 multi-model vs QE comparison tables for a TMD material."
    )
    p.add_argument("--repo-root", default=str(root))
    p.add_argument("--material", required=True, choices=SUPPORTED_MATERIALS)
    p.add_argument("--result-root", default=str(DEFAULT_RESULT_ROOT))
    p.add_argument("--top-n", type=int, default=5)
    p.add_argument("--out-dir", required=True)
    return p.parse_args()


def read_csv_rows(path: Path):
    with path.open() as handle:
        return list(csv.DictReader(handle))


def read_json_pairs(path: Path):
    payload = json.loads(path.read_text())
    if isinstance(payload, dict) and "pairs" in payload:
        return payload["pairs"]
    if isinstance(payload, dict) and "rows" in payload:
        return payload["rows"]
    if isinstance(payload, list):
        return payload
    raise ValueError(f"Unsupported JSON payload at {path}")


def short_label(pair_code: str) -> str:
    return (
        pair_code.replace("Gamma_p0_m8__", "")
        .replace("_q_0.500_0.000_0.000", "")
        .replace("_q_0.333_0.333_0.000", "")
        .replace("_q_0.333_0.667_0.000", "")
    )


def explicit_rank_map(rows: list[dict]) -> dict[str, int]:
    out = {}
    for idx, row in enumerate(rows, start=1):
        out[row["pair_code"]] = int(row.get("rank", idx))
    return out


def phi_value(row: dict, is_qe: bool) -> float:
    return float(row["qe_phi122_mev"] if is_qe else row["phi122_mev"])


def rmse_value(row: dict, is_qe: bool) -> float:
    return float(row["qe_rmse_ev_supercell"] if is_qe else row["rmse_ev_supercell"])


def aggregate_error_stats(method_rows: dict[str, dict[str, dict]], ordered_codes: list[str]):
    qe_map = method_rows["QE stage3"]
    methods = ["GPTFF v2", "GPTFF v1", "CHGNet stage2"]
    out = []
    for method in methods:
        errs = np.asarray(
            [phi_value(method_rows[method][code], False) - phi_value(qe_map[code], True) for code in ordered_codes],
            dtype=float,
        )
        out.append(
            {
                "method": method,
                "phi122_mae_mev": float(np.mean(np.abs(errs))),
                "phi122_rmse_mev": float(np.sqrt(np.mean(errs**2))),
                "phi122_maxae_mev": float(np.max(np.abs(errs))),
            }
        )
    return out


def build_payload(args) -> dict:
    package_root = (
        Path(args.result_root).expanduser().resolve()
        / args.material
        / f"{args.material}_stage2_stage3_core_20260402"
    )
    qe_rows = read_csv_rows(
        package_root
        / "baseline_local"
        / f"{args.material}_gptff_v1_stage3_run"
        / "stage3_qe"
        / "gptff"
        / "results"
        / "qe_ranking.csv"
    )
    qe_rows = qe_rows[: args.top_n]
    ordered_codes = [row["pair_code"] for row in qe_rows]

    v2_rows = read_json_pairs(
        package_root / "stage2_models" / "gptff_v2" / "stage2_outputs" / "gptff" / "screening" / "pair_ranking.json"
    )
    v1_rows = read_json_pairs(
        package_root / "stage2_models" / "gptff_v1" / "stage2_outputs" / "gptff" / "screening" / "pair_ranking.json"
    )
    chg_rows = read_json_pairs(
        package_root / "stage2_models" / "chgnet" / "stage2_outputs" / "chgnet" / "screening" / "pair_ranking.json"
    )

    method_rows = {
        "GPTFF v2": {row["pair_code"]: row for row in v2_rows},
        "GPTFF v1": {row["pair_code"]: row for row in v1_rows},
        "CHGNet stage2": {row["pair_code"]: row for row in chg_rows},
        "QE stage3": {row["pair_code"]: row for row in qe_rows},
    }
    method_ranks = {
        "GPTFF v2": explicit_rank_map(v2_rows),
        "GPTFF v1": explicit_rank_map(v1_rows),
        "CHGNet stage2": explicit_rank_map(chg_rows),
        "QE stage3": explicit_rank_map(qe_rows),
    }

    missing = {
        method: [code for code in ordered_codes if code not in rows]
        for method, rows in method_rows.items()
        if method != "QE stage3"
    }
    missing = {k: v for k, v in missing.items() if v}
    if missing:
        raise KeyError(f"Missing pair codes in stage2 results: {missing}")

    pair_rows = []
    for code in ordered_codes:
        row = {
            "pair_code": code,
            "pair_label": short_label(code),
            "rank_gptff_v2": method_ranks["GPTFF v2"][code],
            "rank_gptff_v1": method_ranks["GPTFF v1"][code],
            "rank_chgnet_stage2": method_ranks["CHGNet stage2"][code],
            "rank_qe_stage3": method_ranks["QE stage3"][code],
            "phi122_gptff_v2_mev": phi_value(method_rows["GPTFF v2"][code], False),
            "phi122_gptff_v1_mev": phi_value(method_rows["GPTFF v1"][code], False),
            "phi122_chgnet_stage2_mev": phi_value(method_rows["CHGNet stage2"][code], False),
            "phi122_qe_stage3_mev": phi_value(method_rows["QE stage3"][code], True),
            "rmse_gptff_v2_ev_supercell": rmse_value(method_rows["GPTFF v2"][code], False),
            "rmse_gptff_v1_ev_supercell": rmse_value(method_rows["GPTFF v1"][code], False),
            "rmse_chgnet_stage2_ev_supercell": rmse_value(method_rows["CHGNet stage2"][code], False),
            "rmse_qe_stage3_ev_supercell": rmse_value(method_rows["QE stage3"][code], True),
        }
        row["delta_phi122_gptff_v2_minus_qe_mev"] = row["phi122_gptff_v2_mev"] - row["phi122_qe_stage3_mev"]
        row["delta_phi122_gptff_v1_minus_qe_mev"] = row["phi122_gptff_v1_mev"] - row["phi122_qe_stage3_mev"]
        row["delta_phi122_chgnet_stage2_minus_qe_mev"] = row["phi122_chgnet_stage2_mev"] - row["phi122_qe_stage3_mev"]
        pair_rows.append(row)

    return {
        "metadata": {
            "repo_root": args.repo_root,
            "material": args.material,
            "package_root": str(package_root),
            "sources": {
                "gptff_v2": str(
                    package_root
                    / "stage2_models"
                    / "gptff_v2"
                    / "stage2_outputs"
                    / "gptff"
                    / "screening"
                    / "pair_ranking.json"
                ),
                "gptff_v1": str(
                    package_root
                    / "stage2_models"
                    / "gptff_v1"
                    / "stage2_outputs"
                    / "gptff"
                    / "screening"
                    / "pair_ranking.json"
                ),
                "chgnet_stage2": str(
                    package_root
                    / "stage2_models"
                    / "chgnet"
                    / "stage2_outputs"
                    / "chgnet"
                    / "screening"
                    / "pair_ranking.json"
                ),
                "qe_stage3": str(
                    package_root
                    / "baseline_local"
                    / f"{args.material}_gptff_v1_stage3_run"
                    / "stage3_qe"
                    / "gptff"
                    / "results"
                    / "qe_ranking.csv"
                ),
            },
            "reference_method": "QE stage3",
            "phi122_unit": PHI_UNIT,
            "top_n": args.top_n,
        },
        "pair_rows": pair_rows,
        "aggregate_error_stats": aggregate_error_stats(method_rows, ordered_codes),
    }


def write_csv(path: Path, pair_rows: list[dict]):
    fieldnames = list(pair_rows[0].keys())
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(pair_rows)


def write_markdown(path: Path, payload: dict):
    material = payload["metadata"]["material"]
    lines = [
        f"# {material.upper()} Stage2 Multi-Model Comparison Data",
        "",
        "## Pair Table",
        "",
        "| Pair | v2 Rank | v1 Rank | CHGNet Rank | QE Rank | v2 phi122 | v1 phi122 | CHGNet phi122 | QE phi122 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["pair_rows"]:
        lines.append(
            f"| `{row['pair_code']}` | {row['rank_gptff_v2']} | {row['rank_gptff_v1']} | {row['rank_chgnet_stage2']} | {row['rank_qe_stage3']} | "
            f"{row['phi122_gptff_v2_mev']:.6f} | {row['phi122_gptff_v1_mev']:.6f} | {row['phi122_chgnet_stage2_mev']:.6f} | {row['phi122_qe_stage3_mev']:.6f} |"
        )
    lines.extend(
        [
            "",
            "## Aggregate Error Stats",
            "",
            f"Reference: `QE stage3`; phi122 unit: `{PHI_UNIT}`",
            "",
            "| Method | MAE | RMSE | MaxAE |",
            "|---|---:|---:|---:|",
        ]
    )
    for row in payload["aggregate_error_stats"]:
        lines.append(
            f"| {row['method']} | {row['phi122_mae_mev']:.6f} | {row['phi122_rmse_mev']:.6f} | {row['phi122_maxae_mev']:.6f} |"
        )
    path.write_text("\n".join(lines) + "\n")


def main():
    args = parse_args()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = build_payload(args)
    stem = f"{args.material}_stage2_model_comparison_data"
    json_path = out_dir / f"{stem}.json"
    csv_path = out_dir / f"{stem}.csv"
    md_path = out_dir / f"{stem}.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    write_csv(csv_path, payload["pair_rows"])
    write_markdown(md_path, payload)
    print(json_path)
    print(csv_path)
    print(md_path)


if __name__ == "__main__":
    main()
