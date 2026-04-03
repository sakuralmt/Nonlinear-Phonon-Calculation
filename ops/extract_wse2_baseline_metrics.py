#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np


def parse_args():
    root = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser(description="Extract WSe2 baseline comparison tables for offline analysis.")
    p.add_argument("--repo-root", default=str(root))
    p.add_argument(
        "--baseline-root",
        default="/Users/lmtsakura/.codex/worktrees/0df5/qiyan/remote_baselines/wse2_stage3_run",
    )
    p.add_argument(
        "--gptff-v2-ranking",
        default="/Users/lmtsakura/.codex/worktrees/0df5/qiyan/local_runs/wse2/remote_stage1_rescreen/gptff_v2_remote_stage1/screening/pair_ranking.json",
    )
    p.add_argument(
        "--gptff-v1-ranking",
        default="/Users/lmtsakura/.codex/worktrees/0df5/qiyan/local_runs/wse2/remote_stage1_rescreen/gptff_v1_remote_stage1/screening/pair_ranking.json",
    )
    p.add_argument(
        "--chgnet-ranking",
        default="/Users/lmtsakura/.codex/worktrees/0df5/qiyan/remote_baselines/wse2_stage3_run/stage2_outputs/chgnet/screening/pair_ranking.csv",
    )
    p.add_argument(
        "--qe-ranking",
        default="/Users/lmtsakura/.codex/worktrees/0df5/qiyan/remote_baselines/wse2_stage3_run/stage3_qe/chgnet/results/qe_ranking.json",
    )
    p.add_argument("--out-dir", required=True)
    return p.parse_args()


def load_json_rows(path: Path, key: str):
    return json.loads(path.read_text())[key]


def load_csv_rows(path: Path):
    with path.open() as handle:
        return list(csv.DictReader(handle))


def short_label(pair_code: str) -> str:
    return (
        pair_code.replace("Gamma_p0_m8__", "")
        .replace("_q_0.500_0.000_0.000", "")
        .replace("_q_0.333_0.333_0.000", "")
        .replace("_q_0.333_0.667_0.000", "")
    )


def phi_value(row: dict, method: str) -> float:
    if method == "QE stage3":
        return float(row["qe_phi122_mev"])
    return float(row["phi122_mev"])


def rmse_value(row: dict, method: str) -> float:
    if method == "QE stage3":
        return float(row["qe_rmse_ev_supercell"])
    return float(row["rmse_ev_supercell"])


def rank_value(method_rows: dict[str, dict[str, dict]], ordered_codes: list[str], method: str, pair_code: str) -> int:
    rows = [method_rows[method][code] for code in ordered_codes]
    for idx, row in enumerate(rows, start=1):
        if row["pair_code"] == pair_code:
            return idx
    raise KeyError(pair_code)


def aggregate_error_stats(method_rows: dict[str, dict[str, dict]], ordered_codes: list[str]):
    qe_map = method_rows["QE stage3"]
    methods = ["GPTFF v2", "GPTFF v1", "CHGNet stage2"]
    out = []
    for method in methods:
        errs = np.asarray(
            [phi_value(method_rows[method][code], method) - phi_value(qe_map[code], "QE stage3") for code in ordered_codes],
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
    v2 = load_json_rows(Path(args.gptff_v2_ranking), "pairs")
    v1 = load_json_rows(Path(args.gptff_v1_ranking), "pairs")
    ch = load_csv_rows(Path(args.chgnet_ranking))
    qe = load_json_rows(Path(args.qe_ranking), "rows")
    method_rows = {
        "GPTFF v2": {row["pair_code"]: row for row in v2},
        "GPTFF v1": {row["pair_code"]: row for row in v1},
        "CHGNet stage2": {row["pair_code"]: row for row in ch},
        "QE stage3": {row["pair_code"]: row for row in qe},
    }
    ordered_codes = [row["pair_code"] for row in qe]
    pair_rows = []
    for code in ordered_codes:
        row = {
            "pair_code": code,
            "pair_label": short_label(code),
            "rank_gptff_v2": rank_value(method_rows, ordered_codes, "GPTFF v2", code),
            "rank_gptff_v1": rank_value(method_rows, ordered_codes, "GPTFF v1", code),
            "rank_chgnet_stage2": rank_value(method_rows, ordered_codes, "CHGNet stage2", code),
            "rank_qe_stage3": rank_value(method_rows, ordered_codes, "QE stage3", code),
            "phi122_gptff_v2_mev": phi_value(method_rows["GPTFF v2"][code], "GPTFF v2"),
            "phi122_gptff_v1_mev": phi_value(method_rows["GPTFF v1"][code], "GPTFF v1"),
            "phi122_chgnet_stage2_mev": phi_value(method_rows["CHGNet stage2"][code], "CHGNet stage2"),
            "phi122_qe_stage3_mev": phi_value(method_rows["QE stage3"][code], "QE stage3"),
            "rmse_gptff_v2_ev_supercell": rmse_value(method_rows["GPTFF v2"][code], "GPTFF v2"),
            "rmse_gptff_v1_ev_supercell": rmse_value(method_rows["GPTFF v1"][code], "GPTFF v1"),
            "rmse_chgnet_stage2_ev_supercell": rmse_value(method_rows["CHGNet stage2"][code], "CHGNet stage2"),
            "rmse_qe_stage3_ev_supercell": rmse_value(method_rows["QE stage3"][code], "QE stage3"),
        }
        row["delta_phi122_gptff_v2_minus_qe_mev"] = row["phi122_gptff_v2_mev"] - row["phi122_qe_stage3_mev"]
        row["delta_phi122_gptff_v1_minus_qe_mev"] = row["phi122_gptff_v1_mev"] - row["phi122_qe_stage3_mev"]
        row["delta_phi122_chgnet_stage2_minus_qe_mev"] = row["phi122_chgnet_stage2_mev"] - row["phi122_qe_stage3_mev"]
        pair_rows.append(row)
    return {
        "metadata": {
            "repo_root": args.repo_root,
            "baseline_root": args.baseline_root,
            "sources": {
                "gptff_v2": args.gptff_v2_ranking,
                "gptff_v1": args.gptff_v1_ranking,
                "chgnet_stage2": args.chgnet_ranking,
                "qe_stage3": args.qe_ranking,
            },
            "reference_method": "QE stage3",
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
    lines = [
        "# WSe2 Baseline Comparison Data",
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
            "| Method | MAE (meV) | RMSE (meV) | MaxAE (meV) |",
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
    json_path = out_dir / "wse2_remote_stage1_comparison_data.json"
    csv_path = out_dir / "wse2_remote_stage1_comparison_data.csv"
    md_path = out_dir / "wse2_remote_stage1_comparison_data.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    write_csv(csv_path, payload["pair_rows"])
    write_markdown(md_path, payload)
    print(json_path)
    print(csv_path)
    print(md_path)


if __name__ == "__main__":
    main()
