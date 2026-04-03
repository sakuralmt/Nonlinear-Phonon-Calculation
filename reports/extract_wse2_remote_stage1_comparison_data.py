#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "reports" / "data"


def _default_paths():
    baseline_root = ROOT / "remote_baselines" / "wse2_stage3_run"
    return {
        "gptff_v2": ROOT / "local_runs" / "wse2" / "remote_stage1_rescreen" / "gptff_v2_remote_stage1" / "screening" / "pair_ranking.json",
        "gptff_v1": ROOT / "local_runs" / "wse2" / "remote_stage1_rescreen" / "gptff_v1_remote_stage1" / "screening" / "pair_ranking.json",
        "mattersim_v1_5m": ROOT / "local_runs" / "wse2" / "mattersim_v1_5m_stage2_test" / "stage2" / "outputs" / "mattersim_v1_5m" / "screening" / "pair_ranking.json",
        "chgnet_stage2": baseline_root / "stage2_outputs" / "chgnet" / "screening" / "pair_ranking.csv",
        "qe_stage3": baseline_root / "stage3_qe" / "chgnet" / "results" / "qe_ranking.json",
    }


def parse_args():
    defaults = _default_paths()
    parser = argparse.ArgumentParser(description="Extract WSe2 stage2/stage3 comparison data into JSON/CSV/Markdown tables.")
    parser.add_argument("--gptff-v2", type=str, default=str(defaults["gptff_v2"]))
    parser.add_argument("--gptff-v1", type=str, default=str(defaults["gptff_v1"]))
    parser.add_argument("--mattersim", type=str, default=str(defaults["mattersim_v1_5m"]))
    parser.add_argument("--chgnet", type=str, default=str(defaults["chgnet_stage2"]))
    parser.add_argument("--qe", type=str, default=str(defaults["qe_stage3"]))
    return parser.parse_args()


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


def _method_sources(args):
    methods = {}
    ordered_codes = None

    def add_json_method(name: str, path_text: str, key: str = "pairs"):
        nonlocal ordered_codes
        path = Path(path_text).expanduser().resolve()
        if not path.exists():
            return None
        rows = load_json_rows(path, key)
        methods[name] = {row["pair_code"]: row for row in rows}
        if name == "QE stage3":
            ordered_codes = [row["pair_code"] for row in rows]
        return path

    def add_csv_method(name: str, path_text: str):
        path = Path(path_text).expanduser().resolve()
        if not path.exists():
            return None
        rows = load_csv_rows(path)
        methods[name] = {row["pair_code"]: row for row in rows}
        return path

    source_paths = {
        "gptff_v2": add_json_method("GPTFF v2", args.gptff_v2),
        "gptff_v1": add_json_method("GPTFF v1", args.gptff_v1),
        "mattersim_v1_5m": add_json_method("MatterSim v1 5M", args.mattersim),
        "chgnet_stage2": add_csv_method("CHGNet stage2", args.chgnet),
        "qe_stage3": add_json_method("QE stage3", args.qe, key="rows"),
    }
    if ordered_codes is None:
        raise FileNotFoundError("QE stage3 ranking JSON is required to build the comparison table.")
    return methods, ordered_codes, source_paths


def phi_value(row: dict, method: str) -> float:
    if method == "QE stage3":
        return float(row["qe_phi122_mev"])
    return float(row["phi122_mev"])


def rmse_value(row: dict, method: str) -> float:
    if method == "QE stage3":
        return float(row["qe_rmse_ev_supercell"])
    return float(row["rmse_ev_supercell"])


def rank_value(method_rows: dict[str, dict[str, dict]], ordered_codes: list[str], method: str, pair_code: str) -> int | None:
    if method not in method_rows or pair_code not in method_rows[method]:
        return None
    rows = [method_rows[method][code] for code in ordered_codes if code in method_rows[method]]
    for idx, row in enumerate(rows, start=1):
        if row["pair_code"] == pair_code:
            return idx
    return None


def aggregate_error_stats(method_rows: dict[str, dict[str, dict]], ordered_codes: list[str]):
    qe_map = method_rows["QE stage3"]
    methods = [name for name in ("GPTFF v2", "GPTFF v1", "MatterSim v1 5M", "CHGNet stage2") if name in method_rows]
    out = []
    for method in methods:
        errs = np.asarray(
            [phi_value(method_rows[method][code], method) - phi_value(qe_map[code], "QE stage3") for code in ordered_codes if code in method_rows[method]],
            dtype=float,
        )
        if errs.size == 0:
            continue
        out.append(
            {
                "method": method,
                "phi122_mae_mev": float(np.mean(np.abs(errs))),
                "phi122_rmse_mev": float(np.sqrt(np.mean(errs**2))),
                "phi122_maxae_mev": float(np.max(np.abs(errs))),
            }
        )
    return out


def build_payload(args):
    method_rows, ordered_codes, source_paths = _method_sources(args)
    pair_rows = []
    for code in ordered_codes:
        row = {
            "pair_code": code,
            "pair_label": short_label(code),
            "rank_gptff_v2": rank_value(method_rows, ordered_codes, "GPTFF v2", code),
            "rank_gptff_v1": rank_value(method_rows, ordered_codes, "GPTFF v1", code),
            "rank_mattersim_v1_5m": rank_value(method_rows, ordered_codes, "MatterSim v1 5M", code),
            "rank_chgnet_stage2": rank_value(method_rows, ordered_codes, "CHGNet stage2", code),
            "rank_qe_stage3": rank_value(method_rows, ordered_codes, "QE stage3", code),
            "phi122_gptff_v2_mev": phi_value(method_rows["GPTFF v2"][code], "GPTFF v2") if code in method_rows.get("GPTFF v2", {}) else None,
            "phi122_gptff_v1_mev": phi_value(method_rows["GPTFF v1"][code], "GPTFF v1") if code in method_rows.get("GPTFF v1", {}) else None,
            "phi122_mattersim_v1_5m_mev": phi_value(method_rows["MatterSim v1 5M"][code], "MatterSim v1 5M") if code in method_rows.get("MatterSim v1 5M", {}) else None,
            "phi122_chgnet_stage2_mev": phi_value(method_rows["CHGNet stage2"][code], "CHGNet stage2") if code in method_rows.get("CHGNet stage2", {}) else None,
            "phi122_qe_stage3_mev": phi_value(method_rows["QE stage3"][code], "QE stage3"),
            "rmse_gptff_v2_ev_supercell": rmse_value(method_rows["GPTFF v2"][code], "GPTFF v2") if code in method_rows.get("GPTFF v2", {}) else None,
            "rmse_gptff_v1_ev_supercell": rmse_value(method_rows["GPTFF v1"][code], "GPTFF v1") if code in method_rows.get("GPTFF v1", {}) else None,
            "rmse_mattersim_v1_5m_ev_supercell": rmse_value(method_rows["MatterSim v1 5M"][code], "MatterSim v1 5M") if code in method_rows.get("MatterSim v1 5M", {}) else None,
            "rmse_chgnet_stage2_ev_supercell": rmse_value(method_rows["CHGNet stage2"][code], "CHGNet stage2") if code in method_rows.get("CHGNet stage2", {}) else None,
            "rmse_qe_stage3_ev_supercell": rmse_value(method_rows["QE stage3"][code], "QE stage3"),
        }
        row["delta_phi122_gptff_v2_minus_qe_mev"] = None if row["phi122_gptff_v2_mev"] is None else row["phi122_gptff_v2_mev"] - row["phi122_qe_stage3_mev"]
        row["delta_phi122_gptff_v1_minus_qe_mev"] = None if row["phi122_gptff_v1_mev"] is None else row["phi122_gptff_v1_mev"] - row["phi122_qe_stage3_mev"]
        row["delta_phi122_mattersim_v1_5m_minus_qe_mev"] = None if row["phi122_mattersim_v1_5m_mev"] is None else row["phi122_mattersim_v1_5m_mev"] - row["phi122_qe_stage3_mev"]
        row["delta_phi122_chgnet_stage2_minus_qe_mev"] = None if row["phi122_chgnet_stage2_mev"] is None else row["phi122_chgnet_stage2_mev"] - row["phi122_qe_stage3_mev"]
        pair_rows.append(row)
    return {
        "metadata": {
            "root": str(ROOT),
            "sources": {key: None if value is None else str(value) for key, value in source_paths.items()},
            "reference_method": "QE stage3",
            "reference_field": "qe_phi122_mev",
        },
        "pair_rows": pair_rows,
        "aggregate_error_stats": aggregate_error_stats(method_rows, ordered_codes),
    }


def write_csv(path: Path, pair_rows: list[dict]):
    fieldnames = [
        "pair_code",
        "pair_label",
        "rank_gptff_v2",
        "rank_gptff_v1",
        "rank_mattersim_v1_5m",
        "rank_chgnet_stage2",
        "rank_qe_stage3",
        "phi122_gptff_v2_mev",
        "phi122_gptff_v1_mev",
        "phi122_mattersim_v1_5m_mev",
        "phi122_chgnet_stage2_mev",
        "phi122_qe_stage3_mev",
        "delta_phi122_gptff_v2_minus_qe_mev",
        "delta_phi122_gptff_v1_minus_qe_mev",
        "delta_phi122_mattersim_v1_5m_minus_qe_mev",
        "delta_phi122_chgnet_stage2_minus_qe_mev",
        "rmse_gptff_v2_ev_supercell",
        "rmse_gptff_v1_ev_supercell",
        "rmse_mattersim_v1_5m_ev_supercell",
        "rmse_chgnet_stage2_ev_supercell",
        "rmse_qe_stage3_ev_supercell",
    ]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(pair_rows)


def write_markdown(path: Path, payload: dict):
    lines = [
        "# WSe2 Remote Stage1 Comparison Data",
        "",
        "## Pair Table",
        "",
        "| Pair | v2 Rank | v1 Rank | MatterSim Rank | CHGNet Rank | QE Rank | v2 phi122 | v1 phi122 | MatterSim phi122 | CHGNet phi122 | QE phi122 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["pair_rows"]:
        phi_v2 = "" if row["phi122_gptff_v2_mev"] is None else f"{row['phi122_gptff_v2_mev']:.6f}"
        phi_v1 = "" if row["phi122_gptff_v1_mev"] is None else f"{row['phi122_gptff_v1_mev']:.6f}"
        phi_ms = "" if row["phi122_mattersim_v1_5m_mev"] is None else f"{row['phi122_mattersim_v1_5m_mev']:.6f}"
        phi_chg = "" if row["phi122_chgnet_stage2_mev"] is None else f"{row['phi122_chgnet_stage2_mev']:.6f}"
        lines.append(
            f"| `{row['pair_code']}` | {row['rank_gptff_v2']} | {row['rank_gptff_v1']} | {row['rank_mattersim_v1_5m']} | {row['rank_chgnet_stage2']} | {row['rank_qe_stage3']} | "
            f"{phi_v2} | {phi_v1} | {phi_ms} | {phi_chg} | {row['phi122_qe_stage3_mev']:.6f} |"
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
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = build_payload(args)
    json_path = DATA_DIR / "wse2_remote_stage1_comparison_data.json"
    csv_path = DATA_DIR / "wse2_remote_stage1_comparison_data.csv"
    md_path = DATA_DIR / "wse2_remote_stage1_comparison_data.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    write_csv(csv_path, payload["pair_rows"])
    write_markdown(md_path, payload)
    print(json_path)
    print(csv_path)
    print(md_path)


if __name__ == "__main__":
    main()
