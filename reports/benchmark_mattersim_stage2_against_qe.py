#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "reports" / "data"
DEFAULT_MODEL = "mattersim-v1.0.0-5M"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server_highthroughput_workflow.stage_contracts import create_stage1_manifest


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark MatterSim stage2 against remote QE stage3 references.")
    parser.add_argument("--remote-baselines-root", type=str, default=str(ROOT / "remote_system_baselines"))
    parser.add_argument("--runs-root", type=str, default=str(ROOT / "local_runs" / "benchmark_mattersim"))
    parser.add_argument("--device", type=str, default="cpu")
    parser.add_argument("--model", type=str, default=DEFAULT_MODEL)
    parser.add_argument("--systems", nargs="*", default=["ws2_monolayer", "mos2_monolayer", "mose2_monolayer"])
    return parser.parse_args()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def load_json_rows(path: Path, key: str) -> list[dict]:
    return load_json(path)[key]


def load_csv_rows(path: Path) -> list[dict]:
    with path.open() as handle:
        return list(csv.DictReader(handle))


def maybe_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    if path.suffix == ".csv":
        return load_csv_rows(path)
    return load_json_rows(path, "pairs")


def qe_phi(row: dict) -> float:
    return float(row["qe_phi122_mev"])


def stage2_phi(row: dict) -> float:
    return float(row["phi122_mev"])


def pick_stage1_source(system_root: Path) -> Path:
    candidates = [
        system_root / "chgnet_strict_stage1_bridge_20260402",
        system_root / "gptff_v1_strict_stage1_bridge_20260402",
        system_root / "gptff_v2_strict_stage1_bridge_20260402",
    ]
    for path in candidates:
        if (path / "stage1" / "outputs" / "mode_pairs.selected.json").exists():
            return path
    raise FileNotFoundError(f"No stage1 source found under {system_root}")


def prepare_stage1(run_root: Path, stage1_source_root: Path, system_id: str) -> Path:
    mode_pairs = stage1_source_root / "stage1" / "outputs" / "mode_pairs.selected.json"
    structure = stage1_source_root / "stage1" / "inputs" / "system.scf.inp"
    pseudo_dir = stage1_source_root / "stage1" / "inputs"
    run_root.mkdir(parents=True, exist_ok=True)
    return create_stage1_manifest(
        run_root=run_root,
        mode_pairs_json=mode_pairs,
        structure=structure,
        pseudo_dir=pseudo_dir,
        system_id=system_id,
        system_dir=None,
    )


def run_mattersim_stage2(run_root: Path, stage1_manifest: Path, model: str, device: str) -> Path:
    stage1 = load_json(stage1_manifest)
    mode_pairs_json = run_root / stage1["files"]["mode_pairs_json"]
    structure = run_root / stage1["files"]["structure"]
    output_root = run_root / "stage2" / "outputs"
    cmd = [
        sys.executable,
        str(ROOT / "mlff_modepair_workflow" / "run_pair_screening_optimized.py"),
        "--backend",
        "mattersim",
        "--device",
        device,
        "--model",
        model,
        "--run-tag",
        "mattersim_v1_5m",
        "--mode-pairs-json",
        str(mode_pairs_json),
        "--structure",
        str(structure),
        "--output-root",
        str(output_root),
    ]
    subprocess.run(cmd, cwd=str(ROOT), check=True)
    return output_root / "mattersim_v1_5m" / "screening" / "pair_ranking.json"


def compute_error_stats(rows: list[dict], qe_map: dict[str, dict], phi_field: str) -> dict:
    errors = []
    for row in rows:
        pair_code = row["pair_code"]
        if pair_code not in qe_map:
            continue
        errors.append(float(row[phi_field]) - qe_phi(qe_map[pair_code]))
    if not errors:
        return {"count": 0, "mae_mev": None, "rmse_mev": None, "maxae_mev": None}
    mae = sum(abs(err) for err in errors) / len(errors)
    rmse = math.sqrt(sum(err * err for err in errors) / len(errors))
    maxae = max(abs(err) for err in errors)
    return {"count": len(errors), "mae_mev": mae, "rmse_mev": rmse, "maxae_mev": maxae}


def rank_map(rows: list[dict]) -> dict[str, int]:
    return {row["pair_code"]: idx for idx, row in enumerate(rows, start=1)}


def top_overlap(rows_a: list[dict], rows_b: list[dict], n: int = 5) -> int:
    top_a = {row["pair_code"] for row in rows_a[:n]}
    top_b = {row["pair_code"] for row in rows_b[:n]}
    return len(top_a & top_b)


def _stage2_candidates(system_dir: Path, run_name: str, tag: str) -> list[Path]:
    return [
        system_dir / run_name / "stage2" / "outputs" / tag / "screening" / "pair_ranking.json",
        system_dir / run_name / "stage2" / "outputs" / "gptff" / "screening" / "pair_ranking.json",
    ]


def _first_existing(paths: list[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def summarize_system(system_dir: Path, runs_root: Path, model: str, device: str) -> dict:
    system_id = system_dir.name
    stage1_source = pick_stage1_source(system_dir)
    qe_path = system_dir / "gptff_v1_strict_stage1_bridge_20260402" / "stage3" / "qe" / "gptff" / "results" / "qe_ranking.json"
    if not qe_path.exists():
        raise FileNotFoundError(f"Missing QE reference for {system_id}: {qe_path}")

    run_root = runs_root / system_id / "mattersim_v1_5m_from_remote_stage1"
    stage1_manifest = prepare_stage1(run_root, stage1_source, system_id)
    mattersim_path = run_mattersim_stage2(run_root, stage1_manifest, model=model, device=device)

    mattersim_rows = load_json_rows(mattersim_path, "pairs")
    qe_rows = load_json_rows(qe_path, "rows")
    qe_map = {row["pair_code"]: row for row in qe_rows}

    chgnet_rows = maybe_rows(_first_existing(_stage2_candidates(system_dir, "chgnet_strict_stage1_bridge_20260402", "chgnet")))
    gptff_v1_rows = maybe_rows(_first_existing(_stage2_candidates(system_dir, "gptff_v1_strict_stage1_bridge_20260402", "gptff_v1")))
    gptff_v2_rows = maybe_rows(_first_existing(_stage2_candidates(system_dir, "gptff_v2_strict_stage1_bridge_20260402", "gptff_v2")))

    result = {
        "system": system_id,
        "run_root": str(run_root),
        "stage1_source_root": str(stage1_source),
        "qe_reference": str(qe_path),
        "mattersim_pair_ranking_json": str(mattersim_path),
        "top5": {
            "mattersim": [row["pair_code"] for row in mattersim_rows[:5]],
            "gptff_v1": [row["pair_code"] for row in gptff_v1_rows[:5]],
            "gptff_v2": [row["pair_code"] for row in gptff_v2_rows[:5]],
            "chgnet": [row["pair_code"] for row in chgnet_rows[:5]],
            "qe": [row["pair_code"] for row in qe_rows[:5]],
        },
        "errors_vs_qe": {
            "mattersim_v1_5m": compute_error_stats(mattersim_rows, qe_map, "phi122_mev"),
            "gptff_v1": compute_error_stats(gptff_v1_rows, qe_map, "phi122_mev"),
            "gptff_v2": compute_error_stats(gptff_v2_rows, qe_map, "phi122_mev"),
            "chgnet": compute_error_stats(chgnet_rows, qe_map, "phi122_mev"),
        },
        "top5_overlap_vs_qe": {
            "mattersim_v1_5m": top_overlap(mattersim_rows, qe_rows, 5),
            "gptff_v1": top_overlap(gptff_v1_rows, qe_rows, 5),
            "gptff_v2": top_overlap(gptff_v2_rows, qe_rows, 5),
            "chgnet": top_overlap(chgnet_rows, qe_rows, 5),
        },
    }

    detailed_rows = []
    qe_ranks = rank_map(qe_rows)
    mattersim_ranks = rank_map(mattersim_rows)
    gptff_v1_ranks = rank_map(gptff_v1_rows)
    gptff_v2_ranks = rank_map(gptff_v2_rows)
    chgnet_ranks = rank_map(chgnet_rows)
    for row in qe_rows:
        pair_code = row["pair_code"]
        detailed_rows.append(
            {
                "pair_code": pair_code,
                "rank_qe": qe_ranks.get(pair_code),
                "phi_qe_mev": qe_phi(row),
                "rank_mattersim": mattersim_ranks.get(pair_code),
                "phi_mattersim_mev": next((stage2_phi(x) for x in mattersim_rows if x["pair_code"] == pair_code), None),
                "rank_gptff_v1": gptff_v1_ranks.get(pair_code),
                "phi_gptff_v1_mev": next((stage2_phi(x) for x in gptff_v1_rows if x["pair_code"] == pair_code), None),
                "rank_gptff_v2": gptff_v2_ranks.get(pair_code),
                "phi_gptff_v2_mev": next((stage2_phi(x) for x in gptff_v2_rows if x["pair_code"] == pair_code), None),
                "rank_chgnet": chgnet_ranks.get(pair_code),
                "phi_chgnet_mev": next((stage2_phi(x) for x in chgnet_rows if x["pair_code"] == pair_code), None),
            }
        )
    result["qe_pairwise_rows"] = detailed_rows
    return result


def write_outputs(payload: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    json_path = DATA_DIR / "mattersim_multi_system_benchmark.json"
    csv_path = DATA_DIR / "mattersim_multi_system_benchmark.csv"
    md_path = DATA_DIR / "mattersim_multi_system_benchmark.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")

    with csv_path.open("w", newline="") as handle:
        fieldnames = ["system", "method", "count", "mae_mev", "rmse_mev", "maxae_mev", "top5_overlap_vs_qe"]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for system in payload["systems"]:
            for method, stats in system["errors_vs_qe"].items():
                writer.writerow(
                    {
                        "system": system["system"],
                        "method": method,
                        "count": stats["count"],
                        "mae_mev": stats["mae_mev"],
                        "rmse_mev": stats["rmse_mev"],
                        "maxae_mev": stats["maxae_mev"],
                        "top5_overlap_vs_qe": system["top5_overlap_vs_qe"][method],
                    }
                )

    lines = ["# MatterSim Multi-System Benchmark", ""]
    for system in payload["systems"]:
        lines.extend(
            [
                f"## {system['system']}",
                "",
                "| Method | Count | MAE (meV) | RMSE (meV) | MaxAE (meV) | Top5 overlap vs QE |",
                "|---|---:|---:|---:|---:|---:|",
            ]
        )
        for method, stats in system["errors_vs_qe"].items():
            lines.append(
                f"| {method} | {stats['count']} | {stats['mae_mev']:.6f} | {stats['rmse_mev']:.6f} | {stats['maxae_mev']:.6f} | {system['top5_overlap_vs_qe'][method]} |"
            )
        lines.append("")
    md_path.write_text("\n".join(lines) + "\n")
    return json_path, csv_path, md_path


def main():
    args = parse_args()
    remote_baselines_root = Path(args.remote_baselines_root).expanduser().resolve()
    runs_root = Path(args.runs_root).expanduser().resolve()
    systems = []
    for system_name in args.systems:
        system_dir = remote_baselines_root / system_name
        if not system_dir.exists():
            continue
        qe_path = system_dir / "gptff_v1_strict_stage1_bridge_20260402" / "stage3" / "qe" / "gptff" / "results" / "qe_ranking.json"
        if not qe_path.exists():
            continue
        systems.append(summarize_system(system_dir, runs_root=runs_root, model=args.model, device=args.device))
    payload = {"systems": systems}
    json_path, csv_path, md_path = write_outputs(payload)
    print(json_path)
    print(csv_path)
    print(md_path)


if __name__ == "__main__":
    main()
