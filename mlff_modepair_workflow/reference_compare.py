#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
from pathlib import Path


BASELINE_SESSION_ID = "019d1465-ee58-79d3-9b54-7d8789910273"
BASELINE_LABEL = "thread_019d1465"
REPO_ROOT = Path(__file__).resolve().parent.parent

COMPARISON_FIELDS = (
    "rank",
    "point_label",
    "gamma_mode_code",
    "target_mode_code",
    "phi122_mev",
    "rmse_ev_supercell",
    "gamma_freq_fit_thz",
    "target_freq_fit_thz",
)

STAGE3_COMPARISON_FIELDS = (
    "rank",
    "point_label",
    "gamma_mode_code",
    "target_mode_code",
    "consensus_phi122_mean_mev",
    "qe_phi122_mev",
    "qe_r2",
    "qe_rmse_ev_supercell",
    "qe_gamma_axis_freq_thz",
    "qe_target_axis_freq_thz",
)

STAGE2_VS_STAGE3_TOP_N = 5


def _baseline_candidate_roots() -> list[Path]:
    roots: list[Path] = []
    env_root = os.environ.get("NPC_WSE2_BASELINE_ROOT")
    if env_root:
        roots.append(Path(env_root).expanduser().resolve())
    roots.extend(
        [
            REPO_ROOT / "remote_baselines" / "wse2_stage3_run",
            REPO_ROOT / "examples" / "wse2" / "contract_handoff" / "release_run",
            REPO_ROOT / "examples" / "wse2_input_example" / "contract_handoff" / "release_run",
        ]
    )
    seen: set[Path] = set()
    unique: list[Path] = []
    for root in roots:
        if root in seen:
            continue
        seen.add(root)
        unique.append(root)
    return unique


def _try_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _normalize_row(row: dict, rank: int | None = None):
    rank_value = rank if rank is not None else row.get("rank")
    return {
        "pair_code": row.get("pair_code"),
        "rank": None if rank_value in {None, ""} else int(rank_value),
        "point_label": row.get("point_label"),
        "gamma_mode_code": row.get("gamma_mode_code"),
        "target_mode_code": row.get("target_mode_code"),
        "phi122_mev": _try_float(row.get("phi122_mev")),
        "rmse_ev_supercell": _try_float(row.get("rmse_ev_supercell")),
        "gamma_freq_fit_thz": _try_float(row.get("gamma_freq_fit_thz")),
        "target_freq_fit_thz": _try_float(row.get("target_freq_fit_thz")),
    }


def _normalize_stage3_row(row: dict, rank: int | None = None):
    rank_value = rank if rank is not None else row.get("rank")
    return {
        "pair_code": row.get("pair_code"),
        "rank": None if rank_value in {None, ""} else int(rank_value),
        "point_label": row.get("point_label"),
        "gamma_mode_code": row.get("gamma_mode_code"),
        "target_mode_code": row.get("target_mode_code"),
        "consensus_phi122_mean_mev": _try_float(row.get("consensus_phi122_mean_mev")),
        "qe_phi122_mev": _try_float(row.get("qe_phi122_mev")),
        "qe_r2": _try_float(row.get("qe_r2")),
        "qe_rmse_ev_supercell": _try_float(row.get("qe_rmse_ev_supercell")),
        "qe_gamma_axis_freq_thz": _try_float(row.get("qe_gamma_axis_freq_thz")),
        "qe_target_axis_freq_thz": _try_float(row.get("qe_target_axis_freq_thz")),
    }


def default_baseline_reference():
    for root in _baseline_candidate_roots():
        ranking_csv_candidates = [
            root / "stage2_outputs" / "chgnet" / "screening" / "pair_ranking.csv",
            root / "stage2" / "outputs" / "chgnet" / "screening" / "pair_ranking.csv",
        ]
        ranking_json_candidates = [
            root / "stage2_outputs" / "chgnet" / "screening" / "pair_ranking.json",
            root / "stage2" / "outputs" / "chgnet" / "screening" / "pair_ranking.json",
        ]
        stage1_manifest_candidates = [
            root / "contracts" / "stage1.manifest.json",
            root / "stage1_manifest.json",
        ]
        stage1_mode_pairs_candidates = [
            root / "stage1" / "outputs" / "mode_pairs.selected.json",
        ]
        stage1_structure_candidates = [
            root / "stage1" / "inputs" / "system.scf.inp",
        ]
        stage3_ranking_candidates = [
            root / "stage3_qe" / "chgnet" / "results" / "qe_ranking.json",
            root / "stage3" / "qe" / "chgnet" / "results" / "qe_ranking.json",
        ]
        ranking_csv = next((path for path in ranking_csv_candidates if path.exists()), ranking_csv_candidates[0])
        ranking_json = next((path for path in ranking_json_candidates if path.exists()), ranking_json_candidates[0])
        stage1_manifest = next((path for path in stage1_manifest_candidates if path.exists()), stage1_manifest_candidates[0])
        stage1_mode_pairs = next((path for path in stage1_mode_pairs_candidates if path.exists()), stage1_mode_pairs_candidates[0])
        stage1_structure = next((path for path in stage1_structure_candidates if path.exists()), stage1_structure_candidates[0])
        stage3_ranking_json = next((path for path in stage3_ranking_candidates if path.exists()), None)
        stage2_manifest = root / "stage2_manifest.json"
        stage3_manifest = root / "stage3_contract.json"
        if ranking_csv.exists() and ranking_json.exists():
            return {
                "session_id": BASELINE_SESSION_ID,
                "label": BASELINE_LABEL,
                "ranking_csv": str(ranking_csv),
                "ranking_json": str(ranking_json),
                "stage1_manifest": str(stage1_manifest) if stage1_manifest.exists() else None,
                "stage1_mode_pairs_json": str(stage1_mode_pairs) if stage1_mode_pairs.exists() else None,
                "stage1_structure": str(stage1_structure) if stage1_structure.exists() else None,
                "stage3_ranking_json": None if stage3_ranking_json is None else str(stage3_ranking_json),
                "source_files": [
                    str(path)
                    for path in (
                        ranking_csv,
                        ranking_json,
                        stage1_manifest,
                        stage1_mode_pairs,
                        stage1_structure,
                        stage2_manifest,
                        stage3_manifest,
                        stage3_ranking_json,
                    )
                    if path is not None and path.exists()
                ],
            }
    return None


def load_baseline_rows(reference: dict | None):
    if not reference:
        return []
    ranking_csv = Path(reference["ranking_csv"])
    with ranking_csv.open() as handle:
        return [_normalize_row(row) for row in csv.DictReader(handle)]


def load_stage3_baseline_rows(reference: dict | None):
    if not reference:
        return []
    stage3_path = reference.get("stage3_ranking_json")
    if not stage3_path:
        return []
    payload = json.loads(Path(stage3_path).read_text())
    return [_normalize_stage3_row(row, rank=index) for index, row in enumerate(payload.get("rows", []), start=1)]


def _field_delta(field: str, current: dict | None, baseline: dict | None):
    current_value = None if current is None else current.get(field)
    baseline_value = None if baseline is None else baseline.get(field)
    if field == "rank":
        if current_value is None or baseline_value is None:
            return None
        return int(current_value) - int(baseline_value)
    current_num = _try_float(current_value)
    baseline_num = _try_float(baseline_value)
    if current_num is None or baseline_num is None:
        return None
    return current_num - baseline_num


def compare_rankings(current_rows: list[dict], baseline_rows: list[dict], float_tol: float = 1.0e-6):
    current_rows = [_normalize_row(row, rank=index) for index, row in enumerate(current_rows, start=1)]
    baseline_rows = [_normalize_row(row) for row in baseline_rows]
    current_by_pair = {row["pair_code"]: row for row in current_rows}
    baseline_by_pair = {row["pair_code"]: row for row in baseline_rows}
    ordered_codes = [row["pair_code"] for row in current_rows]
    ordered_codes.extend(code for code in baseline_by_pair if code not in current_by_pair)

    rows = []
    counts = {"exact_match": 0, "changed": 0, "only_current": 0, "only_baseline": 0}
    for pair_code in ordered_codes:
        current = current_by_pair.get(pair_code)
        baseline = baseline_by_pair.get(pair_code)
        if current is None:
            status = "only_baseline"
        elif baseline is None:
            status = "only_current"
        else:
            status = "exact_match"
            for field in COMPARISON_FIELDS:
                lhs = current.get(field)
                rhs = baseline.get(field)
                if field in {"point_label", "gamma_mode_code", "target_mode_code"}:
                    if lhs != rhs:
                        status = "changed"
                        break
                    continue
                lhs_num = _try_float(lhs)
                rhs_num = _try_float(rhs)
                if lhs_num is None and rhs_num is None:
                    continue
                if lhs_num is None or rhs_num is None or abs(lhs_num - rhs_num) > float_tol:
                    status = "changed"
                    break
        counts[status] += 1
        rows.append(
            {
                "pair_code": pair_code,
                "status": status,
                "current": current,
                "baseline": baseline,
                "deltas": {field: _field_delta(field, current, baseline) for field in COMPARISON_FIELDS},
            }
        )
    return {
        "comparison_fields": list(COMPARISON_FIELDS),
        "summary": {"current_pairs": len(current_rows), "baseline_pairs": len(baseline_rows), **counts},
        "rows": rows,
    }


def compare_stage3_rankings(current_rows: list[dict], baseline_rows: list[dict], float_tol: float = 1.0e-6):
    current_rows = [_normalize_stage3_row(row, rank=index) for index, row in enumerate(current_rows, start=1)]
    baseline_rows = [_normalize_stage3_row(row) for row in baseline_rows]
    current_by_pair = {row["pair_code"]: row for row in current_rows}
    baseline_by_pair = {row["pair_code"]: row for row in baseline_rows}
    ordered_codes = [row["pair_code"] for row in current_rows]
    ordered_codes.extend(code for code in baseline_by_pair if code not in current_by_pair)

    rows = []
    counts = {"exact_match": 0, "changed": 0, "only_current": 0, "only_baseline": 0}
    for pair_code in ordered_codes:
        current = current_by_pair.get(pair_code)
        baseline = baseline_by_pair.get(pair_code)
        if current is None:
            status = "only_baseline"
        elif baseline is None:
            status = "only_current"
        else:
            status = "exact_match"
            for field in STAGE3_COMPARISON_FIELDS:
                lhs = current.get(field)
                rhs = baseline.get(field)
                if field in {"point_label", "gamma_mode_code", "target_mode_code"}:
                    if lhs != rhs:
                        status = "changed"
                        break
                    continue
                lhs_num = _try_float(lhs)
                rhs_num = _try_float(rhs)
                if lhs_num is None and rhs_num is None:
                    continue
                if lhs_num is None or rhs_num is None or abs(lhs_num - rhs_num) > float_tol:
                    status = "changed"
                    break
        counts[status] += 1
        rows.append(
            {
                "pair_code": pair_code,
                "status": status,
                "current": current,
                "baseline": baseline,
                "deltas": {field: _field_delta(field, current, baseline) for field in STAGE3_COMPARISON_FIELDS},
            }
        )
    return {
        "comparison_fields": list(STAGE3_COMPARISON_FIELDS),
        "summary": {"current_pairs": len(current_rows), "baseline_pairs": len(baseline_rows), **counts},
        "rows": rows,
    }


def write_comparison_outputs(output_dir: Path, reference: dict | None, current_rows: list[dict]):
    if not reference:
        return None
    baseline_rows = load_baseline_rows(reference)
    comparison = compare_rankings(current_rows, baseline_rows)
    payload = {
        "baseline_reference": {
            "session_id": reference["session_id"],
            "label": reference["label"],
            "source_files": list(reference.get("source_files", [])),
            "ranking_csv": reference["ranking_csv"],
            "ranking_json": reference["ranking_json"],
        },
        "reference_comparison": comparison,
    }
    session_prefix = reference["session_id"].split("-")[0]
    json_path = output_dir / f"comparison_against_{session_prefix}.json"
    csv_path = output_dir / f"comparison_against_{session_prefix}.csv"
    md_path = output_dir / f"comparison_against_{session_prefix}.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")

    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "pair_code",
                "status",
                "current_rank",
                "baseline_rank",
                "rank_delta",
                "current_phi122_mev",
                "baseline_phi122_mev",
                "phi122_delta_mev",
                "current_rmse_ev_supercell",
                "baseline_rmse_ev_supercell",
                "rmse_delta_ev_supercell",
                "current_gamma_freq_fit_thz",
                "baseline_gamma_freq_fit_thz",
                "gamma_freq_fit_delta_thz",
                "current_target_freq_fit_thz",
                "baseline_target_freq_fit_thz",
                "target_freq_fit_delta_thz",
            ]
        )
        for row in comparison["rows"]:
            current = row["current"] or {}
            baseline = row["baseline"] or {}
            deltas = row["deltas"]
            writer.writerow(
                [
                    row["pair_code"],
                    row["status"],
                    current.get("rank"),
                    baseline.get("rank"),
                    deltas.get("rank"),
                    current.get("phi122_mev"),
                    baseline.get("phi122_mev"),
                    deltas.get("phi122_mev"),
                    current.get("rmse_ev_supercell"),
                    baseline.get("rmse_ev_supercell"),
                    deltas.get("rmse_ev_supercell"),
                    current.get("gamma_freq_fit_thz"),
                    baseline.get("gamma_freq_fit_thz"),
                    deltas.get("gamma_freq_fit_thz"),
                    current.get("target_freq_fit_thz"),
                    baseline.get("target_freq_fit_thz"),
                    deltas.get("target_freq_fit_thz"),
                ]
            )

    summary = comparison["summary"]
    md_lines = [
        f"# Comparison against {reference['session_id']}",
        "",
        f"- Baseline label: `{reference['label']}`",
        f"- Current pairs: `{summary['current_pairs']}`",
        f"- Baseline pairs: `{summary['baseline_pairs']}`",
        f"- Exact matches: `{summary['exact_match']}`",
        f"- Changed: `{summary['changed']}`",
        f"- Only current: `{summary['only_current']}`",
        f"- Only baseline: `{summary['only_baseline']}`",
    ]
    md_path.write_text("\n".join(md_lines) + "\n")
    return {
        "baseline_reference": payload["baseline_reference"],
        "reference_comparison": comparison,
        "output_files": {"json": str(json_path), "csv": str(csv_path), "markdown": str(md_path)},
    }


def write_stage3_comparison_outputs(output_dir: Path, reference: dict | None, current_rows: list[dict]):
    baseline_rows = load_stage3_baseline_rows(reference)
    session_id = BASELINE_SESSION_ID if not reference else reference["session_id"]
    json_path = output_dir / f"stage3_comparison_against_{session_id.split('-')[0]}.json"
    csv_path = output_dir / f"stage3_comparison_against_{session_id.split('-')[0]}.csv"
    md_path = output_dir / f"stage3_comparison_against_{session_id.split('-')[0]}.md"
    if not baseline_rows:
        payload = {
            "baseline_reference": None if not reference else {
                "session_id": reference["session_id"],
                "label": reference["label"],
                "stage3_ranking_json": reference.get("stage3_ranking_json"),
                "source_files": list(reference.get("source_files", [])),
            },
            "reference_comparison": None,
            "note": "No stage3 baseline ranking was found for the reference session.",
        }
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
        csv_path.write_text("pair_code,status\n")
        md_path.write_text(
            "\n".join(
                [
                    f"# Stage3 comparison against {session_id}",
                    "",
                    "- No stage3 baseline ranking was found for the reference session.",
                ]
            )
            + "\n"
        )
        return {
            "baseline_reference": payload["baseline_reference"],
            "reference_comparison": None,
            "output_files": {"json": str(json_path), "csv": str(csv_path), "markdown": str(md_path)},
            "note": payload["note"],
        }

    comparison = compare_stage3_rankings(current_rows, baseline_rows)
    payload = {
        "baseline_reference": {
            "session_id": reference["session_id"],
            "label": reference["label"],
            "stage3_ranking_json": reference.get("stage3_ranking_json"),
            "source_files": list(reference.get("source_files", [])),
        },
        "reference_comparison": comparison,
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["pair_code", "status", "current_rank", "baseline_rank", "rank_delta", "current_qe_phi122_mev", "baseline_qe_phi122_mev", "qe_phi122_delta_mev"])
        for row in comparison["rows"]:
            current = row["current"] or {}
            baseline = row["baseline"] or {}
            writer.writerow(
                [
                    row["pair_code"],
                    row["status"],
                    current.get("rank"),
                    baseline.get("rank"),
                    row["deltas"].get("rank"),
                    current.get("qe_phi122_mev"),
                    baseline.get("qe_phi122_mev"),
                    row["deltas"].get("qe_phi122_mev"),
                ]
            )
    md_path.write_text(
        "\n".join(
            [
                f"# Stage3 comparison against {reference['session_id']}",
                "",
                f"- Current pairs: `{comparison['summary']['current_pairs']}`",
                f"- Baseline pairs: `{comparison['summary']['baseline_pairs']}`",
                f"- Exact matches: `{comparison['summary']['exact_match']}`",
                f"- Changed: `{comparison['summary']['changed']}`",
                f"- Only current: `{comparison['summary']['only_current']}`",
                f"- Only baseline: `{comparison['summary']['only_baseline']}`",
            ]
        )
        + "\n"
    )
    return {
        "baseline_reference": payload["baseline_reference"],
        "reference_comparison": comparison,
        "output_files": {"json": str(json_path), "csv": str(csv_path), "markdown": str(md_path)},
    }


def write_stage2_vs_stage3_reference_outputs(output_dir: Path, reference: dict | None, current_stage2_rows: list[dict], top_n: int = STAGE2_VS_STAGE3_TOP_N):
    stage2_baseline_rows = load_baseline_rows(reference)
    stage3_baseline_rows = load_stage3_baseline_rows(reference)
    session_id = BASELINE_SESSION_ID if not reference else reference["session_id"]
    json_path = output_dir / "comparison_local_stage2_vs_remote_stage3.json"
    csv_path = output_dir / "comparison_local_stage2_vs_remote_stage3.csv"
    md_path = output_dir / "comparison_local_stage2_vs_remote_stage3.md"

    if not stage3_baseline_rows:
        payload = {
            "baseline_reference": None if not reference else {
                "session_id": reference["session_id"],
                "label": reference["label"],
                "ranking_csv": reference.get("ranking_csv"),
                "ranking_json": reference.get("ranking_json"),
                "stage3_ranking_json": reference.get("stage3_ranking_json"),
                "source_files": list(reference.get("source_files", [])),
            },
            "note": "No stage3 baseline ranking was found for the reference session.",
        }
        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
        csv_path.write_text("pair_code,remote_qe_rank,local_gptff_stage2_rank,remote_chgnet_stage2_rank\n")
        md_path.write_text(
            "\n".join(
                [
                    f"# Stage2 vs remote stage3 comparison against {session_id}",
                    "",
                    "- No stage3 baseline ranking was found for the reference session.",
                ]
            )
            + "\n"
        )
        return {
            "baseline_reference": payload["baseline_reference"],
            "reference_comparison": None,
            "output_files": {"json": str(json_path), "csv": str(csv_path), "markdown": str(md_path)},
            "note": payload["note"],
        }

    current_rows = [_normalize_row(row, rank=index) for index, row in enumerate(current_stage2_rows, start=1)]
    baseline_stage2_rows = [_normalize_row(row) for row in stage2_baseline_rows]
    baseline_stage3_rows = [_normalize_stage3_row(row) for row in stage3_baseline_rows]
    current_by_pair = {row["pair_code"]: row for row in current_rows}
    baseline_stage2_by_pair = {row["pair_code"]: row for row in baseline_stage2_rows}

    local_top = current_rows[:top_n]
    remote_stage2_top = baseline_stage2_rows[:top_n]
    remote_stage3_top = baseline_stage3_rows[:top_n]
    local_top_codes = [row["pair_code"] for row in local_top]
    remote_stage2_top_codes = [row["pair_code"] for row in remote_stage2_top]
    remote_stage3_top_codes = [row["pair_code"] for row in remote_stage3_top]
    local_vs_stage3_overlap = sorted(set(local_top_codes) & set(remote_stage3_top_codes))
    remote_stage2_vs_stage3_overlap = sorted(set(remote_stage2_top_codes) & set(remote_stage3_top_codes))

    rows = []
    for row in remote_stage3_top:
        local_row = current_by_pair.get(row["pair_code"])
        remote_stage2_row = baseline_stage2_by_pair.get(row["pair_code"])
        rows.append(
            {
                "pair_code": row["pair_code"],
                "remote_qe_rank": row.get("rank"),
                "local_gptff_stage2_rank": None if local_row is None else local_row.get("rank"),
                "remote_chgnet_stage2_rank": None if remote_stage2_row is None else remote_stage2_row.get("rank"),
                "local_gptff_phi122_mev": None if local_row is None else local_row.get("phi122_mev"),
                "remote_chgnet_phi122_mev": None if remote_stage2_row is None else remote_stage2_row.get("phi122_mev"),
                "qe_phi122_mev": row.get("qe_phi122_mev"),
                "consensus_phi122_mean_mev": row.get("consensus_phi122_mean_mev"),
                "point_label": row.get("point_label"),
                "gamma_mode_code": row.get("gamma_mode_code"),
                "target_mode_code": row.get("target_mode_code"),
            }
        )

    payload = {
        "baseline_reference": None if not reference else {
            "session_id": reference["session_id"],
            "label": reference["label"],
            "ranking_csv": reference.get("ranking_csv"),
            "ranking_json": reference.get("ranking_json"),
            "stage3_ranking_json": reference.get("stage3_ranking_json"),
            "source_files": list(reference.get("source_files", [])),
        },
        "summary": {
            "top_n": top_n,
            "local_stage2_top_count": len(local_top_codes),
            "remote_stage2_top_count": len(remote_stage2_top_codes),
            "remote_stage3_top_count": len(remote_stage3_top_codes),
            "local_stage2_vs_remote_stage3_overlap": len(local_vs_stage3_overlap),
            "remote_stage2_vs_remote_stage3_overlap": len(remote_stage2_vs_stage3_overlap),
        },
        "local_gptff_stage2_top5": local_top_codes,
        "remote_chgnet_stage2_top5": remote_stage2_top_codes,
        "remote_qe_stage3_top5": remote_stage3_top_codes,
        "intersection_local_stage2_top5_vs_remote_qe_top5": local_vs_stage3_overlap,
        "intersection_remote_stage2_top5_vs_remote_qe_top5": remote_stage2_vs_stage3_overlap,
        "rows": rows,
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")
    with csv_path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "pair_code",
                "remote_qe_rank",
                "local_gptff_stage2_rank",
                "remote_chgnet_stage2_rank",
                "local_gptff_phi122_mev",
                "remote_chgnet_phi122_mev",
                "qe_phi122_mev",
                "consensus_phi122_mean_mev",
                "point_label",
                "gamma_mode_code",
                "target_mode_code",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["pair_code"],
                    row["remote_qe_rank"],
                    row["local_gptff_stage2_rank"],
                    row["remote_chgnet_stage2_rank"],
                    row["local_gptff_phi122_mev"],
                    row["remote_chgnet_phi122_mev"],
                    row["qe_phi122_mev"],
                    row["consensus_phi122_mean_mev"],
                    row["point_label"],
                    row["gamma_mode_code"],
                    row["target_mode_code"],
                ]
            )
    md_lines = [
        f"# Stage2 vs remote stage3 comparison against {session_id}",
        "",
        f"- Local GPTFF stage2 top{top_n} vs remote QE stage3 top{top_n} overlap: `{len(local_vs_stage3_overlap)}/{top_n}`",
        f"- Remote CHGNet stage2 top{top_n} vs remote QE stage3 top{top_n} overlap: `{len(remote_stage2_vs_stage3_overlap)}/{top_n}`",
    ]
    md_path.write_text("\n".join(md_lines) + "\n")
    return {
        "baseline_reference": payload["baseline_reference"],
        "reference_comparison": {"summary": payload["summary"], "rows": rows},
        "output_files": {"json": str(json_path), "csv": str(csv_path), "markdown": str(md_path)},
    }
