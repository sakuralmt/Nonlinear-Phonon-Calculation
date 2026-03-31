#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server_highthroughput_workflow import stage23_pipeline as pipeline


def parse_args():
    p = argparse.ArgumentParser(description="Continue server workflow after MLFF screening finishes.")
    p.add_argument("--run-root", type=str, required=True)
    p.add_argument("--backend-tag", type=str, required=True)
    p.add_argument("--ranking-csv", type=str, required=True)
    return p.parse_args()


def load_summary(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Missing workflow summary: {path}")
    return json.loads(path.read_text())


def save_summary(path: Path, payload: dict):
    path.write_text(json.dumps(payload, indent=2))


def find_or_create_attempt(summary: dict, backend_tag: str):
    for attempt in summary.get("attempts", []):
        if attempt.get("backend_tag") == backend_tag:
            return attempt
    attempt = {"backend_tag": backend_tag}
    summary.setdefault("attempts", []).append(attempt)
    return attempt


def main():
    args = parse_args()
    run_root = Path(args.run_root).expanduser().resolve()
    summary_path = run_root / "workflow_summary.json"
    ranking_csv = Path(args.ranking_csv).expanduser().resolve()
    qe_root = run_root / "qe_recheck" / args.backend_tag

    summary = load_summary(summary_path)
    attempt = find_or_create_attempt(summary, args.backend_tag)

    try:
        if not ranking_csv.exists():
            raise FileNotFoundError(f"Missing ranking csv: {ranking_csv}")

        summary["status"] = "mlff_finished"
        attempt["screening_csv"] = str(ranking_csv)
        ranking_json = pipeline.normalize_ranking_csv(ranking_csv, args.backend_tag)
        attempt["screening_ranking_json"] = str(ranking_json)
        save_summary(summary_path, summary)

        summary["status"] = "qe_running"
        save_summary(summary_path, summary)
        final_state, qe_ranking_json = pipeline.run_qe_recheck({"tag": args.backend_tag}, ranking_json, qe_root)

        attempt["qe_run_root"] = str(qe_root)
        attempt["qe_final_state"] = final_state
        attempt["qe_ranking_json"] = str(qe_ranking_json)
        attempt["status"] = "qe_completed"
        summary["selected_backend"] = args.backend_tag
        summary["status"] = "qe_completed"
        save_summary(summary_path, summary)
        print(f"continuation complete for backend: {args.backend_tag}", flush=True)
    except Exception as exc:
        attempt["status"] = "failed"
        attempt["error"] = f"{type(exc).__name__}: {exc}"
        summary["status"] = "continuation_failed"
        save_summary(summary_path, summary)
        raise


if __name__ == "__main__":
    main()
