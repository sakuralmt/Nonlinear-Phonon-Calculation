#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def parse_args():
    root = Path(__file__).resolve().parents[1]
    default_out = root / "reports" / "output" / "wse2_baseline_analysis"
    p = argparse.ArgumentParser(description="Run the full offline WSe2 baseline analysis toolchain.")
    p.add_argument("--repo-root", default=str(root))
    p.add_argument(
        "--baseline-root",
        default="/Users/lmtsakura/.codex/worktrees/0df5/qiyan/remote_baselines/wse2_stage3_run",
    )
    p.add_argument(
        "--gptff-v1-pair-dir",
        default="/Users/lmtsakura/.codex/worktrees/0df5/qiyan/local_runs/wse2/remote_stage1_rescreen/gptff_v1_remote_stage1/screening_refined",
    )
    p.add_argument(
        "--gptff-v2-pair-dir",
        default="/Users/lmtsakura/.codex/worktrees/0df5/qiyan/local_runs/wse2/remote_stage1_rescreen/gptff_v2_remote_stage1/screening_refined",
    )
    p.add_argument(
        "--chgnet-pair-dir",
        default="/Users/lmtsakura/qiyan_shared/result/wse2/wse2_stage2_stage3_core_20260402/stage2_singlepair_rerun/wse2_remote_baseline_singlepair_stage2/stage2/outputs/chgnet/screening_refined",
    )
    p.add_argument(
        "--qe-top1-dir",
        default="/Users/lmtsakura/qiyan_shared/result/wse2/wse2_stage2_stage3_core_20260402/stage3_real_top1",
    )
    p.add_argument("--python", default=None, help="Python interpreter used to run the sub-tools.")
    p.add_argument("--out-dir", default=str(default_out))
    return p.parse_args()


def pick_python(explicit: str | None) -> str:
    if explicit:
        return explicit
    try:
        __import__("numpy")
        __import__("matplotlib")
        return sys.executable
    except Exception:
        fallback = Path("/opt/anaconda3/bin/python3")
        if fallback.exists():
            return str(fallback)
        return sys.executable


def run(cmd: list[str], cwd: Path) -> None:
    subprocess.run(cmd, check=True, cwd=str(cwd))


def main():
    args = parse_args()
    repo_root = Path(args.repo_root).expanduser().resolve()
    baseline_root = Path(args.baseline_root).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    python_bin = pick_python(args.python)

    qe_ranking = json.loads((baseline_root / "stage3_qe" / "chgnet" / "results" / "qe_ranking.json").read_text())
    top1_pair = qe_ranking["rows"][0]["pair_code"]

    metrics_json = out_dir / "wse2_remote_stage1_comparison_data.json"
    four_panel_json = out_dir / "wse2_top1_models_vs_qe_2x2_summary.json"
    v1_refit_json = out_dir / "gptff_v1_quartic_vs_quintic_refit.json"
    v2_refit_json = out_dir / "gptff_v2_quartic_vs_quintic_refit.json"

    run(
        [
            python_bin,
            str(repo_root / "ops" / "extract_wse2_baseline_metrics.py"),
            "--repo-root",
            str(repo_root),
            "--baseline-root",
            str(baseline_root),
            "--out-dir",
            str(out_dir),
        ],
        repo_root,
    )
    run(
        [
            python_bin,
            str(repo_root / "ops" / "plot_wse2_baseline_metrics.py"),
            "--data-json",
            str(metrics_json),
            "--out-dir",
            str(out_dir),
        ],
        repo_root,
    )
    run(
        [
            python_bin,
            str(repo_root / "ops" / "plot_pes_model_vs_qe_2x2.py"),
            "--pair-code",
            top1_pair,
            "--gptff-v1-summary",
            str(Path(args.gptff_v1_pair_dir) / top1_pair / "summary.json"),
            "--gptff-v1-grid",
            str(Path(args.gptff_v1_pair_dir) / top1_pair / "energy_grid_eV.dat"),
            "--gptff-v2-summary",
            str(Path(args.gptff_v2_pair_dir) / top1_pair / "summary.json"),
            "--gptff-v2-grid",
            str(Path(args.gptff_v2_pair_dir) / top1_pair / "energy_grid_eV.dat"),
            "--chgnet-summary",
            str(Path(args.chgnet_pair_dir) / top1_pair / "summary.json"),
            "--chgnet-grid",
            str(Path(args.chgnet_pair_dir) / top1_pair / "energy_grid_eV.dat"),
            "--qe-summary",
            str(Path(args.qe_top1_dir) / "summary.json"),
            "--qe-grid",
            str(Path(args.qe_top1_dir) / "energy_grid_ry.dat"),
            "--qe-pair-meta",
            str(Path(args.qe_top1_dir) / "pair_meta.json"),
            "--summary-out",
            str(four_panel_json),
            "--out",
            str(out_dir / "wse2_top1_models_vs_qe_2x2.png"),
            "--title",
            f"WSe2 top1 pair {top1_pair}: GPTFF v1 / GPTFF v2 / CHGNet / QE",
        ],
        repo_root,
    )
    run(
        [
            python_bin,
            str(repo_root / "ops" / "plot_pes_refit_compare.py"),
            "--label",
            "GPTFF v1",
            "--pair-code",
            top1_pair,
            "--summary",
            str(Path(args.gptff_v1_pair_dir) / top1_pair / "summary.json"),
            "--grid",
            str(Path(args.gptff_v1_pair_dir) / top1_pair / "energy_grid_eV.dat"),
            "--summary-out",
            str(v1_refit_json),
            "--out",
            str(out_dir / "gptff_v1_quartic_vs_quintic_refit.png"),
        ],
        repo_root,
    )
    run(
        [
            python_bin,
            str(repo_root / "ops" / "plot_pes_refit_compare.py"),
            "--label",
            "GPTFF v2",
            "--pair-code",
            top1_pair,
            "--summary",
            str(Path(args.gptff_v2_pair_dir) / top1_pair / "summary.json"),
            "--grid",
            str(Path(args.gptff_v2_pair_dir) / top1_pair / "energy_grid_eV.dat"),
            "--summary-out",
            str(v2_refit_json),
            "--out",
            str(out_dir / "gptff_v2_quartic_vs_quintic_refit.png"),
        ],
        repo_root,
    )

    four_panel = json.loads(four_panel_json.read_text())
    v1_refit = json.loads(v1_refit_json.read_text())
    v2_refit = json.loads(v2_refit_json.read_text())
    top1_summary = {
        "pair_code": top1_pair,
        "four_panel": four_panel,
        "refits": {"gptff_v1": v1_refit, "gptff_v2": v2_refit},
        "artifacts": {
            "metrics_json": str(metrics_json),
            "phi122_bar": str(out_dir / "wse2_phi122_bar_comparison.png"),
            "phi122_error": str(out_dir / "wse2_phi122_error_vs_qe.png"),
            "rmse": str(out_dir / "wse2_rmse_comparison.png"),
            "aggregate_error": str(out_dir / "wse2_aggregate_error_metrics.png"),
            "four_panel": str(out_dir / "wse2_top1_models_vs_qe_2x2.png"),
            "v1_refit": str(out_dir / "gptff_v1_quartic_vs_quintic_refit.png"),
            "v2_refit": str(out_dir / "gptff_v2_quartic_vs_quintic_refit.png"),
        },
        "notes": [
            "This is an offline analysis toolchain and does not enter npc or the runtime workflow.",
            "Baseline ranking identity comes from remote_baselines/wse2_stage3_run.",
            "Raw CHGNet and raw QE top1 grids are loaded from the preserved formal data copies under qiyan_shared.",
            f"Sub-tools executed with Python interpreter: {python_bin}",
        ],
    }
    (out_dir / "wse2_top1_refit_summary.json").write_text(json.dumps(top1_summary, indent=2))
    (out_dir / "wse2_top1_refit_summary.md").write_text(
        "\n".join(
            [
                "# WSe2 baseline offline analysis",
                "",
                f"Top1 pair: `{top1_pair}`",
                "",
                "## Quartic four-panel summary",
                "",
            ]
            + [
                f"- {panel['label']}: phi122={panel['phi122_mev']:.6f} meV, r2={panel['r2']:.6f}, rmse={panel['rmse_ev']:.6f} eV"
                for panel in four_panel["panels"]
            ]
            + [
                "",
                "## High-order refit sensitivity",
                "",
                f"- GPTFF v1: quartic={v1_refit['quartic']['phi122_mev']:.6f} meV, quintic={v1_refit['quintic']['phi122_mev']:.6f} meV, delta={v1_refit['phi122_delta_mev']:.6f} meV",
                f"- GPTFF v2: quartic={v2_refit['quartic']['phi122_mev']:.6f} meV, quintic={v2_refit['quintic']['phi122_mev']:.6f} meV, delta={v2_refit['phi122_delta_mev']:.6f} meV",
                "",
                "## Artifacts",
                "",
            ]
            + [f"- `{k}`: `{v}`" for k, v in top1_summary["artifacts"].items()]
            + [""]
        )
    )

    print(out_dir)


if __name__ == "__main__":
    main()
