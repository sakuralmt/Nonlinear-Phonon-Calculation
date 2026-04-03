#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


SUPPORTED_MATERIALS = ("mos2", "mose2", "ws2")


def parse_args():
    root = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser(description="Run offline TMD stage2 multi-model analysis toolchain.")
    p.add_argument("--repo-root", default=str(root))
    p.add_argument("--result-root", default="/Users/lmtsakura/qiyan_shared/result")
    p.add_argument("--material", choices=[*SUPPORTED_MATERIALS, "all"], default="all")
    p.add_argument("--top-n", type=int, default=5)
    p.add_argument("--python", default=None)
    p.add_argument("--out-dir", default=str(root / "reports" / "output" / "tmd_stage2_model_analysis"))
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
    out_root = Path(args.out_dir).expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    python_bin = pick_python(args.python)
    materials = SUPPORTED_MATERIALS if args.material == "all" else (args.material,)

    for material in materials:
        material_out = out_root / material
        material_out.mkdir(parents=True, exist_ok=True)
        data_json = material_out / f"{material}_stage2_model_comparison_data.json"
        run(
            [
                python_bin,
                str(repo_root / "ops" / "extract_tmd_stage2_model_metrics.py"),
                "--repo-root",
                str(repo_root),
                "--result-root",
                str(Path(args.result_root).expanduser().resolve()),
                "--material",
                material,
                "--top-n",
                str(args.top_n),
                "--out-dir",
                str(material_out),
            ],
            repo_root,
        )
        run(
            [
                python_bin,
                str(repo_root / "ops" / "plot_tmd_stage2_model_metrics.py"),
                "--data-json",
                str(data_json),
                "--out-dir",
                str(material_out),
            ],
            repo_root,
        )
    print(out_root)


if __name__ == "__main__":
    main()
