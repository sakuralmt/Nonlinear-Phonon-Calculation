#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


# ============================
# User configuration
# ============================
WORK_DIR = Path.cwd()
SCF_TEMPLATE = "scf.inp"
RUN_ROOT_NAME = "hex_qgamma_qpair_run"

APPLY_SELECTION_RULES = True
GAMMA_OPTICAL_ONLY = True


SCRIPT_DIR = Path(__file__).resolve().parent
SELECT_SCRIPT = SCRIPT_DIR / "select_modes_qgamma_qpair.py"


def main():
    work_dir = WORK_DIR.expanduser().resolve()
    run_root = work_dir / RUN_ROOT_NAME
    output_dir = run_root / "mode_selection"

    cmd = [
        sys.executable,
        str(SELECT_SCRIPT),
        "--run-root",
        str(run_root),
        "--scf-template",
        str(work_dir / SCF_TEMPLATE),
        "--output-dir",
        str(output_dir),
    ]
    if APPLY_SELECTION_RULES:
        cmd.append("--apply-selection-rules")
    if GAMMA_OPTICAL_ONLY:
        cmd.append("--gamma-optical-only")

    subprocess.run(cmd, check=True, text=True)
    print(f"mode selection output: {output_dir}")


if __name__ == "__main__":
    main()
