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
GRID_N = 6

RUN_ROOT_NAME = "hex_qgamma_qpair_run"
SCREEN_DIR_NAME = "screening"
JOB_DIR_NAME = "matdyn_job"
EXTRACT_DIR_NAME = "extracted"

FLEIG = "screened_hex_6x6.eig"


SCRIPT_DIR = Path(__file__).resolve().parent
EXTRACT_SCRIPT = SCRIPT_DIR / "extract_screened_eigs.py"


def main():
    work_dir = WORK_DIR.expanduser().resolve()
    run_root = work_dir / RUN_ROOT_NAME
    screening_json = run_root / SCREEN_DIR_NAME / "screening_summary.json"
    eig_file = run_root / JOB_DIR_NAME / FLEIG
    extract_dir = run_root / EXTRACT_DIR_NAME

    cmd = [
        sys.executable,
        str(EXTRACT_SCRIPT),
        "--eig-file",
        str(eig_file),
        "--screening-json",
        str(screening_json),
        "--scf-template",
        str(work_dir / SCF_TEMPLATE),
        "--q-format",
        "auto",
        "--grid-n",
        str(GRID_N),
        "--output-dir",
        str(extract_dir),
    ]
    subprocess.run(cmd, check=True, text=True)
    print(f"extracted to: {extract_dir}")


if __name__ == "__main__":
    main()
