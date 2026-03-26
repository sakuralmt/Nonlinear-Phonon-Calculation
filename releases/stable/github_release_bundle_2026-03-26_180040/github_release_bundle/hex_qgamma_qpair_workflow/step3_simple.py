#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


# ============================
# User configuration
# ============================
WORK_DIR = Path.cwd()
RUN_ROOT_NAME = "hex_qgamma_qpair_run"


SCRIPT_DIR = Path(__file__).resolve().parent
PAIR_SCRIPT = SCRIPT_DIR / "generate_mode_pairs_qgamma_qpair.py"


def main():
    work_dir = WORK_DIR.expanduser().resolve()
    run_root = work_dir / RUN_ROOT_NAME
    output_dir = run_root / "mode_pairs"

    cmd = [
        sys.executable,
        str(PAIR_SCRIPT),
        "--run-root",
        str(run_root),
        "--output-dir",
        str(output_dir),
    ]
    subprocess.run(cmd, check=True, text=True)
    print(f"mode pairs output: {output_dir}")


if __name__ == "__main__":
    main()
