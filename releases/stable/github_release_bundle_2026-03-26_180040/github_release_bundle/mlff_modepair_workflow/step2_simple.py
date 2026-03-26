#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
SCREEN_SCRIPT = SCRIPT_DIR / "run_pair_screening.py"


def main():
    cmd = [sys.executable, str(SCREEN_SCRIPT), "--backend", "chgnet"]
    subprocess.run(cmd, check=True, text=True)


if __name__ == "__main__":
    main()
