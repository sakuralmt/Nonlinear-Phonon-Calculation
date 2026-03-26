#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
BENCHMARK_SCRIPT = SCRIPT_DIR / "benchmark_golden_pair.py"


def main():
    cmd = [sys.executable, str(BENCHMARK_SCRIPT), "--backend", "chgnet"]
    subprocess.run(cmd, check=True, text=True)


if __name__ == "__main__":
    main()
