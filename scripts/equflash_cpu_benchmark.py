"""Run a 108-atom EquFlash CPU benchmark with isolated diagnostic fallbacks."""

from __future__ import annotations

import os

from scripts.equflash_cpu_naive_probe import install_cpu_compatibility


def main() -> None:
    install_cpu_compatibility(os.environ["EQUFLASH_SOURCE_ROOT"])
    from scripts.benchmark_advanced_stage1_cpu import main as benchmark_main

    benchmark_main()


if __name__ == "__main__":
    main()
