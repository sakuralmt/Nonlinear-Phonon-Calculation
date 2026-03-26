#!/usr/bin/env python3
from __future__ import annotations

import json

from step0b_tune_phonon import run_pes_tuning


if __name__ == "__main__":
    print(json.dumps(run_pes_tuning(), indent=2))
