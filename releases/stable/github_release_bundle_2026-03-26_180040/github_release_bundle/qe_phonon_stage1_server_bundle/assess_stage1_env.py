#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stage1_env import assess_stage1_environment


def parse_args():
    parser = argparse.ArgumentParser(description="Assess stage1 QE runtime settings for the current machine.")
    parser.add_argument("--refresh", action="store_true", help="Force re-running the environment assessment.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = assess_stage1_environment(force_refresh=args.refresh)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
