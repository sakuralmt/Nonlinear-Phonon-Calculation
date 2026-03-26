#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! python3 -m build --version >/dev/null 2>&1; then
  python3 -m pip install --user build
fi

if ! python3 -m twine --version >/dev/null 2>&1; then
  python3 -m pip install --user twine
fi

python3 -m build
python3 -m twine check dist/*
