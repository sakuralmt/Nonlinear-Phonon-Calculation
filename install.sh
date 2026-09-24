#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"
if [[ "${1:-}" == "--wheel" ]]; then
  python3 -m pip install .
else
  python3 -m pip install --editable .
fi
python3 -m nonlinear_phonon_calculation.cli --help
