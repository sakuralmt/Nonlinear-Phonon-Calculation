#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"
python_bin="${NPC_INSTALL_PYTHON:-python3}"
if (( $# > 1 )) || [[ "${1:-}" != "" && "${1:-}" != "--wheel" ]]; then
  echo "Usage: NPC_INSTALL_PYTHON=/path/to/python ./install.sh [--wheel]" >&2
  exit 2
fi
"$python_bin" -c 'import sys; sys.version_info >= (3, 10) or sys.exit("Python 3.10+ is required; set NPC_INSTALL_PYTHON to a supported environment.")'
if [[ "${1:-}" == "--wheel" ]]; then
  "$python_bin" -m pip install .
else
  "$python_bin" -m pip install --editable .
fi
# Isolated import cannot accidentally validate the source checkout instead of
# the package installed into the selected environment.
"$python_bin" -I -m nonlinear_phonon_calculation.cli --help
