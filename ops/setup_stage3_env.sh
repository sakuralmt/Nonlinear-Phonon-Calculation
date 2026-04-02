#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")"/.. && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
STAGE3_MODE="${STAGE3_MODE:-submit_collect}"

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "ERROR: missing required executable: $name" >&2
    exit 1
  fi
}

echo "[stage3] using python: $PYTHON_BIN"
require_cmd "$PYTHON_BIN"
"$PYTHON_BIN" -m pip --version >/dev/null

echo "[stage3] installing repository entrypoint"
"$PYTHON_BIN" -m pip install --user --editable "$ROOT_DIR"

echo "[stage3] installing Python dependencies"
"$PYTHON_BIN" -m pip install --user \
  numpy scipy matplotlib ase pymatgen phonopy pandas

echo "[stage3] checking QE executables"
require_cmd pw.x

if [[ "$STAGE3_MODE" == "submit_collect" ]]; then
  echo "[stage3] checking Slurm executables for submit_collect"
  require_cmd sbatch
  require_cmd squeue
fi

echo "[stage3] environment ready"
if [[ "$STAGE3_MODE" == "prepare_only" ]]; then
  echo "note: Slurm checks were skipped because STAGE3_MODE=prepare_only."
else
  echo "note: submit_collect requires a Slurm host."
fi
