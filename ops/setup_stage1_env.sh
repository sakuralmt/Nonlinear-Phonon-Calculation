#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")"/.. && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "ERROR: missing required executable: $name" >&2
    exit 1
  fi
}

echo "[stage1] using python: $PYTHON_BIN"
require_cmd "$PYTHON_BIN"
"$PYTHON_BIN" -m pip --version >/dev/null

echo "[stage1] installing repository entrypoint"
"$PYTHON_BIN" -m pip install --user --editable "$ROOT_DIR"

echo "[stage1] installing Python dependencies"
"$PYTHON_BIN" -m pip install --user \
  numpy scipy matplotlib ase pymatgen phonopy pandas

echo "[stage1] checking QE and Slurm executables"
require_cmd pw.x
require_cmd ph.x
require_cmd q2r.x
require_cmd matdyn.x
require_cmd sbatch
require_cmd squeue

echo "[stage1] environment ready"
echo "note: stage1 requires a Slurm host with a stable QE phonon frontend."
