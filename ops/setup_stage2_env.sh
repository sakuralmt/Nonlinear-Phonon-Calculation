#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")"/.. && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
GPTFF_SOURCE="${GPTFF_SOURCE:-}"

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "ERROR: missing required executable: $name" >&2
    exit 1
  fi
}

python_has_module() {
  local module="$1"
  "$PYTHON_BIN" - <<PY >/dev/null 2>&1
import importlib.util, sys
sys.exit(0 if importlib.util.find_spec("$module") is not None else 1)
PY
}

echo "[stage2] using python: $PYTHON_BIN"
require_cmd "$PYTHON_BIN"
"$PYTHON_BIN" -m pip --version >/dev/null

echo "[stage2] installing repository entrypoint"
"$PYTHON_BIN" -m pip install --user --editable "$ROOT_DIR"

echo "[stage2] installing Python dependencies"
"$PYTHON_BIN" -m pip install --user \
  numpy scipy matplotlib ase pymatgen phonopy pandas scikit-learn tqdm psutil torch chgnet

if python_has_module gptff; then
  echo "[stage2] gptff import already available"
elif [[ -n "$GPTFF_SOURCE" && -d "$GPTFF_SOURCE" ]]; then
  echo "[stage2] installing gptff from: $GPTFF_SOURCE"
  "$PYTHON_BIN" -m pip install --user -e "$GPTFF_SOURCE"
else
  echo "ERROR: gptff is not importable." >&2
  echo "Set GPTFF_SOURCE=/path/to/GPTFF and rerun this script, or preinstall gptff manually." >&2
  exit 1
fi

echo "[stage2] verifying imports"
"$PYTHON_BIN" - <<'PY'
import gptff
import chgnet
import torch
import phonopy
import pymatgen
print("gptff import OK")
print("chgnet import OK")
print("torch import OK")
print("phonopy import OK")
print("pymatgen import OK")
PY

echo "[stage2] environment ready"
echo "note: stage2 supports gptff_v1, gptff_v2, and chgnet; default is gptff_v2."
