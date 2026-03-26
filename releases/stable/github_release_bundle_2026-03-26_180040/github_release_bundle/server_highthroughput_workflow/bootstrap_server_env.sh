#!/usr/bin/env bash
set -euo pipefail

# ============================
# User configuration
# ============================
REPO_ROOT="${REPO_ROOT:-$(pwd)}"
CONDA_ROOT="${CONDA_ROOT:-$HOME/miniconda3}"
ENV_NAME="${ENV_NAME:-qiyan-ht}"
RESET_ENV="${RESET_ENV:-0}"
PYTORCH_INDEX_URL="${PYTORCH_INDEX_URL:-https://download.pytorch.org/whl/cpu}"
GPTFF_SOURCE="${GPTFF_SOURCE:-$REPO_ROOT/GPTFF}"

CONDA_BIN="$CONDA_ROOT/bin/conda"
PYTHON_BIN="$CONDA_ROOT/envs/$ENV_NAME/bin/python"
PIP_BIN="$CONDA_ROOT/envs/$ENV_NAME/bin/pip"

if [[ ! -x "$CONDA_BIN" ]]; then
  echo "missing conda: $CONDA_BIN" >&2
  exit 1
fi

if [[ ! -d "$GPTFF_SOURCE" ]]; then
  echo "missing GPTFF source: $GPTFF_SOURCE" >&2
  exit 1
fi

if "$CONDA_BIN" env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
  if [[ "$RESET_ENV" == "1" ]]; then
    "$CONDA_BIN" remove -y -n "$ENV_NAME" --all
  fi
fi

if ! "$CONDA_BIN" env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
  "$CONDA_BIN" create -y -n "$ENV_NAME" python=3.11 --override-channels -c conda-forge
fi

"$CONDA_BIN" install -y -n "$ENV_NAME" --override-channels -c conda-forge \
  numpy scipy matplotlib ase pymatgen phonopy pandas scikit-learn tqdm psutil

"$PIP_BIN" install --no-cache-dir --index-url "$PYTORCH_INDEX_URL" torch==2.6.0
"$PIP_BIN" install --no-cache-dir chgnet==0.4.2
"$PIP_BIN" install --no-cache-dir -e "$GPTFF_SOURCE"

"$PYTHON_BIN" - <<'PY'
from gptff.model.mpredict import ASECalculator
from chgnet.model import CHGNet
import phonopy
import pymatgen
print("gptff import OK")
print("chgnet import OK")
print("phonopy", phonopy.__version__)
print("pymatgen", getattr(pymatgen, "__version__", "unknown"))
PY

echo "environment ready: $ENV_NAME"
