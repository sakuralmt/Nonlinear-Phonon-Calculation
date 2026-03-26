#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_MODE="${NPC_INSTALL_MODE:-editable}"

if [[ "${1:-}" == "--wheel" ]]; then
  INSTALL_MODE="wheel"
  shift
fi

if [[ "${1:-}" == "--editable" ]]; then
  INSTALL_MODE="editable"
  shift
fi

cd "$ROOT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "ERROR: python3 is required." >&2
  exit 1
fi

if ! python3 -m pip --version >/dev/null 2>&1; then
  echo "ERROR: python3 -m pip is required." >&2
  exit 1
fi

if [[ "$INSTALL_MODE" == "editable" ]]; then
  python3 -m pip install --user --editable .
else
  python3 -m pip install --user .
fi

USER_BASE="$(python3 -m site --user-base)"
NPC_BIN="$USER_BASE/bin/npc"

echo
echo "Installed npc command."
echo "Command path: $NPC_BIN"
echo "Smoke command: npc"
echo "Uninstall: python3 -m pip uninstall nonlinear-phonon-calculation"

if [[ -n "${PYTHONUSERBASE:-}" ]]; then
  echo "Runtime note: export PYTHONUSERBASE=$PYTHONUSERBASE before running npc from this custom user base."
fi

if command -v pipx >/dev/null 2>&1; then
  echo "Optional isolated install: pipx install ."
fi

case ":$PATH:" in
  *":$USER_BASE/bin:"*)
    ;;
  *)
    echo "PATH note: $USER_BASE/bin is not currently on PATH."
    echo "You can still run: $NPC_BIN"
    ;;
esac
