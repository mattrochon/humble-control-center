#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
PY_EXE="${VENV_DIR}/bin/python"

if [ ! -x "$PY_EXE" ]; then
  echo "Creating virtualenv at $VENV_DIR"
  python3 -m venv "$VENV_DIR"
fi

echo "Installing project into venv..."
"$PY_EXE" -m pip install --upgrade pip
"$PY_EXE" -m pip install -e "$PROJECT_ROOT"
echo "Done."
