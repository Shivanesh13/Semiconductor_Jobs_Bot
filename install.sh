#!/usr/bin/env bash
# Create/update .venv and install all dependencies including python-jobspy (JobSpy).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

pick_python() {
  for cmd in python3.12 python3.11 python3.10 python3; do
    if command -v "$cmd" &>/dev/null && "$cmd" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
      echo "$cmd"
      return 0
    fi
  done
  return 1
}

PY="$(pick_python)" || {
  echo "error: need Python 3.10 or newer (required by python-jobspy)." >&2
  echo "  Install: https://www.python.org/downloads/ or: brew install python@3.12" >&2
  exit 1
}

echo "Using: $($PY --version)"

if [[ -d .venv ]] && ! .venv/bin/python -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' 2>/dev/null; then
  echo "Replacing .venv (needs Python 3.10+ for python-jobspy)."
  rm -rf .venv
fi
if [[ ! -d .venv ]]; then
  "$PY" -m venv .venv
fi
# shellcheck source=/dev/null
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo "Done. Activate with: source .venv/bin/activate"
echo "Then run: python bot.py"
