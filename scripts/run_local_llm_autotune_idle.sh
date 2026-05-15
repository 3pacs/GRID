#!/usr/bin/env bash
# Report-only idle LLM autotune pass for the local GRID fleet.

set -euo pipefail

GRID_DIR="${GRID_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PYTHON_BIN="${PYTHON_BIN:-$GRID_DIR/.venv/bin/python}"
REPORT_DIR="${GRID_LLM_AUTOTUNE_REPORT_DIR:-$GRID_DIR/outputs/local_llm_autotune}"
TIMEOUT="${GRID_LLM_AUTOTUNE_TIMEOUT:-8}"
HOSTS="${GRID_LLM_AUTOTUNE_HOSTS:-grid-svr gridz4 koala redbox z400 panda ocr-node ANIK-PC}"
LOCK_DIR="${TMPDIR:-/tmp}/grid-local-llm-autotune.lock"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "local_llm_autotune: another run is active; exiting"
  exit 0
fi
trap 'rmdir "$LOCK_DIR"' EXIT

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "local_llm_autotune: missing python interpreter: $PYTHON_BIN" >&2
  exit 2
fi

mkdir -p "$REPORT_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
REPORT="$REPORT_DIR/local-llm-autotune-$STAMP.json"

args=(
  "$PYTHON_BIN"
  -m scripts.local_llm_autotune
  --idle-autotune
  --benchmark
  --timeout "$TIMEOUT"
  --report "$REPORT"
)

for host in $HOSTS; do
  args+=(--ssh-host "$host")
done

cd "$GRID_DIR"
exec "${args[@]}"
