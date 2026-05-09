#!/usr/bin/env bash
# Run GRID audit/backtest commands against the configured live DB env file.

set -euo pipefail

GRID_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="${GRID_AUDIT_ENV_FILE:-$HOME/.config/grid/live-db.env}"

if [[ ! -f "$ENV_FILE" ]]; then
  cat >&2 <<EOF
Missing GRID audit env file: $ENV_FILE

Create it with DB_HOST, DB_PORT, DB_NAME, DB_USER, and DB_PASSWORD.
Keep it outside the repo and chmod 600 it.
EOF
  exit 2
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

export GRID_AUDIT_ENV_FILE="$ENV_FILE"
export PYTHONPATH="$GRID_DIR${PYTHONPATH:+:$PYTHONPATH}"
cd "$GRID_DIR"

exec "$@"
