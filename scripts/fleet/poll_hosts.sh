#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
HOST_ARGS=()

if [[ -n "${GRID_FLEET_HOSTS:-}" ]]; then
  IFS=',' read -ra HOSTS <<< "${GRID_FLEET_HOSTS}"
  for host in "${HOSTS[@]}"; do
    [[ -n "${host// /}" ]] && HOST_ARGS+=(--host "${host// /}")
  done
fi

exec "${PYTHON:-python3}" "${ROOT}/scripts/fleet_audit.py" \
  "${HOST_ARGS[@]}" \
  --coordinator "${GRID_COORDINATOR_URL:-http://100.75.185.36:8100}" \
  --timeout "${GRID_FLEET_TIMEOUT:-8}" \
  --json-only
