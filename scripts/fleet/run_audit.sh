#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${GRID_FLEET_AUDIT_DIR:-/tmp/grid-fleet-audit}"
JSON_OUT="${OUT_DIR}/fleet-audit-${STAMP}.json"
MD_OUT="${OUT_DIR}/fleet-audit-${STAMP}.md"

mkdir -p "${OUT_DIR}"

if [[ -f "${HOME}/.config/grid/live-db.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${HOME}/.config/grid/live-db.env"
  set +a
fi

ARGS=(
  --coordinator "${GRID_COORDINATOR_URL:-http://100.75.185.36:8100}"
  --timeout "${GRID_FLEET_TIMEOUT:-8}"
  --output "${JSON_OUT}"
  --markdown "${MD_OUT}"
)

if [[ -n "${GRID_FLEET_HOSTS:-}" ]]; then
  ARGS+=(--host "${GRID_FLEET_HOSTS}")
fi

if [[ "${GRID_FLEET_WRITE_DB:-0}" == "1" ]]; then
  ARGS+=(--write-db)
fi

"${PYTHON:-python3}" "${ROOT}/scripts/fleet_audit.py" "${ARGS[@]}"

if [[ "${GRID_FLEET_REPORT_TO_HUB:-0}" == "1" && -x "${HOME}/scripts/agent_hub/report_to_hub.sh" ]]; then
  "${HOME}/scripts/agent_hub/report_to_hub.sh" fleet-hermes "fleet-audit-${STAMP}" "${MD_OUT}"
fi

printf '%s\n' "${JSON_OUT}"
printf '%s\n' "${MD_OUT}"
