#!/usr/bin/env bash
# ============================================================
# GRID cron setup — installs GRID automation crontab entries.
#
# Cron schedule (all times are server-local):
#
#   Hourly          — Hardened GRID scoring, backtests, feedback, cache warm
#   Hourly          — Separate AstroGrid swing/macro learning loop
#   02:00 weekdays  — Autoresearch (hypothesis generation/refinement)
#   06:00 weekdays  — Daily market briefing
#   06:30 weekdays  — AI analyst daily report
#   07:00 Monday    — Weekly market briefing
#   17:00 weekdays  — TradingAgents run (if AGENTS_SCHEDULE_ENABLED)
#
# Usage:
#   bash scripts/setup_cron.sh           # install cron jobs
#   bash scripts/setup_cron.sh --remove  # remove GRID cron jobs
#   bash scripts/setup_cron.sh --show    # show current GRID cron jobs
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
GRID_ROOT="${GRID_ROOT:-$(cd -- "${SCRIPT_DIR}/.." && pwd)}"
CRON_SCRIPT="${GRID_ROOT}/scripts/grid_cron.sh"
CATCHUP_SCRIPT="${GRID_ROOT}/scripts/grid_hourly_catchup.sh"
ASTROGRID_CATCHUP_SCRIPT="${GRID_ROOT}/scripts/astrogrid_hourly_catchup.sh"
MARKER="# GRID-CRON"

if [[ ! -x "$CRON_SCRIPT" ]]; then
    echo "ERROR: $CRON_SCRIPT not found or not executable"
    exit 1
fi
if [[ ! -f "$CATCHUP_SCRIPT" ]]; then
    echo "ERROR: $CATCHUP_SCRIPT not found"
    exit 1
fi
if [[ ! -f "$ASTROGRID_CATCHUP_SCRIPT" ]]; then
    echo "ERROR: $ASTROGRID_CATCHUP_SCRIPT not found"
    exit 1
fi

ACTION="${1:-install}"

case "$ACTION" in
    --show)
        echo "Current GRID cron jobs:"
        crontab -l 2>/dev/null | grep "$MARKER" || echo "  (none)"
        exit 0
        ;;

    --remove)
        echo "Removing GRID cron jobs..."
        crontab -l 2>/dev/null | grep -v "$MARKER" | crontab - 2>/dev/null || true
        echo "Done. Remaining crontab:"
        crontab -l 2>/dev/null || echo "  (empty)"
        exit 0
        ;;

    install|*)
        ;;
esac

echo "Installing GRID cron jobs..."
echo "GRID_ROOT: $GRID_ROOT"
echo ""

# Build the new cron entries
CRON_ENTRIES=$(cat <<EOF
# --- GRID Hourly Analysis Catch-up ---
0 * * * * cd ${GRID_ROOT} && /usr/bin/flock -n /tmp/grid-hourly-catchup.lock /bin/bash ${CATCHUP_SCRIPT} >> /data/grid/logs/hourly-catchup.log 2>&1 ${MARKER}-hourly-catchup
# --- AstroGrid Hourly Learning Catch-up ---
5 * * * * cd ${GRID_ROOT} && /usr/bin/flock -n /tmp/astrogrid-hourly-catchup.lock /bin/bash ${ASTROGRID_CATCHUP_SCRIPT} >> /data/grid/logs/astrogrid-hourly-catchup.log 2>&1 ${MARKER}-astrogrid-hourly-catchup
# --- GRID LLM Automation ---
0 2 * * 1-5 ${CRON_SCRIPT} autoresearch ${MARKER}-autoresearch
0 6 * * 1-5 ${CRON_SCRIPT} briefing daily ${MARKER}-briefing-daily
30 6 * * 1-5 ${CRON_SCRIPT} analyst ${MARKER}-analyst
0 7 * * 1 ${CRON_SCRIPT} briefing weekly ${MARKER}-briefing-weekly
0 17 * * 1-5 ${CRON_SCRIPT} agents ${MARKER}-agents
EOF
)

# Merge with existing crontab (remove old GRID entries first)
EXISTING=$(crontab -l 2>/dev/null | grep -v "$MARKER" | grep -v "^# --- GRID LLM" | grep -v "^# --- GRID Hourly" | grep -v "^# --- AstroGrid Hourly" || true)

echo "${EXISTING}
${CRON_ENTRIES}" | crontab -

echo "Installed cron jobs:"
echo ""
crontab -l | grep "$MARKER"
echo ""
echo "Logs will be written to: ~/grid_v4/logs/cron/"
echo ""
echo "Prerequisites:"
echo "  1. llama-server must be running: bash scripts/start_llamacpp.sh"
echo "  2. PostgreSQL must be running: docker compose up -d"
echo "  3. .env must have AGENTS_ENABLED=true for agent cron job"
echo ""
echo "Manage:"
echo "  bash scripts/setup_cron.sh --show    # view installed jobs"
echo "  bash scripts/setup_cron.sh --remove  # remove all GRID jobs"
