#!/usr/bin/env bash
# ============================================================
# GRID cron runner — wraps Python scripts for crontab execution.
#
# Handles: virtualenv activation, working directory, logging,
# and llama-server health checks before LLM tasks.
#
# Usage:
#   grid_cron.sh autoresearch          # run autoresearch loop
#   grid_cron.sh analyst               # run AI analyst briefing
#   grid_cron.sh briefing daily        # generate daily market briefing
#   grid_cron.sh briefing weekly       # generate weekly market briefing
#   grid_cron.sh agents                # run TradingAgents one-shot
#
# All output is logged to ~/grid_v4/logs/cron/
# ============================================================
set -euo pipefail

GRID_ROOT="${GRID_ROOT:-$HOME/grid_v4/grid_repo/grid}"
LOG_DIR="${HOME}/grid_v4/logs/cron"
VENV="${HOME}/grid_v4/venv/bin/activate"

mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
JOB="${1:-help}"

# ── Activate virtualenv if it exists ──────────────────────────
if [[ -f "$VENV" ]]; then
    # shellcheck disable=SC1090
    source "$VENV"
fi

cd "$GRID_ROOT"

# ── Check llama-server is running ─────────────────────────────
check_llm() {
    local url="${LLAMACPP_BASE_URL:-http://localhost:8080}/health"
    if ! curl -sf "$url" >/dev/null 2>&1; then
        echo "[$(date)] ERROR: llama-server not responding at $url" | tee -a "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        echo "Start it with: bash scripts/start_llamacpp.sh"
        exit 1
    fi
}

# ── Job dispatch ──────────────────────────────────────────────
case "$JOB" in
    autoresearch)
        check_llm
        echo "[$(date)] Starting autoresearch..." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        python scripts/autoresearch.py \
            --max-iter "${AUTORESEARCH_MAX_ITER:-5}" \
            --layer "${AUTORESEARCH_LAYER:-REGIME}" \
            >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    analyst)
        check_llm
        echo "[$(date)] Starting AI analyst..." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        python scripts/ai_analyst.py --quiet \
            >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    briefing)
        check_llm
        BTYPE="${2:-daily}"
        echo "[$(date)] Generating $BTYPE briefing..." >> "$LOG_DIR/${JOB}_${BTYPE}_${TIMESTAMP}.log"
        python -c "
from ollama.client import get_client
from ollama.market_briefing import MarketBriefingEngine
from db import get_engine
client = get_client()
engine = get_engine()
mbe = MarketBriefingEngine(client, engine)
result = mbe.generate_briefing(briefing_type='${BTYPE}', save=True)
print(f'Briefing generated: {result.get(\"title\", \"unknown\")}')
" >> "$LOG_DIR/${JOB}_${BTYPE}_${TIMESTAMP}.log" 2>&1
        ;;

    agents)
        check_llm
        echo "[$(date)] Running TradingAgents..." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        python -c "
from agents.runner import run_agent
result = run_agent()
print(f'Agent run complete: {result}')
" >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    help|*)
        echo "Usage: grid_cron.sh {autoresearch|analyst|briefing [daily|weekly]|agents}"
        echo ""
        echo "Jobs:"
        echo "  autoresearch    Autonomous hypothesis generation/refinement loop"
        echo "  analyst         AI analyst daily briefing"
        echo "  briefing TYPE   Market briefing (daily, weekly)"
        echo "  agents          Run TradingAgents one-shot"
        exit 0
        ;;
esac

echo "[$(date)] $JOB completed." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
