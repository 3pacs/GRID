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
#   grid_cron.sh check                 # preflight only: env, interpreter,
#                                      # imports, LLM endpoint; runs no job
#
# Runs from the tree this script lives in. In production the crontab calls
# /data/grid_v4/grid_release/scripts/grid_cron.sh, so jobs run the deployed
# release, not the old /home/grid/grid_v4/grid_repo checkout (OPS-W1).
# The GRID env file is loaded by scripts/grid_cron_env.sh.
#
# All output is logged to ~/grid_v4/logs/cron/
# ============================================================
set -euo pipefail

GRID_ROOT="${GRID_ROOT:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)}"
export GRID_ROOT
LOG_DIR="${HOME}/grid_v4/logs/cron"

mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
JOB="${1:-help}"

# ── Load the GRID env file (fail closed) and pick the interpreter ─
# shellcheck source=grid_cron_env.sh
source "$GRID_ROOT/scripts/grid_cron_env.sh"
if [[ "$JOB" != "help" ]] && ! grid_cron_load_env 2>>"$LOG_DIR/${JOB}_${TIMESTAMP}.log"; then
    echo "[$(date)] ERROR: $JOB not started: GRID env file not loaded (see $LOG_DIR/${JOB}_${TIMESTAMP}.log)" >&2
    exit 78
fi
GRID_PYTHON_BIN="$(grid_cron_python)"
python() { "$GRID_PYTHON_BIN" "$@"; }

cd "$GRID_ROOT"

# ── Check the reasoning backend is running ────────────────────
# autoresearch/analyst/briefing reason via the Ollama client (ollama/client.py,
# default http://localhost:11434). The legacy llama.cpp :8080 preflight was left
# behind by that migration and aborted every nightly run even though Ollama was
# up. Gate on the endpoint the code actually calls; LLM_HEALTH_URL overrides.
# (Ported from the grid-svr checkout's unpushed 820d0127, which is what the
# production crontab has been running since 2026-07-15.)
llm_health_url() {
    echo "${LLM_HEALTH_URL:-${OLLAMA_BASE_URL:-http://localhost:11434}/api/tags}"
}

check_llm() {
    local url
    url="$(llm_health_url)"
    if ! curl -sf "$url" >/dev/null 2>&1; then
        echo "[$(date)] ERROR: reasoning backend not responding at $url" | tee -a "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        echo "Start it with: ollama serve"
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
from db import get_engine
from agents.runner import AgentRunner
engine = get_engine()
runner = AgentRunner(engine)
result = runner.run()
print(f'Agent run complete: {result}')
" >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    bottom-detector)
        echo "[$(date)] Running Bottom Detector Monitor..." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        python scripts/bottom_detector_monitor.py \
            >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    psi-oracle)
        echo "[$(date)] Running PSI Oracle..." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        python scripts/run_psi_oracle.py --persist \
            >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    sentiment)
        echo "[$(date)] Running sentiment cycle..." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        python -c "
from intelligence.sentiment_scorer import run_sentiment_cycle
from db import get_engine
engine = get_engine()
result = run_sentiment_cycle(engine)
s = result['sentiment']
print(f'Sentiment: {s[\"score\"]:+.3f} ({s[\"label\"]})')
print(f'Scoring: {result[\"scoring\"]}')
" >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    flows)
        echo "[$(date)] Refreshing dollar flows..." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        # GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 W3.3: normalize_all_flows()
        # already persists internally now (and returns a
        # FlowNormalizationSummary, not a bare list), so this calls the
        # single-entry-point script instead of persisting a second time.
        # See scripts/run_dollar_flows.py --dry-run to preview a run first.
        python scripts/run_dollar_flows.py --days 7 \
            >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    paper-review)
        echo "[$(date)] Running paper trading review (P&L + stale trade closer)..." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        python scripts/paper_trading_review.py \
            >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    forecast)
        echo "[$(date)] Running TimesFM signal forecasts..." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        python -c "
from db import get_engine
from inference.timesfm_service import forecast_signals
engine = get_engine()
results = forecast_signals(engine, horizon=30)
up = sum(1 for r in results if r.direction == 'UP')
down = sum(1 for r in results if r.direction == 'DOWN')
flat = sum(1 for r in results if r.direction == 'FLAT')
print(f'Forecasted {len(results)} signals: {up} UP, {down} DOWN, {flat} FLAT')
" >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    thesis-snapshot)
        echo "[$(date)] Running thesis snapshot + scoring cycle..." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        python -c "
from db import get_engine
from analysis.thesis_scorer import score_thesis, snapshot_thesis
from intelligence.thesis_tracker import score_old_theses
engine = get_engine()
# Score current thesis and snapshot it
thesis = score_thesis(engine)
snap_id = snapshot_thesis(engine, thesis)
print(f'Thesis: {thesis[\"direction\"]} score={thesis[\"score\"]:+.1f} conv={thesis[\"conviction\"]}%')
print(f'Snapshot ID: {snap_id}')
# Score any old unscored theses against actual market moves
try:
    scored = score_old_theses(engine)
    print(f'Scored {scored} old theses')
except Exception as e:
    print(f'Old thesis scoring skipped: {e}')
" >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log" 2>&1
        ;;

    check)
        # Preflight for the scheduled jobs (briefing, analyst): no job runs,
        # nothing is written except this log. Fails if any import fails;
        # an unreachable LLM endpoint is reported but does not fail the check,
        # since check_llm already gates each real run.
        {
            echo "[$(date)] CHECK root=$GRID_ROOT python=$GRID_PYTHON_BIN env_file=$GRID_ENV_FILE"
            if curl -sf "$(llm_health_url)" >/dev/null 2>&1; then
                echo "LLM endpoint OK: $(llm_health_url)"
            else
                echo "LLM endpoint NOT responding: $(llm_health_url)"
            fi
        } | tee -a "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        "$GRID_ROOT/scripts/grid_cron_run.sh" --check \
            ollama.client ollama.market_briefing db scripts/ai_analyst.py \
            2>&1 | tee -a "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
        ;;

    help|*)
        echo "Usage: grid_cron.sh {check|autoresearch|analyst|briefing [daily|weekly]|agents|bottom-detector|psi-oracle|thesis-snapshot|flows}"
        echo ""
        echo "Jobs:"
        echo "  check            Preflight only (env, interpreter, imports, LLM endpoint)"
        echo "  autoresearch    Autonomous hypothesis generation/refinement loop"
        echo "  analyst          AI analyst daily briefing"
        echo "  briefing TYPE    Market briefing (daily, weekly)"
        echo "  agents           Run TradingAgents one-shot"
        echo "  bottom-detector  Mega-rally setup scanner (daily)"
        echo "  psi-oracle       PSI timing oracle (daily)"
        echo "  paper-review     Paper trading P&L review + stale trade closer"
        echo "  forecast         TimesFM signal forecasts (30d horizon)"
        echo "  thesis-snapshot  Score thesis and snapshot for accuracy tracking"
        echo "  flows            Refresh dollar flows normalization"
        exit 0
        ;;
esac

echo "[$(date)] $JOB completed." >> "$LOG_DIR/${JOB}_${TIMESTAMP}.log"
