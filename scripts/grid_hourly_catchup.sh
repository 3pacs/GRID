#!/usr/bin/env bash
# Run GRID's evidence-building jobs on an hourly cadence.
#
# This is intentionally analysis/scoring heavy and narrative light. Cron should
# wrap this script in flock so a slow backlog pass cannot overlap with itself.

set -u

# Symlinks resolved (pwd -P): when cron calls the script through
# /data/grid_v4/grid_release, the whole pass stays on one release even if a
# deploy swaps the link mid-run.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
GRID_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
export GRID_ROOT

# shellcheck source=grid_cron_env.sh
source "${SCRIPT_DIR}/grid_cron_env.sh"

# Same interpreter as the release systemd units unless PYTHON_BIN/GRID_PYTHON
# says otherwise (this used to default to the old ~/grid_v4/venv).
PYTHON_BIN="${PYTHON_BIN:-$(grid_cron_python)}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="${PYTHON_FALLBACK:-/usr/bin/python3}"
fi

cd "${GRID_ROOT}" || exit 1

# The release tree has no .env, so load the GRID env file explicitly and
# stop here if it is missing: every step needs DB settings.
grid_cron_load_env || exit $?

run_step() {
    local name="$1"
    shift
    local start_ts
    start_ts="$(date -Is)"
    echo "[$start_ts] START ${name}"
    "$@"
    local rc=$?
    local end_ts
    end_ts="$(date -Is)"
    if [[ ${rc} -eq 0 ]]; then
        echo "[$end_ts] OK ${name}"
    else
        echo "[$end_ts] FAIL ${name} rc=${rc}"
    fi
    return 0
}

run_inline_python() {
    local name="$1"
    local code="$2"
    run_step "${name}" "${PYTHON_BIN}" -c "${code}"
}

run_step "oracle_trade_scoring" \
    "${PYTHON_BIN}" scripts/score_oracle_trades.py

run_step "baseline_prediction_scoring" \
    "${PYTHON_BIN}" scripts/baseline_predictions.py score

run_inline_python "contagion_backtest_scoring" \
    "from db import get_engine; from intelligence.contagion_backtest import score_all_windows; print(score_all_windows(get_engine()))"

run_step "contagion_feedback" \
    "${PYTHON_BIN}" scripts/run_postmortem_feedback.py --since-hours 48

run_step "dashboard_cache_warm" \
    "${PYTHON_BIN}" scripts/warm_dashboard_cache.py
