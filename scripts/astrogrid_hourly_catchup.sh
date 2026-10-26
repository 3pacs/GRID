#!/usr/bin/env bash
# Run AstroGrid's own learning/backtest loop on an hourly cadence.
#
# Keep this separate from GRID's hourly catch-up. AstroGrid has its own working
# tree in production and its own model/backtest tables.

set -u

# Symlinks resolved (pwd -P): when cron calls the script through
# /data/grid_v4/grid_release, the whole pass stays on one release even if a
# deploy swaps the link mid-run.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
ASTROGRID_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd -P)"
GRID_ROOT="${ASTROGRID_ROOT}"
export GRID_ROOT

# shellcheck source=grid_cron_env.sh
source "${SCRIPT_DIR}/grid_cron_env.sh"

# Same interpreter as the release systemd units unless PYTHON_BIN/GRID_PYTHON
# says otherwise (this used to default to the old ~/grid_v4/venv).
PYTHON_BIN="${PYTHON_BIN:-$(grid_cron_python)}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="${PYTHON_FALLBACK:-/usr/bin/python3}"
fi

cd "${ASTROGRID_ROOT}" || exit 1

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

run_step "astrogrid_learning_loop_swing" \
    "${PYTHON_BIN}" scripts/run_astrogrid_learning_loop.py \
    --provider-mode deterministic \
    --horizon swing \
    --score-limit "${ASTROGRID_SCORE_LIMIT:-500}" \
    --backtest-limit "${ASTROGRID_BACKTEST_LIMIT:-500}" \
    --backtest-window-days "${ASTROGRID_BACKTEST_WINDOW_DAYS:-365}"

run_step "astrogrid_learning_loop_macro" \
    "${PYTHON_BIN}" scripts/run_astrogrid_learning_loop.py \
    --provider-mode deterministic \
    --horizon macro \
    --score-limit "${ASTROGRID_SCORE_LIMIT:-500}" \
    --backtest-limit "${ASTROGRID_BACKTEST_LIMIT:-500}" \
    --backtest-window-days "${ASTROGRID_BACKTEST_WINDOW_DAYS:-365}"
