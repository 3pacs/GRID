#!/usr/bin/env bash
# Run AstroGrid's own learning/backtest loop on an hourly cadence.
#
# Keep this separate from GRID's hourly catch-up. AstroGrid has its own working
# tree in production and its own model/backtest tables.

set -u

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ASTROGRID_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/home/grid/grid_v4/venv/bin/python}"

if [[ ! -x "${PYTHON_BIN}" ]]; then
    PYTHON_BIN="${PYTHON_FALLBACK:-/usr/bin/python3}"
fi

cd "${ASTROGRID_ROOT}" || exit 1

if [[ -f ".env" ]]; then
    set -a
    set +u
    # shellcheck disable=SC1091
    source ".env"
    set -u
    set +a
fi

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
