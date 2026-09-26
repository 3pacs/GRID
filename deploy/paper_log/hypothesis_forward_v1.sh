#!/usr/bin/env bash
# ============================================================
# Daily run of the hypothesis-loop forward log v1 (S10).
# Rules: docs/paper_log/hypothesis-forward-v1-preregistration.md
#
# Runs from an immutable `git -c core.autocrlf=false archive` extraction at
# /data/grid/paper_log/code/<sha>/ (VERSION file = that sha), like the GEX
# paper log. Read-only DB (default_transaction_read_only=on, statement
# timeout <= 60 s), reads only through the latest-vintage adapter, appends to
# the hash-chained JSONL and rewrites STATUS.md. No orders, no DB writes, no
# weights, no promotion.
#
# NOT installed by anything in this repo. Proposed crontab line (user grid;
# grid-svr cron ignores CRON_TZ, so the schedule is UTC):
#
#   20 6 * * * /usr/bin/flock -n /tmp/paper-log-hypothesis-forward-v1.lock /bin/bash /data/grid/paper_log/code/<sha>/deploy/paper_log/hypothesis_forward_v1.sh >> /data/grid/paper_log/hypothesis_forward_v1/job.log 2>&1  # GRID-CRON-hypothesis-forward-v1
#
# Uninstall: delete that crontab line. Never delete the JSONL log: it is the
# permanent record (archive it first if v1 is ever abandoned).
# ============================================================
set -euo pipefail

CODE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="${HYPOTHESIS_FORWARD_LOG_DIR:-/data/grid/paper_log/hypothesis_forward_v1}"
ENV_FILE="${GRID_ENV_FILE:-/home/grid/grid_v4/grid_repo/.env}"
VENV_PYTHON="${GRID_VENV_PYTHON:-/data/grid_v4/venv/bin/python}"

if [[ ! -s "${CODE_ROOT}/VERSION" ]]; then
    echo "$(date -u +%FT%TZ) refused: ${CODE_ROOT}/VERSION missing (run only from an installed archive)" >&2
    exit 1
fi

mkdir -p "${LOG_DIR}"
cd "${CODE_ROOT}"
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a
echo "$(date -u +%FT%TZ) hypothesis_forward_v1 run, code $(cat "${CODE_ROOT}/VERSION")"
PYTHONPATH="${CODE_ROOT}" "${VENV_PYTHON}" -m scripts.research_forward_log run --log-dir "${LOG_DIR}"
