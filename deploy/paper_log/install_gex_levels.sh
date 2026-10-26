#!/usr/bin/env bash
# ============================================================
# Prepare (NOT install/activate) the SPY GEX-levels forward paper log v1
# on grid-svr. See docs/paper_log/gex-levels-v1-preregistration.md and
# deploy/paper_log/README.md before running this.
#
# Run this ON grid-svr, as the `grid` user, after copying a `git archive`
# of the target commit there (see "How the archive gets to grid-svr"
# below, and deploy/paper_log/README.md). This script:
#
#   1. Extracts the archive into /data/grid/paper_log/code/<sha>/
#   2. Writes a VERSION file (the pinned commit SHA) into that directory
#      — paper_log.gex_levels.storage.resolve_code_sha() reads this at
#      run time; every JSONL record's `code_sha` traces back to it.
#   3. Creates /data/grid/paper_log/gex_levels_v1/ (the JSONL log directory)
#   4. Prints — never applies — the crontab block to add
#
# It never touches crontab, never restarts a service, never runs the job,
# never writes to the database (it doesn't even open one). The operator
# reviews the printed block and installs it by hand.
#
# Usage (on grid-svr):
#   ./install_gex_levels.sh <commit-sha>
#
# ── How the archive gets to grid-svr ────────────────────────────────────
# This script does not build or copy the archive itself — it only unpacks
# one that is already sitting next to it, at $GEX_LEVELS_ARCHIVE (default:
# /tmp/gex-levels-<sha>.tar.gz). Build and copy that archive from
# wherever this branch is checked out, BEFORE running this script:
#
#   git -C /path/to/GRID-claude-wt-paper-log archive --format=tar.gz \
#       -o /tmp/gex-levels-<sha>.tar.gz <sha>
#   scp /tmp/gex-levels-<sha>.tar.gz grid-svr:/tmp/gex-levels-<sha>.tar.gz
#   scp deploy/paper_log/install_gex_levels.sh grid-svr:/tmp/
#   ssh grid-svr 'bash /tmp/install_gex_levels.sh <sha>'
#
# `git archive` of a SHA that isn't on origin yet needs to run from a
# checkout that has that commit (e.g. this worktree, before or after
# pushing) — it packages exactly that commit's tree, uncommitted changes
# and .git/ history are never included.
# ============================================================
set -euo pipefail

SHA="${1:?usage: install_gex_levels.sh <commit-sha>}"
ARCHIVE="${GEX_LEVELS_ARCHIVE:-/tmp/gex-levels-${SHA}.tar.gz}"

CODE_ROOT="/data/grid/paper_log/code/${SHA}"
LOG_DIR="/data/grid/paper_log/gex_levels_v1"
VENV_PYTHON="/data/grid_v4/venv/bin/python"
ENV_FILE="/home/grid/grid_v4/grid_repo/.env"
JOB_LOG="${LOG_DIR}/job.log"

if [[ ! -f "$ARCHIVE" ]]; then
    echo "error: archive not found at $ARCHIVE" >&2
    echo "  build it with: git archive --format=tar.gz -o $ARCHIVE $SHA" >&2
    echo "  (from a checkout that has commit $SHA), then scp it here" >&2
    exit 1
fi

echo "== paper_log gex_levels_v1 — install prep for commit ${SHA} =="
echo

mkdir -p "$CODE_ROOT"
tar -xzf "$ARCHIVE" -C "$CODE_ROOT"
echo "$SHA" > "$CODE_ROOT/VERSION"
echo "code extracted:  $CODE_ROOT"
echo "VERSION written: $(cat "$CODE_ROOT/VERSION")"
echo

mkdir -p "$LOG_DIR"
echo "log directory:   $LOG_DIR"
if [[ -f "$LOG_DIR/gex_levels_v1.jsonl" ]]; then
    n=$(wc -l < "$LOG_DIR/gex_levels_v1.jsonl")
    echo "  (already has an existing log: $n record(s) — this install will"
    echo "  APPEND to it, never overwrite it; the first-record prereg_sha256"
    echo "  check only applies to a genuinely empty/new log file.)"
fi
echo

if [[ ! -x "$VENV_PYTHON" ]]; then
    echo "WARNING: $VENV_PYTHON not found or not executable." >&2
    echo "  Confirm the shared venv path before installing the crontab block below." >&2
    echo >&2
fi

if [[ ! -f "$ENV_FILE" ]]; then
    echo "WARNING: $ENV_FILE not found." >&2
    echo "  Confirm the .env path before installing the crontab block below." >&2
    echo >&2
fi

cat <<CRONBLOCK
== Crontab block (PRINTED ONLY — nothing below has been applied to crontab) ==

grid-svr's system TZ is UTC, and its crontab already contains several
other per-block "CRON_TZ=America/Los_Angeles" lines above this one in
sequence. cron applies whichever CRON_TZ line most recently preceded a
given entry, so this block sets its OWN CRON_TZ (America/New_York, to
match the pre-registration's 08:45/16:30 ET schedule) and MUST be
appended at the very end of the crontab — never inserted in the middle —
so no earlier block's CRON_TZ can leak into it, and it can't leak
America/New_York into any block that follows.

Install with \`crontab -e\` (append at the end), or:
  crontab -l > /tmp/crontab.bak.\$(date +%s)
  (crontab -l; cat <<'EOF'
  <paste the block below>
  EOF
  ) | crontab -

---8<--- paper_log gex_levels_v1 (SPY GEX structural levels forward paper log; append at end) ---8<---
CRON_TZ=America/New_York
45 8 * * 1-5 /usr/bin/flock -n /tmp/paper-log-gex-levels-preopen.lock /bin/bash -c 'cd ${CODE_ROOT} && set -a && source ${ENV_FILE} && set +a && PYTHONPATH=${CODE_ROOT} ${VENV_PYTHON} -m paper_log.gex_levels preopen --log-dir ${LOG_DIR}' >> ${JOB_LOG} 2>&1
30 16 * * 1-5 /usr/bin/flock -n /tmp/paper-log-gex-levels-postclose.lock /bin/bash -c 'cd ${CODE_ROOT} && set -a && source ${ENV_FILE} && set +a && PYTHONPATH=${CODE_ROOT} ${VENV_PYTHON} -m paper_log.gex_levels postclose --log-dir ${LOG_DIR}' >> ${JOB_LOG} 2>&1
---8<--- end paper_log gex_levels_v1 ---8<---

Notes on that block:
  - flock -n: if a run is somehow still going when the next one is due,
    the new one skips silently rather than queuing or overlapping — same
    pattern as the rest of grid-svr's crontab (e.g. the hourly-catchup
    entries). preopen and postclose use separate lock files since they
    run ~7h45m apart and do independent work; the job's own JSONL
    append-lock (paper_log.gex_levels.storage) is a second, independent
    guard against concurrent writes to the log file itself.
  - \`source ${ENV_FILE}\` loads DB credentials into the process
    environment inside the bash subshell — nothing in this block ever
    prints/logs .env contents.
  - The job never writes to the database (preopen opens a read-only,
    short-statement-timeout connection only; postclose/status/evaluate
    open no database connection at all) and calls no brokerage/trading
    API — see docs/paper_log/gex-levels-v1-preregistration.md's
    "Integrity" section and this repo's CLAUDE.md PIT rule.
CRONBLOCK

cat <<UNINSTALL

== Uninstall ==
  1. \`crontab -e\` and delete everything between the
     "---8<--- paper_log gex_levels_v1 ... ---8<---" marker lines above
     (including the CRON_TZ=America/New_York line — it belongs only to
     this block).
  2. Confirm no run is in flight:
       ls /tmp/paper-log-gex-levels-*.lock  # should be absent/stale
  3. Code and logs are separate; decide independently:
       rm -rf ${CODE_ROOT}          # this commit's extracted code — safe,
                                     # it's just an archive, re-creatable
                                     # any time from git.
       # DO NOT rm ${LOG_DIR}/gex_levels_v1.jsonl unless you are
       # deliberately abandoning the v1 forward test — it is the
       # permanent, append-only research record the pre-registration
       # commits to keeping ("the log is kept as-is"). Archive it
       # elsewhere first if you truly want it gone from here.

== Done. Nothing was installed into crontab, started, or restarted. ==
UNINSTALL
