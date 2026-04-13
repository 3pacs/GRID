#!/bin/bash
# deploy_to_grid_svr.sh — LEGACY, SUPERSEDED by scripts/deploy.py (2026-04-13).
#
# ⚠️  PREFER `python3 scripts/deploy.py <files...>` for any new deploy work.
# The Python helper does atomic dual-tree write with SHA256 verification,
# optional pre-image snapshot, optional grid-api restart, optional smoke test,
# and writes every run to `.grid_backups/deploy_log.jsonl`. See
# scripts/deploy.py --help for the full flag set.
#
# This bash script is kept for the specific one-shot Trial Gem Hunter install
# flow below, which predates deploy.py and touches DB migrations the generic
# helper doesn't know about. Do NOT add new deploy steps here — call
# deploy.py from your caller instead.
#
# DUAL DEPLOYMENT TREES (read this before editing):
# grid-svr hosts the GRID code at TWO live paths:
#   GRID_REPO_HOME = /home/grid/grid_v4/grid_repo
#       — WorkingDirectory for grid-hermes, grid-extractor, grid-intelligence,
#         grid-realtime, grid-spider, grid-backlinker, grid-breaking-news.
#   GRID_REPO_DATA = /data/grid_v4/astrogrid_dedup
#       — WorkingDirectory for grid-api.
# Both trees are LIVE. Every code-affecting deploy must rsync to BOTH or
# services will silently run stale code. Use the `dual_rsync` helper below
# instead of plain scp/rsync to a single path. Run
# `python3 scripts/verify_deployment_sync.py` after deploy to confirm
# the trees are in sync.

set -e

GRID_HOST="grid@100.75.185.36"
GRID_REPO_HOME="/home/grid/grid_v4/grid_repo"
GRID_REPO_DATA="/data/grid_v4/astrogrid_dedup"
# Back-compat alias for legacy callers below.
GRID_REPO="$GRID_REPO_HOME"
LOCAL="$(dirname "$0")"

# dual_rsync <local_path> <repo_relative_path>
# rsync's the local file/dir into BOTH live trees on grid-svr in one shot.
dual_rsync() {
    local src="$1"
    local rel="$2"
    rsync -az --delete "$src" "$GRID_HOST:$GRID_REPO_HOME/$rel"
    rsync -az --delete "$src" "$GRID_HOST:$GRID_REPO_DATA/$rel"
}

echo "==> Deploying Trial Gem Hunter to grid-svr..."

# 1. Signal module
ssh "$GRID_HOST" "mkdir -p $GRID_REPO/grid/signals $GRID_REPO/grid/ingestors $GRID_REPO/grid/scripts/migrations"
scp "$LOCAL/grid/signals/trial_signal.py"         "$GRID_HOST:$GRID_REPO/grid/signals/"
scp "$LOCAL/grid/ingestors/trial_ingestor.py"     "$GRID_HOST:$GRID_REPO/grid/ingestors/"
scp "$LOCAL/grid/scripts/migrations/add_trial_signals.sql" \
    "$GRID_HOST:$GRID_REPO/grid/scripts/migrations/"

# 2. AutoAgent tasks
ssh "$GRID_HOST" "mkdir -p $GRID_REPO/tasks/trial-gem-hunter/{tests,environment}"
scp "$LOCAL/tasks/trial-gem-hunter/task.toml"              "$GRID_HOST:$GRID_REPO/tasks/trial-gem-hunter/"
scp "$LOCAL/tasks/trial-gem-hunter/instruction.md"         "$GRID_HOST:$GRID_REPO/tasks/trial-gem-hunter/"
scp "$LOCAL/tasks/trial-gem-hunter/tests/test.py"          "$GRID_HOST:$GRID_REPO/tasks/trial-gem-hunter/tests/"
scp "$LOCAL/tasks/trial-gem-hunter/environment/Dockerfile" "$GRID_HOST:$GRID_REPO/tasks/trial-gem-hunter/environment/"

# 3. CLAUDE.md to repo root
scp "$LOCAL/CLAUDE.md" "$GRID_HOST:$GRID_REPO/CLAUDE.md"

# 4. Apply DB migration
echo "==> Applying DB migration..."
ssh "$GRID_HOST" "psql -U grid -d griddb -f $GRID_REPO/grid/scripts/migrations/add_trial_signals.sql"

# 5. Seed initial data
echo "==> Seeding trial cache..."
ssh "$GRID_HOST" "cd $GRID_REPO && source .env 2>/dev/null; python3 -m grid.ingestors.trial_ingestor"

# 6. Add cron job (idempotent)
echo "==> Adding cron job..."
ssh "$GRID_HOST" '(crontab -l 2>/dev/null | grep -v trial_ingestor; echo "0 6 * * * cd ~/grid_v4/grid_repo && python -m grid.ingestors.trial_ingestor >> /var/log/grid/trial_ingestor.log 2>&1") | crontab -'

echo ""
echo "✓ Deployed. Test with:"
echo "  ssh grid@100.75.185.36"
echo "  cd ~/grid_v4/grid_repo"
echo "  python -m grid.signals.trial_signal --output table"
echo ""
echo "  To run AutoAgent:"
echo "  uv run harbor run -p tasks/ --task-name trial-gem-hunter -l 1 -n 1 \\"
echo "    --agent-import-path agent:AutoAgent -o jobs --job-name latest"
