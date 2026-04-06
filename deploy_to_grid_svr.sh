#!/bin/bash
# deploy_to_grid_svr.sh
# Run from your Mac: bash deploy_to_grid_svr.sh
# Copies trial gem hunter files into the correct grid_repo locations via Tailscale

set -e

GRID_HOST="grid@100.75.185.36"
GRID_REPO="~/grid_v4/grid_repo"
LOCAL="$(dirname "$0")"

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
