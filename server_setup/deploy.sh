#!/bin/bash
set -e

# GRID deploy script — run from project root
# Usage: ./server_setup/deploy.sh [user@server-ip]
# Default: grid@100.75.185.36 (Tailscale)

SERVER=${1:-grid@100.75.185.36}
REMOTE_DIR="/home/grid/grid_v4/grid_repo"

echo "=== Building PWA ==="
cd pwa && npm install && npm run build && cd ..

echo "=== Syncing code to server ==="
rsync -avz --delete \
    --exclude 'node_modules' \
    --exclude '.git' \
    --exclude '__pycache__' \
    --exclude 'pwa/node_modules' \
    --exclude '.env' \
    --exclude '.claude' \
    --exclude '.venv' \
    --exclude 'venv' \
    --exclude '*.pyc' \
    --exclude 'data/' \
    --exclude 'outputs/' \
    --exclude '.mypy_cache' \
    --exclude '.pytest_cache' \
    ./ "$SERVER":"$REMOTE_DIR"/

echo "=== Deploying systemd service + timer files ==="
# Both *.service AND *.timer must land in /etc/systemd/system/. Previously
# the loop only matched *.service, so any timer added under server_setup/
# silently failed to install and the scheduled job never fired.
for unit in server_setup/*.service server_setup/*.timer; do
    [ -e "$unit" ] || continue   # tolerate empty match if no timers exist
    scp "$unit" "$SERVER":/tmp/
    name=$(basename "$unit")
    ssh "$SERVER" "sudo cp /tmp/$name /etc/systemd/system/$name && rm /tmp/$name"
done
ssh "$SERVER" "sudo systemctl daemon-reload"

# Enable any timers we just shipped so they fire on schedule. Idempotent —
# `enable --now` is safe to re-run.
for tmr in server_setup/*.timer; do
    [ -e "$tmr" ] || continue
    name=$(basename "$tmr")
    ssh "$SERVER" "sudo systemctl enable --now $name" || true
done

echo "=== Installing Python dependencies ==="
ssh "$SERVER" "cd $REMOTE_DIR && pip install -r requirements.txt -r requirements-api.txt 2>/dev/null || true"

echo "=== Restarting services ==="
ssh "$SERVER" "sudo systemctl restart grid-api grid-intelligence"

echo "=== Service status ==="
ssh "$SERVER" "sudo systemctl status grid-api grid-intelligence --no-pager -l" || true

echo "=== Deploy complete ==="
