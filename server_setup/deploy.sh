#!/bin/bash
set -e

# GRID deploy script — run from your local machine
# Usage: ./server_setup/deploy.sh user@your-droplet-ip

SERVER=$1
if [ -z "$SERVER" ]; then
    echo "Usage: ./deploy.sh user@server-ip"
    exit 1
fi

echo "=== Building PWA ==="
cd pwa && npm install && npm run build && cd ..

echo "=== Syncing to server ==="
rsync -avz --exclude 'node_modules' --exclude '.git' --exclude '__pycache__' \
    ./ "$SERVER":/opt/grid/

echo "=== Installing Python dependencies ==="
ssh "$SERVER" "cd /opt/grid && python3 -m venv venv && ./venv/bin/pip install -r requirements.txt -r requirements-api.txt"

echo "=== Restarting API service ==="
ssh "$SERVER" "sudo systemctl restart grid-api"

echo "=== Deploy complete ==="
ssh "$SERVER" "sudo systemctl status grid-api --no-pager"
