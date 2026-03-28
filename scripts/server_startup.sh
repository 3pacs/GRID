#!/bin/bash
# GRID Server Startup Script
# Run this after reboot to bring everything online.
# Usage: bash scripts/server_startup.sh

set -e
VENV=~/grid_v4/venv/bin/python3
REPO=~/grid_v4/grid_repo
DB="postgresql://grid:gridmaster2026@localhost:5432/griddb"

echo "=== GRID Server Startup ==="
echo ""

# 1. Add swap if not present
if ! swapon --show | grep -q swapfile; then
    echo "[1/9] Adding 8GB swap..."
    sudo fallocate -l 8G /swapfile 2>/dev/null || true
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile 2>/dev/null || true
    sudo swapon /swapfile
    grep -q swapfile /etc/fstab || echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
    echo "  Swap added."
else
    echo "[1/9] Swap already active."
fi

# 2. Pull latest code
echo "[2/9] Pulling latest code..."
cd $REPO && git pull origin main

# 3. Seed DB tables
echo "[3/9] Seeding database tables..."
psql $DB -f $REPO/schema.sql 2>&1 | grep -c "CREATE" | xargs -I {} echo "  {} tables created/updated"

# 4. Build PWA
echo "[4/9] Building PWA..."
cd $REPO/pwa && npm run build 2>&1 | tail -1

# 5. Restart services
echo "[5/9] Restarting services..."
sudo systemctl restart grid-api grid-hermes grid-llamacpp
sleep 3

# 6. Verify services
echo "[6/9] Checking services..."
for svc in grid-api grid-hermes grid-llamacpp grid-tao-miner grid-crucix; do
    status=$(systemctl is-active $svc 2>/dev/null || echo "inactive")
    echo "  $svc: $status"
done

# 7. Run bulk resolver
echo "[7/9] Running bulk resolver..."
cd $REPO && $VENV scripts/bulk_resolve.py 2>&1 | tail -5

# 8. Run first intelligence cycle
echo "[8/9] Running intelligence cycle..."
cd $REPO && $VENV -c "
from sqlalchemy import create_engine
engine = create_engine('$DB')
try:
    from intelligence.trust_scorer import run_trust_cycle
    print('Trust:', run_trust_cycle(engine))
except Exception as e: print(f'Trust FAILED: {e}')
try:
    from intelligence.cross_reference import run_all_checks
    r = run_all_checks(engine, skip_narrative=True)
    print(f'CrossRef: {len(r.checks)} checks, {len(r.red_flags)} red flags')
except Exception as e: print(f'CrossRef FAILED: {e}')
try:
    from analysis.flow_thesis import generate_unified_thesis
    from intelligence.thesis_tracker import snapshot_thesis
    t = generate_unified_thesis(engine)
    print(f'Thesis: {t[\"overall_direction\"]} conviction {t[\"conviction\"]}')
    snapshot_thesis(engine, t)
except Exception as e: print(f'Thesis FAILED: {e}')
"

# 9. Install watchdog
echo "[9/9] Installing watchdog..."
bash $REPO/scripts/install_watchdog.sh

echo ""
echo "=== GRID Server Online ==="
echo "API: http://localhost:8000/api/v1/system/health"
echo "LLM: http://localhost:8080/health"
echo "PWA: https://grid.stepdad.finance"
