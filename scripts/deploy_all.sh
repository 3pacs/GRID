#!/usr/bin/env bash
# GRID — Master Deployment Script
# Runs taxonomy, parsers, pullers, starts coordinator, registers server as worker, adds crons.
#
# Usage: cd ~/grid_v4 && source venv/bin/activate && bash grid_repo/grid/scripts/deploy_all.sh

set -euo pipefail

GRID_DIR="${GRID_DIR:-$HOME/grid_v4/grid_repo/grid}"
SCRIPTS="$GRID_DIR/scripts"
VENV="${VENV:-$HOME/grid_v4/venv}"
LOG_DIR="/data/grid/logs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $1"; }
ok()  { echo -e "${GREEN}  ✓${NC} $1"; }
err() { echo -e "${RED}  ✗${NC} $1"; }
warn(){ echo -e "${YELLOW}  !${NC} $1"; }

# ── Preflight ──────────────────────────────────────────────────

log "GRID Deploy — $(date)"
log "Grid dir: $GRID_DIR"

# Ensure venv is active
if [ -z "${VIRTUAL_ENV:-}" ]; then
    log "Activating venv..."
    source "$VENV/bin/activate"
fi

cd "$GRID_DIR"

# Ensure log dir
mkdir -p "$LOG_DIR"

# Check PostgreSQL
if ! pg_isready -q 2>/dev/null; then
    err "PostgreSQL is not running"
    exit 1
fi
ok "PostgreSQL is running"

# Check required Python packages
python3 -c "import psycopg2, requests, numpy, pandas, sklearn, fastapi, uvicorn" 2>/dev/null || {
    warn "Installing missing packages..."
    pip install psycopg2-binary requests numpy pandas scikit-learn==1.5.2 fastapi uvicorn yfinance pyarrow
}
ok "Python packages verified"

# ── Step 1: Signal Taxonomy ────────────────────────────────────

log "Step 1: Applying signal taxonomy..."
python3 "$SCRIPTS/signal_taxonomy.py" 2>&1 | tee "$LOG_DIR/signal_taxonomy.log"
ok "Signal taxonomy applied"

# ── Step 2: Parse EDGAR ────────────────────────────────────────

if [ -d "/data/grid/bulk/edgar" ] && ls /data/grid/bulk/edgar/*.zip 1>/dev/null 2>&1; then
    log "Step 2: Parsing EDGAR XBRL data..."
    python3 "$SCRIPTS/parse_edgar.py" 2>&1 | tee "$LOG_DIR/parse_edgar.log"
    ok "EDGAR parsed"
else
    warn "Step 2: Skipping EDGAR — no bulk data at /data/grid/bulk/edgar/"
fi

# ── Step 3: Parse EIA ──────────────────────────────────────────

log "Step 3: Parsing EIA energy data..."
python3 "$SCRIPTS/parse_eia.py" 2>&1 | tee "$LOG_DIR/parse_eia.log"
ok "EIA parsed"

# ── Step 4: Parse GDELT ────────────────────────────────────────

if [ -d "/data/grid/bulk/gdelt" ] && ls /data/grid/bulk/gdelt/*.{zip,csv,CSV} 1>/dev/null 2>&1; then
    log "Step 4: Parsing GDELT events..."
    python3 "$SCRIPTS/parse_gdelt.py" 2>&1 | tee "$LOG_DIR/parse_gdelt.log"
    ok "GDELT parsed"
else
    warn "Step 4: Skipping GDELT — no bulk data at /data/grid/bulk/gdelt/"
fi

# ── Step 5: Pull Options Chains ────────────────────────────────

log "Step 5: Pulling options chains..."
python3 "$SCRIPTS/pull_options.py" 2>&1 | tee "$LOG_DIR/pull_options.log"
ok "Options pulled"

# ── Step 6: Pull Intraday Data ─────────────────────────────────

log "Step 6: Pulling intraday bars..."
python3 "$SCRIPTS/pull_intraday.py" 2>&1 | tee "$LOG_DIR/pull_intraday.log"
ok "Intraday data pulled"

# ── Step 7: Start Compute Coordinator ──────────────────────────

log "Step 7: Starting compute coordinator on :8100..."

# Kill existing coordinator if running
if lsof -ti:8100 >/dev/null 2>&1; then
    warn "Killing existing process on :8100"
    kill $(lsof -ti:8100) 2>/dev/null || true
    sleep 1
fi

nohup python3 -m uvicorn scripts.compute_coordinator:app --host 0.0.0.0 --port 8100 \
    >> "$LOG_DIR/compute_coordinator.log" 2>&1 &
COORD_PID=$!
sleep 2

if kill -0 "$COORD_PID" 2>/dev/null; then
    ok "Compute coordinator started (PID: $COORD_PID)"
else
    err "Compute coordinator failed to start — check $LOG_DIR/compute_coordinator.log"
fi

# ── Step 8: Register Server as Worker ──────────────────────────

log "Step 8: Registering server as compute worker..."
sleep 2  # give coordinator time to initialize

# Worker auto-registers via worker.py, start it in background
nohup python3 "$SCRIPTS/worker.py" --coordinator http://localhost:8100 \
    >> "$LOG_DIR/worker.log" 2>&1 &
WORKER_PID=$!
sleep 2

if kill -0 "$WORKER_PID" 2>/dev/null; then
    ok "Worker started (PID: $WORKER_PID)"
else
    warn "Worker may have failed — check $LOG_DIR/worker.log"
fi

# ── Step 9: Add Cron Entries ───────────────────────────────────

log "Step 9: Adding cron entries..."

# Only add if not already present
CRON_MARKER="# GRID_DEPLOY_MANAGED"
EXISTING_CRON=$(crontab -l 2>/dev/null || echo "")

add_cron() {
    local schedule="$1"
    local cmd="$2"
    local comment="$3"
    if ! echo "$EXISTING_CRON" | grep -qF "$cmd"; then
        EXISTING_CRON="$EXISTING_CRON
$schedule cd $GRID_DIR && source $VENV/bin/activate && $cmd >> $LOG_DIR/cron.log 2>&1 $CRON_MARKER # $comment"
        ok "Added cron: $comment"
    else
        warn "Cron already exists: $comment"
    fi
}

add_cron "30 19 * * 1-5" "python3 scripts/pull_options.py" "Daily options pull (12:30 Pacific)"
add_cron "0 20 * * 1-5" "python3 scripts/pull_intraday.py" "Daily intraday pull (1pm Pacific)"

echo "$EXISTING_CRON" | crontab -
ok "Cron entries updated"

# ── Final Stats ────────────────────────────────────────────────

log "═══════════════════════════════════════════"
log "GRID Deploy Complete"
log "═══════════════════════════════════════════"

python3 -c "
import psycopg2, os
c = psycopg2.connect(
    host=os.environ.get('GRID_DB_HOST', 'localhost'),
    port=int(os.environ.get('GRID_DB_PORT', 5432)),
    dbname=os.environ.get('GRID_DB_NAME', 'griddb'),
    user=os.environ.get('GRID_DB_USER', 'grid'),
    password=os.environ.get('GRID_DB_PASSWORD', ''),
)
cur = c.cursor()
cur.execute('SELECT count(*) FROM resolved_series')
print(f'  resolved_series:  {cur.fetchone()[0]:>10,} rows')
cur.execute('SELECT count(DISTINCT feature_id) FROM resolved_series')
print(f'  features w/ data: {cur.fetchone()[0]:>10,}')
cur.execute('SELECT count(*) FROM feature_registry')
print(f'  feature_registry: {cur.fetchone()[0]:>10,}')
cur.execute('SELECT count(*) FROM source_catalog')
print(f'  source_catalog:   {cur.fetchone()[0]:>10,}')

# Check new tables
for t in ['edgar_submissions','edgar_numeric','gdelt_events','gdelt_daily_summary',
          'options_snapshots','options_daily_signals','compute_jobs','compute_workers']:
    try:
        cur.execute(f'SELECT count(*) FROM {t}')
        print(f'  {t+\":\":<25} {cur.fetchone()[0]:>10,} rows')
    except:
        c.rollback()
        print(f'  {t+\":\":<25} not created')
c.close()
"

# Check coordinator health
if curl -sf http://localhost:8100/health >/dev/null 2>&1; then
    ok "Compute coordinator: healthy"
else
    warn "Compute coordinator: not responding"
fi

# Disk usage
echo ""
log "Disk usage:"
du -sh /data/grid/bulk/*/ 2>/dev/null || true
du -sh /data/grid/intraday/ 2>/dev/null || true
du -sh /data/grid/logs/ 2>/dev/null || true
echo ""
df -h /data | tail -1

log "Done. All services running."
