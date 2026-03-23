#!/usr/bin/env bash
# ============================================================
# GRID — First-run bootstrap script
# Sets up the database, schema, API keys, and verifies readiness.
#
# Usage:
#   cd grid && bash scripts/bootstrap.sh
#
# Idempotent — safe to run multiple times.
# ============================================================
set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[OK]${NC}   $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail()  { echo -e "${RED}[FAIL]${NC} $1"; }

GRID_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$GRID_DIR"

echo "============================================"
echo "  GRID Bootstrap — $(date '+%Y-%m-%d %H:%M')"
echo "============================================"
echo ""

# ── 1. Check .env exists ─────────────────────────────────────
if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        cp .env.example .env
        warn ".env created from .env.example — edit it with your API keys"
    else
        fail "No .env or .env.example found"
        exit 1
    fi
else
    info ".env exists"
fi

# ── 2. Check Python dependencies ─────────────────────────────
echo ""
echo "--- Checking Python dependencies ---"
if python3 -c "import fastapi, sqlalchemy, pandas, loguru" 2>/dev/null; then
    info "Core Python dependencies installed"
else
    warn "Missing Python dependencies — installing..."
    pip install -r requirements.txt 2>&1 | tail -3
fi

# ── 3. Docker / PostgreSQL ────────────────────────────────────
echo ""
echo "--- Checking database ---"
if command -v docker &>/dev/null; then
    if docker ps --format '{{.Names}}' | grep -q grid_db; then
        info "PostgreSQL container running (grid_db)"
    else
        warn "Starting PostgreSQL via docker compose..."
        docker compose up -d
        echo "    Waiting for PostgreSQL to be ready..."
        for i in {1..30}; do
            if docker exec grid_db pg_isready -U grid_user -d grid &>/dev/null; then
                info "PostgreSQL ready"
                break
            fi
            sleep 1
            if [ "$i" -eq 30 ]; then
                fail "PostgreSQL did not become ready in 30s"
                exit 1
            fi
        done
    fi
else
    warn "Docker not found — assuming external PostgreSQL at DB_HOST"
fi

# ── 4. Apply database schema ─────────────────────────────────
echo ""
echo "--- Applying database schema ---"
if python3 db.py 2>&1; then
    info "Database schema applied"
else
    fail "Schema application failed — check DB_HOST/DB_PASSWORD in .env"
    exit 1
fi

# ── 5. Build PWA ─────────────────────────────────────────────
echo ""
echo "--- Building PWA frontend ---"
if [ -d pwa_dist ] && [ -f pwa_dist/index.html ]; then
    info "PWA already built (pwa_dist/index.html exists)"
else
    if command -v npm &>/dev/null; then
        (cd pwa && npm install --silent && npm run build --silent)
        info "PWA built to pwa_dist/"
    else
        warn "npm not found — PWA will not be served (API still works)"
    fi
fi

# ── 6. API key audit ─────────────────────────────────────────
echo ""
echo "--- API key audit ---"
python3 -c "
from config import settings
audit = settings.audit_api_keys()
for key, is_set in sorted(audit.items()):
    status = '\033[0;32m[SET]\033[0m' if is_set else '\033[1;33m[MISSING]\033[0m'
    print(f'  {status}  {key}')
configured = sum(audit.values())
total = len(audit)
print()
print(f'  {configured}/{total} API keys configured')
if configured < total:
    print(f'  Sources with missing keys will degrade gracefully.')
"

# ── 7. LLM check ─────────────────────────────────────────────
echo ""
echo "--- LLM availability ---"
python3 -c "
from llamacpp.client import LlamaCppClient
c = LlamaCppClient()
if c.is_available:
    print('\033[0;32m[OK]\033[0m   llama.cpp server available at ${LLAMACPP_BASE_URL:-http://localhost:8080}')
else:
    print('\033[1;33m[WARN]\033[0m llama.cpp not available — briefings/reasoning disabled')
    print('       Start with: bash scripts/start_llamacpp.sh')
" 2>/dev/null || warn "LLM check failed"

# ── 8. Verify API starts ─────────────────────────────────────
echo ""
echo "--- Verifying API startup ---"
python3 -c "
from api.main import app
from fastapi.testclient import TestClient
client = TestClient(app)
r = client.get('/api/v1/system/health')
if r.status_code == 200:
    print('\033[0;32m[OK]\033[0m   API health check passed — status: ' + r.json()['status'])
else:
    print('\033[0;31m[FAIL]\033[0m API health check returned ' + str(r.status_code))
" 2>/dev/null || warn "API verification failed (expected if DB is not reachable)"

echo ""
echo "============================================"
echo "  Bootstrap complete!"
echo ""
echo "  Start GRID:"
echo "    uvicorn api.main:app --host 0.0.0.0 --port 8000"
echo ""
echo "  Or with systemd:"
echo "    sudo systemctl start grid-api"
echo "============================================"
