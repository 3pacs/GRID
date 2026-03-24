# GRID Server Configuration — grid-svr

## Quick Reference

| Item | Value |
|------|-------|
| **Server** | grid-svr (Tailscale) |
| **User** | grid |
| **Repo** | ~/grid_v4/grid_repo |
| **Grid code** | ~/grid_v4/grid_repo/grid/ |
| **Config (.env)** | ~/grid_v4/grid_repo/grid/.env |

## Database

| Item | Value |
|------|-------|
| **Engine** | PostgreSQL 14 |
| **Data directory** | /data/postgresql/14/main/ (12TB drive) |
| **Cluster config** | /etc/postgresql/14/main/postgresql.conf |
| **Log** | /var/log/postgresql/postgresql-14-main.log |
| **Host** | localhost |
| **Port** | 5432 |
| **User** | grid |
| **Password** | gridmaster2026 |
| **Database (live)** | **griddb** (212K+ resolved_series, 567 features, 40 sources) |
| **Database (empty)** | grid (schema only — do not use) |

**IMPORTANT:** DB_NAME must be `griddb`, not `grid`.

## Storage

| Mount | Device | Size | Contents |
|-------|--------|------|----------|
| / | /dev/mapper/ubuntu--vg-ubuntu--lv | 98G | OS, code, Python packages |
| /data | /dev/sdc1 | 11T | PostgreSQL data, LLM outputs, logs |
| /boot | /dev/sdb2 | 2G | Boot partition |

## 12TB Drive Symlinks

All LLM outputs and data stored on /data (11T drive):

| Local Path | Symlink Target |
|------------|---------------|
| outputs/llm_insights | /data/grid/outputs/llm_insights |
| outputs/market_briefings | /data/grid/outputs/market_briefings |
| outputs/insight_reviews | /data/grid/outputs/insight_reviews |
| outputs/analyst_reports | /data/grid/outputs/analyst_reports |
| outputs/clustering | /data/grid/outputs/clustering |
| outputs/orthogonality | /data/grid/outputs/orthogonality |
| outputs/backtest | /data/grid/outputs/backtest |
| outputs/paper_trades | /data/grid/outputs/paper_trades |
| API logs | /data/grid/logs/api.log |

## API

| Item | Value |
|------|-------|
| **Start** | `cd ~/grid_v4/grid_repo/grid && nohup python3 -m uvicorn api.main:app --host 0.0.0.0 --port 8000 > /data/grid/logs/api.log 2>&1 &` |
| **Stop** | `kill $(lsof -t -i:8000)` |
| **Health** | `curl -s localhost:8000/api/v1/system/health` |
| **Port** | 8000 |
| **Python** | python3 (3.10, system) |
| **Packages** | ~/.local/lib/python3.10/site-packages/ |
| **Login password** | grid2026 |

## API Keys (in .env)

| Key | Status |
|-----|--------|
| FRED_API_KEY | Set (bc8b4507...) |
| EIA_API_KEY | Set (QAz3bg00...) |
| NOAA_TOKEN | Set (TAbZzkQb...) |
| GRID_JWT_SECRET | Set (auto-generated) |
| GRID_MASTER_PASSWORD_HASH | Set (bcrypt for grid2026) |
| KOSIS_API_KEY | Empty — Korean registration needed |
| COMTRADE_API_KEY | Empty — UN registration needed |
| JQUANTS_EMAIL/PASSWORD | Empty — Japanese registration needed |
| USDA_NASS_API_KEY | Empty — https://quickstats.nass.usda.gov/api |
| GDELT_API_KEY | Not needed — public API |

## Email Alerts

| Item | Value |
|------|-------|
| **Method** | Local postfix (localhost:25) |
| **To** | stepdadfinance@gmail.com |
| **Status** | Postfix installed, may need relay config for Gmail delivery |
| **Fallback** | Switch ALERT_SMTP_HOST to smtp.gmail.com with app password, or use Brevo |

## Other Projects

| Path | Project |
|------|---------|
| ~/grid_v4/Crucix/ | Crucix (has its own .env) |
| ~/grid_v4/scripts/ | Legacy data loading scripts |

## Useful Commands

```bash
# Start API (survives terminal close)
cd ~/grid_v4/grid_repo/grid && nohup python3 -m uvicorn api.main:app --host 0.0.0.0 --port 8000 > /data/grid/logs/api.log 2>&1 &

# Stop API
kill $(lsof -t -i:8000)

# Restart API
kill $(lsof -t -i:8000); sleep 1; cd ~/grid_v4/grid_repo/grid && nohup python3 -m uvicorn api.main:app --host 0.0.0.0 --port 8000 > /data/grid/logs/api.log 2>&1 &

# Health check
curl -s localhost:8000/api/v1/system/health | python3 -m json.tool

# Database shell
PGPASSWORD=gridmaster2026 psql -h localhost -U grid -d griddb

# Count data
PGPASSWORD=gridmaster2026 psql -h localhost -U grid -d griddb -c "SELECT COUNT(*) FROM resolved_series;"

# Apply schema changes
PGPASSWORD=gridmaster2026 psql -h localhost -U grid -d griddb -f ~/grid_v4/grid_repo/grid/schema.sql

# Run daily data pull
cd ~/grid_v4/grid_repo/grid && nohup python3 -c "from ingestion.scheduler import run_daily_pulls; run_daily_pulls()" > /data/grid/logs/daily_pull.log 2>&1 &

# Check pull progress
tail -f /data/grid/logs/daily_pull.log

# Send test email
cd ~/grid_v4/grid_repo/grid && python3 -c "from alerts.email import send_test_email; send_test_email()"

# Pull latest code
cd ~/grid_v4/grid_repo && git fetch origin claude/add-best-practices-JAa0w && git merge origin/claude/add-best-practices-JAa0w

# Check API logs
tail -50 /data/grid/logs/api.log
```
