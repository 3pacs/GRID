# GRID Server Services — Quick Reference

All services run on `grid-svr` (Tailscale: `100.75.185.36`, user: `grid`).

## Services (Boot Order)

| # | Service | Port | Process | Location |
|---|---------|------|---------|----------|
| 1 | **PostgreSQL + TimescaleDB** | 5432 | Docker container `grid_db` | `~/grid_v4/grid_repo/grid/docker-compose.yml` |
| 2 | **llama.cpp (Hermes 8B)** | 8080 | `llama-server` (CUDA, RTX PRO 4000) | `~/grid_v4/grid_repo/grid/vendor/llama.cpp/build/bin/llama-server` |
| 3 | **Crucix** | 3117 | Node.js app | `~/grid_v4/Crucix/` (has own `.env`) |
| 4 | **GRID API (uvicorn)** | 8000 | `python3 -m uvicorn api.main:app` | `~/grid_v4/grid_repo/grid/` |
| 5 | **Hermes Operator** | — | `python3 scripts/hermes_operator.py` | `~/grid_v4/grid_repo/grid/` |

## Start Commands (Manual)

```bash
# 1. Database (auto-restarts via Docker)
cd ~/grid_v4/grid_repo/grid && docker compose up -d

# 2. llama.cpp
cd ~/grid_v4/grid_repo/grid && bash scripts/start_llamacpp.sh &

# 3. Crucix
cd ~/grid_v4/Crucix && node index.js &
# (verify: curl -s http://localhost:3117)

# 4. GRID API
cd ~/grid_v4/grid_repo/grid && python3 -m uvicorn api.main:app --host 0.0.0.0 --port 8000 &

# 5. Hermes Operator (autonomous daemon)
cd ~/grid_v4/grid_repo/grid && python3 scripts/hermes_operator.py &
```

## Health Checks

```bash
pg_isready -U grid -d griddb                           # PostgreSQL
curl -s localhost:8080/health                           # llama.cpp
curl -s localhost:3117                                  # Crucix
curl -s localhost:8000/api/v1/system/health             # GRID API
ps aux | grep hermes_operator                           # Hermes Operator
```

## Stop Commands

```bash
kill $(pgrep -f uvicorn)                  # GRID API
kill $(pgrep -f llama-server)             # llama.cpp
kill $(pgrep -f hermes_operator)          # Hermes Operator
kill $(pgrep -f "node.*Crucix")           # Crucix
cd ~/grid_v4/grid_repo/grid && docker compose down  # PostgreSQL
```

## Restart All

```bash
kill $(pgrep -f uvicorn) $(pgrep -f llama-server) $(pgrep -f hermes_operator) 2>/dev/null
cd ~/grid_v4/grid_repo/grid && docker compose up -d
sleep 2
bash scripts/start_llamacpp.sh &
sleep 5
python3 -m uvicorn api.main:app --host 0.0.0.0 --port 8000 &
sleep 3
python3 scripts/hermes_operator.py &
```

## Systemd Services

Service files in `server_setup/` — install with:
```bash
sudo cp server_setup/grid-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable grid-db grid-api grid-llamacpp grid-hermes grid-crucix
sudo systemctl start grid-db grid-llamacpp grid-crucix grid-api grid-hermes
```

## Key Paths

| Item | Path |
|------|------|
| Repo | `~/grid_v4/grid_repo/grid/` |
| Crucix | `~/grid_v4/Crucix/` |
| Crucix latest.json | `/data/grid_v4/Crucix/runs/latest.json` |
| GGUF Model | `~/grid_v4/grid_repo/grid/models/Hermes-3-Llama-3.1-8B.Q4_K_M.gguf` |
| llama-server binary | `~/grid_v4/grid_repo/grid/vendor/llama.cpp/build/bin/llama-server` |
| API logs | `/data/grid/logs/api.log` |
| Cron logs | `~/grid_v4/logs/cron/` |
| .env | `~/grid_v4/grid_repo/grid/.env` |
| DB credentials | `grid` / `gridmaster2026` / `griddb` on localhost:5432 |

## Celestial/Astro Ingestion

Not a separate service. Five pullers run inside the scheduler (started by GRID API):
- `ingestion/celestial/lunar.py` — Lunar phases
- `ingestion/celestial/vedic.py` — Vedic/Jyotish astrology
- `ingestion/celestial/planetary.py` — Planetary aspects/retrogrades
- `ingestion/celestial/solar.py` — Solar activity/geomagnetic
- `ingestion/celestial/chinese.py` — Chinese calendar cycles

All compute deterministic features from pure math — no external APIs needed.

## Cron Jobs

Installed via `bash scripts/setup_cron.sh`:
- **02:00 weekdays** — Autoresearch
- **06:00 weekdays** — Daily market briefing
- **06:30 weekdays** — AI analyst daily report
- **07:00 Monday** — Weekly market briefing
- **17:00 weekdays** — TradingAgents (if enabled)
