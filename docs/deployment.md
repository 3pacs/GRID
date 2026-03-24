# GRID Production Deployment Guide

This guide covers deploying GRID to a production environment. GRID is a single-operator system designed to run on a dedicated Linux server (DigitalOcean Droplet recommended).

---

## Prerequisites

- Ubuntu 24.04 LTS (or equivalent Debian-based distro)
- Python 3.11+
- Docker and Docker Compose (for PostgreSQL + TimescaleDB)
- Node.js 18+ and npm (for PWA build)
- A domain name with DNS pointing to the server IP (required for HTTPS/PWA)

### Hardware Requirements

| Tier | vCPU | RAM | Disk | Cost | Notes |
|------|------|-----|------|------|-------|
| Minimum (no LLM) | 2 | 4 GB | 80 GB SSD | ~$24/mo | Data ingestion + API only |
| Recommended | 4 | 8 GB | 160 GB SSD | ~$48/mo | Includes llama.cpp inference |
| GPU (research) | 4+ | 16+ GB | 200+ GB | $500+/mo | Only if Hyperspace research earnings justify it |

---

## Environment Variables

All configuration is managed through environment variables loaded by `config.py` via pydantic-settings. Copy `.env.example` to `.env` and edit.

### Required in Production

These variables **must** be set when `ENVIRONMENT=production`. The application will refuse to start without them.

| Variable | Description | How to Generate |
|----------|-------------|-----------------|
| `ENVIRONMENT` | Must be `production` | Set to `production` |
| `DB_PASSWORD` | PostgreSQL password | Choose a strong password (not `changeme`) |
| `FRED_API_KEY` | FRED API key | Register at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) |
| `GRID_JWT_SECRET` | JWT signing secret | `python -c "import secrets; print(secrets.token_urlsafe(64))"` |
| `GRID_MASTER_PASSWORD_HASH` | Bcrypt hash of login password | `python -c "from passlib.context import CryptContext; print(CryptContext(schemes=['bcrypt']).hash('your-password'))"` |
| `GRID_ALLOWED_ORIGINS` | Comma-separated CORS origins | e.g. `https://grid.yourdomain.com` |

### Database

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL hostname |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `grid` | Database name |
| `DB_USER` | `grid_user` | Database user |
| `DB_PASSWORD` | `changeme` | Database password (rejected in production) |

The connection URL is constructed automatically as `postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}`.

### Authentication

| Variable | Default | Description |
|----------|---------|-------------|
| `GRID_JWT_SECRET` | _(empty)_ | JWT signing secret (required in production) |
| `GRID_MASTER_PASSWORD_HASH` | _(empty)_ | Bcrypt hash for login |
| `GRID_JWT_EXPIRE_HOURS` | `168` | Token expiry (7 days) |
| `GRID_ALLOWED_ORIGINS` | `*` | CORS allowed origins (comma-separated) |

### API Keys (Optional -- Graceful Degradation)

Data sources with missing keys will be skipped. The system logs warnings at startup for unconfigured keys.

| Variable | Source |
|----------|--------|
| `KOSIS_API_KEY` | Korea Statistics (KOSIS) |
| `COMTRADE_API_KEY` | UN Comtrade |
| `JQUANTS_EMAIL` | J-Quants (Japan) |
| `JQUANTS_PASSWORD` | J-Quants (Japan) |
| `USDA_NASS_API_KEY` | USDA NASS |
| `NOAA_TOKEN` | NOAA Climate Data |
| `EIA_API_KEY` | US Energy Information Administration |
| `GDELT_API_KEY` | GDELT Project |

### LLM Integration

**llama.cpp (recommended):**

| Variable | Default | Description |
|----------|---------|-------------|
| `LLAMACPP_BASE_URL` | `http://localhost:8080` | llama.cpp server URL |
| `LLAMACPP_ENABLED` | `true` | Enable llama.cpp inference |
| `LLAMACPP_TIMEOUT_SECONDS` | `120` | Request timeout |
| `LLAMACPP_CHAT_MODEL` | `hermes` | Chat model name |
| `LLAMACPP_EMBED_MODEL` | `hermes` | Embedding model name |

**Hyperspace P2P:**

| Variable | Default | Description |
|----------|---------|-------------|
| `HYPERSPACE_BASE_URL` | `http://localhost:8080/v1` | Hyperspace API URL |
| `HYPERSPACE_ENABLED` | `true` | Enable Hyperspace |
| `HYPERSPACE_TIMEOUT_SECONDS` | `30` | Request timeout |

**Ollama (deprecated -- use llama.cpp):**

| Variable | Default | Description |
|----------|---------|-------------|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama API URL |
| `OLLAMA_ENABLED` | `false` | Disabled by default |

### TradingAgents

| Variable | Default | Description |
|----------|---------|-------------|
| `AGENTS_ENABLED` | `false` | Enable multi-agent debate |
| `AGENTS_LLM_PROVIDER` | `llamacpp` | LLM backend: `llamacpp`, `hyperspace`, `openai`, `anthropic` |
| `AGENTS_LLM_MODEL` | `auto` | Model name |
| `AGENTS_OPENAI_API_KEY` | _(empty)_ | Required if provider is `openai` |
| `AGENTS_ANTHROPIC_API_KEY` | _(empty)_ | Required if provider is `anthropic` |
| `AGENTS_DEBATE_ROUNDS` | `1` | Number of debate rounds |
| `AGENTS_DEFAULT_TICKER` | `SPY` | Default ticker for analysis |
| `AGENTS_SCHEDULE_ENABLED` | `false` | Enable scheduled agent runs |
| `AGENTS_SCHEDULE_CRON` | `0 17 * * 1-5` | Agent schedule (weekdays 5 PM) |
| `AGENTS_BACKTEST_MAX_DAYS` | `365` | Max backtest history |

### Scheduling

| Variable | Default | Description |
|----------|---------|-------------|
| `PULL_SCHEDULE_FRED` | `0 18 * * 1-5` | FRED data pull (weekdays 6 PM) |
| `PULL_SCHEDULE_YFINANCE` | `30 18 * * 1-5` | yfinance pull (weekdays 6:30 PM) |
| `PULL_SCHEDULE_BLS` | `0 9 * * *` | BLS data pull (daily 9 AM) |
| `AUTORESEARCH_ENABLED` | `true` | Enable autonomous research loop |
| `AUTORESEARCH_CRON` | `0 2 * * 1-5` | Research schedule (weekdays 2 AM) |
| `BRIEFING_CRON_DAILY` | `0 6 * * 1-5` | Daily market briefing |
| `BRIEFING_CRON_WEEKLY` | `0 7 * * 1` | Weekly market briefing |

### Miscellaneous

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Logging verbosity: DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `GRID_MAX_BODY_BYTES` | `10485760` | Max request body size (10 MB) |

---

## Database Setup

GRID requires PostgreSQL 15 with the TimescaleDB extension. SQLite and MySQL are not supported (the PIT query engine uses `DISTINCT ON`, `MAKE_INTERVAL`, array types, and partial indexes).

### Using Docker Compose (recommended)

```bash
cd grid
docker compose up -d
```

This starts a `timescale/timescaledb:latest-pg15` container with:
- Database: `grid`
- User: `grid_user`
- Password: from `POSTGRES_PASSWORD` in `docker-compose.yml` (change this)
- Port: 5432
- Data persisted in a Docker volume `grid_pgdata`

### Apply Schema

```bash
cd grid
python db.py
```

This reads `schema.sql` and creates all tables, indexes, and constraints.

### Database Migrations

Alembic is configured for schema migrations:

```bash
cd grid
alembic upgrade head        # Apply all pending migrations
alembic revision -m "desc"  # Create a new migration
```

### Connection Pool

The SQLAlchemy engine is configured with:
- `pool_size=5` -- base connections
- `max_overflow=10` -- burst connections (15 total max)
- `pool_timeout=30` -- wait time for a connection
- `pool_pre_ping=True` -- verify connections before use

For higher load, increase these values or make them configurable via environment variables (see ATTENTION.md item 59).

### Backups

PostgreSQL backups using `pg_dump`:

```bash
# Daily logical backup
pg_dump -U grid_user -d grid -Fc -f /backups/grid_$(date +%Y%m%d).dump

# Restore
pg_restore -U grid_user -d grid -c /backups/grid_20260324.dump
```

Automate with a cron job:

```cron
0 3 * * * pg_dump -U grid_user -d grid -Fc -f /backups/grid_$(date +\%Y\%m\%d).dump && find /backups -name "grid_*.dump" -mtime +30 -delete
```

For the Docker setup, run pg_dump inside the container:

```bash
docker exec grid_db pg_dump -U grid_user -d grid -Fc > /backups/grid_$(date +%Y%m%d).dump
```

---

## Application Setup

### Install Python Dependencies

```bash
cd grid
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Build the PWA

```bash
cd grid/pwa
npm install
npm run build
```

The build output goes to `pwa_dist/`. FastAPI serves it automatically -- the `api/main.py` static file handler looks for `pwa_dist/` first, then `pwa/`.

### Start the API

```bash
cd grid
source venv/bin/activate
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 2
```

For production, use `--workers` based on CPU count (2-4 typical). Do not use `--reload` in production.

---

## Systemd Service

Create `/etc/systemd/system/grid-api.service`:

```ini
[Unit]
Description=GRID Intelligence API
After=network.target docker.service
Requires=docker.service

[Service]
Type=simple
User=grid
Group=grid
WorkingDirectory=/home/grid/grid_v4
Environment="PATH=/home/grid/grid_v4/venv/bin:/usr/bin:/bin"
EnvironmentFile=/home/grid/grid_v4/.env
ExecStart=/home/grid/grid_v4/venv/bin/uvicorn api.main:app \
    --host 127.0.0.1 \
    --port 8000 \
    --workers 2 \
    --log-level warning \
    --limit-max-requests 10000
Restart=always
RestartSec=5
StandardOutput=append:/var/log/grid-api.log
StandardError=append:/var/log/grid-api.log

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable grid-api
sudo systemctl start grid-api
sudo systemctl status grid-api
```

---

## Reverse Proxy

### Caddy (recommended -- automatic HTTPS)

Install Caddy and create `/etc/caddy/Caddyfile`:

```
grid.yourdomain.com {
    reverse_proxy localhost:8000
    encode gzip

    header {
        Strict-Transport-Security "max-age=63072000; includeSubDomains"
        X-Content-Type-Options nosniff
        X-Frame-Options DENY
        Referrer-Policy strict-origin-when-cross-origin
        Permissions-Policy "camera=(), geolocation=(), microphone=()"
    }
}
```

Caddy handles Let's Encrypt certificates automatically. Reload with `sudo systemctl reload caddy`.

### nginx (alternative)

```nginx
server {
    listen 80;
    server_name grid.yourdomain.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name grid.yourdomain.com;

    ssl_certificate     /etc/letsencrypt/live/grid.yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/grid.yourdomain.com/privkey.pem;

    # Security headers (in addition to those set by FastAPI)
    add_header Strict-Transport-Security "max-age=63072000; includeSubDomains" always;

    # Proxy to GRID API
    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Request body limit (matches GRID_MAX_BODY_BYTES)
        client_max_body_size 10M;
    }

    # WebSocket
    location /ws {
        proxy_pass http://127.0.0.1:8000/ws;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 300s;
    }
}
```

Obtain certificates with certbot:

```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d grid.yourdomain.com
```

---

## Security Checklist

Before going live, verify every item:

- [ ] **`ENVIRONMENT=production`** -- Enables all production validators
- [ ] **`DB_PASSWORD`** changed from `changeme` -- Rejected by config validator in production
- [ ] **`GRID_JWT_SECRET`** set to a random 64+ character string -- Rejected if empty or default in production
- [ ] **`GRID_MASTER_PASSWORD_HASH`** set -- Generated with bcrypt
- [ ] **`GRID_ALLOWED_ORIGINS`** set to your domain(s) -- Not `*` in production
- [ ] **Docker Compose password** changed in `docker-compose.yml` -- The `POSTGRES_PASSWORD` must match `DB_PASSWORD`
- [ ] **Firewall configured** -- Only ports 22 (SSH), 80, 443 exposed; port 5432 and 8000 internal only
- [ ] **API docs disabled** -- Swagger UI (`/api/docs`) is automatically disabled when `ENVIRONMENT != development`
- [ ] **Request body limit** -- Default 10 MB via `GRID_MAX_BODY_BYTES` middleware (prevent OOM)
- [ ] **Security headers active** -- Verified in production: HSTS, CSP, X-Frame-Options, X-Content-Type-Options
- [ ] **WebSocket auth** -- Uses first-message auth pattern (tokens not in URLs)
- [ ] **Rate limiting** -- Login attempts limited to 5 per 60 seconds (persisted to disk via shelve)

### Firewall Setup (ufw)

```bash
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow 22/tcp    # SSH
sudo ufw allow 80/tcp    # HTTP (redirect to HTTPS)
sudo ufw allow 443/tcp   # HTTPS
sudo ufw enable
```

---

## Monitoring

### Health Endpoint

`GET /api/v1/system/health` (no authentication required)

Returns `{"status": "ok"}`, `{"status": "degraded"}`, or checks for:
- Database connectivity
- Feature registry population
- Data freshness (pulls within 7 days)
- Connection pool health
- Scheduler thread liveness (`ingestion`, `agent-scheduler`)
- WebSocket client count
- Disk space
- LLM availability
- API key configuration

Use this for uptime monitoring (e.g., UptimeRobot, Healthchecks.io).

### Alerts Endpoint

`GET /api/v1/system/alerts` (authentication required)

Returns active alerts for:
- Disk usage (warning > 80%, critical > 90%)
- CPU temperature (warning > 75C, critical > 85C)
- Memory usage (warning > 85%, critical > 95%)
- Load average (warning > CPU count, critical > 2x CPU count)

### System Status

`GET /api/v1/system/status` (authentication required)

Comprehensive dashboard data: database size, feature counts, model states, journal entries, Hyperspace status, server metrics (CPU, memory, disk, temperature, load average).

### Log Access

`GET /api/v1/system/logs?source=api&lines=50` (authentication required)

Sources: `api` (`/var/log/grid-api.log`), `hyperspace`, `system` (syslog).

### Server-Log Git Sink

ERROR-level and above log messages are automatically written to `.server-logs/errors.jsonl` and committed via git. This provides a persistent audit trail of errors.

---

## Known Production Issues

These are documented in `ATTENTION.md` and should be addressed based on priority:

### Critical

- **Request body size limit** (item 47) -- Handled by `RequestSizeLimitMiddleware` (default 10 MB). Also set `--limit-max-requests` on uvicorn.
- **WebSocket idle eviction** (item 58) -- Idle clients are evicted after 5 minutes. The broadcast loop runs every 10 seconds.
- **Graceful shutdown** (item 50) -- Shutdown handler flushes git sink, closes WebSocket connections, disposes database engine.

### High Priority

- **Connection pool sizing** (item 59) -- Default pool of 5+10 overflow may be too small under load. Monitor `pool_checked_out` in health endpoint.
- **LLM insight file rotation** (item 49) -- Files in `outputs/llm_insights/` grow unbounded. Implement cleanup for files older than 90 days.
- **Error log rotation** (item 61) -- `errors.jsonl` and market briefings accumulate indefinitely. Add retention policy.
- **Monthly scheduler fragility** (item 62) -- If server is down on the 5th of the month, monthly pulls are missed entirely.
- **`on_event` deprecation** (item 51) -- FastAPI startup/shutdown events should migrate to lifespan handlers.

### Medium Priority

- **CORS origin validation** (item 64) -- If `GRID_ALLOWED_ORIGINS` is not set in production, only localhost is allowed (safe but confusing).
- **No dependency lock file** (item 33) -- `requirements.txt` uses minimum versions. Consider `pip freeze > requirements.lock` for reproducible builds.
- **Admin endpoints lack pagination** (item 48) -- `model_registry`, `source_catalog`, `feature_registry`, `hypothesis_registry` queries have no LIMIT.

---

## Deployment Checklist

1. Provision server (2+ vCPU, 4+ GB RAM, 80+ GB SSD)
2. Install Docker, Python 3.11, Node.js 18
3. Clone repository to `/home/grid/grid_v4`
4. Copy `.env.example` to `.env`, configure all required variables
5. Change password in `docker-compose.yml` to match `DB_PASSWORD`
6. Start database: `docker compose up -d`
7. Apply schema: `python db.py`
8. Install Python deps: `pip install -r requirements.txt`
9. Build PWA: `cd pwa && npm install && npm run build`
10. Install and configure reverse proxy (Caddy or nginx)
11. Create and enable systemd service
12. Configure firewall (ufw)
13. Verify health: `curl https://grid.yourdomain.com/api/v1/system/health`
14. Set up automated backups (cron + pg_dump)
15. Configure uptime monitoring on the health endpoint
