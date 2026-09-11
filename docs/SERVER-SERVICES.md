# GRID Server Services — Quick Reference

All services run on `grid-svr` (Tailscale: `100.75.185.36`, user: `grid`).

## Deploy Pipeline (grid-api)

`grid-api` is **not** manually deployed — every push to `main` runs `.github/workflows/deploy.yml`:

1. **`verify`** (runs on GitHub-hosted `ubuntu-latest`): starts Postgres, runs
   `tests/test_api.py` and `tests/test_pit.py`.
2. **`deploy`** (runs on the self-hosted `grid-svr` runner, only if `verify` passed or was
   skipped): fetches `main` straight from GitHub and hard-resets
   `/data/grid_v4/grid_release` to it, `pip install -r requirements.txt`, `cd pwa && npm ci
   && npm run build`, then `alembic upgrade head`. It writes a systemd drop-in
   (`/etc/systemd/system/grid-api.service.d/zz-release-worktree.conf`) pointing `grid-api`'s
   `WorkingDirectory` at `/data/grid_v4/grid_release` and restarts the service. It then
   verifies `/api/v1/system/health`, that the built PWA (`pwa_dist/assets/...`) is served,
   and that `/openapi.json` lists the `actor-network`, `gold/stream`, and
   `conviction/sweeps/latest` routes.

So the **live `grid-api` tree is `/data/grid_v4/grid_release`**, kept in sync with `main` by
this workflow — not the dev/source checkout at `~/grid_v4/grid_repo/grid/` referenced
elsewhere on this page. The manual start/restart commands below still apply to the other
services (Postgres, llama.cpp, Crucix, Hermes Operator, Coordinator, Worker), and to
`grid-api` only if you're intentionally bypassing the automated deploy (e.g. local
debugging on the server).

## Deploy Path (grid-hermes)

`grid-hermes` does **not** share the `grid-api` release tree, and
`.github/workflows/deploy.yml` never touches it: that workflow hard-resets
`/data/grid_v4/grid_release` and restarts `grid-api` only. So **merging to `main`
does not deploy Hermes.**

Verified on 2026-09-11 (ops-exec runs
[144](https://github.com/3pacs/GRID/actions/runs/34549441175) and
[151](https://github.com/3pacs/GRID/actions/runs/34550741131)):

| Fact | Value |
|---|---|
| `WorkingDirectory` | `/home/grid/grid_v4/grid_repo` — a symlink to `/data/grid_v4/grid_repo` |
| `EnvironmentFile` | `/home/grid/grid_v4/grid_repo/.env` |
| `ExecStart` | `/usr/bin/python3 scripts/hermes_operator.py` |
| `/proc/<MainPID>/cwd` | `/data/grid_v4/grid_repo` |

Hermes updates its own tree from inside its cycle via
`scripts.hermes_operator.git_pull()`, which runs `git pull --ff-only origin main`
(see `tests/test_hermes_git_pull.py`). Two things follow, and both bite:

1. **`--ff-only` refuses once the tree carries local commits.** As of
   2026-09-11 the Hermes tree was **60 commits ahead of its last-known
   `origin/main`, 0 behind**, with several local `Merge /data/grid_v4/grid_release`
   commits and a dirty working tree. `HEAD` is not an ancestor of `origin/main`,
   so the self-pull cannot advance it — silently, since a refused pull is logged
   at `warning` and the cycle carries on.
2. **A pulled tree is not a running tree.** The process imports its modules at
   start, so new code only takes effect on `systemctl restart grid-hermes`.

**Getting a Hermes-side change live therefore takes an operator**, in this order:

```bash
# 1. Reconcile the tree (an operator decision — the local commits and the dirty
#    working tree must be dealt with first; do not blind-reset it).
cd /data/grid_v4/grid_repo && git status -sb && git log --oneline origin/main..HEAD

# 2. Once HEAD is an ancestor of origin/main again:
cd /data/grid_v4/grid_repo && git pull --ff-only origin main

# 3. Restart so the process picks up the new modules.
sudo systemctl restart grid-hermes

# 4. Confirm the cycle's resolution step is actually running.
journalctl -u grid-hermes -f | grep -iE 'Resolution|Resolver'
```

## Resolver Catch-Up Runbook

`resolved_series` is the PIT table every analytical surface reads. Its per-cycle
writer is Hermes cycle step 3b (`run_fast_resolution` →
`Resolver.resolve_pending(lookback_days=2)`). To recover a gap, walk the backlog
in bounded chunks of `raw_series.pull_timestamp`:

```bash
cd /data/grid_v4/grid_release

# Measure first — every phase runs, nothing is written.
python -m normalization.resolver --since 2026-04-04 --until 2026-04-11 \
    --chunk-days 7 --workers 8 --dry-run

# Then write. Idempotent: re-running a chunk hits
# ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING.
python -m normalization.resolver --since 2026-04-04 --until 2026-05-02 \
    --chunk-days 7 --workers 8
```

Run it in slices that fit the 28-minute `ops-exec` cap rather than one long
invocation, and keep `--chunk-days` at 7 or below: chunk cost scales with rows
pulled in the window, not with `raw_series` size (~1.93 B rows), because
`idx_raw_series_pull_timestamp` serves the window as an index scan.

## Services (Boot Order)

| # | Service | Port | Process | Location |
|---|---------|------|---------|----------|
| 1 | **PostgreSQL + TimescaleDB** | 5432 | Docker container `grid_db` | `~/grid_v4/grid_repo/grid/docker-compose.yml` |
| 2 | **llama.cpp (Qwen3.8-27B, RTX 3090)** | 8086 (shim on 8081) | `llama-server` (CUDA) | `/data/vendor/llama.cpp/build/bin/llama-server` |
| 3 | **Crucix** | 3117 | Node.js app | `~/grid_v4/Crucix/` (has own `.env`) |
| 4 | **GRID API (uvicorn)** | 8000 | `python3 -m uvicorn api.main:app` | `/data/grid_v4/grid_release` (deployed tree — see [Deploy Pipeline](#deploy-pipeline-grid-api) below) |
| 5 | **Hermes Operator** | — | `python3 scripts/hermes_operator.py` | `/home/grid/grid_v4/grid_repo` → `/data/grid_v4/grid_repo` (see [Deploy Path (grid-hermes)](#deploy-path-grid-hermes)) |
| 6 | **Compute Coordinator** | 8100 | `uvicorn scripts.compute_coordinator:app` | `~/grid_v4/grid_repo/grid/` |
| 7 | **Compute Worker** | — | `python3 scripts/worker.py` | `~/grid_v4/grid_repo/grid/` |

## Start Commands (Manual)

```bash
# 1. Database (auto-restarts via Docker)
cd ~/grid_v4/grid_repo/grid && docker compose up -d

# 2. llama.cpp
cd ~/grid_v4/grid_repo/grid && bash scripts/start_llamacpp.sh &

# 3. Crucix
cd ~/grid_v4/Crucix && node server.mjs &
# (verify: curl -s http://localhost:3117)

# 4. GRID API (bypasses the deploy.yml-managed systemd drop-in — see Deploy Pipeline above)
cd /data/grid_v4/grid_release && python3 -m uvicorn api.main:app --host 0.0.0.0 --port 8000 &

# 5. Hermes Operator (autonomous daemon)
cd ~/grid_v4/grid_repo/grid && python3 scripts/hermes_operator.py &
```

## Health Checks

```bash
pg_isready -U grid -d griddb                           # PostgreSQL
curl -s localhost:8081/health                           # llama.cpp shim -> :8086 (RTX 3090)
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
cd /data/grid_v4/grid_release && python3 -m uvicorn api.main:app --host 0.0.0.0 --port 8000 &
sleep 3
cd ~/grid_v4/grid_repo/grid && python3 scripts/hermes_operator.py &
```

## Gemma Micro Models (CPU, ports 8082-8085)

| # | Service | Port | Model | Purpose |
|---|---------|------|-------|---------|
| 8 | **grid-micro-classifier** | 8082 | `gemma-4-e4b-signal-classifier.gguf` | Signal domain/urgency classification |
| 9 | **grid-micro-narrator** | 8083 | `gemma-4-e4b-anomaly-narrator.gguf` | One-line anomaly summaries |
| 10 | **grid-micro-extractor** | 8084 | `gemma-4-e4b-edgar-extractor.gguf` | Structured SEC filing extraction |
| 11 | **grid-micro-mapper** | 8085 | `gemma-4-e4b-knowledge-mapper.gguf` | Wiki-style knowledge entries with [[backlinks]] |

All run on CPU (`CUDA_VISIBLE_DEVICES=`), 4 threads each. Models at `/data/models/micro/`.

```bash
# Health check all micro models
for p in 8082 8083 8084 8085; do echo -n "Port $p: "; curl -s localhost:$p/health | head -c 50; echo; done
```

## Systemd Services

Service files in `server_setup/` — install with:
```bash
sudo cp server_setup/grid-*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable grid-db grid-api grid-hermes grid-crucix \
  grid-micro-classifier grid-micro-narrator grid-micro-extractor grid-micro-mapper
sudo systemctl start grid-db grid-crucix grid-api grid-hermes \
  grid-micro-classifier grid-micro-narrator grid-micro-extractor grid-micro-mapper
```

## Retired: `grid-llamacpp` on :8080 (2026-09-10)

`grid-llamacpp.service` ran `Qwen3.6-27B-Q4_K_M.gguf` with `LLAMACPP_NGL=0` —
CPU-only inference holding **17.9 GB of RAM**. It was also the only thing GRID
ever asked for embeddings, via `HYPERSPACE_BASE_URL` defaulting to
`http://localhost:8080/v1`; llama-server was never started with `--embeddings`,
so every one of those calls returned **HTTP 501** and the unit's journal
contained nothing else.

The operator's directive of 2026-09-10 — *"there should be no CPU-only Qwens —
use another machine on the tailnet"* — retired it:

```bash
sudo systemctl disable --now grid-llamacpp
```

`server_setup/grid-llamacpp.service` was deleted from the repo so a future
`cp server_setup/grid-*.service` cannot resurrect it. **Do not recreate it.**
GPU inference is unaffected — it lives on :8086 behind the :8081 shim.

Where its traffic went:

| Old consumer of :8080 | Now |
|---|---|
| Embeddings (`hyperspace/embeddings.py`) | `llm.router.embed()` → `EMBED_PROVIDER_CHAIN` (gridz4 → koala → z400 → grid-svr Ollama, all `nomic-embed-text`) |
| `gemma` provider (`GEMMA_BASE_URL`) | Removed from every `_fallback_chain`; it had been `GEMMA_ENABLED=false` and dead already |
| Chat/REASON | Unaffected — `LLAMACPP_BASE_URL` already pointed at the RTX 3090 (`:8086`), not `:8080` |

### Embedding nodes — reachability probed 2026-09-10

| Node | Endpoint | State | Role |
|---|---|---|---|
| gridz4 | `gridz4:11434` | active, direct | **primary** — the "another machine on the tailnet" |
| koala | `koala:11434` | **offline, last seen 48d ago** | in chain, resumes automatically if it returns |
| z400 | `z400:11434` | **not a tailnet peer** | in chain, resumes automatically if it returns |
| grid-svr | `localhost:11434` | active | last resort — shares the RTX 3090 with the chat model |

`redbox:11434` has no Ollama (it serves llama.cpp on `:8080`); `ocr-node` and
`panda` have been offline 31 and 18 days.

Note `ollama_z4` (gridz4 **:11434**, Ollama) is a different daemon from
`llamacpp_z4` (gridz4 **:8080**, llama.cpp). Both live on gridz4.

Embedding health check:

```bash
# primary
curl -s -m 10 http://gridz4:11434/api/embeddings \
  -d '{"model":"nomic-embed-text","prompt":"probe"}' | head -c 80
# last resort
curl -s -m 10 http://localhost:11434/api/embeddings \
  -d '{"model":"nomic-embed-text","prompt":"probe"}' | head -c 80
```

## Public Access (Cloudflare Tunnel)

| Item | Value |
|------|-------|
| Domain | `grid.stepdad.finance` |
| Tunnel ID | `78b96513-f55f-42e0-a9ff-1915941e92cb` |
| Config | `/etc/cloudflared/config.yml` |
| Service | `cloudflared.service` (systemd, auto-starts on boot) |
| DNS | CNAME `grid` → `78b96513-f55f-42e0-a9ff-1915941e92cb.cfargotunnel.com` |
| Nameservers | Cloudflare (set in Namecheap) |

No port forwarding needed. Traffic flows: User → Cloudflare → tunnel → localhost:8000.

## Key Paths

| Item | Path |
|------|------|
| Repo (dev/source, pushed to `main`) | `~/grid_v4/grid_repo/grid/` |
| Repo (live `grid-api` tree, see [Deploy Pipeline](#deploy-pipeline-grid-api)) | `/data/grid_v4/grid_release` |
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
- **17:00 weekdays** — [[TradingAgents]] (if enabled)

## Alien Runner (Self-Hosted CI, Dell Precision 5810)

`alien` is a second GitHub Actions self-hosted runner, separate from `grid-svr`.
It runs only the test lane (`test.yml`: Lint, Backend Tests, Frontend Build) so
CI stops paying the ~10-minute `ubuntu-latest` pip-install / Postgres-pull tax
on every PR, and so a `grid-svr` runner stall (it has stalled before — see
`00-COMMON.md`) doesn't take out CI too. It never gets deploy credentials —
`deploy.yml`, `ops-exec.yml`, and `gemini-task.yml` stay pinned to
`[self-hosted, grid-svr]`.

Workflow jobs opt in via the `TEST_RUNNER` repository variable (Settings →
Secrets and variables → Actions → Variables): when set to any non-empty value,
`test.yml` targets `[self-hosted, alien, tests]`; when unset, jobs fall back to
`ubuntu-latest` so CI never blocks on the Dell being offline.

### One-time setup (run as the operator, on alien)

```bash
# 1. PostgreSQL 15 + TimescaleDB — persistent, not a per-run container.
#    The test job resets the schema (DROP SCHEMA public CASCADE; CREATE SCHEMA
#    public) instead of spinning up a fresh instance every run.
sudo apt-get update
sudo sh -c "echo 'deb https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main' > /etc/apt/sources.list.d/pgdg.list"
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /usr/share/keyrings/pgdg.gpg
sudo apt-get update && sudo apt-get install -y postgresql-15 postgresql-15-timescaledb
sudo timescaledb-tune --quiet --yes
sudo systemctl enable --now postgresql
sudo -u postgres psql -c "CREATE ROLE grid LOGIN PASSWORD 'testpass';"
sudo -u postgres psql -c "CREATE DATABASE griddb_test OWNER grid;"
sudo -u postgres psql -d griddb_test -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
# matches the DB_URL the CI job already uses:
# postgresql://grid:testpass@localhost:5432/griddb_test

# 2. Python 3.11 + Node 20
sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:deadsnakes/ppa && sudo apt-get update
sudo apt-get install -y python3.11 python3.11-venv python3.11-dev
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

# 3. GitHub Actions runner — get a fresh download URL and registration token
#    from https://github.com/3pacs/GRID/settings/actions/runners/new (token
#    is single-use and expires in ~1 hour, so pull both values at install
#    time rather than hardcoding them here).
mkdir -p ~/actions-runner && cd ~/actions-runner
curl -o actions-runner-linux-x64.tar.gz -L <DOWNLOAD_URL_FROM_RUNNERS_PAGE>
tar xzf actions-runner-linux-x64.tar.gz
./config.sh --url https://github.com/3pacs/GRID \
  --token <REGISTRATION_TOKEN_FROM_RUNNERS_PAGE> \
  --name alien --labels self-hosted,alien,tests --unattended
sudo ./svc.sh install
sudo ./svc.sh start
# registers as systemd unit actions.runner.3pacs-GRID.alien
systemctl status actions.runner.3pacs-GRID.alien --no-pager

# 4. Flip CI over once the runner shows "Idle" on the runners page:
#    gh variable set TEST_RUNNER --body alien -R 3pacs/GRID
#    (or Settings → Secrets and variables → Actions → Variables → New)
```

### Health / recovery

```bash
systemctl status actions.runner.3pacs-GRID.alien --no-pager   # runner service
sudo systemctl restart actions.runner.3pacs-GRID.alien        # if it drops off "Idle"
pg_isready -U grid -d griddb_test                              # persistent test DB
```

To pull CI off alien without touching the workflow file, unset (or delete) the
`TEST_RUNNER` repository variable — jobs fall back to `ubuntu-latest` on the
next run.
