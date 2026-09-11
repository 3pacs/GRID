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

## Services (Boot Order)

| # | Service | Port | Process | Location |
|---|---------|------|---------|----------|
| 1 | **PostgreSQL + TimescaleDB** | 5432 | Docker container `grid_db` | `~/grid_v4/grid_repo/grid/docker-compose.yml` |
| 2 | **llama.cpp (Qwen3.8-27B, RTX 3090)** | 8086 (shim on 8081) | `llama-server` (CUDA) | `/data/vendor/llama.cpp/build/bin/llama-server` |
| 3 | **Crucix** | 3117 | Node.js app | `~/grid_v4/Crucix/` (has own `.env`) |
| 4 | **GRID API (uvicorn)** | 8000 | `python3 -m uvicorn api.main:app` | `/data/grid_v4/grid_release` (deployed tree — see [Deploy Pipeline](#deploy-pipeline-grid-api) below) |
| 5 | **Hermes Operator** | — | `python3 scripts/hermes_operator.py` | `~/grid_v4/grid_repo/grid/` |
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


### Where it actually lives (verified 2026-09-11)

`alien` is a **Windows** host (tailnet name `precision5810`, Windows hostname
`ALIEN`, SSH lands in Git Bash as `owner`). The Linux runners do not run on
Windows: they live in the dedicated **WSL2 distro `GitHubActions`** (Ubuntu
24.04, systemd enabled, unprivileged `runner` user with rootless Docker, no
Windows interop/automount, egress guard). Run every command below as root
inside that distro, e.g. from an SSH session on alien:

```bash
wsl.exe -d GitHubActions -u root -- bash -s < setup-script.sh   # or interactive:
wsl.exe -d GitHubActions -u root
```

Runners follow the fleet layout `/opt/github-actions/<repo>/<runner-name>/`
owned by `runner`, with a systemd drop-in that orders them after the egress
guard and the `runner` user session. The GRID runner is
`/opt/github-actions/GRID/alien` → unit `actions.runner.3pacs-GRID.alien.service`.

**Boot contract:** WSL2 shuts the distro down when nothing holds it, which takes
every Linux runner on alien offline at once (all 19 sibling runners were
offline on 2026-09-10 for exactly this reason). The Windows scheduled task
`GitHubActions-WSL-Start` is what holds it open, and it **does exist** — an
earlier revision of this section said it was absent, which was wrong. Check
before concluding anything:

```bash
ssh alien 'MSYS_NO_PATHCONV=1 schtasks /query /tn GitHubActions-WSL-Start /v /fo LIST'
ssh alien 'powershell -NoProfile -Command "Get-ScheduledTaskInfo -TaskName GitHubActions-WSL-Start"'
```

`MSYS_NO_PATHCONV=1` is load-bearing: without it MSYS rewrites `/query` to
`C:/Program Files/Git/query` and `schtasks` fails with an argument error that
reads like "task not found". That is exactly how the false "absent" claim got
into this file.

As installed the task runs
`wsl.exe -d GitHubActions -u root --exec /usr/bin/sleep infinity` as `owner`
(LogonType S4U, RunLevel HighestAvailable, `ExecutionTimeLimit` PT0S).

**Its real failure mode is that it cannot recover on its own.** It has only a
boot trigger and a logon trigger, with `RestartOnFailure` Count 5 / Interval
PT1M. On 2026-09-02 it ran and exited 1, burned its five one-minute retries,
and then had nothing left to re-fire it — Windows `LastBootUpTime` was
2026-08-19, so across 22 days of uptime the distro stayed down and took every
runner with it. `schtasks /run /tn GitHubActions-WSL-Start` brings it straight
back up, which is the recovery action, not re-registration.

**Operator fix — add a repetition trigger to the existing task.** Do not
re-register it. `Set-ScheduledTask` keeps the triggers it already has and adds
a 15-minute repeat that runs forever; `MultipleInstancesPolicy` is already
`IgnoreNew`, so re-firing while the distro is healthy is a no-op (elevated
PowerShell on alien):

```powershell
$repeat = New-ScheduledTaskTrigger -Once -At (Get-Date) `
            -RepetitionInterval (New-TimeSpan -Minutes 15) `
            -RepetitionDuration ([TimeSpan]::MaxValue)
Set-ScheduledTask -TaskName GitHubActions-WSL-Start -Trigger @(
  (New-ScheduledTaskTrigger -AtStartup),
  (New-ScheduledTaskTrigger -AtLogOn),
  $repeat)

# The Task Scheduler operational log is disabled, which is why the 2026-09-02
# failure left no record at all. Turn it on so the next one is diagnosable.
wevtutil set-log Microsoft-Windows-TaskScheduler/Operational /enabled:true

Start-ScheduledTask -TaskName GitHubActions-WSL-Start
wsl.exe -l -v    # GitHubActions must say Running
```

Gotcha when testing this by hand from an SSH (Git Bash) session on alien: MSYS
rewrites leading-slash arguments, so `--exec /bin/sh -c ...` reaches wsl.exe as
`C:/Program Files/Git/usr/bin/sh` and fails with `execvpe ... No such file`.
Use `--exec sleep infinity` (no path) or `MSYS_NO_PATHCONV=1`.

**Do not set `TEST_RUNNER` while the distro is stopped.** Confirm
`wsl.exe -l -v` says `Running` and that
`gh api repos/3pacs/GRID/actions/runners` reports `alien` as `online` first.
With the variable set and the distro down, every PR's Lint / Backend Tests /
Frontend Build queues against an offline runner, and a job that is mid-run when
the distro idles out is cancelled (this happened to a foreign PR's Backend
Tests on 2026-09-11 01:15Z during activation testing). The way back is
`gh variable delete TEST_RUNNER -R 3pacs/GRID`, then cancel and re-run anything
already queued so it lands on `ubuntu-latest`.

### One-time setup (as root inside the `GitHubActions` distro)

```bash
# 1. PostgreSQL 15 + TimescaleDB — persistent service, not a per-run container.
#    The test job resets the schema (DROP SCHEMA public CASCADE; CREATE SCHEMA
#    public) instead of spinning up a fresh instance every run.
#    Differences from the generic recipe: no lsb_release in the distro (use
#    /etc/os-release); keys go in /etc/apt/keyrings with signed-by; TimescaleDB
#    is NOT in pgdg — it comes from Timescale's packagecloud repo and the
#    package is timescaledb-2-postgresql-15 (timescaledb-tune is in
#    timescaledb-tools and needs explicit --pg-config/--conf-path).
. /etc/os-release
install -d -m 0755 /etc/apt/keyrings
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /etc/apt/keyrings/pgdg.gpg
echo "deb [signed-by=/etc/apt/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" > /etc/apt/sources.list.d/pgdg.list
curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey | gpg --dearmor -o /etc/apt/keyrings/timescaledb.gpg
echo "deb [signed-by=/etc/apt/keyrings/timescaledb.gpg] https://packagecloud.io/timescale/timescaledb/ubuntu/ ${VERSION_CODENAME} main" > /etc/apt/sources.list.d/timescaledb.list
apt-get update
apt-get install -y postgresql-15 postgresql-client-15 timescaledb-2-postgresql-15 timescaledb-tools build-essential libpq-dev
timescaledb-tune --quiet --yes --pg-config=/usr/lib/postgresql/15/bin/pg_config --conf-path=/etc/postgresql/15/main/postgresql.conf
systemctl enable --now postgresql && systemctl restart postgresql
sudo -u postgres psql -c "CREATE ROLE grid LOGIN PASSWORD 'testpass';"
sudo -u postgres psql -c "CREATE DATABASE griddb_test OWNER grid;"
sudo -u postgres psql -d griddb_test -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
# matches the DB_URL the CI job already uses:
# postgresql://grid:testpass@localhost:5432/griddb_test
# Note: the job's `DROP SCHEMA public CASCADE` also drops the timescaledb
# extension (it lives in public). The test suite does not need it — the
# ubuntu-latest path uses a plain postgres:15 container — so this is fine.
# Pin the cluster and the test DB to UTC. The distro's system zone is
# America/Los_Angeles; the hosted postgres:15 container is UTC, and
# tests/test_regime_history_writer.py::TestStalenessFields compares Python's
# UTC date with Postgres current_date (it failed 164 == 165 on the first run).
sed -i "s/^timezone = .*/timezone = 'UTC'/; s/^log_timezone = .*/log_timezone = 'UTC'/" /etc/postgresql/15/main/postgresql.conf
sudo -u postgres psql -c "ALTER DATABASE griddb_test SET timezone TO 'UTC';"
pg_ctlcluster 15 main reload

# 2. Python 3.11 + Node 20
#    Both actions/setup-python@v6 and actions/setup-node@v6 provision their
#    own toolchain into the runner tool cache (_work/_tool) on first run and
#    reuse it afterwards, so a system install is a convenience, not a
#    requirement. Do NOT run the nodesource setup_20 script: the distro's
#    system Node (v22) is shared with the sibling runners for other repos.
apt-get install -y software-properties-common
add-apt-repository -y ppa:deadsnakes/ppa && apt-get update
apt-get install -y python3.11 python3.11-venv python3.11-dev

# 3. GitHub Actions runner — get a fresh registration token from
#    https://github.com/3pacs/GRID/settings/actions/runners/new, or
#    `gh api -X POST repos/3pacs/GRID/actions/runners/registration-token --jq .token`
#    (single-use, expires in ~1 hour; never write it to disk or a doc).
install -d -o runner -g runner /opt/github-actions/GRID/alien
su - runner -c 'cd /opt/github-actions/GRID/alien \
  && curl -sSLo r.tgz https://github.com/actions/runner/releases/download/v2.337.0/actions-runner-linux-x64-2.337.0.tar.gz \
  && tar xzf r.tgz && rm r.tgz'
/opt/github-actions/GRID/alien/bin/installdependencies.sh
su - runner -c 'cd /opt/github-actions/GRID/alien && ./config.sh --unattended \
  --url https://github.com/3pacs/GRID --token <REGISTRATION_TOKEN> \
  --name alien --labels self-hosted,alien,tests --work _work'
# The 2.337.0 tarball ships no top-level svc.sh; copy the identical script
# from any sibling runner directory (or use bin/runsvc.sh with a hand-written
# unit modelled on the siblings).
cp /opt/github-actions/obsidian-vault/precision5810-vault-linux/svc.sh /opt/github-actions/GRID/alien/svc.sh
cd /opt/github-actions/GRID/alien && ./svc.sh install runner
install -d /etc/systemd/system/actions.runner.3pacs-GRID.alien.service.d
cat > /etc/systemd/system/actions.runner.3pacs-GRID.alien.service.d/override.conf <<'CONF'
[Unit]
After=github-actions-egress-guard.service user@1000.service postgresql.service
Requires=github-actions-egress-guard.service user@1000.service
Wants=postgresql.service

[Service]
Environment=CI_ARTIFACT_ROOT=/var/lib/github-actions-artifacts
Environment=DOCKER_HOST=unix:///run/user/1000/docker.sock
Environment=XDG_RUNTIME_DIR=/run/user/1000
Environment=TZ=UTC
CONF
systemctl daemon-reload && systemctl enable --now actions.runner.3pacs-GRID.alien.service
journalctl -u actions.runner.3pacs-GRID.alien.service -n 5   # expect "Listening for Jobs"

# 4. Flip CI over once the runner shows "Idle" on the runners page (or
#    `gh api repos/3pacs/GRID/actions/runners` reports status "online"):
#    gh variable set TEST_RUNNER --body alien -R 3pacs/GRID
#    Setting it while no alien runner is online queues every PR's test jobs
#    indefinitely — check first, and delete the variable to fall back.
```

### Health / recovery

```bash
# Windows side (SSH to alien, Git Bash):
wsl.exe -l -v                                                  # GitHubActions must be Running
MSYS_NO_PATHCONV=1 schtasks /query /tn GitHubActions-WSL-Start /v /fo LIST   # boot contract
# Inside the distro (wsl.exe -d GitHubActions -u root):
systemctl status actions.runner.3pacs-GRID.alien --no-pager    # runner service
systemctl restart actions.runner.3pacs-GRID.alien              # if it drops off "Idle"
pg_isready -h localhost -U grid -d griddb_test                  # persistent test DB
# From anywhere with gh:
gh api repos/3pacs/GRID/actions/runners --jq '.runners[] | "\(.name) \(.status) busy=\(.busy)"'
```

To pull CI off alien without touching the workflow file, delete the
`TEST_RUNNER` repository variable — jobs fall back to `ubuntu-latest` on the
next run (a re-run of an already-queued run re-evaluates the variable too).

### Measured on 2026-09-11 (first activation, PR #450)

| Job | alien (warm tool cache) | ubuntu-latest |
|---|---|---|
| Lint | 1m56s | ~50s |
| Frontend Build | 1m26s | ~1m07s |
| Backend Tests (8,061 tests) | 14m58s job / 14m12s `Run tests` | 8m23s job / 6m47s `Run tests` |

alien's CPU is a Xeon E5-2698 v3 (2.3 GHz, 2014): roughly 2x slower per core
than the hosted VM, and pytest runs single-process. The ON_ALIEN branch was
verified (`Reset persistent Postgres (alien)` ran, `Start ephemeral Postgres
(hosted runner)` skipped), and the fallback was verified by deleting
`TEST_RUNNER` and re-running: jobs re-evaluate the variable and land on
`ubuntu-latest`. Two alien-only test failures were found and fixed:
`TestGauntlet::test_run_gauntlet` takes ~90 s there (pytest `--timeout` raised
60 -> 180 and the job `timeout-minutes` 15 -> 25 in `test.yml`), and the
timezone mismatch above. The handoff's "under 4 minutes" bar holds for Lint and
Frontend Build; Backend Tests will not get there on this CPU without
parallelising the suite (`pytest-xdist -n auto` across 24 cores, which needs
per-worker DB isolation first), so treat alien as a resilience/second-runner
win for the test lane, not a speed win, until that lands.
