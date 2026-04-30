# Langfuse self-hosted stack

Backs the GRID agent-honesty / eval workflow: harvest production LLM traces, label
failures, grow a regression dataset, gate deploys on regression scores.

Self-hosted because traces include outputs from paid APIs, internal theses, and
publishing-firewall-relevant content — sovereignty matters. Chosen over Braintrust
and LangSmith for that reason.

## Stack

Six containers managed by `docker compose`:

| Container | Image | Purpose | Host port |
|---|---|---|---|
| `langfuse-web` | `langfuse/langfuse:3` | UI + REST API | **3000** |
| `langfuse-worker` | `langfuse/langfuse-worker:3` | Background event processing | — |
| `postgres` | `postgres:15-alpine` | App data | internal |
| `clickhouse` | `clickhouse/clickhouse-server` | Trace event store | internal |
| `redis` | `redis:7` | Queue / cache | internal |
| `minio` | `minio/minio` | S3-compatible blob storage | internal |

## First-time bring-up

```bash
cd deploy/langfuse
cp .env.example .env

# Generate the secrets — note the format constraints in .env.example
sed -i "s|replace-with-openssl-rand-base64-32|$(openssl rand -base64 32 | tr -d '\n')|" .env
sed -i "s|replace-with-openssl-rand-hex-32|$(openssl rand -hex 32)|" .env
# ... fill the rest with `openssl rand -hex 24` (hex only — see gotcha below)

chmod 600 .env

# ClickHouse needs the bind-mount owned by UID 101 BEFORE first start.
# Pick a host directory for persistent volumes (replace /mirror with your own).
sudo mkdir -p /mirror/services/langfuse/{postgres,clickhouse-data,clickhouse-logs,redis,minio}
sudo chown -R 101:101 /mirror/services/langfuse/{clickhouse-data,clickhouse-logs}

# Edit docker-compose.yml volume paths if you're not using /mirror.

docker compose pull
docker compose up -d
```

First boot runs ~390 Postgres migrations and ~50 ClickHouse migrations — give it
2–3 minutes before hitting the UI. Health check:

```bash
curl -s http://localhost:3000/api/public/health
# {"status":"OK","version":"3.x.x"}
```

Then browse to http://localhost:3000 — the first signup becomes the org owner.
Create a project, generate API keys at Settings → API Keys.

## Day-to-day ops

```bash
docker compose ps                    # status
docker compose logs langfuse-web     # tail web logs
docker compose logs --tail=50 -f     # follow all containers
docker compose restart               # cycle
docker compose down                  # stop (volumes preserved)
docker compose up -d                 # start
```

## Wiring it into GRID

API keys go in the **GRID repo's** `.env` (not this stack's `.env`):

```
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_HOST=http://grid-svr:3000
```

Tracing fan-out lives in `llm/feedback_loop.py::_emit_to_langfuse`. The
`@observe` decorator on `api/routers/chat.ask_grid` creates the parent span.
End-to-end smoke: `python -m scripts.smoke_langfuse` from the repo root.

## Gotchas

- **ClickHouse password format.** `+`, `/`, `=`, `@`, `&`, `%`, `#` and spaces
  all break the migration URL parser. Use `openssl rand -hex N` exclusively.
  Symptom: `error: failed to open database: code: 516, message: clickhouse:
  Authentication failed: password is incorrect`. If you hit this, regenerate
  the password, wipe `clickhouse-data` and `clickhouse-logs`, and restart the
  ClickHouse container.
- **ClickHouse volume permissions.** The image runs as UID 101. Bind-mounted
  directories must be `chown -R 101:101` or ClickHouse can't `mkdir
  /var/lib/clickhouse/tmp`.
- **Postgres volume permissions.** First Postgres container chowns the bind
  mount to UID 70 (postgres-internal). To wipe it later you'll need `sudo`.
- **Port 3000 collisions.** This is the only port we expose; if the host
  already binds 3000, change the `langfuse-web` ports mapping.
- **MinIO ports 9000/9001.** Stay internal. If you need the MinIO console,
  add a host mapping but expect collisions with anything else using those
  ports.
- **No UPS protection.** ext4's default journaling tolerates clean crashes;
  power loss mid-write is a real risk for ClickHouse/Postgres state. Stand
  up a UPS before this becomes load-bearing.
