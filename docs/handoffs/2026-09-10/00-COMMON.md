# Common rules for every 2026-09-10 hand-off

Read this first, then your numbered hand-off. Each hand-off is independent and
can run in parallel with the others; the only shared resource is the grid-svr
runner (see "Server lane").

## Repo discipline

1. Start from `origin/main`. Work on your own branch named in the hand-off
   (`claude/handoff-NN-<slug>`). Never push to another agent's branch or to main.
2. Before creating any new module run `python3 scripts/pre_create_check.py "<concept>"`
   and read `docs/MODULE_INVENTORY.md`. Extend the canonical module; create new
   files only when both show no coverage. Read `docs/agent_preamble/*.md`.
3. Parameterized SQL only (`text(...)` with bound params; whitelist identifiers).
   `tests/test_no_sql_fstrings.py` blocks regressions.
4. PIT correctness: every analytical read goes through `store/pit.py` semantics;
   never store a future `obs_date`; never write to `decision_journal`.
5. `raw_series` is a TimescaleDB hypertable — bound `obs_date` on both sides in
   every query or it walks every chunk (~100 s per ticker).
6. Type hints on new functions; `loguru` `log`; `log.error` only for real bugs,
   `log.warning` for upstream/transient failures.
7. Every changed module gets tests in `tests/`. Run the touched test files plus
   `tests/test_no_sql_fstrings.py` locally before pushing.
8. Open a PR to `main` with a body that states what changed, what was verified,
   what is left. The operator's standing rule is "merge and push as necessary,
   don't wait": merge with squash once Lint, Frontend Build and Backend Tests
   are green, then reset your branch onto `origin/main`.
9. Never commit `.env`, keys, app passwords or tokens. Never print secrets in
   job logs. No model identifiers in code, commits or PR text.
10. End your session with an agent report: `agent-report <agent> <slug> body.md`
    where the wrapper exists (`/usr/local/bin/agent-report` on grid-svr); else
    leave the body in your PR description. Body: what changed, what was
    verified, what is blocked, what is left.

## Server lane (grid-svr)

There is no SSH. Server work goes through GitHub Actions on the self-hosted
runner (`runs-on: [self-hosted, grid-svr]`):

- `ops-exec.yml` (workflow_dispatch; inputs `script`, `workdir`, `source_env`):
  runs a bash script on the box, 28-minute cap, output in the job log and the
  step summary. Trigger it with the GitHub Actions API/MCP, then read
  `list_workflow_jobs` → `get_job_logs`.
- Long jobs: detach so the 28-minute cap and the runner's orphan cleanup do not
  kill them —
  `PYTHONPATH=$REL env -u RUNNER_TRACKING_ID setsid nohup python3 /tmp/job.py > /tmp/job.log 2>&1 < /dev/null & disown`
  — and poll the log with a second ops-exec.
- Trees: release tree `/data/grid_v4/grid_release` (deploy.yml resets it to main
  on every push, restarts `grid-api`, verifies health); Hermes tree
  `/home/grid/grid_v4/grid_repo` (daemons: grid-hermes, grid-intelligence,
  grid-extractor, grid-worker, grid-coordinator, grid-scheduler). Update the
  Hermes tree with
  `cd /home/grid/grid_v4/grid_repo && git stash -q; git fetch -q /data/grid_v4/grid_release HEAD && git merge -q --no-edit FETCH_HEAD; git stash pop -q || true`
  then `sudo systemctl restart <units>`.
- Health: `curl -fsS http://localhost:8000/api/v1/system/health`. Error log:
  `python3 scripts/audit_error_log.py --hours 24 --top 20` in the Hermes tree.
- `.env` lives at `/home/grid/grid_v4/grid_repo/.env` (source it with
  `set -a; . …/.env; set +a`; never `cat` it).
- If the runner shows every job "queued" for more than a few minutes it has
  stalled (it did at 19:28 UTC today after a "lost communication" job). Only a
  restart on the box clears it: `sudo systemctl restart actions.runner.3pacs-GRID.grid-svr`.
  Ask the operator; do not spin.

## Gemini lane (preferred for server-side legwork)

`gemini-task.yml` (workflow_dispatch; inputs `prompt`, `model`, `workdir`,
`yolo`, `cli`) runs the Gemini CLI (`agy`) on grid-svr with the repo as cwd and
returns the transcript as the job log and an artifact. Use it for anything that
is mostly "run things on the box and report": log audits, FRED lookups, service
checks, data probes. Keep code changes that need review in your own PR.

## Fleet facts (verified 2026-09-10)

- LLM tiers: grid-svr RTX 3090 llama-server Qwen3.8-27B (100.75.185.36:8086 behind
  the :8081 shim; REASON + ORACLE), redbox `qwen3.8-27b` (100.126.129.45:8080, LOCAL),
  gridz4 `Qwen3.8-27B-Q4_K_M` (gridz4:8080), Ollama on grid-svr :11434
  (`qwen3.8:27b`, `gemma3:12b-it-q4_K_M`, `qwen3-vl`, `nomic-embed-text`).
  Tailnet Ollama nodes with `nomic-embed-text`: koala:11434, z400:11434
  (ocr-node:11434 has gemma3 + vision).
- Operator directive 2026-09-10: **no CPU-only Qwen servers.** Use another GPU
  machine on the tailnet or a frontier model (Gemini preferred).
- DB: PostgreSQL 15 + TimescaleDB, `griddb` on localhost:5432, app role `grid`.
