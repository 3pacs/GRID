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
returns the transcript as the job log and an artifact.

Two corrections from first real use:

- **`/etc/grid/gemini.env` does not exist** on the box. `GEMINI_API_KEY` lives in
  the repo `.env`. The workflow's Lanes check tolerates the missing file.
- **`agy` matches models on display name, not API id.** `gemini-flash-latest` is
  rejected. Its catalog: Gemini 3.8 / 3.7 / 3.6 Flash (High, Medium, Low),
  Gemini 3.1 Pro (High, Low), Claude Sonnet 4.6 (Thinking), Claude Opus 4.6
  (Thinking), GPT-OSS 120B (Medium). The workflow defaults to
  `Gemini 3.8 Flash (High)`. Use it for anything that
is mostly "run things on the box and report": log audits, FRED lookups, service
checks, data probes. Keep code changes that need review in your own PR.

## Fleet facts (verified 2026-09-10)

- LLM tiers: grid-svr RTX 3090 llama-server Qwen3.8-27B (100.75.185.36:8086 behind
  the :8081 shim; REASON + ORACLE), redbox `qwen3.8-27b` (100.126.129.45:8080, LOCAL),
  gridz4 `Qwen3.8-27B-Q4_K_M` (gridz4:8080), Ollama on grid-svr :11434
  (`qwen3.8:27b`, `gemma3:12b-it-q4_K_M`, `qwen3-vl`, `nomic-embed-text`).
  Tailnet embed nodes — **corrected 2026-09-10 after probing from grid-svr**;
  the earlier claim that koala and z400 carry `nomic-embed-text` was wrong:

  | host | tailnet | state |
  |---|---|---|
  | gridz4 | 100.68.9.27 | **ACTIVE**, `nomic-embed-text:latest` on **:11434** — the only live one |
  | koala | 100.123.236.28 | offline, last seen 48 d ago |
  | z400 | — | not a tailnet peer at all |
  | ocr-node | — | offline 31 d (anything routing to `ollama_ocr` is dead) |
  | panda | — | offline 18 d |
  | redbox | 100.126.129.45 | llama.cpp on :8080, **no** Ollama on :11434 |

  Note gridz4 runs two different servers: Ollama on **:11434** (`ollama_z4`,
  embeddings) and llama.cpp on **:8080** (`llamacpp_z4`, chat). Don't conflate them.
- `llamacpp` is **not** a :8080 provider. `config.py` points it at :8081 and the
  live `.env` at 100.75.185.36:8086 — both the RTX 3090. The dead :8080 provider
  was `gemma` (already disabled), now removed from the fallback chains.
- The CPU-only `grid-llamacpp` unit on :8080 is **retired** (#427): disabled,
  inactive, :8080 free, and `server_setup/grid-llamacpp.service` deleted so it
  cannot be resurrected by copying the unit files back.
- Operator directive 2026-09-10: **no CPU-only Qwen servers.** Use another GPU
  machine on the tailnet or a frontier model (Gemini preferred).
- DB: PostgreSQL 15 + TimescaleDB, `griddb` on localhost:5432, app role `grid`.
