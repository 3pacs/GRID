# GRID — Claude Code Guidelines

This file is the **always-on core**: identity, standing rules, non-negotiable guardrails,
and a load-on-demand index. Heavy reference material lives in `docs/reference/` and is
loaded only when a task needs it (see the Reference Index at the bottom). Keep this file
lean — when you add durable detail, put it in a reference file and link it here.

## Core Principle — better data, better tools than we had yesterday

GRID's standing direction: **prefer finding better data and rebuilding better tools over
preserving legacy exactness.** When an external source (a library, API, or dataset) offers
richer or cleaner data than a hand-rolled implementation, rebuild the tool around the
better data rather than cramming the new source into the old shape. Do not be precious
about reproducing every legacy measurement — a proxy that is as good or better with better
data wins. (Guardrails below are the exception: PIT correctness, SQL safety, and journal
immutability are non-negotiable regardless.)

## Standing rule — session-end reporting (mandatory)

Every agent session on every host calls `agent-report <agent> <slug> <body.md>` (or
`~/scripts/agent_hub/report_to_hub.sh` on Mac mini) before terminating. Body must
summarize: what changed, what was verified, what is blocked, what is left. Reports land in
`00-Agent-Reports/YYYY-MM-DD/` in `~/dev/obsidian-vault` (GitHub `3pacs/obsidian-vault`).
Wrapper paths: `/usr/local/bin/agent-report` on grid-svr/gridz4/redbox/panda/koala;
`~/bin/agent-report` on ocr-node. Token in `/etc/agent-hub/token` (0600) or
`~/.config/agent-hub/token`. Full ruleset: `~/dev/obsidian-vault/AGENTS.md`.

## Project Overview

GRID is a systematic, multi-agent trading intelligence platform. It ingests
macroeconomic/market data from 48 data pullers (all registered in [[Hermes Scheduler|Hermes
scheduler]]), resolves multi-source conflicts using [[PIT Store|point-in-time]] (PIT)
correct methodology, performs unsupervised [[Regime Discovery|regime discovery]], and runs
[[Walk-Forward Backtesting|walk-forward backtesting]] with an immutable [[Decision
Journal|decision journal]].

**See `docs/planning/ROADMAP.md` for the full 4-week tactical plan and 4-quarter strategic plan.**

## Quick Orientation

A **SessionStart hook** auto-injects live server state + codebase index into every
conversation. If you need to re-orient mid-session, read `.claude/CODEBASE_INDEX.md` — it
has the module function index, DB schema, server ops, and integration map. Run
`/grid-orient` to rebuild the index after major changes.

### Before You Build ANYTHING New

> **Assume any capability that sounds obvious already exists somewhere in the 700+ module
> codebase.** CLAUDE.md is an intentionally-curated subset, not a complete inventory. The
> authoritative full inventory is `docs/MODULE_INVENTORY.md` (700+ modules across 30
> directories with docstrings, APIs, DB I/O, and import graphs).

Pre-build checklist:

1. **Read `docs/MODULE_INVENTORY.md`** — authoritative inventory with APIs and import graphs.
2. **Run `/grid-check-exists <keyword>`** — searches intelligence/ + analysis/ + physics/ +
   features/ + discovery/ + trading/ + oracle/ for similar modules.
3. **Grep for the concept** across those directories if the keyword search doesn't hit.
4. **Read the top 50 lines** of any match to confirm relevance before deciding to extend or rebuild.
5. **If it exists, the task is almost always "extend and wire," not "build new."** See
   `docs/reference/CODEBASE_MAP.md` for known "I almost rebuilt it but it exists" cases.

## Agent dispatch policy

Every backend agent prompt must include the preamble from `docs/AGENT_PROMPT_TEMPLATE.md`.
This enforces grep-before-create discipline and prevents the duplication documented in
`docs/MODULE_OVERLAP_AUDIT.md`.

## Tech Stack

- **Backend:** Python 3.11+, [[FastAPI]], [[SQLAlchemy]] 2.0, [[PostgreSQL]] 15 + [[TimescaleDB]]
- **Frontend:** React 18, Vite, [[Zustand]], served as PWA from [[FastAPI]]
- **LLM:** Dual local inference — Nemotron-Cascade-2 30B GPU (:8080) + Nemotron-3-Super-120B
  CPU (:8081). OpenRouter Claude fallback. See `llm/router.py` for the 3-tier taxonomy
  (LOCAL/REASON/ORACLE).
- **Config:** pydantic-settings, environment variables via `.env`

## Server Deployment

- Repo on server: `~/grid_v4` (user: `grid`, host: `grid-svr`)
- **Systemd services**: grid-api, grid-llamacpp, grid-crucix, grid-hermes, grid-coordinator,
  grid-worker, cloudflared. Restart core: `sudo systemctl restart grid-api grid-llamacpp
  grid-crucix grid-hermes`
- **Public URL**: `https://grid.stepdad.finance` (Cloudflare Tunnel)
- **Role-based auth**: admin (master password) and contributor (user accounts)
- Full reference: `docs/SERVER-SERVICES.md`

## Essential Commands

```bash
# Database
cd grid && docker compose up -d                    # Start PostgreSQL + TimescaleDB

# Backend
cd grid && pip install -r requirements.txt
cd grid && python -m uvicorn api.main:app --reload --port 8000

# Frontend
cd grid/pwa && npm install && npm run dev          # Dev server on :5173
cd grid/pwa && npm run build                       # Production build

# Tests
cd grid && python -m pytest tests/ -v              # Full suite: 5,702 tests across 301 files
cd grid && python -m pytest tests/test_pit.py -v   # PIT store tests
cd grid && python -m pytest tests/test_api.py -v   # API tests
```

## Architecture Rules (non-negotiable)

<important if="modifying any data query, [[Feature Engineering|feature engineering]], or inference code">
**PIT ([[PIT Store|Point-in-Time]]) Correctness is non-negotiable.** Every data query MUST
use `store/pit.py` to prevent [[PIT Store|lookahead bias]]. Never access future data
relative to the decision timestamp. The `assert_no_lookahead()` guard must pass for all
inference paths.
</important>

<important if="writing SQL or database queries">
**Never use string `.format()` or f-strings for SQL.** Always use parameterized queries via [[SQLAlchemy]].
</important>

<important if="adding or modifying data sources">
**Multi-source [[Conflict Resolution|conflict resolution]]** goes through
`normalization/resolver.py`. Every new data source needs: an ingestion module, [[Entity
Map|entity map]]ping in `entity_map.py`, and PIT-compatible timestamps. Use the scheduler
pattern from `ingestion/scheduler.py`.
</important>

<important if="modifying journal or decision logging code">
**[[Decision Journal|Immutable Journal]]** — entries in `journal/log.py` must never be
updated or deleted. Every recommendation gets logged with full provenance. Validate
confidence/probability are 0-1 and not NaN/infinity.
</important>

## Key Patterns

- **[[Model Governance]]:** CANDIDATE → SHADOW → STAGING → PRODUCTION (see `governance/registry.py`)
- **Graceful Degradation:** [[Hyperspace]]/[[Ollama]] calls return `None` if offline; system operates without them
- **Config:** All settings via `config.py` (pydantic-settings). Copy `.env.example` to `.env`
- **Logging:** `loguru` imported as `log` from config — use throughout
- **Log levels:** Reserve `log.error` for unhandled application bugs. Transient network
  errors, handled-with-blacklist timeouts, and operational git/infra failures use
  `log.warning` so `errors.jsonl` stays signal-rich.

## Error-Log Hygiene

`.server-logs/errors.jsonl` is the canonical operational health signal. To audit it:

```bash
python3 scripts/audit_error_log.py --hours 24            # default: top patterns last 24h
python3 scripts/audit_error_log.py --hours 168 --top 25  # weekly top 25
python3 scripts/audit_error_log.py --new-only            # only patterns absent in baseline week
```

The script normalizes volatile noise (timestamps, [[FRED]] series IDs, hex addresses,
request IDs) so similar-but-not-identical errors bucket together. Run it before declaring
"all green" — it catches regressions the test suite can't.

## Gotchas

- `DISTINCT ON` in `store/pit.py` is [[PostgreSQL]]-specific — SQLite/MySQL will not work
- `assert_no_lookahead()` raises ValueError but does NOT roll back the transaction ([[ATTENTION]].md #8)
- `_resolve_source_id()` auto-creates [[Source Catalog Table|source_catalog]] entries — unknown sources can appear silently (#25)
- `pd.to_numeric(errors="coerce")` in ingestion silently converts bad data to NaN (#13)
- NaN handling varies across modules (ffill limits, dropna timing) — follow the existing module's pattern (#14)
- `ingestion/scheduler.py` is the authoritative scheduler (the old `scheduler_v2.py` no longer exists; don't recreate it) (#39)

## Code Style

- Type hints on all new functions
- Follow existing patterns in each module — don't introduce new frameworks
- Keep API routes thin; business logic belongs in domain modules
- Every new module needs a test file in `grid/tests/`

## Workflow Best Practices

- Start complex tasks in **plan mode** before execution
- Use subagents for independent subtasks (parallel investigation, code review)
- Perform `/compact` at ~50% context usage on long sessions
- Break work into phases — verify each phase works before moving to the next
- After fixing a bug, confirm the fix with a test — don't just eyeball it
- Reference `grid/ATTENTION.md` for the full 64-item audit when fixing issues

## Reference Index (load on demand)

Read the matching file only when the task calls for it — keep this core file lean.

| When you're... | Read |
|---|---|
| Orienting in the tree / checking if something already exists | `docs/reference/CODEBASE_MAP.md` (+ `docs/MODULE_INVENTORY.md`) |
| Touching conviction multipliers, `signal_provenance`, or calibration | `docs/reference/CONVICTION_STACK.md` |
| Generating predictions or signal/thesis logic | `docs/reference/PREDICTION_SOP.md` |
| Working on options, the oracle engine, or trial-gem-hunter | `docs/reference/SUBSYSTEMS.md` |
| Rebuilding SEC ingestion (edgartools) | `docs/planning/SEC_TOOLS_REBUILD.md` |
| Standing up a web session / hooks | `docs/SERVER-SERVICES.md`, `.claude/CODEBASE_INDEX.md` |
