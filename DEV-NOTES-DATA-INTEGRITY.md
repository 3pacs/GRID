# Data Integrity Fix Plan — For Incoming Dev

## Your Mission

Fix the broken data integrity layer across GRID. This is separate from the LLM/inference work happening on main. Work in your own worktree/branch.

## Context

GRID is a trading intelligence platform. It pulls data from 48+ sources (market data, macro, options, news, etc.), stores it in PostgreSQL + TimescaleDB, and runs analysis/inference on top. The data layer has accumulated significant tech debt:

- **281 silently swallowed exceptions** across 82 files (data failures hidden)
- **f-string SQL injection patterns** in 3 locations
- **No input validation** on the main chat API
- **Race conditions** on shared state in async routers
- **N+1 queries** causing unbounded DB round-trips
- **1,217 print() statements** instead of structured logging

## Priority Order

### Phase 1: CRITICAL Security (do first)

| ID | Issue | File | Fix |
|----|-------|------|-----|
| C1 | Unauthenticated SSRF — push notification accepts arbitrary URL | `api/routers/notifications.py:21,75-90` | Add `Depends(require_auth)`, whitelist https:// URLs |
| C2 | No auth on notification routes | `api/routers/notifications.py:21` | Add auth dependency to router |
| C3 | CORS wildcard default `"*"` | `config.py:137` | Change default to `""` |
| C4 | Relative path subprocess | `api/routers/system.py:762` | Use `Path(__file__).resolve()` |
| C5 | f-string DDL injection | `oracle/model_factory.py:88` | Whitelist column names, validate with regex |

### Phase 2: HIGH Security

| ID | Issue | File | Fix |
|----|-------|------|-----|
| H1 | No input validation on ChatAskRequest | `api/routers/chat.py:37-41` | Add Pydantic validators |
| H2 | Prompt injection via history role | `api/routers/chat.py:793-795` | Restrict to {"user","assistant"} |
| H3 | Sleuth lead ID collision + raw user input | `api/routers/chat.py:643-655` | Use uuid, truncate input |
| H4 | Race condition on _timesfm_last_run | `api/routers/chat.py:685-741` | Add threading.Lock |
| H5 | f-string SQL (3 locations) | `chat.py:285`, `flows.py:326-365`, `sleuth.py:392-425` | Parameterized queries |
| H6 | XSS via dangerouslySetInnerHTML | `Briefings.jsx:210`, `MarketDiary.jsx:510` | Install + use DOMPurify |
| H7 | Payment middleware silently bypasses | `api/main.py:454-456` | Log error, return 500 |
| H8 | Path traversal in AstroGrid | `scripts/astrogrid_web_smoke.py:43-48` | Add .resolve() + boundary check |

### Phase 3: Systemic Quality

| ID | Issue | Scope | Fix |
|----|-------|-------|-----|
| H9 | 281 swallowed exceptions | 82 files | Replace `pass` with `log.warning()` — start with mcp_server.py (24), llm_taskqueue.py (26), system.py (19), chat.py (17) |
| H10 | 1,217 print() statements | 129 files | Replace with structured logging — start with intelligence/ (112), ingestion/ (bulk) |
| H12 | 9 unprotected global cache dicts | 7 router files | Create `utils/ttl_cache.py` with thread-safe TTLCache class |
| H13 | N+1 in actor enrichment | `intelligence/actor_discovery.py:1416` | Batch queries |
| H14 | N+1 in watchlist gatherer | `api/routers/chat.py:118` | Batch queries |

### Phase 4: Structural

| ID | Issue | Fix |
|----|-------|-----|
| H11 | 16 files over 800 lines | Split largest: flow_thesis.py (1695), causation.py, llm_taskqueue.py, hermes_operator.py |
| H15 | Global FLOW_KNOWLEDGE mutation | Return new dict instead of mutating |
| H17 | Smart scheduler thread leaks | `ingestion/smart_scheduler.py:235` — add counter, document |
| H21 | _intelligence_loop 150+ lines nested in lifespan | Extract to intelligence/scheduler.py |

## Architecture Notes

- **Database:** PostgreSQL 15 + TimescaleDB on localhost:5432, db=griddb, user=grid
- **API:** FastAPI at port 8000, served via Cloudflare tunnel
- **Auth:** JWT-based, `api/middleware/auth.py` has `require_auth` dependency
- **Config:** pydantic-settings in `config.py`, env vars from `.env`
- **LLM Router:** Just refactored — `llm/router.py` has 3-tier taxonomy (LOCAL/REASON/ORACLE). Don't change this, it's on main.
- **Tests:** `python -m pytest tests/ -v` — 1,148 tests across 76 files

## Rules

1. **Work in a separate branch** — don't touch `main` directly
2. **Don't modify `llm/router.py`, `config.py` LLM sections, or `server_setup/`** — those are actively being worked on
3. **Test everything** — run `pytest` before pushing
4. **PIT correctness is sacred** — see `store/pit.py`, never access future data
5. **Every number needs 2+ independent source confirmations** — this is a trading system
6. **No silent error swallowing** — that's literally the main thing you're fixing
