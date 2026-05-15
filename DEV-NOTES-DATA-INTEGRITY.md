# Data Integrity Fix Plan — For Incoming Dev

> **Status audit 2026-05-13:** Phase 1 (C1–C5) and most of Phase 2 (H1–H8) have shipped to main. H13, H14, H15, H17, H21 are also done. H12 has five PRs in flight (#145–#149) covering the six drop-in TTLCache targets; the remaining three cache dicts in H12 need architectural decisions (see notes inline). H9, H10, H11 (and the remaining hard parts of H12) are the real open work. Always `git grep` the cited file:line before starting — anything tagged DONE below was verified against main on 2026-05-13.

## Your Mission

Fix the broken data integrity layer across GRID. This is separate from the LLM/inference work happening on main. Work in your own worktree/branch.

## Context

GRID is a trading intelligence platform. It pulls data from 48+ sources (market data, macro, options, news, etc.), stores it in [[PostgreSQL]] + [[TimescaleDB]], and runs analysis/inference on top. The data layer has accumulated significant tech debt:

- **281 silently swallowed exceptions** across 82 files (data failures hidden)
- **f-string SQL injection patterns** in 3 locations
- **No input validation** on the main chat API
- **Race conditions** on shared state in async routers
- **N+1 queries** causing unbounded DB round-trips
- **1,217 print() statements** instead of structured logging

## Priority Order

### Phase 1: CRITICAL Security (do first) — ✅ ALL DONE

| ID | Status | Issue | File | Fix |
|----|--------|-------|------|-----|
| C1 | ✅ DONE | Unauthenticated SSRF — push notification accepts arbitrary URL | `api/routers/notifications.py:21,75-90` | Add `Depends(require_auth)`, whitelist https:// URLs |
| C2 | ✅ DONE | No auth on notification routes | `api/routers/notifications.py:21` | Add auth dependency to router |
| C3 | ✅ DONE | CORS wildcard default `"*"` | `config.py:137` | Change default to `""` |
| C4 | ✅ DONE | Relative path subprocess | `api/routers/system.py:762` | Use `Path(__file__).resolve()` |
| C5 | ✅ DONE | f-string DDL injection | `oracle/model_factory.py:88` | Whitelist column names, validate with regex |

### Phase 2: HIGH Security — ✅ ALL DONE

| ID | Status | Issue | File | Fix |
|----|--------|-------|------|-----|
| H1 | ✅ DONE | No input validation on ChatAskRequest | `api/routers/chat.py` | Pydantic `field_validator` on `context_ticker`, `timeframe`, `session_id` |
| H2 | ✅ DONE | Prompt injection via history role | `api/routers/chat.py` | `_VALID_ROLES = {"user","assistant"}` + validator |
| H3 | ✅ DONE | Sleuth lead ID collision + raw user input | `api/routers/chat.py` | `uuid.uuid4().hex[:12]` for lead IDs |
| H4 | ✅ DONE | Race condition on _timesfm_last_run | `api/routers/chat.py` | `_timesfm_lock = threading.Lock()` wraps get/set |
| H5 | ✅ DONE | f-string SQL (3 locations) | `chat.py`, `flows.py`, `sleuth.py` | No `execute(f"...")` patterns remain |
| H6 | ✅ DONE | XSS via dangerouslySetInnerHTML | `Briefings.jsx`, `MarketDiary.jsx`, `IntelligenceSearch.jsx` (PR #143) | DOMPurify on all three |
| H7 | ✅ DONE | Payment middleware silently bypasses | `api/main.py` | Returns 500 on middleware error (no silent bypass) |
| H8 | ✅ DONE | Path traversal in AstroGrid | `scripts/astrogrid_web_smoke.py` | `.resolve()` + archive-root boundary check |

### Phase 3: Systemic Quality

| ID | Status | Issue | Scope | Fix |
|----|--------|-------|-------|-----|
| H9 | OPEN | 281 swallowed exceptions | 82 files | Replace `pass` with `log.warning()` — start with mcp_server.py (24), llm_taskqueue.py (26), system.py (19), chat.py (17) |
| H10 | OPEN (partial) | print() statements in library code | many | Most intelligence/ prints are inside `if __name__ == "__main__":` CLI blocks — those are legitimate stdout, leave them. Target prints outside `__main__`/`_cli()` blocks first (e.g. `intelligence/universe_ranker.py:871` engine-bootstrap print). |
| H12 | IN FLIGHT (5 PRs) | 9 unprotected global cache dicts | 7 router files | Six `{"data": None, "ts": …}` caches covered by open PRs #145–#149 (`_thesis_cache`, `_lever_cache`, `_risk_map_cache` + `_globe_cache`, `_actor_graph_cache`, `_influence_graph_cache`). Remaining: `intel_cross_reference.py:25` (already has per-key lock map — needs design call), `canvas.py:92` (tuple key — needs TTLCache key-type widening or string encoding), `surfacer.py:1507-1510` (function-local, not global — likely not a thread-safety issue). |
| H13 | ✅ DONE | N+1 in actor enrichment | `intelligence/actor_discovery.py` | `enrich_all_actors` now uses 4 batched queries (actors, trust, connections, flows) with `= ANY(:aids)` |
| H14 | ✅ DONE | N+1 in watchlist gatherer | `api/routers/chat.py` | `_gather_watchlist_context` batch-fetches latest values with `feature_id = ANY(:fids)` |

### Phase 4: Structural

| ID | Status | Issue | Fix |
|----|--------|-------|-----|
| H11 | OPEN | 16 files over 800 lines | Split largest: flow_thesis.py (note: already split — `flow_thesis_data.py` + `flow_thesis_scoring.py`), causation.py (now split into `causation_core.py` + `causation_graph.py` + `causation_scoring.py`), llm_taskqueue.py, hermes_operator.py |
| H15 | ✅ DONE | Global FLOW_KNOWLEDGE mutation | `analysis/flow_thesis_data.py:415` — exposed as `types.MappingProxyType` (immutable view) |
| H17 | ✅ DONE | Smart scheduler thread leaks | `ingestion/smart_scheduler.py` — `_active_threads` set + counter, surfaced via `get_status` (PR #144) |
| H21 | ✅ DONE | _intelligence_loop 150+ lines nested in lifespan | No `_intelligence_loop` definition in `api/main.py` — extracted |

## Architecture Notes

- **Database:** [[PostgreSQL]] 15 + [[TimescaleDB]] on localhost:5432, db=griddb, user=grid
- **API:** [[FastAPI]] at port 8000, served via Cloudflare tunnel
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
