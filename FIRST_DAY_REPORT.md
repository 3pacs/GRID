# GRID First-Day Audit Report

> Compiled by exhaustive review of every file in the codebase.
> Categorized by severity. Fix order matters.

---

## SEVERITY 1: Will Bite You on Day 1

### 1.1 Twelve scripts hardcode wrong database credentials

These scripts use `dbname='griddb', user='grid', password='grid2026'` but docker-compose creates `grid` / `grid_user` / `changeme`:

| Script | Line |
|--------|------|
| `scripts/autoresearch.py` | 261 |
| `scripts/auto_regime.py` | 16 |
| `scripts/ai_analyst.py` | ~20 |
| `scripts/load_yfinance.py` | ~15 |
| `scripts/load_wave2.py` | ~15 |
| `scripts/load_wave3.py` | ~15 |
| `scripts/load_alt_data.py` | ~15 |
| `scripts/load_more_data.py` | ~15 |
| `scripts/load_ticker_deep.py` | ~15 |
| `scripts/bridge_to_pg.py` | ~10 |
| `scripts/bridge_crucix.py` | ~10 |
| `scripts/run_pipeline.py` | 11 |

**Fix:** Replace hardcoded `psycopg2.connect(...)` with `from db import get_engine` in all 12 scripts.

### 1.2 `autoresearch.py` and `auto_regime.py` hardcode sys.path

Both add `/home/grid/grid_v4/grid_repo/grid` to sys.path. Will crash on any other machine.

**Fix:** Remove hardcoded paths. Use relative imports or `from db import get_engine`.

### 1.3 FRED/BLS/yfinance pullers crash if source_catalog not seeded

These three pullers raise `RuntimeError` in `__init__` if their source isn't in `source_catalog`. All other pullers auto-create their source entry.

**Fix:** Either make them auto-create like the others, or ensure `db.py` seeds these three sources.

---

## SEVERITY 2: Security — Fix Before Any External Access

### 2.1 SQL injection in config router field names

`api/routers/config.py:91-96` and `:141-146` build SQL column names from user input via f-string. The allowlist (`allowed_fields`) partially mitigates this, but a safer pattern exists.

```python
# Current (risky if allowlist is wrong):
set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
text(f"UPDATE source_catalog SET {set_clauses} WHERE id = :id")

# Safer: validate field names explicitly against schema
```

### 2.2 Backtest chart endpoint has incomplete path traversal protection

`api/routers/backtest.py:115` strips `/` and `..` but doesn't handle encoded paths, null bytes, or symlinks.

**Fix:** Use `pathlib.Path.resolve()` and verify result is inside `_CHART_DIR`.

### 2.3 Default credentials ship in code

| What | Default | Where |
|------|---------|-------|
| DB password | `changeme` | `config.py:50`, `docker-compose.yml:17` |
| JWT secret | `""` (empty) | `config.py:97` |
| Master password | `""` (empty) | `config.py:96` |

Validators only reject these in `production` environment. `staging` and `development` accept them silently.

### 2.4 WebSocket auth leaks tokens to logs

`api/main.py:202` passes JWT as `?token=` query parameter. Visible in access logs, proxy logs, browser history.

### 2.5 Rate limiting resets on restart

`api/auth.py:26-29` uses in-memory dict. Multi-instance deployments have no shared rate limiting.

---

## SEVERITY 3: Silent Failures — Things That Look OK But Aren't

### 3.1 Scripts bypass PIT correctness

These files query `resolved_series` directly, bypassing `store/pit.py`:
- `scripts/ai_analyst.py`
- `scripts/autoresearch.py`
- `scripts/auto_regime.py`

Any analysis from these scripts could contain lookahead bias.

### 3.2 Six API key-dependent sources fail silently

Only `FRED_API_KEY` is validated at startup. These silently return empty data:
- `KOSIS_API_KEY` — Korean economic data
- `COMTRADE_API_KEY` — UN trade flows
- `JQUANTS_EMAIL/PASSWORD` — Japan equities (crashes on auth, not silent)
- `USDA_NASS_API_KEY` — Agricultural data
- `NOAA_TOKEN` — Vessel traffic
- `EIA_API_KEY` — Energy data

### 3.3 `pd.to_numeric(errors="coerce")` silently creates NaN

Used in multiple ingestion modules. Bad data becomes NaN without logging. Only FRED puller logs coercion count — others don't.

### 3.4 Workflow endpoints don't execute

`api/routers/workflows.py:70-91` returns `"accepted"` but doesn't actually run the workflow. Comment says "dispatched via CLI" but no queue exists.

### 3.5 Discovery jobs are in-memory only

`api/routers/discovery.py:20` stores jobs in a Python dict. All jobs disappear on API restart. No persistence layer.

### 3.6 Signal endpoint returns 200 OK on failure

`api/routers/signals.py:17-46` catches all exceptions and returns HTTP 200 with an `error` field inside the response body. Clients can't distinguish success from failure by status code.

### 3.7 NaN handling varies across modules

| Module | Strategy |
|--------|----------|
| `discovery/orthogonality.py:156` | `ffill(limit=5)` |
| `discovery/clustering.py:114` | `ffill().dropna()` |
| `features/lab.py` | Varies by transformation |
| FRED puller | Filter `.` values, coerce, log, dropna |
| yfinance puller | Silent `dropna()` |
| BLS puller | Parse or skip (no NaN) |

### 3.8 `_resolve_source_id()` auto-creates sources silently

22 pullers auto-create `source_catalog` entries on first run. No operator notification. Unknown sources can appear in the database with default trust scores.

---

## SEVERITY 4: Architecture Gaps — Won't Crash, But Limits You

### 4.1 Zero test coverage on 8 critical modules

| Module | Lines | Tests |
|--------|-------|-------|
| `normalization/resolver.py` | 260 | 0 |
| `normalization/entity_map.py` | 240 | 0 |
| `features/lab.py` | 612 | 0 |
| `discovery/orthogonality.py` | 407 | 0 |
| `discovery/clustering.py` | 447 | 0 |
| `validation/gates.py` | 203 | 0 |
| `governance/registry.py` | 314 | 0 |
| `inference/live.py` | 273 | 0 |

### 4.2 Hardcoded magic numbers everywhere

11+ thresholds that should be configurable:
- Conflict resolution: 0.5% fixed (false-positives on VIX/commodities)
- Z-score window: 252 days
- Slope window: 63 days
- Missing data dropout: 30%
- Correlation threshold: 0.8
- PCA variance: 85%
- Min journal entries for promotion: 20
- Dedup overlap: 1 hour (hardcoded in every puller)

### 4.3 Series/ticker lists hardcoded in Python

FRED (20 series), yfinance (35 tickers), BLS (5 series), OECD (11 countries), Comtrade (6 flows) — all hardcoded in module constants. Adding a series requires code change + redeploy.

### 4.4 `@lru_cache()` on database engine never clears

`api/dependencies.py:19-40` caches engine, PIT store, journal, model registry forever. Config changes require restart.

### 4.5 No database migration system

Schema changes are applied via `db.py` running `schema.sql`. No Alembic, no versioning, no rollback capability.

### 4.6 Frontend inconsistencies

- Error handling: 8 views fail silently, 4 show notifications, 3 show inline errors
- Loading states: 10 views have no skeleton/spinner during fetch
- Regime/verdict colors: duplicated in 5+ JSX files
- No pagination on any list view (fixed limits: 3, 20, 50, 100)
- Expandable rows (Models, Agents) are divs — not keyboard accessible

### 4.7 `.env.example` missing 15+ env vars

These exist in `config.py` but not in `.env.example`:
- `HYPERSPACE_*` (5 vars)
- `GRID_JWT_EXPIRE_HOURS`, `GRID_ALLOWED_ORIGINS`
- `AGENTS_*` (10 vars: ENABLED, LLM_PROVIDER, LLM_MODEL, API keys, schedule config)

---

## SEVERITY 5: Good to Know

### 5.1 `assert_no_lookahead()` is actually safe in current code

Despite ATTENTION.md #8 warning about no transaction rollback, the assertion is only called on read paths (before returning data), never mid-write. The gap is theoretical unless future code adds write-after-assertion patterns.

### 5.2 Core inference pipeline IS PIT-correct

`features/lab.py → discovery/ → inference/live.py → journal/log.py` all use `PITStore.get_pit()`. The lookahead risk is in scripts outside the pipeline (see 3.1).

### 5.3 LLM integration degrades gracefully

Both llama.cpp and Ollama clients return `None` when offline. Downstream code handles this. System runs without any LLM.

### 5.4 Journal immutability works correctly

Once an outcome is recorded, it cannot be overwritten. The only gap: if a wrong outcome is recorded, the only fix is to retire the model and start fresh. No correction mechanism.

### 5.5 Scheduler architecture is sound

v1 (FRED, yfinance, BLS, EDGAR) and v2 (international, trade, physical, altdata) run as daemon threads from API startup. Each puller is independently fault-tolerant.

---

## Quick Reference: What's Running When API Starts

```
uvicorn api.main:app
  ├── FastAPI app on :8000
  │     ├── 12 routers (67 endpoints)
  │     ├── Security headers middleware
  │     ├── CORS middleware
  │     └── PWA static file serving
  ├── WebSocket broadcast loop (10s interval)
  ├── Agent scheduler (if AGENTS_SCHEDULE_ENABLED)
  ├── Ingestion v1 thread (FRED/yfinance/BLS/EDGAR)
  └── Ingestion v2 thread (international/trade/physical/altdata)
```

```
llama.cpp server on :8080 (separate process)
  └── Hermes 8B model
        ├── /v1/chat/completions (briefings, reasoning)
        ├── /v1/embeddings (feature similarity)
        └── /health (monitoring)
```

```
PostgreSQL + TimescaleDB on :5432 (Docker)
  ├── raw_series (ingested data)
  ├── resolved_series (conflict-resolved, PIT-stamped)
  ├── feature_registry (feature catalog)
  ├── source_catalog (data source metadata)
  ├── decision_journal (immutable decision log)
  ├── model_registry (CANDIDATE→SHADOW→STAGING→PRODUCTION)
  ├── hypothesis_registry (auto-generated hypotheses)
  └── validation_results (backtest outputs)
```

---

## Recommended Fix Order

1. **Fix hardcoded DB credentials in 12 scripts** (Severity 1.1, 1.2) — 30 min
2. **Fix path traversal in backtest chart serving** (Severity 2.2) — 5 min
3. **Add missing env vars to .env.example** (Severity 4.7) — 10 min
4. **Fix scripts that bypass PIT** (Severity 3.1) — 1 hour
5. **Add `log.warning()` for NaN coercion** in all pullers (Severity 3.3) — 30 min
6. **Write tests for resolver.py and features/lab.py** (Severity 4.1) — 2 hours
7. **Extract hardcoded thresholds to config** (Severity 4.2) — 1 hour
8. **Standardize frontend error handling** (Severity 4.6) — 2 hours
