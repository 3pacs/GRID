# Concerns

Comprehensive audit of the GRID codebase, compiled from ATTENTION.md, CLAUDE.md, `.claude/rules/`, and direct code inspection.

---

## Critical Security Issues

### SQL Injection

- **`api/routers/regime.py:85-93`** — Originally flagged as using `.format()` for SQL INTERVAL with user-supplied `days` parameter. Current code uses `text()` with `:days` bind parameter and `make_interval(days => :days)`, so this appears **fixed**.
- **`journal/log.py:241`** — Originally flagged as string interpolation in interval clause. Current code uses `text()` with `:days` bind parameter, so this also appears **fixed**.
- **`api/routers/config.py:91-96`** — Dynamic SQL column names via f-string: `f"UPDATE source_catalog SET {set_clauses} WHERE id = :id"`. While column names come from a server-side whitelist (`allowed_fields = {"active", "priority_rank", "trust_score"}`), this pattern is fragile. If the whitelist is ever modified to include user input, it becomes injectable.
- **`api/routers/config.py:141-146`** — Same dynamic column f-string pattern for `feature_registry` updates, with whitelist `{"active", "transformation_version", "feature_family"}`.
- **`scripts/deploy_all.sh:195`** — `cur.execute(f'SELECT count(*) FROM {t}')` — table name via f-string. Table names come from a hardcoded list, but this is still unsafe practice.
- **`scripts/compute_coordinator.py:221`** — `f"UPDATE compute_jobs SET state=%s, {ts_col}=NOW() WHERE id=%s"` — column name via f-string. Validated against `_ALLOWED_TS_COLS` whitelist at line 219, but the pattern is fragile.

### Hardcoded Credentials and Weak Defaults

- **`config.py:50`** — `DB_PASSWORD` defaults to `"changeme"`. No validation rejects this in staging/production.
- **`api/auth.py:35`** — JWT secret defaults to empty string; falls back to `"dev-secret-change-me"` implicitly if `GRID_JWT_SECRET` env var is unset. Code now raises `RuntimeError` in non-development environments (line 38-39), but development mode silently uses a weak secret.
- **`ingestion/international/jquants.py:82`** — Credentials (email/password) sent in JSON body to external API. The password is stored as a plain-text config field `JQUANTS_PASSWORD` in `config.py:59`.

### Authentication Weaknesses

- **`api/main.py:202-210`** — WebSocket auth accepts JWT token via `?token=` query parameter. Tokens leak to server logs, proxy logs, and browser history. Should use subprotocol or first-message auth.
- **`api/auth.py:118-124`** — Rate limiting uses an in-memory `defaultdict` (`_login_attempts`). Resets on app restart and does not work across multiple instances. No persistent backing store (Redis, database).

### Missing Security Headers

- **`api/main.py:48-63`** — Security headers middleware is present and sets `X-Content-Type-Options`, `X-Frame-Options`, `X-XSS-Protection`, `Referrer-Policy`, and `Strict-Transport-Security`. However, **Content-Security-Policy (CSP) header is still missing**, which is the most impactful security header for XSS prevention.

---

## Data Integrity Risks

### PIT Correctness Gaps

- **`store/pit.py:191-215`** — `assert_no_lookahead()` raises `ValueError` but does NOT roll back the calling transaction. If called mid-inference, partial results could persist in the database. The caller must handle rollback.
- **`store/pit.py`** — Uses PostgreSQL-specific `DISTINCT ON` syntax. The entire PIT query engine is incompatible with SQLite or MySQL. This is by design but is a hard lock-in.
- **Vintage policy ambiguity** — `FIRST_RELEASE` vs `LATEST_AS_OF` produce different values for the same query. No enforcement or documentation ensures callers specify the intended policy consistently.

### NaN Handling Inconsistency

Different modules handle missing data with conflicting strategies:

- **`discovery/orthogonality.py:160`** — `ffill(limit=5)` then `dropna()`
- **`discovery/clustering.py:118`** — `ffill(limit=5).dropna()` (same pattern but inline)
- **`features/lab.py:96`** — `replace(0, np.nan)` for division safety; other transformations vary
- **`normalization/resolver.py:144`** — Uses `float("inf")` for division-by-zero case

Risk: the same feature can produce different values depending on which module processes it.

### Silent Data Coercion

- **`ingestion/fred.py:171-176`** — `pd.to_numeric(errors="coerce")` silently converts invalid data to NaN with no logging. Bad upstream data disappears without trace.

### Conflict Resolution Threshold

- **`normalization/resolver.py:132-146`** — Fixed 0.5% threshold (`CONFLICT_THRESHOLD`) for all features. High-volatility features (VIX, commodities) will false-positive as conflicts. Threshold is not configurable per feature or per family.

### Division by Zero

- **`normalization/resolver.py:139-144`** — Partially handled: produces `float("inf")` when reference is zero and comparison is non-zero, which then exceeds threshold. Edge case where both are zero yields `0.0` (correct). But `float("inf")` propagating downstream could cause issues.
- **`features/lab.py:96`** — `ratio()` replaces zero denominator with NaN, which is correct but undocumented.

### Bounds Checking

- **`journal/log.py:85-99`** — Now validates NaN and infinity for `state_confidence` and `transition_probability` (lines 87, 96). This appears **fixed** from the original ATTENTION.md item #21.

---

## Technical Debt

### Copy-Pasted Code Across Ingestion Modules

- **All ingestion modules** — `_resolve_source_id()` and `_row_exists()` methods are copy-pasted identically across every puller (`fred.py`, `bls.py`, `yfinance_pull.py`, `ecb.py`, `imf.py`, all international modules, etc.). Should be extracted into a `BasePuller` class.
- **`_resolve_source_id()`** auto-creates `source_catalog` entries if missing. Unknown sources can silently appear in the database without operator awareness.

### Deprecated Modules

- **`ingestion/scheduler_v2.py`** exists alongside `ingestion/scheduler.py`. Per CLAUDE.md, `scheduler.py` is authoritative, but `api/main.py:195` logs "Ingestion scheduler v2 started", indicating both are in use at runtime. Consolidation needed.

### Stale Cache

- **`api/dependencies.py:19-40`** — Four `@lru_cache()` decorators on `get_db_engine()`, `get_pit_store()`, `get_journal()`, `get_model_registry()`. Cache never clears. Configuration changes require full application restart.

### No Database Migration System

- No Alembic or equivalent. Schema changes require manual SQL against `schema.sql`. No version tracking for database state.

### No Dependency Lock File

- **`requirements.txt`** — Uses minimum version constraints (`>=`) with no lock file. Builds are not reproducible.

### Incomplete Recommendation Engine

- **`inference/live.py:154-216`** — `_generate_recommendation()` is a basic threshold stub. Does not handle all model types or regime-specific logic.

---

## Performance Issues

### N+1 Query Patterns

- **`api/routers/models.py:90-97`** — The comment says "single connection, avoids N+1" and validation results are fetched in a single query per model. However, if called for multiple models (e.g., a list endpoint), each model triggers its own query. No batch/JOIN approach exists.
- **`discovery/orthogonality.py:75-79`** — Feature name lookups use `WHERE id = ANY(:ids)` which is a single query. The original ATTENTION.md item about N+1 here may be resolved, though the pattern should be verified under load.

### Missing Database Indexes

- **`schema.sql`** — Missing indexes documented in ATTENTION.md #16:
  - `decision_journal(model_version_id)` — heavily queried
  - `decision_journal(outcome_recorded_at)` — outcome statistics queries
  - `resolved_series(feature_id, obs_date) WHERE conflict_flag = TRUE` — conflict reporting

### Connection Pool

- **No explicit pool configuration** visible in `db.py` or `config.py`. Default SQLAlchemy pool settings may be insufficient for production concurrency.

### Transition Matrix

- **`discovery/clustering.py:296-316`** — `_compute_transition_matrix()` uses `np.add.at(trans, (labels[:-1], labels[1:]), 1)` which is O(n), not O(n^2). The original ATTENTION.md item #28 about O(n^2) nested loops appears to have been **fixed** with this vectorized approach.

---

## Missing Tests

### Zero-Coverage Modules

These critical modules have no test files:

- `normalization/entity_map.py` — entity disambiguation
- `features/lab.py` — feature transformation engine
- `discovery/orthogonality.py` — orthogonality audit
- `discovery/clustering.py` — regime clustering
- `validation/gates.py` — promotion gate checkers
- `governance/registry.py` — model lifecycle state machine
- `inference/live.py` — live inference engine
- `hyperspace/` — all Hyperspace LLM modules
- `ollama/` — all Ollama integration modules
- `api/routers/config.py` — configuration endpoints
- `api/routers/discovery.py` — discovery endpoints

Note: Some previously zero-coverage modules now have test files:
- `tests/test_resolver.py` exists for `normalization/resolver.py`
- `tests/test_hyperspace.py` exists for `hyperspace/`
- `tests/test_journal.py` exists for `journal/log.py`
- `tests/test_international.py`, `test_trade.py`, `test_physical.py` exist for ingestion submodules

### Weak Test Coverage

- **`tests/test_api.py`** — Tests login flow but not protected endpoints with valid tokens, error cases, or edge cases.
- **No integration tests** — Nothing tests the full pipeline: ingestion -> resolution -> feature engineering -> inference.
- **No frontend tests** — No Jest, Vitest, or Cypress configuration in `pwa/`.

---

## Configuration Issues

### Missing API Key Validation

- **`config.py`** — Only `FRED_API_KEY` is validated at startup. The following keys are required by their respective modules but never validated:
  - `KOSIS_API_KEY` (Korean exports)
  - `COMTRADE_API_KEY` (UN trade data)
  - `JQUANTS_EMAIL` / `JQUANTS_PASSWORD` (Japan market data)
  - `USDA_NASS_API_KEY` (agricultural data)
  - `NOAA_TOKEN` (vessel/port data)
  - `EIA_API_KEY` (energy data)

### Hardcoded Defaults

- **`config.py:50`** — `DB_PASSWORD: str = "changeme"` with no environment-based rejection.
- **`config.py:46`** — `DB_HOST: str = "localhost"` — reasonable for dev, but no production override enforcement.

### PWA Build Dependency

- **`api/main.py:241-244`** — PWA static file serving assumes `pwa_dist/` or `pwa/` directory exists. If the PWA is not built, the application silently returns 404 for all frontend routes with no warning at startup.

---

## Known Bugs

### From ATTENTION.md (with current status)

| # | Description | Status |
|---|-------------|--------|
| 1 | SQL injection in `regime.py:85-93` | Appears fixed (uses bind params now) |
| 1 | SQL injection in `journal/log.py:241` | Appears fixed (uses bind params now) |
| 2 | Weak JWT secret default | Partially fixed (raises in non-dev, but dev mode still weak) |
| 3 | Default DB password `"changeme"` | Still present |
| 4 | Config duplication | Fixed per ATTENTION.md |
| 5 | WebSocket token in query param | Still present at `api/main.py:205` |
| 6 | In-memory rate limiting | Still present at `api/auth.py:118-124` |
| 8 | `assert_no_lookahead()` no rollback | Still present at `store/pit.py:191-215` |
| 10 | Missing security headers | Partially fixed (CSP still missing) |
| 15 | Fixed conflict threshold | Still present at `normalization/resolver.py:146` |
| 17 | No database migration system | Still missing |
| 18 | `@lru_cache()` never clears | Still present at `api/dependencies.py:19-40` |
| 20 | Division by zero in resolver | Handled with `float("inf")` at `resolver.py:144` but downstream propagation unclear |
| 21 | Missing NaN/infinity bounds check in journal | Appears fixed at `journal/log.py:85-99` |
| 25 | Auto-created source entries | Still present in all ingestion modules |
| 28 | O(n^2) transition matrix | Appears fixed with vectorized `np.add.at` at `clustering.py:311` |
| 32 | No graceful shutdown / startup failure | Still present: `api/main.py:137-140` warns but continues |
| 37 | PWA silent 404 if not built | Still present |
| 39 | Dual scheduler confusion | Still present: both `scheduler.py` and `scheduler_v2.py` exist and are started |

### From Code Inspection

- **`api/routers/config.py:91-96` and `141-146`** — Dynamic SQL column construction via f-string. While guarded by server-side whitelists, this pattern is a maintenance risk. If whitelists are ever expanded carelessly, SQL injection becomes possible.
- **`api/main.py:128`** — Uses deprecated `@app.on_event("startup")` pattern instead of FastAPI lifespan context manager.
- **`ingestion/international/akshare_macro.py`** — Fragile heuristic column detection (`_find_date_column()`, `_find_value_column()`) that will break when AKShare API changes column names.
