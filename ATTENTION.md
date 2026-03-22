# GRID Codebase — Granular Attention List

Complete audit of every issue, gap, and improvement opportunity across the codebase.

---

## CRITICAL — Fix Immediately

### 1. SQL Injection in API Routes
- **`api/routers/regime.py:85-93`** — String `.format()` used for SQL INTERVAL with user-supplied `days` parameter. Use parameterized query instead.
- **`journal/log.py:241`** — Same pattern: string interpolation in SQL interval clause.

### 2. Weak JWT Secret Default
- **`api/auth.py:35`** — Default JWT secret is `"dev-secret-change-me"` if `GRID_JWT_SECRET` not set. Should raise an error in non-development environments, not silently use a weak default.

### 3. Default Database Password
- **`config.py:50`** — DB_PASSWORD defaults to `"changeme"`. Add validation that rejects this default in staging/production.

### 4. Config Duplication (FIXED)
- **`config.py:71-93`** — Auth fields and External API Keys were defined twice. The second definition silently overwrote the first. **Fixed in this commit** — removed duplicates and added Ollama config.

---

## HIGH — Fix Soon

### 5. WebSocket Token in Query Parameter
- **`api/main.py:117-152`** — WebSocket auth accepts token via `?token=` query parameter. Tokens appear in server logs, proxy logs, and browser history. Use a subprotocol or first-message auth pattern instead.

### 6. In-Memory Rate Limiting
- **`api/auth.py:110-120`** — Login rate limit state is an in-memory dict (`_login_attempts`). Resets on app restart and doesn't work across multiple instances. Use Redis or database-backed rate limiting for production.

### 7. Missing API Key Validation
- **`config.py`** — Only `FRED_API_KEY` is validated at startup. These keys are also required but never validated:
  - `KOSIS_API_KEY` (Korean exports — critical leading indicator)
  - `COMTRADE_API_KEY` (UN trade data)
  - `JQUANTS_EMAIL` / `JQUANTS_PASSWORD` (Japan market data)
  - `USDA_NASS_API_KEY` (agricultural data)
  - `NOAA_TOKEN` (vessel/port data)
  - `EIA_API_KEY` (energy data)

### 8. PIT Lookahead Safety
- **`store/pit.py:191-215`** — `assert_no_lookahead()` raises ValueError but doesn't roll back the calling transaction. If called mid-inference, partial results could persist.

### 9. Missing Response Pagination
- **`api/routers/journal.py:36-66`** — Limits to 100 rows but doesn't return total count. Client can't know if there are more results. Need proper offset/limit with total.

### 10. Missing Security Headers
- **`api/main.py`** — No security headers configured:
  - `X-Content-Type-Options: nosniff`
  - `X-Frame-Options: DENY`
  - `Strict-Transport-Security` (for HTTPS)
  - `Content-Security-Policy`

---

## MEDIUM — Improve When Possible

### 11. Duplicate Code Across Pullers
- **All ingestion modules** — `_resolve_source_id()` and `_row_exists()` methods are copy-pasted across every puller (fred.py, bls.py, yfinance_pull.py, ecb.py, imf.py, etc.). Extract a `BasePuller` class.

### 12. Missing Retry Logic
- **`ingestion/fred.py`** — No retry/backoff for transient API failures (timeouts, rate limits).
- **`ingestion/bls.py`** — 60-second timeout but no backoff.
- **`ingestion/ecb.py`** — Has `@retry` decorator but only 3 attempts with no jitter (thundering herd risk).
- **Most international modules** — No retry at all.

### 13. Silent Data Coercion
- **`ingestion/fred.py:171-176`** — `pd.to_numeric(errors="coerce")` silently converts invalid data to NaN. Should log warnings when coercion occurs.

### 14. NaN Handling Inconsistency
Different modules handle NaN/missing data differently:
- `discovery/orthogonality.py:156` — `ffill(limit=5)`
- `discovery/clustering.py:114` — `ffill().dropna()`
- `features/lab.py` — Varies by transformation
- **Risk**: Same feature can produce different values depending on which module processes it. Standardize.

### 15. Conflict Resolution Threshold
- **`normalization/resolver.py:132-146`** — Uses fixed 0.5% threshold for all features. High-volatility features (VIX, commodities) will false-positive as conflicts. Make threshold configurable per feature or per family.

### 16. Missing Database Indexes
- **`schema.sql`** — Missing indexes that would improve query performance:
  - `decision_journal(model_version_id)` — heavily queried
  - `decision_journal(outcome_recorded_at)` — for outcome statistics
  - `resolved_series(feature_id, obs_date) WHERE conflict_flag = TRUE` — for conflict reporting

### 17. No Database Migration System
- No Alembic, Flyway, or any migration tracking. Schema changes require manual SQL. Add Alembic for versioned migrations.

### 18. Stale Cache Risk
- **`api/dependencies.py:19-40`** — Uses `@lru_cache()` for database engine but cache never clears. Config changes require restart.

### 19. Incomplete Recommendation Engine
- **`inference/live.py:154-216`** — `_generate_recommendation()` is a basic threshold stub. Only implements simple scoring, doesn't handle all model types or regime-specific logic.

### 20. Division by Zero Edge Cases
- **`normalization/resolver.py:139-142`** — Incomplete handling when reference value is 0.
- **`features/lab.py:96`** — `ratio()` handles it correctly, but no comment explaining why.

### 21. Missing Bounds Checking
- **`journal/log.py:85-93`** — Validates 0-1 range for confidence/probability but doesn't check for NaN or infinity. NaN could be stored silently.

---

## TEST COVERAGE GAPS

### 22. Modules Without Tests
These modules have **zero test coverage**:
- `normalization/resolver.py` — Critical conflict resolution logic
- `normalization/entity_map.py` — Entity disambiguation
- `features/lab.py` — Feature transformation engine
- `discovery/orthogonality.py` — Orthogonality audit
- `discovery/clustering.py` — Regime clustering
- `validation/gates.py` — Promotion gate checkers
- `governance/registry.py` — Model lifecycle state machine
- `inference/live.py` — Live inference engine
- `hyperspace/` — All Hyperspace modules
- `ollama/` — All Ollama modules (new)
- `ingestion/altdata/` — GDELT, Opportunity Insights, NOAA AIS
- `ingestion/international/` — All 18 international modules
- `ingestion/physical/` — VIIRS, USDA, Patents, EU KLEMS, OFR
- `ingestion/trade/` — Comtrade, CEPII, Atlas ECI, WIOD
- `api/routers/config.py` — Configuration endpoints
- `api/routers/discovery.py` — Discovery endpoints

### 23. Existing Test Quality
- **`tests/test_api.py`** — Only ~100 lines; tests login but not protected endpoints with valid tokens. No error case testing.
- **`tests/test_pit.py`** — Tests exist but need verification that all edge cases are covered (empty results, boundary dates, mixed vintage policies).
- **No integration tests** — Nothing tests the full pipeline from ingestion → resolution → feature engineering → inference.

---

## INTERNATIONAL INGESTION — SPECIFIC ISSUES

### 24. Fragile Column Detection
- **`ingestion/international/akshare_macro.py`** — `_find_date_column()` and `_find_value_column()` use heuristic name matching. Will break when AKShare API changes column names.

### 25. Auto-Created Source Entries
- **All international modules** — `_resolve_source_id()` auto-creates source_catalog entries if they don't exist. This means unknown sources can appear in the database without operator awareness.

### 26. Unaudited International Modules
These modules exist but weren't fully verified for completeness:
- `bis.py`, `eurostat.py`, `kosis.py`, `mas.py`, `oecd.py`, `rbi.py`, `jquants.py`, `abs_au.py`, `dbnomics.py`, `bcb.py`

---

## PERFORMANCE

### 27. N+1 Query Patterns
- **`api/routers/models.py:91-98`** — Fetches validation results without JOIN; separate query per row.
- **`discovery/orthogonality.py:75-80`** — Feature lookups in loops could be batched.

### 28. Inefficient Transition Matrix
- **`discovery/clustering.py:292-313`** — O(n^2) nested loop for transition matrix computation. Will be slow for >10K observations.

### 29. Missing Connection Pool Configuration
- No explicit connection pool sizing visible. Default SQLAlchemy pool may be insufficient for production load.

---

## DEPLOYMENT & OPERATIONS

### 30. Incomplete Health Checks
- **`api/routers/system.py:30-39`** — Health endpoint checks database connectivity but not:
  - Feature registry populated (>0 features)
  - Recent data pull success
  - Schema version compatibility
  - Connection pool health
  - Ollama/Hyperspace availability

### 31. No Alerting System
Missing monitoring and alerting for:
- Failed data pulls (by source, with frequency)
- API 5xx error rates
- Database connection pool exhaustion
- Model staleness (no inference in 24h)
- Journal entry failures
- Data quality degradation (rising NaN rates)

### 32. Missing Graceful Shutdown
- **`api/main.py:103-111`** — Warns if database unavailable at startup but continues. First API request will fail with a cryptic error. Should fail fast or serve a degraded health status.

### 33. No Dependency Lock File
- **`requirements.txt`** — Uses minimum version constraints (`>=`) but no lock file. Builds are not reproducible. Add `requirements.lock` or use Poetry.

---

## FEATURE ENGINEERING GAPS

### 34. Missing Feature Importance Tracking
- No mechanism to track which features actually contributed to model performance over time.

### 35. Transformation Version Mismatch Risk
- **`schema.sql:72`** — `transformation_version` exists in feature_registry but no validation that the model's version matches the current version.

### 36. Incomplete Feature Families
- **`schema.sql:67-68`** — Feature family CHECK constraint includes `'earnings'` family but no features with this family exist in seed data.

---

## FRONTEND (PWA)

### 37. Build Dependency
- **`api/main.py:156-177`** — PWA static file serving assumes `pwa_dist/` or `pwa/` directory exists. If PWA isn't built, returns 404 silently.

### 38. No PWA Test Suite
- No frontend tests visible (no Jest, Vitest, or Cypress configuration).

---

## DOCUMENTATION

### 39. Scheduler Confusion
- Two scheduler files exist: `scheduler.py` and `scheduler_v2.py`. Unclear which is authoritative. Document or consolidate.

### 40. PostgreSQL Dependency Undocumented
- **`store/pit.py`** uses `DISTINCT ON` which is PostgreSQL-specific. This critical requirement is not mentioned in README prerequisites (it says PostgreSQL but doesn't say "PostgreSQL required, not compatible with MySQL/SQLite").

### 41. Missing Architecture Diagram
- README has a text table but no visual architecture diagram showing data flow from ingestion → resolution → features → discovery → inference → journal.

---

## PRIORITY ORDER

**Week 1**: Items 1-4 (security critical), Items 22-23 (test foundation)
**Week 2**: Items 5-10 (security + data integrity)
**Week 3**: Items 11-16 (code quality + reliability)
**Week 4**: Items 17-21 (infrastructure + edge cases)
**Ongoing**: Items 24-41 (incremental improvements)
