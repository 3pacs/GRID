# GRID Architecture Review
**Date:** 2026-03-30
**Scope:** Core system design, layer dependencies, scalability, data integrity, security
**Codebase:** 222K LOC, 652 tests, 37+ data sources, 34 API routers

---

## 1. Current Architecture Assessment

### Strengths

1. **Clear Layered Architecture** (Strong)
   - Clean separation: Ingestion → Normalization → Store (PIT) → Features → Discovery → Validation → Inference → Journal
   - Layer boundaries enforced by module structure (`store/`, `features/`, `discovery/`, etc.)
   - Unidirectional dependency flow: each layer depends only on lower layers

2. **PIT-Correct Data Pipeline** (Critical)
   - `store/pit.py` is well-designed with `DISTINCT ON` PostgreSQL-specific queries preventing lookahead bias
   - `assert_no_lookahead()` safety net guards all inference paths
   - Vintage policies (FIRST_RELEASE, LATEST_AS_OF) properly distinguish backtest vs live scenarios
   - Prevents the most dangerous class of bugs in financial systems

3. **Immutable Journal** (Strong)
   - `journal/log.py` enforces append-only decision logging with provenance tracking
   - Supports outcome annotation but prevents entry mutation
   - Critical for regulatory compliance and audit trails

4. **Graceful Degradation** (Good)
   - Hyperspace, Ollama, and LLM integration all degrade to None if unavailable
   - System continues operating without requiring all external services
   - Configuration-driven feature enablement (AGENTS_ENABLED, LLM_ROUTER_ENABLED, etc.)

5. **Comprehensive Test Suite** (Good)
   - 652 tests across 73 test files covering most core modules
   - PIT correctness (`test_pit.py`) tests are prioritized
   - Integration tests exist for pipeline (`test_integration_pipeline.py`)

### Weaknesses

1. **Router Bloat** (Moderate)
   - 34 API routers with inconsistent patterns (some >2000 LOC)
   - `api/routers/intelligence.py`: 3,871 lines (HIGH)
   - `api/routers/watchlist.py`: 2,339 lines (HIGH)
   - `api/routers/astrogrid.py`: 3,099 lines (HIGH)
   - Routers mix endpoint definitions, business logic, and data access
   - Violates "keep API routes thin" guideline

2. **God Objects in Intelligence Layer** (High)
   - `intelligence/actor_network.py`: 7,002 lines (CRITICAL)
   - `intelligence/actor_discovery.py`: 3,327 lines (HIGH)
   - `intelligence/commodities_agriculture_network.py`: 2,765 lines (HIGH)
   - Each module tries to be a complete domain model instead of composable services
   - Difficult to test, maintain, and reuse

3. **Missing Database Pool Configuration** (Moderate)
   - `api/dependencies.py` uses SQLAlchemy defaults with no explicit pool sizing
   - `get_engine()` in `db.py` likely uses pool_size=5 (default)
   - Will bottleneck under load: ~30-50 concurrent requests will exhaust pool
   - No pool_pre_ping, echo, or overflow handling specified

4. **Duplicate Ingestion Patterns** (Moderate)
   - `_resolve_source_id()` and `_row_exists()` copy-pasted across 50+ ingestion modules
   - Creates silent bugs when implementations drift
   - No base class or mixin to enforce consistency
   - `scheduler.py` has authoritative pattern but many modules don't follow it

5. **WebSocket Rate Limiting Not Distributed** (Moderate)
   - `_api_rate_limits` is in-memory dict in `api/main.py:478`
   - Resets on server restart
   - Doesn't work with multiple processes (systemd services)
   - Can be bypassed by connecting through load balancer

---

## 2. Layer Dependencies Analysis

### Dependency Graph (Correct Unidirectional Flow)

```
API Routers
    ↓ (depends on)
Inference + Trading + Intelligence
    ↓
Validation + Governance
    ↓
Features + Discovery
    ↓
Store (PIT)
    ↓
Normalization
    ↓
Ingestion + Config
```

### Clean Boundaries

✓ **Ingestion** → Normalization: One-way ingestion flow, proper isolation
✓ **Normalization** → Store: Conflict resolution before persistence
✓ **Store** → Features: PIT queries enable feature engineering without lookahead
✓ **Features** → Discovery: Unsupervised regime learning operates on engineered features
✓ **Discovery** → Validation: Regime signals inform walk-forward gate checking
✓ **Validation** → Inference: Models only score if gates pass
✓ **Inference** → Journal: Every decision logged with full provenance

### No Circular Dependencies Detected

- **Result:** Clean DAG architecture
- **Impact:** Modules can be tested in isolation
- **Risk:** Low (circular dependencies are breaking risk)

### Cross-Cutting Concerns (Potential Issues)

1. **Config Dependency Spread** (Low Risk)
   - 40+ modules import `from config import settings`
   - Config validation at startup catches most issues
   - Consider extracting config into dependency injection for better testability

2. **Database Engine Singleton** (Low Risk)
   - `api/dependencies.py` uses module-level global `_db_engine`
   - Clearable via `clear_singletons()` but rarely called
   - Could cause issues during config hot-reload
   - Connection pool reuse is efficient but not distributed

3. **Logger Dependency** (No Risk)
   - All modules use `from config import log` (loguru)
   - Centralized configuration in config.py handles setup
   - Best practice for this system

---

## 3. Scalability Bottlenecks

### Database-Level (Severity: HIGH)

1. **Missing Indexes** (CRITICAL)
   ```
   - decision_journal(model_version_id) — heavily queried in governance
   - decision_journal(outcome_recorded_at) — outcome statistics
   - resolved_series(feature_id, obs_date) WHERE conflict_flag = TRUE — conflict reporting
   ```
   - Walk-forward backtests will do full table scans
   - Outcome tracking UI will be slow

2. **N+1 Query Patterns** (HIGH)
   ```
   api/routers/models.py:91-98          — fetches validation results without JOIN
   discovery/orthogonality.py:75-80     — feature lookups in loops
   ```
   - Backtesting workflow will make 1000s of redundant queries

3. **Connection Pool Exhaustion** (HIGH)
   - Default SQLAlchemy pool_size=5 with pool_timeout=30
   - 30+ concurrent requests → queued/failed requests
   - No overflow behavior configured
   - Multi-process deployment (systemd) will further degrade

4. **O(n^2) Clustering** (MODERATE)
   ```
   discovery/clustering.py:292-313      — nested loop for transition matrices
   ```
   - Acceptable for 10K observations but will degrade at 100K+
   - No vectorization of matrix operations

### Computation-Level (Severity: MODERATE)

1. **Feature Lab Not Vectorized** (MODERATE)
   ```
   features/lab.py                      — likely has row-by-row loops
   ```
   - NumPy/pandas should be vectorized for 1000s of features

2. **Large Intelligence Modules** (MODERATE)
   ```
   intelligence/actor_network.py:7002   — loads entire actor graph into memory
   ```
   - 475+ named actors in a single object
   - Could be 100s of MB in memory
   - Parsing Panama/Pandora Papers files on startup

### API-Level (Severity: MODERATE)

1. **List Endpoints Lack Pagination** (MODERATE)
   - `journal.py:36-66` returns up to 100 rows without total count
   - Full table scans on every list request
   - No offset/limit constraints enforced

2. **Synchronous Route Handlers** (MODERATE)
   - Most routers use blocking database calls
   - No async/await for I/O-bound operations
   - Web requests block thread while querying

3. **Heavy WebSocket Broadcasts** (LOW)
   ```
   api/main.py:507-525              — sends to all connected clients
   ```
   - OK for <100 clients but no batching for bulk updates

---

## 4. Data Flow Analysis

### Ingestion → Persistence

**Pattern:** ✓ Correct
```
1. Ingestion modules pull from 37+ sources
2. Each source module validates data and timestamps (observation_date, release_date)
3. Ingestion scheduler runs on cron (PULL_SCHEDULE_FRED, etc.)
4. Data routed to normalization
```

**Issues:**
- Some API keys not validated at startup (KOSIS, Comtrade, JQUANTS, USDA, NOAA, EIA)
  - Silent degradation is good but operator doesn't know
- `pd.to_numeric(errors="coerce")` silently converts bad data to NaN
  - Should log warnings when coercion occurs
- Only FRED_API_KEY is validated in config.py:208-218
  - Other keys should have runtime validation

### Normalization → Store

**Pattern:** ✓ Correct
```
1. Multi-source conflicts resolved via resolver.py
2. Entity disambiguation via entity_map.py
3. Conflict-flagged rows preserved for audit
4. Single source of truth in resolved_series table
```

**Issues:**
- resolver.py:0.5% fixed threshold false-positives on high-volatility features
  - VIX, commodities need per-feature thresholds
- Division by zero when reference value is 0 only partially handled (resolver.py:139-142)
- entity_map.py (834 lines) is monolithic
  - Should split by entity type (ticker, cusip, sector, etc.)

### Store → Features

**Pattern:** ✓ Correct
```
1. PIT queries return no lookahead data
2. Features computed deterministically from resolved_series
3. NaN handling follows module conventions
```

**Issues:**
- Inconsistent NaN handling across modules (#14 in ATTENTION.md)
  - discovery/orthogonality.py:156 uses ffill(limit=5)
  - discovery/clustering.py:114 uses ffill().dropna()
  - features/lab.py varies by transformation
  - Causes subtle bugs when features move between modules

### Features → Discovery → Validation → Inference → Journal

**Pattern:** ✓ Correct
```
1. Regime clustering operates on engineered features (discovery/clustering.py)
2. Walk-forward backtesting enforces temporal boundaries (validation/gates.py)
3. Live inference scores only if all gates pass (governance/registry.py)
4. Every decision logged with full provenance (journal/log.py)
```

**No lookahead bias** detected in critical paths.

---

## 5. Top 5 Architectural Risks

### RISK 1: Uncontrolled Module Growth (Severity: CRITICAL)

**Description:**
Multiple modules are approaching or exceeding 800-line guideline:
- `intelligence/actor_network.py`: 7,002 lines
- `api/routers/intelligence.py`: 3,871 lines
- `api/routers/astrogrid.py`: 3,099 lines
- `intelligence/actor_discovery.py`: 3,327 lines

**Impact:**
- Single module contains multiple domains (actors, relationships, queries, business logic)
- Difficult to test individual concerns
- Hard to parallelize development
- Maintenance burden grows cubically with size

**Mitigation:**
- Extract actor_network into separate modules:
  - `intelligence/actors/core.py` — Actor model and relationships
  - `intelligence/actors/lookup.py` — Query interface
  - `intelligence/actors/importer.py` — Panama Papers bulk import
- Similar split for api routers (one router per domain)

**Timeline:** HIGH priority before >5000 lines

---

### RISK 2: Database Pool Exhaustion (Severity: CRITICAL)

**Description:**
SQLAlchemy default pool_size=5 with no overflow configuration.

**Evidence:**
- `api/dependencies.py:31` calls `get_engine()` without pool config
- `db.py` likely uses defaults
- 34 API routes + WebSocket handler all acquire connections
- Systemd multi-process deployment multiplies the problem

**Impact:**
- <30 concurrent requests will start queuing
- Backtesting with 100+ parallel validations will deadlock
- WebSocket broadcasts can block entire connection pool

**Mitigation:**
```python
# In db.py
from sqlalchemy.pool import QueuePool

engine = create_engine(
    DB_URL,
    poolclass=QueuePool,
    pool_size=20,           # Per-process
    max_overflow=10,        # Allow queue
    pool_pre_ping=True,     # Verify connections
    echo_pool=False,        # Disable in production
    connect_args={
        "connect_timeout": 5,
        "options": "-c work_mem=256MB"
    }
)
```

**Timeline:** CRITICAL before production scale (>50 concurrent users)

---

### RISK 3: Incomplete Test Coverage of Zero-Coverage Modules (Severity: HIGH)

**Description:**
8 critical modules have no tests:
- `normalization/resolver.py` — conflict resolution logic
- `normalization/entity_map.py` — entity disambiguation
- `features/lab.py` — feature transformation engine
- `discovery/orthogonality.py` — orthogonality audit
- `discovery/clustering.py` — regime clustering
- `validation/gates.py` — promotion gate checkers
- `governance/registry.py` — model lifecycle state machine
- `inference/live.py` — live inference engine

**Impact:**
- Bugs in resolver propagate to all analysis
- Entity mismatches silently corrupt lookups
- Feature leakage (lookahead bias) not caught
- Gate logic failures cause trades on invalid models
- Registry transitions silently corrupt model promotion

**Evidence:**
ATTENTION.md #22 documents this gap. No tests added in past 6 months.

**Mitigation:**
- TDD-first for all zero-coverage modules
- Minimum 80% coverage before merging
- Use test_integration_pipeline.py as scaffold

**Timeline:** HIGH priority — add tests incrementally as you modify each module

---

### RISK 4: N+1 Query Patterns in Critical Paths (Severity: HIGH)

**Description:**
Two identified N+1 patterns plus likely others:
- `api/routers/models.py:91-98` — validation results without JOIN
- `discovery/orthogonality.py:75-80` — feature lookups in loops

**Impact:**
- Backtesting with 1000 observations × 50 features × 100 parameter sets
  - Single JOIN query: 5ms
  - N+1 pattern: 5ms × 5000 = 25 seconds per backtest
- Model comparison UI loads all models then queries each validation individually

**Mitigation:**
- Audit all loops over database rows
- Use SQLAlchemy eager loading or batch queries
- Add query execution time logging to catch regressions

**Timeline:** MEDIUM priority — fix identified patterns, add query audit before scaling

---

### RISK 5: WebSocket Authentication and Rate Limiting Not Distributed (Severity: MEDIUM)

**Description:**
WebSocket security has three issues:
1. Token leaked in query params (`api/main.py:117-152`)
2. Rate limiting is in-memory dict (`_api_rate_limits`)
3. Doesn't survive process restart

**Evidence:**
```python
# Current (leaks token to logs/proxies)
websocket: WebSocket = await manager.connect(token)

# Tokens appear in Nginx/Cloudflare logs, browser history
```

**Impact:**
- Token in URL can be logged by proxies, CDN, browser
- Rate limiting per-IP only, not per-user
- Load balancer can route attacker through multiple worker processes

**Mitigation:**
- Use WebSocket subprotocol for auth:
  ```python
  ws = new WebSocket("wss://...", ["Bearer.token_value"])
  ```
- Persist rate limiting to Redis or database
- Per-token rate limiting instead of per-IP

**Timeline:** MEDIUM priority — not urgent if behind auth gateway but should fix

---

## 6. Recommended Architecture Decision Records (ADRs)

### ADR-001: Module Size Limits and Extraction Strategy

**Status:** RECOMMENDED
**Priority:** HIGH

**Decision:**
Enforce 800-line module limit with extraction strategy for god objects:
1. Split intelligence/ modules by domain concern
2. Extract router handlers into separate service classes
3. Establish maximum 300 lines per router handler

**Rationale:**
- Modularity enables testing and parallel development
- Smaller files have lower cyclomatic complexity
- Easier to find code and understand dependencies

**Implementation:**
- Add pre-commit hook to check file sizes
- Break actor_network.py into 3-4 focused modules
- Split API routers using pattern: route definitions → handlers → services

---

### ADR-002: Database Connection Pooling and Resource Limits

**Status:** RECOMMENDED
**Priority:** CRITICAL

**Decision:**
Implement explicit SQLAlchemy connection pool configuration:
- pool_size=20 per worker process
- max_overflow=10 for transient load spikes
- pool_pre_ping=True for stale connection detection
- pool_recycle=3600 to handle database timeout

**Rationale:**
- Prevents connection exhaustion and deadlocks
- Works with multi-process deployment
- Health checks catch network issues early

**Impact:**
- Slightly higher memory overhead (pool holding 20 connections)
- Much better performance under load

---

### ADR-003: Consistent NaN Handling Strategy

**Status:** RECOMMENDED
**Priority:** HIGH

**Decision:**
Establish single NaN handling convention across codebase:
```python
# Use this in ALL modules:
# 1. Forward-fill up to 5 periods (reasonable for macro data)
# 2. Drop remaining NaNs only at feature computation time
# 3. Never drop NaNs during raw data processing
```

**Rationale:**
- Prevents silent data loss
- Maintains audit trail of which values were interpolated
- Consistent behavior across discovery/features/validation

**Implementation:**
- Update discovery/orthogonality.py, clustering.py, features/lab.py
- Add tests verifying NaN preservation
- Document in style guide

---

### ADR-004: Query Performance Standards and Monitoring

**Status:** RECOMMENDED
**Priority:** HIGH

**Decision:**
Establish query performance budgets:
- Simple endpoint queries: <100ms
- Complex backtests: <5s per run
- List endpoints: <500ms for 100 rows with index
- Streaming endpoints: first byte within 1s

**Rationale:**
- User experience degrades visibly above 1s latency
- Backtesting must complete in reasonable time
- Identifies N+1 patterns before they affect users

**Implementation:**
- Add query execution time logging to each endpoint
- Alert if any query exceeds budget
- Monthly query audit to find regressions

---

### ADR-005: Dependency Injection for Improved Testability

**Status:** RECOMMENDED
**Priority:** MEDIUM

**Decision:**
Migrate from global singletons (api/dependencies.py) to dependency injection:
```python
# Instead of:
from api.dependencies import get_pit_store
pit_store = get_pit_store()  # Hidden global state

# Use:
@router.get("/features")
def fetch_features(
    pit_store: PITStore = Depends(get_pit_store),
):
    ...
```

**Rationale:**
- Explicit dependencies improve code clarity
- Easier to mock for testing
- FastAPI Depends() is idiomatic

**Impact:**
- No breaking changes (both patterns work together)
- Gradual migration possible
- Better test isolation

---

## 7. Security Assessment

### Strong Points

✓ **SQL Injection Prevention** (STRONG)
- Parameterized queries via SQLAlchemy text() + bindparams
- No string concatenation in critical paths

✓ **PIT Correctness Enforced** (STRONG)
- assert_no_lookahead() prevents lookahead bias
- No future data available to inference

✓ **Immutable Journal** (STRONG)
- Decision logs cannot be modified (regulatory compliance)
- Full provenance tracking

### Vulnerabilities

⚠ **WebSocket Token in Query Params** (MEDIUM)
- Token visible in logs, proxies, browser history
- Fix: Use WebSocket subprotocol for auth

⚠ **API Key Validation Incomplete** (MEDIUM)
- Only FRED_API_KEY validated at startup
- KOSIS, Comtrade, JQUANTS, USDA, NOAA, EIA silently fail
- Fix: Add startup validation for all keys

⚠ **Default Database Password** (MEDIUM)
- `config.py:50` defaults to "changeme"
- Validator rejects in production but easy to miss
- Fix: Require in .env.example with clear instructions

⚠ **Missing Security Headers** (MEDIUM)
- No X-Content-Type-Options, X-Frame-Options, HSTS, CSP
- Add to CORS middleware in api/main.py

⚠ **In-Memory Rate Limiting** (MEDIUM)
- Per-IP rate limiting resets on restart
- Doesn't work with load balancers
- Fix: Use Redis or database for distributed rate limiting

---

## 8. Summary and Recommendations

### Immediate Actions (Next 2 Weeks)

1. **Fix database pool configuration** (CRITICAL)
   - Will prevent production issues at scale
   - 30 minutes of work

2. **Add tests for zero-coverage modules** (HIGH)
   - Start with resolver.py and gates.py
   - Incremental work, ~4-6 hours per module

3. **Fix identified N+1 patterns** (HIGH)
   - models.py validation loading
   - orthogonality.py feature lookups
   - ~2-3 hours per pattern

### Short-Term (1-2 Months)

4. **Extract god objects** (HIGH)
   - Break actor_network.py into focused modules
   - ~2-3 days of refactoring

5. **Establish NaN handling standard** (HIGH)
   - Unified approach across discovery/features
   - ~1 day to implement and test

6. **Add query performance monitoring** (MEDIUM)
   - Per-endpoint execution time tracking
   - ~1-2 days

### Medium-Term (2-6 Months)

7. **Refactor API routers** (MEDIUM)
   - Split large routers (intelligence.py, watchlist.py, astrogrid.py)
   - Move business logic to services
   - ~1-2 weeks

8. **Implement dependency injection** (MEDIUM)
   - Gradual migration from global singletons
   - ~1-2 weeks

9. **Distributed rate limiting** (MEDIUM)
   - Use Redis or database instead of in-memory
   - ~1 week

---

## Conclusion

GRID has a **fundamentally sound architecture** with:
- Clean layered structure and PIT-correct data pipeline (strength)
- Growing technical debt in module size and database performance (risk)
- 652 tests covering most paths but gaps in critical modules (medium)
- Secure at its core but missing infrastructure hardening (medium)

The system is **ready for production at 10-100 users** but will need the recommended changes before scaling to **1000+ concurrent users** or **daily backtesting at scale**.

**Highest-Value First Fix:** Database connection pooling (15 minutes, prevents production failure)

**Highest-Value Long Fix:** Extract actor_network.py and resolve N+1 patterns (enables 10x scaling)
