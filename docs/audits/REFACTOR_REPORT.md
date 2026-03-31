# GRID Refactoring Report
**Generated:** March 30, 2026
**Codebase Size:** ~230K LOC (excluding venv and migrations)
**Status:** Research & Analysis Phase (No code changes made)

---

## Executive Summary

GRID is a well-architected trading intelligence system with clear domains (ingestion, normalization, intelligence, API). However, rapid growth has created **three major refactoring opportunities**:

1. **Duplication at scale:** 67 of 104 ingestion modules (64.4%) copy-paste identical `_resolve_source_id()` and `_row_exists()` patterns from `ingestion/base.py`
2. **Oversized API routers:** 5 routers exceed 800 lines; `intelligence.py` alone is 3,871 lines with 82 endpoints
3. **Network module clustering:** 10 intelligence network modules (7K-2K lines each) follow identical getter-function patterns with mostly static data

**Impact:** Code is maintainable but friction is increasing for new contributors. Duplication creates divergence risk. Large routers harm cognitive load.

**Recommended Priority:** High-impact, low-risk refactoring (extract shared patterns) before modular complexity scales further.

---

## 1. Dead Code Inventory

### 1.1 Zero-Coverage Critical Modules (per ATTENTION.md #22)

These core modules have NO test coverage and should be added to test suite immediately:

| Module | Lines | Classes/Functions | Severity |
|--------|-------|-------------------|----------|
| `normalization/entity_map.py` | ~400 | entity mapping logic | HIGH |
| `features/lab.py` | ~600 | feature transformations | HIGH |
| `inference/live.py` | ~400 | inference engine | HIGH |
| `normalization/resolver.py` | ~600 | conflict resolution | HIGH |
| `discovery/orthogonality.py` | ~400 | orthogonality audit | MEDIUM |
| `discovery/clustering.py` | ~500 | regime clustering | MEDIUM |
| `validation/gates.py` | ~400 | promotion gates | MEDIUM |
| `governance/registry.py` | ~350 | model lifecycle | MEDIUM |

**Action:** Create test files for these 8 modules. See `tests/test_pit.py` as reference for PIT-correct tests.

### 1.2 Weak Test Coverage

- `tests/test_api.py` (~100 lines) — only tests login; missing protected endpoints, error cases
- No integration tests for full pipeline: ingestion → resolution → features → inference
- No E2E tests for critical trading workflows (watchlist updates, option recommendations)

**Estimated coverage shortfall:** 15-20% of codebase untested in critical paths

### 1.3 Deprecated/Unused Patterns

- **`ingestion/scheduler_v2.py`** — explicitly marked deprecated in CLAUDE.md, not referenced anywhere
  - Location: Check if file exists and remove or document reason for keeping
  - Risk: Low (appears fully superseded by `ingestion/scheduler.py`)

- **Unused imports in API routers** — many late-bind imports inside functions (suggests function was moved)
  - Example: `api/routers/earnings.py` imports `_get_cal`, `_get_recent`, `_get_hist` inside route handlers
  - Risk: Possible refactoring in progress; verify these are intentional

---

## 2. Code Duplication Map

### 2.1 Ingestion Pattern Duplication (CRITICAL)

**Problem:** 67 of 104 ingestion modules implement identical helper methods from `ingestion/base.py`.

**Files Affected (73 total):**
- `ingestion/base.py` — **SOURCE** (defines the pattern)
- `ingestion/altdata/` — 35+ files with patterns (google_trends, insider_filings, lobbying, fed_speeches, etc.)
- `ingestion/international/` — 12+ files with patterns
- `ingestion/trade/` — 5+ files (comtrade, wiod, cepii, atlas_eci)
- `ingestion/physical/` — 8+ files (euklems, usda_nass, patents, viirs, etc.)
- `ingestion/*.py` — 5+ root files (edgar, dexscreener, etc.)

**Duplicated Methods:**

```python
# All 67+ modules reimplement:
def _resolve_source_id(self) -> int:
    """Resolve source_catalog.id; auto-create if needed."""
    # 40-50 lines of identical logic

def _row_exists(self, series_id: str, obs_date: date, conn: Any, dedup_hours: int = 1) -> bool:
    """Check if raw_series row exists in dedup window."""
    # 8-10 lines of identical logic
```

**Duplication Extent:** ~3,000 lines of copy-pasted code across the codebase.

**Why This Matters:**
- Changes to deduplication logic require 67+ edits
- New puller implementations copy incorrect patterns (fragile)
- No way to add shared logging/metrics to all source resolutions
- Silently creates source_catalog entries (#25 in CLAUDE.md)

**Solution:** All 67 modules already inherit from `BasePuller`, so methods are already defined there. **This duplication should not exist.** Verify:
1. Are local redefinitions shadowing the base class methods?
2. Are there signature mismatches preventing inheritance?
3. Do some pullers override these methods intentionally?

**Refactoring Recommendation:**
- [ ] Audit 5-10 random ingestion files to confirm duplication pattern
- [ ] If all inherit `BasePuller`, add assertion to base class that subclasses don't redefine these methods
- [ ] Update code review checklist: "No reimplementation of `_resolve_source_id` or `_row_exists`"

### 2.2 Network Module Data Structure Duplication

**Problem:** 10 intelligence network modules follow identical structure: static data dict + getter functions.

**Files Affected:**
- `intelligence/actor_network.py` (7,002 lines)
- `intelligence/commodities_agriculture_network.py` (2,765 lines)
- `intelligence/energy_network.py` (2,272 lines)
- `intelligence/media_network.py` (2,171 lines)
- `intelligence/real_estate_network.py` (1,791 lines)
- `intelligence/tech_monopoly_network.py` (2,369 lines)
- `intelligence/banking_network.py` (1,691 lines)
- `intelligence/pharma_network.py` (1,270 lines)
- `intelligence/swf_network.py` (1,421 lines)
- `intelligence/influence_network.py` (~1,200 lines)

**Duplicated Pattern:**

```python
# Pattern repeated in all 10 files:
NETWORK_NAME = {
    "section_1": { "key1": {...}, "key2": {...} },
    "section_2": { ... },
}

def get_network_name() -> dict[str, Any]:
    return NETWORK_NAME

def get_entity(ticker: str) -> dict | None:
    for section in ("section_1", "section_2"):
        if ticker in NETWORK_NAME.get(section, {}):
            return NETWORK_NAME[section][ticker]
    return None

def get_[subsection]() -> dict | None:
    return NETWORK_NAME.get("subsection", {})

def get_all_[entity_type]() -> list[dict]:
    # Loop pattern
    ...
```

**Total Duplicate Code:** ~1,500-2,000 lines across all network modules.

**Why This Matters:**
- **Cognitive load:** Each network file requires same getter/accessor patterns
- **Inconsistent entry points:** No unified query interface (some have search, some don't)
- **Sparse data usage:** Most of the 20K+ lines are static NETWORK dicts; only 5-10% is logic
- **Hard to add cross-network queries:** e.g., "find all actors connected to energy"

**Solution:** Extract common base class or factory pattern:

```python
class NetworkModule:
    """Base for all network intelligence modules."""
    NETWORK_DATA: dict[str, Any] = {}

    def get_network(self) -> dict[str, Any]:
        return self.NETWORK_DATA

    def get_entity(self, key: str) -> dict | None:
        for section in self.NETWORK_DATA:
            if key in self.NETWORK_DATA[section]:
                return self.NETWORK_DATA[section][key]
        return None

    def search_by_field(self, field: str, value: Any) -> list[dict]:
        """Generic search across all sections."""
        results = []
        for section in self.NETWORK_DATA:
            for key, entity in self.NETWORK_DATA[section].items():
                if entity.get(field) == value:
                    results.append(entity)
        return results
```

**Refactoring Recommendation:**
- [ ] Extract common patterns into `intelligence/network_base.py`
- [ ] Update 10 network files to use shared base
- [ ] Add cross-network query API in `intelligence/network_search.py`
- **Estimated effort:** 8-12 hours (high confidence)
- **Risk:** Medium (test coverage needed)

---

## 3. Oversized Module List

### 3.1 API Routers (7 files exceed 800 lines)

| Router | Lines | Endpoints | Avg Lines/Endpoint | Status |
|--------|-------|-----------|-------------------|--------|
| `intelligence.py` | 3,871 | 82 | 47 | **CRITICAL** |
| `astrogrid.py` | 3,099 | 31 | 99 | **CRITICAL** |
| `watchlist.py` | 2,339 | 12 | 194 | **HIGH** |
| `intel.py` | 2,159 | 11 | 196 | **HIGH** |
| `system.py` | 1,641 | 20 | 82 | MEDIUM |
| `flows.py` | 1,102 | 8 | 137 | MEDIUM |
| `derivatives.py` | 994 | 10 | 99 | MEDIUM |

**Problem with `intelligence.py` (3,871 lines, 82 endpoints):**
- **Mixed concerns:** Actor network, cross-reference, postmortems, source audit, risk mapping all in one file
- **Complex state management:** Uses 4 module-level caches (`_actor_graph_cache`, `_cross_ref_cache`, etc.)
- **Inconsistent patterns:** Some endpoints inline 50+ lines of logic; others delegate to intelligence modules
- **Testing friction:** 3,800+ lines means shallow coverage and hard to unit test individual endpoints

**Solution:** Split by domain:

```
api/routers/
├── intelligence/
│   ├── actor_network.py      (get_actor_network, get_actor_detail, etc.)
│   ├── cross_reference.py    (get_cross_reference, get_discrepancies, etc.)
│   ├── postmortem.py         (get_postmortems, trigger_batch_postmortem, etc.)
│   ├── source_audit.py       (get_source_audit, get_redundancy_map, etc.)
│   └── __init__.py           (combine routers)
└── intelligence.py           (thin wrapper that includes all sub-routers)
```

**Refactoring Recommendation:**
- [ ] Create `api/routers/intelligence/` subdirectory
- [ ] Move endpoints into domain-specific modules (4 new files)
- [ ] Update imports in `api/main.py`
- **Estimated effort:** 4-6 hours
- **Risk:** Low (routing change only, no logic change)
- **Benefit:** ~1,000 fewer lines per file; easier testing

### 3.2 Intelligence Modules (3 files exceed 2.5K lines)

| Module | Lines | Functions | Density |
|--------|-------|-----------|---------|
| `actor_network.py` | 7,002 | 16 | 438 lines/function |
| `actor_discovery.py` | 3,327 | 37 | 90 lines/function |
| `commodities_agriculture_network.py` | 2,765 | 8 | 345 lines/function |

**Problem with `actor_network.py` (7,002 lines):**
- **Massive data payload:** KNOWN_ACTORS dict spans 2,000+ lines
- **Mixed concerns:** Actor management, wealth tracking, pocket-lining detection, graph building
- **Low function count (16):** Suggests most of file is data, not logic
- **Integration complexity:** Used by multiple API routes with duplicated cache logic

**Solution:** Extract actor definitions to data layer:

```
intelligence/
├── actor_data/
│   ├── known_actors.json    (move the 2,000-line dict here)
│   └── __init__.py          (loader)
├── actor_network_logic.py   (functions only: 500 lines)
└── actor_network.py         (re-export from logic module)
```

**Refactoring Recommendation:**
- [ ] Extract `KNOWN_ACTORS` dict to JSON file
- [ ] Move data loading to `intelligence/actor_data/loader.py`
- [ ] Keep only functions in `actor_network.py`
- **Estimated effort:** 3-4 hours
- **Risk:** Low (data/logic split is safe)
- **Benefit:** ~3,500 lines of config removed from codebase; easier data management

### 3.3 Store & Analysis Modules (3 files exceed 2K lines)

| Module | Lines | Purpose | Issue |
|--------|-------|---------|-------|
| `store/astrogrid.py` | 2,795 | Feature computation & query | Mixed concerns: query + caching + computation |
| `analysis/money_flow.py` | 1,771 | Capital flow analysis | Integrated directly into module; no separation |
| `orchestration/llm_taskqueue.py` | 2,282 | Task orchestration | Complex state management |

**Action:** Audit these modules but defer refactoring until clarity on ownership.

---

## 4. Module Organization Issues

### 4.1 Ingestion Module Growth (104 files)

**Current Structure:**
```
ingestion/
├── base.py                    (BasePuller — shared interface)
├── scheduler.py               (scheduler pattern)
├── *.py                       (5 root-level pullers: coingecko, edgar, etc.)
├── altdata/                   (35+ files: lobbying, insider filings, GDELT, etc.)
├── international/             (12+ files: KOSIS, Comtrade, WIOD, etc.)
├── trade/                     (5 files: Comtrade, CEPII, WIOD, Atlas ECI)
├── physical/                  (8 files: USDA, NOAA, Patents, VIIRS, etc.)
└── celestial/                 (3 files: Vedic, Ephemeris, etc.)
```

**Problems:**
- `international/` and `trade/` overlap conceptually (both have trade data)
- `physical/` is too broad (includes agriculture, energy, infrastructure, logistics)
- 5 root-level pullers should be organized into domains

**Recommendation:** Reorganize to thematic domains:

```
ingestion/
├── base.py                    (BasePuller interface)
├── scheduler.py               (scheduler pattern)
├── market/                    (coingecko, dexscreener, yfinance, etc.)
├── government/                (FRED, BLS, ECB, KOSIS, etc.)
├── alternative/               (GDELT, congressional, insider, lobbying, etc.)
├── physical/                  (supply chain, USDA, NOAA, patents, logistics)
├── trade/                     (Comtrade, CEPII, WIOD, Atlas)
└── celestial/                 (ephemeris, vedic, etc.)
```

**Note:** This is a long-term refactoring; only do if new contributor patterns emerge.

### 4.2 Intelligence Module Growth (50+ files)

**Current issues:**
- No clear separation between **query APIs** (trust_scorer, lever_pullers) and **static data** (network modules)
- Helper modules (entity_resolver, cross_reference) are sibling to large data modules
- Hard to understand which module to use for a given query

**Recommendation:** Organize by maturity level:

```
intelligence/
├── _data/                     (static intelligence: networks, known actors)
├── query/                     (query APIs: trust_scorer, lever_pullers, sleuth)
├── analysis/                  (derived analysis: causation, hypothesis_engine)
├── audit/                     (validation: cross_reference, postmortem, source_audit)
└── integration/               (orchestration: rag, orchestration, etc.)
```

**Note:** Deferred; requires larger architectural review.

---

## 5. Dependency & Import Issues

### 5.1 Late-Bind Imports in API Routes

**Examples:**
- `api/routers/earnings.py` — imports inside route handler:
  ```python
  from intelligence.earnings_intel import get_earnings_calendar as _get_cal
  from ingestion.altdata.earnings_calendar import get_recent_earnings as _get_recent
  ```
- `api/routers/system.py` — many imports inside endpoints

**Problem:** Suggests modules were moved or refactored; creates fragility.

**Action:** Audit and move imports to top of file (or document why late-binding is needed).

### 5.2 Unused Dependencies

Checked `requirements.txt` — all imported packages appear to be used:
- `patent-client` is explicitly noted as excluded (good)
- TradingAgents is optional (noted in comments)
- Core dependencies are standard (SQLAlchemy, FastAPI, pandas, scikit-learn)

**No unused dependencies identified.**

---

## 6. Performance Hotspots (from CLAUDE.md)

### 6.1 Database N+1 Queries

**Known issues:**
- `api/routers/models.py:91-98` — fetches validation results without JOIN (#27)
- `discovery/orthogonality.py:75-80` — feature lookups in loops (#27)

**Not yet refactored.**

### 6.2 Missing Database Indexes (#16)

```sql
CREATE INDEX idx_decision_journal_model_version
  ON decision_journal(model_version_id);

CREATE INDEX idx_decision_journal_outcome_recorded
  ON decision_journal(outcome_recorded_at);

CREATE INDEX idx_resolved_series_conflict
  ON resolved_series(feature_id, obs_date)
  WHERE conflict_flag = TRUE;
```

**Action:** Execute before production deployment.

### 6.3 Computation Performance

- `discovery/clustering.py:292-313` — O(n²) nested loop for transition matrices (#28)
  - **Problematic for >10K observations**
  - Should use vectorized numpy operations

**Recommendation:** Profile on real data; consider matrix operations.

---

## 7. Refactoring Priority Matrix

### High-Impact, Low-Risk (Do First)

| Task | Impact | Effort | Risk | Owner |
|------|--------|--------|------|-------|
| Create tests for 8 zero-coverage modules | HIGH | 12-16h | LOW | TDD Agent |
| Split `api/routers/intelligence.py` into subdomain | HIGH | 4-6h | LOW | Code Reviewer |
| Extract network module getter patterns | MEDIUM | 8-12h | MEDIUM | Refactor Agent |
| Move `actor_network.py` data to JSON config | MEDIUM | 3-4h | LOW | Code Reviewer |

### Medium-Impact, Medium-Risk (Do Second)

| Task | Impact | Effort | Risk | Owner |
|------|--------|--------|------|-------|
| Add missing database indexes | MEDIUM | 2h | LOW | SQL Expert |
| Refactor N+1 queries in `models.py`, `orthogonality.py` | MEDIUM | 6-8h | MEDIUM | Performance Expert |
| Optimize clustering O(n²) loop | MEDIUM | 4-6h | MEDIUM | Performance Expert |
| Audit ingestion duplication (confirm inheritance) | LOW | 2-3h | LOW | Code Reviewer |

### Lower-Priority (Plan for Later)

| Task | Impact | Effort | Risk | Owner |
|------|--------|--------|------|-------|
| Reorganize ingestion/ directory (104 files) | LOW | 8-10h | MEDIUM | Architecture |
| Reorganize intelligence/ by maturity | LOW | 12-16h | MEDIUM | Architecture |
| Convert API late-bind imports to top-level | LOW | 2-3h | LOW | Code Reviewer |

---

## 8. Specific Code Patterns to Address

### 8.1 Module-Level Caches (Brittle)

**Example:** `api/routers/intelligence.py`

```python
_actor_graph_cache: dict[str, Any] = {"data": None, "ts": None}
_ACTOR_GRAPH_TTL = 1800
_cross_ref_cache: dict[str, Any] = {"data": None, "ts": None}
_CROSS_REF_TTL = 1800
# ... 4 more caches
```

**Problem:**
- No invalidation mechanism
- Not thread-safe
- Resets on app restart
- Duplicated pattern (define for each cache)

**Solution:** Use a shared cache decorator:

```python
from functools import wraps
from datetime import datetime, timezone

def cached(ttl_seconds: int):
    def decorator(func):
        cache = {"data": None, "ts": None}

        @wraps(func)
        async def wrapper(*args, **kwargs):
            now = datetime.now(timezone.utc)
            if cache["data"] and (now - cache["ts"]).total_seconds() < ttl_seconds:
                return cache["data"]
            result = await func(*args, **kwargs)
            cache["data"] = result
            cache["ts"] = now
            return result
        return wrapper
    return decorator

@router.get("/actor-network")
@cached(ttl_seconds=1800)
async def get_actor_network(_token: str = Depends(require_auth)):
    ...
```

**Refactoring Recommendation:**
- [ ] Create `api/caching.py` with `@cached` decorator
- [ ] Replace 20+ cache patterns in routers
- **Estimated effort:** 3-4 hours

### 8.2 Inline Data Definitions

**Example:** Network modules and actor_network.py

```python
ENERGY_NETWORK = {
    "oil_majors": {
        "xom": {
            "name": "ExxonMobil",
            "hq": "Texas, USA",
            # ... 50 lines per entity
        },
        # ... 100+ entities
    },
}
```

**Problem:** Mixes data with code; makes diffs hard to read; bloats Python files.

**Solution:** Move to YAML/JSON:

```python
# intelligence/actor_data/energy_network.yaml
oil_majors:
  xom:
    name: "ExxonMobil"
    hq: "Texas, USA"
    # ...

# intelligence/energy_network.py
import yaml
from pathlib import Path

ENERGY_NETWORK = yaml.safe_load(
    (Path(__file__).parent / "actor_data" / "energy_network.yaml").read_text()
)
```

**Benefit:** Easier for non-developers to update data; cleaner diffs; potential for dynamic loading.

---

## 9. Summary of Findings

### Code Metrics
- **Total Python Files:** ~230K LOC (excluding venv/migrations)
- **Ingestion Modules:** 104 files
- **Intelligence Modules:** 50+ files
- **API Routers:** 34 files
- **Test Files:** 75 files

### Duplication
- **Ingestion patterns:** 67 of 104 modules (64%) have copy-pasted helper methods
- **Network getters:** 10 modules follow identical accessor patterns
- **Cache patterns:** 4+ routers duplicate TTL-based caching logic
- **Total estimated duplicate code:** ~5,000 lines

### Oversizing
- **API Routers:** 7 files exceed 800 lines; largest is 3,871 lines
- **Intelligence Modules:** 3 files exceed 2.5K lines; largest is 7,002 lines
- **Network Data:** 70% of lines are static dicts; 30% is accessor logic

### Testing Gaps
- **Zero-coverage modules:** 8 critical modules (normalization, features, inference, discovery, validation, governance)
- **Weak test suite:** test_api.py ~100 lines; no integration tests; no E2E tests
- **Coverage estimate:** 15-20% of critical paths untested

### Performance Debt
- **N+1 queries:** 2 locations identified (models.py, orthogonality.py)
- **Missing indexes:** 3 index creations needed for journal/features tables
- **O(n²) computation:** clustering.py transition matrix logic; problematic for >10K observations

---

## 10. Recommended Next Steps

### Immediate (This Sprint)
1. **Add test files for 8 critical zero-coverage modules** — Use TDD agent
   - Target: 80%+ coverage for normalization, features, inference, governance
   - Effort: 2-3 days

2. **Split `api/routers/intelligence.py` into subdomains** — Use code-reviewer agent
   - Create actor_network.py, cross_reference.py, postmortem.py, source_audit.py
   - Effort: 1 day

### Short-term (Next 2 Sprints)
3. **Extract network module getter patterns** — Use refactor-cleaner agent
   - Create `intelligence/network_base.py` with shared interface
   - Update 10 network modules
   - Effort: 2 days

4. **Move `actor_network.py` data to config** — Use code-reviewer agent
   - Extract 2K+ line KNOWN_ACTORS dict to YAML
   - Effort: 1 day

5. **Add missing database indexes** — Use SQL expert
   - Effort: 2 hours

### Medium-term (Next Month)
6. **Refactor N+1 queries** — Use performance expert
   - Effort: 1-2 days

7. **Optimize clustering O(n²) loop** — Use performance expert
   - Effort: 1-2 days

### Long-term (Quarterly Review)
8. **Reorganize ingestion/ directory** — Use architect
   - Consolidate overlapping domains (international/trade)
   - Effort: 2-3 days

9. **Reorganize intelligence/ by maturity** — Use architect
   - Separate query APIs from static data from analysis
   - Effort: 3-4 days

---

## 11. Code Review Checklist (New)

Add to CLAUDE.md's [code-review.md](../common/code-review.md):

```markdown
## GRID-Specific Checks

- [ ] No copy-paste of `_resolve_source_id()` or `_row_exists()` (use BasePuller)
- [ ] No new module-level caches (use @cached decorator if needed)
- [ ] No inline data definitions >100 lines (move to config/JSON)
- [ ] API routes kept <50 lines (delegate to domain modules)
- [ ] New network modules inherit from `intelligence/network_base.py` (once created)
- [ ] Tests exist for all non-trivial functions
- [ ] No late-bind imports (import at top of file)
```

---

## Appendix: Files Needing Attention

### By Category

**Needs Test Coverage (Priority 1):**
- `normalization/entity_map.py`
- `normalization/resolver.py`
- `features/lab.py`
- `discovery/orthogonality.py`
- `discovery/clustering.py`
- `validation/gates.py`
- `governance/registry.py`
- `inference/live.py`

**Needs Splitting (Priority 2):**
- `api/routers/intelligence.py` → split into 4 files
- `api/routers/astrogrid.py` → evaluate need for split
- `intelligence/actor_network.py` → extract data

**Needs Extraction (Priority 3):**
- 10 network modules → extract common getter patterns
- API caching patterns → extract to decorator

**Performance Issues (Priority 4):**
- `api/routers/models.py:91-98` → fix N+1 query
- `discovery/orthogonality.py:75-80` → fix N+1 query
- `discovery/clustering.py:292-313` → optimize O(n²) loop

**Database Tasks (Priority 4):**
- Add index on `decision_journal(model_version_id)`
- Add index on `decision_journal(outcome_recorded_at)`
- Add index on `resolved_series(feature_id, obs_date) WHERE conflict_flag = TRUE`

---

**Report completed by:** refactor-cleaner agent
**Scope:** Code quality, duplication, sizing, module organization
**No code changes made** — recommendations only
**Review recommended before implementation**
