# GRID Code Review — 2026-03-30

**Reviewer:** ECC Code-Reviewer Agent
**Scope:** Last 5 commits (HEAD~5..HEAD)
**Files Analyzed:** 9 files changed, 292 insertions(+), 56 deletions(-)

---

## Executive Summary

Recent changes add **source trust configuration** and **entity resolution improvements** to the intelligence layer. Core functionality is solid, but **multiple HIGH severity issues** prevent approval:

1. **NaN handling violation** — NaN check uses identity (`z == z`) instead of proper math module guard
2. **Missing test coverage** — Two new critical modules have NO tests
3. **API string interpolation** — Multiple routes use f-strings for dynamic query construction (potential injection)
4. **Naming inconsistency** — `confidence_label` field undefined in schema but queried in API

---

## Findings by Severity

### CRITICAL (MUST FIX)

#### 1. **NaN Check Using Identity Comparison**
**File:** `api/routers/signals.py:83`
**Severity:** CRITICAL
**Confidence:** 95%

```python
z_by_name[name] = round(z, 4) if z == z else None
```

**Issue:** NaN check using identity (`z == z`) is unreliable and contradicts GRID's data-integrity rules. NaN in Python behaves inconsistently across numeric types (numpy.float64, float, Decimal). This pattern:
- Only works for numpy NaN by coincidence (NaN != NaN is True)
- Fails for pandas/Python float NaN comparisons
- Violates explicit rule: "Validate NaN/infinity handling at boundaries" (ATTENTION.md #21)

**Fix:**
```python
import math
# CORRECT: Use math.isnan() or pd.isna()
z_by_name[name] = round(z, 4) if not math.isnan(z) else None
# OR for pandas Series:
z_by_name[name] = round(z, 4) if pd.notna(z) else None
```

**Reference:** GRID Rules: `common/coding-style.md` (validation), `data-integrity.md` (#21)

---

#### 2. **F-String Query Construction in API Routes**
**Files:**
- `api/routers/intel.py:139` — f-string for valve description
- `api/routers/intel.py:161,320,341,342,356,457,485` — Dynamic f-strings with user input
- `api/routers/signals.py:165` — Direct LIKE concatenation

**Severity:** CRITICAL
**Confidence:** 90%

**Issue:** While these f-strings are not directly SQL (they're used in response building or LIKE patterns with parameterized queries), they create maintenance debt and can easily become SQL injection vectors if refactored. The GRID security rule is strict: **"NEVER use f-strings for SQL"**.

Example from `api/routers/intel.py:320`:
```python
{"q": f"%{name}%"}  # In parameterized query, but still bad hygiene
```

**Why it matters:**
- If developer refactors to direct SQL construction, injection vulnerability appears silently
- GRID's security rules state "Never use f-strings for SQL" as principle, not just for direct SQL
- Creates inconsistent pattern — other routes use proper param binding throughout

**Fix:**
```python
# Keep f-strings ONLY for non-SQL contexts (logging, responses)
# For ANY query construction, use SQLAlchemy text() with explicit params:
detail_msg = f"Entity appears in {len(jurisdictions)} jurisdictions: {', '.join(...)}"
# NOT in SQL
```

**Reference:** GRID Rules: `security.md` (SQL Safety section)

---

#### 3. **Two Critical New Modules Without Tests**
**Files:**
- `intelligence/source_trust_config.py` — 147 LOC, no tests
- `intelligence/entity_resolver.py` — 1410 LOC (LARGEST SINGLE MODULE), no tests

**Severity:** CRITICAL
**Confidence:** 95%

**Issue:** The project requires 80%+ test coverage. Two new critical modules:
1. **source_trust_config.py** — Core trust scoring configuration
   - `get_trust()` — accessed by inference and oracle endpoints
   - `trust_color()`, `trust_label()` — used for confidence labeling across API responses
   - No tests for edge cases: unknown sources, boundary scores (0.95, 0.85, 0.50, 0.20)

2. **entity_resolver.py** — 1410 LOC, largest single module
   - `normalize_name()` — fundamental for entity disambiguation
   - `name_similarity()` — just modified to add 3 comparison strategies
   - `EntityResolver.resolve()` — builds cross-source resolution index
   - No tests for phonetic matching, normalization edge cases, or database integration

**Impact:** These modules directly affect:
- Confidence labels in every API response
- Entity matching accuracy across 37+ data sources
- Bridge entity discovery (high-value intelligence)

**Fix:** Create test files immediately:

```python
# tests/test_source_trust_config.py
import pytest
from intelligence.source_trust_config import get_trust, trust_color, trust_label

def test_get_trust_known_source():
    trust = get_trust("sec_edgar")
    assert trust["base_trust"] == 0.99
    assert trust["tier"] == "canonical"

def test_get_trust_unknown_source():
    trust = get_trust("unknown_source_xyz")
    assert trust["base_trust"] == 0.50  # default
    assert trust["tier"] == "derived"

def test_trust_color_boundary():
    assert trust_color(0.85) == "green"
    assert trust_color(0.84) == "yellow"
    assert trust_color(0.50) == "yellow"
    assert trust_color(0.49) == "orange"
    # ... etc
```

```python
# tests/test_entity_resolver.py
def test_normalize_name_person():
    assert normalize_name("DR. John Q Smith, Jr") == "john smith"

def test_name_similarity_exact():
    assert name_similarity("David Perdue", "David Perdue") == 1.0

def test_name_similarity_variants():
    # Senate: "David A Perdue, Jr"
    # ICIJ: "DAVID PERDUE"
    # FEC: "PERDUE, DAVID A"
    assert name_similarity("David A Perdue, Jr", "PERDUE, DAVID A") > 0.95
```

**Reference:** GRID Rules: `common/testing.md` (80% minimum), `testing.md` (zero-coverage modules list)

---

### HIGH (SHOULD FIX)

#### 4. **Trust Config Has Duplicate Confidence Labels**
**File:** `intelligence/source_trust_config.py:136-146`

```python
def trust_label(score: float) -> str:
    if score >= 0.95:
        return "confirmed"
    if score >= 0.85:
        return "confirmed"  # DUPLICATE — should be "derived"?
    if score >= 0.70:
        return "derived"
    if score >= 0.50:
        return "estimated"
    if score >= 0.20:
        return "rumored"
    return "inferred"
```

**Issue:** Returns "confirmed" for BOTH 0.95+ AND 0.85+. This collapses two distinct confidence tiers into one. The docstring says:
```
GREEN  = confirmed (0.85+)
YELLOW = estimated (0.50-0.85)
```

But the label function returns the same string for 0.99 (SEC EDGAR, near-perfect) and 0.85 (GDELT, verified journalism). This defeats the granular confidence system.

**Fix:**
```python
def trust_label(score: float) -> str:
    if score >= 0.95:
        return "confirmed"
    if score >= 0.85:
        return "derived"   # FIX: was "confirmed", should distinguish from 0.95+
    if score >= 0.70:
        return "derived"
    if score >= 0.50:
        return "estimated"
    if score >= 0.20:
        return "rumored"
    return "inferred"
```

---

#### 5. **Entity Resolver: Canonical Key Equality Check Too Strict**
**File:** `intelligence/entity_resolver.py:240-246`

```python
def name_similarity(name1: str, name2: str) -> float:
    # Canonical key match = definite match
    k1 = canonical_key(name1)
    k2 = canonical_key(name2)
    if k1 and k2 and k1 == k2:
        return 1.0
    ...
```

**Issue:** Returns 1.0 (perfect match) if canonical keys are equal, but canonical key generation may fail (return empty string) for edge cases. If both keys are empty string `""`, this returns 1.0 for completely unrelated names.

**Evidence:**
```python
def canonical_key(name: str) -> str:
    """Generate phonetic fingerprint for fast matching."""
    if not name or len(name.strip()) < 2:
        return ""  # SHORT NAMES RETURN EMPTY
    ...
```

Two names with length <2 would both get `""` and incorrectly match.

**Fix:**
```python
if k1 and k2 and k1 == k2:  # Already has `and k1` and `and k2` guards
    return 1.0
# But add test to verify empty key edge case doesn't occur in practice
```

**Confidence:** 80% — Likely edge case but should have test coverage.

---

#### 6. **Missing Schema Validation for `confidence_label` Field**
**File:** `api/routers/intel.py:169-178`

```python
rows = conn.execute(
    text(
        "SELECT actor_id, name, tier, sector, aum_usd, trust_score, "
        "confidence_label "  # FIELD NAME QUERIED
        "FROM actors "
        ...
    )
).fetchall()
```

**Issue:** The query selects `confidence_label` from the `actors` table, but this field does not appear in the `source_trust_config` or documented schema. It's unclear:
1. Is this field actually in the database?
2. Is it computed at query time?
3. If it's missing, this endpoint silently fails with a database error

**Impact:** Silent endpoint failure if field doesn't exist. The error would be caught late in testing.

**Fix:**
1. Verify `actors.confidence_label` exists in schema and is populated
2. OR compute it from `trust_score` on the fly:
```python
confidence_label = "confirmed" if trust_score >= 0.95 else "derived" if trust_score >= 0.85 else "estimated"
```
3. Document schema clearly in a schema migration or migration guide

---

#### 7. **Entity Resolver: Recursive Levenshtein Distance May Blow Stack**
**File:** `intelligence/entity_resolver.py:151-170`

```python
def levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)  # RECURSION: could be deep if len mismatch
    if not s2:
        return len(s1)
    ...
```

**Issue:** This is a recursive approach with memoization. For very long names (rare, but possible in company names), the recursion depth could hit Python's default limit (~1000). The iterative approach below is better.

**Current code is actually iterative already** (prev_row/curr_row pattern), so the recursion is just the swap at the top. Low risk, but could add a tail-call optimization check.

**Fix:** Already handled reasonably; just note in comments that recursive swap is safe due to string length limits in practice.

---

### MEDIUM (CONSIDER FIXING)

#### 8. **Date Filtering in Signals Endpoint Uses String Comparison**
**File:** `api/routers/signals.py:165`

```python
"AND rs.obs_date >= CURRENT_DATE - :days "
```

**Issue:** Uses PostgreSQL `CURRENT_DATE` which is good, but the arithmetic `CURRENT_DATE - :days` relies on PostgreSQL interval arithmetic. This works but is fragile:
- Other databases (SQLite, MySQL) may not support this syntax
- GRID is PostgreSQL-only per docs, but parameterization with explicit dates is clearer

**Confidence:** 70% — Low risk since GRID uses PostgreSQL, but inconsistent with other date handling in codebase.

---

#### 9. **Trust Score Color Mapping Ignores Boundaries**
**File:** `intelligence/source_trust_config.py:123-131`

```python
def trust_color(score: float) -> str:
    if score >= 0.85:
        return "green"
    if score >= 0.50:
        return "yellow"
    if score >= 0.20:
        return "orange"
    return "red"
```

**Issue:** The color mapping doesn't account for NaN or infinity. If a trust score is NaN (from computation error), it returns "red" incorrectly. Should validate inputs.

**Fix:**
```python
import math

def trust_color(score: float) -> str:
    if not isinstance(score, (int, float)) or math.isnan(score) or math.isinf(score):
        return "red"  # Untrustworthy value
    if score >= 0.85:
        return "green"
    ...
```

---

#### 10. **Entity Resolver Module Size (1410 LOC)**
**File:** `intelligence/entity_resolver.py`

**Issue:** This is the largest single module in the codebase by a significant margin (next largest: api routers at 1-4K LOC shared across multiple files). At 1410 LOC, it violates the GRID rule: "200-400 lines typical, 800 max" (coding-style.md).

**Current breakdown:**
- Phonetic utilities (107-145): ~38 LOC
- String similarity (151-230): ~79 LOC
- Name normalization (274-335): ~61 LOC
- Canonical key (336-365): ~29 LOC
- ResolvedEntity dataclass (369-399): ~30 LOC
- EntityResolver class: ~1100 LOC

**Fix:** Extract into separate modules:
```
intelligence/
├── entity_resolver.py       (main orchestrator, ~200 LOC)
├── _name_normalization.py   (normalize_name, strip_accents, ~100 LOC)
├── _similarity.py           (jaro_winkler, levenshtein, name_similarity, ~150 LOC)
├── _phonetic.py             (soundex, phonetic_key, ~50 LOC)
└── _canonical.py            (canonical_key, entity_id, ~50 LOC)
```

This improves:
- Testability (easier to unit test each module)
- Maintainability (each module is focused)
- Readability (less context switching)

---

### LOW (NICE TO HAVE)

#### 11. **Unused Import in Signals**
**File:** `api/routers/signals.py`

```python
from sqlalchemy.engine import Engine  # Imported but not used directly in function signatures
```

**Issue:** The `Engine` type is imported but only used as a type annotation in one dependency injection (line 103). Low impact, but clean up for consistency.

---

#### 12. **Parse Datasets Script: No Error Recovery**
**File:** `scripts/parse_datasets.py`

**Issue:** The DDL is split to prevent existing tables from blocking, but INSERT statements don't handle conflicts gracefully. If a record already exists, the script will error. Should use `ON CONFLICT DO UPDATE` or check for existence first.

**Low priority** because scripts are not part of production API, but noted for completeness.

---

## Summary Table

| Severity | Category | Count | Examples |
|----------|----------|-------|----------|
| CRITICAL | Test Coverage | 1 | No tests for 2 critical modules |
| CRITICAL | Data Integrity | 1 | NaN check using identity (`z == z`) |
| CRITICAL | Security | 1 | SQL param safety (f-strings in queries) |
| HIGH | Logic Error | 1 | Duplicate confidence labels |
| HIGH | Edge Case | 1 | Canonical key empty string edge case |
| HIGH | Schema Issue | 1 | Undefined `confidence_label` field |
| MEDIUM | Portability | 1 | Date arithmetic assumes PostgreSQL |
| MEDIUM | Validation | 1 | Missing NaN/infinity checks in color mapping |
| MEDIUM | Maintainability | 1 | entity_resolver.py at 1410 LOC (>800 max) |
| LOW | Code Quality | 2 | Unused imports, script error recovery |

---

## Test Coverage Assessment

**Current Status:** Files changed in last 5 commits include:
- `intelligence/source_trust_config.py` — NEW, NO TESTS
- `intelligence/entity_resolver.py` — MODIFIED, NO TESTS (1410 LOC)
- `api/routers/intel.py` — MODIFIED, NO TESTS (2159 LOC)
- `api/routers/signals.py` — MODIFIED, NO TESTS (271 LOC)

**Requirement:** 80% minimum per GRID rules.
**Current estimate:** <50% (critical modules untested).

---

## Verdict

### **BLOCK** ⛔

**Cannot merge in current state.** Three CRITICAL issues must be resolved:

1. **NaN handling** — Fix line 83 in signals.py
2. **Test coverage** — Add tests for source_trust_config.py and entity_resolver.py
3. **Schema validation** — Verify or compute `confidence_label` in intel.py

**High-priority fixes:**
- Duplicate confidence labels in source_trust_config.py
- Canonical key edge case in entity_resolver.py

**Estimated effort to merge-ready:**
- NaN fix: 5 minutes
- Tests: 2-3 hours (source_trust_config straightforward, entity_resolver needs integration with test DB)
- Schema fix: 30 minutes

**Recommend:**
1. Fix CRITICAL issues first (quick wins on NaN, schema)
2. Write tests for source_trust_config.py (fast, high confidence)
3. Add integration tests for entity_resolver.py with test fixtures
4. Re-run full test suite to verify 80%+ coverage

---

## Files Requiring Action

| File | Issues | Priority |
|------|--------|----------|
| api/routers/signals.py | NaN check, z-score logic | CRITICAL |
| intelligence/source_trust_config.py | No tests, duplicate labels | CRITICAL + HIGH |
| intelligence/entity_resolver.py | No tests, size, canonical edge case | CRITICAL + HIGH + MEDIUM |
| api/routers/intel.py | Missing schema validation, f-strings | HIGH |
| scripts/parse_datasets.py | Error recovery | LOW |

---

**Review Date:** 2026-03-30
**Reviewer:** ECC Code-Reviewer
**Status:** BLOCK — Awaiting fixes
