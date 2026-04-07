# Reference Hallucination Guard — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-[[development]] (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a post-generation URL/reference verification system that catches hallucinated URLs before they reach users or storage, reducing non-resolving citations to <1%.

**[[architecture|Architecture]]:** Extract URLs from LLM output → async HTTP HEAD + Wayback Machine classification → confidence adjustment via GuardCheck pattern → clean/annotate output. Plugs into existing `hallucination_guard.py` and `llm_logger.py` flows.

**Tech Stack:** Python asyncio, aiohttp, dataclasses (frozen), existing GuardCheck pattern

**Spec:** `docs/superpowers/specs/2026-04-06-reference-hallucination-guard-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `verification/__init__.py` | Create | Package marker |
| `verification/ref_extractor.py` | Create | URL/DOI extraction from LLM text |
| `verification/url_health.py` | Create | Async URL liveness + Wayback classification |
| `verification/ref_guard.py` | Create | Reference confidence guard (extends GuardCheck) |
| `verification/annotator.py` | Create | Clean/replace bad refs in output text |
| `oracle/hallucination_guard.py` | Modify | Add ref_hallucinated as 9th check |
| `oracle/report.py` | Modify | Wire ref verification before email send |
| `outputs/llm_logger.py` | Modify | Wire ref verification before insight persist |
| `config.py` | Modify | Add REF_CHECK_* settings |
| `schema.sql` | Modify | Add ref_verification_log table |
| `tests/test_ref_extractor.py` | Create | URL extraction tests |
| `tests/test_url_health.py` | Create | URL health classification tests (mocked) |
| `tests/test_ref_guard.py` | Create | Guard logic + confidence adjustment tests |
| `tests/test_annotator.py` | Create | Output cleaning tests |

---

## Task 1: Database Schema — `ref_verification_log` Table

**Files:**
- Modify: `schema.sql` (append after last table)

- [ ] **Step 1: Add ref_verification_log table to schema.sql**

```sql
CREATE TABLE IF NOT EXISTS ref_verification_log (
    id              SERIAL PRIMARY KEY,
    source_module   TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    url             TEXT NOT NULL,
    classification  TEXT NOT NULL,
    http_status     INTEGER,
    wayback_url     TEXT,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    latency_ms      INTEGER
);

CREATE INDEX idx_ref_log_url ON ref_verification_log(url);
CREATE INDEX idx_ref_log_classification ON ref_verification_log(classification);
CREATE INDEX idx_ref_log_checked_at ON ref_verification_log(checked_at);
```

**Verify:** `psql -c "\d ref_verification_log"` shows correct columns

---

## Task 2: Configuration — REF_CHECK Settings

**Files:**
- Modify: `config.py` (add to Settings class)

- [ ] **Step 1: Add ref check settings to config.py**

Add to the pydantic-settings `Settings` class:

```python
# Reference hallucination guard
ref_check_enabled: bool = True
ref_check_timeout_s: float = 5.0
ref_check_max_concurrent: int = 10
ref_check_rate_limit: float = 10.0
ref_check_cache_ttl_s: int = 3600
ref_check_wayback_enabled: bool = True
ref_check_reject_threshold: float = 0.4
```

**Verify:** `from config import settings; print(settings.ref_check_enabled)` returns `True`

---

## Task 3: Reference Extractor

**Files:**
- Create: `verification/__init__.py`
- Create: `verification/ref_extractor.py`

- [ ] **Step 1: Create verification package**

`__init__.py` — empty package marker.

- [ ] **Step 2: Implement ref_extractor.py**

```python
@dataclass(frozen=True)
class ExtractedRef:
    url: str
    anchor_text: str | None
    position: int          # char offset in original text
    ref_type: str          # "markdown_link", "raw_url", "doi"

def extract_refs(text: str) -> list[ExtractedRef]:
    """Extract all URLs, markdown links, and DOIs from LLM-generated text."""
```

Patterns to match:
1. Markdown links: `\[([^\]]+)\]\((https?://[^\)]+)\)`
2. Raw URLs: `https?://[^\s\)\]>\"]+`
3. DOIs: `doi:10\.\d{4,}/[^\s]+` → convert to `https://doi.org/...`

Deduplicate by URL. Return sorted by position.

- [ ] **Step 3: Write tests for ref_extractor**

Create `tests/test_ref_extractor.py`:
- Test markdown link extraction
- Test raw URL extraction
- Test DOI extraction and conversion
- Test deduplication
- Test empty text / no URLs
- Test URLs with special characters (parentheses, query params, fragments)

**Verify:** `python -m pytest tests/test_ref_extractor.py -v` — all pass

---

## Task 4: URL Health Checker

**Files:**
- Create: `verification/url_health.py`

- [ ] **Step 1: Implement url_health.py**

```python
class URLClassification(str, Enum):
    LIVE = "LIVE"
    DEAD = "DEAD"
    LIKELY_HALLUCINATED = "LIKELY_HALLUCINATED"
    UNKNOWN = "UNKNOWN"

@dataclass(frozen=True)
class URLCheckResult:
    url: str
    classification: URLClassification
    http_status: int | None
    wayback_url: str | None
    latency_ms: int
    error: str | None

async def check_url(session: aiohttp.ClientSession, url: str,
                    timeout_s: float = 5.0) -> URLCheckResult:
    """Check single URL: HEAD request → classify → Wayback lookup if 404."""

async def check_urls(urls: list[str], *,
                     max_concurrent: int = 10,
                     rate_limit: float = 10.0,
                     timeout_s: float = 5.0,
                     wayback_enabled: bool = True) -> list[URLCheckResult]:
    """Batch check URLs with concurrency + rate limiting."""
```

Logic:
1. HTTP HEAD with `timeout_s` — follow redirects up to 3 hops
2. If 200-299: LIVE
3. If 404/410: query Wayback CDX API `http://web.archive.org/cdx/search/cdx?url={url}&output=json&limit=1`
   - If snapshot found: DEAD (+ set `wayback_url`)
   - If no snapshot: LIKELY_HALLUCINATED
4. All other: UNKNOWN

Rate limiting via `asyncio.Semaphore` + per-domain token bucket.
In-memory LRU cache (keyed by URL, TTL from config).

- [ ] **Step 2: Write tests for url_health (mocked HTTP)**

Create `tests/test_url_health.py`:
- Mock aiohttp responses for each classification
- Test Wayback CDX integration (mocked)
- Test timeout handling
- Test rate limiting doesn't exceed limits
- Test cache returns same result without re-requesting
- Test graceful degradation when Wayback is down

**Verify:** `python -m pytest tests/test_url_health.py -v` — all pass

---

## Task 5: Reference Guard

**Files:**
- Create: `verification/ref_guard.py`

- [ ] **Step 1: Implement ref_guard.py**

```python
@dataclass(frozen=True)
class ReferenceVerdict:
    original_confidence: float
    adjusted_confidence: float
    checks: tuple[GuardCheck, ...]
    action: str  # "pass", "clean", "flag", "reject"
    url_results: tuple[URLCheckResult, ...]
    reasons: tuple[str, ...]

def verify_references(text: str, original_confidence: float,
                      url_results: list[URLCheckResult]) -> ReferenceVerdict:
    """Run reference verification checks and produce confidence-adjusted verdict."""
```

Checks (mirrors `hallucination_guard.py` pattern):
1. `ref_hallucinated` — any LIKELY_HALLUCINATED → 0.5x per instance (compound)
2. `ref_dead` — >50% DEAD → 0.8x
3. `ref_unreachable` — >30% UNKNOWN → 0.9x
4. `ref_density` — >5 refs in <500 words → 0.9x (over-citation = compensation)

Action thresholds:
- `pass`: adjusted >= original
- `clean`: DEAD refs exist but confidence OK (replace with Wayback URLs)
- `flag`: any LIKELY_HALLUCINATED ref
- `reject`: adjusted < original * reject_threshold (default 0.4)

- [ ] **Step 2: Write tests for ref_guard**

Create `tests/test_ref_guard.py`:
- Test all-LIVE refs → pass, no adjustment
- Test single HALLUCINATED → 0.5x + flag
- Test multiple HALLUCINATED → compounds (0.5 * 0.5 = 0.25x) → reject
- Test majority DEAD → 0.8x + clean
- Test over-citation density penalty
- Test empty URL list → pass (no refs to check)

**Verify:** `python -m pytest tests/test_ref_guard.py -v` — all pass

---

## Task 6: Output Annotator

**Files:**
- Create: `verification/annotator.py`

- [ ] **Step 1: Implement annotator.py**

```python
@dataclass(frozen=True)
class AnnotatedOutput:
    original_text: str
    cleaned_text: str
    replacements: tuple[Replacement, ...]
    removed_count: int
    replaced_count: int

def annotate_output(text: str, url_results: list[URLCheckResult]) -> AnnotatedOutput:
    """Replace dead refs with Wayback URLs, remove hallucinated refs."""
```

Rules:
- LIVE: keep as-is
- DEAD with wayback_url: replace URL with wayback_url
- LIKELY_HALLUCINATED: remove the markdown link, keep anchor text with `[source not verified]`
- UNKNOWN: append `[unverified]` after the link

- [ ] **Step 2: Write tests for annotator**

Create `tests/test_annotator.py`:
- Test LIVE URLs unchanged
- Test DEAD URLs replaced with Wayback links
- Test HALLUCINATED markdown links → anchor text + tag
- Test UNKNOWN URLs → appended tag
- Test mixed results in same text
- Test text with no URLs → unchanged

**Verify:** `python -m pytest tests/test_annotator.py -v` — all pass

---

## Task 7: Wire into Oracle Report Pipeline

**Files:**
- Modify: `oracle/report.py`

- [ ] **Step 1: Add ref verification before email send**

In `report.py`, before the email is constructed/sent, add:

```python
from verification.ref_extractor import extract_refs
from verification.url_health import check_urls
from verification.ref_guard import verify_references
from verification.annotator import annotate_output

# After report text is generated, before sending:
if settings.ref_check_enabled:
    refs = extract_refs(report_text)
    if refs:
        url_results = await check_urls([r.url for r in refs])
        verdict = verify_references(report_text, confidence, url_results)
        annotated = annotate_output(report_text, url_results)
        report_text = annotated.cleaned_text
        log.info(f"Ref guard: {verdict.action}, {annotated.removed_count} removed, "
                 f"{annotated.replaced_count} replaced")
```

**Verify:** Generate a test report with known bad URLs → confirm they're cleaned

---

## Task 8: Wire into LLM Logger

**Files:**
- Modify: `outputs/llm_logger.py`

- [ ] **Step 1: Add ref verification before insight persistence**

Same pattern as Task 7, but applied before insights are written to disk/DB.

```python
# Before persisting insight:
if settings.ref_check_enabled:
    refs = extract_refs(insight_text)
    if refs:
        url_results = await check_urls([r.url for r in refs])
        verdict = verify_references(insight_text, 1.0, url_results)
        if verdict.action == "reject":
            log.warning(f"Insight rejected: {len(refs)} refs, {verdict.reasons}")
            return  # Don't persist
        annotated = annotate_output(insight_text, url_results)
        insight_text = annotated.cleaned_text
```

**Verify:** Log an insight with a hallucinated URL → confirm it's caught

---

## Task 9: Add as 9th Check in Hallucination Guard

**Files:**
- Modify: `oracle/hallucination_guard.py`

- [ ] **Step 1: Add ref_hallucinated check to verify_predictions()**

Add a 9th check that examines the prediction's `rationale` or `narrative` field for URLs:

```python
def _check_reference_validity(prediction: dict) -> GuardCheck:
    """Check if any cited URLs in prediction rationale are hallucinated."""
    # Synchronous wrapper — uses cached results if available
    # Returns GuardCheck with adjustment based on hallucinated ref count
```

This check is optional (skipped if `ref_check_enabled=False`) and uses cached URL results to avoid blocking the synchronous guard pipeline.

**Verify:** `python -m pytest tests/test_hallucination_guard.py -v` — existing + new tests pass

---

## Task 10: Audit Log Persistence

**Files:**
- Modify: `verification/url_health.py` (add DB logging)

- [ ] **Step 1: Add audit logging to check_urls()**

After each URL check, persist to `ref_verification_log`:

```python
async def _log_check(result: URLCheckResult, source_module: str,
                     content_hash: str, session) -> None:
    """Write URL check result to audit log."""
```

Use batch INSERT for efficiency. Include `source_module` to track which GRID component generated the content.

**Verify:** After running a report, `SELECT count(*) FROM ref_verification_log` shows entries

---

## Task 11: Integration Test

**Files:**
- Create: `tests/test_ref_integration.py`

- [ ] **Step 1: End-to-end test with mixed URLs**

```python
def test_full_pipeline_mixed_urls():
    """Text with LIVE, DEAD, HALLUCINATED, UNKNOWN URLs → cleaned output."""
    text = """
    According to [Reuters](https://reuters.com/real-article) and
    [this study](https://example.com/hallucinated-study-12345) ...
    """
    # Mock HTTP responses, run full pipeline, verify:
    # - LIVE refs kept
    # - DEAD refs replaced with Wayback URLs
    # - HALLUCINATED refs removed with [source not verified] tag
    # - Confidence adjusted correctly
    # - Audit log entries created
```

**Verify:** `python -m pytest tests/test_ref_integration.py -v` — all pass

---

## Dependencies

Add to `requirements.txt`:
```
aiohttp>=3.9
```

(aiohttp may already be present from realtime listener work — check before adding)

---

## Rollout Strategy

1. **Phase 1 (this plan):** Core verification pipeline + Oracle report + LLM logger integration
2. **Phase 2 (future):** Content verification — does the URL's content actually support the claim?
3. **Phase 3 (future):** Agent tool — give urlhealth to [[TradingAgents]] as a callable tool for self-correction
4. **Phase 4 (future):** [[Trust Scorer|Trust scorer]] integration — track ref accuracy per LLM model/source over time
