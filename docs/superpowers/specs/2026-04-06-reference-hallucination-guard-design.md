# Reference Hallucination Guard — Design Spec

**Date:** 2026-04-06
**Status:** Draft
**Source:** [arxiv 2604.03173 — Detecting and Correcting Reference Hallucinations](https://arxiv.org/abs/2604.03173) (Rao, Wong, Callison-Burch, UPenn)
**Scope:** Post-generation URL/reference verification for all LLM outputs in GRID

---

## Overview

LLMs hallucinate 3–13% of citation URLs (they never existed). GRID uses LLM-generated content in Oracle reports, market briefings, insight scanning, and agent workflows — all of which can cite external sources. This module adds a **post-generation reference verification gate** that catches hallucinated URLs before they reach users or storage.

Modeled after the existing `oracle/hallucination_guard.py` (deterministic confidence adjustment) and `store/pit.py` (`assert_no_lookahead()` safety net pattern).

## Architecture

```
LLM Output (any module)
    │
    ▼
┌──────────────────────────┐
│  Reference Extractor     │ ← regex + markdown link parser
│  Pulls all URLs from text│
└──────────┬───────────────┘
           │ list[URL]
           ▼
┌──────────────────────────┐
│  URL Health Checker      │ ← async HTTP HEAD + Wayback Machine
│  LIVE / DEAD / HALLUC /  │
│  UNKNOWN classification  │
└──────────┬───────────────┘
           │ list[URLCheckResult]
           ▼
┌──────────────────────────┐
│  Reference Guard         │ ← extends GuardCheck pattern
│  Confidence adjustment   │
│  + audit trail           │
└──────────┬───────────────┘
           │ ReferenceVerdict
           ▼
┌──────────────────────────┐
│  Output Annotator        │ ← marks/removes bad refs
│  Returns cleaned text    │
└──────────────────────────┘
```

## URL Classification (from paper)

| Status | HTTP Result | Wayback Machine | Meaning |
|--------|-------------|-----------------|---------|
| LIVE | 200 | — | URL resolves, content accessible |
| DEAD | 404/410 | Snapshot exists | Real page, moved or deleted — recoverable |
| LIKELY_HALLUCINATED | 404/410 | No snapshot | Never existed — fabricated by LLM |
| UNKNOWN | Other/timeout | — | Needs manual inspection |

## Components

### 1. Reference Extractor (`verification/ref_extractor.py`)

Extracts URLs from LLM-generated text:
- Markdown links: `[text](url)`
- Raw URLs: `https://...`
- Academic citations with DOIs: `doi:10.xxxx/...`
- Returns `list[ExtractedRef]` with URL, anchor text, position in text

### 2. URL Health Checker (`verification/url_health.py`)

Async URL verification:
- HTTP HEAD request (5s timeout, fallback to GET)
- Rate limiting: max 10 requests/second, 2 concurrent per domain
- Wayback Machine CDX API lookup for 404s: `http://web.archive.org/cdx/search/cdx?url={url}&output=json&limit=1`
- Returns `URLCheckResult(url, status, classification, wayback_url, latency_ms)`
- Cache results for 1 hour (same URL checked once per session)

### 3. Reference Guard (`verification/ref_guard.py`)

Extends the `GuardCheck` pattern from `oracle/hallucination_guard.py`:

| Check | Trigger | Adjustment |
|-------|---------|------------|
| `ref_hallucinated` | Any LIKELY_HALLUCINATED URL | 0.5x per hallucinated ref |
| `ref_dead` | >50% of refs are DEAD | 0.8x |
| `ref_unreachable` | >30% UNKNOWN (network issues) | 0.9x |
| `ref_domain_age` | Domain registered <30 days | 0.7x |
| `ref_density` | >5 refs in <500 words (over-citation) | 0.9x |

Produces `ReferenceVerdict` (mirrors `GuardVerdict`):
- `original_confidence`, `adjusted_confidence`
- `checks: tuple[GuardCheck, ...]`
- `action: "pass" | "clean" | "flag" | "reject"`
- `cleaned_refs: list[CleanedRef]` — DEAD URLs replaced with Wayback links

### 4. Output Annotator (`verification/annotator.py`)

Post-processing of LLM text:
- Replace DEAD URLs with Wayback Machine archived versions
- Remove or strike-through LIKELY_HALLUCINATED URLs
- Append `[unverified]` tag to UNKNOWN URLs
- Return cleaned text + annotation log

## Integration Points

| Module | Integration | Priority |
|--------|-------------|----------|
| `oracle/report.py` | Verify all URLs before email send | P0 |
| `outputs/llm_logger.py` | Verify before persisting insights | P0 |
| `outputs/insight_scanner.py` | Add ref check as scan criterion | P1 |
| `ollama/` market briefings | Verify briefing citations | P1 |
| `agents/` TradingAgents | Add urlhealth as agent tool | P2 |
| `oracle/hallucination_guard.py` | Add `ref_hallucinated` as 9th check | P2 |
| `intelligence/trust_scorer.py` | Track ref accuracy per source | P3 |

## Database Schema

```sql
-- Reference verification audit log
CREATE TABLE IF NOT EXISTS ref_verification_log (
    id              SERIAL PRIMARY KEY,
    source_module   TEXT NOT NULL,        -- 'oracle/report', 'outputs/llm_logger', etc.
    content_hash    TEXT NOT NULL,        -- SHA256 of original text
    url             TEXT NOT NULL,
    classification  TEXT NOT NULL,        -- 'LIVE', 'DEAD', 'LIKELY_HALLUCINATED', 'UNKNOWN'
    http_status     INTEGER,
    wayback_url     TEXT,                 -- archived version if available
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    latency_ms      INTEGER
);

CREATE INDEX idx_ref_log_url ON ref_verification_log(url);
CREATE INDEX idx_ref_log_classification ON ref_verification_log(classification);
CREATE INDEX idx_ref_log_checked_at ON ref_verification_log(checked_at);
```

## Configuration

```python
# config.py additions
REF_CHECK_ENABLED: bool = True           # Kill switch
REF_CHECK_TIMEOUT_S: float = 5.0         # Per-URL timeout
REF_CHECK_MAX_CONCURRENT: int = 10       # Max parallel checks
REF_CHECK_RATE_LIMIT: float = 10.0       # Requests per second
REF_CHECK_CACHE_TTL_S: int = 3600        # 1 hour cache
REF_CHECK_WAYBACK_ENABLED: bool = True   # Enable Wayback lookup
REF_CHECK_REJECT_THRESHOLD: float = 0.4  # Reject if adjusted < 40% original
```

## Graceful Degradation

- If Wayback Machine API is down: classify 404s as UNKNOWN (not HALLUCINATED)
- If all URL checks timeout: skip verification, log warning, pass through unchanged
- If `REF_CHECK_ENABLED=False`: bypass entirely (for [[development]]/testing)
- No external API keys required (HTTP HEAD + Wayback CDX are free)

## Non-Goals

- Content verification (checking if the URL's content actually supports the claim) — future phase
- Real-time URL monitoring (continuous re-checking of previously verified URLs)
- Blocking LLM generation — only post-generation verification
