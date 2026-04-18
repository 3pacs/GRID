---
source: /Users/anikdang/grid_obsidian/Architecture/Security.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
---
tags: [architecture, security]
---
# Security Audit & Fixes

## Overview
Full security audit conducted across Sessions [[Sessions/S19-2026-04-04|S19]] and [[Sessions/S20-2026-04-04|S20]]. Started with 5 CRITICAL + 21 HIGH vulnerabilities. All CRITICALs and 13 HIGHs now resolved.

## CRITICAL (Phase 1) -- All Fixed in S17/S19

| ID | Issue | File | Status |
|----|-------|------|--------|
| C1 | Unauthenticated SSRF | `api/routers/notifications.py` | **FIXED** |
| C2 | No auth on notifications | `api/routers/notifications.py` | **FIXED** |
| C3 | CORS wildcard default | `config.py` | **FIXED** |
| C4 | Relative path subprocess | `api/routers/system.py` | **FIXED** |
| C5 | f-string DDL injection | `oracle/model_factory.py` | **FIXED** |

## HIGH (Phase 2) -- 13/21 Fixed

| ID | Issue | Status | Session |
|----|-------|--------|---------|
| H1 | No input validation ChatAskRequest | **FIXED** | S20 |
| H2 | Prompt injection via history | **FIXED** | S20 |
| H3 | Sleuth lead ID collision | **FIXED** | S20 |
| H4 | Race condition _timesfm_last_run | **FIXED** | S19 |
| H5 | f-string SQL (3 locations) | **FIXED** | S20 |
| H6 | XSS dangerouslySetInnerHTML | **FIXED** | S20 |
| H7 | Payment middleware bypass | **FIXED** | S20 |
| H8 | Path traversal AstroGrid | **FIXED** | S20 |
| H9 | 281 swallowed exceptions | **86/281 FIXED** | S20 |
| H10 | 1,217 print() statements | Open | -- |
| H12 | 9 unprotected global caches | **FIXED** | S20 |
| H13 | N+1 actor enrichment | **FIXED** | S20 |
| H14 | N+1 watchlist gatherer | **FIXED** | S20 |

## Remaining HIGH (8 open)

| ID | Issue | File |
|----|-------|------|
| H10 | 1,217 print() → logging | 129 files |
| H11 | 16 files over 800 lines | flow_thesis.py, causation.py, etc. |
| H15 | FLOW_KNOWLEDGE mutation | intelligence/ |
| H17 | Smart scheduler thread leaks | ingestion/smart_scheduler.py |
| H21 | _intelligence_loop nested in lifespan | api/main.py |
| H9 | ~195 remaining swallowed exceptions | Various |

## Key Security Patterns Added

### DOMPurify (XSS Prevention)
```javascript
import DOMPurify from 'dompurify'
const clean = DOMPurify.sanitize(html, { FORBID_TAGS: ['script', 'iframe'] })
```
Used in: `Briefings.jsx`, `MarketDiary.jsx`

### Prompt Injection Sanitizer
5 regex patterns strip common injection phrases from chat history content. Logs warnings for audit trail.

### Thread-Safe TTLCache
`utils/ttl_cache.py` — replaces 9 bare dicts in router files with locked, TTL-evicting caches.

### SQL Injection Prevention
All dynamic SQL now uses either parameterized queries or frozenset assertions before identifier interpolation.

## Related
- [[Data-Integrity/Audit-2026-04-04]]
- [[Architecture/API-Layer]]
- [[Sessions/S20-2026-04-04]]
