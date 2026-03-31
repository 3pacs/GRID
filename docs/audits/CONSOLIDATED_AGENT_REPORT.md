# GRID Consolidated Agent Report — 10-Agent Deep Audit

**Date**: 2026-03-30
**Codebase**: 222K LOC, Python 3.11+, FastAPI + PostgreSQL 15 + TimescaleDB + React 18 PWA
**Agents Run**: architect, tdd-guide, code-reviewer, python-reviewer, database-reviewer, security-reviewer, performance-optimizer, build-error-resolver, refactor-cleaner, doc-updater

---

## Verdict: BLOCK — 11 CRITICAL findings across 6 domains

The codebase is architecturally sound (grade B, clean 8-layer separation, no circular imports) and production-deployed. But 11 critical issues must be resolved before scaling beyond the current single-user deployment.

---

## CRITICAL Findings (Fix Immediately)

| # | Finding | Source Agent | File(s) | Impact |
|---|---------|-------------|---------|--------|
| 1 | **WebSocket token leakage** — JWT in query params leaks to logs/proxies | security | `api/main.py:571-591` | Credential theft |
| 2 | **Default JWT secret** — `"dev-secret-change-me"` in production | security | `api/auth.py:35` | Auth bypass |
| 3 | **Default DB password** — `"changeme"` in config | security | `config.py:50` | Full DB compromise |
| 4 | **SQL injection (4 NEW locations)** — f-strings in SQL queries | python-reviewer | `trust_scorer.py:696`, `signal_taxonomy.py:218`, `push_notify.py:174`, `oracle/engine.py:626` | Data exfiltration |
| 5 | **SQL injection (2 KNOWN)** — `.format()` in INTERVAL clauses | database | `regime.py:85-93`, `journal/log.py:241` | Data exfiltration |
| 6 | **NaN bug** — `z == z` identity check instead of `math.isnan()` | code-reviewer | `signals.py:83` | Silent data corruption |
| 7 | **Schema mismatch** — queries `confidence_label` column that doesn't exist | code-reviewer | `intel.py:169` | Runtime crashes |
| 8 | **DB pool exhaustion** — default 10 connections, fails at 20+ users | performance | `db.py:43-52` | Service outage |
| 9 | **O(n²) clustering** — 60-180s for 10K observations | performance | `clustering.py:264-341` | Timeout/OOM |
| 10 | **Unbounded feature matrix** — loads 80+ years into memory | performance | `pit.py` + `clustering.py:102-109` | OOM crash |
| 11 | **Missing entity_map tests** — 1410 LOC with zero test coverage | code-reviewer | `normalization/entity_map.py` | Silent regression |

---

## HIGH Priority Findings

| # | Finding | Source Agent | File(s) |
|---|---------|-------------|---------|
| 1 | Missing DB indexes on `decision_journal(model_version_id, decision_timestamp)` | database | `schema.sql` |
| 2 | PIT lookahead race condition — `assert_no_lookahead()` doesn't rollback | database | `store/pit.py` |
| 3 | N+1 query in models endpoint | performance | `api/routers/models.py:91-98` |
| 4 | N+1 query in orthogonality | performance | `discovery/orthogonality.py:75-80` |
| 5 | Incomplete API key validation (only FRED checked at startup) | security | `config.py` |
| 6 | Non-persistent rate limiting (in-memory, resets on restart) | security | `api/auth.py:110-120` |
| 7 | CORS misconfiguration | security | `api/main.py` |
| 8 | 8 zero-coverage critical modules | tdd-guide | See table below |
| 9 | Missing pagination metadata on list endpoints | performance | `journal.py:36-66` |
| 10 | `intelligence.py` router — 3,871 lines, 82 endpoints | refactor | `api/routers/intelligence.py` |
| 11 | 67/104 ingestion modules duplicate `_resolve_source_id()` | refactor | `ingestion/` |
| 12 | `actor_network.py` — 7,002 lines (70% static data) | refactor | `intelligence/actor_network.py` |
| 13 | Async/sync mixing in API handlers | python-reviewer | Multiple routers |
| 14 | Bare `except:` clauses swallowing errors | python-reviewer | Multiple files |
| 15 | CLAUDE.md out of date — claims 14 routers (actually 33), 354 tests (actually 75 files) | doc-updater | `CLAUDE.md` |

---

## Zero-Coverage Critical Modules

| Module | LOC | Test Status | Agent Finding |
|--------|-----|-------------|---------------|
| `normalization/entity_map.py` | ~400 | **Truly missing** | code-reviewer |
| `normalization/resolver.py` | ~600 | Weak (exists but thin) | tdd-guide |
| `features/lab.py` | ~600 | Weak | tdd-guide |
| `discovery/orthogonality.py` | ~400 | Weak | tdd-guide |
| `discovery/clustering.py` | ~500 | Weak | tdd-guide |
| `validation/gates.py` | ~400 | Weak | tdd-guide |
| `governance/registry.py` | ~350 | Weak | tdd-guide |
| `inference/live.py` | ~400 | Weak | tdd-guide |

**TDD assessment**: 6 of 8 "zero-coverage" modules actually have tests, but coverage is below 30%. `entity_map.py` truly has nothing. The tdd-guide produced 5 test skeleton suites (~800 LOC) ready for implementation.

---

## Architecture Assessment

**Grade: B** (production-ready at 100-500 users with fixes)

**Strengths:**
- Clean 8-layer architecture with no circular dependencies
- PIT correctness enforced at the query engine level
- Immutable decision journal with trigger-based protection
- Well-normalized database schema with proper constraints
- Graceful degradation when LLM services are offline

**Weaknesses:**
- Connection pool sizing for production scale
- No connection pool monitoring
- Missing security headers (X-Content-Type-Options, X-Frame-Options, HSTS, CSP)
- `@lru_cache()` on DB engine never clears — config changes require restart
- Health endpoint only checks DB connectivity, not data freshness or pool health

---

## Refactoring Priorities

**Phase 1 — Quick Wins (1-2 days):**
- Add 3 missing DB indexes (5 min)
- Increase connection pool defaults (2 min)
- Fix 6 SQL injection locations (2-3h)
- Fix NaN bug in signals.py (10 min)
- Fix schema mismatch in intel.py (20 min)
- Add pagination metadata to list endpoints (1h)

**Phase 2 — Security Hardening (2-3 days):**
- Migrate WebSocket auth to first-message pattern
- Add security headers middleware
- Validate all API keys at startup
- Implement persistent rate limiting (Redis or DB-backed)
- Rotate JWT secret and DB password in production

**Phase 3 — Test Coverage (2-3 weeks):**
- Write tests for 8 zero-coverage modules (target 80%+)
- Add integration tests for full pipeline
- Add API endpoint tests beyond login

**Phase 4 — Performance (1 week):**
- Vectorize clustering O(n²) loop
- Add bounds to feature matrix loading
- Fix N+1 queries with JOINs
- Add connection pool monitoring

**Phase 5 — Refactoring (2-3 weeks):**
- Split `intelligence.py` router into 4 domain files
- Extract `actor_network.py` static data to YAML config
- Extract shared ingestion patterns to eliminate duplication
- Split other oversized routers (>800 lines)

---

## Build Health

**Status: 90% Ready**
- All Python modules syntactically valid, no circular imports
- Frontend: 95 files, all imports resolve, build output exists
- Blocker: venv broken (macOS→Linux transition) — rebuild with `pip install -r requirements.txt`
- Python 3.10 available in sandbox but 3.11+ required

---

## Documentation Gaps

- CLAUDE.md has stale counts (14 vs 33 routers, 354 tests vs 75 files)
- 40% of API surface undocumented in api-reference.md
- `intelligence/` (48 files) has no module README
- `orchestration/` completely undocumented
- 25 ATTENTION.md items marked "FIXED" without test verification
- Missing GLOSSARY.md for domain terms (PIT, regime, lever, etc.)

---

## Individual Report Locations

| Agent | Report | Key Verdict |
|-------|--------|-------------|
| architect | `ARCHITECTURE_REVIEW.md` + `_FIXES.md` + `_INDEX.md` + `_EXECUTIVE_SUMMARY.md` | Grade B, CRITICAL: pool exhaustion |
| tdd-guide | `TDD_ASSESSMENT.md` | 6/8 have weak tests, 5 skeletons ready |
| code-reviewer | `CODE_REVIEW.md` | BLOCK — 3 CRITICAL |
| python-reviewer | `PYTHON_REVIEW.md` | 4 NEW SQL injection locations |
| database-reviewer | `DATABASE_REVIEW.md` | Missing PIT index, race condition |
| security-reviewer | `SECURITY_AUDIT.md` | 3 CRITICAL, 5 HIGH |
| performance-optimizer | `PERFORMANCE_AUDIT.md` | 3 CRITICAL bottlenecks |
| build-error-resolver | `BUILD_HEALTH.md` | 90% ready, venv rebuild needed |
| refactor-cleaner | `REFACTOR_REPORT.md` + `_INDEX.md` + `_SUMMARY.txt` | 3,871-line router, 67/104 duplication |
| doc-updater | `DOC_AUDIT.md` | 40% API undocumented, stale CLAUDE.md |

---

## Recommended Execution Order

1. **Now**: Fix SQL injections (6 locations) + NaN bug + schema mismatch = 3-4 hours
2. **Today**: Add DB indexes + increase pool size + fix pagination = 1 hour
3. **This week**: Security hardening (WebSocket, headers, rate limiting, secrets) = 2-3 days
4. **Next sprint**: Test coverage for 8 critical modules = 2-3 weeks
5. **Following sprint**: Performance optimization + refactoring = 3-4 weeks

**Total estimated effort to reach grade A: ~120 hours across 6-8 weeks**

---

*This is a living document. Update as findings are resolved. Cross-reference ATTENTION.md for the full 64-item audit.*
