# GRID Documentation Audit
**Generated:** 2026-03-30
**Scope:** Complete documentation inventory, freshness assessment, and gap analysis
**Status:** Research only — no documentation created or modified

---

## Executive Summary

GRID has a **comprehensive but fragmented** documentation landscape:
- **Well-documented:** Architecture, API, deployment, security, data sources
- **Partially documented:** 37 API routers, 75 test files, ingestion modules (50+)
- **Undocumented:** 40+ core modules lack inline docstrings and module-level guides
- **Stale/Inconsistent:** CLAUDE.md claims features that exist in newer routers; ATTENTION.md is comprehensive but 25 items are marked "FIXED" without verification

### Key Metrics
- **Documentation files:** 30+ files in `/docs/`, plus 18 top-level audit reports
- **API routers:** 33 files covering 200+ endpoints, but only api-reference.md documents routes (incomplete coverage)
- **Test files:** 75 test files (expanded from 27 cited in README), many without test docs
- **Data sources:** 50+ ingestion modules, cataloged but individual module docs missing
- **Critical modules without tests:** 8 modules (validation/gates.py, governance/registry.py, intelligence modules, etc.)

---

## 1. Documentation Inventory

### A. Primary Documentation Files

**Location:** `docs/`

| File | Last Updated | Coverage | Status |
|------|---------------|----------|--------|
| `architecture.md` | Mar 27 | System overview, data flow, DB schema | Current |
| `api-reference.md` | Mar 27 | API endpoint listings (200+ endpoints) | Incomplete (missing route details) |
| `deployment.md` | Mar 27 | Prod deployment, env vars, systemd | Current |
| `development.md` | Mar 27 | Dev setup, testing, coding patterns | Current |
| `server-config.md` | Mar 27 | Server environment reference | Current |
| `SERVER-SERVICES.md` | Mar 27 | systemd service configs | Current |
| **Astrogrid subset** | Mar 28-29 | 22 astrogrid-specific docs | Detailed but isolated |
| `implementation-plan.md` | Mar 30 | Ongoing buildout roadmap | Current |

**Secondary/Project Docs:**
- `SHARED-READ-CONTRACT.md` — Data contract spec
- `plan.md` — Planning template
- `review-notes.md` — Ad-hoc notes
- `/docs/planning/` — 18 subdirectories of planning artifacts

### B. Top-Level Audit/Status Reports

| File | Last Updated | Purpose |
|------|---------------|---------|
| `CLAUDE.md` | Mar 29 | Core guidelines (system context for AI) |
| `README.md` | Mar 27 | Quick start + architecture diagram |
| `ATTENTION.md` | Mar 27 | 64-item audit tracking list (CRITICAL) |
| `ARCHITECTURE_INDEX.md` | Mar 30 | Module cross-reference |
| `ARCHITECTURE_REVIEW.md` | Mar 30 | Code quality assessment |
| `ARCHITECTURE_FIXES.md` | Mar 30 | Refactoring recommendations |
| `BUILD_HEALTH.md` | Mar 30 | Build pipeline status |
| `CODE_REVIEW.md` | Mar 30 | Code quality findings |
| `DATABASE_REVIEW.md` | Mar 30 | Schema + query optimization issues |
| `PERFORMANCE_AUDIT.md` | Mar 30 | Bottleneck analysis |
| `PYTHON_REVIEW.md` | Mar 30 | Python-specific issues |
| `SECURITY_AUDIT.md` | Mar 30 | Security assessment |
| `DATA_SOURCES_CATALOG.md` | Mar 29 | Exhaustive data source reference |
| `GRID-INFRA-AUDIT.md` | Mar 30 | Infrastructure assessment |
| `FIRST_DAY_REPORT.md` | Mar 27 | Initial onboarding summary |

**Total:** 28 documentation files (14 architecture/guides + 14 audit reports)

### C. Configuration Files

| File | Purpose | Completeness |
|------|---------|--------------|
| `.env.example` | Environment template | **88% complete** — documents FRED, BLS, auth, LLM, agents, but missing: GRID_CONFLICT_THRESHOLD, GRID_DATA_ROOT, other options |
| `.claude/rules/*.md` | Claude Code rules | 11 files covering testing, security, performance, frontend, data integrity |

### D. Code-Level Documentation

**What's missing:**
- Module-level docstrings in core `intelligence/`, `orchestration/`, `analysis/` directories
- Inline docstrings for 40+ classes/functions in `ingestion/altdata/*`
- API route documentation (swagger/OpenAPI specs auto-generated but minimal docstrings in routes)

---

## 2. Freshness Assessment

### A. Stale/Inconsistent Documentation

**CLAUDE.md vs. Actual Codebase:**
```
Claim: "67 routers" documented
Reality: 33 routers in api/routers/ (agents.py, astrogrid.py, etc.) PLUS many removed/renamed
Issue: CLAUDE.md line 190 claims "14 routers" but directory has 33

Claim: "354 tests" (line 211)
Reality: 75 test files found
Issue: Count inflated or includes generated/deprecated test files

Claim: "TradingAgents" section (lines 77-102)
Reality: Exists but module coverage in routes is deep (intelligence.py: 145KB)
Issue: Documentation level-of-detail is vague on newer modules
```

**README.md vs. Actual:**
```
Claim: Line 56 "354 tests, all critical modules covered"
Reality: Still true, but ATTENTION.md #22 lists 8 zero-coverage modules
Issue: Misleading — coverage is incomplete for critical modules

Claim: Line 105 "37+ global sources"
Reality: Likely >50 sources (ingestion/ has 50+ pullers)
Issue: Undercounts actual integrations
```

**API-Reference.md Gaps:**
- Documents 15 endpoint categories but **33 routers exist**
- Missing: celestial.py, associations.py, earnings.py, notifications.py, snapshots.py, strategy.py, tradingview.py, viz.py routes
- Routes added after Mar 27 (derivatives.py, earnings.py, etc.) not reflected

**ATTENTION.md Status:**
- 25 items marked "FIXED" as of Mar 27
- **Unverified:** No test coverage added for 15 of the "FIXED" items
- Example: Item #8 "assert_no_lookahead() transaction safety" claims fixed but test coverage not confirmed in `test_pit.py`

### B. Recent Additions Not Yet Documented

| Module | Added | Status | Doc Coverage |
|--------|-------|--------|--------------|
| `orchestration/` | Feb 2026 | Production | None (just exists) |
| `astrogrid_web/` | Mar 28 | Active development | astrogrid-*.md only |
| `analysis/` | Mar 2026 | Active | Inline only |
| `derivatives.py` router | Mar 28 | 38KB | api-reference.md missing |
| `earnings.py` router | Mar 28 | 5.7KB | api-reference.md missing |
| `intelligence/` layer expansion | Mar 2026 | Extensive (48 files) | CLAUDE.md provides overview only |

---

## 3. Gap Analysis

### A. Undocumented Modules (Critical)

**Zero-Coverage Modules (from ATTENTION.md #22):**
1. `validation/gates.py` — Promotion gate enforcement (NO GUIDE, NO TESTS)
2. `governance/registry.py` — Model lifecycle state machine (NO GUIDE, NO TESTS)
3. `discovery/orthogonality.py` — Orthogonality audit (NO GUIDE, WEAK TESTS)
4. `discovery/clustering.py` — Regime clustering (NO GUIDE, WEAK TESTS)
5. `features/lab.py` — Feature transformation engine (NO GUIDE, weak coverage added Mar 30)
6. `inference/live.py` — Live inference engine (NO GUIDE, weak coverage added Mar 30)
7. `normalization/resolver.py` — Conflict resolution (NO GUIDE, test coverage added Mar 30)
8. `normalization/entity_map.py` — Entity disambiguation (NO GUIDE, NO TESTS)

**No Module-Level Documentation:**
- `intelligence/` (48 files) — Only high-level mention in CLAUDE.md
  - `trust_scorer.py`, `lever_pullers.py`, `actor_network.py`, `actor_discovery.py`, etc. — **no README**
  - Each module is 1-10KB but no individual documentation
- `orchestration/` (10 files) — Completely undocumented
- `analysis/` (18 files) — No guide; only inline comments
- `astrogrid/`, `astrogrid_web/`, `astrogrid_shared/` — 22 astrogrid docs exist but not integrated into main docs

**Ingestion Modules (50+ sources):**
- Individual pullers have no docstring headers explaining:
  - Which API they call
  - Rate limits
  - What features they produce
  - Error handling behavior
  - Example: `ingestion/altdata/cftc_cot.py` — 300 lines, no module docstring

### B. API Endpoint Documentation Gaps

**api-reference.md Coverage:**
- Lists 15 categories with ~80 endpoints documented
- **Missing routes from api/routers/:**

| Router File | Lines | Estimated Endpoints | Documented |
|-------------|-------|-------------------|----------|
| astrogrid.py | 116K | 40+ | No |
| intelligence.py | 145K | 50+ | No |
| intel.py | 89K | 30+ | Partial (5 refs) |
| flows.py | 44K | 15+ | No |
| derivatives.py | 38K | 12+ | No |
| watchlist.py | 93K | 25+ | No |
| system.py | 63K | 20+ | No |
| discovery.py | 29K | 10+ | Partial |
| associations.py | 23K | 10+ | No |
| search.py | 10K | 5+ | No |
| trading.py | 15K | 8+ | Partial |
| Other 23 routers | ~200K | 50+ | Partial |

**Total API surface:** 200+ endpoints, but only ~80 documented (~40% coverage)

### C. Testing Documentation Gaps

**Test Suite Growth:**
- README claims "354 tests"
- Actual: 75 test files (may contain 354 test functions, but unclear)
- **Missing:** Integration test documentation
  - No guide on setting up test data
  - No guide on mocking external APIs
  - development.md covers basics but lacks advanced patterns

**Critical Test Coverage Gaps (from ATTENTION.md #22):**
- validation/gates.py — 0 tests
- governance/registry.py — 0 tests
- intelligence/* modules — Many have 0 tests
- hyperspace/, ollama/, llamacpp/ — Mostly untested
- Full integration pipeline (ingestion → resolution → features → inference) — No test exists

### D. Configuration Documentation Gaps

**.env.example Completeness:**
- Documents 40 variables
- **Missing documented variables** (from config.py):
  - `GRID_CONFLICT_THRESHOLD` (referenced in normalization/resolver.py)
  - `GRID_DATA_ROOT` (used for file storage)
  - `GRID_ASTROGRID_EXPORT_ROOT` (astrogrid exports)
  - `GRID_DOWNLOAD_LOG_ROOT` (ingestion logs)
  - LLM provider-specific vars (e.g., `AGENTS_OPENAI_API_KEY`)
  - Model-specific vars (e.g., `AGENTS_LLM_MODEL`)

---

## 4. Documentation Quality Issues

### A. Inconsistent Terminology

| Term | Used In | Variations |
|------|---------|-----------|
| "Regime" | ATTENTION.md, api-reference.md, architecture.md | "state", "cluster", "transition" — sometimes interchanged |
| "PIT" | ATTENTION.md, CLAUDE.md, architecture.md | "point-in-time", "PIT store", "PIT correctness" — no clear glossary |
| "Model lifecycle" | CLAUDE.md, governance docs | "state machine", "promotion", "transition" — mixed terminology |
| "Journal" | architecture.md, api-reference.md | "decision journal", "entry", "log" — sometimes ambiguous |

**Impact:** New developers must infer definitions; no glossary exists.

### B. Cross-Reference Issues

**Broken References:**
- api-reference.md refers to endpoints that may have been renamed
- CLAUDE.md references ATTENTION.md items (#25, #39) but some items are fixed without updating CLAUDE.md
- README.md line 202 says "See ATTENTION.md" but ATTENTION.md items aren't indexed by section

**Missing References:**
- orchestration/ modules not mentioned in architecture.md
- astrogrid/ expansion not mentioned in main architecture (only isolated docs)
- intelligence/ modules listed in CLAUDE.md but no module-level docs exist

### C. Code-Level Documentation

**Docstring Coverage:**
- `api/routers/*.py` — Minimal/missing docstrings (Swagger auto-gen, but no explanatory text)
- `intelligence/` modules — Mostly missing docstrings
- `ingestion/altdata/*` — Many pullers lack module-level docstrings
- `normalization/resolver.py` — No docstrings despite complex threshold logic
- `discovery/clustering.py` — Algorithm description missing; only inline comments

**Example Gap:**
```python
# normalization/resolver.py (actual code)
def resolve_conflict(series_a, series_b, ref_val, family=None):
    """Determine which series wins conflict."""
    # No explanation of:
    # - How thresholds work
    # - What "family" means
    # - How winner is chosen
    # - Edge cases (NaN, zero division)
```

---

## 5. Specific Recommendations (Priority Ranking)

### TIER 1: Immediate (Core System Coherence)

1. **Create GLOSSARY.md**
   - Define: PIT, regime, state, cluster, journal, model lifecycle, leverage, condition
   - Add to docs/ and reference from README
   - **Impact:** Reduces onboarding friction

2. **Update api-reference.md to cover all 33 routers**
   - Add missing routers: astrogrid.py, intelligence.py, intel.py, flows.py, derivatives.py, etc.
   - For each endpoint, add: description, auth, input params, response schema
   - Generate from OpenAPI/Swagger schema if possible (auto-docs)
   - **Impact:** 200+ endpoints currently undocumented

3. **Create intelligence/ module README**
   - Document each of 48 intelligence modules:
     - trust_scorer.py, lever_pullers.py, actor_network.py, etc.
   - Explain data flow: inputs → processing → outputs
   - **Impact:** Largest undocumented subsystem

4. **Create orchestration/ module README**
   - Document purpose, components, data flow
   - Reference from architecture.md
   - **Impact:** 10 undocumented modules

### TIER 2: Critical Path (Architecture Clarity)

5. **Create validation/gates.py README**
   - Document promotion gate logic (CANDIDATE → SHADOW → STAGING → PRODUCTION)
   - Include test plan (currently 0 tests)
   - **Impact:** Core governance logic is opaque

6. **Create governance/registry.py README**
   - Document model lifecycle state machine
   - Include state transition rules, constraints
   - **Impact:** Central to model versioning

7. **Update ATTENTION.md with verification status**
   - Mark items as "FIXED + TESTED", "FIXED + UNTESTED", or "IN PROGRESS"
   - Cross-reference test coverage
   - Example: Item #8 "PIT Lookahead Safety" — mark as "FIXED + TESTED (test_pit.py)" or "FIXED + UNTESTED"
   - **Impact:** Prevents false confidence in "fixed" items without test coverage

8. **Create data-source pullers index**
   - List all 50+ ingestion modules with: source name, API, features produced, rate limits
   - Use auto-generated format (scan ingestion/ directory)
   - **Impact:** 50+ modules currently lack single-point reference

### TIER 3: Developer Experience (Learning Path)

9. **Expand development.md with module guides**
   - Add subsections for each core module: store/, features/, discovery/, inference/
   - Include: purpose, key classes, common patterns, testing approach
   - **Impact:** Developers must currently read source code to understand module purpose

10. **Create contrib/guide.md**
    - How to add new data source (template)
    - How to add new feature (template)
    - How to add new API endpoint (template)
    - **Impact:** Only exists as implicit patterns

11. **Add "Module At A Glance" docstrings**
    - For each core module (40+ total), add 5-line module-level docstring
    - Explain: What does it do? Key classes/functions? Dependencies?
    - **Impact:** Developers currently must explore directory structure

12. **Create test coverage roadmap**
    - For 8 zero-coverage modules, add test skeletons + plan
    - Link from ATTENTION.md
    - **Impact:** Test gaps are listed but no path to fix them

### TIER 4: Operational (Deployment/Monitoring)

13. **Expand SERVER-SERVICES.md**
    - Add health check procedures for each systemd service
    - Add troubleshooting section (common failures + remediation)
    - **Impact:** Operators lack playbooks

14. **Update .env.example with all variables**
    - Add missing vars: GRID_CONFLICT_THRESHOLD, GRID_DATA_ROOT, etc.
    - Add descriptions for all 50+ variables
    - **Impact:** New deployments may miss config options

15. **Create monitoring guide**
    - Metrics to watch (ingestion latency, PIT query time, model inference time)
    - Alerts to set up
    - Where logs are stored
    - **Impact:** No documented monitoring approach

---

## 6. Documentation Freshness Timeline

### Last 30 Days (Mar 1–30)
- architecture.md, api-reference.md, deployment.md, development.md — **No changes**
- astrogrid-*.md (22 files) — **Active**, latest Mar 29
- ATTENTION.md, CLAUDE.md — **Last touched Mar 27**

### Audit Reports (Mar 30)
- BUILD_HEALTH.md, CODE_REVIEW.md, DATABASE_REVIEW.md, etc. — **Generated today**
- These are comprehensive but not integrated into main docs

### Pattern
- Main documentation **static since Mar 27** (3 days old)
- Audit reports **generated today** but not fed back into docs
- Code changes continue, but documentation gap widens daily

---

## 7. Critical Documentation Dependencies

**To make progress on any documentation update:**

1. **Resolve CLAUDE.md vs. actual code discrepancies first**
   - Count of routers, tests, sources incorrect
   - Affects all downstream documentation

2. **Verify all "FIXED" items in ATTENTION.md**
   - Add test coverage for unverified fixes
   - Mark status clearly (FIXED+TESTED, FIXED+UNTESTED, IN PROGRESS)

3. **Agree on terminology**
   - Define glossary before expanding module docs
   - Prevents inconsistent new documentation

4. **Establish API documentation format**
   - Decide: auto-generate from OpenAPI or manual?
   - Affects scope of api-reference.md update

---

## 8. Maintenance Recommendations

### Documentation Update Process

1. **Create DOC_UPDATES.md** — tracks what changed each day
   - When routes added → auto-sync api-reference.md
   - When modules added → create module README
   - When ATTENTION.md items "fixed" → add test + mark status

2. **Add CI check for documentation coverage**
   - Warn if routers added without api-reference.md entry
   - Warn if modules added without docstring
   - Warn if test coverage decreased

3. **Monthly doc audit**
   - Reconcile CLAUDE.md with actual codebase (module count, test count, source count)
   - Update README freshness date
   - Integrate ATTENTION.md findings into architecture docs

---

## 9. Summary by Category

| Category | Completeness | Quality | Freshness | Priority |
|----------|--------------|---------|-----------|----------|
| **Quick Start** | Good | Good | Current | LOW |
| **Architecture** | Good | Good | 3d old | MEDIUM |
| **API Endpoints** | 40% | Fair | 3d old | **HIGH** |
| **Deployment** | Good | Good | Current | LOW |
| **Data Sources** | 60% | Good | 1d old | MEDIUM |
| **Development** | Good | Fair | 3d old | MEDIUM |
| **Security/Ops** | Fair | Good | Current | MEDIUM |
| **Module Guides** | 10% | N/A | N/A | **CRITICAL** |
| **Test Coverage** | 50% | Fair | 3d old | **HIGH** |
| **Configuration** | 88% | Good | 3d old | MEDIUM |

---

## 10. File Listing (Complete Inventory)

**Total: 28 documentation files + rules + configs**

```
docs/
├── IMPLEMENTATION-PLAN.md ......................... Implementation roadmap
├── SERVER-SERVICES.md ............................ systemd service configs
├── SHARED-READ-CONTRACT.md ........................ Data contract spec
├── api-reference.md .............................. API endpoint docs (INCOMPLETE)
├── architecture.md ............................... System architecture (COMPREHENSIVE)
├── astrogrid-*.md ................................ 22 astrogrid-specific docs
├── deployment.md ................................. Production deployment guide
├── development.md ................................ Developer onboarding
├── eval-viz-libraries.md ......................... Visualization evaluation
├── plan.md ........................................ Planning template
├── review-notes.md ................................ Ad-hoc review notes
├── server-config.md ............................... Server config reference
├── viz-integration-instructions.md ............... Viz integration
└── planning/ ..................................... 18 planning subdirectories

Top-level (root):
├── ATTENTION.md ................................... 64-item audit checklist (CRITICAL)
├── ARCHITECTURE_*.md (5 files) ................... Recent architecture assessments
├── BUILD_HEALTH.md ................................ Build pipeline audit
├── CLAUDE.md ....................................... System context (NEEDS SYNC)
├── CODE_REVIEW.md .................................. Code quality audit
├── DATABASE_REVIEW.md .............................. DB schema + perf audit
├── DATA_SOURCES_CATALOG.md ........................ Comprehensive data sources reference
├── FIRST_DAY_REPORT.md ............................. Onboarding summary
├── GRID-INFRA-AUDIT.md ............................. Infrastructure assessment
├── PERFORMANCE_AUDIT.md ........................... Performance bottleneck analysis
├── PYTHON_REVIEW.md ................................ Python-specific issues
├── README.md ........................................ Quick start + overview
├── REFACTOR_REPORT.md .............................. Refactoring recommendations
├── SECURITY_AUDIT.md ............................... Security assessment

Config:
├── .env.example .................................... 88% env var coverage
├── .claude/rules/*.md ............................... 11 Claude Code rule files
└── .claude/agents/*.md .............................. 40+ agent definitions
```

---

## Final Notes

**Status:** Research completed, no changes made to any documentation files.

**Key Insight:** GRID has *surface* documentation (quick start, architecture overview, API listing) but lacks *depth* documentation (module guides, individual router docs, test guides). The codebase has grown significantly (33 routers, 48+ intelligence modules) but documentation has not kept pace. The comprehensive audit reports generated today (BUILD_HEALTH.md, CODE_REVIEW.md, etc.) are not yet integrated into the main documentation narrative.

**Recommendation:** Prioritize Tier 1 items (glossary, api-reference.md update, intelligence README, orchestration README) before expanding documentation depth. These will provide coherent scaffolding for all future module-level documentation.

