# GRID Infrastructure Audit — Claude Code Configuration

**Date**: 2026-03-30 (Updated)
**Project**: GRID — Capital Flow Intelligence API
**Scope**: Agents, Skills, Hooks, Rules, Commands, MCPs, Memory

---

## 1. Agents (`.claude/*.md`)

### GRID-Custom Agents (3)

| Agent | Model | Max Turns | Purpose |
|-------|-------|-----------|---------|
| `pit-reviewer` | sonnet | 15 | PIT correctness — lookahead bias, missing `as_of`, direct table queries |
| `security-scanner` | sonnet | 15 | SQL injection, auth issues, secrets in code, missing headers |
| `simplify` | sonnet | 10 | Duplication, over-engineering, dead code, inconsistency |

### ECC Agents (11) — installed 2026-03-30

| Agent | Model | Purpose |
|-------|-------|---------|
| `planner` | opus | Implementation planning — phases, risks, dependencies |
| `architect` | opus | System design, scalability, ADRs, trade-off analysis |
| `tdd-guide` | sonnet | Test-first workflow — RED/GREEN/REFACTOR, 80%+ coverage |
| `code-reviewer` | sonnet | Confidence-filtered code review — security, quality, perf |
| `python-reviewer` | sonnet | PEP 8, type hints, Pythonic patterns, FastAPI-specific checks |
| `database-reviewer` | sonnet | PostgreSQL optimization, schema design, indexing, RLS |
| `security-reviewer` | sonnet | OWASP Top 10, auth bypasses, injection, secrets |
| `performance-optimizer` | sonnet | Algorithm efficiency, caching, query optimization |
| `build-error-resolver` | sonnet | Diagnose and fix build failures |
| `refactor-cleaner` | sonnet | Dead code removal, module extraction |
| `doc-updater` | sonnet | Documentation maintenance |

### GSD Agents (18) — Get Shit Done v1.28.0

`gsd-planner`, `gsd-executor`, `gsd-debugger`, `gsd-verifier`, `gsd-plan-checker`, `gsd-phase-researcher`, `gsd-project-researcher`, `gsd-research-synthesizer`, `gsd-codebase-mapper`, `gsd-integration-checker`, `gsd-ui-auditor`, `gsd-ui-checker`, `gsd-ui-researcher`, `gsd-nyquist-auditor`, `gsd-user-profiler`, `gsd-advisor-researcher`, `gsd-assumptions-analyzer`, `gsd-roadmapper`

**Total: 32 agents**

---

## 2. Hooks (`.claude/hooks/`)

| Hook | Type | Purpose | Mode |
|------|------|---------|------|
| `grid-sql-guard.js` | PreToolUse | Scans Write/Edit on `.py` files for SQL injection patterns (f-string, .format, concat, % near SQL keywords). Escalates severity for INTERVAL patterns. | **Active** |
| `gsd-statusline.js` | Notification | Context usage bar, current task, model display. Writes bridge file for context-monitor. | **Active** |
| `gsd-context-monitor.js` | PostToolUse | Injects WARNING (≤35%) or CRITICAL (≤25%) context alerts. 5-call debounce. | **Active** |
| `gsd-prompt-guard.js` | PreToolUse | Scans `.planning/` writes for prompt injection (13 patterns + invisible Unicode). Advisory. | **Active** |
| `gsd-workflow-guard.js` | PreToolUse | Warns on edits outside GSD tracking. Opt-in via config. | **Disabled by default** |
| `gsd-check-update.js` | SessionStart | Background npm version check + stale hook detection. | **Active** |

**Total: 6 hooks** (5 GSD + 1 GRID-custom)

---

## 3. Rules (`.claude/rules/`)

### GRID-Specific (5 files, top-level)

| File | Key Directives |
|------|----------------|
| `data-integrity.md` | PIT correctness mandatory, `as_of` required, NaN handling per-module, scheduler.py authoritative |
| `security.md` | No f-strings in SQL, known injection locations, JWT/DB password defaults, WebSocket token leak |
| `testing.md` | pytest, 8 zero-coverage critical modules, PIT tests highest priority |
| `performance.md` | N+1 patterns with file:line refs, missing indexes, O(n²) clustering |
| `frontend.md` | React 18 + Zustand + Vite, no frontend tests exist |

### ECC Common (10 files, `common/`)

`agents.md`, `code-review.md`, `coding-style.md`, `development-workflow.md`, `git-workflow.md`, `hooks.md`, `patterns.md`, `performance.md`, `security.md`, `testing.md`

### ECC Python (5 files, `python/` — all triggered on `**/*.py`, `**/*.pyi`)

`coding-style.md` (PEP 8, frozen dataclasses, black/ruff/isort), `hooks.md` (auto-format, print() warnings), `patterns.md` (Protocol typing, dataclasses, context managers), `security.md` (bandit, dotenv), `testing.md` (pytest marks, coverage)

**Total: 20 rule files across 3 tiers**

---

## 4. Commands (`.claude/commands/`)

### GRID-Custom (4)

| Command | Purpose |
|---------|---------|
| `/audit` | Reviews all 40+ ATTENTION.md issues, checks current status |
| `/fix-sql-injection` | Finds/fixes SQL injection patterns in `.py` files |
| `/new-source` | Scaffolds new ingestion module with PIT compliance |
| `/test-coverage` | Analyzes test gaps, suggests highest-impact tests |

### ECC Commands (7) — installed 2026-03-30

| Command | Purpose |
|---------|---------|
| `/tdd` | Invokes tdd-guide for RED/GREEN/REFACTOR workflow |
| `/code-review` | Security + quality review of uncommitted changes |
| `/plan` | Invokes planner agent for implementation planning |
| `/python-review` | Python-specific code review |
| `/update-codemaps` | Generate token-lean architecture docs in `docs/CODEMAPS/` |
| `/verify` | Verification pass on completed work |
| `/build-fix` | Diagnose and fix build failures |

### GSD Commands (57)

Full project management: planning, execution, review, tracking, workflow, meta.

**Total: 68 commands**

---

## 5. Skills (`.claude/skills/`)

### GRID-Custom (3) — created 2026-03-30

| Skill | Purpose |
|-------|---------|
| `alpha-validation` | Validates predictions against Prediction Causation Standard (lever + condition + thesis + invalidation) |
| `data-health` | Checks freshness of 37+ data sources, NaN accumulation, API key availability |
| `actor-network-query` | Query 475+ named actors, trust scores, dollar flows, network expansion |

### ECC Skills (5)

| Skill | Purpose |
|-------|---------|
| `python-patterns` | Comprehensive Python idioms — decorators, concurrency, packaging |
| `python-testing` | pytest patterns, fixtures, mocking, coverage |
| `tdd-workflow` | Full TDD methodology with examples |
| `security-review` | Security audit workflow |
| `search-first` | Research-before-code methodology |

**Total: 8 local skills** (+ Cowork plugin skills: data, design, finance, marketing, legal, productivity)

---

## 6. Connected MCPs

### Active & Relevant

| MCP | Tools | Relevance |
|-----|-------|-----------|
| **OKX Crypto** | `get_book`, `get_candlestick`, `get_ticker(s)`, `get_trades`, etc. | Crypto market data |
| **Crypto.com** | Same pattern as OKX | Crypto market data |
| **Google Calendar** | Event CRUD, free time, meeting times | Scheduling |
| **Gmail** | Search, read, draft | Communication |
| **Google Drive** | Search, fetch | File access |
| **Claude in Chrome** | Browser automation, page reading | Web scraping |

### Suggested — Not Yet Connected (2026-03-30)

| MCP | Why |
|-----|-----|
| **S&P Global** | Company fundamentals, relationships, capitalization — institutional data |
| **FactSet AI-Ready Data** | Prices, fundamentals, estimates, M&A, geographic revenue |
| **Moody's** | Credit opinions, sector outlooks, upgrade/downgrade factors |
| **MT Newswires** | Real-time financial news — event-driven trading signals |
| **Bigdata.com** | Real-time financial data, event calendars, tearsheets |
| **LunarCrush** | Social media sentiment for crypto + stocks — feeds social signal type |
| **Aiera** | Live earnings calls, SEC filings, equity summaries — feeds insider/congressional |
| **Quartr** | Company research, earnings events, financial documents |

### Low Relevance — Consider Removing

| MCP | Issue |
|-----|-------|
| ICD-10 Medical Codes | Zero GRID relevance |
| Netlify | GRID deploys via Cloudflare Tunnel, not Netlify |
| Domain Name Checker | One-time utility, not ongoing |

---

## 7. Memory System

| File | Type | Content |
|------|------|---------|
| `user_anik.md` | user | Founder, Palantir-as-a-service vision, autonomous work style |
| `project_grid_state.md` | project | 222K LOC, 652 tests, deployed, 37+ sources |
| `project_subnet_doa.md` | project | Bittensor subnet killed — economics don't work |
| `reference_grid_architecture.md` | reference | Tech stack, data flow pipeline, deployment, directory structure |
| `reference_grid_signals.md` | reference | 11 signal source types, evaluation windows, confidence labels |
| `reference_grid_gotchas.md` | reference | SQL injection locations, PIT traps, NaN issues, zero-coverage modules |
| `feedback_work_style.md` | feedback | Autonomous execution, action over discussion, alpha-first |
| `project_ecc_install.md` | project | ECC v1.9.0 components installed — full inventory |

**Total: 8 memory entries**

---

## 8. AUDIT SCORECARD — Updated

| Category | Before | After | Change | Notes |
|----------|--------|-------|--------|-------|
| **Security** | 6/10 | 7/10 | +1 | SQL guard hook added. Still need to verify fixes at regime.py/log.py |
| **Testing** | 5/10 | 5/10 | — | tdd-guide agent + /tdd command added but no new tests written yet |
| **Data Integrity** | 8/10 | 8/10 | — | Already strong. data-health skill adds monitoring capability |
| **Automation** | 4/10 | 6/10 | +2 | SQL guard hook, ECC agents auto-trigger on code changes |
| **Market Data** | 3/10 | 3/10 | — | 8 MCPs suggested, awaiting user connection |
| **Memory** | 3/10 | 7/10 | +4 | 5 new entries: architecture, signals, gotchas, feedback, install state |
| **Skill Specialization** | 2/10 | 7/10 | +5 | 3 GRID skills (alpha, data-health, actor) + 5 ECC Python skills |
| **Agent Coverage** | 7/10 | 9/10 | +2 | 11 ECC agents added (planner, tdd, reviewer, architect, DB, etc.) |
| **MCP Relevance** | 4/10 | 4/10 | — | Suggestions made, awaiting connection decisions |
| **Rules** | 7/10 | 9/10 | +2 | ECC common (10) + Python (5) with path triggers added |
| **Overall** | **5/10** | **7/10** | **+2** | Foundation hardened. Next: connect MCPs, write tests, build GRID MCP server |

---

## 9. WHAT WAS DONE (2026-03-30)

### Installed
- 11 ECC agents (planner through doc-updater)
- 10 ECC common rules + 5 ECC Python rules (with `paths:` triggers)
- 7 ECC commands (/tdd, /code-review, /plan, /python-review, /update-codemaps, /verify, /build-fix)
- 5 ECC skills (python-patterns, python-testing, tdd-workflow, security-review, search-first)
- 1 GRID SQL injection guard hook (grid-sql-guard.js)
- 3 GRID-custom skills (alpha-validation, data-health, actor-network-query)
- 5 new memory entries (architecture, signals, gotchas, feedback, install state)

### Suggested (Awaiting Action)
- 8 market data MCPs (S&P Global, FactSet, Moody's, MT Newswires, Bigdata, LunarCrush, Aiera, Quartr)
- Remove 3 irrelevant MCPs (ICD-10, Netlify, Domain Checker)

---

## 10. REMAINING HIGH-IMPACT ITEMS

### Immediate
1. **Connect market data MCPs** — S&P Global, FactSet, LunarCrush, Aiera are highest value
2. **Verify SQL injection fixes** — Run `/fix-sql-injection` to check regime.py and log.py
3. **Run `/update-codemaps`** — Generate architecture docs for agent context

### Short-Term
4. **Build GRID MCP server** — Wraps grid.stepdad.finance API so Claude queries live data directly
5. **Set up scheduled tasks** — Daily data freshness check using data-health skill
6. **Write tests for zero-coverage modules** — Use `/tdd` on resolver.py, entity_map.py, gates.py
7. **Remove irrelevant MCPs** — ICD-10, evaluate Netlify and Domain Checker

### Medium-Term
8. **Alpha leaderboard skill** — Query prediction journal, compute accuracy by source/actor/lever
9. **Automated PIT audit** — Scheduled weekly scan for PIT violations
10. **Production config validation hook** — Check JWT/DB defaults aren't active

---

*This document is a living audit. Update it as changes are made.*
