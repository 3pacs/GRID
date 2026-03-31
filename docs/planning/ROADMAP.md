# GRID Roadmap — Financial Palantir

> **Single source of truth.** Every session starts here. Updated 2026-03-31 (post-audit).
>
> GRID exists to give a solo operator the same decision-quality as a team of analysts,
> quants, and OSINT specialists. Intelligence amplifier, not a trading bot.

---

## System Snapshot (2026-03-31, updated session 2)

| Metric | Value |
|--------|-------|
| Tests | 1,282+ across 80+ files |
| Python modules | 370+ |
| Frontend views | 51 (45 routes in App.jsx) |
| Intelligence modules | 14 (split into focused subpackages) |
| Active data sources | 46 (54 deactivated — duplicates + noise cleaned) |
| Features registered | 1,238 |
| Features with data | 1,194 (96.4% coverage) |
| Raw data rows | 76.7M |
| Resolved rows | 1.58M |
| Known actors | 495 (Fed, Treasury, Congress, HFs, corps) |
| Oracle predictions | 615 (scoring Apr 17) |
| Paper strategies | 12 active |
| API endpoints | 176+ in api.js, 82 in intelligence router alone |

**Running services:** API :8000, llama.cpp :8080 (Qwen 32B, ctx 8192), Crucix :3117, Hermes (PID 270283), PostgreSQL
**Intelligence loop:** Hourly briefings, 4h capital flows + 100x digest, 6h Oracle cycle, nightly research + taxonomy audit

---

## Completed Phases (1-17)

<details>
<summary>Phase 1-12.5: Core Platform (ALL COMPLETE)</summary>

1. 24/7 LLM Intelligence Loop
2. Data Source Resilience (multi-source fallback)
3. Hypothesis Engine (18 candidates, 8 passed, promote-to-feature)
4. UX Narrative (interpret.js, z-scores, no raw numbers)
5. Navigation & Polish (bottom nav, unified cards)
6. Physics-Driven Intelligence (GEX, momentum, force scores)
7. Data Gap Closure (FRED, CoinGecko, taxonomy fixes)
8. Backtest Pipeline (874 winners, auto TACTICAL hypotheses)
9. Taxonomy Guardian (nightly audit, auto-fix)
10. Social Sentiment (Reddit 12 subs, Bluesky, Google Trends)
11. Paper Trading Bot (signal executor, Kelly sizing, auto-kill, circuit breaker)
12. Exchange Integrations (Hyperliquid, Polymarket CLOB, Kalshi, wallet manager)
12.5. Sentiment Data Pipeline (AlphaVantage, HF 57M rows, Pushshift, FinBERT)
</details>

<details>
<summary>Phase 13: AstroGrid (IN PROGRESS — Codex branch)</summary>

- App scaffold done on `codex/astrogrid-dedup`
- Ephemeris pullers live: lunar, planetary, vedic, chinese calendar (all in Hermes scheduler)
- Remaining: 3D orrery, market-astro correlation engine, celestial narrative
- Owner: Codex agent (parallel workstream)
</details>

<details>
<summary>Phase 14: Oracle & Deep Intelligence (COMPLETE)</summary>

- Oracle engine: 5 competing models, signal/anti-signal, weight evolution, calibration (Brier/ECE)
- 100x digest: 3-layer supervised filter
- Intelligence layer (14 modules, 22,354 lines):
  - trust_scorer.py (1,100 lines) — Bayesian with recency decay, convergence detection
  - lever_pullers.py (1,376 lines) — 5 categories (Fed, Congress, insider, institutional, dealer)
  - actor_network.py (7,002 lines) — 495 actors, wealth migration, pocket-lining detection
  - cross_reference.py (1,435 lines) — lie detector (GDP vs electricity, trade bilateral, inflation vs inputs)
  - source_audit.py (939 lines) — pairwise source comparison, auto-promote
  - postmortem.py (1,344 lines) — failure categorization, lessons learned
  - sleuth.py (1,228 lines) — investigative leads, rabbit holes, daily investigations
  - thesis_tracker.py (961 lines) — thesis versioning, scoring vs SPY, postmortems
  - dollar_flows.py (1,081 lines) — 7+ signal types normalized to USD
  - event_sequence.py (998 lines) — chronological timeline from 6 data sources
  - forensics.py (927 lines) — reconstructs what preceded price moves
  - causation.py (2,387 lines) — traces actions to contracts, legislation, insider knowledge
  - flow_thesis.py (804 lines) — 10+ capital flow theses with mechanisms
  - flow_aggregator.py (772 lines) — sector/time/actor-tier aggregation
- Options edge: recommender (1,200+ lines), tracker (700+ lines)
- 9+ altdata modules wired
- Bulk data: CBOE 35yr, Binance 5yr, DeFiLlama, Open-Meteo, EIA
</details>

<details>
<summary>Phase 15: Hermes Email + Sentiment Expansion (COMPLETE)</summary>

- hermes@stepdad.finance: IMAP poll, allowlist, LLM triage, event triggers
- Fear & Greed, Alpha Vantage sentiment, pytrends, Reddit/Pushshift, GDELT GKG
- Prediction markets: Polymarket rapid-shift + Kalshi macro events
- Gov contracts (USASpending API), Legislation (Congress.gov API)
</details>

<details>
<summary>Phase 16: Living Graphs & UX Polish (COMPLETE)</summary>

- Flows narrative, TimeframeComparison, Hypothesis results UI, Capital flow granularity
- LivingGraph renderers: PhaseSpace, Orbital, ForceNetwork, ParticleSystem, RiverFlow, Ridgeline, Chord
- DerivativesGrid SPA, VizDashboard, llama.cpp ctx 8192, prompt optimizer
</details>

<details>
<summary>Phase 17: Palantir Core — HOW MUCH / WHEN / WHY (COMPLETE — code exists)</summary>

All MASTER-PLAN items are built:
- **HOWMUCH-1**: dollar_flows.py — 7 source types normalized to USD ✓
- **HOWMUCH-2**: flow_aggregator.py — sector/time/actor-tier aggregation ✓
- **HOWMUCH-3**: MoneyFlow.jsx — D3 Sankey/Bubble with drill-down, time slider ✓
- **WHEN-1**: event_sequence.py — chronological timeline from 6 sources ✓
- **WHEN-2**: Pattern detection in event_sequence.py (`find_recurring_patterns()`) ✓
- **WHEN-3**: Timeline.jsx — 1,129 lines, horizontal timeline, event markers, price overlay ✓
- **WHEN-4**: forensics.py — backward-looking "what preceded this move?" ✓
- **WHY-1**: causation.py — 2,387 lines, traces actions to causes ✓
- **WHY-2**: gov_contracts.py — USASpending API ✓
- **WHY-3**: legislation.py — Congress.gov API ✓
- **WHY-4**: Causal narrative via LLM (causation.py `generate_causal_narrative()`) ✓
- **WHY-5**: WhyView.jsx — 1,122 lines, forensic reconstruction + causation trees ✓

**Status: Code complete but NOT fully wired** — see "Wiring Gaps" below.
</details>

<details>
<summary>Phase 17.5: Audit & Hardening (COMPLETE — reports generated)</summary>

- 10-agent audit run: architecture, security, database, performance, Python, refactoring, infra, docs
- 16 audit reports in docs/audits/
- MCP export endpoints (450 lines + 589 lines tests)
- SQL guard hook
- Project-level Claude Code config (14 agents, 8 commands, 15 rules, 8 skills)
</details>

---

## WIRING GAPS (code exists but isn't connected)

### In Progress (agents working now — 2026-03-31)

| Gap | Agent | Status |
|-----|-------|--------|
| Oracle API in frontend (Predictions.jsx broken) | build-error-resolver | Wiring api.js |
| IntelDashboard APIs (trust scores, convergence) | build-error-resolver | Wiring api.js |
| 13 high-priority dormant pullers | python-reviewer | Registering in Hermes |
| Missing DB indexes (PIT, validation, feature_registry) | database-reviewer | Adding to schema.sql |

### Completed (2026-03-31 session)
- [x] All wiring gaps closed (Oracle API, IntelDashboard, all 48 pullers, options recommender/tracker)
- [x] Signal Connectivity Architecture built — 12 new modules:
  - signal_registry.py, 6 adapters, signal_aggregator.py, model_factory.py, model_evolver.py, ensemble.py
  - API: 7 endpoints (signal_registry.py router), frontend: 7 api.js methods
  - Hermes: 2h signal refresh, model migration on startup
  - DB: signal_registry table + indexes live, oracle_models extended

### Remaining Dormant Pullers (lower priority — register after high-priority batch)

discord_scanner, earnings_calendar, export_controls, fara, foia_cables,
hf_financial_news, lobbying, news_scraper, noaa_ais, offshore_leaks,
opencorporates, opportunity, repo_market, social_attention,
uk_companies_house, world_news, yield_curve_full

---

## SECURITY

### Fixed (2026-03-31)
| Issue | Fix | File |
|-------|-----|------|
| ~~JWT default secret~~ | Crash on startup if weak secret in non-dev | api/auth.py |
| ~~WebSocket token leak~~ | First-message auth (5s timeout, no query params) | api/main.py |
| ~~DB default password~~ | Empty default + crash if unset in non-dev | config.py |

### Remaining
| Issue | Severity | File | Description |
|-------|----------|------|-------------|
| Rate limit resets | HIGH | api/auth.py:43 | File-based /tmp, lost on restart |
| CORS misconfiguration | HIGH | api/main.py:392 | `allow_credentials=True` with dev origins |
| Incomplete key validation | MEDIUM | config.py:208 | Only FRED key validated at startup |

### Not An Issue (stop re-flagging)
| Item | Why It's Fine |
|------|--------------|
| SQL injection in regime.py | Audit confirmed: already uses parameterized `text()` + `:days` binding. Line numbers in critique were wrong. |

---

## Open Bugs

### In Progress (agents working now — 2026-03-31)
| ID | Severity | Description | Agent |
|----|----------|-------------|-------|
| BUG-03 | **CRITICAL** | Oracle silent skip on missing price | python-reviewer fixing |
| BUG-04 | HIGH | Paper trading threshold not stored per trade | python-reviewer fixing |

---

## Performance Issues (from audit)

| Issue | File | Impact |
|-------|------|--------|
| Connection pool too small (10+20) | db.py:43 | Deadlocks at 30+ concurrent |
| Feature matrix unbounded loading | store/pit.py:133 | OOM on large date ranges |
| O(n²) clustering | discovery/clustering.py:316 | 60-180s for 10K+ obs |
| N+1 query in models | api/routers/models.py:91 | 10-20ms → should be 1-3ms |
| ~~Missing DB indexes~~ | schema.sql | **IN PROGRESS** — database-reviewer agent adding |
| No connection pool monitoring | api/main.py | Can't detect exhaustion |

---

## Technical Debt

| Item | Priority | Notes |
|------|----------|-------|
| 8 critical modules with zero tests | HIGH | resolver.py, gates.py, inference.py, features/lab.py |
| Single route registry | MEDIUM | Routes in 3 places |
| Lazy loading | MEDIUM | Only 4/51 views |
| God objects | MEDIUM | actor_network.py 7K lines, routers 3-3.8K each |
| NaN handling inconsistent | MEDIUM | ffill(5) vs ffill().dropna() across modules |
| Time formatting | LOW | Mixed locale methods |
| Markdown escaping in alerts | LOW | Could break Telegram |

---

## Data Gaps

| Family | Coverage | Action |
|--------|----------|--------|
| systemic | 0% | Re-pull OFR data, run resolver |
| alternative | 6% | Register dormant pullers in Hermes |
| trade | 33% | Comtrade resolver mapping |
| rates | 60% | entity_map entries |
| macro | 67% | Run resolver with new mappings |
| international | Low | Wire intl pullers |

124 features with zero resolved rows. Hermes retries, but resolver mappings need manual fix (wn_*, FRED date parse, analyst int64).

---

## 4-WEEK TACTICAL PLAN (revised post-audit)

### Week 1 (Mar 31 - Apr 6): Security + Wiring

**Goal:** Fix critical security holes. Wire the dormant 50% of the system.

**Security (day 1):**
- [x] Fix JWT secret: crash on startup if weak in production
- [x] Fix WebSocket auth: first-message pattern, no query params
- [x] Remove DB default password from config.py
- [x] ~~SQL injection in regime.py~~ — already parameterized, not an issue

**Wiring:**
- [x] Wire Oracle API endpoints into api.js (Predictions.jsx unblocked)
- [x] Wire getTrustScores + getConvergenceAlerts into api.js (IntelDashboard unblocked)
- [x] Register all 48 pullers in Hermes (both batches done)
- [x] Wire options_recommender into scheduler (daily 07:00) + API endpoint
- [x] Wire options_tracker into scheduler (weekly) + API endpoint

**Bugs:**
- [x] FIX BUG-03 — Oracle no_data verdict (Verdict.NO_DATA enum + placeholder per model)
- [x] FIX BUG-04 — Freeze threshold per trade (threshold_used column + auto-migration)

**Infra:**
- [x] Add 3 missing DB indexes (+ migration script)
- [x] Fix connection pool (20+40 + 80% utilization warning)
- [x] Fix PIT date range cap (10yr, configurable)
- [x] Sharpen all 11 ECC agents + CLAUDE.md with GRID context
- [ ] Run DB index migration on live database
- [ ] Restart services with latest code
- [ ] Verify all wiring changes work end-to-end

### Week 2 (Apr 7 - Apr 13): Data + Testing

**Goal:** Fill data gaps. Shore up test coverage on critical paths.

- [x] Fix resolver mappings (wn_* 33 SEED_MAPPINGS, FRED date try-catch, analyst float() in base.py)
- [x] Fix connection pool (20+40 + 80% monitoring) — done in Week 1
- [x] Fix feature matrix unbounded loading (10yr cap) — done in Week 1
- [x] Register remaining dormant pullers — done in Week 1 (all 48 active)
- [x] Fix CORS for production (strict allowlist)
- [x] Fix rate limiting (DB-backed with shelve fallback)
- [x] Fix N+1 query in models.py (LEFT JOIN + pagination metadata)
- [x] Run full ingestion cycle — 1.58M resolved rows (up from 450K), 1,194/1,238 features with data
- [x] Verify pullers: 46 active sources (54 deactivated — 9 duplicate groups merged, 41 noise entries removed)
- [x] Tests for resolver/gates/inference: 128 passed, 2 skipped (2,681 lines of tests)
- [x] Frontend smoke test: all views build clean (9.8s), API serving, signals registry 200

### Week 3 (Apr 14 - Apr 20): Oracle Scoring + Evidence

**Goal:** Apr 17 is scoring day. Prove the system works.

- [ ] **Apr 5+: Score Oracle predictions** (10,893 pending, earliest expiry Apr 5 — pipeline verified)
- [ ] Run calibration report (Brier score, ECE) — calibration.py verified working
- [ ] Post-mortem: weight adjustment blocked until scoring (all 5 models at weight=1.0)
- [x] Score thesis accuracy — lookback widened 7→90 days, 1 scored (partial), remaining <3 days old
- [x] Trust scorer baselines — fixed 3 bugs (NULL outcomes, JSONB extraction, COALESCE type), 20 signals scored, 15 sources updated
- [x] Paper trading P&L review — 8 OPEN trades, combined unrealized P&L: -$194.31
- [x] Forensic reports — threshold lowered 3%→1.5%, 5 reports generated (SPY 2, QQQ 2, AAPL 1)
- [x] Pattern library — 54 recurring patterns found (whale:bearish clustering dominant across 11 tickers)

### Week 4 (Apr 21 - Apr 27): Polish + Performance + Visual Overhaul

**Goal:** The system is proven. Make it fast, clean, and look like it belongs.

- [x] Lazy load remaining 47 views (React.lazy) — done
- [x] Fix N+1 queries + pagination metadata — done
- [x] Fix CORS configuration for production — done
- [x] Persistent rate limiting (DB-backed) — done
- [x] Consolidate route registry (routes.js → App/NavBar/CommandPalette) — done
- [x] NaN handling standardized (ffill limit=5, orthogonality guard fix, importance fix) — done
- [x] God object extraction: actor_network.py split into 6 modules (intelligence/actors/), routers split (watchlist, astrogrid, intelligence facades)
- [x] Standardize time formatting — 13 files updated to use Intl.DateTimeFormat utilities
- [x] Fix features/lab.py zscore bug — window capping + zero-std handling

**Visual overhaul (NEW — user hates current look):**
- [ ] Branding refresh: color palette, typography, logo/wordmark
- [ ] Layout redesign: spacing, hierarchy, card styles, information density
- [ ] Dark mode polish (if exists) or implement proper dark theme
- [ ] Consistent component library: buttons, inputs, badges, status indicators
- [ ] Mobile responsiveness audit — touch targets, bottom nav, collapsible sections
- [ ] Data visualization consistency: chart colors, axis styles, tooltip format across all D3 views

---

## 4-QUARTER STRATEGIC PLAN

### Q2 2026 (Apr - Jun): The Evidence Quarter

**Theme:** Prove GRID works. The code is built — now prove it generates alpha.

**Milestones:**
- Oracle v1 scored and calibrated (Apr 17+, weekly thereafter)
- Paper trading P&L after 60 days — which strategies survive?
- Trust scorer baselines — which sources are reliable over time?
- All 48 pullers active and producing data
- All 51 views fully wired to real data (no placeholders)
- Thesis accuracy: target 60%+ directional
- Data gap closure: all families >50% coverage
- AstroGrid v1 merged from Codex branch
- Security audit findings all resolved
- Exchange credentials set up (Hyperliquid testnet first)

**Key decisions:**
- Which Oracle models to keep vs kill?
- Paper → live trading: ready or not?
- Qwen 32B sufficient or upgrade to 72B?

### Q3 2026 (Jul - Sep): The Refinement Quarter

**Theme:** Self-improving systems. The flywheel accelerates.

**Milestones:**
- Oracle v2: regime-conditional weights (different models for GROWTH vs CRISIS)
- Adaptive thesis weights from accuracy tracking (monthly auto-rebalance)
- Trust scorer v2: Kelly-weighted sizing, network effects
- Sleuth v2: auto-follow-up on leads, investigation reports connecting multiple leads
- Cross-reference v2: company-level (earnings vs capex vs hiring), analyst vs insider divergence
- Trade recommender v2: trust convergence layer, earnings proximity check
- Pattern library: top 50 predictive sequences catalogued
- Auto-forensic reports for every >3% daily move
- Options tracker self-improving scanner weights
- Performance: Redis caching, query optimization, connection pool monitoring
- International data fully wired

**Key decisions:**
- Live trading with real capital? Size and scope.
- Multi-user demand assessment
- GPU scaling plan

### Q4 2026 (Oct - Dec): The Platform Quarter

**Theme:** From tool to platform. GRID becomes productizable.

**Milestones:**
- API documentation + rate limiting + key management
- Multi-tenant isolation (if multi-user)
- Pricing tiers + beta program (5-10 users)
- Mobile native wrapper (Capacitor)
- Crucix iOS
- Public track record page (Oracle scorecard, thesis accuracy, strategy returns)
- Compliance review for external users
- 1-year of scored predictions accumulating

**Key decisions:**
- Solo tool vs SaaS?
- Open source components?
- Partnership / licensing?

### Q1 2027 (Jan - Mar): The Scale Quarter

**Theme:** Growth — more users, more markets, or more intelligence.

**Milestones:**
- If SaaS: onboarding, billing, user management
- If solo: deeper markets (commodities, forex, fixed income, private markets)
- Causal inference v2: LLM fine-tuned on forensic reports
- Real-time streaming: WebSocket for all intelligence layers
- Autonomous research: Hermes generates + tests hypotheses unprompted
- "Lie detector" public dashboard
- GRID SDK for custom views/agents
- 1-year Oracle track record → statistical significance

---

## The Palantir Test

GRID earns the label when it answers all seven:

1. **"Where is money flowing?"** → Dollar-quantified flow maps ✅ (dollar_flows.py + MoneyFlow.jsx)
2. **"Who moved it?"** → 495 named actors with trust scores ✅ (actor_network.py + ActorNetwork.jsx)
3. **"How much?"** → USD estimates with confidence bands ✅ (dollar_flows.py normalizer)
4. **"When did they act?"** → Forensic timeline ✅ (event_sequence.py + Timeline.jsx)
5. **"Why did they act?"** → Causal connections ✅ (causation.py + WhyView.jsx)
6. **"What happened next?"** → Pattern matching ✅ (event_sequence.py `find_recurring_patterns()`)
7. **"What will happen now?"** → Scored thesis ✅ (Oracle + thesis_tracker.py)

**All 7 are code-complete.** The gap is operational: wiring, data flow, and evidence accumulation. Q2's job is proving they work in practice, not building more.

---

## Accepted / Known — STOP RE-FLAGGING

These items have been investigated and are either by design, already fixed, or not worth fixing now. Do NOT add them back to open issues.

| Item | Why It's Fine |
|------|--------------|
| SQL injection in regime.py | Already parameterized (`text()` + `:days`). Audit line numbers were wrong. Verified 2026-03-31. |
| "Two hypothesis registries" (BUG-05) | Fixed. DuckDB documented as read-only mirror in ARCHITECTURE.md. Single Postgres registry is authoritative. |
| Operator view not wired (BUG-06) | Fixed. Operator.jsx fully wired to /api/v1/system/health + freshness + Hermes status. |
| Zero features monitoring (BUG-07) | Fixed. Hermes has retry logic with cooldown + strategy variation (3 attempts, 6h cooldown after 5 fails). |
| fed_liquidity FRED API key | Fixed. Proper config inheritance via config.py + .env. Key validated at startup. |
| Alert state persistence (audit item) | Fixed. PostgreSQL-backed via push_subscriptions + notification_preferences tables. |
| "ActorNetwork view building" | Done. 1,673 lines, D3 force graph with animated particles, fully functional. |
| "IntelDashboard building" | UI done (80+ lines renders). API wiring in progress (agent working 2026-03-31). |
| "TrendTracker building" | Done. 1,095 lines, 6 trend categories, fully functional. |
| WhyView / Timeline "planned" | Done. WhyView 1,122 lines, Timeline 1,129 lines, both working with all APIs wired. |
| MASTER-PLAN Wave 1-3 (HOWMUCH/WHEN/WHY) | All code-complete. dollar_flows, event_sequence, forensics, causation, gov_contracts, legislation, flow_aggregator — all exist and are functional. |
| Options recommender "built but never called" | Known. Will wire in Week 1. Not a bug — was intentionally deferred until trading pipeline matures. |
| Pushshift Reddit "stub" | By design. It processes local .zst archive dumps, not live API. No pull_* methods needed — it's a batch ingest tool. |
| Exchange creds not set (Hyperliquid, Polymarket, Kalshi) | Known. Architecturally complete, waiting for live trading decision in Q2. Not a bug. |

---

## Session Pickup Protocol

Every new session:
1. Read this file first
2. `git log --oneline -10` for recent changes
3. Check Security / Bugs / Wiring Gaps tables — anything new?
4. Pick up from the 4-week plan
5. **Update this file before ending the session**
