# GRID — Claude Code Guidelines

## Project Overview

GRID is a systematic, multi-agent trading intelligence platform. It ingests macroeconomic/market data from 48 data pullers (all registered in [[Hermes Scheduler|Hermes scheduler]]), resolves multi-source conflicts using [[PIT Store|point-in-time]] (PIT) correct methodology, performs unsupervised [[Regime Discovery|regime discovery]], and runs [[Walk-Forward Backtesting|walk-forward backtesting]] with an immutable [[Decision Journal|decision journal]].

**See `docs/planning/ROADMAP.md` for the full 4-week tactical plan and 4-quarter strategic plan.**

## Quick Orientation

A **SessionStart hook** auto-injects live server state + codebase index into every conversation. If you need to re-orient mid-session, read `.claude/CODEBASE_INDEX.md` — it has the module function index, DB schema, server ops, and integration map. Run `/grid-orient` to rebuild the index after major changes.

## Server Deployment

- Repo location on server: `~/grid_v4` (user: `grid`, host: `grid-svr`)
- **Systemd services** for all components (grid-api, grid-llamacpp, grid-crucix, grid-hermes, grid-coordinator, grid-worker, cloudflared)
- Restart all: `sudo systemctl restart grid-api grid-llamacpp grid-crucix grid-hermes`
- **Public URL**: `https://grid.stepdad.finance` (Cloudflare Tunnel, no port forwarding)
- **Role-based auth**: admin (master password) and contributor (user accounts)
- See `docs/SERVER-SERVICES.md` for full service reference

## Tech Stack

- **Backend:** Python 3.11+, [[FastAPI]], [[SQLAlchemy]] 2.0, [[PostgreSQL]] 15 + [[TimescaleDB]]
- **Frontend:** React 18, Vite, [[Zustand]], served as PWA from [[FastAPI]]
- **LLM:** Dual local inference — Nemotron-Cascade-2 30B GPU (:8080) + Nemotron-3-Super-120B CPU (:8081). OpenRouter Claude fallback. See `llm/router.py` for 3-tier taxonomy (LOCAL/REASON/ORACLE).
- **Config:** pydantic-settings, environment variables via `.env`

## Essential Commands

```bash
# Database
cd grid && docker compose up -d                    # Start PostgreSQL + TimescaleDB

# Backend
cd grid && pip install -r requirements.txt
cd grid && python -m uvicorn api.main:app --reload --port 8000

# Frontend
cd grid/pwa && npm install && npm run dev          # Dev server on :5173
cd grid/pwa && npm run build                       # Production build

# Tests
cd grid && python -m pytest tests/ -v              # Full suite: 1,148 tests across 76 files
cd grid && python -m pytest tests/test_pit.py -v   # PIT store tests
cd grid && python -m pytest tests/test_api.py -v   # API tests
```

## Architecture Rules

<important if="modifying any data query, [[Feature Engineering|feature engineering]], or inference code">
**PIT ([[PIT Store|Point-in-Time]]) Correctness is non-negotiable.** Every data query MUST use `store/pit.py` to prevent [[PIT Store|lookahead bias]]. Never access future data relative to the decision timestamp. The `assert_no_lookahead()` guard must pass for all inference paths.
</important>

<important if="writing SQL or database queries">
**Never use string `.format()` or f-strings for SQL.** Always use parameterized queries via [[SQLAlchemy]].
</important>

<important if="adding or modifying data sources">
**Multi-source [[Conflict Resolution|conflict resolution]]** goes through `normalization/resolver.py`. Every new data source needs: an ingestion module, [[Entity Map|entity map]]ping in `entity_map.py`, and PIT-compatible timestamps. Use the scheduler pattern from `ingestion/scheduler.py`.
</important>

<important if="modifying journal or decision logging code">
**[[Decision Journal|Immutable Journal]]** — entries in `journal/log.py` must never be updated or deleted. Every recommendation gets logged with full provenance. Validate confidence/probability are 0-1 and not NaN/infinity.
</important>

## Key Patterns

- **[[Model Governance]]:** CANDIDATE → SHADOW → STAGING → PRODUCTION (see `governance/registry.py`)
- **Graceful Degradation:** [[Hyperspace]]/[[Ollama]] calls return `None` if offline; system operates without them
- **Config:** All settings via `config.py` (pydantic-settings). Copy `.env.example` to `.env`
- **Logging:** `loguru` imported as `log` from config — use throughout

## Gotchas

- `DISTINCT ON` in `store/pit.py` is [[PostgreSQL]]-specific — SQLite/MySQL will not work
- `assert_no_lookahead()` raises ValueError but does NOT roll back the transaction ([[ATTENTION]].md #8)
- `_resolve_source_id()` auto-creates [[Source Catalog Table|source_catalog]] entries — unknown sources can appear silently (#25)
- `pd.to_numeric(errors="coerce")` in ingestion silently converts bad data to NaN (#13)
- NaN handling varies across modules (ffill limits, dropna timing) — follow the existing module's pattern (#14)
- Two scheduler files exist (`scheduler.py`, `scheduler_v2.py`) — `scheduler.py` is authoritative (#39)

## Intelligence Layer (14 modules, 14,402 lines)

The intelligence layer tracks who moves markets and why:

- `intelligence/trust_scorer.py` (1,100 lines) — [[Trust Scorer|Bayesian trust]] scoring with recency decay for all signal sources
- `intelligence/lever_pullers.py` (1,376 lines) — identifies and tracks market-moving actors across 5 categories
- `intelligence/actor_network.py` (7,002 lines) — 495 named actors with wealth flow tracking (US deep map: pensions, lobbying, donors, defense, Fed, REITs, media)
- `intelligence/cross_reference.py` (1,435 lines) — government stats vs physical reality ("[[Cross Reference|lie detector]]")
- `intelligence/source_audit.py` (939 lines) — source accuracy comparison + redundancy mapping via pairwise comparison
- `intelligence/postmortem.py` (1,344 lines) — automated failure analysis for bad trades
- `intelligence/sleuth.py` (1,228 lines) — investigative leads and signal pattern discovery
- `intelligence/thesis_tracker.py` (961 lines) — thesis versioning + scoring engine
- `intelligence/dollar_flows.py` (1,081 lines) — USD normalization and capital flow quantification
- `intelligence/event_sequence.py` (998 lines) — chronological timeline reconstruction
- `intelligence/forensics.py` (927 lines) — price move reconstruction from actor signals
- `intelligence/causation.py` (2,387 lines) — traces market actions back to root actor causes
- `intelligence/flow_thesis.py` (804 lines) — 10+ capital flow theses and rotation patterns
- `intelligence/flow_aggregator.py` (772 lines) — sector/time-slice aggregation engine

### Signal Source Types (trust_scorer evaluation windows)
- `congressional` (30d), `insider` (14d), `darkpool` (5d), `social` (5d), `scanner` (7d)
- `foreign_lobbying` (45d) — [[FARA]]-registered foreign agents influencing US policy
- `geopolitical` (7d) — [[GDELT]] tension spikes between country pairs
- `diplomatic_cable` (30d) — declassified [[FOIA]] cables revealing hidden motivations
- `lobbying` (30d) — domestic lobbying disclosure (Senate LDA + OpenSecrets)
- `campaign_finance` (60d) — PAC contributions mapped to policy outcomes
- `offshore_leak` (14d) — ICIJ Panama/Pandora Papers exposure

### Key Principles
- Every data point has a confidence label: confirmed/derived/estimated/rumored/inferred
- Trust scores use Bayesian updating with 90-day recency half-life

### Prediction Causation Standard (SOP)

Every prediction MUST separate **levers** (causes) from **conditions** (amplifiers):

**Levers** = specific actions by identifiable actors that open/close liquidity valves:
- "Fed raised rates 25bp" → credit valve closes → risk assets reprice
- "Tether minted $1B USDT" → crypto liquidity valve opens → BTC bid
- "Whale moved 10K BTC to Binance" → sell-side valve opening → price pressure
- "SEC approved spot ETH ETF" → institutional flow valve opens → ETH bid

**Conditions** = environmental features that amplify or dampen lever effects:
- Weekend low volume → amplifies any move (NOT a cause)
- Options expiry week → pins or accelerates (NOT a cause)
- High funding rates → enables a squeeze (NOT a cause)
- Q-end rebalancing window → creates flow (NOT a cause)

**The rule:** If you cannot name the valve, the flow direction, and the actor pulling it, do not generate the prediction. Conditions alone produce 50/50 noise.

**Required prediction structure:**
```
LEVER:     [Who] did [what] affecting [which liquidity valve]
CONDITION: [Environmental factor] that amplifies/dampens the lever
THESIS:    Lever + condition → expected [direction] [magnitude] [timeframe]
INVALIDATION: [Specific condition] that proves the lever thesis wrong
```

**Wrong:** "BTC bearish because weekend low volume"
**Right:** "Whale X moved Y BTC to Binance (lever) in thin weekend book (condition) → expect 5-8% drawdown within 12h. Invalidated if BTC reclaims $71K."
- Post-mortems are mandatory for every failed trade
- Source accuracy auto-updates resolver priorities

## Options Edge

- `trading/options_recommender.py` — generates specific trade recommendations (strike, expiry, entry, target, stop, Kelly)
- `trading/options_tracker.py` — outcome tracking + self-improving scanner weights
- `discovery/options_scanner.py` — 7-signal [[Options Scanner|mispricing detector]] (now with LLM sanity check)
- `physics/dealer_gamma.py` — [[Dealer Gamma|GEX]], [[Dealer Gamma|vanna]], charm, [[Dealer Gamma|gamma walls]]

## Oracle Engine

- `oracle/engine.py` — 5 competing models, signal/anti-signal weighting, dynamic weight evolution
- `oracle/calibration.py` — [[Oracle Calibration|Brier score]], expected calibration error (ECE), reliability metrics
- `oracle/report.py` — email digest sent after each prediction cycle
- **615 predictions locked, scoring begins Apr 17 2026**
- Runs every 6 hours via [[Hermes Scheduler|Hermes operator]]

## Data Sources (expanded)

New ingestion modules (all 48 pullers registered in `hermes_operator.py` scheduler):
- `ingestion/altdata/congressional.py` — [[Congressional Trading|congressional trading]] disclosures
- `ingestion/altdata/insider_filings.py` — [[Insider Filings|SEC Form 4]] with cluster buy detection
- `ingestion/altdata/dark_pool.py` — [[Dark Pool|FINRA dark pool]] weekly data
- `ingestion/altdata/unusual_whales.py` — whale options flow detection
- `ingestion/altdata/prediction_odds.py` — [[Polymarket]] rapid probability shifts
- `ingestion/altdata/smart_money.py` — Reddit + Finviz trust-scored social signals
- `ingestion/altdata/supply_chain.py` — shipping rates, container index, ISM
- `ingestion/altdata/fed_liquidity.py` — Fed net liquidity equation
- `ingestion/altdata/institutional_flows.py` — [[Institutional Flows|ETF flows]] + SEC [[Institutional Flows|13F]] holdings
- `ingestion/altdata/fara.py` — DOJ [[FARA]] foreign agent lobbying (who foreign governments pay to influence US policy)
- `ingestion/altdata/foia_cables.py` — State Dept + NSA Archive declassified diplomatic cables
- `ingestion/altdata/gdelt.py` — enhanced with actor-level tone, country-pair tension scoring, geopolitical event signals

## Frontend Views (51 total views, 45 routes)

- [[MoneyFlow View|MoneyFlow]] — global money flow D3 visualization (Central Banks → Markets → Sectors)
- [[Cross Reference View|CrossReference]] — government stats vs physical reality [[Cross Reference|lie detector]]
- Predictions — oracle scoreboard + calibration chart
- [[Actor Network View|ActorNetwork]] — D3 force graph of financial power structure
- [[Intel Dashboard View|IntelDashboard]] — unified intelligence command center
- [[TrendTracker View|TrendTracker]] — momentum, regime, rotation, vol, liquidity trends
- Timeline.jsx (1,129 lines) — forensic event timeline reconstruction
- WhyView.jsx (1,122 lines) — "why did this move?" [[Causation|causation]] reconstruction

## Code Style

- Type hints on all new functions
- Follow existing patterns in each module — don't introduce new frameworks
- Keep API routes thin; business logic belongs in domain modules
- Every new module needs a test file in `grid/tests/`

## Workflow Best Practices

- Start complex tasks in **plan mode** before execution
- Use subagents for independent subtasks (parallel investigation, code review)
- Perform `/compact` at ~50% context usage on long sessions
- Break work into phases — verify each phase works before moving to the next
- After fixing a bug, confirm the fix with a test — don't just eyeball it
- Reference `grid/ATTENTION.md` for the full 64-item audit when fixing issues

## Directory Structure

```
grid/
├── api/           # FastAPI routes, auth, middleware (14 routers)
├── alerts/        # Email alerting system (failure, regime, 100x, digest)
├── ingestion/     # 50+ data source pullers (FRED, BLS, ECB, altdata, etc.)
├── normalization/ # Multi-source conflict resolution
├── store/         # PIT-correct query engine (PostgreSQL DISTINCT ON)
├── features/      # Feature engineering + importance tracking
├── discovery/     # Unsupervised regime clustering + options scanner
├── validation/    # Walk-forward backtesting gates
├── inference/     # Live model scoring
├── journal/       # Immutable decision log
├── governance/    # Model lifecycle state machine
├── intelligence/  # Trust scoring, actor network, cross-reference, postmortem
├── trading/       # Options recommender, tracker, signal executor, exchanges
├── agents/        # TradingAgents multi-agent framework
├── hyperspace/    # Local LLM inference layer (P2P)
├── ollama/        # Ollama integration + market briefings
├── llamacpp/      # llama.cpp direct inference
├── outputs/       # LLM insight logging + scanner
├── server_log/    # Git-backed error logging + operator inbox
├── pwa/           # React 18 PWA frontend (Zustand, Vite)
├── docs/          # Architecture, API, deployment, development guides
├── tests/         # pytest suite (1,148 tests across 76 files)
└── scripts/       # Migration and utility scripts
```

## Trial Gem Hunter (Clinical Trial Signal Domain)

Orthogonal signal domain: ClinicalTrials.gov Phase 2/3 → biotech equity prediction.

- `grid/signals/trial_signal.py` — main signal class (score, regime gate, position sizing)
- `grid/ingestors/trial_ingestor.py` — daily CT.gov ingestor (cron job #9, 6am)
- `grid/scripts/migrations/add_trial_signals.sql` — DB schema (trial_signals, trial_cache, catalyst_calendar)
- `tasks/trial-gem-hunter/` — AutoAgent self-improvement harness

### Signal Logic
1. Fetch Phase 2/3 trials (ACTIVE_NOT_RECRUITING, readout 30-180d, industry sponsor, mcap < $2B)
2. Score: endpoint clarity x phase x disease priority x enrollment x FDA flags
3. Regime gate: BUY only in GROWTH/NEUTRAL, WATCHLIST in FRAGILE/CRISIS
4. Position sizing: Kelly-inspired, max 5% per trial bet

### DB Tables
- `trial_signals` — scored picks
- `trial_cache` — raw CT.gov JSON (24h TTL)
- `catalyst_calendar` — upcoming readout dates
- Views: `trial_gems`, `trial_signal_performance`, `upcoming_catalysts`
