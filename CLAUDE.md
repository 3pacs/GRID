# GRID — Claude Code Guidelines

## Project Overview

GRID is a systematic, multi-agent trading intelligence platform. It ingests macroeconomic/market data from 48 data pullers (all registered in [[Hermes Scheduler|Hermes scheduler]]), resolves multi-source conflicts using [[PIT Store|point-in-time]] (PIT) correct methodology, performs unsupervised [[Regime Discovery|regime discovery]], and runs [[Walk-Forward Backtesting|walk-forward backtesting]] with an immutable [[Decision Journal|decision journal]].

**See `docs/planning/ROADMAP.md` for the full 4-week tactical plan and 4-quarter strategic plan.**

## Quick Orientation

A **SessionStart hook** auto-injects live server state + codebase index into every conversation. If you need to re-orient mid-session, read `.claude/CODEBASE_INDEX.md` — it has the module function index, DB schema, server ops, and integration map. Run `/grid-orient` to rebuild the index after major changes.

### Before You Build ANYTHING New

> **Assume any capability that sounds obvious already exists somewhere in the 702-module codebase.** CLAUDE.md is an intentionally-curated subset, not a complete inventory. The authoritative full inventory is `docs/MODULE_INVENTORY.md` (702 modules across 30 directories with docstrings, APIs, DB I/O, and import graphs, generated 2026-04-14). `docs/MODULE_CATALOG.md` is the older curated version.

Pre-build checklist:

1. **Read `docs/MODULE_INVENTORY.md`** — authoritative inventory of 649 modules with APIs and import graphs.
2. **Run `/grid-check-exists <keyword>`** — searches intelligence/ + analysis/ + physics/ + features/ + discovery/ + trading/ + oracle/ for similar modules.
3. **Grep for the concept** across the above directories if the keyword search doesn't hit.
4. **Read the top 50 lines** of any match to confirm relevance before deciding to extend or rebuild.
5. **If it exists, the task is almost always "extend and wire," not "build new."**

Known examples of "I almost built it but it already exists" (from the 2026-04-13 session):
- `analysis/vol_surface.py` — SVI parameterization, skew, butterfly checks. Not wired into `discovery/options_scanner.py` or `trading/options_recommender.py`.
- `intelligence/earnings_transcript_analyzer.py` — tone / Q&A split / guidance extraction.
- `intelligence/hypothesis_engine.py` — LLM-driven hypothesis generation with kill criteria.
- `intelligence/prediction_calibration.py` — Brier / reliability tracking (but not persisted, not per-horizon).
- `intelligence/signal_registry.py` + `signal_backlinker.py` + `signal_extractor.py` — signal inventory.
- `physics/dealer_gamma.py` — [[Dealer Gamma|vanna]] and charm are computed at lines 248-250 but never used in scoring.
- **Sector networks** (refactored post-merge): the standalone `banking_network.py` / `energy_network.py` / `pharma_network.py` / `defense_contractors.py` / `tech_monopoly_network.py` / `real_estate_network.py` / `commodities_agriculture_network.py` / `defi_protocols.py` / `media_network.py` / `swf_network.py` modules have been consolidated into `intelligence/sector_networks/*.yaml` loaded by `intelligence/sector_networks/loader.py`. Extend the YAML files, not the deleted Python modules.

Full session orientation: **`docs/planning/SESSION-ROADMAP-2026-04-13.md`**.

## Agent dispatch policy

Every backend agent prompt must include the preamble from `docs/AGENT_PROMPT_TEMPLATE.md`. This enforces grep-before-create discipline and prevents the class of duplication documented in `docs/MODULE_OVERLAP_AUDIT.md`.

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
- `ingestion/scheduler.py` is the authoritative scheduler (the old `scheduler_v2.py` no longer exists; don't recreate it) (#39)

## Conviction Stack (13-layer adjuster chain — 2026-04-14)

Every live prediction runs through `intelligence.signal_provenance.build_provenance_report`, which stacks 13 independent multipliers into `compute_aggregate_conviction`. All 13 are defensive — they wrap their DB calls in try/except and return neutral `1.0` on any failure, so a missing upstream can never break the live path.

| Layer | Module | Range | Scope |
|---|---|---|---|
| disagreement | oracle/engine | [0.60, 1.00] | per-prediction |
| fragility | oracle/engine (Shapley) | [0.50, 1.50] | per-prediction |
| red_team | intelligence/llm_red_team | [0.50, 1.00] | per-prediction |
| fudge_alerts | intelligence/cross_reference | [0.10, 1.00] | per-sector |
| cooccurrence_lift | intelligence/signal_cooccurrence | [0.75, 1.25] | per-signal-pair |
| confidence_bucket | intelligence/confidence_bucket_tracker | [0.60, 1.08] | per-horizon × 0.05-bucket |
| historical_scenario | intelligence/historical_scenario_library | [0.70, 1.10] | per-macro-snapshot |
| null_hypothesis | intelligence/null_hypothesis_forecaster | [0.50, 1.00] | per-horizon global |
| meta_learning_edge | intelligence/meta_learning_matrix | [0.40, 1.50] | per-signal × condition-cube |
| contra_indicator | intelligence/contra_indicator_ensemble | [0.85, 1.15] | global crowd |
| short_squeeze | intelligence/short_squeeze_composite | [0.90, 1.15] | per-ticker |
| prediction_market_arb | intelligence/prediction_market_arbitrage | [0.95, 1.10] | per-ticker × horizon |
| convergence | intelligence/signal_convergence_scanner | [0.92, 1.25] | per-ticker × direction × 7d |

Run `python3 -m scripts.audit_conviction_stack` for the full offline puzzle map (taxonomy, entry points, [[Orthogonality Audit|orthogonality]] hypothesis per layer, redundancy check). Run `python3 -m scripts.call_a_trade` to see a worked TSM example with every adjuster shown in the `adjusters:` ticket line.

**Data state on grid-svr as of 2026-04-14:** 31,793 oracle_predictions · 1,312 scored · 61k signal_sources · 2.2M [[Resolved Series Table|resolved_series]] (1947→2026) · 1,188 eligible features. Calibration tables populating: per_signal_brier=1 (aggregate only — oracle doesn't yet write Shapley contributions), confidence_bucket=3, signal_cooccurrence=410, regime_brier=0 (blocked on oracle enrichment), meta_learning=0 (same block).

**Known gap:** `oracle/engine.py` write path doesn't populate `signals.{regime,fci_regime,vix_level,signal_contributions}` JSONB keys, which blocks the per-signal / per-regime / meta-learning calibrators from learning anything beyond the aggregate. Fix in progress in a gap-fix agent worktree.

## Intelligence Layer (143 modules, ~92,759 lines)

**Authoritative inventory:** [`docs/MODULE_INVENTORY.md`](docs/MODULE_INVENTORY.md) — generated 2026-04-13, catalogs all 649 modules across 30 directories with docstrings, public APIs, DB table I/O, and import graphs. Read this BEFORE creating any new intelligence module to avoid duplication.

The intelligence layer tracks who moves markets and why. The top-level `intelligence/` tree contains 143 modules (the original "14-module scaffold" is historical and should no longer be cited). Below are the most load-bearing ones; see [[MODULE_INVENTORY]].md for the rest and for every `physics/`, `features/`, `discovery/`, `oracle/`, `analysis/`, `inference/` module alongside.

- `intelligence/actor_network.py` (153 LOC façade) — thin re-export shim; the real [[Actor Network|actor network]] now lives in the `intelligence/actors/` subpackage (db, registry, expand, etc.). Do not edit the façade — extend the subpackage.
- `intelligence/actor_discovery.py` (3,533 LOC) — automated actor discovery & enrichment at 250K+ scale (board interlocks, 3-degree expansion, ICIJ import)
- `intelligence/causation.py` (26 LOC re-export shim) — real logic split across `causation_core.py` (194), `causation_graph.py` (1,178), `causation_scoring.py` (1,089). Extend the split modules, not the shim.
- `intelligence/sector_networks/` (YAML-driven) — banking / energy / pharma / defense / tech_monopoly / real_estate / commodities / defi / media / sovereign_wealth actor meshes, loaded by `sector_networks/loader.py`. Replaces the prior standalone `*_network.py` modules.
- `intelligence/global_levers.py` (2,258 LOC) — macro lever identification
- `intelligence/hypothesis_engine.py` (2,137 LOC) — hypothesis generation, scoring, kill
- `intelligence/deep_graph.py` (1,772 LOC) — multi-hop graph traversal engine
- `intelligence/cross_reference.py` (1,435 LOC) — government stats vs physical reality ([[Cross Reference|lie detector]])
- `intelligence/entity_resolver.py` (1,411 LOC) — canonical actor disambiguation
- `intelligence/lever_pullers.py` (1,376 LOC) — identifies market-moving actors across 5 categories
- `intelligence/postmortem.py` (1,344 LOC) — automated failure analysis for bad trades
- `intelligence/trust_scorer.py` (1,100 LOC) — [[Trust Scorer|Bayesian trust]] scoring with recency decay

**Canonical scoring/flow stack** (also referenced in other sections): `trust_scorer`, `dollar_flows`, `flow_aggregator`, `flow_thesis`, `forensics`, `event_sequence`, `thesis_tracker`, `sleuth`, `source_audit`. The original 14-module description in prior CLAUDE.md revisions is now historical — do not use it as the working model; always reconcile against MODULE_INVENTORY.md.

> **Note on location:** `flow_thesis.py` and `flow_aggregator.py` live in `analysis/`, not `intelligence/`. (Location bug fixed from earlier CLAUDE.md revisions.)

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
