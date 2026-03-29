# GRID — Claude Code Guidelines

## Project Overview

GRID is a systematic, multi-agent trading intelligence platform. It ingests macroeconomic/market data from 37+ global sources, resolves multi-source conflicts using point-in-time (PIT) correct methodology, performs unsupervised regime discovery, and runs walk-forward backtesting with an immutable decision journal.

## Server Deployment

- Repo location on server: `~/grid_v4` (user: `grid`, host: `grid-svr`)
- **Systemd services** for all components (grid-api, grid-llamacpp, grid-crucix, grid-hermes, grid-coordinator, grid-worker, cloudflared)
- Restart all: `sudo systemctl restart grid-api grid-llamacpp grid-crucix grid-hermes`
- **Public URL**: `https://grid.stepdad.finance` (Cloudflare Tunnel, no port forwarding)
- **Role-based auth**: admin (master password) and contributor (user accounts)
- See `docs/SERVER-SERVICES.md` for full service reference

## Tech Stack

- **Backend:** Python 3.11+, FastAPI, SQLAlchemy 2.0, PostgreSQL 15 + TimescaleDB
- **Frontend:** React 18, Vite, Zustand, served as PWA from FastAPI
- **LLM:** Hyperspace P2P node + Ollama (local inference), TradingAgents (multi-agent)
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
cd grid && python -m pytest tests/ -v              # Full suite
cd grid && python -m pytest tests/test_pit.py -v   # PIT store tests
cd grid && python -m pytest tests/test_api.py -v   # API tests
```

## Architecture Rules

<important if="modifying any data query, feature engineering, or inference code">
**PIT (Point-in-Time) Correctness is non-negotiable.** Every data query MUST use `store/pit.py` to prevent lookahead bias. Never access future data relative to the decision timestamp. The `assert_no_lookahead()` guard must pass for all inference paths.
</important>

<important if="writing SQL or database queries">
**Never use string `.format()` or f-strings for SQL.** Always use parameterized queries via SQLAlchemy.
</important>

<important if="adding or modifying data sources">
**Multi-source conflict resolution** goes through `normalization/resolver.py`. Every new data source needs: an ingestion module, entity mapping in `entity_map.py`, and PIT-compatible timestamps. Use the scheduler pattern from `ingestion/scheduler.py`.
</important>

<important if="modifying journal or decision logging code">
**Immutable Journal** — entries in `journal/log.py` must never be updated or deleted. Every recommendation gets logged with full provenance. Validate confidence/probability are 0-1 and not NaN/infinity.
</important>

## Key Patterns

- **Model Governance:** CANDIDATE → SHADOW → STAGING → PRODUCTION (see `governance/registry.py`)
- **Graceful Degradation:** Hyperspace/Ollama calls return `None` if offline; system operates without them
- **Config:** All settings via `config.py` (pydantic-settings). Copy `.env.example` to `.env`
- **Logging:** `loguru` imported as `log` from config — use throughout

## Gotchas

- `DISTINCT ON` in `store/pit.py` is PostgreSQL-specific — SQLite/MySQL will not work
- `assert_no_lookahead()` raises ValueError but does NOT roll back the transaction (ATTENTION.md #8)
- `_resolve_source_id()` auto-creates source_catalog entries — unknown sources can appear silently (#25)
- `pd.to_numeric(errors="coerce")` in ingestion silently converts bad data to NaN (#13)
- NaN handling varies across modules (ffill limits, dropna timing) — follow the existing module's pattern (#14)
- Two scheduler files exist (`scheduler.py`, `scheduler_v2.py`) — `scheduler.py` is authoritative (#39)

## Intelligence Layer (NEW)

The intelligence layer tracks who moves markets and why:

- `intelligence/trust_scorer.py` — Bayesian trust scoring for all signal sources
- `intelligence/lever_pullers.py` — identifies and tracks market-moving actors
- `intelligence/actor_network.py` — 475+ named actors with wealth flow tracking (US deep map: pensions, lobbying, donors, defense, Fed, REITs, media)
- `intelligence/actor_discovery.py` — 250K+ scale actor discovery: 3-degree BFS expansion, ICIJ Panama/Pandora Papers bulk import, batch Form 4 / 13F / congressional discovery, board interlocks
- `intelligence/cross_reference.py` — government stats vs physical reality ("lie detector")
- `intelligence/source_audit.py` — source accuracy comparison + redundancy mapping
- `intelligence/postmortem.py` — automated failure analysis for bad trades
- `intelligence/dollar_flows.py` — normalizes all signal sources into estimated USD amounts

### Signal Source Types (trust_scorer evaluation windows)
- `congressional` (30d), `insider` (14d), `darkpool` (5d), `social` (5d), `scanner` (7d)
- `foreign_lobbying` (45d) — FARA-registered foreign agents influencing US policy
- `geopolitical` (7d) — GDELT tension spikes between country pairs
- `diplomatic_cable` (30d) — declassified FOIA cables revealing hidden motivations
- `lobbying` (30d) — domestic lobbying disclosure (Senate LDA + OpenSecrets)
- `campaign_finance` (60d) — PAC contributions mapped to policy outcomes
- `offshore_leak` (14d) — ICIJ Panama/Pandora Papers exposure

### Key Principles
- Every data point has a confidence label: confirmed/derived/estimated/rumored/inferred
- Trust scores use Bayesian updating with 90-day recency half-life
- Post-mortems are mandatory for every failed trade
- Source accuracy auto-updates resolver priorities

## Options Edge

- `trading/options_recommender.py` — generates specific trade recommendations (strike, expiry, entry, target, stop, Kelly)
- `trading/options_tracker.py` — outcome tracking + self-improving scanner weights
- `discovery/options_scanner.py` — 7-signal mispricing detector (now with LLM sanity check)
- `physics/dealer_gamma.py` — GEX, vanna, charm, gamma walls

## Data Sources (expanded)

New ingestion modules:
- `ingestion/altdata/congressional.py` — congressional trading disclosures
- `ingestion/altdata/insider_filings.py` — SEC Form 4 with cluster buy detection
- `ingestion/altdata/dark_pool.py` — FINRA dark pool weekly data
- `ingestion/altdata/unusual_whales.py` — whale options flow detection
- `ingestion/altdata/prediction_odds.py` — Polymarket rapid probability shifts
- `ingestion/altdata/smart_money.py` — Reddit + Finviz trust-scored social signals
- `ingestion/altdata/supply_chain.py` — shipping rates, container index, ISM
- `ingestion/altdata/fed_liquidity.py` — Fed net liquidity equation
- `ingestion/altdata/institutional_flows.py` — ETF flows + SEC 13F holdings
- `ingestion/altdata/fara.py` — DOJ FARA foreign agent lobbying (who foreign governments pay to influence US policy)
- `ingestion/altdata/foia_cables.py` — State Dept + NSA Archive declassified diplomatic cables
- `ingestion/altdata/gdelt.py` — enhanced with actor-level tone, country-pair tension scoring, geopolitical event signals

## Frontend Views (expanded)

- MoneyFlow — global money flow D3 visualization (Central Banks → Markets → Sectors)
- CrossReference — government stats vs physical reality lie detector
- Predictions — oracle scoreboard + calibration chart
- ActorNetwork — D3 force graph of financial power structure (building)
- IntelDashboard — unified intelligence command center (building)
- TrendTracker — momentum, regime, rotation, vol, liquidity trends (building)

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
├── tests/         # pytest suite (354 tests)
└── scripts/       # Migration and utility scripts
```
