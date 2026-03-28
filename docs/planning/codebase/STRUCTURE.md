# Structure

## Directory Layout

```
grid/
├── api/                          # FastAPI REST API and WebSocket server
│   ├── main.py                   # App entry point, middleware, WebSocket, PWA serving
│   ├── auth.py                   # JWT authentication (single-operator, bcrypt + jose)
│   ├── dependencies.py           # @lru_cache singleton factories (engine, PITStore, journal, registry)
│   ├── routers/                  # API route handlers (thin — delegate to domain modules)
│   │   ├── agents.py             # TradingAgents run/status endpoints
│   │   ├── backtest.py           # Backtesting endpoints
│   │   ├── config.py             # System configuration endpoints
│   │   ├── discovery.py          # Clustering and orthogonality endpoints
│   │   ├── journal.py            # Decision journal CRUD (read + outcome recording)
│   │   ├── models.py             # Model registry and governance endpoints
│   │   ├── ollama.py             # Ollama/LLM inference endpoints
│   │   ├── physics.py            # Market physics verification endpoints
│   │   ├── regime.py             # Regime state and history endpoints
│   │   ├── signals.py            # Signal/feature snapshot endpoints
│   │   ├── system.py             # Health check, status, data freshness
│   │   └── workflows.py          # Workflow list/enable/disable/run endpoints
│   └── schemas/                  # Pydantic request/response models
│       ├── auth.py               # LoginRequest, LoginResponse, TokenVerifyResponse
│       ├── journal.py            # Journal entry schemas
│       ├── models.py             # Model registry schemas
│       ├── regime.py             # Regime state schemas
│       └── system.py             # System status schemas
│
├── ingestion/                    # 37+ data source pullers
│   ├── scheduler.py              # V1 scheduler (AUTHORITATIVE): FRED, yfinance, BLS, EDGAR
│   ├── scheduler_v2.py           # V2 scheduler: international, trade, physical, altdata
│   ├── fred.py                   # FRED API puller (FREDPuller class)
│   ├── bls.py                    # Bureau of Labor Statistics puller
│   ├── edgar.py                  # SEC EDGAR (Form 4, 13F, 8-K)
│   ├── yfinance_pull.py          # Yahoo Finance market data puller
│   ├── sec_velocity.py           # SEC 8-K filing velocity tracker
│   ├── seed_v2.py                # V2 data seeding utility
│   ├── dexscreener.py            # DexScreener crypto DEX data
│   ├── pumpfun.py                # Pump.fun memecoin data
│   ├── crypto_bootstrap.py       # Crypto data bootstrap utility
│   ├── international/            # International macro data sources
│   │   ├── ecb.py                # European Central Bank SDW
│   │   ├── imf.py                # IMF data
│   │   ├── oecd.py               # OECD statistics
│   │   ├── eurostat.py           # Eurostat
│   │   ├── bis.py                # Bank for International Settlements
│   │   ├── kosis.py              # Korea Statistical Information Service
│   │   ├── jquants.py            # J-Quants (Japan equity data)
│   │   ├── edinet.py             # EDINET (Japan corporate filings)
│   │   ├── bcb.py                # Central Bank of Brazil
│   │   ├── rbi.py                # Reserve Bank of India
│   │   ├── mas.py                # Monetary Authority of Singapore
│   │   ├── abs_au.py             # Australian Bureau of Statistics
│   │   └── akshare_macro.py      # AkShare China macro data
│   ├── trade/                    # International trade data
│   │   ├── comtrade.py           # UN Comtrade
│   │   ├── wiod.py               # World Input-Output Database
│   │   ├── cepii.py              # CEPII gravity/distance data
│   │   └── atlas_eci.py          # Atlas of Economic Complexity (ECI)
│   ├── physical/                 # Physical economy and alternative indicators
│   │   ├── usda_nass.py          # USDA National Agricultural Statistics
│   │   ├── ofr.py                # Office of Financial Research
│   │   ├── dbnomics.py           # DBnomics aggregator
│   │   ├── viirs.py              # VIIRS nighttime lights satellite data
│   │   ├── patents.py            # Patent filing data
│   │   └── euklems.py            # EU KLEMS productivity data
│   └── altdata/                  # Alternative data sources
│       ├── gdelt.py              # GDELT event database
│       ├── noaa_ais.py           # NOAA AIS vessel tracking
│       └── opportunity.py        # Opportunity Insights economic tracker
│
├── normalization/                # Multi-source conflict resolution
│   ├── resolver.py               # Resolver class: priority-based selection, conflict detection
│   └── entity_map.py             # EntityMap: raw series_id -> feature_registry name mapping
│
├── store/                        # Point-in-time correct query engine
│   └── pit.py                    # PITStore class: DISTINCT ON queries, assert_no_lookahead()
│
├── features/                     # Feature engineering
│   ├── lab.py                    # FeatureLab: z-score, rolling slope, lagged change, ratio, spread
│   └── registry.py               # FeatureRegistry: read-only query interface for feature_registry table
│
├── discovery/                    # Unsupervised regime discovery
│   ├── clustering.py             # ClusterDiscovery: PCA + GMM/KMeans/Agglomerative
│   └── orthogonality.py          # OrthogonalityAudit: feature independence analysis
│
├── validation/                   # Backtesting and promotion gates
│   ├── backtest.py               # Walk-forward backtesting engine
│   └── gates.py                  # GateChecker: promotion requirement enforcement
│
├── inference/                    # Live model scoring
│   └── live.py                   # LiveInference: runs production models on latest PIT data
│
├── journal/                      # Immutable decision log
│   └── log.py                    # DecisionJournal: append-only logging, outcome recording
│
├── governance/                   # Model lifecycle state machine
│   └── registry.py               # ModelRegistry: state transitions, gate checks, rollback
│
├── agents/                       # TradingAgents multi-agent framework
│   ├── runner.py                 # AgentRunner: orchestrates multi-agent deliberation
│   ├── adapter.py                # Parses agent decisions into structured format
│   ├── config.py                 # Agent LLM configuration builder
│   ├── context.py                # GRIDContext: regime intelligence for agent prompts
│   ├── progress.py               # WebSocket progress broadcasting
│   ├── scheduler.py              # Agent run scheduler (cron-based)
│   └── backtest.py               # Agent backtesting harness
│
├── hyperspace/                   # Hyperspace P2P LLM integration
│   ├── client.py                 # HyperspaceClient: API wrapper
│   ├── embeddings.py             # Embedding generation and similarity search
│   ├── reasoner.py               # LLM reasoning chains
│   ├── research_agent.py         # Research agent for market analysis
│   └── monitor.py                # Hyperspace node monitoring
│
├── ollama/                       # Ollama local LLM integration (deprecated)
│   ├── client.py                 # OllamaClient: API wrapper
│   ├── reasoner.py               # Reasoning chains
│   ├── market_briefing.py        # Daily market briefing generation
│   └── knowledge/                # Static knowledge files for LLM context
│
├── physics/                      # Market physics verification
│   ├── verify.py                 # MarketPhysicsVerifier: sanity checks on data/features
│   ├── transforms.py             # Mathematical transforms
│   ├── conventions.py            # Financial convention definitions (units, annualization)
│   └── waves.py                  # WaveTask + build_execution_waves for parallel execution
│
├── backtest/                     # Backtesting engine
│   ├── engine.py                 # Core backtesting loop
│   ├── paper_trade.py            # Paper trading simulator
│   └── charts.py                 # Backtest visualization
│
├── orchestration/                # Multi-model task dispatch
│   ├── dispatch.py               # Interactive prompt generator for external LLMs (Gemini, ChatGPT, Perplexity)
│   ├── integrate.py              # Result integration from external model responses
│   ├── reconcile.py              # Cross-model reconciliation
│   ├── briefs/                   # Task briefs for different model specialists
│   └── inbox/                    # Results inbox from external models
│
├── workflows/                    # Declarative workflow system
│   ├── loader.py                 # YAML+Markdown workflow parser, enable/disable via symlinks
│   └── available/                # 13 workflow definitions (Markdown + YAML frontmatter)
│       ├── pull-fred.md
│       ├── pull-ecb.md
│       ├── pull-yfinance.md
│       ├── pull-bls.md
│       ├── pull-weekly-intl.md
│       ├── pull-monthly-trade.md
│       ├── pull-annual-datasets.md
│       ├── compute-features.md
│       ├── resolve-conflicts.md
│       ├── run-clustering.md
│       ├── audit-orthogonality.md
│       ├── check-regime.md
│       ├── verify-physics.md
│       ├── sweep-parameters.md
│       ├── validate-backtest.md
│       └── promote-model.md
│
├── pwa/                          # React 18 PWA frontend
│   ├── index.html                # HTML entry point
│   ├── manifest.json             # PWA manifest
│   ├── service-worker.js         # Service worker for offline support
│   ├── vite.config.js            # Vite bundler configuration (proxy /api -> :8000)
│   ├── package.json              # NPM dependencies
│   └── src/
│       ├── app.jsx               # Root React component
│       ├── api.js                # Centralized API fetch wrapper
│       ├── auth.js               # Auth state management
│       ├── store.js              # Zustand state store
│       ├── styles/
│       │   └── shared.js         # Shared style definitions
│       ├── components/           # Reusable UI components
│       │   ├── ConfidenceMeter.jsx
│       │   ├── DecisionModal.jsx
│       │   ├── KillSwitch.jsx
│       │   ├── NavBar.jsx
│       │   ├── RegimeCard.jsx
│       │   ├── SignalCard.jsx
│       │   ├── StatusDot.jsx
│       │   └── TransitionGauge.jsx
│       └── views/                # Page-level view components
│           ├── Dashboard.jsx
│           ├── Regime.jsx
│           ├── Signals.jsx
│           ├── Journal.jsx
│           ├── JournalEntry.jsx
│           ├── Models.jsx
│           ├── Discovery.jsx
│           ├── Backtest.jsx
│           ├── Agents.jsx
│           ├── Briefings.jsx
│           ├── Hyperspace.jsx
│           ├── Physics.jsx
│           ├── Workflows.jsx
│           ├── Settings.jsx
│           ├── SystemLogs.jsx
│           └── Login.jsx
│
├── pwa_dist/                     # Production PWA build output (served by FastAPI)
│
├── scripts/                      # Standalone scripts and utilities
│   ├── run_pipeline.py           # Legacy DuckDB ingest pipeline
│   ├── auto_regime.py            # Standalone regime detection
│   ├── worker.py                 # Background task worker
│   ├── setup_auth.py             # Auth credential setup
│   ├── ai_analyst.py             # AI-powered analysis script
│   ├── autoresearch.py           # Automated research runner
│   ├── compute_coordinator.py    # Compute task coordination
│   ├── signal_taxonomy.py        # Signal classification utility
│   ├── fix_model_eligible.py     # Database maintenance: fix feature eligibility flags
│   ├── bridge_to_pg.py           # Migration: DuckDB -> PostgreSQL bridge
│   ├── bridge_crucix.py          # Crucix data bridge
│   ├── sources_expanded.py       # Expanded source definitions for run_pipeline.py
│   ├── load_yfinance.py          # Bulk yfinance data loader
│   ├── load_wave2.py             # Wave 2 data loader (international/trade)
│   ├── load_wave3.py             # Wave 3 data loader (physical/altdata)
│   ├── load_alt_data.py          # Alternative data loader
│   ├── load_more_data.py         # Additional data loader
│   ├── load_ticker_deep.py       # Deep ticker data loader
│   ├── pull_intraday.py          # Intraday data puller
│   ├── pull_options.py           # Options data puller
│   ├── parse_edgar.py            # EDGAR filing parser
│   ├── parse_eia.py              # EIA data parser
│   ├── parse_gdelt.py            # GDELT event parser
│   ├── bootstrap.sh              # System bootstrap script
│   ├── build_llamacpp.sh         # Build llama.cpp from source
│   ├── start_llamacpp.sh         # Start llama.cpp server
│   ├── deploy_all.sh             # Full deployment script
│   ├── overnight_bulk.sh         # Overnight bulk data operations
│   └── bulk_download.sh          # Bulk data download
│
├── tests/                        # pytest test suite
│   ├── test_pit.py               # PIT correctness tests (highest priority)
│   ├── test_api.py               # API endpoint tests (~100 lines, weak)
│   ├── test_journal.py           # Decision journal tests
│   ├── test_ingestion.py         # Ingestion module tests
│   ├── test_resolver.py          # Conflict resolution tests
│   ├── test_international.py     # International source tests
│   ├── test_trade.py             # Trade data tests
│   ├── test_physical.py          # Physical data tests
│   └── test_hyperspace.py        # Hyperspace integration tests
│
├── llamacpp/                     # llama.cpp server configuration
├── hyperspace_setup/             # Hyperspace node setup files
├── server_setup/                 # Server deployment configuration
├── projects/                     # Project planning documents
│
├── config.py                     # Central Settings class (pydantic-settings singleton)
├── db.py                         # Database access layer (engine, connections, health check)
├── cli.py                        # CLI entry point for workflow management
├── schema.sql                    # Complete database schema with seed data
├── dashboard.py                  # Legacy dashboard (standalone)
├── grid_dashboard.html           # Legacy HTML dashboard
├── docker-compose.yml            # PostgreSQL 15 + TimescaleDB container
├── requirements.txt              # Python dependencies
├── requirements-api.txt          # API-specific dependencies
├── ATTENTION.md                  # 40-item audit of known issues
├── FIRST_DAY_REPORT.md           # System overview document
├── HOSTING.md                    # Hosting/deployment documentation
└── README.md                     # Project README
```

## Key Files

### Core Infrastructure
- `grid/config.py` — Single `Settings` instance used everywhere. All env vars, API keys, LLM config, DB credentials. Validates critical settings per environment.
- `grid/db.py` — Singleton SQLAlchemy engine (`get_engine()`), raw psycopg2 connections (`get_connection()`), schema application (`apply_schema()`), health check.
- `grid/schema.sql` — Complete database DDL with 9 tables, indexes, immutability trigger, and seed data (10 sources, 36 features).
- `grid/api/dependencies.py` — `@lru_cache()` factories that wire domain classes together: `get_db_engine()`, `get_pit_store()`, `get_journal()`, `get_model_registry()`.

### Pipeline Critical Path
- `grid/store/pit.py` — **Most critical file.** PITStore enforces no-lookahead via `DISTINCT ON` queries. Failure here means lookahead bias in all downstream inference.
- `grid/normalization/resolver.py` — Converts raw_series to resolved_series. Priority-based source selection and conflict detection.
- `grid/normalization/entity_map.py` — Maps raw series IDs to canonical feature names. Contains ~60 hardcoded seed mappings.
- `grid/features/lab.py` — Feature transformation engine (22K lines). Z-score, rolling slope, lagged change, ratio, spread computations.
- `grid/inference/live.py` — Runs production models on latest PIT data. Generates recommendations.

### Governance and Audit
- `grid/governance/registry.py` — Model state machine with enforced transitions and gate checks.
- `grid/validation/gates.py` — Gate requirements for each promotion step.
- `grid/validation/backtest.py` — Walk-forward backtesting engine.
- `grid/journal/log.py` — Immutable decision journal with full provenance.

### Entry Points
- `grid/api/main.py` — FastAPI app: routes, middleware, WebSocket, PWA serving, startup hooks (schedulers, agent system).
- `grid/cli.py` — CLI for workflow management: list/enable/disable/run/validate/status/schedule/waves/verify/conventions.
- `grid/ingestion/scheduler.py` — V1 ingestion scheduler (FRED, yfinance, BLS, EDGAR). **Authoritative** — not scheduler_v2.

### Frontend
- `grid/pwa/src/app.jsx` — Root React component
- `grid/pwa/src/store.js` — Zustand state management
- `grid/pwa/src/api.js` — Centralized fetch wrapper for API calls

## Naming Conventions

### Python Modules
- **Domain modules**: Named by their function in singular or descriptive form: `pit.py`, `lab.py`, `registry.py`, `resolver.py`, `live.py`, `log.py`, `gates.py`, `clustering.py`
- **Ingestion pullers**: Named after the data source: `fred.py`, `bls.py`, `edgar.py`, `ecb.py`, `comtrade.py`
- **Classes**: PascalCase descriptive names: `PITStore`, `FeatureLab`, `DecisionJournal`, `ModelRegistry`, `Resolver`, `ClusterDiscovery`, `LiveInference`, `GateChecker`, `AgentRunner`
- **Module-level constants**: SCREAMING_SNAKE_CASE: `CONFLICT_THRESHOLD`, `SEED_MAPPINGS`, `_VALID_TRANSITIONS`

### Database Tables
- snake_case: `source_catalog`, `raw_series`, `feature_registry`, `resolved_series`, `hypothesis_registry`, `model_registry`, `validation_results`, `decision_journal`, `agent_runs`

### API Routes
- All prefixed with `/api/v1/`: `/api/v1/auth`, `/api/v1/regime`, `/api/v1/signals`, `/api/v1/journal`, `/api/v1/models`, `/api/v1/discovery`, `/api/v1/agents`, `/api/v1/system`

### Frontend
- React components: PascalCase `.jsx` files: `RegimeCard.jsx`, `ConfidenceMeter.jsx`, `TransitionGauge.jsx`
- Views: PascalCase `.jsx` matching route names: `Dashboard.jsx`, `Regime.jsx`, `Journal.jsx`
- Utilities: camelCase `.js` files: `api.js`, `auth.js`, `store.js`

### Workflows
- Kebab-case `.md` files with YAML frontmatter: `pull-fred.md`, `compute-features.md`, `run-clustering.md`, `verify-physics.md`

### Scripts
- snake_case `.py` and kebab-free `.sh`: `run_pipeline.py`, `auto_regime.py`, `bootstrap.sh`, `deploy_all.sh`

## Configuration Files

### Environment Configuration
- `grid/.env` — Primary config file (not committed). All settings loaded via pydantic-settings in `grid/config.py`.
- `grid/.env.example` — Template with all available settings (should be kept in sync with `config.py` Settings class).

### Docker
- `grid/docker-compose.yml` — Single service: PostgreSQL 15 with TimescaleDB extension. Container name `grid_db`, port 5432, volume `grid_pgdata`.

### Python Dependencies
- `grid/requirements.txt` — Full Python dependency list
- `grid/requirements-api.txt` — API-specific dependencies (FastAPI, uvicorn, etc.)

### Frontend
- `grid/pwa/package.json` — NPM dependencies (React 18, Zustand, Lucide React, Vite)
- `grid/pwa/vite.config.js` — Vite config: dev server on :5173, proxy `/api` to :8000
- `grid/pwa/manifest.json` — PWA manifest for installability
- `grid/pwa/service-worker.js` — Service worker for offline caching

### Database Schema
- `grid/schema.sql` — Complete DDL applied via `python db.py` or `psql -f schema.sql`. Includes seed data for `source_catalog` (10 sources) and `feature_registry` (36 features).

### Workflow Definitions
- `grid/workflows/available/*.md` — 13 declarative workflow files with YAML frontmatter (name, group, schedule, secrets, depends_on, description). Enabled via symlink to `workflows/enabled/`.

### Project Documentation
- `grid/ATTENTION.md` — 40-item audit of known issues, bugs, and technical debt
- `grid/CLAUDE.md` (at repo root: `/home/user/17th/CLAUDE.md`) — Claude Code development guidelines
- `grid/README.md` — Project README
- `grid/HOSTING.md` — Deployment documentation
- `grid/FIRST_DAY_REPORT.md` — System overview

### Security Rules
- `/home/user/17th/.claude/rules/security.md` — SQL safety, auth, API endpoint rules
- `/home/user/17th/.claude/rules/testing.md` — Test framework and coverage expectations
- `/home/user/17th/.claude/rules/frontend.md` — React/PWA development rules
- `/home/user/17th/.claude/rules/performance.md` — Database and computation performance rules
- `/home/user/17th/.claude/rules/data-integrity.md` — PIT correctness and ingestion rules
