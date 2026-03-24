# GRID Architecture

## System Overview

```
                              GRID Intelligence Platform
    ┌─────────────────────────────────────────────────────────────────────┐
    │                                                                     │
    │  ┌───────────┐   ┌──────────────┐   ┌───────────┐   ┌───────────┐ │
    │  │  React 18  │   │  FastAPI      │   │ PostgreSQL│   │ LLM Layer │ │
    │  │  PWA       │──▶│  REST + WS    │──▶│ TimescaleDB│  │ Ollama /  │ │
    │  │  Zustand   │◀──│  Port 8000    │◀──│ Port 5432 │  │ Hyperspace│ │
    │  │  Port 5173 │   │              │   │           │   │ llama.cpp │ │
    │  └───────────┘   └──────┬───────┘   └───────────┘   └─────┬─────┘ │
    │                         │                                   │       │
    │                         ▼                                   │       │
    │            ┌─────────────────────────┐                     │       │
    │            │  Background Services     │◀────────────────────┘       │
    │            │  - Ingestion Scheduler   │                             │
    │            │  - Agent Scheduler       │                             │
    │            │  - Insight Scanner       │                             │
    │            │  - Git Sink (error logs) │                             │
    │            │  - Operator Inbox        │                             │
    │            └─────────────────────────┘                             │
    └─────────────────────────────────────────────────────────────────────┘
```

## Data Flow Pipeline

```
    ┌──────────────────────────────────────────────────────────────────┐
    │                    DATA INGESTION (37+ sources)                   │
    │                                                                  │
    │  Domestic          International        Alternative     Physical │
    │  ├─ FRED           ├─ ECB               ├─ Reddit       ├─ NOAA │
    │  ├─ BLS            ├─ BOJ               ├─ Options      ├─ EIA  │
    │  ├─ Census         ├─ BOE               ├─ yFinance     ├─ USDA │
    │  ├─ Treasury       ├─ RBI               │               │       │
    │  └─ BEA            ├─ KOSIS             │               │       │
    │                    ├─ AKShare            │               │       │
    │                    ├─ Eurostat           │               │       │
    │                    └─ Comtrade           │               │       │
    └──────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
    ┌──────────────────────────────────────────────────────────────────┐
    │                 RAW SERIES (raw_series table)                     │
    │  Every observation stored with:                                  │
    │    - observation_date (when event occurred)                       │
    │    - release_date    (when data was published)                    │
    │    - vintage_date    (revision tracking)                         │
    │    - source_id       (which provider)                            │
    └──────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
    ┌──────────────────────────────────────────────────────────────────┐
    │              CONFLICT RESOLUTION (normalization/)                 │
    │                                                                  │
    │  resolver.py:                                                    │
    │    - Compare same indicator across sources                       │
    │    - Flag conflicts when values diverge > threshold              │
    │    - Per-family thresholds (vol: 2%, commodity: 1.5%, etc.)      │
    │    - Winner = highest-priority source                            │
    │                                                                  │
    │  entity_map.py:                                                  │
    │    - Disambiguate same concept from different naming conventions  │
    │                                                                  │
    │  Output: resolved_series table (canonical, deduplicated)         │
    └──────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
    ┌──────────────────────────────────────────────────────────────────┐
    │               PIT QUERY ENGINE (store/pit.py)                    │
    │                                                                  │
    │  HARD CONSTRAINTS (never violated):                              │
    │    1. release_date <= as_of_date  (no future data)               │
    │    2. obs_date <= as_of_date      (no future observations)       │
    │    3. Vintage policy:                                            │
    │       FIRST_RELEASE  → earliest vintage per (feature, obs_date)  │
    │       LATEST_AS_OF   → latest vintage available at as_of_date    │
    │                                                                  │
    │  assert_no_lookahead() — safety net before any inference output   │
    │                                                                  │
    │  Uses PostgreSQL DISTINCT ON for efficient latest-vintage queries │
    └──────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
    ┌──────────────────────────────────────────────────────────────────┐
    │             FEATURE ENGINEERING (features/lab.py)                 │
    │                                                                  │
    │  Transformations:                                                │
    │    zscore_normalize  — rolling z-score (window=252 days)         │
    │    rolling_slope     — annualised linear regression slope        │
    │    pct_change_lagged — percentage change over N days             │
    │    ratio             — feature_a / feature_b                     │
    │    spread            — feature_a - feature_b                     │
    │                                                                  │
    │  All transforms use PIT-correct inputs only                      │
    └──────────────────────────┬───────────────────────────────────────┘
                               │
                     ┌─────────┴─────────┐
                     ▼                   ▼
    ┌────────────────────────┐  ┌────────────────────────┐
    │  REGIME DISCOVERY      │  │  LIVE INFERENCE        │
    │  (discovery/)          │  │  (inference/live.py)   │
    │                        │  │                        │
    │  orthogonality.py:     │  │  - Load production     │
    │    PCA, correlation    │  │    model per layer     │
    │    heatmaps, scree     │  │  - Get latest PIT data │
    │                        │  │  - Compute features    │
    │  clustering.py:        │  │  - Score against       │
    │    GMM, KMeans, Agg    │  │    parameter snapshot  │
    │    Test k=2..6         │  │  - Generate BUY/SELL/  │
    │    Transition matrices │  │    HOLD recommendation │
    │    Persistence metrics │  │                        │
    │                        │  │  assert_no_lookahead() │
    │  options_scanner.py:   │  │  before persisting     │
    │    7-signal mispricing  │  │                        │
    │    100x opportunity     │  │                        │
    │    detection            │  │                        │
    └────────────────────────┘  └───────────┬────────────┘
                                            │
                                            ▼
    ┌──────────────────────────────────────────────────────────────────┐
    │          VALIDATION & GOVERNANCE                                  │
    │                                                                  │
    │  validation/gates.py — promotion gate enforcement:               │
    │    CANDIDATE → SHADOW:  validation_run_id + hypothesis PASSED    │
    │    SHADOW → STAGING:    operator approval                        │
    │    STAGING → PRODUCTION: ≥20 journal entries + no layer conflict │
    │                                                                  │
    │  governance/registry.py — model lifecycle state machine:         │
    │    CANDIDATE → SHADOW → STAGING → PRODUCTION → FLAGGED → RETIRED│
    │                                                                  │
    │  Enforces: one PRODUCTION model per layer, auto-demotion         │
    └──────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
    ┌──────────────────────────────────────────────────────────────────┐
    │              DECISION JOURNAL (journal/log.py)                    │
    │                                                                  │
    │  IMMUTABLE — entries never updated or deleted                     │
    │                                                                  │
    │  Each entry records:                                             │
    │    - model_version_id, inferred_state, state_confidence          │
    │    - transition_probability, contradiction_flags                  │
    │    - grid_recommendation, baseline_recommendation                │
    │    - action_taken, counterfactual, operator_confidence            │
    │    - annotation (LLM reasoning, first 500 chars)                 │
    │                                                                  │
    │  Outcomes recorded separately (never modify original decision)    │
    │  Validates: confidence 0-1, no NaN/infinity                      │
    └──────────────────────────────────────────────────────────────────┘
```

## LLM Integration

```
    ┌──────────────────────────────────────────────────────────────────┐
    │                     LLM INTEGRATION LAYER                        │
    │                                                                  │
    │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
    │  │   Hyperspace     │  │     Ollama       │  │   llama.cpp     │ │
    │  │   (P2P local)    │  │   (local)        │  │   (local)       │ │
    │  │                  │  │                  │  │                  │ │
    │  │  reasoner.py:    │  │  reasoner.py:    │  │  client.py:     │ │
    │  │  - explain_rel   │  │  - explain_rel   │  │  - chat()       │ │
    │  │  - gen_hypotheses│  │  - gen_hypotheses│  │  - is_available  │ │
    │  │  - critique_bt   │  │  - critique_bt   │  │                  │ │
    │  │                  │  │  - regime_trans   │  │                  │ │
    │  │  embeddings.py:  │  │                  │  │                  │ │
    │  │  - semantic sim  │  │  market_briefing:│  │                  │ │
    │  │                  │  │  - hourly/daily/ │  │                  │ │
    │  │                  │  │    weekly reports │  │                  │ │
    │  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
    │                                                                  │
    │  All calls return None if provider is offline (graceful degrade) │
    │  All outputs logged to outputs/llm_insights/*.md (timestamped)   │
    │  Insight scanner runs daily/weekly reviews of accumulated output  │
    │                                                                  │
    │  ┌─────────────────────────────────────────────────────────────┐ │
    │  │  TradingAgents (Multi-Agent Deliberation)                   │ │
    │  │                                                             │ │
    │  │  agents/runner.py orchestrates:                              │ │
    │  │    1. Build GRID regime context                              │ │
    │  │    2. Run analyst agents (fundamental, technical, sentiment) │ │
    │  │    3. Bull vs. bear debate                                   │ │
    │  │    4. Risk assessment                                        │ │
    │  │    5. Final BUY/SELL/HOLD decision                          │ │
    │  │    6. Log to decision journal + agent_runs table             │ │
    │  │    7. Log full deliberation to outputs/llm_insights/         │ │
    │  │                                                             │ │
    │  │  Providers: OpenAI, Anthropic, Hyperspace, llama.cpp         │ │
    │  └─────────────────────────────────────────────────────────────┘ │
    └──────────────────────────────────────────────────────────────────┘
```

## API Architecture

```
    ┌──────────────────────────────────────────────────────────────────┐
    │                    FastAPI Application                            │
    │                                                                  │
    │  Middleware Stack:                                                │
    │    SecurityHeaders → RequestSizeLimit → CORS                     │
    │                                                                  │
    │  Auth: JWT (single-operator, bcrypt password, shelve rate limit)  │
    │  WebSocket: first-message auth (token not in URL)                 │
    │                                                                  │
    │  ┌─────────────────────────────────────────────────────────────┐ │
    │  │  API Routes (all /api/v1/* except /health)                   │ │
    │  │                                                             │ │
    │  │  /auth     — login, logout, verify                          │ │
    │  │  /system   — health, status, alerts, logs                   │ │
    │  │  /regime   — current regime, history, feature status         │ │
    │  │  /signals  — live signals, top drivers                      │ │
    │  │  /journal  — decision log (immutable), outcomes              │ │
    │  │  /models   — model registry, transitions, validation        │ │
    │  │  /discovery— hypotheses, clustering jobs                    │ │
    │  │  /config   — sources, features, system config               │ │
    │  │  /physics  — market physics verification, OU/Hurst/energy   │ │
    │  │  /workflows— declarative workflow management                │ │
    │  │  /agents   — TradingAgents runs, status                     │ │
    │  │  /ollama   — briefings, reasoning, insights                 │ │
    │  │  /backtest — walk-forward backtesting                       │ │
    │  │  /options  — mispricing scanner, history                    │ │
    │  │                                                             │ │
    │  │  /ws       — WebSocket (regime updates, agent progress,     │ │
    │  │              signal updates, node status, ping)              │ │
    │  └─────────────────────────────────────────────────────────────┘ │
    │                                                                  │
    │  PWA Serving:                                                    │
    │    Production: pwa_dist/ (Vite build output)                     │
    │    Development: pwa/ (source, proxied via Vite dev server)       │
    │    SPA routing: all non-API paths → index.html                   │
    └──────────────────────────────────────────────────────────────────┘
```

## Database Schema (Key Tables)

```
    ┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
    │ source_catalog   │     │ feature_registry  │     │ raw_series       │
    │                 │     │                  │     │                  │
    │ id              │◀───│ source_id (FK)   │     │ feature_id (FK)  │──▶ feature_registry
    │ name            │     │ id               │◀────│ source_id (FK)   │──▶ source_catalog
    │ active          │     │ name             │     │ obs_date         │
    │ priority_rank   │     │ family           │     │ release_date     │
    │ trust_score     │     │ model_eligible   │     │ vintage_date     │
    │ last_pull_at    │     │ transformation   │     │ value            │
    └─────────────────┘     └──────────────────┘     │ pull_timestamp   │
                                                      └──────────────────┘
                                                               │
                                                               ▼
    ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
    │ resolved_series   │     │ model_registry    │     │ decision_journal │
    │                  │     │                  │     │                  │
    │ feature_id       │     │ id               │◀────│ model_version_id │
    │ obs_date         │     │ name, version    │     │ inferred_state   │
    │ value            │     │ layer            │     │ state_confidence │
    │ conflict_flag    │     │ state            │     │ recommendation   │
    │ conflict_detail  │     │ feature_set      │     │ annotation       │
    │ release_date     │     │ parameter_snap   │     │ outcome_*        │
    │ vintage_date     │     │ promoted_at      │     │ decision_ts      │
    └──────────────────┘     │ retired_at       │     └──────────────────┘
                             └──────────────────┘
                                      │
                             ┌────────┴────────┐
                             ▼                 ▼
                  ┌──────────────────┐  ┌──────────────────┐
                  │ validation_results│  │ hypothesis_reg   │
                  │                  │  │                  │
                  │ model_version_id │  │ id               │
                  │ metric_name      │  │ state            │
                  │ metric_value     │  │ pattern_desc     │
                  │ gate_passed      │  │ falsifiable_stmt │
                  └──────────────────┘  └──────────────────┘

    ┌──────────────────┐     ┌──────────────────┐
    │ agent_runs        │     │ options_mispricing│
    │                  │     │ _scans            │
    │ ticker           │     │                  │
    │ analyst_reports  │     │ ticker           │
    │ bull_bear_debate │     │ score            │
    │ risk_assessment  │     │ payoff_multiple  │
    │ final_decision   │     │ direction        │
    │ decision_journal │     │ thesis           │
    │ _id (FK)         │     │ is_100x          │
    │ llm_provider     │     │ signals (JSONB)  │
    │ duration_seconds │     │ spot_price       │
    └──────────────────┘     └──────────────────┘
```

## Model Lifecycle State Machine

```
                    ┌───────────┐
                    │ CANDIDATE │
                    └─────┬─────┘
                          │ Gates: validation_run_id set
                          │        + hypothesis PASSED
                          ▼
                    ┌───────────┐
              ┌────▶│  SHADOW   │
              │     └─────┬─────┘
              │           │ Gate: operator approval
              │           ▼
              │     ┌───────────┐
    Demotion  │     │  STAGING  │
    (when new │     └─────┬─────┘
    model     │           │ Gates: ≥20 journal entries
    promoted) │           │        no other PRODUCTION in layer
              │           ▼
              │     ┌────────────┐        ┌──────────┐
              └─────│ PRODUCTION │───────▶│ FLAGGED  │
                    └────────────┘        └────┬─────┘
                          │                    │
                          │                    │ operator action
                          ▼                    ▼
                    ┌───────────┐         ┌───────────┐
                    │  RETIRED  │◀────────│ PRODUCTION│ (rollback)
                    └───────────┘         └───────────┘

    Any state ──▶ RETIRED (operator decision)
```

## Directory Structure

```
grid/
├── api/                    # FastAPI routes, auth, middleware
│   ├── main.py             # App entry, WebSocket, startup/shutdown
│   ├── auth.py             # JWT auth, rate limiting (shelve-backed)
│   ├── dependencies.py     # Clearable singletons (engine, PIT, journal)
│   ├── routers/            # Route modules (14 routers)
│   └── schemas/            # Pydantic request/response models
├── ingestion/              # 37+ data source pullers
│   ├── base.py             # BasePuller with shared _resolve_source_id
│   ├── fred.py, bls.py...  # Domestic sources
│   ├── international/      # ECB, BOJ, KOSIS, AKShare, Eurostat, etc.
│   ├── altdata/            # Reddit, options, yFinance
│   ├── trade/              # Comtrade, Atlas ECI
│   ├── physical/           # NOAA, EIA, USDA
│   └── scheduler.py        # Unified scheduler (hourly/daily/weekly/monthly)
├── normalization/          # Multi-source conflict resolution
│   ├── resolver.py         # Priority-based resolution + per-family thresholds
│   └── entity_map.py       # Entity disambiguation mappings
├── store/                  # PIT-correct query engine
│   └── pit.py              # PostgreSQL DISTINCT ON, assert_no_lookahead()
├── features/               # Feature engineering
│   └── lab.py              # zscore, rolling_slope, ratio, pct_change
├── discovery/              # Unsupervised regime discovery
│   ├── clustering.py       # GMM/KMeans/Agg, k=2..6, transition matrices
│   ├── orthogonality.py    # PCA, correlation stability, factor loadings
│   └── options_scanner.py  # 7-signal mispricing, 100x detection
├── validation/             # Walk-forward backtesting
│   └── gates.py            # Promotion gate enforcement
├── inference/              # Live model scoring
│   └── live.py             # Production model inference + recommendations
├── journal/                # Immutable decision log
│   └── log.py              # NaN/inf validation, outcome recording
├── governance/             # Model lifecycle
│   └── registry.py         # State machine: CANDIDATE → PRODUCTION
├── agents/                 # TradingAgents multi-agent framework
│   ├── runner.py           # Orchestration: context → agents → journal
│   ├── adapter.py          # Parse agent decisions
│   ├── context.py          # Build GRID regime context for prompts
│   ├── config.py           # Provider selection (OpenAI/Anthropic/local)
│   ├── progress.py         # WebSocket progress broadcast
│   └── scheduler.py        # Scheduled agent runs
├── hyperspace/             # Local LLM inference (P2P)
│   ├── client.py           # OpenAI-compatible endpoint
│   ├── reasoner.py         # Hypothesis gen, explanations, critiques
│   └── embeddings.py       # Semantic similarity via embeddings
├── ollama/                 # Ollama LLM integration
│   ├── client.py           # Chat API with knowledge document injection
│   ├── reasoner.py         # Enhanced reasoning with knowledge context
│   └── market_briefing.py  # Scheduled hourly/daily/weekly briefings
├── llamacpp/               # llama.cpp direct integration
│   └── client.py           # Local inference endpoint
├── outputs/                # Generated outputs (gitignored content)
│   ├── llm_logger.py       # Timestamped MD logging for all LLM outputs
│   ├── insight_scanner.py  # Periodic review of accumulated insights
│   ├── llm_insights/       # Individual insight files (gitignored)
│   ├── market_briefings/   # Briefing MD files (gitignored)
│   └── insight_reviews/    # Scanner review files (gitignored)
├── physics/                # Market physics verification
├── workflows/              # Declarative workflow system
├── orchestration/          # Task orchestration
├── server_log/             # Git-backed error logging + operator inbox
├── pwa/                    # React 18 PWA frontend
│   ├── src/                # Components, views, Zustand store, API client
│   ├── public/             # Icons, manifest, service worker
│   └── vite.config.js      # Build config, dev proxy
├── pwa_dist/               # Production build output (served by FastAPI)
├── tests/                  # pytest suite (354 tests)
├── scripts/                # Migration, setup, utility scripts
├── schema.sql              # Database schema
├── config.py               # pydantic-settings configuration
├── db.py                   # SQLAlchemy engine + health check
└── ATTENTION.md            # 64-item audit tracking list
```
