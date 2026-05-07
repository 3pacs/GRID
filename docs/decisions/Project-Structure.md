---
source: /Users/anikdang/grid_obsidian/Architecture/Project-Structure.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
# GRID Project Structure

> **Last updated:** 2026-04-05
> **Location:** `/Users/anikdang/dev/GRID`
> **Total Python modules:** 56+ directories, 520+ files (incl. rag/, new pullers, intelligence modules)

This is the canonical directory map. Use this to orient before diving into any module.

## Root Files

| File | Purpose |
|------|---------|
| `config.py` | Centralized pydantic-settings (391 lines, 25+ API keys, all toggles) |
| `db.py` | SQLAlchemy engine + psycopg2 raw connections (pool: 20+40 overflow) |
| `schema.sql` | PostgreSQL 15 + TimescaleDB (30+ tables, 101KB) |
| `mcp_server.py` | Model Context Protocol interface (46KB, full-text search) |
| `cli.py` | CLI entrypoint |
| `dashboard.py` | Dashboard runner |

## Directory Tree (with file counts and purposes)

```
GRID/
├── a2a/                    (4)   Agent-to-Agent protocol
│   ├── agent_card.py              Agent metadata + capabilities
│   ├── client.py                  A2A client
│   └── server.py                  A2A server
│
├── agents/                 (9)   LLM trading agents
│   ├── runner.py                  Multi-round debate execution
│   ├── backtest.py                Historical agent validation
│   ├── personas.py                Investor persona templates
│   ├── scheduler.py               Cron scheduling for agent runs
│   ├── config.py                  Agent configuration
│   ├── context.py                 Agent context management
│   ├── adapter.py                 External framework adapter
│   └── progress.py                Progress tracking
│
├── alerts/                 (4)   Notification system
│   ├── email.py                   Email alerts (failure, regime, 100x)
│   ├── push_notify.py             Push notifications
│   ├── hundredx_digest.py         100x opportunity digest
│   └── scheduler.py               Alert scheduling
│
├── alpha_research/         (21)  Evolutionary factor mining
│   ├── conviction_scorer.py       Signal conviction scoring
│   ├── debate.py                  Multi-model debate protocol
│   ├── ensemble.py                Ensemble signal combination
│   ├── heartbeat.py               Health monitoring
│   ├── adapters/                  Signal adapters
│   ├── data/                      Panel builder, split adjuster, shares tracker
│   ├── signals/                   Credit cycle, macro regime, quanta alpha, exposure
│   ├── strategies/                Adaptive rotation
│   └── validation/                Gauntlet, metrics
│
├── analysis/               (29)  Market analysis engines
│   ├── capital_flows.py           Capital flow tracking
│   ├── vol_surface.py             Volatility surface analysis
│   ├── hypothesis_tester.py       Statistical hypothesis testing
│   ├── flow_thesis.py             Flow thesis engine
│   ├── research_agent.py          Deep research + synthesis
│   ├── sector_map.py              Sector mapping
│   ├── money_flow.py              Money flow analysis
│   ├── money_flow_engine/         8-layer money flow system
│   │   ├── layer_monetary.py      Fed, central bank flows
│   │   ├── layer_sovereign.py     Government, fiscal flows
│   │   ├── layer_institutional.py Pension, endowment flows
│   │   ├── layer_corporate.py     Buybacks, capex, M&A
│   │   ├── layer_credit.py        Debt markets, spreads
│   │   ├── layer_crypto.py        On-chain flows
│   │   ├── layer_market.py        Equity market flows
│   │   └── layer_retail.py        Retail sentiment, flows
│   └── ...                        astro, ephemeris, viz, prompt optimizer
│
├── api/                    (69)  FastAPI backend
│   ├── main.py                    App entrypoint, CORS, middleware
│   ├── auth.py                    JWT authentication
│   ├── dependencies.py            Dependency injection
│   ├── routers/            (58)   Route handlers
│   │   ├── intelligence*.py       8 intelligence routers (actors, companies, forensics, etc.)
│   │   ├── astrogrid*.py          5 astrogrid routers
│   │   ├── watchlist*.py          5 watchlist routers
│   │   ├── trading.py             Trade execution
│   │   ├── oracle.py              Oracle predictions
│   │   ├── flows.py               Capital flows
│   │   ├── regime.py              Market regime
│   │   ├── signals.py             Signal registry
│   │   ├── chat.py                LLM chat
│   │   ├── journal.py             Decision journal
│   │   └── ...                    40+ more routers
│   └── schemas/             (6)   Pydantic schemas
│
├── autoagent/              (5)   Autonomous agent runner
│   ├── agent.py                   AutoAgent base
│   ├── grid_autoagent_runner.py   GRID-specific runner
│   ├── baseline_signal_generator.py  Signal generation
│   └── tasks/                     Task definitions
│
├── backtest/               (4)   Backtesting engine
│   ├── engine.py                  Walk-forward backtester
│   ├── paper_trade.py             Paper trading simulator
│   └── charts.py                  Backtest visualization
│
├── derivatives/                  Derivatives analysis (TypeScript)
│   └── src/                       Source files
│
├── discovery/              (5)   Pattern discovery
│   ├── changepoint_detector.py    Regime changepoint detection
│   ├── clustering.py              Unsupervised clustering
│   ├── options_scanner.py         Options opportunity scanner
│   └── orthogonality.py           Signal orthogonality analysis
│
├── features/               (5)   Feature engineering
│   ├── lab.py                     Feature laboratory
│   ├── registry.py                Feature registry (1,281 features)
│   ├── alpha101.py                WorldQuant Alpha101 factors
│   └── importance.py              Feature importance tracking
│
├── gemma/                  (3)   Gemma 3 27B QAT (128K context)
│   ├── client.py                  Gemma client via llama.cpp
│   └── micro.py                   GemmaMicroPool (CPU classifiers, ports 8082-8084)
│
├── governance/             (2)   Model governance
│   └── registry.py                State machine: CANDIDATE → SHADOW → STAGING → PRODUCTION
│
├── hyperspace/             (6)   P2P local LLM inference
│   ├── client.py                  Hyperspace client
│   ├── embeddings.py              all-MiniLM-L6-v2 embeddings
│   ├── reasoner.py                Local reasoning
│   ├── research_agent.py          Research agent
│   └── monitor.py                 Health monitoring
│
├── inference/              (12)  ML inference pipeline
│   ├── live.py                    Live scoring pipeline (HOT PATH)
│   ├── turboquant.py              KV cache quantization (3-bit, 5x compression)
│   ├── ensemble.py                Model ensemble
│   ├── training.py                Model training
│   ├── tuning.py                  Hyperparameter tuning
│   ├── calibration.py             Prediction calibration
│   ├── circuit_breaker.py         Inference circuit breaker
│   ├── failure_analysis.py        Failure root cause analysis
│   ├── timesfm_service.py         Google TimesFM service
│   ├── trade_logger.py            Trade decision logging
│   └── trained_models.py          Model artifact storage
│
├── ingestion/              (118) Data ingestion (90+ sources)
│   ├── base.py                    BasePuller pattern (all pullers extend this)
│   ├── scheduler.py               48-source cron scheduler
│   ├── smart_scheduler.py         Adaptive scheduling
│   ├── fred.py                    FRED (300+ macro series)
│   ├── bls.py                     BLS (employment, CPI, PPI)
│   ├── yfinance_pull.py           Yahoo Finance OHLCV
│   ├── tiingo_pull.py             Tiingo prices
│   ├── tiingo_fundamentals_pull.py  Tiingo fundamentals
│   ├── tiingo_news_pull.py        Tiingo news
│   ├── edgar.py                   SEC Edgar filings
│   ├── coingecko.py               Crypto prices
│   ├── dexscreener.py             DEX token data
│   ├── options.py                 Options data
│   ├── altdata/            (59)   Alternative data
│   │   ├── congressional.py       Congressional trading
│   │   ├── dark_pool.py           FINRA dark pool
│   │   ├── lobbying.py            Lobbying data
│   │   ├── gdelt.py               Geopolitical events
│   │   ├── fed_speeches.py        Fed speeches
│   │   ├── insider_filings.py     Insider trades
│   │   ├── offshore_leaks.py      ICIJ offshore leaks
│   │   ├── opencorporates.py      Corporate registries
│   │   ├── google_trends.py       Google Trends
│   │   ├── social_attention.py    Social media attention
│   │   ├── bookmarks.py           Bookmark intelligence
│   │   ├── obsidian_sync.py       Obsidian vault sync
│   │   └── ...                    46+ more altdata sources
│   ├── international/      (16)   International datasets
│   │   ├── ecb.py                 European Central Bank
│   │   ├── imf.py                 IMF data
│   │   ├── kosis.py               Korean statistics
│   │   ├── jquants.py             Japan exchange data
│   │   ├── eurostat.py            EU statistics
│   │   └── ...                    11+ more
│   ├── physical/           (7)    Physical economy
│   │   ├── viirs.py               Satellite nightlight data
│   │   ├── patents.py             Patent filings
│   │   ├── usda_nass.py           Agriculture data
│   │   └── ...
│   ├── celestial/          (5)    Celestial data (experimental)
│   │   ├── planetary.py, lunar.py, solar.py, vedic.py, chinese.py
│   ├── trade/              (4)    International trade
│   │   ├── comtrade.py            UN Comtrade
│   │   └── ...
│   └── ml/                 (2)    ML-powered ingestion
│       └── finbert_scorer.py      FinBERT sentiment scoring
│
├── intelligence/           (89)  Intelligence analysis (CORE)
│   ├── actor_network.py           495 named actors, wealth flows (7,002 lines)
│   ├── actor_discovery.py         Automated actor discovery
│   ├── causation.py               Root cause tracing (2,387 lines)
│   ├── causation_core.py          Causation primitives
│   ├── causation_graph.py         Causal graph construction
│   ├── causation_scoring.py       Causal scoring
│   ├── cross_reference.py         Multi-source cross-referencing
│   ├── sleuth.py                  LLM investigative detective
│   ├── trust_scorer.py            Bayesian trust scoring with recency decay
│   ├── postmortem.py              Automated prediction post-mortems
│   ├── lever_pullers.py           Who controls what levers
│   ├── forensics.py               Financial forensics
│   ├── company_analyzer.py        Deep company analysis
│   ├── deep_dive.py               Deep-dive intelligence
│   ├── deep_graph.py              Knowledge graph construction
│   ├── entity_resolver.py         Cross-source entity resolution
│   ├── rag.py                     RAG retrieval
│   ├── hypothesis_engine.py       Hypothesis generation
│   ├── pattern_engine.py          Pattern recognition
│   ├── signal_registry.py         Signal catalog
│   ├── thesis_tracker.py          Thesis lifecycle tracking
│   ├── obsidian_agent.py          Obsidian vault integration
│   ├── opsec.py                   Operational security
│   ├── actors/              (6)   Actor subsystem
│   │   ├── models.py, db.py, graph.py, analysis.py, ingestion.py, seed_data.py
│   ├── adapters/            (18)  Intelligence adapters (bridge pattern)
│   ├── regime/              (4)   Market regime detection
│   │   ├── classifier.py, forecast.py, state_vector.py, episode_matcher.py
│   ├── Network modules:
│   │   ├── banking_network.py     Banking relationships
│   │   ├── energy_network.py      Energy sector network
│   │   ├── pharma_network.py      Pharma network
│   │   ├── media_network.py       Media ownership
│   │   ├── real_estate_network.py Real estate connections
│   │   ├── tech_monopoly_network.py  Tech monopoly map
│   │   ├── defense_contractors.py Defense network
│   │   ├── swf_network.py         Sovereign wealth funds
│   │   ├── commodities_agriculture_network.py
│   │   ├── defi_protocols.py      DeFi protocol network
│   │   └── influence_network.py   Influence mapping
│   └── ...                        gov_intel, insider_intel, legislative_intel, etc.
│
├── journal/                (2)   Immutable decision journal
│   └── log.py                     No updates, no deletes
│
├── knowledge/              (3)   Knowledge management
│   ├── loader.py                  .md knowledge doc loader
│   ├── selector.py                Context-relevant selection
│   └── tree.py                    Knowledge tree structure
│
├── llamacpp/               (2)   llama.cpp direct inference
│   └── client.py                  Nemotron models (port 8080)
│
├── llm/                    (3)   LLM routing layer
│   ├── router.py                  3-tier: LOCAL/REASON/ORACLE with fallback chains
│   └── protocol.py                LLM protocol definitions
│
├── migrations/             (6)   Alembic migrations
│   ├── env.py
│   └── versions/                  5 migration scripts
│
├── normalization/          (3)   Data normalization
│   ├── resolver.py                Multi-source conflict resolution
│   └── entity_map.py              SEED_MAPPINGS for entity resolution
│
├── ollama/                 (7)   Ollama integration (port 11434)
│   ├── client.py                  Ollama client (qwen2.5:7b)
│   ├── router.py                  Ollama routing
│   ├── reasoner.py                Reasoning engine
│   ├── market_briefing.py         Daily market briefings
│   ├── dealer_flow_briefing.py    Dealer flow analysis
│   └── celestial_briefing.py      Celestial briefings
│
├── oracle/                 (22)  Self-improving prediction loop
│   ├── engine.py                  Core oracle engine
│   ├── ensemble.py                5-model ensemble
│   ├── model_evolver.py           Dynamic weight evolution
│   ├── scoreboard.py              Performance tracking
│   ├── run_cycle.py               Prediction cycle runner
│   ├── calibration.py             Calibration
│   ├── firewall.py                Safety guardrails
│   ├── sanity_checker.py          Sanity checks
│   ├── claim_extractor.py         Claim extraction from reports
│   ├── claim_verifier.py          Claim verification
│   ├── citation_extractor.py      Source citation tracking
│   ├── publisher_gate.py          Publication gates
│   └── ...                        forecaster, psi_oracle, report, publish
│
├── orchestration/          (8)   Task orchestration
│   ├── event_bus.py               Pub/sub event system (14KB)
│   ├── llm_taskqueue.py           Task queue + execution (30KB)
│   ├── llm_task_workers.py        Task worker implementations (55KB)
│   ├── llm_task_models.py         Task models
│   ├── dispatch.py                Interactive dispatcher
│   ├── grid_worker.py             Background worker (12KB)
│   ├── integrate.py               Integration layer
│   └── reconcile.py               State reconciliation
│
├── payments/               (2)   Payment processing
│   └── x402.py                    x402 payment protocol
│
├── physics/                (8)   Market physics models
│   ├── dealer_gamma.py            Dealer gamma exposure
│   ├── momentum.py                Momentum/energy models
│   ├── news_energy.py             News energy impact
│   ├── waves.py                   Market wave analysis
│   ├── transforms.py              Physics transforms
│   └── conventions.py             Conventions + verify.py
│
├── pwa/                          React 18 + Vite PWA frontend
│   ├── src/                       51 views, 45 routes, Zustand state
│   └── public/                    Static assets
│
├── scripts/                (82)  Operational scripts
│   ├── hermes_operator.py         Hermes daemon operator
│   ├── run_full_pipeline.py       Full pipeline runner
│   ├── run_intelligence_cycles.py Intelligence cycle runner
│   ├── run_alpha_backtest.py      Alpha backtesting
│   ├── bulk_historical_pull.py    Historical data backfill
│   ├── import_icij_relationships.py  ICIJ data import
│   ├── security_audit.py          Security audit
│   ├── tao_miner.py               TAO mining (Bittensor)
│   └── ...                        70+ more scripts
│
├── server_log/             (4)   Server log management
│   ├── git_sink.py                Git-backed log storage
│   ├── inbox.py                   Log inbox
│   └── sanitizer.py               Log sanitization
│
├── store/                  (4)   PIT-correct query engine
│   ├── pit.py                     Point-in-time queries (DISTINCT ON, no lookahead)
│   ├── snapshots.py               Data snapshots
│   └── astrogrid.py               AstroGrid data store
│
├── strategy/               (2)   Trading strategy
│   └── engine.py                  Strategy execution engine
│
├── subnet/                 (10)  Distributed compute subnet
│   ├── distributed_compute.py     Compute coordination
│   ├── miner.py                   Compute miner
│   ├── validator.py               Task validation
│   ├── reputation.py              Miner reputation scoring
│   ├── sybil_detector.py          Sybil attack detection
│   ├── honeypot.py                Honeypot challenges
│   └── ...                        oauth, semantic_scorer, stake_verifier
│
├── tests/                  (76)  Test suite (1,148 tests)
│   ├── conftest.py                Shared fixtures
│   └── test_*.py                  76 test files covering all modules
│
├── timeseries/             (4)   Time series forecasting
│   ├── timesfm_forecaster.py      Google TimesFM
│   ├── autobnn.py                 AutoBNN (Bayesian neural nets)
│   └── _model_pool.py             Model pool management
│
├── trading/                (11)  Trade execution
│   ├── paper_engine.py            Paper trading engine
│   ├── hyperliquid.py             Hyperliquid DEX integration
│   ├── signal_executor.py         Signal → trade execution
│   ├── options_recommender.py     Options recommendation engine
│   ├── options_tracker.py         Options position tracking
│   ├── prediction_markets.py      Prediction market trading
│   ├── prediction_pmxt.py         PMXT predictions
│   ├── strategy151.py             Strategy 151 implementation
│   ├── wallet_manager.py          Crypto wallet management
│   └── circuit_breaker.py         Trading circuit breaker
│
├── utils/                  (1)   Utilities
│   └── ttl_cache.py               TTL cache implementation
│
├── validation/             (4)   Model validation
│   ├── gates.py                   Validation gates (must-pass)
│   ├── backtest.py                Walk-forward backtesting
│   └── execution_sim.py           Execution simulation
│
├── workflows/              (2)   Workflow system
│   ├── loader.py                  Workflow loader
│   ├── available/                 Available workflow definitions
│   └── enabled/                   Enabled workflows
│
├── docs/                         Documentation
│   ├── planning/                  Architecture planning docs
│   ├── audits/                    Audit reports
│   └── superpowers/               Superpowers specs
│
├── data/                         Local data files
├── output/                       Output artifacts
├── outputs/                      Generated outputs (briefings, images)
├── models/                       Model artifacts + cache
└── projects/                     Sub-projects
    └── grid-regime-discovery/     Regime discovery project
```

## LLM Stack

```
┌────────────────────────────────────────────────────┐
│                  llm/router.py                      │
│          3-tier: LOCAL / REASON / ORACLE             │
├────────────────────────────────────────────────────┤
│ LOCAL (cheap)    │ llamacpp Nemotron-Cascade-2      │
│ REASON (analysis)│ llamacpp Nemotron-Super-49B      │
│ ORACLE (heavy)   │ OpenRouter Claude Sonnet         │
├────────────────────────────────────────────────────┤
│ Providers:                                          │
│  - gemma/client.py    Gemma 3 27B QAT (128K, GPU)  │
│  - gemma/micro.py     GemmaMicroPool (CPU, 8082-84) │
│  - llamacpp/client.py Nemotron (port 8080)          │
│  - ollama/client.py   qwen2.5:7b (port 11434)      │
│  - hyperspace/        P2P inference layer            │
│ Cloud fallbacks: HF, Anthropic, OpenAI, OpenRouter  │
└────────────────────────────────────────────────────┘
```

## Data Flow

```
External APIs (90+ sources)
    │
    ▼
ingestion/ pullers → raw_series table
    │
    ▼
normalization/resolver.py → resolved_series (PIT-correct)
    │
    ▼
features/lab.py → feature_registry (1,281 features)
    │
    ├──────────────────────────────────┐
    ▼                                  ▼
store/pit.py (no lookahead)    intelligence/ (89 modules)
    │                                  │
    ▼                                  ▼
inference/live.py              actor_network, causation,
oracle/engine.py               sleuth, forensics, cross_ref
    │                                  │
    ├──────────────────────────────────┤
    ▼                                  ▼
trading/ (paper + live)        api/ (58 routers)
journal/ (immutable log)           │
                                   ▼
                              pwa/ (React frontend)
```

## Key Patterns

- **[[Base Puller|BasePuller]]**: All ingestion extends `ingestion/base.py` with `SOURCE_NAME`, `insert_raw_series()`
- **[[PIT Store|PIT-correct]]**: All queries via `store/pit.py` with `DISTINCT ON`, `assert_no_lookahead()`
- **3-tier LLM**: `get_llm(tier=Tier.LOCAL|REASON|ORACLE)` with fallback chains
- **[[Actor Network|Actor network]]**: Named actors with edges (wealth flows, influence, control)
- **[[Decision Journal|Immutable journal]]**: `journal/log.py` — decisions logged, never modified
- **Adapter pattern**: `intelligence/adapters/` bridges intelligence modules to API layer
- **[[Model Governance|Model governance]]**: `CANDIDATE → SHADOW → STAGING → PRODUCTION → RETIRED`

## Related Notes

- [[Overview]] — System [[architecture]] layers
- [[Module-Sizes]] — Module line counts and complexity
- [[Config-Map]] — All config variables mapped
- [[Data-Pipeline]] — Detailed data flow
- [[API-Endpoints-Master]] — All API routes
- [[ML-Inference]] — Model inference details
- [[Intelligence-Layer]] — Intelligence module details
- [[Orchestration-Layer]] — Task orchestration
