# GRID Python Module Catalog — Complete Reference

**Generated:** 2026-03-30  
**Repository:** `/home/grid/grid_v4/grid_repo/`  
**Total Modules:** 405 across 40+ directories

---

## Executive Summary

This document provides a comprehensive inventory of every Python module in the GRID trading intelligence platform. The system is organized into seven functional layers:

1. **Ingestion Layer** (104 modules) — 50+ data sources (macro, crypto, celestial, intelligence)
2. **Intelligence Layer** (46 modules) — [[Actor Network|Actor network]]s, [[Trust Scorer|trust scoring]], [[Causation|causation]] analysis
3. **Analysis Layer** (16 modules) — Correlation engines, hypothesis testing, sector mapping
4. **Trading & Execution** (11 modules) — [[Options Scanner|Options scanner]], signal executor, paper trading
5. **LLM & Reasoning** (22 modules) — [[Ollama]], [[Hyperspace]], [[TradingAgents]], Hyperspace P2P
6. **Inference & Models** (18 modules) — Live predictions, calibration, governance
7. **Core Infrastructure** (151 modules) — API routes, database, features, journal, workflows

---

## INGESTION LAYER (104 modules)

### Core Ingestion Framework

| Module | Status | Purpose |
|--------|--------|---------|
| **ingestion/__init__.py** | ACTIVE | Layer entry point |
| **ingestion/base.py** | ACTIVE | Base `BasePuller` class with retry logic, shared across all pullers |
| **ingestion/scheduler.py** | ACTIVE | Main scheduler: orchestrates daily/weekly/monthly pulls for 50+ sources |
| **ingestion/smart_scheduler.py** | ACTIVE | Runs only due/stale pullers per cycle (optimization layer) |

### Primary Data Sources (21 modules)

Core financial & economic data:

| Module | Data Source | Status |
|--------|-------------|--------|
| **yfinance_pull.py** | Yahoo Finance (equities, options, crypto) | ACTIVE |
| **fred.py** | Federal Reserve Economic Data (macro) | ACTIVE |
| **bls.py** | Bureau of Labor Statistics (employment) | ACTIVE |
| **edgar.py** | SEC EDGAR (10-K/10-Q filings) | ACTIVE |
| **options.py** | Yahoo Finance options chains | ACTIVE |
| **coingecko.py** | CoinGecko API (crypto prices, free) | ACTIVE |
| **dexscreener.py** | DexScreener (DEX trading data) | ACTIVE |
| **pumpfun.py** | Pump.fun (Solana token data) | ACTIVE |
| **price_fallback.py** | Fallback price puller when yfinance fails | DORMANT |
| **openbb_pipeline.py** | OpenBB (alternative financial data) | DORMANT |
| **crucix_bridge.py** | Crucix DuckDB → PostgreSQL bridge | ACTIVE |
| **sec_velocity.py** | SEC 8-K filing velocity (proprietary) | ACTIVE |
| **social_sentiment.py** | Reddit, Bluesky, Google Trends sentiment | ACTIVE |
| **web_scraper.py** | Multi-source RSS/web scraping | EXPERIMENTAL |
| **wiki_history.py** | Wikipedia daily events + RSS news | DORMANT |
| **crypto_bootstrap.py** | Bootstrap script for crypto data setup | DORMANT |
| **seed_v2.py** | Initial database seeding | DORMANT |

### Alternative Data (49 modules)

High-value alternative data sources in `ingestion/altdata/`:

**Sentiment & Market Structure:**
- `aaii_sentiment.py` — AAII individual investor sentiment (weekly)
- `alphavantage_sentiment.py` — Alpha Vantage news sentiment API
- `fear_greed.py` — Fear & Greed Index (crypto/stock sentiment)
- `google_trends.py` — Google search trends
- `social_attention.py` — Wikipedia article traffic attention
- `smart_money.py` — Reddit/FinTwit smart money signals (LLM-scored)

**Trading Activity & Flows:**
- `dark_pool.py` — [[Dark Pool|FINRA dark pool]] transparency data
- `congressional.py` — Congressional insider trading disclosures
- `insider_filings.py` — [[Insider Filings|SEC Form 4]] [[Insider Filings|insider filings]] with cluster detection
- `unusual_whales.py` — Unusual options flow detection
- `institutional_flows.py` — [[Institutional Flows|ETF flows]] + [[Institutional Flows|13F]] holdings
- `kalshi.py` — Kalshi prediction markets
- `prediction_odds.py` — [[Polymarket]] rapid probability shifts (fast-change detector)
- `prediction_pmxt.py` — Multi-platform prediction markets (pmxt SDK)
- `repo_market.py` — Repo rates + money market stress indicators

**Economic Indicators:**
- `ads_index.py` — Aruoba-Diebold-Scotti Business Conditions Index (daily)
- `baltic_dry.py` — [[Baltic Dry Index|Baltic Dry]] Index + shipping costs
- `cboe_indices.py` — CBOE VIX, put/call ratios, skew indices
- `cftc_cot.py` — [[CFTC COT|CFTC]] [[CFTC COT|Commitments of Traders]] (weekly positioning)
- `fed_liquidity.py` — Fed net liquidity equation (proprietary)
- `fed_speeches.py` — Federal Reserve communications
- `yield_curve_full.py` — Full US Treasury [[Yield Curve|yield curve]] daily

**Intelligence & Geopolitical:**
- `analyst_ratings.py` — Wall Street analyst buy/hold/sell counts
- `asset_registries.py` — FAA/Coast Guard luxury asset registries
- `campaign_finance.py` — FEC [[Campaign Finance|campaign finance]] + PAC contributions
- `export_controls.py` — BIS Entity List (export controls)
- `fara.py` — DOJ [[FARA]] foreign agent lobbying
- `foia_cables.py` — State Dept declassified diplomatic cables
- `gdelt.py` — [[GDELT]] geopolitical event data (event-level tone + tension)
- `gov_contracts.py` — USASpending.gov government contracts
- `hf_financial_news.py` — HuggingFace financial news multimodal
- `legislation.py` — Congress.gov bills, hearings, votes
- `lobbying.py` — Senate/House lobbying disclosure
- `news_scraper.py` — RSS financial news with LLM sentiment (free)
- `noaa_ais.py` — [[NOAA]] AIS vessel traffic data
- `nyfed.py** — NY Federal Reserve data
- `offshore_leaks.py` — ICIJ Panama/Pandora Papers database
- `opencorporates.py` — Global company registry
- `opportunity.py` — Opportunity Insights Economic Tracker
- `supply_chain.py` — Container shipping, ISM, leading indicators
- `uk_companies_house.py` — UK corporate registry
- `world_news.py` — WorldNewsAPI events

**Crypto-Native:**
- `discord_scanner.py` — Solana Discord sentiment analyzer
- `earnings_calendar.py` — Earnings dates from multiple sources
- `memecoin_classifier.py` — Solana memecoin signal classification
- `telegram_scanner.py` — Solana Telegram sentiment

### Celestial Data (6 modules)

Esoteric correlation analysis data in `ingestion/celestial/`:
- `lunar.py` — Lunar phases, illumination, nodes
- `planetary.py` — Planetary positions, aspects, retrograde
- `solar.py` — Solar activity (sunspots, CME)
- `chinese.py` — Chinese lunar calendar, Feng Shui dates
- `vedic.py` — Vedic (Jyotish) astrological positions
- `__init__.py` — Module init

### International Central Bank Data (14 modules)

Global central bank & statistical agency data in `ingestion/international/`:

| Module | Institution | Status |
|--------|-------------|--------|
| **ecb.py** | European Central Bank | ACTIVE |
| **bcb.py** | Banco Central do Brasil | ACTIVE |
| **bis.py** | Bank for International Settlements | ACTIVE |
| **imf.py** | IMF IFS + WEO databases | ACTIVE |
| **rbi.py** | Reserve Bank of India | ACTIVE |
| **mas.py** | Monetary Authority of Singapore | ACTIVE |
| **jquants.py** | Japan Exchange Group (J-Quants) | ACTIVE |
| **oecd.py** | OECD SDMX API | ACTIVE |
| **abs_au.py** | Australian Bureau of Statistics | DORMANT |
| **akshare_macro.py** | AKShare China macro | DORMANT |
| **edinet.py** | Japan FSA EDINET filings | DORMANT |
| **eurostat.py** | Eurostat bulk download | DORMANT |
| **kosis.py** | Korea Statistical Information Service | DORMANT |

### Trade & Economic Complexity (5 modules)

In `ingestion/trade/`:
- `comtrade.py` — UN Comtrade v2 bilateral trade flows
- `cepii.py` — CEPII BACI trade data
- `wiod.py` — World Input-Output Database
- `atlas_eci.py` — Harvard Atlas Economic Complexity Index

### Physical Economy Data (7 modules)

In `ingestion/physical/`:
- `dbnomics.py` — DBnomics aggregated CB data
- `euklems.py` — EU KLEMS industry productivity
- `ofr.py` — OFR Financial Stability Monitor
- `patents.py` — USPTO PatentsView innovation
- `usda_nass.py` — [[USDA]] crop/livestock data
- `viirs.py` — NASA VIIRS nighttime lights (economic activity proxy)

### ML Enrichment (2 modules)

In `ingestion/ml/`:
- `finbert_scorer.py` — FinBERT sentiment scoring pipeline

---

## INTELLIGENCE LAYER (46 modules)

The crown jewel: maps global financial power structure, tracks actors, validates sources.

### Core Intelligence Framework

| Module | Purpose | Status |
|--------|---------|--------|
| **trust_scorer.py** | Bayesian source trust scoring + convergence detection | ACTIVE |
| **actor_network.py** | 475+ named actors with wealth/power mapping | ACTIVE |
| **actor_discovery.py** | 250K+ scale automated actor discovery (3-degree BFS) | EXPERIMENTAL |
| **lever_pullers.py** | Identifies market-moving actors from all sources | ACTIVE |
| **source_audit.py** | Compares source accuracy + redundancy mapping | ACTIVE |
| **resolution_audit.py** | Multi-source conflict resolution supervisor | ACTIVE |

### Network Maps (7 modules)

Domain-specific actor & relationship networks:
- `energy_network.py` — Oil majors, OPEC+, renewables cartel
- `banking_network.py` — Central banks, major banks, payment systems
- `real_estate_network.py` — REITs, HK tycoons, UAE royals, Chinese developers
- `commodities_agriculture_network.py` — ABCD+ grain traders, mining giants, precious metals
- `media_network.py` — Disney, Comcast, Fox, Netflix, Meta, TikTok, Reuters, Bloomberg
- `defense_contractors.py` — Raytheon, Lockheed, General Dynamics + [[Supply Chain|supply chain]]
- `pharma_network.py` — Big Pharma, biotech, generics, distribution

### Intelligence Analysis (20 modules)

Domain-specific intelligence engines:
- `company_analyzer.py` — Deep company fundamental analysis
- `earnings_intel.py` — Earnings predictions + management signal analysis
- `news_intel.py` — News intelligence + narrative analysis
- `news_impact.py` — Attribution: which news moves which markets
- `legislative_intel.py` — Legislative trading detection + bill impact
- `gov_intel.py` — Government contract intelligence + insider overlap
- `export_intel.py` — Export control tracking + revenue impact
- `dollar_flows.py` — Normalize all signals into USD flow estimates
- `cross_reference.py` — "[[Cross Reference|Lie detector]]" for government statistics
- `postmortem.py` — Automated failure analysis for bad trades
- `causation.py` — Causal connection engine (lever → outcome)
- `deep_graph.py` — Deep graph traversal for relationship discovery
- `entity_resolver.py` — Entity name/ID disambiguation
- `event_sequence.py` — Timeline reconstruction of events
- `forensics.py` — Forensic analysis of suspicious patterns
- `global_levers.py` — Hierarchical model of world money flows
- `hypothesis_engine.py` — Hypothesis discovery from patterns
- `influence_network.py` — Crown Jewel: influence loops & leverage points
- `institutional_map.py` — Private credit, hedge funds, VCs, PE
- `pattern_engine.py` — Automated pattern detection
- `prediction_calibration.py** — Prediction market calibration
- `sleuth.py** — Investigative research engine
- `opsec.py** — Operations security audit logging

### Other Intelligence Modules
- `defi_protocols.py` — DeFi protocol analysis + composability risk
- `market_diary.py** — Automated daily market diary
- `rag.py** — Retrieval-augmented generation intelligence system
- `swf_network.py** — Sovereign Wealth Fund intelligence
- `tech_monopoly_network.py** — Big Tech monopoly power mapping
- `trend_tracker.py** — Divergence analysis for market trends
- `source_trust_config.py** — Trust scorer configuration

---

## ANALYSIS LAYER (16 modules)

Research & hypothesis engines.

### Core Analysis

| Module | Purpose | Status |
|--------|---------|--------|
| **astro_correlations.py** | Celestial-market correlation engine (statistical) | ACTIVE |
| **backtest_scanner.py** | Cross-asset lead/lag backtest scanner | ACTIVE |
| **hypothesis_tester.py** | Hypothesis backtesting + validation | ACTIVE |
| **flow_thesis.py** | Flow knowledge base (Fed, dealer gamma, vanna, etc.) | ACTIVE |
| **flow_aggregator.py** | Aggregates dollar flows by sector/actor/time | ACTIVE |
| **capital_flows.py** | Deep research engine for capital flow questions | ACTIVE |

### Infrastructure Analysis
- `market_universe.py` — Comprehensive S&P 500 GICS mapping
- `sector_map.py` — Sector → subsector → actor mapping with influence weights
- `research_agent.py` — Autonomous gap analyzer + hypothesis generator
- `ephemeris.py` — Pure-math planetary position calculator (Copernicus module)
- `prompt_optimizer.py` — Feature selection via [[Orthogonality Audit|orthogonality]] analysis
- `taxonomy_audit.py` — Signal taxonomy audit engine
- `money_flow.py` — Global money flow hierarchical map
- `vol_surface.py` — Volatility surface engine
- `viz_intelligence.py** — Intelligence visualization support

---

## TRADING & EXECUTION (11 modules)

### Options Intelligence
| Module | Purpose | Status |
|--------|---------|--------|
| **discovery/options_scanner.py** | 7-signal mispricing detector | ACTIVE |
| **trading/options_recommender.py** | Trade recommendation engine (strike/expiry/Kelly) | ACTIVE |
| **trading/options_tracker.py** | Outcome tracking + self-improvement loop | ACTIVE |

### Signal Execution & Paper Trading
- `trading/signal_executor.py` — Paper trading signal executor (hourly during market hours)
- `trading/paper_engine.py` — Paper trading engine + position tracking
- `trading/circuit_breaker.py` — Strategy-level circuit breaker (kill switch)

### Real Exchanges & Prediction Markets
- `trading/hyperliquid.py` — Hyperliquid perpetual trading integration
- `trading/prediction_pmxt.py` — Unified prediction market trader (pmxt SDK)
- `trading/prediction_markets.py` — [[Polymarket]] + Kalshi integration
- `trading/wallet_manager.py` — Multi-wallet management (EXCH-04)
- `trading/strategy151.py` — Kakushadze & Serur (2018) 151 strategies

### Regime & Discovery
- `discovery/clustering.py` — Unsupervised [[Regime Discovery|regime clustering]] engine
- `discovery/orthogonality.py` — [[Orthogonality Audit|Orthogonality audit]] + feature independence

---

## LLM & REASONING (22 modules)

### Ollama Local Inference (7 modules)

| Module | Purpose | Status |
|--------|---------|--------|
| **ollama/client.py** | Unified LLM client (OpenAI, Ollama, llama.cpp) | ACTIVE |
| **ollama/market_briefing.py** | Hourly market briefing generation | ACTIVE |
| **ollama/dealer_flow_briefing.py** | Dealer flow narrative synthesis | ACTIVE |
| **ollama/celestial_briefing.py** | Celestial narrative synthesis | EXPERIMENTAL |
| **ollama/reasoner.py** | Ollama-powered reasoning layer | ACTIVE |
| **ollama/router.py** | Dual-LLM task router | ACTIVE |

### TradingAgents Framework (9 modules)

Multi-agent LLM deliberation system in `agents/`:

| Module | Purpose | Status |
|--------|---------|--------|
| **agents/runner.py** | Main orchestration runner | ACTIVE |
| **agents/context.py** | GRID context builder (regime-aware) | ACTIVE |
| **agents/config.py** | LLM provider configuration | ACTIVE |
| **agents/personas.py** | Investor persona system | ACTIVE |
| **agents/adapter.py** | Decision output adapter → journal | ACTIVE |
| **agents/backtest.py** | Agent decision backtesting | EXPERIMENTAL |
| **agents/scheduler.py** | Scheduled agent runs (weekdays 5 PM) | ACTIVE |
| **agents/progress.py** | WebSocket progress broadcasting | ACTIVE |

### Hyperspace P2P (6 modules)

Distributed inference via [[Hyperspace]] P2P network in `hyperspace/`:
- `client.py` — [[Hyperspace]] API client
- `monitor.py` — Node monitoring
- `embeddings.py` — Semantic embedding layer
- `reasoner.py` — LLM-assisted reasoning layer
- `research_agent.py` — Research agent definition

### LLM Infrastructure (2 modules)
- `llm/protocol.py` — LLM client protocol interface
- `llamacpp/client.py` — [[llama.cpp]] server client

---

## INFERENCE & MODELS (18 modules)

### Live Inference
| Module | Purpose | Status |
|--------|---------|--------|
| **inference/live.py** | Live prediction engine (PIT-correct) | ACTIVE |
| **inference/calibration.py** | Probability calibration scoring | ACTIVE |
| **inference/ensemble.py** | Weighted ensemble classifier | ACTIVE |

### Model Training & Testing
- `inference/training.py` — [[PIT Store|PIT-correct]] training pipeline
- `inference/trained_models.py` — Trained model abstractions
- `inference/tuning.py` — Parameter tuning
- `inference/failure_analysis.py` — Failure regime analysis
- `inference/trade_logger.py` — Execution-granularity trade logging
- `inference/circuit_breaker.py` — Inference kill switch

### Governance & Oracle
- `governance/registry.py` — [[Model Governance|Model lifecycle]] state machine (CANDIDATE → SHADOW → STAGING → PROD)
- `oracle/engine.py` — Oracle prediction engine
- `oracle/calibration.py` — [[Oracle Calibration|Oracle calibration]] metrics
- `oracle/run_cycle.py` — One-shot oracle cycle (score → evolve → predict → report)
- `oracle/scoreboard.py` — Oracle scoreboard
- `oracle/publish.py` — Comparable record publication contract
- `oracle/report.py` — Prediction digest + scorecard
- `oracle/astrogrid_universe.py` — [[AstroGrid]] scoring universe definitions

---

## API & ROUTING (45 modules)

### Core API (4 modules)
| Module | Purpose |
|--------|---------|
| **api/main.py** | FastAPI application entry point |
| **api/auth.py** | JWT authentication + role-based access control |
| **api/dependencies.py** | Shared FastAPI dependencies |

### API Routers (34 modules)

Comprehensive REST API organized by domain:

**Core Intelligence & Prediction**
- `routers/oracle.py` — Predictions, scoreboard, latest cycle
- `routers/intelligence.py` — [[Cross Reference|Cross-reference]] [[Cross Reference|lie detector]]
- `routers/intel.py` — Core paid API (trust scores, actor data)
- `routers/signals.py` — Live signals endpoints

**Market Analysis**
- `routers/flows.py` — Sector flow analysis (rotation, momentum)
- `routers/derivatives.py` — Dealer flow intelligence
- `routers/physics.py` — Market physics endpoints
- `routers/regime.py** — Regime state endpoints
- `routers/celestial.py** — Celestial signals endpoints
- `routers/astrogrid.py** — AstroGrid celestial intelligence

**Research & Discovery**
- `routers/discovery.py` — Discovery engine endpoints
- `routers/search.py` — Universal search across all registries
- `routers/knowledge.py` — Knowledge tree endpoints
- `routers/associations.py` — Feature association discovery

**Trading & Execution**
- `routers/trading.py` — Paper trading, Hyperliquid perps, prediction markets
- `routers/options.py` — [[Options Scanner|Options scanner]] + recommendations
- `routers/strategy.py` — Regime-independent strategy overlay
- `routers/backtest.py` — Backtesting + paper trade endpoints

**Earnings & Fundamentals**
- `routers/earnings.py` — Earnings calendar + predictions
- `routers/signals.py` — Signal-level endpoints

**Models & Analytics**
- `routers/models.py` — [[Model Governance|Model registry]] endpoints
- `routers/model_comparison.py` — Model drift monitoring
- `routers/journal.py` — [[Decision Journal|Decision journal]] endpoints
- `routers/snapshots.py** — Analytical snapshot queries

**LLM & Chat**
- `routers/chat.py` — Conversational chat endpoint
- `routers/ollama.py` — LLM integration endpoints
- `routers/agents.py` — [[TradingAgents]] orchestration

**Operations**
- `routers/config.py` — System configuration endpoints
- `routers/system.py` — Health + status endpoints
- `routers/notifications.py` — Push notification endpoints
- `routers/watchlist.py` — Watchlist management
- `routers/workflows.py` — Workflow management
- `routers/tradingview.py` — TradingView webhook integration
- `routers/viz.py` — Visualization API

### API Schemas (7 modules)

Pydantic request/response models:
- `schemas/auth.py` — Auth schemas
- `schemas/journal.py` — Journal schemas
- `schemas/models.py` — [[Model Governance|Model registry]] schemas
- `schemas/regime.py` — Regime schemas
- `schemas/system.py` — System status schemas
- `schemas/watchlist.py` — Watchlist schemas

---

## CORE INFRASTRUCTURE (151 modules)

### Data Store & Queries
| Module | Purpose | Status |
|--------|---------|--------|
| **store/pit.py** | Point-in-time query engine (PostgreSQL DISTINCT ON) | ACTIVE |
| **store/snapshots.py** | Analytical snapshot persistence | ACTIVE |
| **store/astrogrid.py** | AstroGrid persistence helpers | ACTIVE |

### Data Resolution & Normalization
- `normalization/resolver.py` — Multi-source [[Conflict Resolution|conflict resolution]]
- `normalization/entity_map.py` — Entity name/ID disambiguation
- `normalization/__init__.py` — Normalization layer init

### Feature Engineering
- `features/lab.py` — Feature transformation engine (standardization, winsorization, scaling)
- `features/alpha101.py` — WorldQuant 101 Formulaic Alphas
- `features/importance.py` — Feature importance tracking
- `features/registry.py` — Feature registry query interface

### Validation & Gates
- `validation/backtest.py` — [[Walk-Forward Backtesting|Walk-forward backtesting]] engine
- `validation/execution_sim.py` — Execution simulation layer
- `validation/gates.py` — [[Walk-Forward Backtesting|Promotion gate]] enforcement (CANDIDATE → PROD)

### Immutable Journal
- `journal/log.py` — Immutable [[Decision Journal|decision journal]] (no updates/deletes)
- Can't accidentally destroy trade history

### Knowledge Management
- `knowledge/loader.py` — Consolidated knowledge loading + injection
- `knowledge/selector.py` — TF-IDF + [[Orthogonality Audit|orthogonality]] doc selection
- `knowledge/tree.py` — Knowledge tree storage (Q&A interactions)

### Orchestration & Workflows
- `orchestration/dispatch.py` — Task dispatcher (generates prompts)
- `orchestration/integrate.py` — Multi-model integration
- `orchestration/llm_taskqueue.py` — LLM task queue (keeps Qwen grinding)
- `orchestration/reconcile.py` — Contribution reconciliation
- `workflows/loader.py` — Workflow loader

### Outputs & Logging
- `outputs/llm_logger.py` — Timestamped markdown logger
- `outputs/insight_scanner.py` — Periodic LLM insight scanner
- `server_log/git_sink.py` — Loguru sink to git-tracked JSONL
- `server_log/inbox.py` — Operator inbox (git-based comms)
- `server_log/sanitizer.py` — Sanitize logs (scrub secrets)

### Root Configuration
- **config.py** — Pydantic settings (all config via env vars)
- **db.py** — Database access layer
- **cli.py** — CLI workflow management
- **dashboard.py** — Standalone dashboard (port 8080)
- **mcp_server.py** — Model Context Protocol interface

---

## OPERATIONAL SCRIPTS (56 modules)

Utility and operational scripts in `scripts/`:

### Data Loading & Migration
- `migrate_and_load.py` — Unified migration + bulk loading
- `load_yfinance.py`, `load_ticker_deep.py`, `load_wave2.py`, `load_wave3.py`, `load_more_data.py` — Historical data loading
- `load_alt_data.py` — Alternative data loading
- `bulk_historical_pull.py` — Bulk download from trusted sources
- `bulk_resolve.py` — Fast bulk resolver (initial population)
- `seed_astrogrid_prediction_corpus.py` — [[AstroGrid]] corpus seeding
- `parse_datasets.py` — Unified dataset parser
- `parse_edgar.py` — SEC [[EDGAR]] XBRL parsing
- `parse_eia.py` — [[EIA]] energy data parsing
- `parse_gdelt.py` — [[GDELT]] event CSV parsing
- `download_pushshift.py` — Pushshift Reddit historical dump download

### Feature & Backtest
- `compute_derived_features.py` — Fill missing features (no new ingestion)
- `compute_coordinator.py` — BOINC-inspired distributed compute
- `fill_missing_features.py` — Fill gaps via direct API calls
- `train_regime_model.py` — [[Regime Discovery|Regime clustering]] model training
- `baseline_predictions.py` — Baseline prediction generation

### Data Integration & Bridging
- `bridge_crucix.py` — Crucix DuckDB → [[PostgreSQL]] bridge
- `bridge_to_pg.py` — Generic bridge to [[PostgreSQL]]
- `import_icij_relationships.py` — Import 3.3M ICIJ relationships

### Research & Analysis
- `ai_analyst.py` — Generate theses using local LLM + GRID data
- `autoresearch.py` — Autonomous hypothesis generation, testing, refinement
- `run_astrogrid_learning_loop.py` — [[AstroGrid]] scoring/backtest/review cycle
- `run_full_pipeline.py` — End-to-end pipeline execution
- `run_pipeline.py` — Full data ingest pipeline
- `astrogrid_web_smoke.py` — [[AstroGrid]] smoke tests

### Crypto-Specific
- `generate_crypto_predictions.py` — Generate predictions for 24/7 scoring
- `score_crypto_predictions.py` — Score against live Crypto.com prices
- `queue_crypto_backlog.py` — Queue crypto research tasks

### LLM & Inference
- `drain_backlog.py` — LLM task backlog processor (daemon)
- `queue_100k.py`, `queue_massive_backlog.py` — Queue research tasks for Qwen
- `run_finbert.py` — FinBERT sentiment scoring CLI

### Operations & Maintenance
- `hermes_operator.py` — [[Hermes Scheduler|Hermes]] main operator
- `daily_digest.py` — Daily email digest (errors + UX audit + status)
- `notify.py` — Email notification module
- `signal_taxonomy.py` — Signal taxonomy mapping
- `sources_expanded.py` — Expanded source listing
- `ux_auditor.py` — UX testing + improvement
- `edge_ui.py` — Human-in-the-loop edge worker with web UI
- `export_astrogrid_local_data.py` — [[AstroGrid]] data export
- `activate_v2_mappings.py` — Activate V2 [[Entity Map|entity map]]pings
- `assimilator.py** — Human LLM response assimilation
- `fix_model_eligible.py` — Fix model_eligible flags
- `scrape_missing_features.py` — Background feature scraper
- `pull_intraday.py` — Intraday price bar puller
- `pull_options.py` — Daily options chain puller
- `auto_regime.py` — Automated regime detection
- `backfill_celestial.py`, `backfill_celestial_ephemeris.py` — Celestial data backfill
- `setup_auth.py** — Password hash generation
- `tao_miner.py** — Bittensor TAO miner (inference earning)
- `worker.py** — Distributed compute worker

---

## SPECIALIZED MODULES

### Subnet / Bittensor Integration (10 modules)
Modules for `subnet/`:
- `miner.py` — Bittensor subnet miner
- `validator.py` — Bittensor subnet validator
- `distributed_compute.py` — Distributed compute engine
- `dynamic_scorer.py` — Dynamic scoring
- `semantic_scorer.py` — Semantic scoring
- `reputation.py` — Bayesian reputation system
- `stake_verifier.py` — Stake verification
- `sybil_detector.py` — Sybil detection
- `honeypot.py` — Honeypot calibration
- `oauth_miner.py` — Mobile miner via OAuth

### Alerts & Notifications (5 modules)
Modules for `alerts/`:
- `email.py` — Premium newsletter emails
- `hundredx_digest.py` — 100x bundled digest (every 4h)
- `push_notify.py` — Web push notifications
- `scheduler.py` — Alert scheduler

### Backtest & Paper Trading (4 modules)
Modules for `backtest/`:
- `engine.py` — Pitch backtest engine
- `paper_trade.py` — Live paper trading system
- `charts.py` — Backtest chart generation

### Physics & Market Dynamics (8 modules)
Modules for `physics/`:
- `dealer_gamma.py` — [[Dealer Gamma|GEX]], [[Dealer Gamma|vanna]], charm, [[Dealer Gamma|gamma walls]]
- `momentum.py` — News momentum analysis
- `news_energy.py` — News energy decomposition
- `waves.py` — Wave-based pipeline execution
- `transforms.py` — Physics-inspired market transforms
- `verify.py` — Market physics verification
- `conventions.py` — Financial convention locking

### Other Specialized
- `strategy/engine.py` — Regime-to-strategy mappings
- `workflows/__init__.py` — Declarative workflow system
- `artifacts/crypto_exchange_network_intel.py` — Crypto exchange report
- `data/sp500_intel_profiles.py` — S&P 500 intelligence profiles (semi/tech)

---

## KEY STATISTICS

### Module Distribution

| Layer | Count | % |
|-------|-------|-----|
| Ingestion | 104 | 25.7% |
| Scripts | 56 | 13.8% |
| Intelligence | 46 | 11.4% |
| API + Routers + Schemas | 45 | 11.1% |
| Analysis | 16 | 4.0% |
| Trading | 11 | 2.7% |
| Subnet | 10 | 2.5% |
| Inference | 10 | 2.5% |
| Agents | 9 | 2.2% |
| Oracle | 8 | 2.0% |
| Physics | 8 | 2.0% |
| Hyperspace | 6 | 1.5% |
| Other | 70 | 17.3% |

### Data Source Coverage

**50+ data sources across 6 continents:**
- US: Fed, Treasury, [[BLS]], SEC, [[CFTC COT|CFTC]], FINRA, [[NOAA]], [[USDA]], USPTO, Congress
- Europe: [[ECB]], BIS, Eurostat, UK Companies House
- Asia-Pacific: BOJ, PBOC, RBI, MAS, RBA, J-Quants, KOSIS
- LatAm: BCB, IMF
- Global: BIS, OECD, UN Comtrade, [[GDELT]], IMF, World Bank

**Alternative Data:**
- Congressional/insider trading (Form 4)
- [[Dark Pool|Dark pool]] activity (FINRA ATS)
- Prediction markets ([[Polymarket]], Kalshi)
- Celestial/esoteric (lunar, planetary, Vedic)

---

## INTEGRATION CHECKLIST FOR NEW WORK

When planning integrations, ensure you understand:

- [ ] **Data dependency:** Which pullers feed features? (ingestion → store/pit)
- [ ] **PIT correctness:** All historical queries must use [[PIT Store|store/pit.py]]
- [ ] **[[Trust Scorer|Trust scoring]]:** Does signal come from a trust-evaluated source?
- [ ] **[[Actor Network|Actor network]]:** Is the actor in [[Actor Network|actor_network.py]]? Should they be?
- [ ] **Journal logging:** Is the decision logged to [[Decision Journal|immutable journal]]?
- [ ] **API exposure:** Should this be a new router, or extend existing?
- [ ] **[[Feature Engineering|Feature engineering]]:** Are derived features in [[Feature Engineering|features/lab.py]]?
- [ ] **Backtesting:** Can this be tested in validation/backtest.py?
- [ ] **Inference path:** Does the oracle/live engine evaluate this?

---

## CRITICAL PATHS (For Reference)

**Data → Prediction:**
1. ingestion/{source}.py → [[Raw Series Table|raw_series]] table
2. [[Conflict Resolution|normalization/resolver.py]] → conflict resolution
3. [[PIT Store|store/pit.py]] → [[PIT Store|PIT-correct]] historical view
4. [[Feature Engineering|features/lab.py]] → feature transforms
5. [[Live Inference|inference/live.py]] → live prediction
6. [[Oracle Engine|oracle/engine.py]] → oracle prediction
7. [[Decision Journal|journal/log.py]] → immutable decision log

**Decision → Execution:**
1. [[Oracle Engine|oracle/engine.py]] → signal generation
2. trading/signal_executor.py → paper trade execution
3. [[Decision Journal|journal/log.py]] → trade logging
4. [[Options Tracker|trading/options_tracker.py]] → outcome tracking
5. intelligence/[[Postmortem|postmortem.py]] → failure analysis
6. intelligence/[[Trust Scorer|trust_scorer.py]] → source score update

---

Generated by GRID Module Catalog Generator
Last Updated: 2026-03-30 ([[architecture]] notes added 2026-04-13)

---

## Known Architecture Issues (added 2026-04-13)

Discovered during the cryexc-review session (branch `claude/review-cryexc-app-T2icl`). Full context in `docs/planning/SESSION-ROADMAP-2026-04-13.md`.

### Existing modules NOT wired in (quick wins)

| Module | Status | What's needed |
|---|---|---|
| `analysis/vol_surface.py` | Built — SVI parameterization, per-expiry skew, butterfly arbitrage checks | Wire into `discovery/options_scanner.py` and `trading/options_recommender.py`; add LEAPS-specific features (1Y ATM IV, long-dated skew, term structure slope, div risk premium, ρ) |
| `physics/dealer_gamma.py` (vanna & charm) | Computed at lines 248-250, reported at lines 205-206 | Wire into prediction scoring — currently "measured but not actionable" |
| `intelligence/earnings_transcript_analyzer.py` | Tone + Q&A split + guidance + risk phrases via word matching | Extend with quarter-over-quarter delta + semantic embeddings |
| `intelligence/prediction_calibration.py` | Brier / reliability tracking | Add persistence + per-horizon + drift alerts |
| `intelligence/hypothesis_engine.py` | LLM-driven hypothesis generation with 11 kill criteria | Wire into forensics (validate vs actual moves) and cross_reference (macro reality check) |

### Schema gaps in the oracle

The `oracle_predictions` table (`oracle/engine.py:224-250`) is missing the following fields — needed for horizon-conditional modeling and catalyst-aware scoring:

- `horizon` (days to resolution — note: `timeseries_forecasts` already has this field at `engine.py:295`, but it's not plumbed into `oracle_predictions`)
- `as_of_date` (decision timestamp for [[Walk-Forward Backtesting|walk-forward]] PIT backtesting)
- `resolution_date` (when the prediction will be scored)
- `catalyst_type` (earnings / FOMC / FDA / macro_release / none)
- `catalyst_proximity` (days to nearest relevant catalyst)

Additionally, `oracle/engine.py:893` calls `self._next_monthly_expiry()` for every prediction — the oracle hardcodes a single 3rd-Friday expiry across all horizons.

### Crude dealer positioning assumption

`physics/dealer_gamma.py:19-30` assumes dealers are **net short every option** (short calls → negative gamma; short puts → positive gamma). This is the "SpotGamma-style" assumption. No actual market-maker filing data is used. A flow-based inference (tracking customer flow direction) would meaningfully improve single-name [[Dealer Gamma|GEX]] accuracy.

### Calibration not persisted

`oracle/calibration.py:66-195` computes `CalibrationReport` on-demand from the `oracle_predictions` table and returns it. It is **never stored**. This means:

- No calibration drift detection over time
- No per-horizon calibration curves
- No per-regime calibration tracking
- Rerunning the same calibration query pays the full cost each time

### Feature importance is global-only

`features/importance.py` tracks permutation importance, regime correlation, and rolling stability — but not:

- Per-horizon importance (a feature high-signal at 5d may be noise at 365d)
- Per-regime importance (feature importance shifts across regimes)
- Interaction importance between features

### Location bugs (CLAUDE.md historically listed incorrectly)

- `flow_thesis.py` is in `analysis/`, not `intelligence/`. Patched in CLAUDE.md 2026-04-13.
- `flow_aggregator.py` is in `analysis/`, not `intelligence/`. Patched in CLAUDE.md 2026-04-13.

### Genuinely missing (not duplicates)

These capabilities do not exist anywhere in the 405-module codebase as of 2026-04-13 and would be net-new builds:

- Horizon-conditional oracle with per-horizon calibration
- Unified catalyst calendar (FOMC / CPI / NFP / PCE / ISM / FDA AdCom / Treasury auctions / corporate actions)
- Shapley-value attribution per prediction (grep finds zero SHAP references)
- Granger / transfer entropy / mutual information discovery engine
- HMM-style regime transition matrix (`discovery/clustering.py` does discovery but treats regimes as static labels)
- Market-implied probability comparator (for the "edge = |GRID_p − market_p|" principle)
- Causal DAG encoding + do-calculus reasoning layer
- Synthetic control event-study engine
- Pattern library with historical base rates (analog matching)
- Adversarial LLM red-team loop per prediction
- Ensemble disagreement as a meta-feature

See `docs/planning/SESSION-ROADMAP-2026-04-13.md#7-confirmed-genuine-gaps-not-duplicates` for full analysis.
