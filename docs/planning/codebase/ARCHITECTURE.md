# Architecture

## Pattern

GRID follows a **pipeline architecture** with a shared PostgreSQL database as the integration backbone. Data flows through a series of transformation stages — ingestion, normalization/resolution, PIT-correct storage, feature engineering, unsupervised discovery, inference, and decision journaling. Each stage is implemented as an independent Python module with its own domain class, all communicating through the shared database via SQLAlchemy engine passing.

A secondary **layered architecture** governs the API: thin FastAPI route handlers delegate to domain modules, which in turn depend on shared abstractions (`PITStore`, `FeatureLab`, `DecisionJournal`, `ModelRegistry`). A model governance state machine (`CANDIDATE -> SHADOW -> STAGING -> PRODUCTION -> FLAGGED -> RETIRED`) gates promotions with validation checks.

The system also has a **multi-agent deliberation** layer (TradingAgents) that consumes GRID regime context and produces trading recommendations via LLM-driven debate, plus local LLM inference via Hyperspace/Ollama/llama.cpp for market analysis.

## Entry Points

### Primary: FastAPI Server
- `grid/api/main.py` — FastAPI application serving REST API at `/api/v1/*`, WebSocket at `/ws`, and the PWA at `/`
- Started via: `python -m uvicorn api.main:app --reload --port 8000`
- On startup, launches ingestion schedulers (v1 and v2) as background daemon threads and the agent scheduler

### Secondary: CLI
- `grid/cli.py` — Workflow management CLI with commands: `list`, `enable`, `disable`, `run`, `validate`, `status`, `schedule`, `waves`, `verify`, `conventions`
- Dispatches to domain modules based on workflow group (ingestion, features, discovery, physics, validation, governance)

### Scripts
- `grid/scripts/run_pipeline.py` — Legacy DuckDB-based full ingest pipeline (v4 era, predates PostgreSQL migration)
- `grid/scripts/auto_regime.py` — Standalone regime detection
- `grid/scripts/worker.py` — Background task worker
- `grid/scripts/setup_auth.py` — Auth credential setup
- Various data loaders: `load_yfinance.py`, `load_wave2.py`, `load_wave3.py`, `load_alt_data.py`, `load_more_data.py`, `load_ticker_deep.py`
- Various parsers: `parse_edgar.py`, `parse_eia.py`, `parse_gdelt.py`
- Shell scripts: `bootstrap.sh`, `build_llamacpp.sh`, `deploy_all.sh`, `overnight_bulk.sh`, `start_llamacpp.sh`

### Module `__main__` blocks
Most domain modules (`store/pit.py`, `journal/log.py`, `governance/registry.py`, `inference/live.py`, `normalization/resolver.py`, `db.py`, `config.py`) include `if __name__ == "__main__"` blocks for standalone testing and quick diagnostics.

## Data Flow

```
                                    ┌─────────────────┐
                                    │  37+ Data Sources│
                                    │  FRED, BLS, ECB, │
                                    │  yfinance, EDGAR,│
                                    │  DexScreener ... │
                                    └────────┬────────┘
                                             │
                                    ┌────────▼────────┐
                              1.    │    Ingestion     │
                                    │ ingestion/*.py   │
                                    │ scheduler.py (v1)│
                                    │ scheduler_v2.py  │
                                    └────────┬────────┘
                                             │ raw_series table
                                    ┌────────▼────────┐
                              2.    │  Normalization   │
                                    │ resolver.py      │
                                    │ entity_map.py    │
                                    └────────┬────────┘
                                             │ resolved_series table
                                    ┌────────▼────────┐
                              3.    │   PIT Store      │
                                    │ store/pit.py     │
                                    │ PITStore class   │
                                    └────────┬────────┘
                                             │ PIT-correct DataFrames
                                    ┌────────▼────────┐
                              4.    │    Features      │
                                    │ features/lab.py  │
                                    │ features/        │
                                    │   registry.py    │
                                    └────────┬────────┘
                                             │ derived feature vectors
                              ┌──────────────┼──────────────┐
                              │              │              │
                     ┌────────▼───────┐ ┌────▼─────┐ ┌─────▼────────┐
               5.    │   Discovery    │ │Validation│ │   Inference  │
                     │ clustering.py  │ │ gates.py │ │  live.py     │
                     │orthogonality.py│ │backtest.py│ │             │
                     └────────────────┘ └────┬─────┘ └──────┬──────┘
                                             │              │
                                    ┌────────▼────────┐     │
               6.    │  Governance    │     │
                                    │ registry.py     │◄────┘
                                    │ ModelRegistry    │
                                    └────────┬────────┘
                                             │
                                    ┌────────▼────────┐
               7.    │    Journal     │
                                    │ journal/log.py  │
                                    │ DecisionJournal │
                                    └────────┬────────┘
                                             │
                              ┌──────────────┼──────────────┐
                              │              │              │
                     ┌────────▼───────┐ ┌────▼─────┐ ┌─────▼────────┐
                     │   Agents       │ │ Ollama   │ │  Hyperspace  │
                     │ runner.py      │ │ client.py│ │  client.py   │
                     │ (TradingAgents)│ │ reasoner │ │  reasoner    │
                     └────────────────┘ └──────────┘ └──────────────┘
```

### Stage Details

1. **Ingestion** (`grid/ingestion/`): 37+ data pullers organized into subdirectories (`international/`, `trade/`, `physical/`, `altdata/`). Each puller writes to `raw_series` with `observation_date`, `release_date`, and `pull_timestamp`. Two schedulers run: `scheduler.py` (v1, FRED/yfinance/BLS/EDGAR) and `scheduler_v2.py` (international/trade/physical/altdata). Every puller follows the same pattern: `_resolve_source_id()` to get/create the source_catalog entry, then batch insert into `raw_series`.

2. **Normalization** (`grid/normalization/`): `resolver.py` groups `raw_series` by `(series_id, obs_date)`, selects the highest-priority source value, and detects conflicts (>0.5% divergence). `entity_map.py` maps raw series identifiers (e.g. "T10Y2Y", "YF:^GSPC:close") to canonical `feature_registry` names using hardcoded seed mappings plus fuzzy matching. Output goes to `resolved_series`.

3. **PIT Store** (`grid/store/pit.py`): The `PITStore` class enforces no-lookahead constraints via PostgreSQL `DISTINCT ON` queries. Two vintage policies: `FIRST_RELEASE` (earliest vintage per obs_date, for backtests) and `LATEST_AS_OF` (latest revision available at as_of_date, for live inference). `assert_no_lookahead()` validates every result set.

4. **Features** (`grid/features/lab.py`): `FeatureLab` computes derived features (z-scores, rolling slopes, lagged changes, ratios, spreads) from PIT-correct base data. `features/registry.py` provides read-only query access to `feature_registry` metadata. Transformations are vectorized with pandas/numpy/scipy.

5. **Discovery** (`grid/discovery/`): `clustering.py` runs PCA + multiple clustering algorithms (GMM, KMeans, Agglomerative) to discover market regimes. `orthogonality.py` audits feature independence and computes true dimensionality.

6. **Governance** (`grid/governance/registry.py`): `ModelRegistry` enforces the state machine (`CANDIDATE -> SHADOW -> STAGING -> PRODUCTION -> FLAGGED -> RETIRED`) with gate checks from `validation/gates.py`. Only one PRODUCTION model per layer (REGIME/TACTICAL/EXECUTION) enforced by unique partial index.

7. **Journal** (`grid/journal/log.py`): `DecisionJournal` is append-only. Every inference result is logged with full provenance (model, features, confidence, recommendation). Outcomes can be recorded later but immutable fields are protected by a PostgreSQL trigger (`enforce_journal_immutability`).

### DuckDB — Read-Only Mirror (Legacy)

DuckDB (`/data/grid/duckdb/grid.duckdb`) is a **read-only historical archive**, NOT the primary datastore. PostgreSQL is authoritative for all live data, PIT queries, and API serving.

**Current role:**
- Historical data archive from the pre-PostgreSQL v4 era
- Read-only source for migration scripts that bridge data into PostgreSQL
- Crucix OSINT data is bridged from a separate DuckDB into PostgreSQL via `scripts/bridge_crucix.py`

**Migration scripts (DuckDB → PostgreSQL):**
- `scripts/bridge_to_pg.py` — Migrates DuckDB time series into `raw_series`/`resolved_series`
- `scripts/migrate_and_load.py --duckdb` — Migrates hypotheses, flywheel scores, feature metadata
- `scripts/bridge_crucix.py` — Bridges Crucix DuckDB alerts/events into GRID PostgreSQL

**Rules:**
- Never write to DuckDB from the live system — it is frozen/archive-only
- Never query DuckDB for live inference, PIT lookups, or API responses
- All new data goes directly to PostgreSQL via the ingestion pipeline
- If you find code reading from DuckDB at runtime (outside migration scripts), it is a bug

### LLM Layer (parallel, optional)
- **Agents** (`grid/agents/`): TradingAgents multi-agent framework. `runner.py` orchestrates: fetch GRID context -> inject into prompts -> run multi-agent deliberation (bull/bear debate) -> log to `agent_runs` + `decision_journal`. Configurable LLM backend (llamacpp/hyperspace/openai/anthropic).
- **Hyperspace** (`grid/hyperspace/`): P2P LLM node client with embeddings, reasoning, and research agent capabilities.
- **Ollama** (`grid/ollama/`): Local Ollama integration for market briefings and reasoning. Deprecated in favor of llama.cpp.

## Key Abstractions

### `PITStore` (`grid/store/pit.py`)
The central data access abstraction. All analytical queries must go through this class. Methods: `get_pit()`, `get_feature_matrix()`, `get_latest_values()`, `assert_no_lookahead()`. Takes a SQLAlchemy `Engine` at construction.

### `Engine` (SQLAlchemy)
A singleton SQLAlchemy engine created in `grid/db.py:get_engine()`. Configured with pool_size=5, max_overflow=10, pool_pre_ping=True. Passed by reference to every domain class constructor.

### `Settings` (`grid/config.py`)
Pydantic-settings singleton (`settings = Settings()`) loaded from environment variables / `.env` file. Contains all configuration: database credentials, API keys (FRED, KOSIS, Comtrade, etc.), LLM endpoints, auth secrets, agent configuration, pull schedules. Validates critical settings per environment (FRED key required in non-dev, default DB password rejected in non-dev, JWT secret required in production).

### `DecisionJournal` (`grid/journal/log.py`)
Append-only decision log. `log_decision()` validates confidence/probability are finite and in [0,1], inserts with full provenance. `record_outcome()` adds outcome data but rejects if already recorded. DB trigger prevents modification of immutable columns.

### `ModelRegistry` (`grid/governance/registry.py`)
State machine for model lifecycle. `transition()` validates allowed transitions, runs gate checks via `GateChecker`, handles PRODUCTION demotion (only one per layer). `rollback()` retires current model and promotes predecessor.

### `FeatureLab` (`grid/features/lab.py`)
Transformation engine. Takes `Engine` + `PITStore`, computes derived features using a library of transformations: `zscore_normalize()`, `rolling_slope()`, lagged change, ratio, spread. Reads transformation rules from `feature_registry`.

### `Resolver` (`grid/normalization/resolver.py`)
Multi-source conflict resolution. Groups raw_series by (series_id, obs_date), selects highest-priority source, flags conflicts above threshold (0.5% default).

### `GateChecker` (`grid/validation/gates.py`)
Promotion gate enforcement. Checks requirements for each state transition (e.g., CANDIDATE->SHADOW requires validation_run_id and hypothesis in PASSED state).

## Module Communication

### Primary: Engine Passing
All domain classes accept a SQLAlchemy `Engine` in their constructor. The shared engine is created once in `grid/db.py` and injected everywhere. In the API context, `grid/api/dependencies.py` provides `@lru_cache()` singleton factories: `get_db_engine()`, `get_pit_store()`, `get_journal()`, `get_model_registry()`.

### Secondary: Shared PostgreSQL Database
Modules communicate asynchronously through database tables. The ingestion layer writes to `raw_series`, the resolver reads `raw_series` and writes to `resolved_series`, the PIT store reads `resolved_series`, inference writes to `decision_journal`, etc. This makes modules loosely coupled at the cost of eventual consistency.

### Direct Python Imports
Some modules have direct import dependencies:
- `inference/live.py` imports `features/lab.py` and `store/pit.py`
- `governance/registry.py` imports `validation/gates.py`
- `normalization/resolver.py` imports `normalization/entity_map.py`
- `agents/runner.py` imports `agents/adapter.py`, `agents/config.py`, `agents/context.py`, `agents/progress.py`
- All modules import `config.py` for settings and loguru logger

### WebSocket Broadcasting
`grid/api/main.py` maintains a set of WebSocket clients (`_ws_clients`). The agent progress system (`agents/progress.py`) registers a broadcast callback at startup to push real-time agent deliberation progress to connected frontends.

### Workflow System
`grid/workflows/loader.py` loads declarative workflow definitions (Markdown + YAML frontmatter) from `workflows/available/`. `grid/cli.py` dispatches workflow execution to the appropriate domain module based on group. Workflows define dependencies, schedules, and required secrets.

## State Management

### Database Tables (PostgreSQL 15 + TimescaleDB)

| Table | Purpose | Key Properties |
|-------|---------|----------------|
| `source_catalog` | Registry of external data sources | Quality metadata (trust_score, latency_class, revision_behavior), priority ranking |
| `raw_series` | Raw data as pulled from each source | Immutable after write, tracks pull_status (SUCCESS/PARTIAL/FAILED) |
| `feature_registry` | Canonical feature definitions | Transformation rules, normalization method, eligibility flags, 36 seed features |
| `resolved_series` | PIT-resolved data after conflict resolution | Unique on (feature_id, obs_date, vintage_date), conflict tracking |
| `hypothesis_registry` | Hypothesis lifecycle tracking | States: CANDIDATE/TESTING/PASSED/FAILED/KILLED |
| `model_registry` | Model version lifecycle | States: CANDIDATE/SHADOW/STAGING/PRODUCTION/FLAGGED/RETIRED. Unique partial index enforces one PRODUCTION per layer |
| `validation_results` | Backtest/walk-forward results | Links to hypothesis and model, stores era_results, gate_detail as JSONB |
| `decision_journal` | Immutable decision log | Append-only (DB trigger protects core columns). Outcomes recorded separately |
| `agent_runs` | TradingAgents deliberation records | Links to decision_journal, stores analyst_reports, debate, risk_assessment as JSONB |

### Model Governance States

```
CANDIDATE ──► SHADOW ──► STAGING ──► PRODUCTION ──► FLAGGED
    │            │          │                          │
    └──► RETIRED ◄──────────┴──────────── RETIRED ◄───┘
```

- **CANDIDATE**: New model under consideration. Gate: validation_run_id set, hypothesis PASSED.
- **SHADOW**: Running alongside production for comparison. Gate: validation results exist.
- **STAGING**: Pre-production validation. Gate: walk-forward backtest passes.
- **PRODUCTION**: Active model (one per layer). Demotes existing PRODUCTION to SHADOW on promotion.
- **FLAGGED**: Automated monitoring detected issues. Can return to PRODUCTION or retire.
- **RETIRED**: End of lifecycle. Reason recorded.

### In-Memory State
- `_ws_clients` set in `api/main.py` — connected WebSocket clients
- `_login_attempts` dict in `api/auth.py` — rate limiting (resets on restart)
- `@lru_cache()` singletons in `api/dependencies.py` — engine, PITStore, journal, registry (never cleared)
- `schedule` library job queue in ingestion schedulers
