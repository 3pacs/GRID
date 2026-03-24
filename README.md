# GRID — Private Trading Intelligence Engine

GRID is a systematic trading intelligence platform that ingests macroeconomic and market data from 37+ global sources, resolves multi-source conflicts using point-in-time (PIT) correct methodology, engineers features, discovers market regimes via unsupervised clustering, validates hypotheses through walk-forward backtesting, and maintains an immutable decision journal. Every query enforces strict no-lookahead constraints to prevent data leakage.

## Prerequisites

- **Python 3.11+**
- **Docker** and Docker Compose
- **Node.js 18+** (for PWA frontend)
- **PostgreSQL 15+** (required — not compatible with MySQL or SQLite due to `DISTINCT ON`, `MAKE_INTERVAL`, array types, and partial indexes). TimescaleDB extension optional but recommended. Provided via Docker.
- A **FRED API key** (free from https://fred.stlouisfed.org/docs/api/api_key.html)

## Quick Start

```bash
cd grid

# 1. Configure
cp .env.example .env
# Edit .env — set FRED_API_KEY and GRID_MASTER_PASSWORD_HASH

# 2. Start database
docker compose up -d

# 3. Install Python deps + apply schema
pip install -r requirements.txt
python db.py

# 4. Build PWA
cd pwa && npm install && npm run build && cd ..

# 5. Start API server
python -m uvicorn api.main:app --host 0.0.0.0 --port 8000
```

The API serves the PWA at `/` and the REST API at `/api/v1/*`.

## Essential Commands

```bash
# Database
docker compose up -d                                    # Start PostgreSQL + TimescaleDB
python db.py                                            # Apply schema

# Backend
pip install -r requirements.txt
python -m uvicorn api.main:app --reload --port 8000     # Dev server

# Frontend
cd pwa && npm install && npm run dev                    # Dev server on :5173
cd pwa && npm run build                                 # Production build

# Tests
python -m pytest tests/ -v                              # Full suite (342 tests)
python -m pytest tests/test_pit.py -v                   # PIT store tests (critical)
python -m pytest tests/test_integration_pipeline.py -v  # Pipeline integration

# Data ingestion
python -m ingestion.scheduler --historical              # Historical pull (1990+)
python -m ingestion.scheduler                           # Ongoing scheduler

# Discovery
python -m discovery.orthogonality                       # Orthogonality audit
python -m discovery.clustering                          # Regime clustering

# LLM insights
python -m outputs.insight_scanner --days 7              # Review accumulated LLM outputs
```

## Architecture

See [docs/architecture.md](docs/architecture.md) for detailed diagrams covering:
- System overview and component relationships
- Data flow pipeline (ingestion → resolution → PIT → features → inference)
- LLM integration layer (Hyperspace, Ollama, llama.cpp, TradingAgents)
- API architecture and middleware stack
- Database schema (key tables and relationships)
- Model lifecycle state machine

```
  DATA SOURCES (37+)     NORMALIZATION        PIT STORE           FEATURES
  ┌──────────────┐      ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
  │ FRED, BLS    │─────▶│ Conflict     │───▶│ No-lookahead │───▶│ z-score      │
  │ ECB, BOJ     │      │ Resolution   │    │ FIRST_RELEASE│    │ slope, ratio │
  │ yFinance     │      │ Per-family   │    │ LATEST_AS_OF │    │ pct_change   │
  │ 30+ more     │      │ thresholds   │    │ DISTINCT ON  │    │ spread       │
  └──────────────┘      └──────────────┘    └──────────────┘    └──────┬───────┘
                                                                       │
                    ┌──────────────────────────────────────────────────┘
                    │
                    ▼
  DISCOVERY              INFERENCE             JOURNAL              LLM LAYER
  ┌──────────────┐      ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
  │ PCA, GMM     │      │ Production   │───▶│ Immutable    │    │ Ollama       │
  │ Correlation  │      │ model score  │    │ decisions    │    │ Hyperspace   │
  │ k=2..6 test  │      │ BUY/SELL/    │    │ Outcomes     │    │ TradingAgents│
  │ 100x options │      │ HOLD         │    │ Annotations  │    │ Briefings    │
  └──────────────┘      └──────────────┘    └──────────────┘    └──────────────┘
```

## Data Sources (37+)

### Domestic
FRED, BLS, Census, Treasury, BEA, yFinance, Reddit sentiment, options chains

### International Central Banks & Statistical Agencies
ECB, BOJ, BOE, RBI, KOSIS (Korea), AKShare (China), MAS (Singapore), ABS (Australia), BCB (Brazil), Eurostat, OECD, BIS, DBnomics, IMF

### Trade & Complexity
UN Comtrade v2, CEPII BACI, Harvard Atlas ECI, WIOD

### Physical Economy
NOAA AIS, EIA, USDA NASS, NASA VIIRS, EU KLEMS, USPTO PatentsView, OFR, Opportunity Insights, GDELT

### Key Derived Signals
- **China Credit Impulse**: 12-month change in TSF/GDP — leads global growth by 6-12 months
- **Korea Export YoY**: First major economy to report monthly — global trade leading indicator
- **K-Shape Ratio**: High vs low income consumer spend — structural regime indicator
- **VIIRS-Macro Divergence**: Nighttime lights vs official IP — data quality flag
- **BTP-Bund Spread**: Italy-Germany yield differential — Euro area stress barometer
- **OECD CLI Slope**: 3-month rate of change — regime transition early warning

## API Endpoints

See [docs/api-reference.md](docs/api-reference.md) for complete endpoint documentation.

Key endpoints:
| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/system/health` | Health check (no auth) |
| `POST /api/v1/auth/login` | Authenticate |
| `GET /api/v1/regime/current` | Current regime state |
| `GET /api/v1/signals/current` | Live trading signals |
| `GET /api/v1/journal` | Decision journal entries |
| `POST /api/v1/agents/run` | Trigger TradingAgents deliberation |
| `POST /api/v1/ollama/briefing` | Generate market briefing |
| `WS /ws` | Real-time updates (regime, signals, agent progress) |

## LLM Integration

GRID uses local LLM inference via Ollama, Hyperspace (P2P), or llama.cpp. All LLM calls:
- Return `None` if the provider is offline (graceful degradation)
- Log all outputs to timestamped markdown files in `outputs/llm_insights/`
- Are reviewed daily/weekly by the insight scanner for longer-term patterns

### TradingAgents (Multi-Agent Deliberation)
Orchestrates analyst agents (fundamental, technical, sentiment, news), a bull/bear debate, risk assessment, and produces a BUY/SELL/HOLD decision logged to both `agent_runs` table and the immutable decision journal.

### Market Briefings
Ollama generates hourly, daily, and weekly market condition reports saved to `outputs/market_briefings/`.

### Reasoning Layer
Hypothesis generation, economic mechanism explanation, backtest critique, and regime transition analysis — used by the discovery and validation pipelines.

### Privacy Boundary
GRID's signal logic is never sent to external networks. All LLM calls use local inference only. Hyperspace embeddings use public economic concepts, never raw feature values or cluster structures.

## Model Governance

```
CANDIDATE → SHADOW → STAGING → PRODUCTION → FLAGGED → RETIRED
```

- **Promotion gates** enforced at each transition (validation run, hypothesis state, journal count)
- **One PRODUCTION model per layer** — existing model auto-demoted when new one promoted
- **Flagging** is automatic (monitoring-driven), unflagging requires operator action
- **Rollback** retires current model and promotes predecessor

## Testing

342 tests across 18 test files:

```bash
python -m pytest tests/ -v                 # Full suite
python -m pytest tests/test_pit.py -v      # PIT correctness (highest priority)
python -m pytest tests/test_gates.py -v    # Promotion gate logic
python -m pytest tests/test_registry.py -v # Model lifecycle state machine
```

Tests run without PostgreSQL (mocked). PIT tests verify no future data leaks.

## Deployment

See [docs/deployment.md](docs/deployment.md) for production deployment guide covering:
- Environment variables and secrets
- Reverse proxy configuration
- systemd service setup
- Security checklist
- Monitoring and health checks

## Development

See [docs/development.md](docs/development.md) for developer guide covering:
- Code patterns (PIT correctness, SQL safety, graceful degradation)
- Adding new data sources
- Adding new API endpoints
- Testing patterns and fixtures
- Frontend development

## Project Health

See [ATTENTION.md](ATTENTION.md) for the 64-item audit tracking list with current status.

| Category | Status |
|----------|--------|
| Security (SQL injection, auth, secrets) | Fixed |
| Data integrity (PIT, conflicts, NaN) | Fixed |
| Test coverage | 342 tests, all critical modules covered |
| LLM output logging | All outputs logged to timestamped files |
| Production readiness | Documented, critical items addressed |

## Directory Structure

```
grid/
├── api/           # FastAPI routes, auth, middleware (14 routers)
├── ingestion/     # 37+ data source pullers (domestic, intl, alt, physical)
├── normalization/ # Multi-source conflict resolution
├── store/         # PIT-correct query engine (PostgreSQL DISTINCT ON)
├── features/      # Feature engineering (z-score, slopes, ratios)
├── discovery/     # Unsupervised regime clustering + options scanner
├── validation/    # Walk-forward backtesting gates
├── inference/     # Live model scoring
├── journal/       # Immutable decision log
├── governance/    # Model lifecycle state machine
├── agents/        # TradingAgents multi-agent framework
├── hyperspace/    # Local LLM inference layer (P2P)
├── ollama/        # Ollama integration + market briefings
├── llamacpp/      # llama.cpp direct integration
├── outputs/       # LLM insight logging + scanner
├── physics/       # Market physics verification
├── workflows/     # Declarative workflow system
├── server_log/    # Git-backed error logging + operator inbox
├── pwa/           # React 18 PWA frontend (Zustand, Vite)
├── tests/         # pytest suite (342 tests)
├── docs/          # Architecture, API, deployment, development guides
└── scripts/       # Migration and utility scripts
```
