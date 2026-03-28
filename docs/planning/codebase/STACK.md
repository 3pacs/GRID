# Stack

## Languages & Runtime

- **Python 3.11+** — backend, ingestion, analysis, inference
- **Node.js** (implied by Vite/React toolchain) — frontend build only
- **SQL** — PostgreSQL 15 schema, TimescaleDB hypertables

## Backend Framework

- **FastAPI 1.0** (`grid/api/main.py`) — async API server
- **Middleware:**
  - Custom `SecurityHeadersMiddleware` — adds `X-Content-Type-Options`, `X-Frame-Options`, `X-XSS-Protection`, `Referrer-Policy`, HSTS in non-dev (`grid/api/main.py:49-60`)
  - `CORSMiddleware` — origin-restricted in production, permissive in dev (`grid/api/main.py:66-77`)
- **Routing:** 12 routers under `/api/v1/` prefix:
  - `grid/api/auth.py` — login/logout/verify
  - `grid/api/routers/regime.py` — regime state
  - `grid/api/routers/signals.py` — signal data
  - `grid/api/routers/journal.py` — decision journal
  - `grid/api/routers/models.py` — model governance
  - `grid/api/routers/discovery.py` — regime discovery
  - `grid/api/routers/config.py` — system config
  - `grid/api/routers/physics.py` — physical economy data
  - `grid/api/routers/workflows.py` — workflow orchestration
  - `grid/api/routers/agents.py` — TradingAgents endpoints
  - `grid/api/routers/ollama.py` — LLM proxy
  - `grid/api/routers/backtest.py` — backtesting
  - `grid/api/routers/system.py` — health/status
- **WebSocket:** `/ws` endpoint for real-time push updates, query-param token auth (`grid/api/main.py:202-237`)
- **Startup hooks:** DB health check, API key audit, agent scheduler, ingestion schedulers (`grid/api/main.py:128-199`)

## Frontend Framework

- **React 18.3** — functional components with hooks (`grid/pwa/package.json`)
- **Vite 5.1** — build tool, dev server on `:5173`, proxies `/api` and `/ws` to backend `:8000` (`grid/pwa/vite.config.js`)
- **Zustand 4.5** — state management (`grid/pwa/src/store.js`)
- **Lucide React 0.344** — icon library
- **Structure:**
  - `grid/pwa/src/app.jsx` — root component
  - `grid/pwa/src/api.js` — centralized fetch wrapper
  - `grid/pwa/src/auth.js` — auth utilities
  - `grid/pwa/src/components/` — reusable components
  - `grid/pwa/src/views/` — page-level views
  - `grid/pwa/src/styles/` — stylesheets
  - `grid/pwa/src/store.js` — Zustand store
- **PWA:** Service worker at `grid/pwa/service-worker.js`, manifest at `grid/pwa/manifest.json`
- **Production serving:** FastAPI serves built PWA from `pwa_dist/` (`grid/api/main.py:241-262`)

## Database

- **PostgreSQL 15 + TimescaleDB** — Docker image `timescale/timescaledb:latest-pg15` (`grid/docker-compose.yml`)
- **SQLAlchemy 2.0** — engine with connection pool (pool_size=5, max_overflow=10, pool_timeout=30, pool_pre_ping=True) (`grid/db.py:44-49`)
- **psycopg2-binary** — raw connections for schema application and direct SQL (`grid/db.py:56-89`)
- **Schema:** Applied via `grid/schema.sql` through `grid/db.py:apply_schema()`
- **PIT Store:** `grid/store/pit.py` — point-in-time query engine using PostgreSQL `DISTINCT ON` for no-lookahead queries
- **Connection patterns:**
  - SQLAlchemy engine singleton via `grid/db.py:get_engine()`
  - `@lru_cache()` wrappers in `grid/api/dependencies.py` for PITStore, DecisionJournal, ModelRegistry
  - Raw psycopg2 context manager via `grid/db.py:get_connection()`

## Dependencies

### Core
| Package | Version | Purpose |
|---------|---------|---------|
| `fastapi` | (implied) | API framework |
| `sqlalchemy` | >=2.0.0 | ORM / query engine |
| `psycopg2-binary` | >=2.9.9 | PostgreSQL driver |
| `pandas` | >=2.1.0 | Data manipulation |
| `numpy` | >=1.26.0 | Numerical computation |
| `pydantic` | >=2.5.0 | Data validation |
| `pydantic-settings` | >=2.1.0 | Config from env vars |
| `loguru` | >=0.7.2 | Structured logging |

### Data Ingestion
| Package | Version | Purpose |
|---------|---------|---------|
| `fedfred` | >=3.0.0 | FRED API client |
| `yfinance` | >=0.2.36 | Yahoo Finance market data |
| `edgartools` | >=5.0.0 | SEC EDGAR filings |
| `requests` | >=2.31.0 | HTTP client |
| `beautifulsoup4` | >=4.12.0 | HTML parsing |
| `akshare` | >=1.14.0 | China macro data |
| `imfdatapy` | >=0.0.6 | IMF IFS/WEO data |
| `dbnomics` | >=0.2.6 | DBnomics macro aggregator |
| `eurostat` | >=0.2.1 | Eurostat data |
| `comtradeapicall` | >=0.1.4 | UN Comtrade trade data |
| `patent-client` | >=4.0.0 | USPTO PatentsView |
| `boto3` | >=1.34.0 | AWS S3 (VIIRS data) |
| `requests-cache` | >=1.1.0 | HTTP response caching |
| `tenacity` | >=8.2.0 | Retry with exponential backoff |

### Analysis
| Package | Version | Purpose |
|---------|---------|---------|
| `scikit-learn` | >=1.4.0 | Clustering, ML |
| `scipy` | >=1.12.0 | Statistical functions |
| `statsmodels` | >=0.14.0 | Time series analysis |
| `tsfresh` | >=0.20.0 | Time series feature extraction |
| `matplotlib` | >=3.8.0 | Visualization |
| `seaborn` | >=0.13.0 | Statistical plots |

### LLM / Multi-Agent
| Package | Version | Purpose |
|---------|---------|---------|
| `openai` | >=1.12.0 | OpenAI-compatible API client (Hyperspace/llama.cpp) |
| `tradingagents` | >=0.1.0 | Multi-agent trading framework |
| `langgraph` | >=0.2.0 | Agent graph orchestration |
| `langchain-openai` | >=0.1.0 | LangChain OpenAI provider |
| `langchain-anthropic` | >=0.1.0 | LangChain Anthropic provider |

### Auth (implied, not in requirements.txt)
| Package | Purpose |
|---------|---------|
| `python-jose` | JWT encode/decode (`grid/api/auth.py`) |
| `passlib` | bcrypt password hashing (`grid/api/auth.py`) |

### Data Formats
| Package | Version | Purpose |
|---------|---------|---------|
| `pyarrow` | >=14.0.0 | Parquet / columnar |
| `polars` | >=0.20.0 | Fast DataFrame ops |
| `tqdm` | >=4.66.0 | Progress bars |

## Configuration

- **pydantic-settings** `BaseSettings` class in `grid/config.py` — loads from env vars + `.env` file
- **Singleton:** `settings = Settings()` at module level, imported everywhere as `from config import settings`
- **Categories:**
  - Database: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` -> auto-constructed `DB_URL`
  - API keys: `FRED_API_KEY`, `KOSIS_API_KEY`, `COMTRADE_API_KEY`, `JQUANTS_EMAIL/PASSWORD`, `USDA_NASS_API_KEY`, `NOAA_TOKEN`, `EIA_API_KEY`, `GDELT_API_KEY`
  - LLM: `HYPERSPACE_*`, `OLLAMA_*`, `LLAMACPP_*` settings (base URL, model, timeout, enabled flag)
  - Auth: `GRID_JWT_SECRET`, `GRID_MASTER_PASSWORD_HASH`, `GRID_JWT_EXPIRE_HOURS`, `GRID_ALLOWED_ORIGINS`
  - Agents: `AGENTS_ENABLED`, `AGENTS_LLM_PROVIDER`, `AGENTS_LLM_MODEL`, `AGENTS_OPENAI_API_KEY`, `AGENTS_ANTHROPIC_API_KEY`, `AGENTS_DEBATE_ROUNDS`, `AGENTS_SCHEDULE_CRON`
  - Schedules: `PULL_SCHEDULE_FRED`, `PULL_SCHEDULE_YFINANCE`, `PULL_SCHEDULE_BLS`
- **Validators:** FRED key required in non-dev, DB password must change from default in non-dev, JWT secret required in production
- **Template:** `grid/.env.example` with all configurable fields
- **Logging:** `loguru` configured at module level in `grid/config.py`, imported as `log` throughout

## Build & Deploy

- **Docker Compose** (`grid/docker-compose.yml`):
  - Single service: `timescale/timescaledb:latest-pg15` with health check
  - Volume: `grid_pgdata` for persistent storage
  - Default credentials: `grid_user` / `changeme`
- **Backend:** `uvicorn api.main:app --reload --port 8000`
- **Frontend build:** `cd grid/pwa && npm run build` -> output to `grid/pwa_dist/`
- **Schema:** `python grid/db.py` applies `grid/schema.sql`
- **Schedulers:** Two ingestion schedulers start as daemon threads on API startup:
  - v1 (`grid/ingestion/scheduler.py`): FRED, yfinance, BLS, EDGAR — **authoritative**
  - v2 (`grid/ingestion/scheduler_v2.py`): international, trade, physical, altdata
- **Tests:** `cd grid && python -m pytest tests/ -v`
- **No CI/CD pipeline, Dockerfile for the app, or Kubernetes manifests observed**
