# GRID Developer Guide

This guide covers setting up a development environment, running tests, and following the project's coding patterns.

---

## Development Environment Setup

### Prerequisites

- Python 3.11+
- Node.js 18+ and npm
- Docker and Docker Compose (for PostgreSQL + TimescaleDB)
- Git

### 1. Start the Database

GRID requires PostgreSQL 15 with TimescaleDB. SQLite and MySQL are not supported -- the PIT query engine relies on PostgreSQL-specific features (`DISTINCT ON`, `MAKE_INTERVAL`, array types, partial indexes).

```bash
cd grid
docker compose up -d
```

This starts a `timescale/timescaledb:latest-pg15` container on port 5432 with default credentials (`grid_user` / `changeme`).

### 2. Apply the Schema

```bash
cd grid
python db.py
```

Reads `schema.sql` and creates all tables, indexes, and constraints.

### 3. Python Environment

```bash
cd grid
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Environment Configuration

```bash
cp .env.example .env
```

For development, the only variable you may need to set is `FRED_API_KEY` (free at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html)). All other API keys are optional -- data sources with missing keys degrade gracefully. The `ENVIRONMENT` variable defaults to `development`, which relaxes validation on `DB_PASSWORD`, `GRID_JWT_SECRET`, and `FRED_API_KEY`.

### 5. Start the Backend

```bash
cd grid
python -m uvicorn api.main:app --reload --port 8000
```

Swagger UI is available at `http://localhost:8000/api/docs` in development mode.

### 6. Start the Frontend

```bash
cd grid/pwa
npm install
npm run dev
```

The Vite dev server runs on port 5173 and proxies `/api` requests to the backend on port 8000.

---

## Running Tests

### Test Framework

Tests use pytest and live in `grid/tests/`. Most tests use mock fixtures and do not require a running database.

```bash
# Full suite
cd grid && python -m pytest tests/ -v

# PIT correctness tests (highest priority -- must always pass)
cd grid && python -m pytest tests/test_pit.py -v

# API tests
cd grid && python -m pytest tests/test_api.py -v

# Specific test file
cd grid && python -m pytest tests/test_resolver_unit.py -v
```

### Critical Tests

These tests cover the most important system invariants and should never be allowed to break:

| Test File | What It Covers |
|-----------|----------------|
| `test_pit.py` | PIT query correctness, no-lookahead enforcement |
| `test_resolver_unit.py` | Multi-source conflict resolution, per-family thresholds |
| `test_journal_bounds.py` | NaN/infinity rejection in decision journal |
| `test_security.py` | JWT secret and DB password validation |
| `test_integration_pipeline.py` | Full pipeline: ingestion through inference |

### Test Fixtures

Shared fixtures are defined in `tests/conftest.py`:

- **`pg_engine`** -- Real PostgreSQL engine (skips test if DB is unavailable)
- **`mock_engine`** -- Mock SQLAlchemy Engine with `.connect()` and `.begin()` context managers
- **`mock_pit_store`** -- Mock PITStore returning empty DataFrames

Use `mock_engine` and `mock_pit_store` for unit tests that should not require a database. Use `pg_engine` only for integration tests that must exercise real SQL.

### Mocking External APIs

Never hit live endpoints in tests. Mock all external API calls:

```python
from unittest.mock import patch, MagicMock

@patch("ingestion.fred.requests.get")
def test_fred_pull(mock_get):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {"observations": [{"date": "2024-01-01", "value": "3.5"}]},
    )
    # ... test puller logic
```

### Modules That Need Tests

These modules have zero or minimal test coverage. Add tests when modifying them:

- `normalization/entity_map.py` -- Entity disambiguation
- `validation/gates.py` -- Promotion gate checkers (partial: `test_gates.py`)
- `governance/registry.py` -- Model lifecycle state machine (partial: `test_registry.py`)
- `inference/live.py` -- Live inference engine (partial: `test_live_inference.py`)
- `hyperspace/` -- All modules
- `ollama/` -- All modules
- All international ingestion modules (`ingestion/international/`)

---

## Code Patterns

### PIT (Point-in-Time) Correctness

**This is the most important invariant in the system.** Every data query for inference or feature engineering must use `store/pit.py` to prevent lookahead bias. Never query raw tables directly for analytical purposes.

Key rules:
- Every analytical query requires an `as_of` timestamp parameter
- `assert_no_lookahead()` must be called before any inference result is persisted
- When adding features in `features/lab.py`, verify they cannot leak future information
- Walk-forward backtests in `validation/gates.py` enforce temporal boundaries -- never bypass them

Note that `assert_no_lookahead()` clears the DataFrame before raising `ValueError` but does NOT roll back the calling transaction. If called mid-inference, partial results could persist if the caller does not handle the exception.

### SQL Safety

**Never use f-strings, `.format()`, or string concatenation for SQL queries.** Always use parameterized queries:

```python
# CORRECT
from sqlalchemy import text

result = conn.execute(
    text("SELECT * FROM raw_series WHERE feature_id = :fid AND obs_date >= :start"),
    {"fid": feature_id, "start": start_date},
)

# WRONG -- SQL injection vulnerability
result = conn.execute(text(f"SELECT * FROM raw_series WHERE feature_id = {feature_id}"))
```

If you encounter existing SQL injection patterns (f-strings or `.format()` in SQL), fix them on sight. See `ATTENTION.md` items 1, 46, and 54 for tracked instances.

### Graceful Degradation

All optional subsystems (Hyperspace, Ollama, llama.cpp, TradingAgents) must return `None` or a sensible default when offline. The system must operate without any LLM provider available. Startup code wraps all optional subsystem initialization in try/except blocks:

```python
try:
    from agents.scheduler import start_agent_scheduler
    start_agent_scheduler()
except Exception as exc:
    log.debug("Agent scheduler start skipped: {e}", e=str(exc))
```

Missing API keys should log a warning but never crash the system. Use `settings.audit_api_keys()` to check which keys are configured.

### Logging

Use loguru throughout the project, imported from config:

```python
from loguru import logger as log

log.info("Processing {n} records for {src}", n=count, src=source_name)
log.warning("Coerced {n} non-numeric values to NaN", n=coerced_count)
log.error("Database connection failed: {e}", e=str(exc))
```

### Configuration

All settings come from environment variables via `config.py` (pydantic-settings). Never hardcode configuration values. Access settings through the singleton:

```python
from config import settings

url = settings.DB_URL
api_key = settings.FRED_API_KEY
```

### Immutable Decision Journal

Entries in `journal/log.py` are never updated or deleted. Every recommendation gets logged with full provenance. When writing to the journal:

- Validate that `state_confidence` and `transition_probability` are in range 0-1
- Reject NaN and infinity values
- Never modify existing entries -- outcomes are recorded separately

---

## Adding a New Data Source

Follow the existing ingestion module pattern. Each data source gets its own module in `ingestion/` (or the appropriate subdirectory: `international/`, `altdata/`, `trade/`, `physical/`).

### 1. Create the Puller Module

Extend `BasePuller` from `ingestion/base.py`:

```python
"""GRID data puller for [Source Name]."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure


class MySourcePuller(BasePuller):
    SOURCE_NAME = "MY_SOURCE"

    def __init__(self, engine: Engine, api_key: str = "") -> None:
        super().__init__(engine)
        self._api_key = api_key

    @retry_on_failure()
    def pull(self) -> int:
        """Pull data from [Source Name]. Returns count of rows inserted."""
        if not self._api_key:
            log.warning("MY_SOURCE API key not set -- skipping pull")
            return 0

        # Fetch data from API...
        # For each observation:
        #   self._insert_raw(feature_id, obs_date, release_date, value, ...)
        # Use self._row_exists() to avoid duplicates
        # Use self._resolve_source_id() to get/create the source catalog entry

        return inserted_count
```

`BasePuller` provides:
- `_resolve_source_id()` -- looks up or auto-creates a `source_catalog` entry
- `_row_exists()` -- checks for duplicate rows before insert
- `_insert_raw()` -- standardized insert into `raw_series`

The `retry_on_failure` decorator adds exponential backoff with jitter for transient API failures.

### 2. Add Entity Mappings

If the source provides data that overlaps with existing sources (same economic indicator from a different provider), add mappings in `normalization/entity_map.py`.

### 3. Register in the Scheduler

Add your source to `ingestion/scheduler.py` (not `scheduler_v2.py`, which is deprecated). Follow the existing schedule patterns:

- Domestic economic data: weekday evenings (6-7 PM)
- International data: weekday late evenings (8 PM)
- Trade data: weekly (Sunday 3 AM)
- Physical data: monthly (1st of month, 4 AM)

### 4. Store PIT-Compatible Timestamps

Every inserted row must include:
- `observation_date` -- when the event occurred
- `release_date` -- when the data was published
- Use the correct `vintage_date` if the source provides data revisions

### 5. Handle Data Quality

- Log warnings when `pd.to_numeric(errors="coerce")` drops non-numeric data (do not silently coerce)
- Handle API rate limits with exponential backoff (use `retry_on_failure`)
- Return 0 or skip gracefully when API keys are missing

### 6. Add Tests

Create `tests/test_my_source.py` with:
- Parsing tests (mock the API response, verify correct extraction)
- Timestamp handling tests (verify PIT-compatible dates)
- Error handling tests (API failures, missing keys, malformed data)
- NaN handling (follow the existing module's NaN strategy)

---

## Adding a New API Endpoint

### 1. Create or Extend a Router

API routes live in `api/routers/`. Each router is a FastAPI `APIRouter` with a prefix:

```python
"""My feature endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.schemas.my_feature import MyResponse

router = APIRouter(prefix="/api/v1/my-feature", tags=["my-feature"])


@router.get("/", response_model=MyResponse)
async def get_data(_token: str = Depends(require_auth)) -> MyResponse:
    """Return feature data."""
    engine = get_db_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name FROM my_table WHERE active = :active LIMIT :lim"),
            {"active": True, "lim": 100},
        ).fetchall()
    return MyResponse(items=[dict(r._mapping) for r in rows])
```

### 2. Register the Router

Add the router in `api/main.py`:

```python
from api.routers.my_feature import router as my_feature_router
app.include_router(my_feature_router)
```

### 3. Create Pydantic Schemas

Define request/response models in `api/schemas/`:

```python
from pydantic import BaseModel

class MyResponse(BaseModel):
    items: list[dict]
```

### Key Rules for Endpoints

- **Keep route handlers thin** -- delegate business logic to domain modules
- **Use `require_auth`** -- all endpoints except `/api/v1/system/health` require authentication
- **Use parameterized SQL** -- never use f-strings or `.format()` for queries
- **Add pagination** for list endpoints -- use `offset`/`limit` parameters and return a `total` count
- **Use `get_db_engine()`** from `api/dependencies.py` -- do not create your own engine

### 4. Add Tests

Test both the happy path and error cases:
- Valid authenticated request returns expected data
- Missing or invalid token returns 401
- Malformed input returns 422
- Empty results are handled gracefully

---

## Frontend Development

### Stack

- React 18 with functional components and hooks
- Zustand for state management
- Lucide React for icons
- Vite for bundling

### Development Workflow

```bash
cd grid/pwa
npm install
npm run dev    # Dev server on :5173 with hot reload
```

The Vite dev server proxies `/api` requests to the backend on port 8000.

### Patterns

- Components live in `pwa/src/` following the existing structure
- Use the existing Zustand store pattern -- do not introduce Redux or Context API
- API calls should go through a centralized fetch wrapper
- Handle loading and error states for all async operations

### Building for Production

```bash
cd grid/pwa
npm run build
```

The build output goes to `pwa_dist/`. FastAPI serves it automatically -- `api/main.py` checks for `pwa_dist/` first, then falls back to `pwa/`. All non-API paths are routed to `index.html` for SPA routing.

### PWA Assets

- Service worker (`service-worker.js`) and manifest (`manifest.json`) are in `pwa/public/`
- The service worker includes an IndexedDB-backed offline queue for journal POSTs
- Icons are in `pwa/public/icons/` (6 sizes from 76px to 512px)

### Testing

There is currently no frontend test suite (no Jest, Vitest, or Cypress). This is tracked as ATTENTION.md item 38.

---

## Database Migrations

Alembic is configured for schema migrations:

```bash
cd grid
alembic upgrade head          # Apply all pending migrations
alembic revision -m "desc"    # Create a new migration
alembic history               # View migration history
```

Alembic uses `Settings.DB_URL` from `config.py` for the database connection.

For initial setup or fresh databases, use `python db.py` to apply `schema.sql` directly.

---

## Common Gotchas

These are the most frequently encountered issues during development. See `ATTENTION.md` for the complete 64-item list.

### Database

- **`DISTINCT ON`** in `store/pit.py` is PostgreSQL-specific -- GRID will never work on SQLite or MySQL
- **Connection pool** is configured with `pool_size=5, max_overflow=10` -- if tests hang, you may have connection leaks
- **`@lru_cache()`** is no longer used for singletons (replaced with clearable module-level singletons in `api/dependencies.py`), but config changes still require a restart unless `clear_singletons()` is called

### Data Ingestion

- **`_resolve_source_id()`** auto-creates `source_catalog` entries if missing -- unknown sources can appear silently without operator awareness
- **`pd.to_numeric(errors="coerce")`** silently converts bad data to NaN -- always log a warning with the count of coerced values
- **Two scheduler files** exist: `scheduler.py` (authoritative) and `scheduler_v2.py` (deprecated) -- always use `scheduler.py`
- **Only `FRED_API_KEY`** is validated at startup -- other API keys (KOSIS, Comtrade, JQUANTS, etc.) are not validated until the puller runs

### NaN Handling

NaN handling is intentionally inconsistent across modules -- each module has its own data quality requirements:
- `discovery/orthogonality.py` uses `ffill(limit=5)`
- `discovery/clustering.py` uses `ffill().dropna()`
- `features/lab.py` varies by transformation type

When modifying a module, follow that module's existing NaN strategy. Do not introduce a new approach without updating all related modules.

### Authentication

- **JWT secret** defaults to `"dev-secret-change-me"` in development. In production, the system refuses to start without a real secret.
- **Rate limiting** uses `shelve` (persisted to disk) -- survives restarts but does not work across multiple instances. Multi-instance deployments should use Redis.
- **WebSocket auth** supports both first-message auth (preferred) and query-param auth (legacy). First-message auth avoids leaking tokens in URLs and logs.

### Conflict Resolution

- The **0.5% default threshold** in `normalization/resolver.py` false-positives on high-volatility features (VIX, commodities). Per-family thresholds are configured: vol=2%, commodity=1.5%, crypto=3%.
- **Division by zero** when the reference value is 0: if only one value is 0, pct_diff is infinity (always a conflict). If both are 0, no conflict.

### PIT Safety

- **Vintage policies** (`FIRST_RELEASE` vs `LATEST_AS_OF`) produce different values for the same query -- always specify which you intend
- **`assert_no_lookahead()`** clears the DataFrame before raising but does NOT roll back the transaction -- callers must handle this

---

## Project Structure

```
grid/
├── api/               # FastAPI routes, auth, middleware
│   ├── main.py        # App entry, WebSocket, startup/shutdown
│   ├── auth.py        # JWT auth, rate limiting (shelve-backed)
│   ├── dependencies.py# Clearable singletons (engine, PIT, journal)
│   ├── routers/       # Route modules (14 routers)
│   └── schemas/       # Pydantic request/response models
├── ingestion/         # 37+ data source pullers
│   ├── base.py        # BasePuller with shared methods
│   ├── scheduler.py   # Unified scheduler (authoritative)
│   ├── international/ # ECB, BOJ, KOSIS, AKShare, Eurostat, etc.
│   ├── altdata/       # Reddit, options, yFinance
│   ├── trade/         # Comtrade, Atlas ECI
│   └── physical/      # NOAA, EIA, USDA
├── normalization/     # Multi-source conflict resolution
├── store/             # PIT-correct query engine
├── features/          # Feature engineering (zscore, slopes, ratios)
├── discovery/         # Unsupervised regime discovery
├── validation/        # Walk-forward backtesting gates
├── inference/         # Live model scoring
├── journal/           # Immutable decision log
├── governance/        # Model lifecycle state machine
├── agents/            # TradingAgents multi-agent framework
├── hyperspace/        # Local LLM inference (P2P)
├── ollama/            # Ollama LLM integration
├── llamacpp/          # llama.cpp direct integration
├── outputs/           # Generated LLM outputs (gitignored)
├── pwa/               # React 18 PWA frontend
├── tests/             # pytest suite
├── scripts/           # Migration and setup scripts
├── config.py          # pydantic-settings configuration
├── db.py              # SQLAlchemy engine + health check
├── schema.sql         # Database schema
└── ATTENTION.md       # 64-item audit tracking list
```
