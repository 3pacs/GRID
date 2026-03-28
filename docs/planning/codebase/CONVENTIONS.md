# Conventions

## Code Style

- **Indentation:** 4 spaces throughout all Python files.
- **Line length:** No strict enforced limit observed; lines commonly reach 90-100 characters, occasionally longer for SQL strings and log messages.
- **Formatting tools:** None configured (no `pyproject.toml`, `setup.cfg`, or formatter config files found). Code follows a consistent manual style.
- **String quotes:** Double quotes for docstrings and most strings. Single quotes used occasionally in SQL and dictionary keys, but no rigid rule.
- **Trailing commas:** Used consistently in multi-line function arguments, dictionary literals, and import lists.
- **Blank lines:** Two blank lines between top-level definitions (functions, classes). One blank line between methods within a class.
- **Section separators:** Comment banners using `# ---------------------------------------------------------------------------` to divide logical sections within files (see `grid/db.py`, `grid/tests/test_api.py`, `grid/tests/test_pit.py`).
- **Module-level `__main__` blocks:** Most modules include an `if __name__ == "__main__":` block for quick CLI testing (see `grid/db.py`, `grid/store/pit.py`, `grid/ingestion/bls.py`, `grid/normalization/entity_map.py`, `grid/journal/log.py`, `grid/governance/registry.py`).

## Naming

- **Variables:** `snake_case` throughout. Short abbreviations accepted for local scope (`fid`, `sid`, `df`, `conn`, `exc`).
- **Functions:** `snake_case`. Private/internal functions prefixed with underscore (`_resolve_source_id`, `_fetch_and_store`, `_parse_period_to_date`, `_slope`).
- **Classes:** `PascalCase`. Domain-specific names: `PITStore`, `BLSPuller`, `FREDPuller`, `EntityMap`, `DecisionJournal`, `ModelRegistry`, `Resolver`, `HyperspaceClient`.
- **Files:** `snake_case`. Modules named by domain concept (`pit.py`, `lab.py`, `registry.py`, `entity_map.py`, `resolver.py`).
- **Constants:** `UPPER_SNAKE_CASE` at module level. Examples: `BLS_SERIES_LIST`, `SEED_MAPPINGS`, `NEW_MAPPINGS_V2`, `CONFLICT_THRESHOLD`, `_VALID_TRANSITIONS`, `_VALID_VERDICTS`, `_BLS_API_URL`, `_MAX_SERIES_PER_QUERY`.
- **Private constants:** Prefixed with underscore (`_VALID_OPERATOR_CONFIDENCE`, `_BLS_API_URL`, `_MONTH_MAP`, `_client_instance`).
- **Test classes:** `PascalCase` with descriptive names mirroring the behavior being tested (`TestPITNoFutureData`, `TestLogDecision`, `TestRecordOutcome`, `TestClientGracefulDegradation`).
- **Test methods:** `test_` prefix with descriptive snake_case (`test_no_future_data_returned`, `test_record_outcome_immutable`).

## Imports

- **`from __future__ import annotations`** is present at the top of every Python file for PEP 604 union syntax support (`str | None`, `int | None`, `dict[str, Any]`).
- **Import ordering:**
  1. `__future__` imports (always first)
  2. Standard library (`os`, `sys`, `json`, `time`, `math`, `contextlib`, `datetime`, `pathlib`, `typing`, `difflib`, `functools`)
  3. Third-party packages (`requests`, `pandas`, `numpy`, `scipy`, `pytest`, `psycopg2`, `sqlalchemy`, `fastapi`, `pydantic`, `loguru`, `schedule`, `matplotlib`, `seaborn`, `sklearn`, `passlib`)
  4. Local/project imports (`config`, `db`, `store.pit`, `journal.log`, `governance.registry`, `normalization.entity_map`, `api.auth`, `api.dependencies`, `ingestion.*`)
- **Absolute imports** used throughout. No relative imports observed.
- **Specific imports preferred:** `from sqlalchemy import text`, `from loguru import logger as log`, `from sqlalchemy.engine import Engine`. Bare `import module` used for `json`, `os`, `sys`, `time`, `math`.
- **Deferred imports:** Used in scheduler and API routes to avoid circular dependencies and for lazy loading (e.g., `from ingestion.fred import FREDPuller` inside function body in `grid/ingestion/scheduler.py`, `from inference.live import LiveInference` inside route handler in `grid/api/routers/signals.py`).

## Type Hints

- **Required on all function signatures.** Every function and method includes parameter type hints and return type annotations.
- **Union syntax:** Uses PEP 604 pipe syntax (`str | None`, `int | None`, `dict[str, Any] | None`) enabled by `from __future__ import annotations`.
- **Common patterns:**
  - `db_engine: Engine` for SQLAlchemy engine parameters (from `sqlalchemy.engine import Engine`)
  - `-> None` for `__init__` methods
  - `-> int`, `-> bool`, `-> dict[str, Any]`, `-> pd.DataFrame` for return types
  - `list[int]`, `list[str]`, `dict[str, str]`, `dict[str, int]` for collection types
  - `tuple[int, int]` for tuple types
- **`typing` module usage:** `Any`, `Generator` imported from `typing`. Modern generic syntax (`list[int]` not `List[int]`) used throughout.
- **Class attributes documented** in class docstrings under an `Attributes:` section rather than using class-level type annotations.

## Error Handling

- **`ValueError`** is the primary exception type for input validation and business rule violations (invalid vintage policy, invalid verdict, outcome already recorded, invalid state transition, lookahead violation).
- **`RuntimeError`** used for infrastructure/setup failures (e.g., source not found in `source_catalog` in `grid/ingestion/bls.py`).
- **Graceful degradation pattern:** External service calls (Hyperspace, Ollama, APIs) catch broad `Exception`, log the error, and return `None` or a safe default. The system must operate without optional services (see `grid/hyperspace/client.py`, `grid/ingestion/scheduler.py`, `grid/api/routers/signals.py`).
- **API route error handling:** Route handlers wrap logic in `try/except Exception`, log with `log.warning()`, and return a degraded JSON response with an `error` key rather than raising HTTP errors (see `grid/api/routers/signals.py`).
- **Input validation:** Done at method entry. Checks for valid enum values, NaN/infinity, range [0, 1] for probabilities (see `grid/journal/log.py:78-103`).
- **Database errors:** `get_connection()` in `grid/db.py` rolls back on exception and always closes the connection in a `finally` block.
- **No bare `except:`** clauses observed. All catch `Exception` explicitly.

## Logging

- **Library:** `loguru` imported as `from loguru import logger as log` in every module.
- **Configuration:** Centralized in `grid/config.py`. Default handler removed, single stderr handler added with custom format.
- **Format string:** `<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> -- <level>{message}</level>`
- **Log levels used:**
  - `log.info()` for initialization, operation start/completion, state changes
  - `log.debug()` for query details, cache operations, intermediate results
  - `log.warning()` for missing data, empty results, rate limits, degraded service, coercion events
  - `log.error()` for API failures, database errors, health check failures
- **Structured logging:** Uses loguru's keyword argument style: `log.info("Message — key={k}", k=value)`. The em-dash (`--`) separator is used consistently between message text and structured fields.
- **Log level:** Configurable via `LOG_LEVEL` environment variable, defaults to `"INFO"`.

## Common Patterns

### Database Connection
- **Engine creation:** Singleton pattern via module-level `_engine` variable in `grid/db.py`. `get_engine()` creates on first call with pool configuration (`pool_size=5`, `max_overflow=10`, `pool_timeout=30`, `pool_pre_ping=True`).
- **Dependency injection:** All domain classes accept `db_engine: Engine` as a constructor parameter. Dependencies provided via `@lru_cache()` factory functions in `grid/api/dependencies.py`.
- **Context managers:** `engine.connect()` for read-only operations, `engine.begin()` for write operations that auto-commit.

### SQL Queries
- **Always parameterized:** `text()` with named parameters (`:param_name`) and dict binding. Example: `text("SELECT ... WHERE id = :id"), {"id": value}`.
- **No f-strings or `.format()` for SQL** (with known exceptions flagged as bugs in `ATTENTION.md`).
- **`ON CONFLICT ... DO NOTHING`** used for idempotent inserts in ingestion modules.
- **`RETURNING id`** used to get auto-generated IDs after INSERT.
- **`DISTINCT ON`** used in PIT queries (PostgreSQL-specific).

### Ingestion Modules
- **Class-based:** Each source gets a `*Puller` class (e.g., `BLSPuller`, `FREDPuller`, `ECBPuller`, `OFRPuller`).
- **Constructor pattern:** `__init__(self, db_engine: Engine, api_key: str | None = None)` -- engine is required, API key is optional.
- **`_resolve_source_id()`** method in every puller -- looks up `source_catalog.id` by source name. Copy-pasted across modules (acknowledged tech debt).
- **Return value:** Methods return `dict[str, Any]` with keys `status` (`"SUCCESS"`, `"PARTIAL"`, `"FAILED"`), `rows_inserted`, `errors` (list of strings).
- **Module-level constants:** Series lists, API URLs, and rate limit values defined as module constants.

### API Endpoints
- **FastAPI `APIRouter`** with prefix pattern: `router = APIRouter(prefix="/api/v1/{domain}", tags=["{domain}"])`.
- **Auth via dependency:** `_token: str = Depends(require_auth)` on protected routes.
- **Thin handlers:** Route functions delegate to domain modules. Business logic lives in `journal/`, `governance/`, `store/`, `inference/`, etc.
- **Async handlers:** `async def` used for route handlers.
