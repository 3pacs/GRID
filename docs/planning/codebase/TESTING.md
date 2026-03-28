# Testing

## Framework

- **pytest** is the sole test framework. No `pytest.ini`, `pyproject.toml`, `setup.cfg`, or `conftest.py` configuration files exist.
- Tests are invoked from the `grid/` directory: `python -m pytest tests/ -v`.
- No test runner plugins (coverage, xdist, etc.) are configured.
- The test `__init__.py` at `grid/tests/__init__.py` contains only a docstring.

## Test Structure

### File Organization
- All tests live in `grid/tests/`. There are 9 test files:
  - `grid/tests/test_pit.py` -- PIT store correctness (highest priority)
  - `grid/tests/test_api.py` -- FastAPI endpoint integration tests
  - `grid/tests/test_journal.py` -- Decision journal immutability and validation
  - `grid/tests/test_ingestion.py` -- FRED and yfinance puller logic
  - `grid/tests/test_hyperspace.py` -- Hyperspace/embeddings graceful degradation
  - `grid/tests/test_international.py` -- International ingestion module configs
  - `grid/tests/test_resolver.py` -- Conflict resolution logic
  - `grid/tests/test_physical.py` -- Physical economy ingestion module configs
  - `grid/tests/test_trade.py` -- Trade/complexity ingestion module configs

### Naming
- Test files follow `test_{module}.py` convention.
- Test classes use `PascalCase` describing the behavior under test. Each class typically contains 1-3 related test methods. Examples:
  - `TestPITNoFutureData`, `TestPITVintagePolicy`, `TestPITAssertNoLookahead`
  - `TestLogDecision`, `TestRecordOutcome`
  - `TestHealthNoAuth`, `TestProtectedRouteRequiresAuth`, `TestLoginValidReturnsToken`
  - `TestClientGracefulDegradation`, `TestEmbeddingsGracefulDegradation`
- Test methods use `test_` prefix with descriptive snake_case names explaining the expected behavior.
- One test class per logical behavior group; one test file per module or domain area.

### Docstrings
- Every test class and test method has a docstring explaining what is being verified. Example: `"""Query before release_date should return no results."""`

## Fixtures

### Database Fixtures
- `test_engine` is the primary fixture pattern, defined per-file (not shared via `conftest.py`).
- Database fixtures attempt to connect to a real PostgreSQL instance at `postgresql://grid_user:changeme@localhost:5432/grid`.
- If PostgreSQL is unavailable, tests are skipped with `pytest.skip("PostgreSQL not available for {module} tests")`.
- Fixtures set up test data using `engine.begin()` transactions, `yield` the engine (and sometimes additional IDs), then clean up in a post-yield block.
- Cleanup uses explicit `DELETE` statements targeting test-specific markers (e.g., `WHERE name = 'test_feature_pit'`, `WHERE annotation = 'TEST_JOURNAL'`).
- Example from `grid/tests/test_pit.py`: The fixture inserts a test feature into `feature_registry` and two `resolved_series` rows with different vintage dates, yields `(engine, feature_id)`, then deletes the test rows.
- Example from `grid/tests/test_journal.py`: Creates a test hypothesis and model in setup, yields `(engine, model_id)`, deletes by known test markers in teardown.

### No conftest.py
- There is no `conftest.py` file. Fixtures are duplicated across test files. Each file that needs a database defines its own `test_engine` fixture with similar but not identical setup/teardown logic.

## Mocking

### Patterns Used
- **`unittest.mock.MagicMock`** and **`unittest.mock.patch`** are the mocking tools. No `pytest-mock` or third-party mocking library.
- **SQLAlchemy engine mocking:** A `MagicMock()` engine with manually configured context managers for `.connect()` and `.begin()`:
  ```python
  mock_engine = MagicMock()
  mock_conn = MagicMock()
  mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
  mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
  mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
  mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)
  ```
  This pattern is copy-pasted in `grid/tests/test_ingestion.py`, `grid/tests/test_international.py`, and `grid/tests/test_physical.py`.

### What Gets Mocked
- **External APIs:** FRED API via `@patch("ingestion.fred.FredAPI")`, yfinance via `@patch("ingestion.yfinance_pull.yf")`.
- **Database engine:** Mocked in ingestion and API tests to avoid requiring a live database.
- **FastAPI dependencies:** `@patch("api.dependencies.get_db_engine")` and `@patch("api.routers.journal.get_journal")` for API tests.
- **Module-level imports:** `sys.modules.setdefault("yfinance", mock_yf_module)` used to inject fake modules before import (see `grid/tests/test_ingestion.py:97`).
- **Hyperspace tests** use a real `HyperspaceClient` pointed at a wrong port (`localhost:9999`) to test graceful degradation without mocking. The dedup logic test uses `MagicMock(spec=HyperspaceClient)` with controlled embed return values.

### API Test Setup
- `grid/tests/test_api.py` sets environment variables before importing the app:
  ```python
  os.environ["ENVIRONMENT"] = "development"
  os.environ["GRID_JWT_SECRET"] = "test-secret-key-for-testing-only"
  os.environ["GRID_MASTER_PASSWORD_HASH"] = _pwd_ctx.hash("testpassword123")
  ```
- Uses `fastapi.testclient.TestClient(app)` as a module-level client.
- A helper `_auth_header()` generates valid JWT tokens for authenticated requests.

## Coverage

### Current State
- **9 test files** covering PIT store, API, journal, FRED/yfinance ingestion, Hyperspace, international modules, resolver, physical modules, and trade modules.
- PIT tests (`grid/tests/test_pit.py`): 3 test classes, 4 tests -- verify no-lookahead, vintage policies, and assert_no_lookahead guard.
- API tests (`grid/tests/test_api.py`): 7 test classes, 7 tests -- health endpoint, auth, login, protected routes, journal immutability via API, regime uncalibrated state.
- Journal tests (`grid/tests/test_journal.py`): 2 test classes, 3 tests -- log_decision return value, outcome immutability, invalid verdict rejection.
- Ingestion tests (`grid/tests/test_ingestion.py`): 2 test classes, 3 tests -- FRED pull success/failure, yfinance OHLCV.
- Hyperspace tests (`grid/tests/test_hyperspace.py`): 5 test classes, 11 tests -- comprehensive graceful degradation and dedup logic.
- International tests (`grid/tests/test_international.py`): 6 test classes, 9 tests -- series list validation for ECB, OECD, BIS, AKShare, BCB, KOSIS.
- Resolver tests (`grid/tests/test_resolver.py`): 1 test class, 2 tests -- conflict detection and priority resolution.
- Physical tests (`grid/tests/test_physical.py`): 4 test classes, 7 tests -- VIIRS bboxes, patents CPC groups, OFR datasets, Opportunity files.
- Trade tests (`grid/tests/test_trade.py`): 3 test classes, 5 tests -- Comtrade queries, Atlas ECI countries, seed v2 SQL.

### Zero-Coverage Modules (Critical Gaps)
These modules have no test files and are identified in `grid/ATTENTION.md`:
- `grid/normalization/resolver.py` -- has `test_resolver.py` now, but tests are minimal (verify no crash on unmapped series, not actual conflict resolution logic)
- `grid/normalization/entity_map.py` -- no tests
- `grid/features/lab.py` -- no tests (feature transformation engine)
- `grid/discovery/orthogonality.py` -- no tests (orthogonality audit)
- `grid/discovery/clustering.py` -- no tests (regime clustering)
- `grid/validation/gates.py` -- no tests (promotion gate checkers)
- `grid/governance/registry.py` -- no tests (model lifecycle state machine)
- `grid/inference/live.py` -- no tests (live inference engine)
- `grid/backtest/engine.py` -- no tests
- `grid/agents/` -- no tests for any agent modules

### Weak Coverage Areas
- `grid/tests/test_api.py` tests login and basic auth flows but does not test error cases, pagination, or most protected endpoints beyond health/status.
- No integration tests for the full pipeline: ingestion -> resolution -> features -> inference.
- No frontend tests (no Jest, Vitest, or Cypress configured in `grid/pwa/`).
- International/physical/trade tests primarily validate static configuration (series lists, country lists, bounding boxes) rather than actual pull logic.

## Running Tests

```bash
# Full test suite
cd grid && python -m pytest tests/ -v

# Specific test files
cd grid && python -m pytest tests/test_pit.py -v          # PIT correctness
cd grid && python -m pytest tests/test_api.py -v           # API endpoints
cd grid && python -m pytest tests/test_journal.py -v       # Journal immutability
cd grid && python -m pytest tests/test_ingestion.py -v     # Ingestion pullers
cd grid && python -m pytest tests/test_hyperspace.py -v    # Hyperspace integration
cd grid && python -m pytest tests/test_international.py -v # International modules
cd grid && python -m pytest tests/test_resolver.py -v      # Conflict resolution
cd grid && python -m pytest tests/test_physical.py -v      # Physical economy
cd grid && python -m pytest tests/test_trade.py -v         # Trade modules

# Run a specific test class
cd grid && python -m pytest tests/test_pit.py::TestPITNoFutureData -v

# Run a specific test method
cd grid && python -m pytest tests/test_pit.py::TestPITNoFutureData::test_no_future_data_returned -v
```

### Requirements
- PostgreSQL must be running for `test_pit.py`, `test_journal.py`, and `test_resolver.py` (tests skip gracefully if unavailable).
- `test_ingestion.py`, `test_international.py`, `test_physical.py`, `test_trade.py`, and `test_hyperspace.py` run without database or network access (fully mocked or testing static config).
- `test_api.py` uses `TestClient` against the app in-process; some tests mock the database, the health endpoint connects to a real DB if available.
