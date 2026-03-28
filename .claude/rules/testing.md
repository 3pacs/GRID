# Testing Rules

These rules apply when writing or modifying tests.

## Test Framework

- Use `pytest` — tests live in `grid/tests/`
- Run all tests: `cd grid && python -m pytest tests/ -v`
- Run specific: `cd grid && python -m pytest tests/test_pit.py -v`

## Coverage Expectations

- Every new module must have a corresponding test file
- PIT correctness tests are highest priority — `test_pit.py` must always pass
- Test both happy path and edge cases (missing data, API failures, empty results)
- Mock external API calls — never hit live endpoints in tests

### Zero-Coverage Modules (ATTENTION.md #22)

These critical modules have NO tests — add tests when modifying them:
- `normalization/resolver.py` — conflict resolution logic
- `normalization/entity_map.py` — entity disambiguation
- `features/lab.py` — feature transformation engine
- `discovery/orthogonality.py` — orthogonality audit
- `discovery/clustering.py` — regime clustering
- `validation/gates.py` — promotion gate checkers
- `governance/registry.py` — model lifecycle state machine
- `inference/live.py` — live inference engine

### Weak Tests

- `tests/test_api.py` — only ~100 lines, tests login but not protected endpoints or error cases (#23)
- No integration tests exist for the full pipeline: ingestion → resolution → features → inference (#23)

## Test Patterns

- Use fixtures for database sessions and test data
- Test PIT queries with known timestamps to verify no lookahead
- Backtesting tests should validate temporal boundaries
- Journal tests must verify immutability (no updates/deletes)
- Validate NaN/infinity handling at boundaries (ATTENTION.md #21)

## Before Submitting

- Run the full test suite and confirm all tests pass
- If adding a new ingestion source, add tests for parsing and timestamp handling
- If modifying inference paths, verify `assert_no_lookahead()` coverage
- If fixing a bug, write the test that would have caught it first
