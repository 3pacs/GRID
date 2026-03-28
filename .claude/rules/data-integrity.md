# Data Integrity Rules

These rules apply when working with data ingestion, normalization, or query code.

## PIT Correctness

- All data queries MUST go through `store/pit.py` — never query raw tables directly for inference
- The `as_of` timestamp parameter is required for every analytical query
- `assert_no_lookahead()` must be called before any inference result is persisted
- When adding new features in `features/lab.py`, verify they cannot leak future information
- Walk-forward backtests in `validation/gates.py` enforce temporal boundaries — never bypass them

### Gotchas

- `assert_no_lookahead()` raises ValueError but does NOT roll back the calling transaction — if called mid-inference, partial results could persist (ATTENTION.md #8)
- `DISTINCT ON` in pit.py is PostgreSQL-specific — this system will never work on SQLite or MySQL
- Vintage policies (FIRST_RELEASE vs LATEST_AS_OF) produce different values for the same query — always specify which you intend

## Ingestion Modules

- Each data source gets its own module in `ingestion/` (or subdirectory: `international/`, `altdata/`, `trade/`, `physical/`)
- All pullers must store data with valid `observation_date` AND `release_date` timestamps
- Use the scheduler pattern in `ingestion/scheduler.py` (not `scheduler_v2.py` which is deprecated)
- Handle API rate limits gracefully with exponential backoff
- Missing API keys should log a warning but not crash the system (graceful degradation)

### Gotchas

- `_resolve_source_id()` and `_row_exists()` are copy-pasted across every puller — follow the existing pattern, but don't create new variations (ATTENTION.md #11)
- `_resolve_source_id()` auto-creates source_catalog entries if missing — unknown sources appear without operator awareness (#25)
- `pd.to_numeric(errors="coerce")` silently converts bad data to NaN — add `log.warning()` when coercion occurs (#13)
- Only `FRED_API_KEY` is validated at startup — KOSIS, Comtrade, JQUANTS, USDA, NOAA, EIA keys are not (#7)

## NaN Handling

NaN handling is inconsistent across the codebase (ATTENTION.md #14):
- `discovery/orthogonality.py:156` uses `ffill(limit=5)`
- `discovery/clustering.py:114` uses `ffill().dropna()`
- `features/lab.py` varies by transformation

**Rule**: When modifying a module, follow that module's existing NaN strategy. Do not introduce a new approach without updating all related modules.

## Conflict Resolution

- When the same economic indicator comes from multiple sources, `normalization/resolver.py` resolves conflicts
- Entity disambiguation uses `normalization/entity_map.py` — add new mappings there
- The 0.5% fixed threshold in resolver.py false-positives on high-volatility features (VIX, commodities) — make threshold configurable per feature when fixing (#15)
- Division by zero when reference value is 0 in resolver.py:139-142 is only partially handled (#20)
