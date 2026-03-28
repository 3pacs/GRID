# Add New Data Source

Scaffold a new ingestion module following GRID conventions.

## Arguments

Provide the source name and category (international, altdata, trade, physical).

## Instructions

1. Determine the correct subdirectory under `grid/ingestion/`
2. Create the ingestion module following the pattern from existing pullers:
   - `_resolve_source_id()` for source catalog lookup
   - `_row_exists()` for deduplication
   - `pull()` method as the main entry point
   - Proper `observation_date` and `release_date` timestamps for PIT correctness
3. Add entity mappings in `normalization/entity_map.py`
4. Add the source to `ingestion/scheduler.py`
5. Create a test file in `grid/tests/` with:
   - Mocked API responses (never hit live endpoints)
   - Timestamp parsing validation
   - Deduplication logic test
6. Verify PIT compatibility: data must have both observation and release dates
7. Update `grid/README.md` data sources table
