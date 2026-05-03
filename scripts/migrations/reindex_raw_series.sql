-- REINDEX raw_series unique composite index
--
-- Why: psycopg2.errors.IndexCorrupted "uq_raw_series_composite contains
-- unexpected zero page at block 8673041" was observed in Tiingo pulls
-- 2026-04-29 onwards. This is filesystem-level Postgres corruption
-- (typically caused by an unclean shutdown / disk full / fsync issue).
-- Every INSERT into raw_series fails until the index is rebuilt.
--
-- When to run: as soon as the Tiingo IndexCorrupted breaker trips
-- (one ERROR row in errors.jsonl from ingestion/tiingo_pull.py).
--
-- How to run on grid-svr:
--   ssh grid@grid-svr
--   cd ~/grid_v4
--   psql $DATABASE_URL -f scripts/migrations/reindex_raw_series.sql
--
-- Note: REINDEX CONCURRENTLY is non-blocking but takes minutes-to-hours
-- on a 2.2M-row table. It can be cancelled safely; partial work is rolled
-- back. If CONCURRENTLY fails for any reason, fall back to a plain
-- REINDEX which holds a stronger lock but always succeeds.

\timing on

REINDEX INDEX CONCURRENTLY uq_raw_series_composite;

-- Verify the rebuild
SELECT
    schemaname,
    relname AS table_name,
    indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    idx_scan AS scans_since_reset
FROM pg_stat_user_indexes
WHERE indexrelname = 'uq_raw_series_composite';
