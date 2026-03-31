-- ============================================================
-- Migration: add_missing_indexes.sql
-- Adds three indexes identified in the security/performance audit.
-- All use CREATE INDEX CONCURRENTLY — safe to run against a live
-- database with no table locks and no downtime.
--
-- Prerequisites:
--   - Must be run outside of a transaction block (CONCURRENTLY does
--     not work inside BEGIN/COMMIT).
--   - Run as a superuser or the table owner.
--
-- Usage:
--   psql $DATABASE_URL -f scripts/add_missing_indexes.sql
-- ============================================================

-- ------------------------------------------------------------
-- 1. resolved_series — LATEST_AS_OF PIT covering index
--
-- The existing idx_resolved_series_pit_covering indexes
-- (feature_id, obs_date, vintage_date ASC). PostgreSQL cannot
-- reverse-scan an ASC index to satisfy ORDER BY vintage_date DESC
-- inside DISTINCT ON, so the LATEST_AS_OF branch of PITStore.get_pit
-- falls back to a sequential scan or an uncovered index scan on every
-- backtest and live-inference call. This index eliminates that.
-- ------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_resolved_series_pit_latest
    ON resolved_series (feature_id, obs_date, vintage_date DESC)
    INCLUDE (value, release_date)
    WHERE release_date IS NOT NULL;

-- ------------------------------------------------------------
-- 2. validation_results — hypothesis + verdict lookup
--
-- The /models/from-hypothesis endpoint runs:
--   SELECT id FROM validation_results
--   WHERE hypothesis_id = :hid AND overall_verdict = 'PASS'
--   ORDER BY run_timestamp DESC LIMIT 1
--
-- The existing idx_validation_results_hypothesis covers only
-- hypothesis_id. Adding overall_verdict as the second key lets
-- Postgres apply both predicates during the index scan and read
-- the first matching row (newest PASS) directly from the index
-- without a table heap fetch for most cases.
-- ------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_validation_results_hyp_verdict_ts
    ON validation_results (hypothesis_id, overall_verdict, run_timestamp DESC);

-- ------------------------------------------------------------
-- 3. feature_registry — family + subfamily composite
--
-- The autoresearch prompt builder and taxonomy scripts filter or
-- group by (family, subfamily). The existing idx_feature_registry_family
-- covers single-family predicates as a prefix scan of this new
-- composite index, so no regression occurs on family-only queries.
-- ------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_feature_registry_family_subfamily
    ON feature_registry (family, subfamily);

-- ------------------------------------------------------------
-- Verification queries (run after migration completes)
-- ------------------------------------------------------------
-- SELECT indexname, indexdef
-- FROM pg_indexes
-- WHERE tablename IN ('resolved_series', 'validation_results', 'feature_registry')
--   AND indexname IN (
--       'idx_resolved_series_pit_latest',
--       'idx_validation_results_hyp_verdict_ts',
--       'idx_feature_registry_family_subfamily'
--   )
-- ORDER BY tablename, indexname;
