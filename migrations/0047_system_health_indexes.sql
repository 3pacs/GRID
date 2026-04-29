-- Migration: 0047_system_health_indexes.sql
-- Purpose: Keep health/pipeline freshness checks from scanning raw_series.
-- Applies via: sudo -u postgres psql griddb -f migrations/0047_system_health_indexes.sql
--
-- Run outside a transaction block.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_raw_series_pull_timestamp
    ON raw_series(pull_timestamp DESC);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_raw_series_status_source_pull
    ON raw_series(pull_status, source_id, pull_timestamp DESC);
