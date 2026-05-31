-- =============================================================================
-- Migration: 0055_community_summary.sql
-- Purpose:   Cache the /api/v1/intelligence/communities aggregation.
--
--   The live endpoint (store.graph.get_community_list) runs a GROUP BY over
--   actor_analytics (~2.7M rows → ~44K community groups) AND then issues one
--   extra "top member" query PER community (a 44K-query N+1). That is far too
--   slow for an interactive request.
--
--   This migration adds a materialized summary table plus a refresh function
--   that recomputes the whole thing in a SINGLE pass (a window function picks
--   the top member per community — no per-community round trip). The endpoint
--   reads this table and falls back to the live aggregation when it is empty
--   or missing.
--
-- Database:  griddb, user=grid
-- Applies via: sudo -u postgres psql griddb -f migrations/0055_community_summary.sql
-- NOTE:      DO NOT run against the live DB as part of this change set — this
--            file is the artifact; scheduling/refresh is operational.
-- Date:      2026-05-26
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- SECTION 1: SUMMARY TABLE
-- One row per community. ``refreshed_at`` lets the reader reason about
-- staleness; the endpoint also keeps a short in-process TTL cache on top.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS community_summary (
    community_id   BIGINT PRIMARY KEY,
    member_count   INTEGER     NOT NULL,
    max_pagerank   DOUBLE PRECISION NOT NULL DEFAULT 0,
    top_member     TEXT,
    top_category   TEXT,
    refreshed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Sorted reads (endpoint returns communities largest-first).
CREATE INDEX IF NOT EXISTS idx_community_summary_member_count
    ON community_summary (member_count DESC);

-- ---------------------------------------------------------------------------
-- SECTION 2: REFRESH FUNCTION
-- Single-pass recompute: a GROUP BY for counts/max-pagerank, joined to a
-- DISTINCT ON (community_id) ... ORDER BY pagerank DESC pick for the top
-- member. Replaces the 44K-query N+1 with two index-assisted scans.
-- Returns the row count for logging/observability.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION refresh_community_summary()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    n INTEGER;
BEGIN
    -- Full rebuild is cheap relative to the per-request N+1 it replaces, and
    -- keeps the table trivially consistent. TRUNCATE + INSERT inside the
    -- function's implicit transaction.
    TRUNCATE community_summary;

    INSERT INTO community_summary
        (community_id, member_count, max_pagerank, top_member, top_category, refreshed_at)
    SELECT
        agg.community_id,
        agg.member_count,
        agg.max_pagerank,
        top.name        AS top_member,
        top.category    AS top_category,
        NOW()           AS refreshed_at
    FROM (
        SELECT aa.community_id,
               COUNT(*)              AS member_count,
               COALESCE(MAX(aa.pagerank), 0) AS max_pagerank
        FROM actor_analytics aa
        WHERE aa.community_id IS NOT NULL
        GROUP BY aa.community_id
    ) agg
    LEFT JOIN LATERAL (
        SELECT a.name, a.category
        FROM actor_analytics aa2
        JOIN actors a ON a.id = aa2.actor_id
        WHERE aa2.community_id = agg.community_id
        ORDER BY aa2.pagerank DESC NULLS LAST
        LIMIT 1
    ) top ON TRUE;

    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END;
$$;

COMMIT;

-- ---------------------------------------------------------------------------
-- SECTION 3: INITIAL POPULATION (run once, outside the schema transaction)
-- Uncomment to populate immediately after applying the schema. Operationally
-- this is invoked on a schedule (e.g. the actor-analytics rebuild job) so the
-- cache stays warm.
-- ---------------------------------------------------------------------------

-- SELECT refresh_community_summary();
