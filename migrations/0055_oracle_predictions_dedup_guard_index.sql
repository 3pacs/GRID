-- Migration: 0055_oracle_predictions_dedup_guard_index.sql
-- Author: lane-d (claude/lane-d-oracle-dedup-20260527)
-- Purpose: Stop oracle_predictions DUPLICATE WRITES at the source by ensuring
--          the natural-key partial unique index exists on the live DB.
-- Applies via: sudo -u postgres psql griddb -f migrations/0055_oracle_predictions_dedup_guard_index.sql
--
-- Background
-- ----------
-- The dedup design (commit 3f20da4d, docs/TODO-DUP-WRITES.md) added:
--   1. a `dedup_keep` BOOLEAN column (default TRUE),
--   2. a backfill marking historical duplicates dedup_keep=FALSE, and
--   3. a PARTIAL UNIQUE INDEX `oracle_predictions_dedup_unique` so new inserts
--      that collide with an existing keep=TRUE row hit ON CONFLICT instead of
--      writing a fresh UUID duplicate.
-- All three writers (oracle/engine.py, oracle/publish.py,
-- intelligence/obsidian_agent.py) already emit the matching
--   ON CONFLICT (...) WHERE dedup_keep = TRUE DO UPDATE ...
-- clause. The remaining gap is operational: the index from
-- migrations/versions/oracle_predictions_dedup.sql was not applied to the live
-- DB, so ON CONFLICT has no arbiter and the WRITE path keeps inserting dupes
-- (and would raise 42P10 on the non-engine writers). This migration closes that
-- gap idempotently. It is a no-op if the index already exists.
--
-- Index definition MUST stay byte-for-byte identical to the bootstrap in
-- oracle/engine.py:_ensure_tables, oracle/dedup_index.py, and
-- migrations/versions/oracle_predictions_dedup.sql, so the writers' ON CONFLICT
-- arbiter always resolves to this exact index.
--
-- CONCURRENTLY: CREATE INDEX CONCURRENTLY cannot run inside a transaction
-- block. Run this file OUTSIDE a transaction (plain `psql -f`, which autocommits
-- each statement). Do NOT wrap it in BEGIN/COMMIT and do NOT feed it through a
-- migration runner that opens a transaction. CONCURRENTLY lets the build
-- proceed without blocking concurrent INSERT/UPDATE/SELECT on oracle_predictions
-- (it takes only a brief SHARE UPDATE EXCLUSIVE lock), which matters because a
-- postmortem drain is actively writing this table.
--
-- Verified pre-apply (2026-05-27): the 6,881 dedup_keep=TRUE rows are already
-- unique on this natural key (0 duplicate groups among keepers), so the unique
-- build will succeed without an INVALID index. The 2.31M dedup_keep=FALSE rows
-- are excluded by the partial predicate and do not affect the build.

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS oracle_predictions_dedup_unique
    ON oracle_predictions (
        ticker,
        direction,
        expiry,
        prediction_type,
        (COALESCE(model_version, '')),
        ((created_at AT TIME ZONE 'UTC')::date)
    )
    WHERE dedup_keep = TRUE;

-- Helper index for the dedup-aware consumer queries (walk_forward_profitability,
-- calibration, etc.) that filter WHERE dedup_keep = TRUE ORDER BY created_at.
CREATE INDEX CONCURRENTLY IF NOT EXISTS oracle_predictions_dedup_keep_created
    ON oracle_predictions (created_at DESC)
    WHERE dedup_keep = TRUE;

-- No GRANT footer required: this migration creates only indexes on an existing
-- table (oracle_predictions already granted to `grid`); it does not create any
-- new table or sequence.
