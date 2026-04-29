-- 2026-04-29: oracle_predictions natural-key dedup.
--
-- Problem (see docs/TODO-DUP-WRITES.md):
--   The same logical prediction event is written 23–180× under different
--   UUIDs because INSERTs only ON CONFLICT on `id`. Walk-forward metrics
--   were 24× inflated; one QQQ winner was echoed 590 times.
--
-- Solution: option C (model_version-aware, daily window):
--   dedup key = (ticker, direction, expiry, prediction_type,
--                COALESCE(model_version,''), DATE(created_at))
--
-- Strategy:
--   1. Add a `dedup_keep` boolean (default TRUE).
--   2. Backfill: keep the earliest row in each cluster, mark rest FALSE.
--   3. Add a PARTIAL UNIQUE INDEX `WHERE dedup_keep = TRUE`. This way
--      historical duplicates can stay in the table (audit, postmortem)
--      but new INSERTs that collide with an existing keep=TRUE row will
--      raise CONFLICT, which the application handles via ON CONFLICT
--      DO UPDATE.

BEGIN;

-- 1. Add dedup_keep
ALTER TABLE oracle_predictions
    ADD COLUMN IF NOT EXISTS dedup_keep BOOLEAN NOT NULL DEFAULT TRUE;

-- 2. Backfill: rank rows in each cluster by created_at ASC, keep rn=1
WITH ranked AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY ticker,
                            direction,
                            expiry,
                            prediction_type,
                            COALESCE(model_version, ''),
                            DATE(created_at)
               ORDER BY created_at ASC, id ASC
           ) AS rn
    FROM oracle_predictions
)
UPDATE oracle_predictions op
   SET dedup_keep = FALSE
  FROM ranked r
 WHERE op.id = r.id
   AND r.rn > 1;

COMMIT;

-- 3. Partial unique index — only one keep=TRUE row per (natural key, day).
--    CREATE INDEX CONCURRENTLY can't run inside a transaction, so this
--    sits outside the BEGIN/COMMIT block above.
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

-- 4. Helper index for the consumer queries (walk_forward_profitability et al.)
CREATE INDEX CONCURRENTLY IF NOT EXISTS oracle_predictions_dedup_keep_created
    ON oracle_predictions (created_at DESC)
    WHERE dedup_keep = TRUE;
