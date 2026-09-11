-- Migration: 0058_horizon_days.sql
-- Author: lever-package sprint 2 (LEVER-PACKAGE §7 T2.1 / T2.2)
-- Applies via: sudo -u postgres psql griddb -f migrations/0058_horizon_days.sql
--
-- Explicit forecast horizon on the two prediction tables the long-horizon
-- engine writes to:
--
--   oracle_predictions.horizon_days       — calendar days from creation to
--                                           expiry. NULL on legacy rows (they
--                                           all expired at the next monthly
--                                           options expiry, ≤35 d); derived
--                                           from expiry for new rows when not
--                                           requested explicitly. Written by
--                                           oracle/engine.py::_store_predictions.
--   universe_ranking_history.horizon_days — the horizon handed to
--                                           should_i_trade for every ticker in
--                                           the sweep. 7 = the legacy weekday
--                                           sweep; 90 = the Sunday 05:00
--                                           long-horizon sweep in
--                                           intelligence/scheduler.py. Written
--                                           by intelligence/universe_ranker.py.
--
-- Both code paths also issue these ALTERs at startup (ADD COLUMN IF NOT
-- EXISTS), so this file exists for the audit trail and for hosts where the
-- schema is managed by migrations only. Idempotent on re-run.

-- ====== SCHEMA CHANGES ======

ALTER TABLE oracle_predictions
    ADD COLUMN IF NOT EXISTS horizon_days INTEGER;

ALTER TABLE universe_ranking_history
    ADD COLUMN IF NOT EXISTS horizon_days INTEGER NOT NULL DEFAULT 7;

-- Backfill the legacy oracle rows from their expiry so per-horizon
-- calibration (oracle/calibration.py) can bucket them.
UPDATE oracle_predictions
SET horizon_days = GREATEST(1, (expiry - (created_at AT TIME ZONE 'UTC')::date))
WHERE horizon_days IS NULL
  AND expiry IS NOT NULL
  AND created_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_oracle_predictions_horizon_days
    ON oracle_predictions (horizon_days);

CREATE INDEX IF NOT EXISTS idx_universe_ranking_history_horizon_days
    ON universe_ranking_history (horizon_days, generated_at DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- No new tables or sequences: both tables are already owned/granted to the
-- `grid` role. Re-issuing the grants is a no-op and keeps the lint happy.
GRANT ALL ON oracle_predictions TO grid;
GRANT ALL ON universe_ranking_history TO grid;
