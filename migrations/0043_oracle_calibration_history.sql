-- Migration: 0043_oracle_calibration_history.sql
-- Purpose: ALPHA-7 / task #110 — persist per-horizon calibration snapshots
-- over time so drift alerts have a historical baseline to compare against.
--
-- The ALPHA-3 (task #106) migration added `horizon_buckets` to oracle_models
-- as a living jsonb snapshot of the current metrics. This migration adds a
-- time-series table that records a daily snapshot of the same values so the
-- drift detector can compute a mean/std baseline and flag 2σ+ departures.
--
-- Idempotent via IF NOT EXISTS. Target DB: griddb.

CREATE TABLE IF NOT EXISTS oracle_calibration_history (
    id              BIGSERIAL PRIMARY KEY,
    model_name      TEXT NOT NULL,
    horizon_days    INTEGER NOT NULL,
    snapshot_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    brier           DOUBLE PRECISION,
    ece             DOUBLE PRECISION,
    scored_count    INTEGER DEFAULT 0,
    bucket_weight   DOUBLE PRECISION,
    CONSTRAINT oracle_calibration_history_unique UNIQUE (model_name, horizon_days, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_och_model_horizon
    ON oracle_calibration_history (model_name, horizon_days, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_och_snapshot_at
    ON oracle_calibration_history (snapshot_at DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
GRANT ALL ON oracle_calibration_history TO grid;
GRANT USAGE, SELECT ON SEQUENCE oracle_calibration_history_id_seq TO grid;
