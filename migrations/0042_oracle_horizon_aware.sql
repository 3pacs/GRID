-- Migration: 0042_oracle_horizon_aware
-- Purpose: Horizon-conditional oracle — per-horizon weight / calibration
--          buckets on oracle_models. ALPHA-3 / task #106.
--
-- Today every OracleEngine prediction collapses 1d / 7d / 30d / 90d horizons
-- through a single oracle_models.weight column. The Tier A shortlist puts
-- horizon-conditional weights at the #1 inference upgrade (2-4% Brier lift
-- oracle-wide). This migration adds a JSONB horizon_buckets column storing
-- per-horizon {weight, hits, misses, partials, scored, brier, ece} and
-- defaults every existing row to four initialised buckets so the event path
-- can start nudging immediately.
--
-- Additive only. The legacy weight / running_brier / running_ece columns are
-- kept in sync as the unweighted average across buckets so existing callers
-- (scoreboard, report generator, legacy tests) see a representative scalar.
--
-- Apply via:
--   ssh grid@100.75.185.36 \
--     "psql -d griddb -f /data/grid_v4/grid_repo/migrations/0042_oracle_horizon_aware.sql"

BEGIN;

-- ── Schema change ────────────────────────────────────────────────────────

ALTER TABLE oracle_models
    ADD COLUMN IF NOT EXISTS horizon_buckets JSONB;

COMMENT ON COLUMN oracle_models.horizon_buckets IS
    'Per-horizon weight + calibration buckets. '
    'Shape: {"1d": {weight, hits, misses, partials, scored, brier, ece}, '
    '"7d": {...}, "30d": {...}, "90d": {...}}. '
    'ALPHA-3 / task #106. Legacy scalar columns (weight, running_brier, '
    'running_ece) stay in sync as the unweighted average across buckets.';

-- Backfill any row that is missing the column to a freshly initialised
-- bucket set. Each bucket starts at weight=1.0 with zero counters so the
-- event path produces identical math to the legacy scalar column until
-- the first PredictionScored event lands for that (model, horizon) pair.
UPDATE oracle_models
SET horizon_buckets = jsonb_build_object(
    '1d',  jsonb_build_object(
        'weight', 1.0, 'hits', 0, 'misses', 0, 'partials', 0,
        'scored', 0, 'brier', 0.0, 'ece', 0.0),
    '7d',  jsonb_build_object(
        'weight', 1.0, 'hits', 0, 'misses', 0, 'partials', 0,
        'scored', 0, 'brier', 0.0, 'ece', 0.0),
    '30d', jsonb_build_object(
        'weight', 1.0, 'hits', 0, 'misses', 0, 'partials', 0,
        'scored', 0, 'brier', 0.0, 'ece', 0.0),
    '90d', jsonb_build_object(
        'weight', 1.0, 'hits', 0, 'misses', 0, 'partials', 0,
        'scored', 0, 'brier', 0.0, 'ece', 0.0)
)
WHERE horizon_buckets IS NULL;

-- Index the column so per-horizon drift reporting can use a GIN path
-- instead of sequential scans. JSONB path-ops class is sufficient for the
-- `horizon_buckets -> '<bucket>' -> 'weight'` lookups used by evolve_weights.
CREATE INDEX IF NOT EXISTS idx_oracle_models_horizon_buckets
    ON oracle_models USING GIN (horizon_buckets jsonb_path_ops);

-- ── GRANT footer (required) ──────────────────────────────────────────────

GRANT ALL ON oracle_models TO grid;

COMMIT;
