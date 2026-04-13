-- Migration: 0045_oracle_regime_buckets
-- Purpose: Per-regime sub-oracle routing — add regime_buckets JSONB column on
--          oracle_models. ALPHA-13 / task #116. Closes Phase 0 of the Tier A
--          shortlist (#109 in INTELLIGENCE.md catalog).
--
-- Today the ALPHA-3 horizon-conditional oracle routes per-horizon weights via
-- oracle_models.horizon_buckets, and the ALPHA-5 liquidity_regime classifier
-- dampens confidence post-hoc. ALPHA-13 takes the final step: every model
-- head gets its own per-regime weight multiplier so the ensemble can
-- specialise its weights to the prevailing liquidity state instead of
-- running one set of weights through every regime.
--
-- The five canonical regime states come from
-- intelligence/liquidity_regime.py (ALL_STATES):
--
--     CRISIS, TIGHTENING, NEUTRAL, EXPANSION, EXPANSION_STRONG
--
-- Additive only. The legacy weight / running_brier / running_ece columns
-- continue to be updated by the horizon bucket path — regime routing is a
-- multiplicative layer on top of horizon routing at vote-assembly time.
--
-- Apply via:
--   ssh grid@100.75.185.36 \
--     "psql -d griddb -f /data/grid_v4/grid_repo/migrations/0045_oracle_regime_buckets.sql"

BEGIN;

-- ── Schema change ────────────────────────────────────────────────────────

ALTER TABLE oracle_models
    ADD COLUMN IF NOT EXISTS regime_buckets JSONB;

COMMENT ON COLUMN oracle_models.regime_buckets IS
    'Per-regime weight multiplier buckets. '
    'Shape: {"CRISIS": {"weight": 1.0, ...}, "TIGHTENING": {...}, '
    '"NEUTRAL": {...}, "EXPANSION": {...}, "EXPANSION_STRONG": {...}}. '
    'ALPHA-13 / task #116. Multiplied with the ALPHA-3 horizon bucket '
    'weight in EnsemblePredictor.predict at vote-assembly time.';

-- Backfill any row that is missing the column to a freshly initialised
-- bucket set. Each regime starts at weight=1.0 with zero counters so the
-- predict path produces identical math to the horizon-only baseline until
-- the first PredictionScored event with a populated regime tag lands.
UPDATE oracle_models
SET regime_buckets = jsonb_build_object(
    'CRISIS', jsonb_build_object(
        'weight', 1.0, 'hits', 0, 'misses', 0, 'partials', 0,
        'scored', 0, 'brier', 0.0, 'ece', 0.0),
    'TIGHTENING', jsonb_build_object(
        'weight', 1.0, 'hits', 0, 'misses', 0, 'partials', 0,
        'scored', 0, 'brier', 0.0, 'ece', 0.0),
    'NEUTRAL', jsonb_build_object(
        'weight', 1.0, 'hits', 0, 'misses', 0, 'partials', 0,
        'scored', 0, 'brier', 0.0, 'ece', 0.0),
    'EXPANSION', jsonb_build_object(
        'weight', 1.0, 'hits', 0, 'misses', 0, 'partials', 0,
        'scored', 0, 'brier', 0.0, 'ece', 0.0),
    'EXPANSION_STRONG', jsonb_build_object(
        'weight', 1.0, 'hits', 0, 'misses', 0, 'partials', 0,
        'scored', 0, 'brier', 0.0, 'ece', 0.0)
)
WHERE regime_buckets IS NULL;

-- Index the column so per-regime drift reporting can use a GIN path
-- instead of sequential scans. JSONB path-ops class is sufficient for the
-- `regime_buckets -> '<state>' -> 'weight'` lookups used by the router.
CREATE INDEX IF NOT EXISTS idx_oracle_models_regime_buckets
    ON oracle_models USING GIN (regime_buckets jsonb_path_ops);

-- ── GRANT footer (required) ──────────────────────────────────────────────

GRANT ALL ON oracle_models TO grid;

COMMIT;
