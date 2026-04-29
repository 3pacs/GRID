-- CAT-180 — per-horizon, per-confidence-bucket calibration history.
--
-- Sharper than Brier score: detects over/under-confidence in specific
-- probability bins ("when the oracle says 0.75, does it actually hit
-- 75% of the time?"). Feeds the conviction multiplier that discounts
-- systematically overconfident predictions.
--
-- Row grain: (horizon_days, bucket_low, bucket_high). Running Welford
-- updates on every newly scored prediction. n_hits is NUMERIC because
-- partial verdicts contribute 0.5, not 0 or 1.

CREATE TABLE IF NOT EXISTS confidence_bucket_history (
    id SERIAL PRIMARY KEY,
    horizon_days INT NOT NULL,
    bucket_low NUMERIC(4,2) NOT NULL,
    bucket_high NUMERIC(4,2) NOT NULL,
    n_predictions INT NOT NULL DEFAULT 0,
    n_hits NUMERIC(10, 2) NOT NULL DEFAULT 0,  -- float because partial=0.5
    running_brier NUMERIC(8,6),
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(horizon_days, bucket_low, bucket_high)
);

CREATE INDEX IF NOT EXISTS idx_confidence_bucket_last_updated
    ON confidence_bucket_history(last_updated);
