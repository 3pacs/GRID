-- CAT-193 / #295 — Meta learning matrix
--
-- Per-signal × per-condition edge learner. Complements per_signal_brier_history
-- (which measures calibration) with a per-condition-cell edge score (which
-- measures whether trusting a signal in a given (liquidity × FCI × volatility
-- × horizon) cell actually makes money).
--
-- The matrix stores a running aggregate per (signal_source, horizon_bucket,
-- liquidity_regime, fci_bucket, vol_regime) cell. Each scored prediction
-- updates the matching cell with firings/hits/scaled-edge. The read path
-- resolves a weight multiplier in [0.4, 1.5] for the live oracle stack.

CREATE TABLE IF NOT EXISTS meta_learning_matrix (
    id SERIAL PRIMARY KEY,
    signal_source TEXT NOT NULL,
    horizon_bucket TEXT NOT NULL,        -- SHORT / MID / LONG
    liquidity_regime TEXT NOT NULL,      -- 5-state: CRISIS / TIGHTENING / NEUTRAL / EXPANSION / EXPANSION_STRONG
    fci_bucket TEXT NOT NULL,            -- TIGHT / NEUTRAL / EASY
    vol_regime TEXT NOT NULL,            -- LOW / NORMAL / HIGH
    n_predictions INT NOT NULL DEFAULT 0,
    n_firings INT NOT NULL DEFAULT 0,
    n_hits NUMERIC(10,2) NOT NULL DEFAULT 0,
    sum_scaled_edge NUMERIC(10,6) NOT NULL DEFAULT 0,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(signal_source, horizon_bucket, liquidity_regime, fci_bucket, vol_regime)
);

CREATE INDEX IF NOT EXISTS idx_meta_learning_signal ON meta_learning_matrix(signal_source);
CREATE INDEX IF NOT EXISTS idx_meta_learning_last_updated ON meta_learning_matrix(last_updated);
