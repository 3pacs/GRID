-- Signal Forecasts table for TimesFM predictions.
-- One row per feature (latest forecast), upserted on each run.

CREATE TABLE IF NOT EXISTS signal_forecasts (
    feature_id       INTEGER PRIMARY KEY REFERENCES feature_registry(id),
    feature_name     TEXT NOT NULL,
    horizon          INTEGER NOT NULL DEFAULT 30,
    point_forecast   DOUBLE PRECISION[] NOT NULL,
    quantile_10      DOUBLE PRECISION[] NOT NULL,
    quantile_50      DOUBLE PRECISION[] NOT NULL,
    quantile_90      DOUBLE PRECISION[] NOT NULL,
    last_observed    DOUBLE PRECISION NOT NULL,
    last_obs_date    DATE NOT NULL,
    forecast_start_date DATE NOT NULL,
    direction        TEXT NOT NULL CHECK (direction IN ('UP', 'DOWN', 'FLAT')),
    expected_move_pct DOUBLE PRECISION NOT NULL,
    confidence_band_pct DOUBLE PRECISION NOT NULL,
    generated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for freshness checks
CREATE INDEX IF NOT EXISTS idx_signal_forecasts_generated
    ON signal_forecasts (generated_at DESC);

-- Index for family lookups via feature_registry join
CREATE INDEX IF NOT EXISTS idx_signal_forecasts_feature_name
    ON signal_forecasts (feature_name);

COMMENT ON TABLE signal_forecasts IS
    'TimesFM zero-shot forecasts per feature. Upserted every 4h by Hermes.';
