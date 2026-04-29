-- Migration: 0026_contagion_predictions
-- Purpose: Persist every chain_contagion simulation and score predictions
--          7/14/30 days later against actual downstream price moves.
--
-- Populated by: api.routers.contagion.simulate (writes predictions),
--               intelligence.contagion_backtest.score_predictions (writes results).
-- Consumed by:  api.routers.contagion.backtest (accuracy dashboard).
--
-- Idempotent: all DDL uses IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS contagion_predictions (
    id SERIAL PRIMARY KEY,
    shock_node TEXT NOT NULL,
    shock_type TEXT NOT NULL,
    magnitude NUMERIC NOT NULL,
    max_depth INT NOT NULL,
    simulated_at TIMESTAMPTZ DEFAULT NOW(),
    summary JSONB NOT NULL,         -- the "summary" block from simulate_contagion
    ranked_impact JSONB NOT NULL,   -- the "ranked_impact" block
    source TEXT,                     -- "api" | "news_listener" | "scheduled_scenario"
    caller_id TEXT                   -- optional: user id or agent id
);
CREATE INDEX IF NOT EXISTS idx_contagion_predictions_shock ON contagion_predictions(shock_node, simulated_at DESC);
CREATE INDEX IF NOT EXISTS idx_contagion_predictions_simulated_at ON contagion_predictions(simulated_at DESC);

CREATE TABLE IF NOT EXISTS contagion_backtest_results (
    id SERIAL PRIMARY KEY,
    prediction_id INT REFERENCES contagion_predictions(id) ON DELETE CASCADE,
    ticker TEXT NOT NULL,
    predicted_margin_impact_pct NUMERIC,
    predicted_revenue_at_risk_usd NUMERIC,
    scored_at_days INT NOT NULL,      -- 7, 14, or 30
    actual_price_move_pct NUMERIC,    -- the real downstream price move over the window
    price_start NUMERIC,
    price_end NUMERIC,
    accuracy_score NUMERIC,           -- directional correctness, 0..1
    scored_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (prediction_id, ticker, scored_at_days)
);
CREATE INDEX IF NOT EXISTS idx_contagion_backtest_prediction ON contagion_backtest_results(prediction_id);
CREATE INDEX IF NOT EXISTS idx_contagion_backtest_ticker ON contagion_backtest_results(ticker, scored_at DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON contagion_predictions      TO grid;
GRANT ALL ON contagion_backtest_results TO grid;
GRANT USAGE, SELECT ON SEQUENCE contagion_predictions_id_seq      TO grid;
GRANT USAGE, SELECT ON SEQUENCE contagion_backtest_results_id_seq TO grid;
