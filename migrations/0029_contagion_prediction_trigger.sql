-- Migration: 0029_contagion_prediction_trigger
-- Purpose: Add news-article trigger provenance to contagion_predictions so
--          the news_contagion_listener can point every auto-fired simulation
--          back at the exact article that caused it.
--
-- Populated by: intelligence.news_contagion_listener.run_once
-- Consumed by:  api.routers.contagion.backtest (future filter by source)
--               and the frontend timeline / intelligence drawer.
--
-- Idempotent: IF NOT EXISTS on columns and index. Safe to re-run.

ALTER TABLE contagion_predictions
    ADD COLUMN IF NOT EXISTS trigger_news_id INT,
    ADD COLUMN IF NOT EXISTS trigger_url     TEXT;

-- One index per filter path: source='news_listener' scans need a fast lane
-- and trigger_news_id is used for dedup (one prediction per article).
CREATE INDEX IF NOT EXISTS idx_contagion_predictions_source
    ON contagion_predictions(source, simulated_at DESC);

CREATE INDEX IF NOT EXISTS idx_contagion_predictions_trigger_news_id
    ON contagion_predictions(trigger_news_id)
    WHERE trigger_news_id IS NOT NULL;

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON contagion_predictions TO grid;
GRANT USAGE, SELECT ON SEQUENCE contagion_predictions_id_seq TO grid;
