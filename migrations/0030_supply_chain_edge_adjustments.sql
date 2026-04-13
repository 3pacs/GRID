-- Migration: 0030_supply_chain_edge_adjustments
-- Purpose: Audit trail for contagion-backtest-driven learning updates to
--          supply_chain_edges.pct_downstream_cogs. Every time the feedback
--          loop decays or validates an edge, one row is written here so we
--          can trace the full learning trajectory (old value -> new value,
--          which backtest row drove the change, what the miss magnitude was).
--
-- Populated by: intelligence.postmortem.apply_contagion_feedback
-- Consumed by:  edge-learning dashboards, operator audits, replay jobs.
--
-- Adds a boolean `backtest_validated` column to supply_chain_edges so that
-- edges confirmed by a high-accuracy backtest (>= 0.8) can be flagged
-- without rewriting the stored value.
--
-- Idempotent: every DDL uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS supply_chain_edge_adjustments (
    id                      SERIAL PRIMARY KEY,
    edge_id                 INT NOT NULL,
    upstream_id             TEXT NOT NULL,
    downstream_id           TEXT NOT NULL,
    prediction_id           INT,                 -- contagion_predictions.id (nullable for manual replays)
    backtest_result_id      INT,                 -- contagion_backtest_results.id (nullable)
    event_type              TEXT NOT NULL,       -- 'decay' | 'confirm'
    old_pct_downstream_cogs NUMERIC,
    new_pct_downstream_cogs NUMERIC,
    implied_pct_cogs        NUMERIC,             -- pct that would have produced the actual move
    accuracy_score          NUMERIC,             -- 0..1 score from contagion_backtest_results
    delta                   NUMERIC,             -- new - old (capped at +/- 0.02)
    capped                  BOOLEAN DEFAULT FALSE,
    reason                  TEXT,                -- human-readable note
    adjusted_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sc_edge_adj_edge
    ON supply_chain_edge_adjustments(edge_id, adjusted_at DESC);
CREATE INDEX IF NOT EXISTS idx_sc_edge_adj_prediction
    ON supply_chain_edge_adjustments(prediction_id);
CREATE INDEX IF NOT EXISTS idx_sc_edge_adj_event
    ON supply_chain_edge_adjustments(event_type, adjusted_at DESC);

ALTER TABLE supply_chain_edges
    ADD COLUMN IF NOT EXISTS backtest_validated BOOLEAN DEFAULT FALSE;
ALTER TABLE supply_chain_edges
    ADD COLUMN IF NOT EXISTS last_backtest_at TIMESTAMPTZ;

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON supply_chain_edge_adjustments TO grid;
GRANT USAGE, SELECT ON SEQUENCE supply_chain_edge_adjustments_id_seq TO grid;
