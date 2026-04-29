-- Migration: 0022_supply_shock_attributions
-- Purpose: Cross-lens attribution layer — connects upstream commodity/supplier
--          shocks to downstream ticker moves via lagged correlation and
--          event-study analysis. This is the "explained by" layer that closes
--          the loop between the supply chain lens and price action.
--
-- Written by: intelligence/cross_lens.py
-- Read by:    api/routers/attributions.py, frontend actor profile drawer
--
-- Idempotent: uses IF NOT EXISTS + UNIQUE constraint for ON CONFLICT upsert.

CREATE TABLE IF NOT EXISTS supply_shock_attributions (
    id SERIAL PRIMARY KEY,
    upstream_id TEXT NOT NULL,           -- shock source (commodity/supplier)
    downstream_id TEXT NOT NULL,         -- affected ticker
    shock_date DATE NOT NULL,
    shock_magnitude NUMERIC,             -- upstream % move over the window
    downstream_move_pct NUMERIC,         -- downstream % move over the following window
    lag_days INT,                        -- days from shock to downstream move
    correlation NUMERIC,                 -- rolling correlation value
    confidence TEXT,                     -- derived | inferred
    evidence TEXT,                       -- free text explanation
    method TEXT,                         -- "lagged_correlation" | "event_study"
    as_of TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (upstream_id, downstream_id, shock_date, method)
);

CREATE INDEX IF NOT EXISTS idx_ssa_downstream
    ON supply_shock_attributions(downstream_id, shock_date DESC);
CREATE INDEX IF NOT EXISTS idx_ssa_upstream
    ON supply_shock_attributions(upstream_id, shock_date DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON supply_shock_attributions TO grid;
GRANT USAGE, SELECT ON SEQUENCE supply_shock_attributions_id_seq TO grid;
