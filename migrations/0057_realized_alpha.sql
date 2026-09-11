-- Migration: 0057_realized_alpha.sql
-- Author: realized-alpha agent (GRID-4 pivot §8.1 / LEVER-PACKAGE §7 T0.1)
-- Applies via: sudo -u postgres psql griddb -f migrations/0057_realized_alpha.sql
--
-- The realized-alpha truth gate. Written daily by
-- alpha_research/realized_alpha.py (scheduled 06:30 UTC in
-- intelligence/scheduler.py), read by GET /api/v1/alpha/realized.
--
--   realized_alpha_daily  — one row per (as_of, source, horizon_days):
--                            equal-weight mean alpha vs SPY, net of
--                            cost_bps/side, over the trailing horizon.
--   realized_alpha_trades — one row per (as_of, source, source_id): the
--                            per-trade decomposition behind each daily row.
--
-- Idempotent: every statement uses IF NOT EXISTS; GRANTs are no-ops on re-run.

-- ====== SCHEMA CHANGES ======

CREATE TABLE IF NOT EXISTS realized_alpha_daily (
    as_of                   DATE NOT NULL,
    source                  TEXT NOT NULL,
    horizon_days            INTEGER NOT NULL,
    n_trades                INTEGER NOT NULL DEFAULT 0,
    mean_alpha              DOUBLE PRECISION,
    mean_alpha_annualized   DOUBLE PRECISION,
    mean_gross              DOUBLE PRECISION,
    mean_spy                DOUBLE PRECISION,
    hit_rate                DOUBLE PRECISION,
    cost_bps                DOUBLE PRECISION NOT NULL DEFAULT 5.0,
    computed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (as_of, source, horizon_days)
);

CREATE INDEX IF NOT EXISTS idx_realized_alpha_daily_source_horizon
    ON realized_alpha_daily (source, horizon_days, as_of DESC);

CREATE TABLE IF NOT EXISTS realized_alpha_trades (
    id              BIGSERIAL PRIMARY KEY,
    as_of           DATE NOT NULL,
    source          TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    ticker          TEXT,
    entry_date      DATE,
    exit_date       DATE,
    is_open         BOOLEAN NOT NULL DEFAULT FALSE,
    gross_return    DOUBLE PRECISION,
    spy_return      DOUBLE PRECISION,
    alpha           DOUBLE PRECISION,
    cost_bps        DOUBLE PRECISION NOT NULL DEFAULT 5.0,
    UNIQUE (as_of, source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_realized_alpha_trades_exit
    ON realized_alpha_trades (source, exit_date DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======

GRANT ALL ON realized_alpha_daily TO grid;
GRANT ALL ON realized_alpha_trades TO grid;
GRANT USAGE, SELECT ON SEQUENCE realized_alpha_trades_id_seq TO grid;
