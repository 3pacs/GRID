-- Migration: 0033_fundamental_divergence
-- Author: intelligence/fundamental_divergence.py
-- Applies via: sudo -u postgres psql griddb -f migrations/0033_fundamental_divergence.sql
--
-- Purpose: Stores the output of the fundamental-vs-price divergence engine.
-- Each row is one ticker at one as_of date with:
--   * fundamental_score (0..100) — weighted composite of revenue_3y_cagr
--     percentile, gross_margin_trend, and shareholder_yield percentile,
--     all percentile-ranked within the ticker's GICS sector.
--   * price_score (0..100) — 3y stock CAGR percentile within sector.
--   * divergence = fundamental_score − price_score.
--   * classification: long_candidate when divergence > +30 (fundamentals
--     strong, price lagging → mispriced cheap), short_candidate when
--     divergence < −30 (fundamentals weak, price ripping → mispriced
--     expensive), aligned otherwise.
--
-- Idempotent: CREATE IF NOT EXISTS + UNIQUE (ticker, as_of) lets the
-- runner upsert daily without creating duplicates.

CREATE TABLE IF NOT EXISTS fundamental_divergence (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    as_of DATE NOT NULL,
    sector TEXT,
    fundamental_score NUMERIC,
    price_score NUMERIC,
    divergence NUMERIC,
    classification TEXT,  -- long_candidate | short_candidate | aligned
    narrative TEXT,
    UNIQUE (ticker, as_of)
);

CREATE INDEX IF NOT EXISTS idx_fund_div_class ON fundamental_divergence(classification);
CREATE INDEX IF NOT EXISTS idx_fund_div_divergence ON fundamental_divergence(divergence DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON fundamental_divergence TO grid;
GRANT USAGE, SELECT ON SEQUENCE fundamental_divergence_id_seq TO grid;
