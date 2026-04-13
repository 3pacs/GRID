-- Migration: 0025_ticker_metrics_daily
-- Purpose: Daily per-ticker metrics derived from SEC XBRL shares outstanding
--          joined to raw_series close prices. Primary use case: market_cap_usd.
--
-- Populated by: ingestion.altdata.sec_xbrl_shares.SECXBRLSharesPuller
-- Consumed by:  api.routers.capital_flow (buyback_yield ratio),
--               api.routers.actor_detail (market_cap card).
--
-- Idempotent: all DDL uses IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS ticker_metrics_daily (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    obs_date DATE NOT NULL,
    shares_outstanding BIGINT,
    close_price NUMERIC,
    market_cap_usd NUMERIC,       -- shares × close_price
    source TEXT,                  -- e.g. "sec_xbrl + yfinance"
    as_of TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (ticker, obs_date)
);

CREATE INDEX IF NOT EXISTS idx_tmd_ticker
    ON ticker_metrics_daily(ticker, obs_date DESC);

CREATE INDEX IF NOT EXISTS idx_tmd_market_cap
    ON ticker_metrics_daily(market_cap_usd DESC)
    WHERE market_cap_usd IS NOT NULL;

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON ticker_metrics_daily TO grid;
GRANT USAGE, SELECT ON SEQUENCE ticker_metrics_daily_id_seq TO grid;
