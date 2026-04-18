-- Migration: institutional_holdings
-- Purpose: Structured per-holder / per-ticker 13F holdings so the sector
-- connection graph in api/routers/flows.py (_build_sector_connections)
-- can emit common_13f_holder edges between tickers that share institutional
-- holders.
--
-- Column naming mirrors the query in flows.py:
--     SELECT ticker, holder_name, shares_held FROM institutional_holdings ...
--
-- We also keep report_date, filed_date, value_usd, and cik for provenance
-- and PIT queries.

CREATE TABLE IF NOT EXISTS institutional_holdings (
    id            BIGSERIAL PRIMARY KEY,
    cik           TEXT,
    holder_name   TEXT NOT NULL,
    ticker        TEXT NOT NULL,
    cusip         TEXT,
    shares_held   BIGINT,
    value_usd     NUMERIC,
    report_date   DATE NOT NULL,
    filed_date    DATE,
    source        TEXT DEFAULT 'sec_13f',
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_inst_holdings_ticker
    ON institutional_holdings (ticker);
CREATE INDEX IF NOT EXISTS ix_inst_holdings_cik
    ON institutional_holdings (cik);
CREATE INDEX IF NOT EXISTS ix_inst_holdings_report_date
    ON institutional_holdings (report_date DESC);
CREATE INDEX IF NOT EXISTS ix_inst_holdings_holder_ticker
    ON institutional_holdings (holder_name, ticker);

-- Dedupe guard: one row per (holder, ticker, report_date).
CREATE UNIQUE INDEX IF NOT EXISTS ux_inst_holdings_holder_ticker_date
    ON institutional_holdings (holder_name, ticker, report_date);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON institutional_holdings TO grid;
GRANT USAGE, SELECT ON SEQUENCE institutional_holdings_id_seq TO grid;
