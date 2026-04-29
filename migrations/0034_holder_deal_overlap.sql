-- Migration: 0034_holder_deal_overlap
-- Purpose: Detect "pre-positioning" where the same 13F filer held
-- material positions in BOTH the acquirer and the target BEFORE the
-- acquisition was announced. Cross-references two tables:
--
--   institutional_holdings  — 13F snapshots per (holder, ticker, report_date)
--   capital_flows           — period_type='announcement' + flow_type='acquisitions'
--                             rows are M&A deal events with acquirer=actor_id
--                             and target=counterparty_id.
--
-- The overlap detector writes one row per (deal, filer) pair where the
-- filer held BOTH sides as of the latest 13F snapshot before the
-- announcement. Quick-exit flag is set when the NEXT 13F report (the
-- quarter after the deal) shows the position liquidated.
--
-- Idempotent: the UNIQUE constraint + ON CONFLICT in the detector
-- allows the daily Hermes job to refresh in place. Runs in
-- intelligence/holder_deal_overlap.py + scripts/run_holder_deal_overlap.py.

CREATE TABLE IF NOT EXISTS holder_deal_overlap (
    id                            SERIAL PRIMARY KEY,
    deal_announcement_date        DATE NOT NULL,
    acquirer_ticker               TEXT NOT NULL,
    target_ticker                 TEXT NOT NULL,
    filer_name                    TEXT NOT NULL,
    acquirer_position_value_usd   NUMERIC,
    target_position_value_usd     NUMERIC,
    holding_report_date           DATE,
    days_before_announcement      INT,
    pre_position_flag             BOOLEAN DEFAULT false,
    quick_exit_flag               BOOLEAN DEFAULT false,
    narrative                     TEXT,
    as_of                         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (deal_announcement_date, acquirer_ticker, target_ticker, filer_name)
);

CREATE INDEX IF NOT EXISTS idx_holder_overlap_filer
    ON holder_deal_overlap(filer_name);
CREATE INDEX IF NOT EXISTS idx_holder_overlap_pre
    ON holder_deal_overlap(pre_position_flag) WHERE pre_position_flag = true;
CREATE INDEX IF NOT EXISTS idx_holder_overlap_acquirer
    ON holder_deal_overlap(acquirer_ticker);
CREATE INDEX IF NOT EXISTS idx_holder_overlap_target
    ON holder_deal_overlap(target_ticker);
CREATE INDEX IF NOT EXISTS idx_holder_overlap_deal_date
    ON holder_deal_overlap(deal_announcement_date DESC);

-- ====== GRANT FOOTER (REQUIRED — DO NOT SKIP) ======
-- Migrations run as `postgres`; the API and ingestors connect as `grid`.
-- Without these grants the `grid` user gets `permission denied for table`.
GRANT ALL ON holder_deal_overlap TO grid;
GRANT USAGE, SELECT ON SEQUENCE holder_deal_overlap_id_seq TO grid;
