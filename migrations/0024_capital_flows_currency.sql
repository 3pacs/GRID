-- Migration: 0024_capital_flows_currency
-- Purpose: Add a source-currency column to capital_flows so IFRS foreign
-- issuers (20-F, 6-K) can record amounts in their reporting currency
-- alongside the USD-converted value already written to amount_usd.
--
-- Idempotent: IF NOT EXISTS on the column; default 'USD' so all legacy
-- rows (which are US GAAP only) are backfilled to USD implicitly.
--
-- Paired with the IFRS taxonomy support in
-- ``ingestion/altdata/sec_xbrl_financials.py`` — foreign issuers whose
-- facts come back in EUR/GBP/JPY/CHF/CAD/etc. are stored with
-- ``currency`` = the source currency and ``amount_usd`` = the FX-
-- converted value (FX from FRED DEX* series via raw_series).

ALTER TABLE capital_flows
    ADD COLUMN IF NOT EXISTS currency TEXT DEFAULT 'USD';

-- Backfill anything that might have slipped in as NULL during the
-- migration window.
UPDATE capital_flows SET currency = 'USD' WHERE currency IS NULL;

CREATE INDEX IF NOT EXISTS idx_capital_flows_currency
    ON capital_flows(currency) WHERE currency <> 'USD';

-- The original migration 0021 declared the unique key as
--    UNIQUE (actor_id, fiscal_period, period_type, flow_type,
--            counterparty_id, source_filing)
-- PostgreSQL treats NULLs as DISTINCT in UNIQUE constraints, which
-- means when the XBRL puller writes ``counterparty_id = NULL`` the
-- ON CONFLICT upsert silently turns into an INSERT — and re-running
-- the puller piles up duplicate rows. PG15 introduced
-- ``NULLS NOT DISTINCT`` but the griddb instance is on PG14, so we
-- solve it with a functional unique index using
-- ``COALESCE(counterparty_id, '__none__')`` instead. That collapses
-- NULL and '' together for dedup purposes and matches the same
-- COALESCE(NULLIF(counterparty_id,''),'__none__') the API router uses
-- in ``api/routers/capital_flow.py::_DEDUP_SQL``.
--
-- Safe on re-run:
--  - The functional index uses IF NOT EXISTS.
--  - We leave the original constraint in place if present (it still
--    works for non-NULL counterparties); rows with NULL counterparty
--    are uniquely enforced by the functional index below.

-- Pre-dedup existing duplicates so the unique index can be built.
-- Keep the row with the highest ``id`` (latest insert) per logical
-- key — the older row is the stale one. Any 8-K / seed loader
-- duplicates are collapsed here.
WITH dups AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY actor_id, fiscal_period, period_type, flow_type,
                         COALESCE(NULLIF(counterparty_id, ''), '__none__'),
                         source_filing
            ORDER BY
                CASE confidence
                    WHEN 'confirmed' THEN 1
                    WHEN 'derived'   THEN 2
                    WHEN 'estimated' THEN 3
                    WHEN 'rumored'   THEN 4
                    WHEN 'inferred'  THEN 5
                    ELSE 6
                END,
                as_of DESC NULLS LAST,
                id DESC
        ) AS rk
    FROM capital_flows
)
DELETE FROM capital_flows WHERE id IN (SELECT id FROM dups WHERE rk > 1);

CREATE UNIQUE INDEX IF NOT EXISTS capital_flows_dedup_nullable_cp_key
    ON capital_flows (
        actor_id,
        fiscal_period,
        period_type,
        flow_type,
        (COALESCE(NULLIF(counterparty_id, ''), '__none__')),
        source_filing
    );
