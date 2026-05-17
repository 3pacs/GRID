-- Task #186: bridge earnings_events → earnings_calendar consumers
--
-- Background:
--   earnings_events (#151 puller) is the new canonical filings rollup
--   covering 10-K/10-Q/8-K/EARNINGS_CALL/DEF 14A/S-1.  It is rich in
--   filing metadata (event_type, filing_date, period_end, accession,
--   url, summary, sentiment, confidence) but DOES NOT carry the
--   estimate/actual/surprise EPS+revenue columns that ~7 downstream
--   consumers depend on.
--
--   earnings_calendar carries those EPS/revenue columns and is still the
--   only fresh source of estimate-vs-actual data.  961 rows vs 89 rows.
--
-- Strategy (per Task #186 fallback clause):
--   1. Build a compat view (earnings_calendar_compat) exposing the columns
--      earnings_events CAN supply.  New consumers wanting filing-level
--      events (10-K, 8-K, etc.) can read from the view without coupling
--      to the physical table.
--   2. Build a populator (sync_earnings_events_to_calendar) that mirrors
--      events into the calendar so the new puller's coverage backstops
--      the calendar when fmp/altdata pulls miss a ticker.  EPS/revenue
--      stay NULL — they will be filled by the calendar puller when data
--      is available.
--
-- Idempotent.  Safe to re-run.

BEGIN;

-- ──────────────────────────────────────────────────────────────────────
-- 1. Compat view: earnings_events → earnings_calendar column shape
-- ──────────────────────────────────────────────────────────────────────
-- Only includes event types that semantically correspond to a "reported
-- earnings" or "earnings calendar slot": 10-K, 10-Q, EARNINGS_CALL.
-- 8-K / S-1 / DEF 14A are excluded because they are not earnings
-- calendar entries (they are filings, not earnings events).
--
-- Columns mapped:
--   period_end   → earnings_date  (the period the filing covers)
--   filing_date  → kept as filing_date (extra)
--   fiscal_quarter → fiscal_quarter
--   summary, sentiment, confidence → exposed as new columns
--   eps_*, revenue_*, classification, reported → NULL/derived
--     reported = TRUE since these are POST-filing events
DROP VIEW IF EXISTS earnings_calendar_compat;

CREATE VIEW earnings_calendar_compat AS
SELECT
    ticker,
    period_end             AS earnings_date,
    fiscal_quarter,
    NULL::double precision AS eps_estimate,
    NULL::double precision AS eps_actual,
    NULL::double precision AS eps_surprise_pct,
    NULL::double precision AS revenue_estimate,
    NULL::double precision AS revenue_actual,
    NULL::double precision AS revenue_surprise_pct,
    NULL::text             AS classification,
    TRUE                   AS reported,
    -- extra columns from earnings_events
    event_type,
    filing_date,
    accession,
    url,
    summary,
    sentiment,
    confidence,
    source,
    created_at             AS pull_timestamp
FROM earnings_events
WHERE event_type IN ('10-K', '10-Q', 'EARNINGS_CALL');

COMMENT ON VIEW earnings_calendar_compat IS
    'Task #186: read-only bridge exposing earnings_events as an '
    'earnings_calendar shape (EPS/revenue NULL — events lacks those). '
    'Use FROM earnings_calendar (with 961 rows of EPS data) for any '
    'consumer that requires eps_estimate/actual/surprise; use this view '
    'when filing-level event_type coverage is needed.';


-- ──────────────────────────────────────────────────────────────────────
-- 2. Populator: mirror earnings_events → earnings_calendar
-- ──────────────────────────────────────────────────────────────────────
-- Inserts a stub calendar row for every 10-K/10-Q/EARNINGS_CALL in
-- earnings_events that doesn't already have one.  EPS/revenue are left
-- NULL — they'll be filled when the calendar puller (#fmp/altdata)
-- catches up.  Idempotent (ON CONFLICT DO NOTHING).
--
-- Safe to call from hermes_operator on a schedule.
CREATE OR REPLACE FUNCTION sync_earnings_events_to_calendar()
RETURNS TABLE(inserted_count INT, total_events INT) AS $$
DECLARE
    v_inserted INT;
    v_total INT;
BEGIN
    SELECT COUNT(*) INTO v_total FROM earnings_events
        WHERE event_type IN ('10-K', '10-Q', 'EARNINGS_CALL');

    WITH src AS (
        SELECT DISTINCT ON (ee.ticker, ee.period_end)
            ee.ticker,
            ee.period_end,
            ee.fiscal_quarter,
            ee.created_at
        FROM earnings_events ee
        WHERE ee.event_type IN ('10-K', '10-Q', 'EARNINGS_CALL')
          AND ee.ticker IS NOT NULL
          AND ee.period_end IS NOT NULL
        ORDER BY ee.ticker, ee.period_end, ee.created_at DESC NULLS LAST
    ), ins AS (
        INSERT INTO earnings_calendar (
            ticker,
            earnings_date,
            fiscal_quarter,
            reported,
            classification,
            pull_timestamp
        )
        SELECT
            src.ticker,
            src.period_end,
            src.fiscal_quarter,
            TRUE,
            'pending'::text,
            COALESCE(src.created_at, NOW())
        FROM src
        ON CONFLICT (ticker, earnings_date) DO NOTHING
        RETURNING 1
    )
    SELECT COUNT(*)::INT INTO v_inserted FROM ins;

    RETURN QUERY SELECT v_inserted, v_total;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION sync_earnings_events_to_calendar() IS
    'Task #186: mirror new earnings_events rows into earnings_calendar '
    'so calendar consumers see the new puller''s coverage.  Idempotent. '
    'EPS/revenue/surprise columns left NULL — filled by FMP/altdata '
    'puller separately.  Returns (inserted_count, total_eligible_events).';


COMMIT;
