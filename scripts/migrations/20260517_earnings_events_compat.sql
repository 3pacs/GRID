-- Fiscal filing evidence is not an earnings-calendar timestamp.
-- Source bootstrap aligned with av_event_dates_20261108. Historical calendar
-- rows remain untouched; source attribution is insufficient for automatic repair.
-- Unknown actual event dates stay NULL. Preserve period_end under its own name.
BEGIN;
CREATE OR REPLACE VIEW earnings_calendar_compat AS
SELECT ticker,
       NULL::date AS earnings_date,
       fiscal_quarter,
       NULL::double precision AS eps_estimate,
       NULL::double precision AS eps_actual,
       NULL::double precision AS eps_surprise_pct,
       NULL::double precision AS revenue_estimate,
       NULL::double precision AS revenue_actual,
       NULL::double precision AS revenue_surprise_pct,
       NULL::text AS classification,
       TRUE AS reported,
       event_type, filing_date, accession, url, summary, sentiment, confidence,
       source, created_at AS pull_timestamp,
       period_end,
       'unknown_actual_event_date'::text AS earnings_date_basis
FROM earnings_events
WHERE event_type IN ('10-K', '10-Q', 'EARNINGS_CALL');

CREATE OR REPLACE FUNCTION sync_earnings_events_to_calendar()
RETURNS TABLE(inserted_count INT, total_events INT) AS $$
BEGIN
    -- Neither fiscal period end nor SEC filing date is an earnings-call date.
    -- No current writer supplies a separately verified actual event date.
    -- Retain the scheduler ABI but fail closed: do not manufacture calendar rows.
    RETURN QUERY SELECT 0::INT, COUNT(*)::INT FROM earnings_events
        WHERE event_type IN ('10-K', '10-Q', 'EARNINGS_CALL');
END;
$$ LANGUAGE plpgsql;
GRANT SELECT ON earnings_calendar_compat TO grid;
GRANT EXECUTE ON FUNCTION sync_earnings_events_to_calendar() TO grid;
COMMIT;
