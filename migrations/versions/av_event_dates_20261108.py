"""Stop presenting fiscal period ends as actual earnings event dates.

Forward-only source migration. Does not modify historical earnings_events or
calendar rows. The old bridge cannot know which legacy calendar stubs came from
it: reconciliation requires a separately reviewed repair, never a blanket delete.
No existing writer establishes actual call dates; expose NULL and preserve the
fiscal end explicitly. The legacy sync ABI returns zero inserts until a later
reviewed contract can supply independently verified event dates.
"""
from alembic import op

revision = "av_event_dates_20261108"
down_revision = "sleuth_intersections_20261107"
branch_labels = None
depends_on = None

VIEW_SQL = """
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
WHERE event_type IN ('10-K', '10-Q', 'EARNINGS_CALL')
"""
SYNC_SQL = """
CREATE OR REPLACE FUNCTION sync_earnings_events_to_calendar()
RETURNS TABLE(inserted_count INT, total_events INT) AS $$
BEGIN
    -- Neither fiscal period end nor SEC filing date is an earnings-call date.
    -- No current writer supplies a separately verified actual event date.
    -- Retain the scheduler ABI but fail closed: do not manufacture calendar rows.
    RETURN QUERY SELECT 0::INT, COUNT(*)::INT FROM earnings_events
        WHERE event_type IN ('10-K', '10-Q', 'EARNINGS_CALL');
END;
$$ LANGUAGE plpgsql
"""


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    # earnings_events is runtime-created and absent from the baseline schema.
    # If absent, there is no unsafe bridge; its source bootstrap is fixed too.
    if op.get_bind().exec_driver_sql("SELECT to_regclass('earnings_events')").scalar() is None:
        return
    op.execute(VIEW_SQL)
    op.execute(SYNC_SQL)
    op.execute("COMMENT ON VIEW earnings_calendar_compat IS "
               "'Fiscal filing evidence only. earnings_date is unknown; period_end is not a call date. "
               "pull_timestamp is storage time, not historical publication availability.'")
    op.execute("GRANT SELECT ON earnings_calendar_compat TO grid")
    op.execute("GRANT EXECUTE ON FUNCTION sync_earnings_events_to_calendar() TO grid")


def downgrade() -> None:
    raise RuntimeError("Refusing to restore fabricated earnings dates; use a reviewed forward migration")
