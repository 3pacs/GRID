"""Redefine public.yf_latest_daily_vintage so a refresh no longer reads every vintage.

Revision ID: yf_latest_vintage_scan_20261026
Revises: merge_yf_finra_20261023

Why
---
``yf_latest_daily_vintage_20261019`` defined the view with
``SELECT DISTINCT ON (obs_date) ... ORDER BY obs_date, pull_timestamp DESC, id DESC``
per series. On production (2026-10-26, release c17fb7ae) the first
``REFRESH MATERIALIZED VIEW`` was cancelled by its 1200 s ``statement_timeout``
(exit 1, the view stayed unpopulated). The plan was the intended 420 per-series
index scans; the cost is volume. Each series has ~21-22k vintages since
2026-01-01 (4 pulls a day, each re-pulling a multi-day window, plus repair
vintages) for only ~200 distinct dates, and the old query visits every
vintage's heap row (``pull_status`` and ``raw_payload`` are not in any index)
before keeping ~200 of them: ~611 heap blocks per series, 3.8 s cold, 420 x 3.8 s
~ 27 min.

What changes
------------
Same rows, same columns, same unique index, same ``REFRESH ... CONCURRENTLY``
contract; only the SELECT is rewritten in two steps, per series:

1. *Newest timestamp per date, from the index alone.* ``max(pull_timestamp)
   GROUP BY obs_date`` over ``(series_id, source_id = 2, obs_date >= 2026-01-01)``
   is answerable from ``uq_raw_series_composite (series_id, source_id, obs_date,
   pull_timestamp)`` as an index-only scan: no heap block is read for the ~21k
   vintages (the visibility map of an append-mostly table is nearly all set).
2. *Fetch only the winner.* For each date, one backward probe of the same index
   from that timestamp, ``pull_status = 'SUCCESS'``, ``LIMIT 1``, reads the heap
   row (payload included) of the latest SUCCESS vintage. If the newest vintage
   of a date is FAILED / PARTIAL / QUARANTINED the probe walks down to the
   latest SUCCESS one, so the result is *exactly* "latest SUCCESS vintage of
   source 2 per (series, date)", as before. A date with no SUCCESS vintage
   yields no row, as before.

The old ``ORDER BY ..., id DESC`` tiebreak is dropped on purpose:
``uq_raw_series_composite`` is UNIQUE on ``(series_id, source_id, obs_date,
pull_timestamp)``, so two source-2 vintages of one series/date can never share a
``pull_timestamp``; ``id`` could never break a tie. (The migration relies on
that unique index; ``schema.sql`` and the production catalog carry it.)

Cost (arithmetic, see the receipt for the measured fixture)
-----------------------------------------------------------
Per series: ~21k index entries read from ~130 index leaf pages (index-only, no
heap) + ~200 heap blocks for the winners, versus ~130 index pages + ~611 heap
blocks before: ~330 vs ~740 blocks, ~2.2x fewer; at the 6 ms/cold block implied
by the production measurement (3.8 s / 611) that is ~1.7 s/series, 420 series ~
12 min cold in the worst case and seconds-to-a-few-minutes once the index is
cached (the daily refresh). Production's ``relallvisible/relpages`` for
``raw_series`` was not measured here: if the table is mostly not all-visible
the first step degrades toward the old heap behaviour. Operator check (read
only, one series)::

    EXPLAIN (ANALYZE, BUFFERS) SELECT obs_date, max(pull_timestamp)
    FROM raw_series WHERE series_id = 'YF:XLK:close' AND source_id = 2
    AND obs_date >= DATE '2026-01-01' GROUP BY obs_date;

must show ``Index Only Scan using uq_raw_series_composite`` with small
``Heap Fetches``.

Operator notes
--------------
* No index is created and ``raw_series`` is not touched: upgrade is a catalog
  change (DROP + CREATE ... WITH NO DATA), milliseconds.
* The view is *unpopulated* after upgrade (as it is on production now). Populate
  it with ``python3 scripts/refresh_yf_latest_vintage.py`` (V2), then install
  the 10:45Z timer (V3) as before.
* If the view had been populated, upgrade drops its contents; it is derived
  data and the next refresh rebuilds it.
* Downgrade restores the previous definition (also WITH NO DATA). It does not
  traverse ``merge_yf_finra_20261023``, whose own downgrade refuses.
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

from migrations.versions import yf_latest_daily_vintage_20261019 as _v1

revision = "yf_latest_vintage_scan_20261026"
down_revision = "merge_yf_finra_20261023"
branch_labels = None
depends_on = None

VIEW_NAME = _v1.VIEW_NAME
UNIQUE_INDEX_NAME = _v1.UNIQUE_INDEX_NAME
YF_FIELDS = _v1.YF_FIELDS
YF_SOURCE_ID = _v1.YF_SOURCE_ID
FIRST_OBS_DATE = _v1.FIRST_OBS_DATE
PLANNER_HINTS = _v1.PLANNER_HINTS
load_tickers = _v1.load_tickers
_sql_literal = _v1._sql_literal


def build_select_sql(tickers: list[str] | None = None) -> str:
    """The view's SELECT: index-only newest-per-date, then one winner probe per date."""
    tickers = load_tickers() if tickers is None else tickers
    ticker_values = ",".join(f"({_sql_literal(t)})" for t in tickers)
    field_values = ",".join(f"({_sql_literal(f)})" for f in YF_FIELDS)
    return f"""WITH t(ticker) AS (VALUES {ticker_values}),
f(field) AS (VALUES {field_values})
SELECT
    w.series_id,
    t.ticker,
    f.field,
    d.obs_date,
    w.id AS raw_series_id,
    w.pull_timestamp,
    w.value,
    w.raw_payload->>'capture_state' AS capture_state,
    w.raw_payload->>'capture_policy' AS capture_policy,
    COALESCE(w.raw_payload ? 'capture_policy', FALSE) AS policy_marked,
    w.raw_payload->>'repair_run_tag' AS repair_run_tag
FROM t
CROSS JOIN f
CROSS JOIN LATERAL (
    SELECT r.obs_date, max(r.pull_timestamp) AS newest
    FROM raw_series r
    WHERE r.series_id = 'YF:' || t.ticker || ':' || f.field
      AND r.source_id = {YF_SOURCE_ID}
      AND r.obs_date >= DATE '{FIRST_OBS_DATE}'
    GROUP BY r.obs_date
) d
CROSS JOIN LATERAL (
    SELECT r.series_id, r.id, r.pull_timestamp, r.value, r.raw_payload
    FROM raw_series r
    WHERE r.series_id = 'YF:' || t.ticker || ':' || f.field
      AND r.source_id = {YF_SOURCE_ID}
      AND r.obs_date = d.obs_date
      AND r.pull_timestamp <= d.newest
      AND r.pull_status = 'SUCCESS'
    ORDER BY r.pull_timestamp DESC
    LIMIT 1
) w"""


def build_create_sql(tickers: list[str] | None = None) -> str:
    return (
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {VIEW_NAME} AS\n"
        f"{build_select_sql(tickers)}\nWITH NO DATA"
    )


def build_index_sql() -> str:
    return _v1.build_index_sql()


def _grant_sql() -> str:
    return f"""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
            EXECUTE 'GRANT SELECT ON {VIEW_NAME} TO grid';
        END IF;
    END
    $$
"""


def _replace_view(create_sql: str) -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    op.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {VIEW_NAME}"))
    op.execute(text(create_sql))
    op.execute(text(build_index_sql()))
    op.execute(text(_grant_sql()))


def upgrade() -> None:
    _replace_view(build_create_sql())


def downgrade() -> None:
    # Previous definition (yf_latest_daily_vintage_20261019), also WITH NO DATA.
    _replace_view(_v1.build_create_sql())
