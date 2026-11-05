"""Materialized view public.yf_latest_daily_vintage: latest Yahoo daily vintage per day.

Revision ID: yf_latest_daily_vintage_20261019
Revises: people_events_v2_20261001

What it is
----------
One row per ``(series_id, obs_date)`` for every ``ingestion.yfinance_pull.
YF_TICKER_LIST`` ticker x the six daily fields (open, high, low, close, volume,
adj_close), ``obs_date >= 2026-01-01``: the *latest* ``SUCCESS`` vintage
(greatest ``pull_timestamp``, ties broken by ``id``) of ``source_id = 2``
rows. It is a convenience cache for audits and dashboards. It is NOT a
point-in-time source: it forgets every earlier vintage, so an ``as_of`` read
must still go to ``raw_series`` (``store/observations.py``). See
``docs/reference/YF_LATEST_DAILY_VINTAGE.md``.

Columns: series_id, ticker, field, obs_date, raw_series_id, pull_timestamp,
value, capture_state (``raw_payload->>'capture_state'``; NULL on legacy
unmarked rows), capture_policy, policy_marked (``raw_payload ? 'capture_policy'``),
repair_run_tag (``raw_payload->>'repair_run_tag'``).

Why the SELECT looks the way it does
------------------------------------
``raw_series`` holds ~1.93e9 rows (511 GB), one row per vintage. Any "latest
vintage per series/date" question has to be answered with one index probe per
series: a VALUES list of tickers x fields ``CROSS JOIN LATERAL`` into
``raw_series`` with ``x.series_id = 'YF:' || ticker || ':' || field`` so each
probe uses ``idx_raw_series_dedup (series_id, obs_date, pull_timestamp DESC)``.
A whole-table DISTINCT or ``LIKE 'YF:%'`` scan times out (2026-10-19 audit,
q7.sql). The ticker list is generated from ``YF_TICKER_LIST`` when the
migration runs (imported, not hand-copied), so the view text is a literal list.
A ticker added to ``YF_TICKER_LIST`` later is NOT in the view until the view is
rebuilt (new migration, or DROP + re-run of this one).

Cost (operator, read this)
--------------------------
* ``upgrade()`` is cheap: ``CREATE MATERIALIZED VIEW ... WITH NO DATA`` and a
  unique index on an empty relation. It reads no ``raw_series`` rows. It takes
  no lock on ``raw_series`` beyond catalog lookups.
* The first population is NOT run by the migration (it takes ~10 minutes on
  production, ~100 s per field for 70 tickers). The operator runs it by hand:

      python3 scripts/refresh_yf_latest_vintage.py

  The script notices the view is unpopulated and does the plain (blocking,
  but nobody reads an empty view) ``REFRESH MATERIALIZED VIEW``; later runs use
  ``REFRESH MATERIALIZED VIEW CONCURRENTLY`` and never block readers.
* Both refreshes run with ``SET LOCAL enable_hashjoin/mergejoin/seqscan/
  bitmapscan = off`` so the planner keeps the per-series index probes.

Downgrade drops the view (and with it the index). No data is lost: the view is
derived from ``raw_series`` and can be rebuilt.

No ``raw_series`` DDL, no data rewrite. A fresh database without ``raw_series``
(or without the ``source_id``/``raw_payload`` columns) cannot create the view;
that is the expected failure, not something to work around.
"""

from __future__ import annotations

import re

from alembic import op
from sqlalchemy import text

revision = "yf_latest_daily_vintage_20261019"
down_revision = "people_events_v2_20261001"
branch_labels = None
depends_on = None

VIEW_NAME = "public.yf_latest_daily_vintage"
UNIQUE_INDEX_NAME = "uq_yf_latest_daily_vintage_series_obs"
YF_FIELDS: tuple[str, ...] = ("open", "high", "low", "close", "volume", "adj_close")
YF_SOURCE_ID = 2
FIRST_OBS_DATE = "2026-01-01"

# Hints applied (SET LOCAL) in the transaction that populates the view. Shared
# with scripts/refresh_yf_latest_vintage.py so there is one definition.
PLANNER_HINTS: tuple[str, ...] = (
    "SET LOCAL enable_hashjoin = off",
    "SET LOCAL enable_mergejoin = off",
    "SET LOCAL enable_seqscan = off",
    "SET LOCAL enable_bitmapscan = off",
)

# Yahoo symbols: letters, digits and ^ = . - only (e.g. ^VIX, HG=F, BTC-USD,
# DX-Y.NYB). Anything else is refused rather than quoted around: the list is
# interpolated into DDL, which cannot take bind parameters.
_TICKER_RE = re.compile(r"^[A-Za-z0-9^=.\-]{1,24}$")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_tickers() -> list[str]:
    """YF_TICKER_LIST, imported (never hand-copied), validated, order kept."""
    from ingestion.yfinance_pull import YF_TICKER_LIST

    tickers = list(dict.fromkeys(YF_TICKER_LIST))
    if not tickers:
        raise RuntimeError("ingestion.yfinance_pull.YF_TICKER_LIST is empty")
    bad = [t for t in tickers if not _TICKER_RE.match(t)]
    if bad:
        raise RuntimeError(f"refusing to build view SQL: unsafe ticker symbols {bad!r}")
    return tickers


def build_select_sql(tickers: list[str] | None = None) -> str:
    """The view's SELECT: per-series LATERAL probe, latest SUCCESS vintage per day."""
    tickers = load_tickers() if tickers is None else tickers
    ticker_values = ",".join(f"({_sql_literal(t)})" for t in tickers)
    field_values = ",".join(f"({_sql_literal(f)})" for f in YF_FIELDS)
    return f"""WITH t(ticker) AS (VALUES {ticker_values}),
f(field) AS (VALUES {field_values})
SELECT
    x.series_id,
    t.ticker,
    f.field,
    x.obs_date,
    x.id AS raw_series_id,
    x.pull_timestamp,
    x.value,
    x.raw_payload->>'capture_state' AS capture_state,
    x.raw_payload->>'capture_policy' AS capture_policy,
    COALESCE(x.raw_payload ? 'capture_policy', FALSE) AS policy_marked,
    x.raw_payload->>'repair_run_tag' AS repair_run_tag
FROM t
CROSS JOIN f
CROSS JOIN LATERAL (
    SELECT DISTINCT ON (r.obs_date)
        r.series_id, r.id, r.obs_date, r.pull_timestamp, r.value, r.raw_payload
    FROM raw_series r
    WHERE r.series_id = 'YF:' || t.ticker || ':' || f.field
      AND r.source_id = {YF_SOURCE_ID}
      AND r.pull_status = 'SUCCESS'
      AND r.obs_date >= DATE '{FIRST_OBS_DATE}'
    ORDER BY r.obs_date, r.pull_timestamp DESC, r.id DESC
) x"""


def build_create_sql(tickers: list[str] | None = None) -> str:
    return (
        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {VIEW_NAME} AS\n"
        f"{build_select_sql(tickers)}\nWITH NO DATA"
    )


def build_index_sql() -> str:
    return (
        f"CREATE UNIQUE INDEX IF NOT EXISTS {UNIQUE_INDEX_NAME} "
        f"ON {VIEW_NAME} (series_id, obs_date)"
    )


def upgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '60s'")
    # text() so ':' in the SQL is never read as a bind parameter; the SQL has
    # none (asserted in tests/test_yf_latest_daily_vintage.py).
    op.execute(text(build_create_sql()))
    op.execute(text(build_index_sql()))
    # GRANT footer (migrations/_TEMPLATE.sql): alembic runs as the owner role;
    # the API and the scheduler connect as `grid`.
    op.execute(text(f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT SELECT ON {VIEW_NAME} TO grid';
            END IF;
        END
        $$
    """))
    # First population is deliberately NOT done here (~10 minutes against the
    # 511 GB raw_series). Operator, after this migration is applied:
    #
    #     python3 scripts/refresh_yf_latest_vintage.py
    #
    # (or, by hand in psql with the same planner hints:
    #     BEGIN; SET LOCAL statement_timeout = '1200s';
    #     SET LOCAL enable_hashjoin = off; SET LOCAL enable_mergejoin = off;
    #     SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off;
    #     REFRESH MATERIALIZED VIEW public.yf_latest_daily_vintage; COMMIT;)
    #
    # Until then the view exists but is unpopulated: SELECT from it raises
    # "materialized view has not been populated" rather than returning a
    # misleading empty result.


def downgrade() -> None:
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {VIEW_NAME}"))
