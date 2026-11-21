"""Who may read options rows captured from Schwab (2026-11-21).

Schwab market data is licensed for the owner's private use only. Since #1299
the options puller can read the hand-listed core from the owner's Schwab app
(``GRID_OPTIONS_PRIMARY_PROVIDER=schwab``), and those captures land in the same
tables every other options reader uses: ``options_capture_batches``,
``options_snapshots_all`` (and the ``options_snapshots`` view) and
``options_daily_signals``. This module is the one place that decides which of
those rows a reader may see. The rule:

* an **admin request** sees every row;
* every other reader -- a contributor request, and every consumer that is not
  a request at all (e-mail digests, oracle publication, chat/LLM context,
  scanners and engines whose output is stored and served) -- sees no row that
  came from a Schwab capture. Where a reader asks for "the latest", it gets
  the latest non-Schwab value instead (an earlier day), never a stand-in.

How a row is traced to a capture. A capture batch records its provider in
``options_capture_batches.spot_provider`` and in ``clock_metadata->>'provider'``
(#1299). Contract rows carry their ``capture_batch_id``; the daily-signal row
is written from a batch of the same ticker and day (``signal_date`` =
``snap_date``). So a row is Schwab-derived when a Schwab batch exists for its
ticker and day. That is deliberately wider than "the batch it was computed
from": on a day with a Schwab batch and a Cboe batch, the whole day is hidden
from non-admin readers (``VISIBLE_CAPTURE_BATCHES`` is the one batch-level
exception: a listing of batches, or a Cboe batch's own spot, stays visible).

``resolved_series`` is not filtered here: it feeds every point-in-time reader
(``store/pit.py`` feature matrices, models, chat context) with no notion of a
viewer. Instead the puller never writes a Schwab capture's options features
to it (``ingestion/options.py``, ``provider_is_schwab``).

Fail closed. A batch whose provider is unknown (no ``spot_provider``, or one
that is neither ``cboe`` nor ``yahoo``) and that is dated on or after
``SCHWAB_POSSIBLE_FROM`` -- the day the Schwab provider was written, before
which no capture could have come from Schwab -- counts as Schwab. A
daily-signal row whose wing metadata names Schwab counts as Schwab even
without a batch. The flag that admits Schwab rows is honoured only when it is
exactly ``True``: a missing, ``None`` or truthy non-bool value excludes them.

How a reader uses it. Each ``VISIBLE_*`` / ``LATEST_*`` constant is a derived
table that stands in for the table in a ``FROM`` clause, with the caller's
own alias, and the statement goes through :func:`visible_sql`::

    visible_sql("SELECT spot_price FROM " + VISIBLE_DAILY_SIGNALS + " ods "
                "WHERE ods.ticker = :t ORDER BY ods.signal_date DESC LIMIT 1",
                include_schwab)

The SQL is chosen in Python, never in the database. For an admin,
``visible_sql`` swaps each constant for the plain table (the statement is then
exactly the one GRID ran before this module). For everyone else the constant
stays: a top-level ``NOT EXISTS`` with no ``OR`` around it, which PostgreSQL
plans as an anti-join. (An ``OR`` with a parameter cannot be, and cost a read
of the whole batch registry per statement; review of #1322.) The constants
are built from literals only, and the one value they compare against,
``:schwab_from``, is bound, so no caller value reaches the SQL text.
``tests/test_options_provenance.py`` pins that, and
``tests/test_options_raw_read_guard.py`` fails on any new reader that goes
around them.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from typing import Any

SCHWAB_PROVIDER = "schwab"
# The Schwab options provider was written on this day (#1299). A capture of
# unknown provenance dated on or after it may have come from Schwab.
SCHWAB_POSSIBLE_FROM = date(2026, 11, 21)

# One batch ``schwab_pb`` came from Schwab, or its provider is unknown on a
# day Schwab was possible. Every column is COALESCEd so that NULL can never
# turn the outer NOT into NULL (which would hide a row for the wrong reason).
_SCHWAB_BATCH = (
    "(COALESCE(schwab_pb.spot_provider, '') = 'schwab'"
    " OR COALESCE(schwab_pb.clock_metadata->>'provider', '') = 'schwab'"
    " OR (schwab_pb.snap_date >= :schwab_from"
    " AND COALESCE(schwab_pb.spot_provider, '') NOT IN ('cboe', 'yahoo')))"
)


def _no_schwab_batch(row: str, day_column: str) -> str:
    """``NOT EXISTS`` a Schwab batch for ``row``'s ticker and day.

    Called only below, at import, with literal aliases: it builds constants,
    never a statement from a caller's value.
    """
    return ("NOT EXISTS (SELECT 1 FROM options_capture_batches schwab_pb"
            " WHERE schwab_pb.ticker = " + row + ".ticker"
            " AND schwab_pb.snap_date = " + row + "." + day_column
            + " AND " + _SCHWAB_BATCH + ")")


_SIGNAL_VISIBLE = (
    _no_schwab_batch("schwab_v", "signal_date")
    + " AND COALESCE(schwab_v.wing_iv_metadata->>'source', '') <> 'schwab'"
)

#: ``options_daily_signals`` without Schwab-derived days.
VISIBLE_DAILY_SIGNALS = (
    "(SELECT schwab_v.* FROM options_daily_signals schwab_v WHERE " + _SIGNAL_VISIBLE + ")"
)

#: The ``options_snapshots`` view (latest batch per ticker/day) without Schwab days.
VISIBLE_SNAPSHOTS = (
    "(SELECT schwab_v.* FROM options_snapshots schwab_v WHERE "
    + _no_schwab_batch("schwab_v", "snap_date") + ")"
)

#: ``options_snapshots_all`` (every batch) without Schwab days.
VISIBLE_SNAPSHOTS_ALL = (
    "(SELECT schwab_v.* FROM options_snapshots_all schwab_v WHERE "
    + _no_schwab_batch("schwab_v", "snap_date") + ")"
)

#: ``options_capture_batches`` without Schwab (or unknown-after-cutoff) batches.
#: Batch-level: a listing still shows a same-day Cboe batch.
VISIBLE_CAPTURE_BATCHES = (
    "(SELECT schwab_pb.* FROM options_capture_batches schwab_pb WHERE NOT " + _SCHWAB_BATCH + ")"
)

#: ``options_capture_batches`` on days with no Schwab batch for the ticker: the
#: day-level rule of the contracts, for a reader that picks a batch and then
#: loads its chain (``physics/dealer_gamma.py``), so the two always agree.
VISIBLE_CAPTURE_BATCH_DAYS = (
    "(SELECT schwab_b.* FROM options_capture_batches schwab_b WHERE "
    + _no_schwab_batch("schwab_b", "snap_date") + ")"
)

# The tickers that have a row on the latest signal day. A date and a ticker
# name are not Schwab data; the values chosen below are.
_LATEST_DAY_TICKERS = (
    "(SELECT schwab_d.ticker FROM options_daily_signals schwab_d"
    " WHERE schwab_d.signal_date = (SELECT MAX(schwab_m.signal_date) FROM options_daily_signals schwab_m))"
)

#: One row per ticker that has a row on the latest signal day: that row, or
#: when it is Schwab-derived, the ticker's latest non-Schwab row (an earlier
#: ``signal_date``; none when it has none). PostgreSQL only (``LATERAL``).
LATEST_DAILY_SIGNALS = (
    "(SELECT schwab_l.* FROM " + _LATEST_DAY_TICKERS + " schwab_t"
    " CROSS JOIN LATERAL (SELECT schwab_v.* FROM options_daily_signals schwab_v"
    " WHERE schwab_v.ticker = schwab_t.ticker AND " + _SIGNAL_VISIBLE
    + " ORDER BY schwab_v.signal_date DESC LIMIT 1) schwab_l)"
)

#: Each ticker's latest non-Schwab signal row, for "latest row per ticker"
#: reads (``SELECT DISTINCT ON (ticker) ... ORDER BY ticker, signal_date DESC``).
#: One index probe per ticker instead of an anti-join over the whole table.
#: The admin form is the plain table, so the caller keeps its DISTINCT ON.
#: PostgreSQL only (``LATERAL``).
LATEST_SIGNAL_EACH_TICKER = (
    # The ticker list is an index skip scan (one probe per ticker), not a
    # DISTINCT over every row.
    "(SELECT schwab_l.* FROM (WITH RECURSIVE schwab_r(ticker) AS ("
    "SELECT MIN(schwab_d.ticker) FROM options_daily_signals schwab_d"
    " UNION ALL SELECT (SELECT MIN(schwab_n.ticker) FROM options_daily_signals schwab_n"
    " WHERE schwab_n.ticker > schwab_r.ticker) FROM schwab_r WHERE schwab_r.ticker IS NOT NULL)"
    " SELECT schwab_r.ticker FROM schwab_r WHERE schwab_r.ticker IS NOT NULL) schwab_t"
    " CROSS JOIN LATERAL (SELECT schwab_v.* FROM options_daily_signals schwab_v"
    " WHERE schwab_v.ticker = schwab_t.ticker AND " + _SIGNAL_VISIBLE
    + " ORDER BY schwab_v.signal_date DESC LIMIT 1) schwab_l)"
)

#: The admin form of ``LATEST_DAILY_SIGNALS``: every row on the latest signal day.
LATEST_DAILY_SIGNALS_ALL = (
    "(SELECT schwab_v.* FROM options_daily_signals schwab_v"
    " WHERE schwab_v.signal_date = (SELECT MAX(schwab_m.signal_date) FROM options_daily_signals schwab_m))"
)

# What an admin reads instead of each constant. No constant is a substring of
# another (tests/test_options_provenance.py), so the order does not matter.
_ADMIN_FORM: dict[str, str] = {
    LATEST_DAILY_SIGNALS: LATEST_DAILY_SIGNALS_ALL,
    LATEST_SIGNAL_EACH_TICKER: "options_daily_signals",
    VISIBLE_DAILY_SIGNALS: "options_daily_signals",
    VISIBLE_SNAPSHOTS_ALL: "options_snapshots_all",
    VISIBLE_SNAPSHOTS: "options_snapshots",
    VISIBLE_CAPTURE_BATCH_DAYS: "options_capture_batches",
    VISIBLE_CAPTURE_BATCHES: "options_capture_batches",
}


def include_schwab_rows(flag: Any) -> bool:
    """True only for an explicit ``True`` (an admin request). Anything else excludes."""
    return flag is True


def visible_sql_text(sql: str, include_schwab: Any = False) -> str:
    """The statement this viewer runs: the plain tables for an admin, the filtered SQL otherwise."""
    if include_schwab_rows(include_schwab):
        for fragment, plain in _ADMIN_FORM.items():
            sql = sql.replace(fragment, plain)
    return sql


def visible_sql(sql: str, include_schwab: Any = False) -> Any:
    """``text()`` of :func:`visible_sql_text`, with ``:schwab_from`` bound when it is used."""
    from sqlalchemy import text

    chosen = text(visible_sql_text(sql, include_schwab))
    if ":schwab_from" in chosen.text:
        chosen = chosen.bindparams(schwab_from=SCHWAB_POSSIBLE_FROM.isoformat())
    return chosen


def _metadata_provider(clock_metadata: Any) -> str:
    if isinstance(clock_metadata, str):
        try:
            clock_metadata = json.loads(clock_metadata)
        except ValueError:
            return ""
    if isinstance(clock_metadata, Mapping):
        return str(clock_metadata.get("provider") or "").strip().lower()
    return ""


def provider_is_schwab(spot_provider: Any, clock_metadata: Any = None) -> bool:
    """True when either provider field of a capture names Schwab (the writer's check)."""
    return (str(spot_provider or "").strip().lower() == SCHWAB_PROVIDER
            or _metadata_provider(clock_metadata) == SCHWAB_PROVIDER)
