"""Who may read options rows captured from Schwab (2026-11-21).

Schwab market data is licensed for the owner's private use only. Since #1299
the options puller can read the hand-listed core from the owner's Schwab app
(``GRID_OPTIONS_PRIMARY_PROVIDER=schwab``), and those captures land in the same
tables every other options reader uses: ``options_capture_batches``,
``options_snapshots_all`` (and the ``options_snapshots`` view) and
``options_daily_signals``. This module is the one place that decides which of
those rows a reader may see. The rule:

* an **admin request** sees every row;
* the **Oracle forecast engine** sees every row once the operator turns it
  on (owner decision 2026-11-23, :func:`oracle_reads_schwab`):
  ``oracle/engine.py`` and the entry quote it freezes through
  ``oracle/entry_price_policy.load_quote_reference``. The predictions it
  builds from Schwab rows are themselves hidden from non-admin readers
  (``VISIBLE_ORACLE_PREDICTIONS``, below);
* every other reader -- a contributor request, and every consumer that is not
  a request at all (e-mail digests, AstroGrid publication in
  ``oracle/publish.py``, chat/LLM context, scanners and engines whose output is
  stored and served) -- sees no row that came from a Schwab capture. Where a
  reader asks for "the latest", it gets the latest non-Schwab value instead (an
  earlier day), never a stand-in.

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
import os
from collections.abc import Mapping
from datetime import date
from typing import Any

SCHWAB_PROVIDER = "schwab"

#: Owner decision, 2026-11-23: "oracle counts as private use, let it read
#: schwab". The operator turns it on for the process that runs the Oracle
#: (``GRID_ORACLE_SCHWAB=private-use`` on grid-hermes). It is a switch, not a
#: constant, because the hiding below works only through reader code: every
#: long-running process that reads ``oracle_predictions`` must run this
#: release before the Oracle stores its first Schwab-built row. Unset or any
#: other value keeps the Oracle on the non-Schwab rows, as before.
ORACLE_SCHWAB_ENV = "GRID_ORACLE_SCHWAB"
ORACLE_SCHWAB_ON = "private-use"


def oracle_reads_schwab() -> bool:
    """The Oracle engine's ``include_schwab`` flag: True only when the operator set the switch.

    It reads Schwab captures as an admin request does: its entry quote
    (``oracle/entry_price_policy.load_quote_reference``) and its options
    signal row (``oracle/engine.py``). Only those Oracle call sites may pass
    it (``tests/test_options_provenance.py``). Every other reader keeps its
    own flag, and AstroGrid publication (``oracle/publish.py``) stays filtered.
    """
    return os.getenv(ORACLE_SCHWAB_ENV, "").strip().lower() == ORACLE_SCHWAB_ON


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


def _signal_visible(row: str) -> str:
    """A non-admin reader may see the ``options_daily_signals`` row ``row``.

    Called only below, at import, with literal aliases.
    """
    return (_no_schwab_batch(row, "signal_date")
            + " AND COALESCE(" + row + ".wing_iv_metadata->>'source', '') <> 'schwab'")


_SIGNAL_VISIBLE = _signal_visible("schwab_v")

#: True when the ``options_daily_signals`` row aliased ``ods`` is Schwab-derived
#: (a non-admin reader could not see it). The Oracle selects it beside the
#: values it judges, in the same statement, so the tag describes the row the
#: prediction consumed and not a second read of "the latest". Never NULL: both
#: terms of the visibility rule are NOT EXISTS and a COALESCEd comparison.
SIGNAL_ROW_IS_SCHWAB = "(NOT (" + _signal_visible("ods") + "))"

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

#: ``options_snapshots_all`` filtered per batch: rows whose own registered
#: ``capture_batch_id`` is non-Schwab (``NOT _SCHWAB_BATCH``). Unbatched rows
#: (``capture_batch_id IS NULL``) and rows whose batch is missing or Schwab fail
#: closed. Used by batch-addressed readers (``physics/dealer_gamma.py``
#: registered-batch replay, ``paper_log/gex_batch_replay.py``) so a public
#: Yahoo/Cboe capture (e.g. ``grid-options-puller`` GEM Yahoo SPY) stays
#: readable on a day that also has a Schwab capture.
VISIBLE_BATCH_SNAPSHOTS = (
    "(SELECT schwab_v.* FROM options_snapshots_all schwab_v"
    " WHERE schwab_v.capture_batch_id IS NOT NULL"
    " AND EXISTS (SELECT 1 FROM options_capture_batches schwab_pb"
    " WHERE schwab_pb.capture_batch_id = schwab_v.capture_batch_id"
    " AND schwab_pb.ticker = schwab_v.ticker"
    " AND schwab_pb.snap_date = schwab_v.snap_date"
    " AND NOT " + _SCHWAB_BATCH + "))"
)

#: ``options_capture_batches`` without Schwab (or unknown-after-cutoff) batches.
#: Batch-level: a listing (and batch-addressed replay) still sees a same-day
#: non-Schwab Cboe/Yahoo batch.
VISIBLE_CAPTURE_BATCHES = (
    "(SELECT schwab_pb.* FROM options_capture_batches schwab_pb WHERE NOT " + _SCHWAB_BATCH + ")"
)

#: ``options_capture_batches`` on days with no Schwab batch for the ticker
#: (day-level rule, paired with ``VISIBLE_SNAPSHOTS`` / ``VISIBLE_SNAPSHOTS_ALL``).
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

# ── Oracle predictions built from Schwab captures (2026-11-23) ─────────
#
# The Oracle reads Schwab rows (oracle_reads_schwab()), so a prediction it builds
# from them carries Schwab data: the entry price, the target, the options
# signal values, and every return computed from the entry. The owner's
# follow-up decision, the same day: such predictions stay out of contributors'
# sight. Hiding the prices alone is not enough, because a target, an expected
# move or a scored return gives the entry back from the public close. The
# engine tags each one in ``flow_context`` (``ORACLE_PRIVATE_USE_KEY`` =
# ``ORACLE_PRIVATE_USE_SCHWAB``), abstentions included, since their receipt
# holds the price. Every reader that serves people reads through
# ``VISIBLE_ORACLE_PREDICTIONS``, with the alias ``oracle_predictions`` so
# its column references are unchanged. An admin request gets the plain table,
# as with the options constants. ``tests/test_oracle_predictions_read_guard.py``
# fails on a new raw read.
ORACLE_PRIVATE_USE_KEY = "private_use"
ORACLE_PRIVATE_USE_SCHWAB = "schwab"

#: ``oracle_predictions`` without the rows the Oracle built from Schwab captures.
#: No bound value: the tag is a literal. A NULL ``flow_context`` is not tagged.
VISIBLE_ORACLE_PREDICTIONS = (
    "(SELECT schwab_o.* FROM oracle_predictions schwab_o"
    " WHERE COALESCE(schwab_o.flow_context ->> '" + ORACLE_PRIVATE_USE_KEY + "', '')"
    " <> '" + ORACLE_PRIVATE_USE_SCHWAB + "')"
)

# What an admin reads instead of each constant. No constant is a substring of
# another (tests/test_options_provenance.py), so the order does not matter.
_ADMIN_FORM: dict[str, str] = {
    LATEST_DAILY_SIGNALS: LATEST_DAILY_SIGNALS_ALL,
    LATEST_SIGNAL_EACH_TICKER: "options_daily_signals",
    VISIBLE_DAILY_SIGNALS: "options_daily_signals",
    VISIBLE_SNAPSHOTS_ALL: "options_snapshots_all",
    VISIBLE_BATCH_SNAPSHOTS: "options_snapshots_all",
    VISIBLE_SNAPSHOTS: "options_snapshots",
    VISIBLE_CAPTURE_BATCH_DAYS: "options_capture_batches",
    VISIBLE_CAPTURE_BATCHES: "options_capture_batches",
    VISIBLE_ORACLE_PREDICTIONS: "oracle_predictions",
}


def include_schwab_rows(flag: Any) -> bool:
    """True only for an explicit ``True`` (an admin request). Anything else excludes."""
    return flag is True


def oracle_predictions_from(include_schwab: Any = False) -> str:
    """The ``FROM`` source of ``oracle_predictions`` for this viewer.

    The plain table for an admin request, ``VISIBLE_ORACLE_PREDICTIONS``
    otherwise. Callers alias it ``oracle_predictions`` (or their own alias), so
    the statement's column references do not change. Chosen in Python from two
    constants; no caller value reaches the SQL text.
    """
    return "oracle_predictions" if include_schwab_rows(include_schwab) else VISIBLE_ORACLE_PREDICTIONS


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
