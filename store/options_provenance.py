"""Who may read options rows captured from Schwab (2026-11-21).

Schwab market data is licensed for the owner's private use only. Since #1299
the options puller can read the hand-listed core from the owner's Schwab app
(``GRID_OPTIONS_PRIMARY_PROVIDER=schwab``), and those captures land in the same
tables every other options reader uses: ``options_capture_batches``,
``options_snapshots_all`` (and the ``options_snapshots`` view),
``options_daily_signals`` and the options features in ``resolved_series``.
This module is the one place that decides which of those rows a reader may
see. The rule:

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
from non-admin readers.

``resolved_series`` is not filtered here: it feeds every point-in-time reader
(``store/pit.py`` feature matrices, models, chat context) with no notion of a
viewer. Instead the puller never writes a Schwab capture's options features
to it (``ingestion/options.py``, ``provider_is_schwab``), so nothing there can
be Schwab-derived. ``hidden_feature_dates`` remains for a reader that wants to
check one options feature by name.

Fail closed. A batch whose provider is unknown (no ``spot_provider``, or one
that is neither ``cboe`` nor ``yahoo``) and that is dated on or after
``SCHWAB_POSSIBLE_FROM`` -- the day the Schwab provider was written, before
which no capture could have come from Schwab -- counts as Schwab. A
daily-signal row whose wing metadata names Schwab counts as Schwab even
without a batch. The flag that admits Schwab rows is honoured only when it is
exactly ``True``: a missing, ``None`` or truthy non-bool value excludes them.

How a reader uses it. The ``VISIBLE_*`` fragments are derived tables that
stand in for the table in a ``FROM`` clause, with the caller's own alias::

    text("SELECT spot_price FROM " + VISIBLE_DAILY_SIGNALS + " ods "
         "WHERE ods.ticker = :t ORDER BY ods.signal_date DESC LIMIT 1")
    conn.execute(query, {"t": ticker, **provenance_params(include_schwab)})

They are module constants built from literals only; every value they compare
against is a bound parameter (``:include_schwab``, ``:schwab_from``), so no
caller value ever reaches the SQL text. ``tests/test_options_provenance.py``
pins that.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from typing import Any

SCHWAB_PROVIDER = "schwab"
# Providers whose rows any reader may see.
PUBLIC_PROVIDERS: frozenset[str] = frozenset({"cboe", "yahoo"})
# The Schwab options provider was written on this day (#1299). A capture of
# unknown provenance dated on or after it may have come from Schwab.
SCHWAB_POSSIBLE_FROM = date(2026, 11, 21)

# Options features the puller writes to resolved_series as ``{prefix}_{suffix}``
# (``ingestion/options.py`` ``OptionsPuller._push_to_resolved``), with
# ``prefix = ticker.lower().replace("-", "_")``.
OPTIONS_FEATURE_SUFFIXES: tuple[str, ...] = (
    "iv_m110_call_v1", "iv_m90_put_v1", "term_slope", "max_pain", "oi_conc",
    "opt_vol", "total_oi", "iv_skew", "iv_atm", "pcr",
)

# One batch ``schwab_pb`` came from Schwab, or its provider is unknown on a
# day Schwab was possible. Every column is COALESCEd so that NULL can never
# turn the outer NOT into NULL (which would hide a row for the wrong reason).
_SCHWAB_BATCH = (
    "(COALESCE(schwab_pb.spot_provider, '') = 'schwab'"
    " OR COALESCE(schwab_pb.clock_metadata->>'provider', '') = 'schwab'"
    " OR (schwab_pb.snap_date >= :schwab_from"
    " AND COALESCE(schwab_pb.spot_provider, '') NOT IN ('cboe', 'yahoo')))"
)

# A Schwab (or unknown-after-cutoff) batch exists for row ``schwab_v``'s ticker/day.
_SCHWAB_DAY_ON_SNAP_DATE = (
    "EXISTS (SELECT 1 FROM options_capture_batches schwab_pb"
    " WHERE schwab_pb.ticker = schwab_v.ticker"
    " AND schwab_pb.snap_date = schwab_v.snap_date"
    " AND " + _SCHWAB_BATCH + ")"
)
_SCHWAB_DAY_ON_SIGNAL_DATE = (
    "EXISTS (SELECT 1 FROM options_capture_batches schwab_pb"
    " WHERE schwab_pb.ticker = schwab_v.ticker"
    " AND schwab_pb.snap_date = schwab_v.signal_date"
    " AND " + _SCHWAB_BATCH + ")"
)

#: ``options_daily_signals`` without Schwab-derived days (unless admitted).
VISIBLE_DAILY_SIGNALS = (
    "(SELECT schwab_v.* FROM options_daily_signals schwab_v"
    " WHERE (:include_schwab OR NOT ("
    + _SCHWAB_DAY_ON_SIGNAL_DATE
    + " OR COALESCE(schwab_v.wing_iv_metadata->>'source', '') = 'schwab')))"
)

#: The ``options_snapshots`` view (latest batch per ticker/day) without Schwab days.
VISIBLE_SNAPSHOTS = (
    "(SELECT schwab_v.* FROM options_snapshots schwab_v"
    " WHERE (:include_schwab OR NOT " + _SCHWAB_DAY_ON_SNAP_DATE + "))"
)

#: ``options_snapshots_all`` (every batch) without Schwab days.
VISIBLE_SNAPSHOTS_ALL = (
    "(SELECT schwab_v.* FROM options_snapshots_all schwab_v"
    " WHERE (:include_schwab OR NOT " + _SCHWAB_DAY_ON_SNAP_DATE + "))"
)

#: ``options_capture_batches`` without Schwab (or unknown-after-cutoff) batches.
#: Batch-level, not day-level: a listing still shows a same-day Cboe batch.
VISIBLE_CAPTURE_BATCHES = (
    "(SELECT schwab_pb.* FROM options_capture_batches schwab_pb"
    " WHERE (:include_schwab OR NOT " + _SCHWAB_BATCH + "))"
)

_FRAGMENTS = {
    "VISIBLE_DAILY_SIGNALS": VISIBLE_DAILY_SIGNALS,
    "VISIBLE_SNAPSHOTS": VISIBLE_SNAPSHOTS,
    "VISIBLE_SNAPSHOTS_ALL": VISIBLE_SNAPSHOTS_ALL,
    "VISIBLE_CAPTURE_BATCHES": VISIBLE_CAPTURE_BATCHES,
}

# Schwab-derived ticker/days in a date window, for readers that filter in Python.
_SCHWAB_DAYS_SQL = (
    "SELECT DISTINCT schwab_pb.ticker, schwab_pb.snap_date FROM options_capture_batches schwab_pb"
    " WHERE schwab_pb.snap_date >= :start AND schwab_pb.snap_date <= :end AND " + _SCHWAB_BATCH
)


def include_schwab_rows(flag: Any) -> bool:
    """True only for an explicit ``True`` (an admin request). Anything else excludes."""
    return flag is True


def provenance_params(include_schwab: Any = False) -> dict[str, Any]:
    """Bound parameters every ``VISIBLE_*`` fragment needs."""
    return {
        "include_schwab": include_schwab_rows(include_schwab),
        "schwab_from": SCHWAB_POSSIBLE_FROM.isoformat(),
    }


def visible_sql(sql: str, include_schwab: Any = False) -> Any:
    """``text(sql)`` with the ``VISIBLE_*`` parameters already bound.

    For a statement that uses one of the fragments; the caller's own
    parameters are passed to ``execute`` as usual.
    """
    from sqlalchemy import text

    return text(sql).bindparams(**provenance_params(include_schwab))


def _day(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


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
    """True when either provider field of a capture names Schwab (no date rule)."""
    return (str(spot_provider or "").strip().lower() == SCHWAB_PROVIDER
            or _metadata_provider(clock_metadata) == SCHWAB_PROVIDER)


def batch_is_schwab(spot_provider: Any, clock_metadata: Any, snap_date: Any) -> bool:
    """Python mirror of the SQL rule for one ``options_capture_batches`` row.

    Schwab when either provider field names Schwab, or when the provider is
    unknown and the batch is dated on or after ``SCHWAB_POSSIBLE_FROM`` (an
    undatable batch counts as after it).
    """
    if provider_is_schwab(spot_provider, clock_metadata):
        return True
    if str(spot_provider or "").strip().lower() in PUBLIC_PROVIDERS:
        return False
    day = _day(snap_date)
    return day is None or day >= SCHWAB_POSSIBLE_FROM


def schwab_days(conn: Any, start: Any, end: Any) -> set[tuple[str, date]]:
    """Every (ticker, day) in [start, end] that has a Schwab-derived batch.

    Raises what the connection raises: a caller that cannot read provenance
    must not fall back to serving the rows.
    """
    from sqlalchemy import text

    rows = conn.execute(
        text(_SCHWAB_DAYS_SQL),
        {"start": _day(start).isoformat(), "end": _day(end).isoformat(),
         "schwab_from": SCHWAB_POSSIBLE_FROM.isoformat()},
    ).fetchall()
    return {(str(r[0]).upper(), d) for r in rows if (d := _day(r[1])) is not None}


def options_feature_ticker(feature_name: Any) -> str | None:
    """The ticker prefix of a puller-written options feature name, or None.

    ``spy_pcr`` -> ``spy``; ``brk_b_iv_atm`` -> ``brk_b``. Returns the prefix
    as stored (lower case, ``-`` as ``_``); compare it with
    :func:`feature_prefix` of a ticker.
    """
    name = str(feature_name or "").strip().lower()
    for suffix in OPTIONS_FEATURE_SUFFIXES:
        tail = "_" + suffix
        if name.endswith(tail) and len(name) > len(tail):
            return name[: -len(tail)]
    return None


def feature_prefix(ticker: str) -> str:
    """How the options puller prefixes a ticker's features (``BRK-B`` -> ``brk_b``)."""
    return ticker.strip().lower().replace("-", "_")


def hidden_feature_dates(
    conn: Any, feature_name: Any, start: Any, end: Any, *, include_schwab: Any = False,
) -> set[date]:
    """Observation dates of an options feature that this reader must not see.

    Empty for an admin (``include_schwab is True``) and for a feature that is
    not a puller-written options feature. Otherwise every day in [start, end]
    on which the feature's ticker has a Schwab-derived batch.
    """
    if include_schwab_rows(include_schwab):
        return set()
    prefix = options_feature_ticker(feature_name)
    if prefix is None:
        return set()
    return {day for ticker, day in schwab_days(conn, start, end) if feature_prefix(ticker) == prefix}


def drop_hidden_rows(
    rows: Iterable[Any], hidden: set[tuple[str, date]] | set[date], *,
    ticker: Any = None, date_of: Any = None,
) -> list[Any]:
    """Drop rows whose (ticker, day) -- or day, when ``ticker`` is None -- is hidden."""
    kept = []
    for row in rows:
        day = _day(date_of(row)) if date_of is not None else None
        if ticker is None:
            key: Any = day
        else:
            key = (str(ticker(row) or "").upper(), day)
        if day is None or key in hidden:
            continue
        kept.append(row)
    return kept
