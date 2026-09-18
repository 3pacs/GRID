"""Vintage-safe, status-aware reads of ``raw_series`` for analytical code.

Why this exists
---------------
``raw_series`` is an append-only pull log, not a clean time series:

* Several pullers (``ingestion/fred.py``, ``ingestion/altdata/cftc_cot.py``,
  ``baltic_dry.py``, ``international/ecb.py``, ``world_bank_puller.py``)
  record a pull *failure* as a row with ``pull_status = 'FAILED'``,
  ``value = 0`` and ``obs_date = date.today()``. The column is ``NOT NULL``,
  so the marker has to carry a number, and that number is zero.
* Every successful pull appends a new row per ``(series_id, obs_date,
  pull_timestamp)`` (``uq_raw_series_composite``), so one observation date
  routinely has several vintages with different values (revisions, re-pulls,
  recomputed series such as ``COMPUTED:fed_net_liquidity``).

A reader that does ``SELECT value FROM raw_series WHERE series_id = :s ORDER
BY obs_date DESC LIMIT 1`` therefore gets, on any day a pull failed, a
literal ``0`` dated today, and on any day with two vintages an arbitrary one
of them. ``normalization/resolver.py`` already filters ``pull_status =
'SUCCESS'`` when it builds ``resolved_series``; the direct readers in
``intelligence/``, ``analysis/`` and ``api/routers/`` did not. Measured on
griddb 2026-09-17 (read-only): in the trailing 90 days WALCL had 5 FAILED
zero rows (newest on the same date as its newest real value), T10Y2Y 19
(newest dated today), RRPONTSYD 9, WTREGEN 4, BAMLH0A0HYM2 5; RRPONTSYD had
4 of its last 60 observation dates duplicated; ``COMPUTED:fed_net_liquidity``
carried two different values for 2026-09-17.

Contract
--------
Every function here:

* returns only ``pull_status = 'SUCCESS'`` rows (``PARTIAL`` and ``FAILED``
  are never observations);
* collapses vintages to one row per ``obs_date`` — the latest
  ``pull_timestamp`` (``LATEST_AS_OF`` semantics, matching the default in
  ``store/pit.py``) — deterministically, in Python, so the SQL stays
  portable and testable on SQLite;
* accepts an optional ``as_of`` (a ``date``) and, for true point-in-time
  reads, an optional ``as_of_ts`` (a ``datetime``) that also excludes rows
  pulled after that instant;
* returns :class:`Observation` records that carry their own provenance
  (``series_id``, ``obs_date``, ``pull_timestamp``), so a caller can report
  freshness instead of stamping ``now()``.

This module reads only; it never writes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable

from sqlalchemy import text

SUCCESS = "SUCCESS"


@dataclass(frozen=True)
class Observation:
    """One accepted observation of a raw series."""

    series_id: str
    obs_date: date
    value: float
    pull_timestamp: datetime | None = None

    @property
    def age_days(self) -> int:
        return (date.today() - self.obs_date).days


def _coerce_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def _coerce_ts(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v))
    except ValueError:
        return None


def _dedup_latest_vintage(rows: Iterable[tuple], series_id: str) -> list[Observation]:
    """Collapse ``(obs_date, value, pull_timestamp)`` rows to one per obs_date.

    Rows must arrive ordered by ``obs_date ASC, pull_timestamp DESC`` (the
    queries below guarantee it), so the first row seen for a date is its
    latest vintage.
    """
    out: list[Observation] = []
    seen: date | None = None
    for obs_date, value, pull_ts in rows:
        if value is None:
            continue
        d = _coerce_date(obs_date)
        if seen is not None and d == seen:
            continue  # older vintage of the same observation date
        seen = d
        out.append(Observation(series_id, d, float(value), _coerce_ts(pull_ts)))
    return out


_WINDOW_SQL = (
    "SELECT obs_date, value, pull_timestamp FROM raw_series "
    "WHERE series_id = :sid AND pull_status = :ok "
    "  AND (:start IS NULL OR obs_date >= :start) "
    "  AND (:as_of IS NULL OR obs_date <= :as_of) "
    "  AND (:as_of_ts IS NULL OR pull_timestamp <= :as_of_ts) "
    "ORDER BY obs_date ASC, pull_timestamp DESC"
)

_LATEST_SQL = (
    "SELECT obs_date, value, pull_timestamp FROM raw_series "
    "WHERE series_id = :sid AND pull_status = :ok "
    "  AND (:as_of IS NULL OR obs_date <= :as_of) "
    "  AND (:as_of_ts IS NULL OR pull_timestamp <= :as_of_ts) "
    "ORDER BY obs_date DESC, pull_timestamp DESC LIMIT 1"
)


def read_window(
    conn: Any,
    series_id: str,
    *,
    start: date | None = None,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
) -> list[Observation]:
    """Accepted observations of ``series_id`` in ``[start, as_of]``, oldest first.

    One row per observation date (latest vintage). Empty list when the series
    has no accepted rows in the window — never a zero, never a failed pull.
    """
    rows = conn.execute(
        text(_WINDOW_SQL),
        {"sid": series_id, "ok": SUCCESS, "start": start, "as_of": as_of, "as_of_ts": as_of_ts},
    ).fetchall()
    return _dedup_latest_vintage(rows, series_id)


def read_latest(
    conn: Any,
    series_id: str,
    *,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
) -> Observation | None:
    """Newest accepted observation of ``series_id`` at or before ``as_of``.

    ``None`` when there is none. A FAILED marker row dated today is never
    returned, and when a date has several vintages the latest pull wins.
    """
    row = conn.execute(
        text(_LATEST_SQL),
        {"sid": series_id, "ok": SUCCESS, "as_of": as_of, "as_of_ts": as_of_ts},
    ).fetchone()
    if row is None or row[1] is None:
        return None
    return Observation(series_id, _coerce_date(row[0]), float(row[1]), _coerce_ts(row[2]))


def read_latest_n(
    conn: Any,
    series_id: str,
    n: int,
    *,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
) -> list[Observation]:
    """The ``n`` newest accepted observation dates, newest first.

    Replacement for ``ORDER BY obs_date DESC LIMIT n`` readers: the limit is
    applied *after* status filtering and vintage collapsing, so ``n`` means n
    distinct observation dates, not n rows.
    """
    rows = conn.execute(
        text(_WINDOW_SQL),
        {"sid": series_id, "ok": SUCCESS, "start": None, "as_of": as_of, "as_of_ts": as_of_ts},
    ).fetchall()
    obs = _dedup_latest_vintage(rows, series_id)
    return list(reversed(obs))[: max(0, int(n))]


def values(observations: Iterable[Observation]) -> list[float]:
    return [o.value for o in observations]
