"""Receipt-aware ETF flow evidence for the flow map.

``ETF_FLOW:<ticker>:5d`` rows in ``raw_series`` can carry more than one
vintage of a date: the writer revises a recent date whose value changed, and
it stores a date no earlier than the day after it. The materialized
``etf_flows`` table (and the ``dollar_flows`` rows derived from the same
series) keep only the newest value, with no record of when it was received:
they are current views.

A flow map built for a past ``as_of`` must not read a current view, or a
later correction leaks into the past (Codex review 6084431576: 403 first seen
on 20 November, corrected to 490 on the 24th, re-materialized, came back as
490 for ``as_of`` = 20 November). Every builder that takes ``as_of`` goes
through here for a past date:

* ``known_5d_flows`` reads the raw series through
  ``store.observations.read_window_known_at`` with pull evidence only: the
  newest vintage pulled by the end of the UTC day ``as_of``; a date first
  received later is not returned.
* ``is_historical`` says when that is required. On or after today's UTC date
  a builder may keep reading its current view.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import text

from store.observations import Observation, read_window_known_at

RAW_SOURCE = "raw_series:ETF_FLOW:*:5d"
_PATTERN = "ETF_FLOW:%:5d"


def _utc_today() -> date:
    """Today's UTC date. One name so a test can set it."""
    return datetime.now(timezone.utc).date()


def is_historical(as_of: date) -> bool:
    """True when ``as_of`` is a past date: current views must not be read."""
    return as_of < _utc_today()


def known_5d_flows(
    conn: Any,
    as_of: date,
    start: date,
    tickers: Iterable[str] | None = None,
) -> dict[str, list[Observation]]:
    """5-day ETF flow observations in ``[start, as_of]`` held by the end of ``as_of``.

    Keyed by ticker, each list in date order. A ticker with nothing held by
    then is absent. ``tickers`` limits the read; default every ETF flow series.
    """
    if tickers is None:
        series_ids = sorted(str(row[0]) for row in conn.execute(
            text(
                "SELECT DISTINCT series_id FROM raw_series "
                "WHERE series_id LIKE :pattern AND obs_date >= :start AND obs_date <= :end "
                "AND pull_status = 'SUCCESS'"
            ),
            {"pattern": _PATTERN, "start": start, "end": as_of},
        ).fetchall())
    else:
        series_ids = [f"ETF_FLOW:{ticker}:5d" for ticker in tickers]

    held: dict[str, list[Observation]] = {}
    for series_id in series_ids:
        observations = read_window_known_at(conn, series_id, as_of=as_of, lag=None, start=start)
        if observations:
            held[series_id.split(":")[1]] = observations
    return held
