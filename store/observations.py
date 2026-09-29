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
* ``series_id`` is not a promise that only one puller writes to it.
  ``ingestion/tiingo_pull.py`` writes ``YF:{ticker}:{field}`` "Same naming as
  yfinance" under its own ``source_id``, and ``scripts/bulk_download_prices.py``
  loads a ``KAGGLE_BULK`` CSV under the identical ``YF:{ticker}:{field}``
  series_id too. A reader keyed on ``series_id`` alone therefore mixes rows
  written by different sources, and "latest pull wins" then picks whichever
  source happened to pull most recently — not a consistent source.

A reader that does ``SELECT value FROM raw_series WHERE series_id = :s ORDER
BY obs_date DESC LIMIT 1`` therefore gets, on any day a pull failed, a
literal ``0`` dated today, and on any day with two vintages an arbitrary one
of them (possibly from a different source than the previous read).
``normalization/resolver.py`` already filters ``pull_status =
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

* returns only ``pull_status = 'SUCCESS'`` rows (``PARTIAL``, ``FAILED`` and
  ``QUARANTINED`` -- a once-accepted row later found untrustworthy -- are
  never observations);
* collapses vintages to one row per ``obs_date`` — the latest
  ``pull_timestamp`` (``LATEST_AS_OF`` semantics, matching the default in
  ``store/pit.py``) — deterministically, in Python, so the SQL stays
  portable and testable on SQLite;
* accepts an optional ``as_of`` (a ``date``) and, for true point-in-time
  reads, an optional ``as_of_ts`` (a ``datetime``) that also excludes rows
  pulled after that instant;
* accepts an optional ``source`` (the ``source_catalog.name``, matched
  case-insensitively) to constrain the read to one puller's rows; without it,
  a read whose bounds would otherwise mix rows from more than one source for
  the same ``series_id`` raises :class:`MixedSourceError` instead of silently
  picking whichever source's row happens to sort first — see "Multi-source
  series_id" below;
* :func:`read_window_known_at` is the point-in-time variant: it returns an
  observation only if it was known by the end of ``as_of`` (pulled by then,
  or published by then per a caller-declared :class:`PublicationLag` for
  backfilled history), never merely because ``obs_date <= as_of``;
* returns :class:`Observation` records that carry their own provenance
  (``series_id``, ``obs_date``, ``pull_timestamp``, ``source``), so a caller
  can report freshness and origin instead of stamping ``now()`` or assuming
  a single puller.

Multi-source series_id
-----------------------
Pass ``source="yfinance"`` (etc.) when the series_id is known to be written by
more than one puller and the caller wants one of them specifically. Every
known caller of this module was audited when this constraint was added
(2026-09-26): none read a ``YF:*`` id without knowing which source they meant
in practice, so fail-closed is the default for everyone and the two ``YF:*``
callers now pass ``source="yfinance"`` explicitly
(``intelligence/sentiment_scorer.py``, ``intelligence/market_diary.py``).
Every other caller passes a series_id that today has exactly one
contributing source, so the fail-closed check is a no-op for them unless a
new puller starts writing under the same series_id later — in which case
failing closed (and pointing at this docstring) is the correct behaviour,
not a regression.

This module reads only; it never writes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import text

SUCCESS = "SUCCESS"


class MixedSourceError(Exception):
    """A series_id read without an explicit ``source`` spans more than one source_catalog entry.

    ``raw_series``'s uniqueness constraint is ``(series_id, source_id, obs_date,
    pull_timestamp)`` — a series_id is not a guarantee that only one puller
    writes to it. ``YF:{ticker}:{field}`` ids, for example, are written by
    ``ingestion/yfinance_pull.py`` (source ``yfinance``),
    ``ingestion/tiingo_pull.py`` (source ``tiingo``, "Same naming as
    yfinance" per that module's own comment), and
    ``scripts/bulk_download_prices.py``'s ``KAGGLE_BULK`` loader. Reading such
    a series_id without saying which source you mean would let "latest pull
    wins" silently pick whichever puller happened to run most recently.  Pass
    ``source=`` (a ``source_catalog.name``, case-insensitive) to disambiguate.
    """


@dataclass(frozen=True)
class Observation:
    """One accepted observation of a raw series."""

    series_id: str
    obs_date: date
    value: float
    pull_timestamp: datetime | None = None
    source: str | None = None
    # Set only by read_window_known_at: the UTC date the value is taken to
    # have been known by, and whether that came from a pull ("pulled") or a
    # declared publication lag ("modeled_lag").
    known_at: date | None = None
    known_at_basis: str | None = None

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
    """Collapse ``(obs_date, value, pull_timestamp, source_name)`` rows to one per obs_date.

    Rows must arrive ordered by ``obs_date ASC, pull_timestamp DESC`` (the
    queries below guarantee it), so the first row seen for a date is its
    latest vintage.
    """
    out: list[Observation] = []
    seen: date | None = None
    for obs_date, value, pull_ts, source_name in rows:
        if value is None:
            continue
        d = _coerce_date(obs_date)
        if seen is not None and d == seen:
            continue  # older vintage of the same observation date
        seen = d
        out.append(Observation(series_id, d, float(value), _coerce_ts(pull_ts), source_name))
    return out


def _distinct_sources(rows: Iterable[tuple]) -> list[str]:
    """Names present in a fetched ``(..., source_name)`` row set, sorted, ``None`` dropped."""
    return sorted({r[-1] for r in rows if r[-1] is not None})


def _raise_if_mixed(series_id: str, names: list[str]) -> None:
    if len(names) > 1:
        raise MixedSourceError(
            f"{series_id!r}: SUCCESS rows within the read bounds come from "
            f"multiple sources ({', '.join(names)}) — pass source= to disambiguate. "
            "See store.observations.MixedSourceError for why this fails closed."
        )


# Every query joins source_catalog: source is part of an Observation's
# provenance, and detecting a mixed-source series_id requires the name.
_WINDOW_SQL = (
    "SELECT r.obs_date, r.value, r.pull_timestamp, sc.name FROM raw_series r "
    "JOIN source_catalog sc ON sc.id = r.source_id "
    "WHERE r.series_id = :sid AND r.pull_status = :ok "
    "  AND (:source IS NULL OR LOWER(sc.name) = LOWER(:source)) "
    "  AND (:start IS NULL OR r.obs_date >= :start) "
    "  AND (:as_of IS NULL OR r.obs_date <= :as_of) "
    "  AND (:as_of_ts IS NULL OR r.pull_timestamp <= :as_of_ts) "
    "ORDER BY r.obs_date ASC, r.pull_timestamp DESC"
)

_LATEST_SQL = (
    "SELECT r.obs_date, r.value, r.pull_timestamp, sc.name FROM raw_series r "
    "JOIN source_catalog sc ON sc.id = r.source_id "
    "WHERE r.series_id = :sid AND r.pull_status = :ok "
    "  AND (:source IS NULL OR LOWER(sc.name) = LOWER(:source)) "
    "  AND (:as_of IS NULL OR r.obs_date <= :as_of) "
    "  AND (:as_of_ts IS NULL OR r.pull_timestamp <= :as_of_ts) "
    "ORDER BY r.obs_date DESC, r.pull_timestamp DESC LIMIT 1"
)

# Cheap pre-check for read_latest: names only, no values, so it is far
# lighter than fetching the whole bounded window just to test for mixing.
_SOURCE_NAMES_SQL = (
    "SELECT DISTINCT sc.name FROM raw_series r "
    "JOIN source_catalog sc ON sc.id = r.source_id "
    "WHERE r.series_id = :sid AND r.pull_status = :ok "
    "  AND (:as_of IS NULL OR r.obs_date <= :as_of) "
    "  AND (:as_of_ts IS NULL OR r.pull_timestamp <= :as_of_ts)"
)


def read_window(
    conn: Any,
    series_id: str,
    *,
    source: str | None = None,
    start: date | None = None,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
) -> list[Observation]:
    """Accepted observations of ``series_id`` in ``[start, as_of]``, oldest first.

    One row per observation date (latest vintage). Empty list when the series
    has no accepted rows in the window — never a zero, never a failed pull.

    ``source`` (a ``source_catalog.name``, case-insensitive) restricts the
    read to one puller's rows. Without it, if the bounded rows span more than
    one source for this ``series_id``, raises :class:`MixedSourceError`
    rather than silently collapsing vintages across sources.
    """
    rows = conn.execute(
        text(_WINDOW_SQL),
        {
            "sid": series_id, "ok": SUCCESS, "source": source,
            "start": start, "as_of": as_of, "as_of_ts": as_of_ts,
        },
    ).fetchall()
    if source is None:
        _raise_if_mixed(series_id, _distinct_sources(rows))
    return _dedup_latest_vintage(rows, series_id)


def read_latest(
    conn: Any,
    series_id: str,
    *,
    source: str | None = None,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
) -> Observation | None:
    """Newest accepted observation of ``series_id`` at or before ``as_of``.

    ``None`` when there is none. A FAILED marker row dated today is never
    returned, and when a date has several vintages the latest pull wins.

    ``source`` restricts the read to one puller's rows (see :func:`read_window`).
    Without it, raises :class:`MixedSourceError` if this ``series_id`` has
    accepted rows from more than one source within ``[as_of, as_of_ts]`` —
    checked before the single-row read, so a caller never gets an
    inconsistent "whichever source pulled last" answer.
    """
    if source is None:
        names = conn.execute(
            text(_SOURCE_NAMES_SQL),
            {"sid": series_id, "ok": SUCCESS, "as_of": as_of, "as_of_ts": as_of_ts},
        ).fetchall()
        _raise_if_mixed(series_id, sorted({n[0] for n in names if n[0] is not None}))
    row = conn.execute(
        text(_LATEST_SQL),
        {"sid": series_id, "ok": SUCCESS, "source": source, "as_of": as_of, "as_of_ts": as_of_ts},
    ).fetchone()
    if row is None or row[1] is None:
        return None
    return Observation(series_id, _coerce_date(row[0]), float(row[1]), _coerce_ts(row[2]), row[3])


def read_latest_n(
    conn: Any,
    series_id: str,
    n: int,
    *,
    source: str | None = None,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
) -> list[Observation]:
    """The ``n`` newest accepted observation dates, newest first.

    Replacement for ``ORDER BY obs_date DESC LIMIT n`` readers: the limit is
    applied *after* status filtering and vintage collapsing, so ``n`` means n
    distinct observation dates, not n rows.

    ``source`` restricts the read to one puller's rows (see :func:`read_window`);
    without it, raises :class:`MixedSourceError` on a mixed-source series_id.
    """
    rows = conn.execute(
        text(_WINDOW_SQL),
        {
            "sid": series_id, "ok": SUCCESS, "source": source,
            "start": None, "as_of": as_of, "as_of_ts": as_of_ts,
        },
    ).fetchall()
    if source is None:
        _raise_if_mixed(series_id, _distinct_sources(rows))
    obs = _dedup_latest_vintage(rows, series_id)
    return list(reversed(obs))[: max(0, int(n))]


def values(observations: Iterable[Observation]) -> list[float]:
    return [o.value for o in observations]


# ── Known-at (publication-time) reads ────────────────────────────────────────
#
# ``read_window(as_of=...)`` bounds by *observation* date only: a monthly
# series dated the 1st (UNRATE, INDPRO, ...) is returned at an ``as_of`` weeks
# before it was published, and a vintage pulled after ``as_of`` still replaces
# the value a past read saw. ``store/pit.py`` fixes both for ``resolved_series``
# via ``release_date <= as_of``, but ``raw_series`` has no release column, and
# ``resolved_series.release_date`` is the resolver's pull date — on griddb every
# FRED macro row was (back)filled on or after 2026-03-24, so a ``PITStore``
# read of 1990-2025 history returns little or nothing. ``read_window_known_at``
# applies the same "known by the end of ``as_of`` (UTC day)" contract to
# ``raw_series`` with two upper bounds on when a value became known:
#
# * the pull itself (proven: GRID had the row by then), and
# * ``obs_date + PublicationLag`` (modeled, conservative, declared per series by
#   the caller) for history that was backfilled long after publication.


@dataclass(frozen=True)
class PublicationLag:
    """An observation dated ``d`` is taken as public by the end of ``d + days``.

    ``unit`` is ``"calendar"`` or ``"business"`` (US federal holiday calendar;
    a weekend/holiday-dated observation rolls back to the prior business day
    first, the same convention as
    ``analysis.research_real_panel.publication_times``). ``basis`` documents
    where the number comes from. This is a *modeled* availability, used only
    where no pull evidence exists; make it conservative (late), never early.
    """

    days: int
    unit: str = "calendar"
    basis: str = ""
    # Exact reference-date overrides for documented exceptional releases.
    release_overrides: tuple[tuple[date, date], ...] = ()
    unpublished_dates: frozenset[date] = frozenset()

    def known_dates(self, obs_dates: list[date]) -> list[date]:
        """Modeled known-at date for each of ``obs_dates`` (same order)."""
        if not obs_dates:
            return []
        if self.unit == "calendar":
            overrides = dict(self.release_overrides)
            return [overrides.get(d, d + timedelta(days=self.days)) for d in obs_dates]
        if self.unit == "business":
            import numpy as np

            days = np.asarray(obs_dates, dtype="datetime64[D]")
            out = np.busday_offset(days, self.days, roll="backward", holidays=_us_holidays())
            return [d.astype(object) for d in out]
        raise ValueError(f"unknown PublicationLag unit {self.unit!r}")


_US_HOLIDAYS: Any = None


def _us_holidays() -> Any:
    global _US_HOLIDAYS
    if _US_HOLIDAYS is None:
        from pandas.tseries.holiday import USFederalHolidayCalendar

        _US_HOLIDAYS = (
            USFederalHolidayCalendar()
            .holidays("1980-01-01", "2045-12-31")
            .to_numpy()
            .astype("datetime64[D]")
        )
    return _US_HOLIDAYS


def _utc_date(ts: datetime | None) -> date | None:
    if ts is None:
        return None
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc)
    return ts.date()  # naive timestamps are taken as UTC (griddb runs Etc/UTC)


def read_window_known_at(
    conn: Any,
    series_id: str,
    *,
    as_of: date,
    lag: PublicationLag | None,
    source: str | None = None,
    start: date | None = None,
) -> list[Observation]:
    """Accepted observations of ``series_id`` that were known by the end of ``as_of``.

    Point-in-time counterpart of :func:`read_window` (same SUCCESS-only,
    source and ``[start, as_of]`` observation bounds). An observation date is
    returned only if it was known by the end of the UTC day ``as_of``:

    * **pulled** — some accepted vintage of it has
      ``pull_timestamp`` (UTC date) ``<= as_of``. The value is the latest
      such vintage (``LATEST_AS_OF``, as in ``store/pit.py``); vintages pulled
      after ``as_of`` are ignored.
    * **modeled** — otherwise, when ``lag`` is given and
      ``lag.known_dates([obs_date]) <= as_of``. The value is the *earliest*
      pulled vintage. This is a revised vintage as of backfill, not the
      first published value: release-date eligibility does not recover an ALFRED
      vintage. A later re-pull of an existing date does not replace that value.
      With ``lag=None`` there is no modeled path: pull evidence only.

    Either way the result for a given ``as_of`` depends only on rows pulled
    by ``as_of`` or on the earliest vintage of each date, so appending data
    later (new dates, re-pulls, revisions) never changes a past read. The
    mixed-source rule of :func:`read_window` is applied to the vintages known
    at ``as_of`` (every vintage pulled by then, plus the earliest vintage of a
    modeled date), so a second source appearing later cannot break a past
    read.

    Each returned :class:`Observation` has ``known_at`` set and
    ``known_at_basis`` = ``"pulled"`` or ``"modeled_lag"``.
    """
    rows = conn.execute(
        text(_WINDOW_SQL),
        {
            "sid": series_id, "ok": SUCCESS, "source": source,
            "start": start, "as_of": as_of, "as_of_ts": None,
        },
    ).fetchall()

    # Group vintages per obs_date. Rows arrive obs_date ASC, pull_timestamp
    # DESC; values that are NULL are not observations.
    groups: list[tuple[date, list[tuple[float, datetime | None, str | None]]]] = []
    for obs_date, value, pull_ts, source_name in rows:
        if value is None:
            continue
        d = _coerce_date(obs_date)
        if groups and groups[-1][0] == d:
            groups[-1][1].append((float(value), _coerce_ts(pull_ts), source_name))
        else:
            groups.append((d, [(float(value), _coerce_ts(pull_ts), source_name)]))

    modeled = lag.known_dates([d for d, _ in groups]) if lag is not None else [None] * len(groups)

    out: list[Observation] = []
    known_sources: set[str] = set()
    release_overrides = dict(lag.release_overrides) if lag is not None else {}
    for (d, vintages), modeled_known in zip(groups, modeled):
        if lag is not None and d in lag.unpublished_dates:
            continue  # official reference period was never published
        if release_overrides.get(d, date.min) > as_of:
            continue  # an anomalous early pull cannot override an official embargo
        pulled = [(v, ts, s, _utc_date(ts)) for v, ts, s in vintages]
        proven = [p for p in pulled if p[3] is not None and p[3] <= as_of]
        if proven:
            v, ts, s, pull_day = proven[0]  # latest vintage pulled by as_of
            known = pull_day if modeled_known is None else min(pull_day, modeled_known)
            known = max(d, known, release_overrides.get(d, date.min))
            out.append(Observation(series_id, d, v, ts, s, known, "pulled"))
            known_sources.update(p[2] for p in proven if p[2] is not None)
        elif modeled_known is not None and modeled_known <= as_of:
            v, ts, s, _ = pulled[-1]  # earliest pulled vintage
            out.append(Observation(series_id, d, v, ts, s, modeled_known, "modeled_lag"))
            if s is not None:
                known_sources.add(s)

    if source is None:
        _raise_if_mixed(series_id, sorted(known_sources))
    return out
