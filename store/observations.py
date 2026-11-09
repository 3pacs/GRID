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


_VINTAGES_SQL = (
    "SELECT r.obs_date, r.value, r.pull_timestamp, sc.name FROM raw_series r "
    "JOIN source_catalog sc ON sc.id = r.source_id "
    "WHERE r.series_id = :sid AND r.pull_status = :ok "
    "  AND r.obs_date >= :start AND r.obs_date <= :end "
    "ORDER BY r.obs_date ASC, r.pull_timestamp ASC LIMIT :limit"
)
MAX_VINTAGES = 5000


def read_vintages(
    conn: Any,
    series_id: str,
    *,
    start: date,
    end: date,
    limit: int = MAX_VINTAGES,
) -> list[Observation]:
    """Every accepted vintage of ``series_id`` labelled in ``[start, end]``, every source,
    oldest label then oldest pull first.

    Unlike :func:`read_window` nothing is deduplicated: intraday captures, the completed
    close and later revisions all come back with their pull times. For checking a
    statement made at a known instant ("was VIX really 26.9 then?"), where the latest
    vintage alone answers a different question. Bounded by the label window and ``limit``.
    """
    rows = conn.execute(
        text(_VINTAGES_SQL),
        {"sid": series_id, "ok": SUCCESS, "start": start, "end": end, "limit": int(limit)},
    ).fetchall()
    out: list[Observation] = []
    for row in rows:
        if row[1] is None:
            continue
        out.append(Observation(series_id, _coerce_date(row[0]), float(row[1]), _coerce_ts(row[2]), row[3]))
    return out


def read_latest_n(
    conn: Any,
    series_id: str,
    n: int,
    *,
    source: str | None = None,
    start: date | None = None,
    as_of: date | None = None,
    as_of_ts: datetime | None = None,
) -> list[Observation]:
    """The ``n`` newest accepted observation dates, newest first.

    Replacement for ``ORDER BY obs_date DESC LIMIT n`` readers: the limit is
    applied *after* status filtering and vintage collapsing, so ``n`` means n
    distinct observation dates, not n rows.

    ``source`` restricts the read to one puller's rows (see :func:`read_window`);
    without it, raises :class:`MixedSourceError` on a mixed-source series_id.

    ``start`` (optional) bounds the scan to ``obs_date >= start``. Without it
    every accepted vintage back to the first observation is fetched and
    collapsed in Python only to keep ``n`` rows -- ~2M rows for a long YF
    series. Pass it whenever the caller knows how far back ``n`` dates can
    be; fewer than ``n`` dates inside the bound returns fewer rows.
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
    by ``as_of`` or on the earliest vintage of each date. Later re-pulls and
    revisions do not replace an existing modeled vintage; a late backfill of
    a previously absent observation date can change a past modeled read. The
    mixed-source rule of :func:`read_window` is applied to the vintages known
    at ``as_of`` (every vintage pulled by then, plus every source tied at the
    earliest timestamp of a modeled date), so a second source pulled strictly
    later cannot break a past read.

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
            # SQL does not order sources tied at the earliest timestamp.
            # All tied sources must participate in the ambiguity guard.
            known_sources.update(p[2] for p in pulled if p[1] == ts and p[2] is not None)

    if source is None:
        _raise_if_mixed(series_id, sorted(known_sources))
    return out



def read_daily_policy_n(
    conn: Any, series_id: str, n: int, *, start: date,
    source: str = "yfinance", as_of: date | None = None,
    as_of_ts: datetime | None = None,
) -> list[Observation]:
    """Newest daily capture-policy observations, newest first.

    Raw SUCCESS rows without validated policy provenance remain available
    through the existing raw readers. They are not completed-session evidence.
    Filtering occurs before vintage collapse and count. A PIT cutoff applies
    before policy selection, so a later eligible capture cannot repair a past
    as-of view. This does not assert provider-certified finality.
    """
    import json
    from price_close_contract import is_daily_policy_capture
    if source.lower() != "yfinance" or not series_id.startswith("YF:"):
        raise ValueError("daily capture selection requires an explicit yfinance series")
    sql = _WINDOW_SQL.replace("r.pull_timestamp, sc.name", "r.pull_timestamp, sc.name, r.raw_payload")
    rows = conn.execute(text(sql), {
        "sid": series_id, "ok": SUCCESS, "source": source,
        "start": start, "as_of": as_of, "as_of_ts": as_of_ts,
    }).fetchall()
    qualified = []
    for d, value, ts, name, payload in rows:
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (TypeError, ValueError):
                continue
        if is_daily_policy_capture(payload, series_id, _coerce_date(d), _coerce_ts(ts)):
            qualified.append((d, value, ts, name))
    return list(reversed(_dedup_latest_vintage(qualified, series_id)))[:max(0, int(n))]


# ── Completed closes derived from the capture boundary ─────────────────────
#
# ``read_daily_policy_n`` only accepts rows whose payload carries a capture
# marker. The yfinance puller started writing markers in the autumn of 2026
# (``spy_close_v1`` from 2026-09-23, a backfill at 2026-10-19 back to May),
# so every earlier close -- although pulled after its label day had ended --
# is invisible to it. The marker records exactly one fact: the pull happened
# at or after the series' capture boundary. ``read_completed_closes`` applies
# that same predicate to the row's own ``pull_timestamp``, so older captures
# qualify by the identical rule, and it refuses (``ambiguous``) a label whose
# post-boundary vintages disagree, which is how an adjusted-close writer
# sharing the series id shows up.

COMPLETED_CLOSE_REL_TOLERANCE = 5e-4  # 0.05 %; float32 noise is ~1e-8
_COMPLETED_CLOSE_SOURCES = frozenset({"yfinance", "tiingo"})


@dataclass(frozen=True)
class CompletedClose:
    """Every post-boundary vintage of one label, oldest pull first."""

    series_id: str
    obs_date: date
    vintages: tuple[tuple[datetime, float], ...]
    source: str | None
    ambiguous: bool

    @property
    def latest(self) -> Observation:
        ts, value = self.vintages[-1]
        return Observation(self.series_id, self.obs_date, value, ts, self.source)

    @property
    def first(self) -> Observation:
        """The earliest completed vintage (closest to the label's end)."""
        ts, value = self.vintages[0]
        return Observation(self.series_id, self.obs_date, value, ts, self.source)

    @property
    def first_completed_pull(self) -> datetime:
        return self.vintages[0][0]

    def known_at(self, ts: datetime) -> Observation | None:
        """Newest vintage pulled at or before ``ts`` (``None`` if none was)."""
        known = [(t, v) for t, v in self.vintages if t <= ts]
        if not known:
            return None
        t, v = known[-1]
        return Observation(self.series_id, self.obs_date, v, t, self.source)


def read_completed_closes(
    conn: Any,
    series_id: str,
    *,
    start: date,
    end: date,
    boundary: Any,
    source: str | tuple[str, ...] = "yfinance",
    as_of_ts: datetime | None = None,
    rel_tolerance: float = COMPLETED_CLOSE_REL_TOLERANCE,
) -> dict[date, CompletedClose]:
    """Completed closes of ``series_id`` labelled in ``[start, end]``.

    ``boundary(obs_date)`` returns the aware UTC instant after which a capture
    of that label is a completed close, or ``None`` when the label is not a
    session the caller's calendar knows (it is then skipped). A row qualifies
    when it is ``SUCCESS``, from ``source``, pulled at or after the boundary
    (and at or before ``as_of_ts``), positive and finite, and its payload --
    if it has one -- does not label it otherwise (a ``capture_state`` other
    than ``POLICY_ELIGIBLE`` or a different ``price_basis``).

    ``source`` may name several raw-close writers of the same ``YF:*`` id --
    only ``yfinance`` and ``TIINGO`` (whose ``close`` field is the raw,
    unadjusted EOD close; ``adjClose`` goes to ``:adj_close``) are accepted.
    Their vintages of one label are pooled, so a cross-source disagreement
    beyond ``rel_tolerance`` makes the label ``ambiguous`` rather than
    letting one source win silently.
    """
    import json
    from math import isfinite

    sources = (source,) if isinstance(source, str) else tuple(source)
    if (not sources or not series_id.startswith("YF:")
            or any(s.lower() not in _COMPLETED_CLOSE_SOURCES for s in sources)):
        raise ValueError("completed-close selection requires a YF series and yfinance/TIINGO sources")
    sql = _WINDOW_SQL.replace("r.pull_timestamp, sc.name", "r.pull_timestamp, sc.name, r.raw_payload")
    rows: list = []
    for src in sources:
        rows.extend(conn.execute(text(sql), {
            "sid": series_id, "ok": SUCCESS, "source": src,
            "start": start, "as_of": end, "as_of_ts": as_of_ts,
        }).fetchall())
    grouped: dict[date, list[tuple[datetime, float]]] = {}
    names: dict[date, set[str]] = {}
    for d, value, ts, name, payload in rows:
        obs = _coerce_date(d)
        pulled = _coerce_ts(ts)
        edge = boundary(obs)
        if edge is None or pulled is None or pulled.tzinfo is None or pulled < edge:
            continue
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (TypeError, ValueError):
                payload = None
        if isinstance(payload, dict):
            state = payload.get("capture_state")
            if state is not None and state != "POLICY_ELIGIBLE":
                continue
            basis = payload.get("price_basis")
            if basis is not None and basis != series_id:
                continue
        try:
            v = float(value)
        except (TypeError, ValueError):
            continue
        if not isfinite(v) or v <= 0:
            continue
        grouped.setdefault(obs, []).append((pulled, v))
        names.setdefault(obs, set()).add(str(name))
    out: dict[date, CompletedClose] = {}
    for obs, vintages in grouped.items():
        vintages.sort(key=lambda tv: tv[0])
        lo = min(v for _, v in vintages)
        hi = max(v for _, v in vintages)
        out[obs] = CompletedClose(
            series_id, obs, tuple(vintages), "+".join(sorted(names.get(obs, ()))) or None,
            ambiguous=(hi - lo) / lo > rel_tolerance,
        )
    return out


# ── Stock splits (dated corporate actions) ──────────────────────────────────
#
# Why: GRID's ``YF:{T}:close`` history is not on one share basis. TIINGO's
# ``close`` is the as-traded (unadjusted) print; Yahoo's ``Close`` (yfinance
# ``auto_adjust=False``) is split-adjusted for every split up to the moment
# it was pulled. The April 2026 TIINGO backfill (pulled 2026-04-01..04-20)
# appended raw vintages over the 2026-03-29 yfinance backfill, so "latest
# vintage wins" now hands readers as-traded prices for history: NVDA
# 2024-06-07 reads 1208.88 (raw) while 2024-06-10 reads 121.79. A max
# drawdown over that window (1224.40 -> 94.31) reports NVDA "down 92%"
# (measured on griddb 2026-11-06 via /api/v1/ten-year-portfolio/weekly:
# NVDA max_drawdown 0.922, AMZN 0.977, GOOGL 0.971, TSLA 0.949).
#
# Split events are stored additively in ``raw_series`` under
# ``YF:{symbol}:split_factor`` with ``obs_date`` = the effective (ex) date and
# ``value`` = new shares per old share (10.0 = NVDA 10-for-1 on 2024-06-10;
# 0.1 = a 1-for-10 reverse split). Both TIINGO's ``splitFactor`` and
# yfinance's ``Ticker.splits`` use this convention. Only non-unit factors are
# stored: absence of a row means "no split recorded", not "checked, none".
#
# Semantics chosen here (document any change):
# * Price returns are SPLIT-adjusted only, never dividend-adjusted. Splits
#   change the share count, not the holder's wealth; dividends are a cash
#   return and belong in a total-return calculation that asks for it
#   (``adj_close``) explicitly.
# * Point-in-time: a split dated ``s`` multiplies a price only when the
#   price being converted is dated on or after ``s`` (``after < s <=
#   through``); ``as_of_ts`` also hides split rows pulled after that instant.
#   Nothing here reads a price dated after the caller's bound.
# * A yfinance close pulled at ``p`` already carries every split dated in
#   ``(obs_date, p]``; :func:`price_in_basis` undoes that before re-basing,
#   so mixed TIINGO/yfinance vintages land on one basis.

SPLIT_FIELD = "split_factor"
# Sources whose stored ``close`` is split-adjusted as of the pull.
SPLIT_ADJUSTED_AT_PULL_SOURCES: frozenset[str] = frozenset({"yfinance"})
SPLIT_RATIO_EPS = 1e-9


def split_series_id(symbol: str) -> str:
    """``YF:{symbol}:split_factor`` -- same symbol namespace as the closes."""
    return f"YF:{symbol}:{SPLIT_FIELD}"


@dataclass(frozen=True)
class SplitEvent:
    """One recorded split: ``ratio`` new shares per old share, effective ``effective_date``."""

    symbol: str
    effective_date: date
    ratio: float
    source: str | None = None
    pull_timestamp: datetime | None = None


_SPLITS_SQL = (
    "SELECT r.obs_date, r.value, r.pull_timestamp, sc.name FROM raw_series r "
    "JOIN source_catalog sc ON sc.id = r.source_id "
    "WHERE r.series_id = :sid AND r.pull_status = :ok "
    "  AND (:start IS NULL OR r.obs_date >= :start) "
    "  AND (:end IS NULL OR r.obs_date <= :end) "
    "  AND (:as_of_ts IS NULL OR r.pull_timestamp <= :as_of_ts) "
    "ORDER BY r.obs_date ASC, r.pull_timestamp DESC"
)


def read_splits(
    conn: Any,
    symbol: str,
    *,
    start: date | None = None,
    end: date | None = None,
    as_of_ts: datetime | None = None,
) -> list[SplitEvent]:
    """Recorded splits for ``symbol`` with effective dates in ``[start, end]``, oldest first.

    SUCCESS rows only, one event per effective date (latest vintage). A split
    is a fact several providers report, so rows from more than one source are
    expected and do not raise :class:`MixedSourceError`; the latest pull wins.
    Unit or non-positive factors are dropped.
    """
    import math

    rows = conn.execute(
        text(_SPLITS_SQL),
        {"sid": split_series_id(symbol), "ok": SUCCESS, "start": start, "end": end,
         "as_of_ts": as_of_ts},
    ).fetchall()
    out: list[SplitEvent] = []
    for obs in _dedup_latest_vintage(rows, split_series_id(symbol)):
        r = obs.value
        if not math.isfinite(r) or r <= 0 or abs(r - 1.0) < SPLIT_RATIO_EPS:
            continue
        out.append(SplitEvent(symbol, obs.obs_date, r, obs.source, obs.pull_timestamp))
    return out


def cumulative_split_factor(splits: Iterable[SplitEvent], after: date, through: date) -> float:
    """Product of split ratios with ``after < effective_date <= through`` (1.0 if none).

    A price dated ``through`` times this factor is expressed in the share
    basis of ``after``. Splits dated after ``through`` are never applied.
    """
    factor = 1.0
    if through <= after:
        return factor
    for s in splits:
        if after < s.effective_date <= through:
            factor *= s.ratio
    return factor


def _ts_date(ts: datetime | None) -> date | None:
    if ts is None:
        return None
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc)
    return ts.date()


def price_in_basis(
    value: float,
    obs_date: date,
    *,
    basis_date: date,
    splits: Iterable[SplitEvent],
    source: str | None = None,
    pull_timestamp: datetime | None = None,
) -> float:
    """Express a stored close dated ``obs_date`` in the share basis of ``basis_date``.

    1. To as-traded: a close from a source in
       :data:`SPLIT_ADJUSTED_AT_PULL_SOURCES` pulled at ``p`` is multiplied
       back by the splits dated in ``(obs_date, p]`` (Yahoo had already divided
       them out). Other sources (TIINGO) are already as-traded.
    2. Re-base: an earlier ``basis_date`` multiplies by splits in
       ``(basis_date, obs_date]``; a later one divides by splits in
       ``(obs_date, basis_date]``.

    NVDA example: entry 2024-06-05 at 1224.40, exit 2024-06-12 TIINGO 125.20,
    10:1 on 2024-06-10 -> 1252.00 in entry shares (+2.3%, not -89.8%).
    """
    split_list = list(splits)
    as_traded = float(value)
    if source is not None and source.lower() in {s.lower() for s in SPLIT_ADJUSTED_AT_PULL_SOURCES}:
        pulled = _ts_date(pull_timestamp)
        if pulled is not None:
            as_traded *= cumulative_split_factor(split_list, obs_date, pulled)
    if basis_date < obs_date:
        return as_traded * cumulative_split_factor(split_list, basis_date, obs_date)
    if basis_date > obs_date:
        return as_traded / cumulative_split_factor(split_list, obs_date, basis_date)
    return as_traded


def split_adjusted_return(
    conn: Any,
    symbol: str,
    start_date: date,
    end_date: date,
    *,
    source: str,
    as_of_ts: datetime | None = None,
) -> float | None:
    """Split-adjusted (not dividend-adjusted) price return of ``symbol`` from ``start_date`` to ``end_date``.

    Reads the latest accepted ``YF:{symbol}:close`` at or before each date
    from one ``source`` and the recorded splits, puts both prices on the
    share basis of the start close, and returns ``end / start - 1`` as a
    fraction. ``None`` when either close is missing.
    """
    sid = f"YF:{symbol}:close"
    p0 = read_latest(conn, sid, source=source, as_of=start_date, as_of_ts=as_of_ts)
    p1 = read_latest(conn, sid, source=source, as_of=end_date, as_of_ts=as_of_ts)
    if p0 is None or p1 is None or p0.value <= 0 or p1.obs_date < p0.obs_date:
        return None
    splits = read_splits(conn, symbol, as_of_ts=as_of_ts)
    base = p0.obs_date
    v0 = price_in_basis(p0.value, p0.obs_date, basis_date=base, splits=splits,
                        source=p0.source, pull_timestamp=p0.pull_timestamp)
    v1 = price_in_basis(p1.value, p1.obs_date, basis_date=base, splits=splits,
                        source=p1.source, pull_timestamp=p1.pull_timestamp)
    return v1 / v0 - 1.0 if v0 > 0 else None


def split_jump_confirmed(
    points: list[tuple[date, float]], split: SplitEvent, *, tolerance: float = 0.5,
) -> bool:
    """True when ``points`` show the price discontinuity ``split`` implies.

    Compares the last close before the effective date with the first on or
    after it: the observed ratio ``prev / post`` must be within
    ``ratio ** (1 +- tolerance)``. A series some upstream already adjusted
    (ratio ~1 across the date) is left alone, so a split is never applied
    twice. Used for stored histories whose per-row source is unknown
    (``resolved_series``).
    """
    import math

    prev = post = None
    for d, v in points:
        if d < split.effective_date:
            prev = v
        elif post is None:
            post = v
            break
    if prev is None or post is None or prev <= 0 or post <= 0:
        return False
    want = math.log(split.ratio)
    seen = math.log(prev / post)
    return abs(seen - want) <= tolerance * abs(want)


def adjust_history_for_splits(
    points: list[tuple[date, float]],
    splits: Iterable[SplitEvent],
    *,
    verify: bool = True,
) -> tuple[list[tuple[date, float]], list[SplitEvent]]:
    """Back-adjust a ``[(date, close), ...]`` history to the share basis of its last point.

    Every close dated before a split's effective date is divided by that
    split's ratio; splits dated after the last point are never applied (they
    had not happened yet). With ``verify`` (default) a split is applied only
    when :func:`split_jump_confirmed` sees its discontinuity in ``points``.
    Returns the adjusted points and the splits actually applied.
    """
    pts = sorted(points)
    if not pts:
        return [], []
    last = pts[-1][0]
    applied = [
        s for s in sorted(splits, key=lambda e: e.effective_date)
        if pts[0][0] < s.effective_date <= last and (not verify or split_jump_confirmed(pts, s))
    ]
    if not applied:
        return pts, []
    out = [(d, v / cumulative_split_factor(applied, d, last)) for d, v in pts]
    return out, applied
