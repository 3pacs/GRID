"""D1 event selection under the contract's clocks: the §8 fixed-clock path (G14; research only).

Offline and pure. Every time is an explicit argument, so nothing here reads a wall clock, a
database or the network. Given 8-K observations (one record per version GRID saw), a close tape
and a calendar, ``select_events`` decides each accession once, with the clocks of contract §2.1
and §5.1:

* Day 0 is the first regular session whose actual close is strictly after A + 5 minutes; P-1 is
  the last close strictly before A; O is day 0's actual close; D is 20:00 ET on day 0's date.
* Forward period: every observation used must be known and available by D; an unknown clock is
  ``EXCLUDED_UNKNOWN_AVAILABILITY``, an observation after D is ``EXCLUDED_LATE_INPUT`` (counted,
  never deferred, never backdated).
* A filing seen in two different versions, both visible by D, is ``EXCLUDED_REVISED_INPUT``: its
  first availability is ambiguous. A version first visible after D is ignored and logged; the
  decision made at D stands (§2.2, §2.3). Identical re-observations are one event.
* One event per accession; Item 2.02 makes it F1 only.
* Historical period: closes may be read only as ``PROXY_EOD_BY_DEADLINE``; the clocks stay unknown,
  nothing is late, and the result is ``DIAGNOSTIC_ONLY`` and never selected.
* Only closes at or before day 0's close are ever read from the tape, so no later price can
  change direction, eligibility, strength or quintile.

Not modelled: the universe and liquidity filter (G3, the caller must apply it), order latency
(daily data only in v1), outcomes, and any look. A zero day-0 move has direction 0 and is not
excluded; whether it should be is a contract decision, not made here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from typing import Any, Iterable, Mapping, Protocol
from zoneinfo import ZoneInfo

from analysis import d1_ranking as R
from ingestion import market_calendar as MC

SELECTION_VERSION = "d1-selection-v1"
ET = ZoneInfo("America/New_York")
ACCEPTANCE_MARGIN = timedelta(minutes=5)
DEADLINE_ET = time(20, 0)
VOL_CLOSES = R.VOL_RETURNS + 1

FORWARD = "forward"
HISTORICAL = "historical"
PROXY_EOD_BY_DEADLINE = "PROXY_EOD_BY_DEADLINE"

SELECTED = "SELECTED"
DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"
EXCLUDED_UNKNOWN_AVAILABILITY = "EXCLUDED_UNKNOWN_AVAILABILITY"
EXCLUDED_LATE_INPUT = "EXCLUDED_LATE_INPUT"
EXCLUDED_REVISED_INPUT = "EXCLUDED_REVISED_INPUT"
EXCLUDED_INPUT = R.EXCLUDED_INPUT
EXCLUDED_NO_FAMILY = "EXCLUDED_NO_FAMILY"
EXCLUDED_MALFORMED = "EXCLUDED_MALFORMED"

REFUSALS = (EXCLUDED_UNKNOWN_AVAILABILITY, EXCLUDED_LATE_INPUT, EXCLUDED_REVISED_INPUT, EXCLUDED_INPUT,
            EXCLUDED_NO_FAMILY, EXCLUDED_MALFORMED)


class Calendar(Protocol):
    def is_open(self, day: date) -> bool: ...
    def close_time(self, day: date) -> time: ...


class RegularCalendar:
    """The repository's NYSE calendar (actual closes, including early closes)."""

    def is_open(self, day: date) -> bool:
        return MC.is_market_open(day)

    def close_time(self, day: date) -> time:
        return MC.market_close_time(day)


@dataclass(frozen=True)
class Filing:
    """One version of one 8-K as GRID observed it: publication (A) and arrival (retrieved, visible) kept apart."""

    accession: Any
    items: tuple[str, ...]
    acceptance: datetime | None
    retrieved_at: datetime | None
    visible_at: datetime | None
    content_sha256: str


@dataclass(frozen=True)
class Price:
    close: Any
    retrieved_at: datetime | None = None
    visible_at: datetime | None = None


@dataclass(frozen=True)
class SelectedEvent:
    accession: str
    family: str
    items: tuple[str, ...]
    day0: date
    p_prev_date: date
    cutoff_o: datetime
    deadline_d: datetime
    entry_day: date
    direction: int
    strength: float
    quintile: int | None


@dataclass(frozen=True)
class Decision:
    """One accession, one outcome, with the cutoff and the reason. Times are the ones that were observed."""

    accession: Any
    status: str
    reason: str
    period: str
    acceptance: datetime | None = None
    retrieved_at: datetime | None = None
    visible_at: datetime | None = None
    deadline_d: datetime | None = None
    duplicates_dropped: int = 0
    ignored_revisions: tuple[str, ...] = ()
    label: str | None = None


@dataclass(frozen=True)
class SelectionResult:
    version: str
    period: str
    selected: tuple[SelectedEvent, ...]
    diagnostic: tuple[SelectedEvent, ...]
    decisions: tuple[Decision, ...]


def _aware(x: Any) -> bool:
    return isinstance(x, datetime) and x.tzinfo is not None and x.utcoffset() is not None


def _close_dt(cal: Calendar, day: date) -> datetime:
    return datetime.combine(day, cal.close_time(day), tzinfo=ET)


def day0_for(acceptance: datetime, cal: Calendar) -> date:
    """First regular session whose actual close is strictly after A + 5 minutes."""
    horizon = acceptance + ACCEPTANCE_MARGIN
    day = acceptance.astimezone(ET).date()
    for _ in range(15):  # a longer closure would be a calendar defect, not a market
        if cal.is_open(day) and _close_dt(cal, day) > horizon:
            return day
        day += timedelta(days=1)
    raise ValueError("no regular session within 15 days of the acceptance")


def prev_close_date(acceptance: datetime, cal: Calendar) -> date:
    """The session of the last regular close strictly before A."""
    day = acceptance.astimezone(ET).date()
    for _ in range(15):
        if cal.is_open(day) and _close_dt(cal, day) < acceptance:
            return day
        day -= timedelta(days=1)
    raise ValueError("no regular session within 15 days before the acceptance")


def _sessions_ending(day: date, n: int, cal: Calendar) -> list[date]:
    """The n regular sessions ending at ``day`` (inclusive), oldest first."""
    out: list[date] = []
    for _ in range(n * 3 + 30):
        if cal.is_open(day):
            out.append(day)
            if len(out) == n:
                return out[::-1]
        day -= timedelta(days=1)
    raise ValueError("calendar has too few sessions")


def next_session(day: date, cal: Calendar) -> date:
    day += timedelta(days=1)
    for _ in range(15):
        if cal.is_open(day):
            return day
        day += timedelta(days=1)
    raise ValueError("no regular session within 15 days after day 0")


def _decision(accession: Any, status: str, reason: str, period: str, **kw: Any) -> Decision:
    return Decision(accession, status, reason, period, **kw)


def _price_clock_problem(p: Price, deadline: datetime) -> tuple[str, str] | None:
    for name, t in (("retrieved_at", p.retrieved_at), ("visible_at", p.visible_at)):
        if not _aware(t):
            return EXCLUDED_UNKNOWN_AVAILABILITY, f"a price's {name} is unknown or has no time zone"
        if t > deadline:
            return EXCLUDED_LATE_INPUT, f"a price's {name} {t.isoformat()} is after the deadline {deadline.isoformat()}"
    return None


def _one(accession: Any, versions: list[Filing], tape: Mapping[date, Price], cal: Calendar, period: str,
         breakpoints: R.Breakpoints | None) -> tuple[Decision, SelectedEvent | None]:
    dropped = 0
    unique: dict[str, Filing] = {}
    for v in versions:
        have = unique.get(v.content_sha256)
        if have is None:
            unique[v.content_sha256] = v
        else:
            dropped += 1
            if _aware(v.visible_at) and (not _aware(have.visible_at) or v.visible_at < have.visible_at):
                unique[v.content_sha256] = v  # the earliest observation of the same content is its first availability
    ordered = sorted(unique.values(), key=lambda f: (not _aware(f.visible_at), f.visible_at if _aware(f.visible_at)
                                                     else datetime.max.replace(tzinfo=ET)))
    orig, others = ordered[0], ordered[1:]
    base = dict(acceptance=orig.acceptance, retrieved_at=orig.retrieved_at, visible_at=orig.visible_at,
                duplicates_dropped=dropped)
    fam = R.family_for_items(orig.items) if isinstance(orig.items, (tuple, list)) else None
    if fam is None:
        return _decision(accession, EXCLUDED_NO_FAMILY, "no Item 2.02, 1.01, 2.01 or 1.02", period, **base), None
    if not _aware(orig.acceptance):
        return _decision(accession, EXCLUDED_UNKNOWN_AVAILABILITY, "acceptance datetime is unknown or has no time "
                         "zone (clock 2)", period, **base), None
    a = orig.acceptance
    try:
        day0, p_date = day0_for(a, cal), prev_close_date(a, cal)
        entry = next_session(day0, cal)
    except ValueError as exc:
        return _decision(accession, EXCLUDED_MALFORMED, str(exc), period, **base), None
    o = _close_dt(cal, day0)
    d = datetime.combine(day0, DEADLINE_ET, tzinfo=ET)
    base["deadline_d"] = d
    ignored: list[str] = []
    label = None
    if period == FORWARD:
        for name, t in (("retrieved_at", orig.retrieved_at), ("visible_at", orig.visible_at)):
            if not _aware(t):
                return _decision(accession, EXCLUDED_UNKNOWN_AVAILABILITY, f"{name} is unknown or has no time zone "
                                 "(clocks 3 and 4)", period, **base), None
            if t > d:
                return _decision(accession, EXCLUDED_LATE_INPUT, f"{name} {t.isoformat()} is after the deadline "
                                 f"{d.isoformat()}", period, **base), None
        for other in others:
            if not _aware(other.visible_at):
                return _decision(accession, EXCLUDED_UNKNOWN_AVAILABILITY, "a second version of the filing has no "
                                 "visibility time, so it cannot be placed before or after the deadline", period,
                                 **base), None
            if other.visible_at <= d:
                return _decision(accession, EXCLUDED_REVISED_INPUT, "two different versions of the filing were "
                                 "visible by the deadline; first availability is ambiguous", period, **base), None
            ignored.append(other.content_sha256)
    else:
        label = PROXY_EOD_BY_DEADLINE
    # prices: only sessions up to day 0 are ever read
    try:
        window = _sessions_ending(p_date, VOL_CLOSES, cal)
    except ValueError as exc:
        return _decision(accession, EXCLUDED_MALFORMED, str(exc), period, **base), None
    c0_entry = tape.get(day0)
    if period == FORWARD:
        if c0_entry is None:
            return _decision(accession, EXCLUDED_LATE_INPUT, "C0 is not on the tape by the deadline (missing is "
                             "counted, never deferred)", period, ignored_revisions=tuple(ignored), **base), None
        for day in [*window, day0]:
            entry_ = tape.get(day)
            if entry_ is None:
                continue  # an absent close is caught below as a missing input, not as a clock
            problem = _price_clock_problem(entry_, d)
            if problem:
                return _decision(accession, problem[0], problem[1], period, ignored_revisions=tuple(ignored),
                                 **base), None
    closes = [tape[x].close if x in tape else None for x in window]
    c0 = c0_entry.close if c0_entry is not None else None
    s = R.event_strength(closes, c0)
    if s.value is None:
        return _decision(accession, EXCLUDED_INPUT, s.detail or "input", period, ignored_revisions=tuple(ignored),
                         label=label, **base), None
    ratio = c0 / closes[-1] - 1.0
    direction = (ratio > 0) - (ratio < 0)
    q = None
    if breakpoints is not None and breakpoints.status == R.OK and breakpoints.values is not None:
        q = R.quintile(s.value, breakpoints.values)
    ev = SelectedEvent(str(accession), fam, tuple(orig.items), day0, p_date, o, d, entry, direction, s.value, q)
    if period == FORWARD:
        return _decision(accession, SELECTED, "all clocks known and at or before the deadline", period,
                         ignored_revisions=tuple(ignored), **base), ev
    return _decision(accession, DIAGNOSTIC_ONLY, "historical closes are a proxy; availability by D is unknown", period,
                     label=label, **base), ev


def select_events(filings: Iterable[Filing], tape: Mapping[date, Price], *, period: str = FORWARD,
                  calendar: Calendar | None = None, breakpoints: R.Breakpoints | None = None) -> SelectionResult:
    """Decide every accession once. ``selected`` is empty for any refusal; historical never selects."""
    if period not in (FORWARD, HISTORICAL):
        raise ValueError(f"period must be {FORWARD!r} or {HISTORICAL!r}")
    cal = calendar or RegularCalendar()
    groups: dict[Any, list[Filing]] = {}
    decisions: list[Decision] = []
    for f in filings:
        if not isinstance(f, Filing) or not isinstance(f.accession, str) or not f.accession:
            decisions.append(_decision(getattr(f, "accession", None), EXCLUDED_MALFORMED,
                                       "not a Filing with a string accession", period))
            continue
        groups.setdefault(f.accession, []).append(f)
    selected: list[SelectedEvent] = []
    diagnostic: list[SelectedEvent] = []
    for acc in sorted(groups):
        dec, ev = _one(acc, groups[acc], tape, cal, period, breakpoints)
        decisions.append(dec)
        if ev is not None:
            (selected if dec.status == SELECTED else diagnostic).append(ev)
    return SelectionResult(SELECTION_VERSION, period, tuple(selected), tuple(diagnostic), tuple(decisions))
