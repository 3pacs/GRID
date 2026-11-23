"""The policy path the market prices at the next FOMC meeting, from the full Kalshi KXFED strike ladder.

``ingestion/altdata/kalshi_markets.py`` writes one ``raw_series`` row per open KXFED event per pull, with
``series_id = 'KALSHI:<event ticker>'`` (one event per meeting, e.g. ``KALSHI:KXFED-26DEC``). The row's ``value`` is
the yes-price of whichever strike had the most volume that day, so it answers a different question whenever that
strike changes (KXFED-26DEC moved from "above 4.00%" to "above 4.25%" around 2026-11-12). It is never read here.
Every strike's quote is in ``raw_payload['all_strikes']``; this module reads those.

Contract, checked on every strike, or the whole ladder is unusable (a guessed payout is never priced):

* the title asks whether "the upper bound of the federal funds rate" will be "above X%", X equals the ticker's
  ``-T<X>`` suffix and sits on the 0.25 grid, and a title that names the meeting ("following the Fed's Dec 9, 2026
  meeting") names the event's own meeting (``strike_date``, New York date);
* the ticker belongs to the row's event (``<event>-T<X>``), and so does the strike's own ``event_ticker`` when the
  writer recorded it; a recorded payout must be ``strike_type == 'greater'`` with ``floor_strike == X`` and no cap.

Quotes. Kalshi shows a missing bid as 0 and a missing ask as 1, and before ``kalshi-acquired/1`` GRID's writer also
stored a missing or malformed quote as 0.0, so a bid or ask counts only strictly inside (0, 1) (and with a positive
size when the size was recorded). Each strike is a band: [bid, ask], [0, ask] without a bid, [bid, 1] without an
ask; no quote on either side, a crossed quote, or a band wider than ``MAX_SPREAD`` makes the ladder unusable.
Last-trade prices are never used (they can be days old).

The ladder. Strikes must be contiguous in ``STRIKE_STEP`` steps. P(upper bound > k) must not rise with k, so a book
where a higher strike's bid is above a lower strike's ask fits no ladder and is refused. Otherwise each strike's
point is its band's mid, lowered by a running minimum where mids cross and held inside the bands every
non-increasing ladder must respect (the count of moved strikes is reported); the quotes' own bounds are published
beside the point. The expected upper bound after the meeting puts the mass below the lowest strike at that strike
and the mass above the highest at one step above it: E = lowest strike + step x sum of P(above k). Either tail that
can exceed ``MAX_TAIL`` within the quotes means the ladder does not cover the distribution. Hike / hold / cut odds
are given only when both thresholds (the current upper bound and one step below) are quoted strikes.

Clocks and availability (there is no modeled path; GRID's KXFED rows start 2026-04-07 and are all live pulls). No
row records when its commit became visible, so each availability below is a proxy that bounds the commit by the
writer's own discipline, not a persistence receipt (:data:`AVAILABILITY_NOTE`). The record therefore lists KXFED
among its unqualified read paths, and a historical comparison built on these rows is not exact point-in-time evidence:

* rows written under ``kalshi-acquired/1`` carry ``clock.requested_at`` and ``clock.acquired_at``, read before that
  event's markets request and right after its response (the quotes were read between the two), and their
  ``pull_timestamp`` is ``acquired_at``. The writer commits each row in its own transaction under lock and statement
  timeouts and rolls it back unless it is ready to commit within ``kalshi_markets.COMMIT_FENCE`` (30 s) of the
  response; the commit itself is not timestamped. Available ``COMMIT_GRACE`` (1 min) after the response;
* a row whose payload carries a clock that cannot be trusted (another rule, an unreadable or missing instant,
  ``requested_at`` after ``acquired_at``, or a ``pull_timestamp`` that is not ``acquired_at``) is unusable;
* older rows carry only ``pull_timestamp``, the raw_series default, which is the start of one transaction that
  spanned the whole pull, so their quotes arrived after it: acquired by, and available, ``LEGACY_SPAN`` later
  (pull_log 2026-05-27..11-23: 289 Kalshi runs, the longest 70 s).

A row counts for a cutoff only when it is available by it. The puller runs once a day, usually between 00:00 and
11:00 UTC, so the snapshot dated D (``obs_date`` is the UTC pull date) normally reflects the market after the close
before session D and before D opens; :func:`pre_open` checks that for any reading that is set against closes.

The next meeting for a day is the earliest meeting after that day's newest quotes among the KXFED events with a
stored row dated that day or earlier (a row is dated by the UTC day of its response, or for older rows the UTC day its
pull began). A day's reading therefore depends only on rows dated up to that day, cut at the record's availability
cutoff: a row dated later, even one whose request began earlier, never changes it. When that event has no usable
row that day, the day has no reading; a later meeting is never substituted. The 5-session change is taken within one
contract: the next meeting's expected upper bound minus the same event's expected upper bound five XNYS sessions
earlier.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

POLICY_PATH_RULE = "kalshi-fed-path/1"
KXFED_PREFIX = "KALSHI:KXFED-"
KALSHI_SOURCE = "Kalshi"          # source_catalog.name of ingestion/altdata/kalshi_markets.py
STRIKE_STEP = 0.25                # percentage points between adjacent strikes
MAX_SPREAD = 0.10                 # widest usable quote band on any strike (a missing side counts as 0 or 1)
MAX_TAIL = 0.05                   # largest probability mass the quotes allow outside the ladder on either side
MIN_STRIKES = 4
CHANGE_SESSIONS = 5

# The writer's clock rule (ingestion/altdata/kalshi_markets.py CLOCK_RULE; a test keeps the two equal).
ACQUIRED_CLOCK_RULE = "kalshi-acquired/1"
# Acquired-clock rows: the writer rolls a row back unless it is ready to commit within kalshi_markets.COMMIT_FENCE
# (30 s) of the response; the grace leaves the commit itself 30 s more (a test keeps the fence inside the grace).
COMMIT_GRACE = timedelta(minutes=1)
# Transaction-start rows: the whole pull ran in one transaction. pull_log 2026-05-27..2026-11-23 has 289 Kalshi runs
# (Kalshi_Markets, smart:kalshi, smart:kalshi_markets); the longest took 70 s and inserting runs take about 10 s.
LEGACY_SPAN = timedelta(minutes=15)

_CLOCK_TOLERANCE = timedelta(milliseconds=1)   # pull_timestamp must be the acquisition clock (storage rounding only)
AVAILABILITY_NOTE = ("proxy, not a persistence receipt: no KXFED row records when its commit became visible (acquired "
                     "rows: response + 1 min, the writer rolls back any row not ready to commit within 30 s of the "
                     "response; older rows: transaction start + 15 min), so these readings are not exact "
                     "point-in-time evidence, for a decision or for a historical comparison")
_ET = ZoneInfo("America/New_York")
_TICKER_STRIKE = re.compile(r"-T(\d+(?:\.\d+)?)$")
_TITLE_STRIKE = re.compile(r"upper bound of the federal funds rate be above (\d+(?:\.\d+)?)%", re.IGNORECASE)
_TITLE_MEETING = re.compile(r"following the Fed's ([A-Z][a-z]+)\.? (\d{1,2}), (\d{4}) meeting")
_MONTHS = {m: i for i, m in enumerate(("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov",
                                       "dec"), 1)}


@dataclass(frozen=True)
class Ladder:
    """One event's strike ladder from one pull, priced."""

    expected_upper: float                       # % (expected upper bound of the target range after the meeting)
    expected_bounds: tuple[float, float]        # the lowest and highest expected upper bound the quotes allow
    strikes: tuple[float, ...]                  # ascending
    p_above: tuple[float, ...]                  # P(upper bound > strike), non-increasing, inside the bounds below
    lower: tuple[float, ...]                    # per strike, the least P(above) any ladder fitting the quotes has
    upper: tuple[float, ...]                    # per strike, the most
    tail_below: float                           # P(upper bound <= lowest strike)
    tail_above: float                           # P(upper bound > highest strike)
    monotone_adjusted: int                      # strikes whose point is not their own band's mid
    one_sided: int                              # strikes quoted on one side only
    max_spread: float                           # widest quote band


@dataclass(frozen=True)
class Snapshot:
    event: str
    obs_date: date
    pulled_at: datetime                         # the row's stored pull_timestamp
    meeting_at: datetime | None
    ladder: Ladder | None
    clock_basis: str                            # 'acquired' | 'transaction_start' (older rows) | 'clock_unreadable'
    acquired_from: datetime                     # earliest instant the quotes can have been read
    acquired_by: datetime                       # latest
    available_at: datetime                      # when the row is known: compared with a record's cutoff
    reason: str | None = None                   # why the ladder is unusable, when it is


def _num(raw: Any) -> float | None:
    if isinstance(raw, bool) or raw is None:
        return None
    try:
        x = float(raw)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _instant(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        ts = raw
    else:
        try:
            ts = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            return None
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def on_grid(x: float) -> bool:
    return math.isclose(round(x / STRIKE_STEP) * STRIKE_STEP, x, abs_tol=1e-9)


def _side(price: Any, size: Any) -> float | None:
    """A quote side's price, or None when that side is absent (0, 1, missing, malformed, or a recorded size of 0)."""
    p = _num(price)
    if p is None or not 0.0 < p < 1.0:
        return None
    s = _num(size)
    if s is not None and s <= 0:
        return None
    return p


def _contract_problem(s: dict[str, Any], k: float, event_code: str | None, meeting_at: datetime | None) -> str | None:
    """Why a strike is not this event's "upper bound above k" contract, or None when it is."""
    tk = str(s.get("ticker") or "")
    if not on_grid(k):
        return f"strike {tk!r} is off the {STRIKE_STEP} grid"
    if event_code is not None:
        if not tk.startswith(event_code + "-T"):
            return f"strike {tk!r} is not a contract of {event_code}"
        own = s.get("event_ticker")
        if own is not None and own != event_code:
            return f"strike {tk!r} names event {own!r}, not {event_code}"
    kind, floor, cap = s.get("strike_type"), s.get("floor_strike"), s.get("cap_strike")
    if kind is not None and kind != "greater":
        return f"strike {tk!r} pays on {kind!r}, not 'greater'"
    if floor is not None and (_num(floor) is None or not math.isclose(_num(floor), k, abs_tol=1e-9)):
        return f"strike {tk!r} has floor_strike {floor!r}, not {k}"
    if cap is not None:
        return f"strike {tk!r} has a cap strike {cap!r}"
    named = _TITLE_MEETING.search(str(s.get("title") or ""))
    if named is not None and meeting_at is not None:
        month = _MONTHS.get(named.group(1)[:3].lower())
        try:
            day = date(int(named.group(3)), month, int(named.group(2))) if month else None
        except ValueError:
            day = None
        if day is None:
            return f"strike {tk!r}: meeting date in the title is unreadable"
        if day != meeting_at.astimezone(_ET).date():
            return f"strike {tk!r} names the {day.isoformat()} meeting, not the event's {meeting_at.isoformat()}"
    return None


def price_ladder(strikes: Sequence[dict[str, Any]], *, event: str | None = None,
                 meeting_at: datetime | None = None) -> tuple[Ladder | None, str | None]:
    """Price one event's ``all_strikes`` list. Returns (ladder, None) or (None, why it is unusable).

    ``event`` (``KALSHI:<event>`` or the bare event ticker) and ``meeting_at`` bind every strike to that event's
    contract; without them only the title and ticker are checked.
    """
    event_code = event[len("KALSHI:"):] if event and event.startswith("KALSHI:") else event
    rows: list[tuple[float, float, float, bool]] = []
    for s in strikes or ():
        if not isinstance(s, dict):
            return None, "strike entry is not an object"
        tk = str(s.get("ticker") or "")
        m = _TICKER_STRIKE.search(tk)
        t = _TITLE_STRIKE.search(str(s.get("title") or ""))
        if m is None or t is None:
            return None, f"strike {tk!r} is not an 'upper bound above X%' contract"
        k = float(m.group(1))
        if not math.isclose(k, float(t.group(1)), abs_tol=1e-9):
            return None, f"strike {tk!r}: ticker and title name different strikes"
        problem = _contract_problem(s, k, event_code, meeting_at)
        if problem:
            return None, problem
        bid = _side(s.get("yes_bid"), s.get("yes_bid_size"))
        ask = _side(s.get("yes_ask"), s.get("yes_ask_size"))
        if bid is None and ask is None:
            return None, f"strike {tk!r} has no quote on either side"
        lo = bid if bid is not None else 0.0
        hi = ask if ask is not None else 1.0
        if lo > hi + 1e-12:
            return None, f"strike {tk!r} quote is crossed (bid {lo} above ask {hi})"
        if hi - lo > MAX_SPREAD + 1e-12:
            return None, f"strike {tk!r} quote band {hi - lo:.2f} is wider than {MAX_SPREAD}"
        rows.append((k, lo, hi, bid is None or ask is None))
    if len(rows) < MIN_STRIKES:
        return None, f"{len(rows)} strikes; need {MIN_STRIKES}"
    rows.sort()
    ks = [r[0] for r in rows]
    if len(set(ks)) != len(ks):
        return None, "duplicate strike"
    if any(not math.isclose(b - a, STRIKE_STEP, abs_tol=1e-9) for a, b in zip(ks, ks[1:])):
        return None, f"strikes are not contiguous in {STRIKE_STEP} steps"
    lo = [r[1] for r in rows]
    hi = [r[2] for r in rows]
    n = len(rows)
    for b in range(n):                          # P(above) must not rise: a higher strike's bid over a lower ask
        for a in range(b):
            if lo[b] > hi[a] + 1e-12:
                return None, (f"no non-increasing ladder fits the quotes: the {ks[b]:.2f} strike's bid {lo[b]} is "
                              f"above the {ks[a]:.2f} strike's ask {hi[a]}")
    upper = list(hi)
    for i in range(1, n):
        upper[i] = min(upper[i - 1], hi[i])
    lower = list(lo)
    for i in range(n - 2, -1, -1):
        lower[i] = max(lower[i + 1], lo[i])
    if 1.0 - lower[0] > MAX_TAIL + 1e-12 or upper[-1] > MAX_TAIL + 1e-12:
        return None, (f"ladder does not cover the distribution (up to {1.0 - lower[0]:.3f} below, "
                      f"{upper[-1]:.3f} above)")
    ps, adjusted, running = [], 0, 1.0
    for i in range(n):
        mid = (lo[i] + hi[i]) / 2.0
        running = min(running, mid)
        p = min(max(running, lower[i]), upper[i])
        if abs(p - mid) > 1e-12:
            adjusted += 1
        ps.append(p)
    expected = ks[0] + STRIKE_STEP * sum(ps)
    bounds = (ks[0] + STRIKE_STEP * sum(lower), ks[0] + STRIKE_STEP * sum(upper))
    return Ladder(expected_upper=expected, expected_bounds=bounds, strikes=tuple(ks), p_above=tuple(ps),
                  lower=tuple(lower), upper=tuple(upper), tail_below=1.0 - ps[0], tail_above=ps[-1],
                  monotone_adjusted=adjusted, one_sided=sum(1 for r in rows if r[3]),
                  max_spread=max(b - a for a, b in zip(lo, hi))), None


def row_clock(pulled_at: datetime, payload: Any) -> tuple[str, datetime, datetime, datetime, str | None]:
    """(basis, acquired_from, acquired_by, available_at, problem) for a row's stored pull clock and payload.

    ``problem`` is set when the payload carries a clock that cannot be trusted; the row is then unusable and keeps
    the conservative legacy bounds (basis ``clock_unreadable``).
    """
    legacy = (pulled_at, pulled_at + LEGACY_SPAN, pulled_at + LEGACY_SPAN)
    clock = payload.get("clock") if isinstance(payload, dict) else None
    if clock is None:
        return ("transaction_start", *legacy, None)
    if not isinstance(clock, dict) or clock.get("rule") != ACQUIRED_CLOCK_RULE:
        rule = clock.get("rule") if isinstance(clock, dict) else clock
        return ("clock_unreadable", *legacy, f"unrecognised clock {rule!r}")
    acquired, requested = _instant(clock.get("acquired_at")), _instant(clock.get("requested_at"))
    if acquired is None:
        problem = "clock.acquired_at is missing or unreadable"
    elif requested is None:
        problem = "clock.requested_at is missing or unreadable"
    elif requested > acquired:
        problem = "clock.requested_at is after clock.acquired_at"
    elif abs(pulled_at - acquired) > _CLOCK_TOLERANCE:
        problem = "pull_timestamp is not clock.acquired_at"
    else:
        return "acquired", requested, acquired, acquired + COMMIT_GRACE, None
    return ("clock_unreadable", *legacy, problem)


def snapshot_from_row(series_id: str, obs_date: date, pulled_at: Any, payload: Any) -> Snapshot:
    pulled = _instant(pulled_at)
    if pulled is None:
        raise ValueError(f"{series_id} {obs_date}: row has no pull clock")
    if isinstance(payload, (str, bytes)):
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = None
    basis, a_from, a_by, available, clock_problem = row_clock(pulled, payload)
    if not isinstance(payload, dict):
        return Snapshot(series_id, obs_date, pulled, None, None, basis, a_from, a_by, available,
                        "payload is not a JSON object")
    meeting = _instant(payload.get("strike_date"))
    code = series_id[len("KALSHI:"):] if series_id.startswith("KALSHI:") else series_id
    if payload.get("event_ticker") is not None and payload.get("event_ticker") != code:
        return Snapshot(series_id, obs_date, pulled, meeting, None, basis, a_from, a_by, available,
                        f"payload event {payload.get('event_ticker')!r} is not the row's {code}")
    if meeting is None:
        return Snapshot(series_id, obs_date, pulled, None, None, basis, a_from, a_by, available,
                        "payload has no meeting time (strike_date)")
    if clock_problem is not None:
        return Snapshot(series_id, obs_date, pulled, meeting, None, basis, a_from, a_by, available,
                        f"clock: {clock_problem}")
    ladder, why = price_ladder(payload.get("all_strikes") or [], event=series_id, meeting_at=meeting)
    return Snapshot(series_id, obs_date, pulled, meeting, ladder, basis, a_from, a_by, available, why)


def known_by(s: Snapshot, cutoff: datetime, *, inclusive: bool) -> bool:
    """Whether a row is available by a record's cutoff (at or before it when ``inclusive``, else strictly before)."""
    return s.available_at <= cutoff if inclusive else s.available_at < cutoff


def latest_per_event_day(snapshots: Iterable[Snapshot]) -> dict[tuple[str, date], Snapshot]:
    """The latest pull of each (event, obs_date); the caller has already cut the rows at its availability cutoff."""
    out: dict[tuple[str, date], Snapshot] = {}
    for s in snapshots:
        key = (s.event, s.obs_date)
        if key not in out or s.acquired_by > out[key].acquired_by:
            out[key] = s
    return out


@dataclass(frozen=True)
class DayReading:
    obs_date: date
    event: str
    meeting_at: datetime
    pulled_at: datetime
    acquired_from: datetime
    acquired_by: datetime
    available_at: datetime
    clock_basis: str
    ladder: Ladder


def _reading(d: date, s: Snapshot) -> DayReading:
    """Only called for a snapshot with a meeting time and a priced ladder."""
    return DayReading(d, s.event, s.meeting_at, s.pulled_at, s.acquired_from, s.acquired_by, s.available_at,
                      s.clock_basis, s.ladder)


def first_seen(snapshots: Iterable[Snapshot]) -> dict[tuple[datetime, str], date]:
    """(meeting time, event) -> the date of the event's first stored row: the schedule as GRID had stored it.

    A date, not an instant: a row is dated by its response's UTC day (older rows: the UTC day the pull began), so
    knowledge taken this way can only come from rows dated up to the day being read.
    """
    out: dict[tuple[datetime, str], date] = {}
    for s in snapshots:
        if s.meeting_at is not None:
            key = (s.meeting_at, s.event)
            if key not in out or s.obs_date < out[key]:
                out[key] = s.obs_date
    return out


def next_meeting_readings(snapshots: Iterable[Snapshot], sessions: Sequence[date]) -> tuple[dict[date, DayReading],
                                                                                            dict[date, str]]:
    """Per XNYS session date with KXFED rows: the next meeting's priced ladder, or why that day has none.

    The next meeting is chosen from the events with a stored row dated that day or earlier, so a row dated later
    (an event first stored on a later day, even one whose request began the day before) never changes an earlier
    day.
    """
    snaps = list(snapshots)
    by_key = latest_per_event_day(snaps)
    seen = first_seen(snaps)
    session_set = set(sessions)
    by_day: dict[date, list[Snapshot]] = {}
    for (_, d), s in by_key.items():
        if d in session_set:
            by_day.setdefault(d, []).append(s)
    readings: dict[date, DayReading] = {}
    missing: dict[date, str] = {}
    for d in sorted(by_day):
        # The day's newest quotes decide which meeting is next: a pull after a decision reads the following meeting.
        newest = max(s.acquired_by for s in by_day[d])
        known: dict[datetime, list[str]] = {}
        for (m, ev), first in seen.items():
            if first <= d:
                known.setdefault(m, []).append(ev)
        nxt = min((m for m in known if m > newest), default=None)
        if nxt is None:
            missing[d] = "no KXFED meeting known after the pull"
            continue
        if len(known[nxt]) > 1:
            missing[d] = f"{len(known[nxt])} KXFED events name the meeting at {nxt.isoformat()}"
            continue
        snap = by_key.get((known[nxt][0], d))
        if snap is None:
            missing[d] = f"no {known[nxt][0]} row for the next meeting"
        elif snap.acquired_by >= nxt:
            missing[d] = f"{snap.event} may have been read after its meeting"
        elif snap.ladder is None:
            missing[d] = f"{snap.event}: {snap.reason}"
        else:
            readings[d] = _reading(d, snap)
    return readings, missing


@dataclass(frozen=True, eq=False)
class PolicyPath:
    level: pd.Series                            # expected upper bound after the next meeting, per session
    change: pd.Series                           # its same-contract change over CHANGE_SESSIONS sessions
    readings: dict[date, DayReading]            # the next-meeting reading per session
    change_start: dict[date, DayReading]        # per change, the same contract's reading it is taken from
    missing: dict[date, str]                    # why a session with KXFED rows has no reading


def path_series(snapshots: Iterable[Snapshot], sessions: Sequence[date]) -> PolicyPath:
    snaps = list(snapshots)
    readings, missing = next_meeting_readings(snaps, sessions)
    by_key = latest_per_event_day(snaps)
    level = pd.Series({d: r.ladder.expected_upper for d, r in readings.items()}, dtype=float).sort_index()
    pos = {d: i for i, d in enumerate(sessions)}
    change: dict[date, float] = {}
    start: dict[date, DayReading] = {}
    for d, r in readings.items():
        i = pos.get(d)
        if i is None or i < CHANGE_SESSIONS:
            continue
        d0 = sessions[i - CHANGE_SESSIONS]
        earlier = by_key.get((r.event, d0))
        if earlier is None or earlier.ladder is None or earlier.acquired_by >= r.meeting_at:
            continue
        change[d] = r.ladder.expected_upper - earlier.ladder.expected_upper
        start[d] = _reading(d0, earlier)
    return PolicyPath(level, pd.Series(change, dtype=float).sort_index(), readings, start, missing)


def session_open(d: date) -> datetime:
    return datetime.combine(d, time(9, 30), tzinfo=_ET).astimezone(timezone.utc)


def regular_close(d: date) -> datetime:
    return datetime.combine(d, time(16, 0), tzinfo=_ET).astimezone(timezone.utc)


def pre_open(reading: DayReading, previous_session: date) -> bool:
    """Whether every instant the reading's quotes can have been read lies after ``previous_session``'s regular close
    and before its own session opens, so it describes the market between those two sessions."""
    return regular_close(previous_session) <= reading.acquired_from and \
        reading.acquired_by < session_open(reading.obs_date)


def selection_receipt(snapshots: Sequence[Snapshot], *, as_of: date, cutoff: datetime,
                      inclusive: bool) -> dict[str, Any]:
    """Fingerprint of exactly the KXFED rows a record read (event, obs_date, clocks, priced value)."""
    rows = sorted(({"event": s.event, "obs_date": s.obs_date.isoformat(), "pulled_at": s.pulled_at.isoformat(),
                    "clock_basis": s.clock_basis, "available_at": s.available_at.isoformat(),
                    "expected_upper": round(s.ladder.expected_upper, 6) if s.ladder else None,
                    "unusable": s.reason if s.ladder is None else None} for s in snapshots),
                  key=lambda x: (x["event"], x["obs_date"], x["pulled_at"]))
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    bases = sorted({r["clock_basis"] for r in rows})
    return {"schema": "market-state-kxfed-selection/3", "rule": POLICY_PATH_RULE, "as_of": as_of.isoformat(),
            "cutoff": cutoff.isoformat(), "cutoff_inclusive": inclusive, "rows": len(rows),
            "unusable_rows": sum(1 for r in rows if r["unusable"]),
            "clock_basis": {b: sum(1 for r in rows if r["clock_basis"] == b) for b in bases},
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "pit_qualified": False, "availability": AVAILABILITY_NOTE,
            "pit_basis": (f"available_at only, no modeled path: {ACQUIRED_CLOCK_RULE} acquisition clock + "
                          f"{int(COMMIT_GRACE.total_seconds())} s commit grace, or pull_timestamp (transaction "
                          f"start) + {int(LEGACY_SPAN.total_seconds() // 60)} min for older rows; the schedule "
                          f"from rows dated up to each day")}


def outcome_odds(ladder: Ladder, current_upper: float) -> tuple[dict[str, float] | None, dict[str, list[float]] | None,
                                                               str | None]:
    """(P(hike), P(hold), P(cut) at the meeting, their bounds within the quotes, None), or (None, None, why).

    Strikes are the boundaries "above k"; the current upper bound u is held when the upper bound after the meeting is
    above u - step and not above u. Both u and u - step must be quoted strikes: odds are never extrapolated past the
    ladder.
    """
    if not on_grid(current_upper):
        return None, None, f"current upper bound {current_upper} is off the {STRIKE_STEP} grid"
    index = {round(k / STRIKE_STEP): i for i, k in enumerate(ladder.strikes)}
    i_hike = index.get(round(current_upper / STRIKE_STEP))
    i_cut = index.get(round((current_upper - STRIKE_STEP) / STRIKE_STEP))
    if i_hike is None or i_cut is None:
        return None, None, (f"the ladder does not quote both thresholds ({current_upper - STRIKE_STEP:.2f}% and "
                            f"{current_upper:.2f}%)")
    p, lo, hi = ladder.p_above, ladder.lower, ladder.upper
    odds = {"hike": round(p[i_hike], 4), "hold": round(p[i_cut] - p[i_hike], 4), "cut": round(1.0 - p[i_cut], 4)}
    bounds = {"hike": [round(lo[i_hike], 4), round(hi[i_hike], 4)],
              "hold": [round(max(0.0, lo[i_cut] - hi[i_hike]), 4), round(hi[i_cut] - lo[i_hike], 4)],
              "cut": [round(1.0 - hi[i_cut], 4), round(1.0 - lo[i_cut], 4)]}
    return odds, bounds, None


def meetings_seen(snapshots: Iterable[Snapshot], *, on: date | None = None) -> list[datetime]:
    """Meeting times of the KXFED events with a stored row (dated ``on`` or earlier, when given)."""
    return _meetings_known(first_seen(snapshots), on)


def _meetings_known(seen: dict[tuple[datetime, str], date], on: date | None) -> list[datetime]:
    return sorted({m for (m, _), first in seen.items() if on is None or first <= on})


def target_pairing(reading: DayReading, target: pd.Series, meetings: Sequence[datetime]) -> tuple[date, float] | None:
    """The current upper bound to set against a day's reading: DFEDTARU's newest observation on or before the day.

    Refused (None) when that observation predates the last meeting's decision taking effect (the day after the
    meeting): right after a decision the snapshot already prices the following meeting while FRED's next print still
    shows the old target, and the pair would count the decision just made as a priced move. Also refused when the
    target is not on the strike grid (the odds need it there).
    """
    if target is None or target.empty:
        return None
    known = target[[d <= reading.obs_date for d in target.index]]
    if known.empty:
        return None
    t_obs, t_val = known.index[-1], float(known.iloc[-1])
    last = max((m for m in meetings if m < reading.acquired_from), default=None)
    if last is not None and t_obs < last.date() + timedelta(days=1):
        return None
    if not on_grid(t_val):
        return None
    return t_obs, t_val


def priced_move_series(readings: dict[date, DayReading], target: pd.Series,
                       snapshots: Sequence[Snapshot]) -> pd.Series:
    """Expected upper bound after the next meeting minus the current upper bound, per day with a valid pairing.

    Each day's last meeting comes from the schedule GRID had stored by that day.
    """
    out = {}
    seen = first_seen(snapshots)
    for d, r in readings.items():
        pair = target_pairing(r, target, _meetings_known(seen, r.obs_date))
        if pair is not None:
            out[d] = r.ladder.expected_upper - pair[1]
    return pd.Series(out, dtype=float).sort_index()
