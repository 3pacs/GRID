"""The policy path the market prices at the next FOMC meeting, from the full Kalshi KXFED strike ladder.

``ingestion/altdata/kalshi_markets.py`` writes one ``raw_series`` row per open KXFED event per pull, with
``series_id = 'KALSHI:<event ticker>'`` (one event per meeting, e.g. ``KALSHI:KXFED-26DEC``). The row's ``value`` is
the yes-price of whichever strike had the most volume that day, so it answers a different question whenever that
strike changes (KXFED-26DEC moved from "above 4.00%" to "above 4.25%" around 2026-11-12). It is never read here.
Every strike's quote is in ``raw_payload['all_strikes']``; this module reads those.

Contract, checked on every strike: the market title asks whether "the upper bound of the federal funds rate" will be
"above X%" after the meeting, and X equals the ticker's ``-T<X>`` suffix. A strike that does not say exactly that
makes the whole ladder unusable; a guessed payout is never priced.

Per snapshot (one event's row from one pull):

* each strike's probability is the mid of its bid and ask, used only when ``0 <= bid <= ask <= 1`` and
  ``ask - bid <= MAX_SPREAD``. Last-trade prices are never used (they can be days old).
* strikes must be contiguous in ``STRIKE_STEP`` steps; probabilities are made non-increasing in the strike by a
  running minimum (a crossed quote is noise, and the count of adjustments is reported).
* the expected upper bound after the meeting puts the mass below the lowest strike at that strike and the mass above
  the highest at one step above it. Either tail above ``MAX_TAIL`` means the ladder does not cover the distribution,
  and the snapshot is unusable.

Availability: a snapshot is known from its ``pull_timestamp``. There is no modeled path: GRID's KXFED rows start
2026-04-07 and are all live pulls. The puller runs once a day, usually between 00:00 and 06:00 UTC, so the snapshot
dated D (``obs_date`` is the UTC pull date) reflects the market overnight before session D opens.

The next meeting for a day is the event with the earliest meeting time (``strike_date``) after that day's newest
pull, among every KXFED event seen by the cutoff. When that event has no usable row that day, the day has no reading;
a later meeting is never substituted. The 5-session change is taken within one contract: the next meeting's expected
upper bound minus the same event's expected upper bound five XNYS sessions earlier.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

import pandas as pd

POLICY_PATH_RULE = "kalshi-fed-path/1"
KXFED_PREFIX = "KALSHI:KXFED-"
KALSHI_SOURCE = "Kalshi"          # source_catalog.name of ingestion/altdata/kalshi_markets.py
STRIKE_STEP = 0.25                # percentage points between adjacent strikes
MAX_SPREAD = 0.10                 # widest usable bid/ask on any strike
MAX_TAIL = 0.05                   # largest probability mass allowed outside the ladder on either side
MIN_STRIKES = 4
CHANGE_SESSIONS = 5

_TICKER_STRIKE = re.compile(r"-T(\d+(?:\.\d+)?)$")
_TITLE_STRIKE = re.compile(r"upper bound of the federal funds rate be above (\d+(?:\.\d+)?)%", re.IGNORECASE)


@dataclass(frozen=True)
class Ladder:
    """One event's strike ladder from one pull, priced."""

    expected_upper: float                       # % (expected upper bound of the target range after the meeting)
    strikes: tuple[float, ...]                  # ascending
    p_above: tuple[float, ...]                  # P(upper bound > strike), non-increasing
    tail_below: float                           # P(upper bound <= lowest strike)
    tail_above: float                           # P(upper bound > highest strike)
    monotone_adjusted: int                      # strikes whose mid was lowered to keep the ladder non-increasing
    max_spread: float


@dataclass(frozen=True)
class Snapshot:
    event: str
    obs_date: date
    pulled_at: datetime
    meeting_at: datetime | None
    ladder: Ladder | None
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


def price_ladder(strikes: Sequence[dict[str, Any]]) -> tuple[Ladder | None, str | None]:
    """Price one event's ``all_strikes`` list. Returns (ladder, None) or (None, why it is unusable)."""
    rows: list[tuple[float, float, float]] = []
    for s in strikes or ():
        if not isinstance(s, dict):
            return None, "strike entry is not an object"
        m = _TICKER_STRIKE.search(str(s.get("ticker") or ""))
        t = _TITLE_STRIKE.search(str(s.get("title") or ""))
        if m is None or t is None:
            return None, f"strike {s.get('ticker')!r} is not an 'upper bound above X%' contract"
        k = float(m.group(1))
        if not math.isclose(k, float(t.group(1)), abs_tol=1e-9):
            return None, f"strike {s.get('ticker')!r}: ticker and title name different strikes"
        bid, ask = _num(s.get("yes_bid")), _num(s.get("yes_ask"))
        if bid is None or ask is None or not 0.0 <= bid <= ask <= 1.0:
            return None, f"strike {s.get('ticker')!r} has no two-sided quote"
        if ask - bid > MAX_SPREAD + 1e-12:
            return None, f"strike {s.get('ticker')!r} spread {ask - bid:.2f} is wider than {MAX_SPREAD}"
        rows.append((k, (bid + ask) / 2.0, ask - bid))
    if len(rows) < MIN_STRIKES:
        return None, f"{len(rows)} strikes; need {MIN_STRIKES}"
    rows.sort()
    ks = [r[0] for r in rows]
    if len(set(ks)) != len(ks):
        return None, "duplicate strike"
    if any(not math.isclose(b - a, STRIKE_STEP, abs_tol=1e-9) for a, b in zip(ks, ks[1:])):
        return None, f"strikes are not contiguous in {STRIKE_STEP} steps"
    ps, adjusted, running = [], 0, 1.0
    for _, mid, _ in rows:
        if mid > running + 1e-12:
            adjusted += 1
        running = min(running, mid)
        ps.append(running)
    tail_below, tail_above = 1.0 - ps[0], ps[-1]
    if tail_below > MAX_TAIL or tail_above > MAX_TAIL:
        return None, f"ladder does not cover the distribution (tails {tail_below:.3f} below, {tail_above:.3f} above)"
    expected = ks[0] * tail_below + (ks[-1] + STRIKE_STEP) * tail_above
    expected += sum(ks[i + 1] * (ps[i] - ps[i + 1]) for i in range(len(ks) - 1))
    return Ladder(expected_upper=expected, strikes=tuple(ks), p_above=tuple(ps), tail_below=tail_below,
                  tail_above=tail_above, monotone_adjusted=adjusted, max_spread=max(r[2] for r in rows)), None


def snapshot_from_row(series_id: str, obs_date: date, pulled_at: Any, payload: Any) -> Snapshot:
    pulled = _instant(pulled_at)
    if pulled is None:
        raise ValueError(f"{series_id} {obs_date}: row has no pull clock")
    if isinstance(payload, (str, bytes)):
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = None
    if not isinstance(payload, dict):
        return Snapshot(series_id, obs_date, pulled, None, None, "payload is not a JSON object")
    meeting = _instant(payload.get("strike_date"))
    ladder, why = price_ladder(payload.get("all_strikes") or [])
    if meeting is None:
        why = why or "payload has no meeting time (strike_date)"
    return Snapshot(series_id, obs_date, pulled, meeting, ladder if meeting is not None else None, why)


def latest_per_event_day(snapshots: Iterable[Snapshot]) -> dict[tuple[str, date], Snapshot]:
    """The latest pull of each (event, obs_date); the caller has already cut the rows at its availability cutoff."""
    out: dict[tuple[str, date], Snapshot] = {}
    for s in snapshots:
        key = (s.event, s.obs_date)
        if key not in out or s.pulled_at > out[key].pulled_at:
            out[key] = s
    return out


@dataclass(frozen=True)
class DayReading:
    obs_date: date
    event: str
    meeting_at: datetime
    pulled_at: datetime
    ladder: Ladder


def next_meeting_readings(snapshots: Iterable[Snapshot], sessions: Sequence[date]) -> tuple[dict[date, DayReading],
                                                                                            dict[date, str]]:
    """Per XNYS session date with KXFED rows: the next meeting's priced ladder, or why that day has none."""
    by_key = latest_per_event_day(snapshots)
    meetings = sorted({s.meeting_at for s in by_key.values() if s.meeting_at is not None})
    event_of: dict[datetime, str] = {}
    for s in by_key.values():
        if s.meeting_at is not None:
            event_of.setdefault(s.meeting_at, s.event)
    session_set = set(sessions)
    days = sorted({d for (_, d) in by_key if d in session_set})
    readings: dict[date, DayReading] = {}
    missing: dict[date, str] = {}
    for d in days:
        # The day's newest pull decides which meeting is next: a pull after a decision reads the following meeting.
        newest = max(s.pulled_at for (_, dd), s in by_key.items() if dd == d)
        nxt = next((m for m in meetings if m > newest), None)
        if nxt is None:
            missing[d] = "no KXFED meeting after the pull"
            continue
        snap = by_key.get((event_of[nxt], d))
        if snap is None:
            missing[d] = f"no {event_of[nxt]} row for the next meeting"
        elif snap.meeting_at is None or snap.pulled_at >= snap.meeting_at:
            missing[d] = f"{snap.event} was pulled after its meeting"
        elif snap.ladder is None:
            missing[d] = f"{snap.event}: {snap.reason}"
        else:
            readings[d] = DayReading(d, snap.event, snap.meeting_at, snap.pulled_at, snap.ladder)
    return readings, missing


def path_series(snapshots: Iterable[Snapshot], sessions: Sequence[date]) -> tuple[pd.Series, pd.Series,
                                                                                  dict[date, DayReading]]:
    """(expected upper bound per session, its same-contract 5-session change, the day readings)."""
    snaps = list(snapshots)
    readings, _ = next_meeting_readings(snaps, sessions)
    by_key = latest_per_event_day(snaps)
    level = pd.Series({d: r.ladder.expected_upper for d, r in readings.items()}, dtype=float).sort_index()
    pos = {d: i for i, d in enumerate(sessions)}
    change: dict[date, float] = {}
    for d, r in readings.items():
        i = pos.get(d)
        if i is None or i < CHANGE_SESSIONS:
            continue
        earlier = by_key.get((r.event, sessions[i - CHANGE_SESSIONS]))
        if earlier is None or earlier.ladder is None or earlier.pulled_at >= r.meeting_at:
            continue
        change[d] = r.ladder.expected_upper - earlier.ladder.expected_upper
    return level, pd.Series(change, dtype=float).sort_index(), readings


def selection_receipt(snapshots: Sequence[Snapshot], *, as_of: date, cutoff: datetime,
                      inclusive: bool) -> dict[str, Any]:
    """Fingerprint of exactly the KXFED rows a record read (event, obs_date, pull clock, priced value)."""
    rows = sorted(({"event": s.event, "obs_date": s.obs_date.isoformat(), "pulled_at": s.pulled_at.isoformat(),
                    "expected_upper": round(s.ladder.expected_upper, 6) if s.ladder else None,
                    "unusable": s.reason if s.ladder is None else None} for s in snapshots),
                  key=lambda x: (x["event"], x["obs_date"], x["pulled_at"]))
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    return {"schema": "market-state-kxfed-selection/1", "rule": POLICY_PATH_RULE, "as_of": as_of.isoformat(),
            "cutoff": cutoff.isoformat(), "cutoff_inclusive": inclusive, "rows": len(rows),
            "unusable_rows": sum(1 for r in rows if r["unusable"]),
            "sha256": hashlib.sha256(encoded).hexdigest(), "pit_basis": "pull_timestamp only (no modeled path)"}


def outcome_odds(ladder: Ladder, current_upper: float) -> dict[str, float]:
    """P(hike), P(hold), P(cut) at the meeting against the current upper bound, when it sits on the ladder grid.

    Strikes are the boundaries "above k"; the current upper bound u is held when the upper bound after the meeting is
    above u - step and not above u.
    """
    def above(k: float) -> float:
        if k < ladder.strikes[0] - 1e-9:
            return 1.0
        if k > ladder.strikes[-1] + 1e-9:
            return 0.0
        i = round((k - ladder.strikes[0]) / STRIKE_STEP)
        return ladder.p_above[i]

    hike = above(current_upper)
    no_cut = above(current_upper - STRIKE_STEP)
    return {"hike": round(hike, 4), "hold": round(no_cut - hike, 4), "cut": round(1.0 - no_cut, 4)}


def on_grid(x: float) -> bool:
    return math.isclose(round(x / STRIKE_STEP) * STRIKE_STEP, x, abs_tol=1e-9)


def meetings_seen(snapshots: Iterable[Snapshot]) -> list[datetime]:
    return sorted({s.meeting_at for s in snapshots if s.meeting_at is not None})


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
    last = max((m for m in meetings if m < reading.pulled_at), default=None)
    if last is not None and t_obs < last.date() + timedelta(days=1):
        return None
    if not on_grid(t_val):
        return None
    return t_obs, t_val


def priced_move_series(readings: dict[date, DayReading], target: pd.Series,
                       meetings: Sequence[datetime]) -> pd.Series:
    """Expected upper bound after the next meeting minus the current upper bound, per day with a valid pairing."""
    out = {}
    for d, r in readings.items():
        pair = target_pairing(r, target, meetings)
        if pair is not None:
            out[d] = r.ladder.expected_upper - pair[1]
    return pd.Series(out, dtype=float).sort_index()
