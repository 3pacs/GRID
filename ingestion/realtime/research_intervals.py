"""Offline, provenance-preserving rollups of retained native one-minute bars.

No feed, database, timer or production caller is changed. Session boundaries
must come from a verified exchange calendar, including early closes and DST.
This is not a reconstruction of ticks or volume-weighted prices.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from typing import Iterable

MINUTE_INTERVALS = (1, 2, 3, 5, 10, 15, 30, 60, 120, 240)
INTERVALS = tuple(f"{n}m" for n in MINUTE_INTERVALS) + ("1d",)


@dataclass(frozen=True)
class MinuteBar:
    symbol: str
    source: str
    ts: datetime  # inclusive event-time start
    available_at: datetime  # recorded receipt/revision availability, not event time
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    revision: str
    price_basis: str = "unadjusted"
    quality: str = "native_1m"


@dataclass(frozen=True)
class Session:
    key: str
    opens: datetime
    closes: datetime
    calendar_version: str


@dataclass(frozen=True)
class IntervalBar:
    symbol: str
    source: str
    interval: str
    session: str
    start: datetime
    end: datetime
    available_at: datetime
    assessed_at: datetime
    last_source_available_at: datetime | None
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    complete: bool
    expected_minutes: int
    observed_minutes: int
    missing_minutes: tuple[datetime, ...]
    source_revisions: tuple[str, ...]
    price_basis: str
    calendar_version: str
    provenance: str = "derived_from_native_1m"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware timestamps required")
    return value.astimezone(timezone.utc)


def rollup(
    bars: Iterable[MinuteBar], session: Session, interval: str, *, as_of: datetime
) -> list[IntervalBar]:
    """Emit closed windows only, preserving missing bars rather than filling them.

    Input is one symbol/source/adjustment stream with immutable revisions.
    Duplicate minute timestamps refuse; caller must resolve revisions using its
    point-in-time store before calling. Partial/gapped windows have null OHLCV.
    A final short session window is explicitly bounded by the session close.
    Daily means the supplied trading session, never a UTC calendar day.
    """
    if interval not in INTERVALS:
        raise ValueError("unsupported interval; subminute/ticks require native feed")
    now, start, stop = _utc(as_of), _utc(session.opens), _utc(session.closes)
    if not session.key or not session.calendar_version:
        raise ValueError("session and calendar provenance required")
    if (
        stop <= start
        or start.second
        or start.microsecond
        or stop.second
        or stop.microsecond
    ):
        raise ValueError("invalid minute-aligned session")
    rows = list(bars)
    if not rows:
        return []  # no stream identity; caller must report no coverage
    identities = {(b.symbol, b.source, b.price_basis) for b in rows}
    if len(identities) != 1 or any(not b.symbol or not b.source for b in rows):
        raise ValueError("mixed or empty stream identity")
    symbol, source, basis = next(iter(identities))
    selected: dict[datetime, MinuteBar] = {}
    for b in rows:
        ts, received = _utc(b.ts), _utc(b.available_at)
        if b.quality != "native_1m" or not b.revision:
            raise ValueError("native one-minute provenance and revision required")
        if ts.second or ts.microsecond or received < ts + timedelta(minutes=1):
            raise ValueError("unclosed or unaligned source minute")
        if not all(
            math.isfinite(x) and x > 0 for x in (b.open, b.high, b.low, b.close)
        ):
            raise ValueError("invalid price")
        if (
            b.low > min(b.open, b.close)
            or b.high < max(b.open, b.close)
            or b.low > b.high
        ):
            raise ValueError("invalid OHLC ordering")
        if b.volume is not None and (not math.isfinite(b.volume) or b.volume < 0):
            raise ValueError("invalid volume")
        if not (start <= ts < stop) or received > now:
            continue
        if ts in selected:
            raise ValueError("duplicate minute; resolve point-in-time revisions first")
        selected[ts] = b
    step = stop - start if interval == "1d" else timedelta(minutes=int(interval[:-1]))
    result = []
    window = start
    while window < stop:
        end = min(window + step, stop)
        if end > now:
            break
        expected = int((end - window).total_seconds() // 60)
        timestamps = [window + timedelta(minutes=i) for i in range(expected)]
        present = [selected[t] for t in timestamps if t in selected]
        missing = tuple(t for t in timestamps if t not in selected)
        complete = not missing
        last_receipt = max((_utc(b.available_at) for b in present), default=None)
        result.append(
            IntervalBar(
                symbol,
                source,
                interval,
                session.key,
                window,
                end,
                last_receipt if complete else now,
                now,
                last_receipt,
                present[0].open if complete else None,
                max(b.high for b in present) if complete else None,
                min(b.low for b in present) if complete else None,
                present[-1].close if complete else None,
                sum(b.volume for b in present)
                if complete and all(b.volume is not None for b in present)
                else None,
                complete,
                expected,
                len(present),
                missing,
                tuple(b.revision for b in present),
                basis,
                session.calendar_version,
            )
        )
        window = end
    return result
