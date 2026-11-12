"""
US equity market calendar — holidays, half-days, and trading day checks.

Used by the scheduler and data freshness monitors to avoid chasing
data that doesn't exist (weekends, Good Friday, etc.).

Usage:
    from ingestion.market_calendar import is_market_open, last_trading_day, next_trading_day

    if not is_market_open(date.today()):
        log.info("Market closed — skipping pull")
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from typing import Any
from zoneinfo import ZoneInfo
from functools import lru_cache


def _easter_sunday(year: int) -> date:
    """Compute Easter Sunday via the Anonymous Gregorian algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7  # noqa: E741
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


@lru_cache(maxsize=16)
def market_holidays(year: int) -> set[date]:
    """Return the set of NYSE/NASDAQ market holidays for a given year.

    Covers all standard US market closures:
    - New Year's Day (Jan 1, or observed)
    - MLK Day (3rd Monday in Jan)
    - Presidents' Day (3rd Monday in Feb)
    - Good Friday (Friday before Easter)
    - Memorial Day (last Monday in May)
    - Juneteenth (Jun 19, or observed)
    - Independence Day (Jul 4, or observed)
    - Labor Day (1st Monday in Sep)
    - Thanksgiving (4th Thursday in Nov)
    - Christmas Day (Dec 25, or observed)
    """
    holidays: set[date] = set()

    def _observed(d: date) -> date:
        """If holiday falls on Saturday → Friday; Sunday → Monday."""
        if d.weekday() == 5:  # Saturday
            return d - timedelta(days=1)
        if d.weekday() == 6:  # Sunday
            return d + timedelta(days=1)
        return d

    def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
        """Return the nth occurrence of a weekday in a month (1-indexed)."""
        first = date(year, month, 1)
        offset = (weekday - first.weekday()) % 7
        return first + timedelta(days=offset + 7 * (n - 1))

    def _last_weekday(year: int, month: int, weekday: int) -> date:
        """Return the last occurrence of a weekday in a month."""
        if month == 12:
            last_day = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = date(year, month + 1, 1) - timedelta(days=1)
        offset = (last_day.weekday() - weekday) % 7
        return last_day - timedelta(days=offset)

    # New Year's Day
    holidays.add(_observed(date(year, 1, 1)))

    # MLK Day — 3rd Monday in January
    holidays.add(_nth_weekday(year, 1, 0, 3))

    # Presidents' Day — 3rd Monday in February
    holidays.add(_nth_weekday(year, 2, 0, 3))

    # Good Friday — 2 days before Easter Sunday
    easter = _easter_sunday(year)
    holidays.add(easter - timedelta(days=2))

    # Memorial Day — last Monday in May
    holidays.add(_last_weekday(year, 5, 0))

    # Juneteenth — June 19
    holidays.add(_observed(date(year, 6, 19)))

    # Independence Day — July 4
    holidays.add(_observed(date(year, 7, 4)))

    # Labor Day — 1st Monday in September
    holidays.add(_nth_weekday(year, 9, 0, 1))

    # Thanksgiving — 4th Thursday in November
    holidays.add(_nth_weekday(year, 11, 3, 4))

    # Christmas — December 25
    holidays.add(_observed(date(year, 12, 25)))

    return holidays


def is_weekend(d: date) -> bool:
    """Return True if the date is a Saturday or Sunday."""
    return d.weekday() >= 5


def is_market_holiday(d: date) -> bool:
    """Return True if the date is a US market holiday."""
    return d in market_holidays(d.year)


def is_market_open(d: date) -> bool:
    """Return True if the US equity market is open on this date."""
    return not is_weekend(d) and not is_market_holiday(d)


def market_close_time(d: date) -> time:
    """NYSE cash-equity close in New York local time for a trading day.

    Standard early closes: July 3 when open, the Friday after Thanksgiving,
    and December 24 when open. Verified against the exchange's 2026–2028
    calendar: https://www.nyse.com/markets/hours-calendars . Emergency
    closures require an explicit calendar update, as with market_holidays.
    This is the underlying equity close, not eligible options' 13:15 close.
    """
    if not is_market_open(d):
        raise ValueError("non-trading day has no regular-session close")
    november_first = date(d.year, 11, 1)
    thanksgiving = november_first + timedelta(
        days=(3 - november_first.weekday()) % 7 + 21,
    )
    if (d.month, d.day) in ((7, 3), (12, 24)) or d == thanksgiving + timedelta(days=1):
        return time(13, 0)
    return time(16, 0)


def last_trading_day(d: date | None = None) -> date:
    """Return the most recent trading day on or before the given date."""
    if d is None:
        d = date.today()
    while not is_market_open(d):
        d -= timedelta(days=1)
    return d


def next_trading_day(d: date | None = None) -> date:
    """Return the next trading day on or after the given date."""
    if d is None:
        d = date.today()
    while not is_market_open(d):
        d += timedelta(days=1)
    return d


def trading_days_between(start: date, end: date) -> list[date]:
    """Return all trading days in [start, end] inclusive."""
    days = []
    d = start
    while d <= end:
        if is_market_open(d):
            days.append(d)
        d += timedelta(days=1)
    return days


# Exact offline calendar for consumers that cannot infer sessions from observed
# rows. Adapted from the reviewed source-only native_volume_calendar at a3172b27.
# The approximate scheduler helpers above intentionally retain their semantics.

@dataclass(frozen=True)
class MarketSession:
    key: str
    opens: datetime
    closes: datetime
    calendar_version: str


SCHEMA = "grid.xnys-session-manifest.v1"
MAX_BYTES = 1_048_576
VERSION = re.compile(
    r"^exchange_calendars:4\.11\.2:pandas:2\.3\.3:numpy:[0-9.]+:tzdata:[0-9.]+:XNYS:\d{4}-\d{2}-\d{2}:\d{4}-\d{2}-\d{2}$"
)
KEY = re.compile(r"^XNYS:(\d{4}-\d{2}-\d{2}):RTH$")


def canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _day(value: str) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("ISO date required")
    return date.fromisoformat(value)


def _utc(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("UTC ISO timestamp required")
    result = datetime.fromisoformat(value)
    if (
        result.tzinfo is None
        or result.utcoffset().total_seconds() != 0
        or result.second
        or result.microsecond
    ):
        raise ValueError("minute-aligned UTC timestamp required")
    return result.astimezone(timezone.utc)


def _parse(raw: bytes, expected_sha256: str) -> tuple[dict[str, Any], tuple[MarketSession, ...]]:
    if not isinstance(expected_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", expected_sha256
    ):
        raise ValueError("operator-pinned SHA256 required")
    if len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("manifest size or hash mismatch")

    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError("duplicate JSON key")
            out[key] = value
        return out

    doc = json.loads(raw, object_pairs_hook=unique)
    fields = {
        "schema",
        "exchange",
        "session_type",
        "timezone",
        "start",
        "end",
        "calendar_version",
        "sessions",
    }
    if (
        not isinstance(doc, dict)
        or set(doc) != fields
        or (doc["schema"], doc["exchange"], doc["session_type"], doc["timezone"])
        != (SCHEMA, "XNYS", "RTH", "America/New_York")
    ):
        raise ValueError("manifest identity/schema mismatch")
    if not isinstance(doc["calendar_version"], str) or not VERSION.fullmatch(
        doc["calendar_version"]
    ):
        raise ValueError("unreviewed dependency provenance")
    begin, finish = _day(doc["start"]), _day(doc["end"])
    if begin > finish or (finish - begin).days > 1096:
        raise ValueError("invalid coverage bounds")
    dependency_bounds = doc["calendar_version"].split(":")[-2:]
    if begin < _day(dependency_bounds[0]) or finish > _day(dependency_bounds[1]):
        raise ValueError("manifest exceeds dependency calendar coverage")
    rows = doc["sessions"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= 1000:
        raise ValueError("bounded nonempty sessions required")
    output = []
    last_day = None
    last_close = None
    version = f"manifest:{expected_sha256}:{doc['calendar_version']}"
    for row in rows:
        if (
            not isinstance(row, dict)
            or set(row) != {"key", "opens", "closes"}
            or not isinstance(row["key"], str)
        ):
            raise ValueError("session shape")
        match = KEY.fullmatch(row["key"])
        if not match:
            raise ValueError("session identity")
        day = _day(match.group(1))
        op, cl = _utc(row["opens"]), _utc(row["closes"])
        minutes = (cl - op).total_seconds() / 60
        if (
            not begin <= day <= finish
            or (last_day and day <= last_day)
            or op.date() != day
            or cl.date() != day
            or op.astimezone(ZoneInfo("America/New_York")).time() != time(9, 30)
            or not 0 < minutes <= 390
            or (last_close and op <= last_close)
        ):
            raise ValueError("invalid session order/bounds")
        output.append(MarketSession(row["key"], op, cl, version))
        last_day = day
        last_close = cl
    if raw != canonical(doc):
        raise ValueError("canonical manifest bytes required")
    return doc, tuple(output)


class ManifestCalendar:
    """Pinned, bounded schedule using only the Python standard library."""

    def __init__(self, path: str | Path, expected_sha256: str) -> None:
        p = Path(path).absolute()
        try:
            resolved = p.resolve()
        except RuntimeError as exc:  # Python 3.11 reports a symlink cycle this way.
            raise ValueError("manifest symlink cycle refused") from exc
        if resolved != p:
            raise ValueError("manifest symlink refused")
        fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            st = os.fstat(stream.fileno())
            if not stat.S_ISREG(st.st_mode) or stat.S_IMODE(st.st_mode) & 0o022:
                raise ValueError("regular non-group/world-writable manifest required")
            raw = stream.read(MAX_BYTES + 1)
        doc, self.sessions = _parse(raw, expected_sha256)
        self.start, self.end = _day(doc["start"]), _day(doc["end"])
        self.version = self.sessions[0].calendar_version
        self._by_key = {s.key: s for s in self.sessions}
        self._index = {s.key: i for i, s in enumerate(self.sessions)}

    def __call__(self, key: str) -> MarketSession:
        if not isinstance(key, str) or key not in self._by_key:
            raise ValueError("unknown/closed/out-of-range manifest session")
        return self._by_key[key]

    def prior_sessions(self, key: str, count: int = 10) -> tuple[MarketSession, ...]:
        self(key)
        if type(count) is not int or not 1 <= count <= 60:
            raise ValueError("bounded prior-session count required")
        i = self._index[key]
        if i < count:
            raise ValueError("insufficient manifest history")
        return self.sessions[i - count : i]

    def for_date(self, day: date) -> MarketSession | None:
        """None means a known closed day, never an unknown/out-of-range day."""
        if not self.start <= day <= self.end:
            raise ValueError("calendar date outside verified bounds")
        return self._by_key.get(f"XNYS:{day.isoformat()}:RTH")
