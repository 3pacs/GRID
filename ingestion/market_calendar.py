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

from datetime import date, timedelta
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
