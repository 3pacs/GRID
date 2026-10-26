"""NYSE regular-session calendar helpers for the GEX-levels paper log.

Reuses ``ingestion.market_calendar`` (the repo's existing holiday/weekend
calendar — grepped for before writing this, per CLAUDE.md's "grep before
you build" rule) for `is_market_open` / `last_trading_day`. That module has
no concept of *early closes*, which this job needs for an honest
`bars_missing` denominator ("more than 10% of the expected 5-minute bars
are missing ... early closes use the bars that exist").

NYSE's early-close days are 1:00pm ET close and, by long-standing exchange
policy, always one of: the Friday after Thanksgiving, and (when they fall
on a trading day) July 3rd and December 24th. That policy is a fixed
market-structure fact, not something that drifts year to year, so it is
safe to compute rather than needing a maintained date table. An ad hoc,
unscheduled early close (weather, technical outage) is NOT covered by
this — in that case `expected_bar_count` over-estimates the session and
`bars_missing` correctly (safely) excludes the session rather than
silently under-counting a partial day as complete. That is the intended,
safe-direction failure mode; it is called out again in `postclose.py`.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

from ingestion.market_calendar import is_market_holiday, is_market_open, last_trading_day  # noqa: F401

REGULAR_OPEN = time(9, 30)
REGULAR_CLOSE = time(16, 0)
EARLY_CLOSE = time(13, 0)
BAR_MINUTES = 5


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def is_known_early_close(d: date) -> bool:
    """True for NYSE's recurring 1:00pm ET early-close days that fall on a
    trading day: the Friday after Thanksgiving, July 3rd, December 24th."""
    if not is_market_open(d):
        return False

    thanksgiving = _nth_weekday(d.year, 11, 3, 4)  # 4th Thursday in November
    if d == thanksgiving + timedelta(days=1):
        return True

    if d.month == 7 and d.day == 3:
        return True

    if d.month == 12 and d.day == 24:
        return True

    return False


def expected_close_time(d: date) -> time:
    return EARLY_CLOSE if is_known_early_close(d) else REGULAR_CLOSE


def expected_bar_count(d: date) -> int:
    """Number of 5-minute bars in [09:30, expected close) for this date.

    A full day: 09:30..15:55 inclusive start-times = 78 bars (6.5h * 12).
    An early-close day: 09:30..12:55 = 42 bars (3.5h * 12).
    """
    open_dt = datetime.combine(d, REGULAR_OPEN)
    close_dt = datetime.combine(d, expected_close_time(d))
    minutes = (close_dt - open_dt).total_seconds() / 60.0
    return int(minutes // BAR_MINUTES)
