from __future__ import annotations

from datetime import date

from paper_log.gex_levels.sessions import (
    expected_bar_count,
    expected_close_time,
    is_known_early_close,
)


def test_is_known_early_close_friday_after_thanksgiving() -> None:
    # Thanksgiving 2026 is Thursday Nov 26; the Friday after is Nov 27.
    assert is_known_early_close(date(2026, 11, 27)) is True


def test_is_known_early_close_july_3_and_dec_24_when_weekdays() -> None:
    # 2026-12-24 is a Thursday, a plain pre-Christmas weekday.
    assert is_known_early_close(date(2026, 12, 24)) is True
    # 2026-07-03 is itself the *observed* Independence Day holiday (July 4
    # falls on a Saturday that year, shifting the closure to Friday the
    # 3rd) — the market isn't open at all, so it is correctly NOT also an
    # early close. 2029 has no such collision for either date.
    assert is_known_early_close(date(2026, 7, 3)) is False
    assert is_known_early_close(date(2029, 7, 3)) is True
    assert is_known_early_close(date(2029, 12, 24)) is True


def test_is_known_early_close_false_on_regular_trading_day() -> None:
    assert is_known_early_close(date(2026, 9, 24)) is False


def test_is_known_early_close_false_on_weekend_even_if_date_matches() -> None:
    # 2027-07-03 is a Saturday — market isn't even open, so not an early close.
    assert date(2027, 7, 3).weekday() == 5
    assert is_known_early_close(date(2027, 7, 3)) is False


def test_is_known_early_close_false_on_market_holiday() -> None:
    # Dec 24, 2027 falls on a Friday; Dec 25 (observed) doesn't collide with
    # it, but exercise a clean non-early-close regular Monday instead to
    # keep this test's intent obvious.
    assert is_known_early_close(date(2026, 9, 21)) is False  # a Monday, no holiday


def test_expected_close_time_matches_early_close_flag() -> None:
    from datetime import time

    assert expected_close_time(date(2026, 11, 27)) == time(13, 0)
    assert expected_close_time(date(2026, 9, 24)) == time(16, 0)


def test_expected_bar_count_full_day() -> None:
    # 09:30 .. 15:55 start-times inclusive = 6.5h * 12 bars/hour = 78
    assert expected_bar_count(date(2026, 9, 24)) == 78


def test_expected_bar_count_early_close_day() -> None:
    # 09:30 .. 12:55 start-times inclusive = 3.5h * 12 bars/hour = 42
    assert expected_bar_count(date(2026, 11, 27)) == 42
