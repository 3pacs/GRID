"""The daily scheduler obtains one omitted, completed SPY session only."""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

from ingestion.market_calendar import is_market_open
from ingestion.scheduler import _completed_spy_close_window, _pull_completed_spy_close
from price_close_contract import capture_payload


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, 13, 30, tzinfo=timezone.utc)


def test_today_start_requests_yesterdays_completed_close_only():
    puller = MagicMock()
    puller.pull_ticker.return_value = {"outcome": "inserted", "rows_inserted": 1}

    result = _pull_completed_spy_close(puller, "2026-09-23", now_utc=_utc(2026, 9, 23))

    assert result == (date(2026, 9, 22), puller.pull_ticker.return_value)
    puller.pull_ticker.assert_called_once_with(
        "SPY", start_date=date(2026, 9, 22), end_date=date(2026, 9, 23),
        interval="1d", only_fields=frozenset({"close"}),
    )


def test_bulk_start_already_includes_completed_close():
    puller = MagicMock()

    assert _pull_completed_spy_close(
        puller, date(2026, 9, 22), now_utc=_utc(2026, 9, 23)
    ) is None
    puller.pull_ticker.assert_not_called()


def test_monday_uses_last_completed_friday():
    assert _completed_spy_close_window(
        "2026-09-28", now_utc=_utc(2026, 9, 28)
    ) == (date(2026, 9, 25), date(2026, 9, 26))


def test_exchange_holiday_uses_last_trading_session_within_contract():
    # Labor Day is closed in the existing US equity calendar. Tuesday is the
    # final day of the four-calendar-day operational capture allowance.
    assert not is_market_open(date(2026, 9, 7))
    assert _completed_spy_close_window(
        "2026-09-08", now_utc=_utc(2026, 9, 8)
    ) == (date(2026, 9, 4), date(2026, 9, 5))

    # Thanksgiving Thursday is closed; Friday's preceding completed session
    # is Wednesday under the existing exchange-calendar contract.
    assert not is_market_open(date(2026, 11, 26))
    assert _completed_spy_close_window(
        "2026-11-27", now_utc=_utc(2026, 11, 27)
    ) == (date(2026, 11, 25), date(2026, 11, 26))


def test_utc_day_boundary_never_selects_the_unfinished_observation():
    before = datetime(2026, 9, 22, 23, 59, 59, tzinfo=timezone.utc)
    after = before + timedelta(seconds=1)
    same_in_pacific = datetime(
        2026, 9, 22, 17, 0, tzinfo=timezone(timedelta(hours=-7))
    )

    assert _completed_spy_close_window("2026-09-22", now_utc=before) == (
        date(2026, 9, 21), date(2026, 9, 22)
    )
    assert _completed_spy_close_window("2026-09-23", now_utc=after) == (
        date(2026, 9, 22), date(2026, 9, 23)
    )
    assert _completed_spy_close_window("2026-09-23", now_utc=same_in_pacific) == (
        date(2026, 9, 22), date(2026, 9, 23)
    )
    assert capture_payload(date(2026, 9, 22), before) is None
    assert capture_payload(date(2026, 9, 22), after) is not None


def test_old_completed_session_does_not_start_a_history_repair(monkeypatch):
    monkeypatch.setattr(
        "ingestion.market_calendar.last_trading_day",
        lambda _: date(2026, 9, 17),
    )
    puller = MagicMock()

    assert _pull_completed_spy_close(
        puller, "2026-09-23", now_utc=_utc(2026, 9, 23)
    ) is None
    puller.pull_ticker.assert_not_called()
