"""Tests for yfinance fetchers. Every call goes through `_history`, the one
seam this file monkeypatches — no network is ever touched."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest

import paper_log.gex_levels.market_data as market_data
from paper_log.gex_levels.config import EASTERN
from paper_log.gex_levels.market_data import (
    fetch_intraday_bars,
    fetch_previous_close,
    fetch_session_ohlc,
)

FIXED_NOW = datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc)


def _fixed_now() -> datetime:
    return FIXED_NOW


def _daily_frame(rows: list[tuple[str, float, float, float, float]]) -> pd.DataFrame:
    idx = pd.to_datetime([r[0] for r in rows])
    return pd.DataFrame(
        {
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
            "Volume": [1_000_000] * len(rows),
        },
        index=idx,
    )


# ── fetch_previous_close ─────────────────────────────────────────────


def test_fetch_previous_close_returns_last_row_strictly_before(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_history(ticker, **kwargs):
        captured["ticker"] = ticker
        captured["kwargs"] = kwargs
        return _daily_frame([
            ("2026-09-21", 550, 552, 549, 551),
            ("2026-09-22", 551, 553, 550, 552),
            ("2026-09-23", 552, 556, 551, 555),  # the day before `before`
            ("2026-09-24", 555, 558, 554, 557),  # on/after `before` — excluded
        ])

    monkeypatch.setattr(market_data, "_history", fake_history)

    result = fetch_previous_close("SPY", before=date(2026, 9, 24), now_fn=_fixed_now)

    assert result is not None
    assert result.price == 555.0
    assert result.as_of_date == date(2026, 9, 23)
    assert result.fetched_at == FIXED_NOW
    assert captured["ticker"] == "SPY"
    assert captured["kwargs"]["auto_adjust"] is False  # unadjusted, explicit


def test_fetch_previous_close_none_when_no_prior_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        market_data, "_history",
        lambda ticker, **kwargs: _daily_frame([("2026-09-24", 555, 558, 554, 557)]),
    )
    result = fetch_previous_close("SPY", before=date(2026, 9, 24), now_fn=_fixed_now)
    assert result is None


def test_fetch_previous_close_none_when_empty_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(market_data, "_history", lambda ticker, **kwargs: pd.DataFrame())
    result = fetch_previous_close("SPY", before=date(2026, 9, 24), now_fn=_fixed_now)
    assert result is None


def test_fetch_previous_close_works_for_vix_ticker_too(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {}

    def fake_history(ticker, **kwargs):
        captured["ticker"] = ticker
        return _daily_frame([("2026-09-23", 15, 16, 14, 15.5)])

    monkeypatch.setattr(market_data, "_history", fake_history)
    result = fetch_previous_close("^VIX", before=date(2026, 9, 24), now_fn=_fixed_now)
    assert result is not None
    assert result.price == 15.5
    assert captured["ticker"] == "^VIX"


# ── fetch_session_ohlc ────────────────────────────────────────────────


def test_fetch_session_ohlc_returns_the_sessions_own_bar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        market_data, "_history",
        lambda ticker, **kwargs: _daily_frame([("2026-09-24", 555, 560, 553, 558)]),
    )
    result = fetch_session_ohlc("SPY", date(2026, 9, 24), now_fn=_fixed_now)
    assert result is not None
    assert (result.open, result.high, result.low, result.close) == (555.0, 560.0, 553.0, 558.0)
    assert result.fetched_at == FIXED_NOW


def test_fetch_session_ohlc_none_when_no_bar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(market_data, "_history", lambda ticker, **kwargs: pd.DataFrame())
    result = fetch_session_ohlc("SPY", date(2026, 9, 24), now_fn=_fixed_now)
    assert result is None


# ── fetch_intraday_bars ───────────────────────────────────────────────


def _intraday_frame(session_date: date, rows: list[tuple[str, float, float, float, float]]) -> pd.DataFrame:
    """rows are (HH:MM, o, h, l, c) in America/New_York."""
    times = [
        pd.Timestamp(f"{session_date.isoformat()} {t}").tz_localize(EASTERN)
        for t, *_ in rows
    ]
    return pd.DataFrame(
        {
            "Open": [r[1] for r in rows],
            "High": [r[2] for r in rows],
            "Low": [r[3] for r in rows],
            "Close": [r[4] for r in rows],
            "Volume": [10_000] * len(rows),
        },
        index=pd.DatetimeIndex(times),
    )


def test_fetch_intraday_bars_keeps_regular_session_window_only(monkeypatch: pytest.MonkeyPatch) -> None:
    d = date(2026, 9, 24)
    frame = _intraday_frame(d, [
        ("09:25", 100, 101, 99, 100.5),   # pre-market — dropped
        ("09:30", 100.5, 101, 100, 100.8),  # first regular bar — kept
        ("15:55", 102, 103, 101, 102.5),  # last regular bar — kept
        ("16:00", 102.5, 103, 102, 102.7),  # after-hours (bar labeled 16:00) — dropped
    ])
    monkeypatch.setattr(market_data, "_history", lambda ticker, **kwargs: frame)

    result = fetch_intraday_bars("SPY", d, now_fn=_fixed_now)

    assert [b.time.strftime("%H:%M") for b in result.bars] == ["09:30", "15:55"]
    assert result.fetched_at == FIXED_NOW


def test_fetch_intraday_bars_sorted_ascending_even_if_source_is_not(monkeypatch: pytest.MonkeyPatch) -> None:
    d = date(2026, 9, 24)
    frame = _intraday_frame(d, [("09:40", 1, 2, 1, 2), ("09:30", 1, 2, 1, 2), ("09:35", 1, 2, 1, 2)])
    monkeypatch.setattr(market_data, "_history", lambda ticker, **kwargs: frame)

    result = fetch_intraday_bars("SPY", d, now_fn=_fixed_now)
    times = [b.time.strftime("%H:%M") for b in result.bars]
    assert times == sorted(times)


def test_fetch_intraday_bars_respects_early_close_cutoff(monkeypatch: pytest.MonkeyPatch) -> None:
    early_close_day = date(2026, 11, 27)  # Friday after Thanksgiving
    frame = _intraday_frame(early_close_day, [
        ("12:55", 1, 2, 1, 2),  # last valid bar on an early-close day
        ("13:00", 1, 2, 1, 2),  # after the 13:00 cutoff — dropped
    ])
    monkeypatch.setattr(market_data, "_history", lambda ticker, **kwargs: frame)

    result = fetch_intraday_bars("SPY", early_close_day, now_fn=_fixed_now)
    assert [b.time.strftime("%H:%M") for b in result.bars] == ["12:55"]


def test_fetch_intraday_bars_empty_when_no_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(market_data, "_history", lambda ticker, **kwargs: pd.DataFrame())
    result = fetch_intraday_bars("SPY", date(2026, 9, 24), now_fn=_fixed_now)
    assert result.bars == ()


def test_fetch_intraday_bars_handles_tz_naive_source_frame(monkeypatch: pytest.MonkeyPatch) -> None:
    """Defensive: if yfinance ever returns a tz-naive index, localize (not
    convert) to America/New_York rather than crashing or mis-shifting."""
    d = date(2026, 9, 24)
    idx = pd.DatetimeIndex([pd.Timestamp(f"{d.isoformat()} 09:30")])  # naive
    frame = pd.DataFrame({"Open": [1.0], "High": [2.0], "Low": [1.0], "Close": [1.5], "Volume": [1]}, index=idx)
    monkeypatch.setattr(market_data, "_history", lambda ticker, **kwargs: frame)

    result = fetch_intraday_bars("SPY", d, now_fn=_fixed_now)
    assert len(result.bars) == 1
    assert result.bars[0].time.strftime("%H:%M") == "09:30"
