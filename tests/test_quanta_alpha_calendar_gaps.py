"""Equity momentum factors survive weekend and holiday rows from 24-hour tickers.

The price panel's index is the union of every ticker's dates, so crypto and
GOLD put Saturday, Sunday and exchange-holiday rows into it on which every
equity is NaN. Before the fix the first equity return after such a row was
NaN, so on every Monday and every post-holiday session the equity momentum
factors ranked only the 24-hour tickers.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from alpha_research.signals.quanta_alpha import (
    _rank_cross_section,
    _zscore_ts,
    dual_horizon_equity,
    vol_regime_adaptive_equity,
)

EQUITIES = ["QQQ", "IWM", "DIA", "XLF", "XLK", "XLE"]
ALWAYS_ON = ["BTC", "ETH", "GOLD"]
THANKSGIVING = pd.Timestamp("2026-11-26")


def _sessions(end: str, n: int = 70, holidays: tuple[pd.Timestamp, ...] = ()) -> pd.DatetimeIndex:
    days = pd.bdate_range(end=end, periods=n + len(holidays))
    return days[~days.isin(holidays)][-n:]


def _panel(end: str, holidays: tuple[pd.Timestamp, ...] = (), with_always_on: bool = True) -> pd.DataFrame:
    """Equities on NYSE sessions only; 24-hour tickers on every calendar day."""
    rng = np.random.default_rng(7)
    sessions = _sessions(end, holidays=holidays)
    eq = pd.DataFrame(
        100 * np.exp(np.cumsum(rng.normal(0, 0.01, (len(sessions), len(EQUITIES))), axis=0)),
        index=sessions, columns=EQUITIES,
    )
    if not with_always_on:
        return eq
    days = pd.date_range(sessions[0], end, freq="D")
    on = pd.DataFrame(
        100 * np.exp(np.cumsum(rng.normal(0, 0.02, (len(days), len(ALWAYS_ON))), axis=0)),
        index=days, columns=ALWAYS_ON,
    )
    return eq.reindex(days).join(on)


FACTORS = [dual_horizon_equity, vol_regime_adaptive_equity]


@pytest.mark.parametrize("factor", FACTORS)
def test_monday_after_a_weekend_ranks_every_equity(factor) -> None:
    panel = _panel("2026-11-23")  # Monday; BTC/ETH/GOLD have Sat/Sun rows
    assert panel.loc["2026-11-22", EQUITIES].isna().all()

    ranked = factor(panel)

    assert ranked.loc["2026-11-23", EQUITIES].notna().all()


@pytest.mark.parametrize("factor", FACTORS)
def test_session_after_a_holiday_ranks_every_equity(factor) -> None:
    panel = _panel("2026-11-27", holidays=(THANKSGIVING,))  # GOLD/crypto have 11-26
    assert panel.loc["2026-11-26", EQUITIES].isna().all()

    ranked = factor(panel)

    assert ranked.loc["2026-11-27", EQUITIES].notna().all()


@pytest.mark.parametrize("factor", FACTORS)
@pytest.mark.parametrize(("end", "holidays"), [
    ("2026-11-23", ()),
    ("2026-11-27", (THANKSGIVING,)),
    ("2026-11-25", ()),
])
def test_equity_order_does_not_depend_on_24_hour_tickers(factor, end, holidays) -> None:
    """Weekend rows no longer shorten or blank an equity's windows."""
    mixed = factor(_panel(end, holidays=holidays)).loc[end, EQUITIES]
    alone = factor(_panel(end, holidays=holidays, with_always_on=False)).loc[end, EQUITIES]

    assert list(mixed.sort_values().index) == list(alone.sort_values().index)


def _old_dual_horizon_equity(prices: pd.DataFrame) -> pd.DataFrame:
    returns = prices.pct_change(fill_method=None)
    return _rank_cross_section(1.5 * _zscore_ts(returns, 10) + 1.0 * _zscore_ts(returns, 20))


def _old_vol_regime_adaptive_equity(prices: pd.DataFrame) -> pd.DataFrame:
    returns = prices.pct_change(fill_method=None)
    short_mom = _zscore_ts(returns, 10)
    long_mom = _zscore_ts(returns, 40)
    vol_zscore = _zscore_ts(returns.rolling(10, min_periods=5).std(), 30)
    signal = pd.DataFrame(np.where(vol_zscore > 0.3, short_mom, long_mom),
                          index=prices.index, columns=prices.columns)
    return _rank_cross_section(signal)


@pytest.mark.parametrize(("factor", "old"), [
    (dual_horizon_equity, _old_dual_horizon_equity),
    (vol_regime_adaptive_equity, _old_vol_regime_adaptive_equity),
])
def test_gap_free_panel_is_unchanged(factor, old) -> None:
    """With no NaN rows (an ordinary equity-only panel) the output is identical."""
    panel = _panel("2026-11-25", with_always_on=False)
    panel.loc[panel.index[:5], "XLE"] = np.nan  # a late-listed ticker: leading NaNs only

    pd.testing.assert_frame_equal(factor(panel), old(panel))
