"""Equity momentum factors on each ticker's own market calendar.

The price panel's index is the union of every ticker's dates, so crypto and
GOLD put Saturday, Sunday and exchange-holiday rows into it on which every
equity is NaN. Before the fix the first equity return after such a row was
NaN, so on every Monday and every post-holiday session the equity momentum
factors ranked only the 24-hour tickers, and equities and 24-hour series were
ranked in one cross-section.

The controls compare against the old implementation run on an equity-only
panel indexed by real NYSE sessions: that is exactly "NYSE-session windows,
a missing session is a NaN slot", the intended definition.
"""
from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from alpha_research.signals.quanta_alpha import (
    CALENDAR_CONTINUOUS,
    CALENDAR_XNYS,
    _rank_cross_section,
    _zscore_ts,
    dual_horizon_equity,
    vol_regime_adaptive_equity,
)
from ingestion.market_calendar import is_market_open

EQUITIES = ["QQQ", "IWM", "DIA", "XLF", "XLK", "XLE"]
ALWAYS_ON = ["BTC", "ETH", "SOL"]  # 24x7 series (GOLD is XNYS by its own observations)
THANKSGIVING = pd.Timestamp("2026-11-26")


def _sessions(end: str, n: int = 70) -> pd.DatetimeIndex:
    days, day = [], pd.Timestamp(end).date()
    while len(days) < n:
        if is_market_open(day):
            days.append(day)
        day -= timedelta(days=1)
    return pd.DatetimeIndex(sorted(days))


def _panel(end: str, with_always_on: bool = True) -> pd.DataFrame:
    """Equities on real NYSE sessions only; 24-hour tickers on every calendar day."""
    rng = np.random.default_rng(7)
    sessions = _sessions(end)
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


PAIRS = [
    (dual_horizon_equity, _old_dual_horizon_equity),
    (vol_regime_adaptive_equity, _old_vol_regime_adaptive_equity),
]
FACTORS = [new for new, _ in PAIRS]


def _equity_view(ranked: pd.DataFrame, like: pd.DataFrame) -> pd.DataFrame:
    return ranked.loc[like.index, EQUITIES]


@pytest.mark.parametrize("factor", FACTORS)
def test_monday_after_a_weekend_ranks_every_equity(factor) -> None:
    panel = _panel("2026-11-23")  # Monday; BTC/ETH/SOL have Sat/Sun rows
    assert panel.loc["2026-11-22", EQUITIES].isna().all()

    ranked = factor(panel, min_peers=len(EQUITIES))

    assert ranked.loc["2026-11-23", EQUITIES].notna().all()


@pytest.mark.parametrize("factor", FACTORS)
def test_session_after_a_holiday_ranks_every_equity(factor) -> None:
    panel = _panel("2026-11-27")  # crypto has Thanksgiving 11-26
    assert panel.loc["2026-11-26", EQUITIES].isna().all()

    ranked = factor(panel, min_peers=len(EQUITIES))

    assert ranked.loc["2026-11-27", EQUITIES].notna().all()


@pytest.mark.parametrize(("factor", "old"), PAIRS)
@pytest.mark.parametrize("end", ["2026-11-23", "2026-11-25", "2026-11-27"])
def test_equities_equal_the_old_code_on_an_nyse_session_panel(factor, old, end) -> None:
    """Calendars are not conflated: 24-hour series change no equity value."""
    mixed = factor(_panel(end), min_peers=1)
    reference = old(_panel(end, with_always_on=False))

    pd.testing.assert_frame_equal(_equity_view(mixed, reference), reference, check_freq=False, check_index_type=False)


@pytest.mark.parametrize(("factor", "old"), PAIRS)
def test_a_missing_session_is_a_nan_slot_and_the_next_days_match_the_control(factor, old) -> None:
    """XLE's data lacks Tuesday 11-24, a session the market held.

    Wednesday's return (Mon->Wed) is not bridged; Tuesday stays one slot of
    each window. Thursday 11-26 is a holiday, so Friday 11-27 is the next
    return; every value equals the old code on an NYSE-session panel with
    Tuesday NaN (the subsequent-day control).
    """
    end = "2026-11-30"
    mixed = _panel(end)
    mixed.loc["2026-11-24", "XLE"] = np.nan
    control = _panel(end, with_always_on=False)
    control.loc["2026-11-24", "XLE"] = np.nan

    new_ranked = factor(mixed, min_peers=1)
    reference = old(control)

    pd.testing.assert_frame_equal(_equity_view(new_ranked, reference), reference, check_freq=False, check_index_type=False)
    assert np.isnan(new_ranked.loc["2026-11-25", "XLE"])
    assert not np.isnan(new_ranked.loc["2026-11-27", "XLE"])
    assert not np.isnan(new_ranked.loc["2026-11-30", "XLE"])


@pytest.mark.parametrize("factor", FACTORS)
def test_24_hour_series_are_ranked_only_among_themselves(factor) -> None:
    ranked = factor(_panel("2026-11-23"), min_peers=len(ALWAYS_ON))

    assert ranked.attrs["calendar"] == {
        **{t: CALENDAR_XNYS for t in EQUITIES}, **{t: CALENDAR_CONTINUOUS for t in ALWAYS_ON}}
    sunday = ranked.loc["2026-11-22"]
    assert sunday[EQUITIES].isna().all()
    assert sorted(sunday[ALWAYS_ON]) == pytest.approx([1 / 3, 2 / 3, 1.0])
    sizes = ranked.attrs["rank_universe_size"]
    assert set(sizes.loc["2026-11-23", EQUITIES]) == {len(EQUITIES)}
    assert set(sizes.loc["2026-11-23", ALWAYS_ON]) == {len(ALWAYS_ON)}


@pytest.mark.parametrize("factor", FACTORS)
def test_a_lone_name_gets_no_rank(factor) -> None:
    """Only QQQ has a value on the newest session: never percentile 1.0 alone."""
    panel = _panel("2026-11-25")
    panel.loc["2026-11-25", [t for t in EQUITIES if t != "QQQ"]] = np.nan

    ranked = factor(panel, min_peers=2)

    assert np.isnan(ranked.loc["2026-11-25", "QQQ"])
    assert ranked.loc["2026-11-24", EQUITIES].notna().all()


@pytest.mark.parametrize("factor", FACTORS)
@pytest.mark.parametrize(("present", "ranked_expected"), [(len(EQUITIES), True), (len(EQUITIES) - 1, False)])
def test_peer_minimum_boundary(factor, present, ranked_expected) -> None:
    panel = _panel("2026-11-25")
    panel.loc["2026-11-25", EQUITIES[present:]] = np.nan

    ranked = factor(panel, min_peers=len(EQUITIES))

    assert bool(ranked.loc["2026-11-25", EQUITIES[:present]].notna().all()) is ranked_expected
    assert bool(ranked.loc["2026-11-25", EQUITIES[:present]].isna().all()) is not ranked_expected


@pytest.mark.parametrize(("factor", "old"), PAIRS)
def test_gap_free_panel_is_unchanged(factor, old) -> None:
    """An equity-only NYSE-session panel (leading NaNs only) gives the old output."""
    panel = _panel("2026-11-25", with_always_on=False)
    panel.loc[panel.index[:5], "XLE"] = np.nan  # a late-listed ticker

    pd.testing.assert_frame_equal(factor(panel, min_peers=1), old(panel), check_freq=False, check_index_type=False)


@pytest.mark.parametrize(("factor", "old"), PAIRS)
def test_two_consecutive_missing_sessions_are_two_slots(factor, old) -> None:
    """XLE lacks Mon 11-23 and Tue 11-24: neither is bridged, both occupy window slots."""
    end = "2026-11-30"
    mixed = _panel(end)
    control = _panel(end, with_always_on=False)
    for frame in (mixed, control):
        frame.loc[["2026-11-23", "2026-11-24"], "XLE"] = np.nan

    new_ranked = factor(mixed, min_peers=1)
    reference = old(control)

    pd.testing.assert_frame_equal(_equity_view(new_ranked, reference), reference,
                                  check_freq=False, check_index_type=False)
    assert np.isnan(new_ranked.loc["2026-11-25", "XLE"])
    assert not np.isnan(new_ranked.loc["2026-11-27", "XLE"])


@pytest.mark.parametrize(("factor", "old"), PAIRS)
def test_a_stray_holiday_print_of_an_equity_is_dropped(factor, old) -> None:
    """A provider row for XLE on Thanksgiving is not a session: it changes nothing."""
    end = "2026-11-30"
    mixed = _panel(end)
    mixed.loc["2026-11-26", "XLE"] = 1e6  # absurd value: any use of it would show
    reference = old(_panel(end, with_always_on=False))

    new_ranked = factor(mixed, min_peers=1)

    assert new_ranked.attrs["calendar"]["XLE"] == CALENDAR_XNYS
    pd.testing.assert_frame_equal(_equity_view(new_ranked, reference), reference,
                                  check_freq=False, check_index_type=False)
