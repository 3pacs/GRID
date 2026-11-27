"""
Proven signals from QuantaAlpha research (Saulius.io).

All signals operate on close-price panels (dates x tickers).
Returns are cross-sectional rank signals in [-1, 1] range.

Evidence:
  - Vol Regime Adaptive: OOS Sharpe 1.72, Return 38.7%
  - Dual Horizon Momentum: OOS Sharpe 1.18, RankIC 0.024 (highest)
  - Trend Volume Gate: OOS Sharpe 1.11, Return 25.1%
  - Vol-Price Divergence: OOS Sharpe 1.03, MaxDD -16.9% (best risk-adj)
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

import numpy as np
import pandas as pd


def _rank_cross_section(df: pd.DataFrame) -> pd.DataFrame:
    """Rank each row (date) cross-sectionally → percentile in [0, 1]."""
    return df.rank(axis=1, pct=True)


def _zscore_ts(series: pd.DataFrame, window: int) -> pd.DataFrame:
    """Rolling time-series z-score per column."""
    mu = series.rolling(window, min_periods=max(window // 2, 5)).mean()
    sigma = series.rolling(window, min_periods=max(window // 2, 5)).std()
    return (series - mu) / (sigma + 1e-8)


#: A ticker is ranked on a date only against at least this many peers on the
#: same market calendar, so each 20 % tail holds at least four names. A row
#: with fewer gives no rank (never percentile 1.0 for a lone name).
MIN_RANK_PEERS = 20

CALENDAR_XNYS = "XNYS"            # trades NYSE sessions only
CALENDAR_CONTINUOUS = "24x7"      # observed on weekends/holidays (crypto, GOLD)
#: Share of a ticker's observations on non-NYSE days above which it is a
#: 24-hour series. A daily-traded series has about 2/7 on weekends alone; an
#: equity with a stray holiday print stays XNYS and that print is dropped.
_CONTINUOUS_SHARE = 0.15


def _ticker_calendar(dates: list) -> str:
    from ingestion.market_calendar import is_market_open

    off = sum(1 for d in dates if not is_market_open(d))
    return CALENDAR_CONTINUOUS if dates and off / len(dates) > _CONTINUOUS_SHARE else CALENDAR_XNYS


def _calendar_grid(first, last, calendar: str) -> list:
    """Every date the ticker's market was open from ``first`` to ``last``."""
    from ingestion.market_calendar import is_market_open

    days, day = [], first
    while day <= last:
        if calendar == CALENDAR_CONTINUOUS or is_market_open(day):
            days.append(day)
        day += timedelta(days=1)
    return days


def _on_observed_dates(
    prices: pd.DataFrame,
    raw_fn: Callable[[pd.DataFrame], pd.DataFrame],
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Apply a returns transform to each ticker on its own market calendar.

    The price panel's index is the union of every ticker's observation dates,
    so it carries weekend and exchange-holiday rows for 24-hour tickers on
    which every equity is NaN. On that index the first equity return after a
    closed day was NaN and windows counted other tickers' days.

    Here each ticker is placed on its own calendar (``_ticker_calendar``):
    NYSE sessions for an equity, every day for a 24-hour series. Closed days
    are not rows, so Friday to Monday and Wednesday to Friday over Thanksgiving
    are one step. A session the market held but the data lacks stays a NaN
    row: the return into it and the return out of it are NaN, and it occupies
    one slot of every rolling window (a 20-session window after a missing
    session covers 20 sessions with 19 or fewer returns; ``min_periods``
    decides). ``raw_fn`` runs on those returns; the result is placed back on
    the shared index. Returns the result and each ticker's calendar.
    """
    out: dict[str, pd.Series] = {}
    calendars: dict[str, str] = {}
    for col in prices.columns:
        own = prices[col].dropna()
        if own.empty:
            out[col] = pd.Series(np.nan, index=prices.index, dtype=float)
            continue
        dates = [ts.date() for ts in own.index]
        calendar = _ticker_calendar(dates)
        calendars[col] = calendar
        grid = pd.DatetimeIndex(_calendar_grid(dates[0], dates[-1], calendar))
        series = own[own.index.isin(grid)].reindex(grid)
        returns = series.pct_change(fill_method=None).to_frame(col)
        out[col] = raw_fn(returns)[col].reindex(prices.index)
    return pd.DataFrame(out, index=prices.index, columns=prices.columns), calendars


def _rank_by_calendar(
    signal: pd.DataFrame, calendars: dict[str, str], min_peers: int,
) -> pd.DataFrame:
    """Cross-sectional percentile per row within each market calendar.

    Equities are never ranked against 24-hour series. A row of a calendar
    with fewer than ``min_peers`` values gives no rank. The returned frame's
    ``attrs["rank_universe_size"]`` holds, per cell, the number of peers its
    rank was taken among.
    """
    ranked = pd.DataFrame(np.nan, index=signal.index, columns=signal.columns)
    sizes = pd.DataFrame(np.nan, index=signal.index, columns=signal.columns)
    for calendar in sorted(set(calendars.values())):
        cols = [c for c in signal.columns if calendars.get(c) == calendar]
        group = signal[cols]
        count = group.notna().sum(axis=1)
        enough = count >= min_peers
        ranked.loc[enough, cols] = group.loc[enough].rank(axis=1, pct=True)
        sizes.loc[enough, cols] = group.loc[enough].notna().mul(count[enough], axis=0).where(
            group.loc[enough].notna())
    ranked.attrs["rank_universe_size"] = sizes
    ranked.attrs["calendar"] = dict(calendars)
    return ranked


def vol_regime_adaptive_momentum(
    prices: pd.DataFrame,
    short_window: int = 5,
    long_window: int = 20,
    vol_window: int = 20,
    vol_zscore_window: int = 20,
    vol_threshold: float = 0.5,
) -> pd.DataFrame:
    """
    Vol Regime Adaptive Momentum.

    When recent 5-day volatility is unusually high (z-score > threshold),
    use short-term momentum. Otherwise, use medium-term momentum.
    Rank cross-sectionally.

    OOS Sharpe: 1.72 | Return: 38.7% | MaxDD: -15.8%
    """
    returns = prices.pct_change(fill_method=None)

    short_mom = _zscore_ts(returns, short_window)
    long_mom = _zscore_ts(returns, long_window)

    vol_5d = returns.rolling(short_window, min_periods=3).std()
    vol_zscore = _zscore_ts(vol_5d, vol_zscore_window)

    high_vol = vol_zscore > vol_threshold
    signal = pd.DataFrame(
        np.where(high_vol, short_mom, long_mom),
        index=prices.index,
        columns=prices.columns,
    )

    return _rank_cross_section(signal)


def dual_horizon_momentum(
    prices: pd.DataFrame,
    short_window: int = 5,
    medium_window: int = 10,
    short_weight: float = 2.0,
    medium_weight: float = 1.0,
) -> pd.DataFrame:
    """
    Dual Horizon Momentum with Directional Volume.

    Blends 5-day and 10-day return z-scores (2:1 weighted).
    Highest RankIC of all 20 factors tested.

    OOS Sharpe: 1.18 | Return: 25.7% | RankIC: 0.024
    """
    returns = prices.pct_change(fill_method=None)

    z_short = _zscore_ts(returns, short_window)
    z_med = _zscore_ts(returns, medium_window)

    signal = short_weight * z_short + medium_weight * z_med
    return _rank_cross_section(signal)


def trend_volume_gate(
    prices: pd.DataFrame,
    volume: pd.DataFrame | None = None,
    fast_ema: int = 20,
    slow_ema: int = 60,
    momentum_window: int = 10,
) -> pd.DataFrame:
    """
    Trend + Volume Gated Momentum.

    Only fires when EMA trend AND volume confirm.
    When volume data is unavailable, uses price-derived proxy.

    OOS Sharpe: 1.11 | Return: 25.1% | MaxDD: -21.8%
    """
    returns = prices.pct_change(fill_method=None)
    ema_fast = prices.ewm(span=fast_ema, adjust=False).mean()
    ema_slow = prices.ewm(span=slow_ema, adjust=False).mean()

    trend_up = ema_fast > ema_slow
    momentum = _zscore_ts(returns, momentum_window)

    if volume is not None and not volume.empty:
        # Use real volume data
        aligned_volume = volume.reindex(
            index=prices.index, columns=prices.columns
        )
        vol_5d = aligned_volume.rolling(5, min_periods=3).mean()
        vol_20d = aligned_volume.rolling(20, min_periods=10).mean()
        volume_confirm = vol_5d > vol_20d
    else:
        # Proxy: absolute return magnitude as volume substitute
        abs_ret = returns.abs()
        vol_5d = abs_ret.rolling(5, min_periods=3).mean()
        vol_20d = abs_ret.rolling(20, min_periods=10).mean()
        volume_confirm = vol_5d > vol_20d

    signal = pd.DataFrame(
        np.where(trend_up & volume_confirm, momentum, 0.0),
        index=prices.index,
        columns=prices.columns,
    )

    return _rank_cross_section(signal)


def vol_price_divergence(
    prices: pd.DataFrame,
    volume: pd.DataFrame | None = None,
    sma_window: int = 20,
    zscore_window: int = 20,
    price_threshold: float = 1.5,
) -> pd.DataFrame:
    """
    Volume-Price Divergence Contrarian.

    When price extends past 20-day SMA without volume confirmation,
    bet on mean reversion.

    OOS Sharpe: 1.03 | Return: 24.3% | MaxDD: -16.9% (best risk-adjusted)
    """
    sma = prices.rolling(sma_window, min_periods=10).mean()
    deviation = prices - sma
    dev_zscore = _zscore_ts(deviation, zscore_window)

    price_extended = dev_zscore.abs() > price_threshold

    if volume is not None and not volume.empty:
        aligned_vol = volume.reindex(index=prices.index, columns=prices.columns)
        vol_zscore = _zscore_ts(aligned_vol, zscore_window)
        low_volume = vol_zscore < 0
    else:
        abs_ret = prices.pct_change(fill_method=None).abs()
        ret_zscore = _zscore_ts(abs_ret, zscore_window)
        low_volume = ret_zscore < 0

    # Mean reversion: bet against the extension when volume is low
    reversion_direction = np.sign(sma.values - prices.values)

    signal = pd.DataFrame(
        np.where(price_extended & low_volume, reversion_direction, 0.0),
        index=prices.index,
        columns=prices.columns,
    )

    return _rank_cross_section(signal)


def vol_regime_adaptive_equity(
    prices: pd.DataFrame,
    short_window: int = 10,
    long_window: int = 40,
    vol_window: int = 30,
    vol_zscore_window: int = 30,
    vol_threshold: float = 0.3,
    min_peers: int = MIN_RANK_PEERS,
) -> pd.DataFrame:
    """
    Vol Regime Adaptive Momentum — equity-tuned variant.

    Equities mean-revert faster and have noisier short-term momentum than
    commodity futures.  Wider windows + lower vol threshold reduce whipsaw.
    Each ticker is computed on its own market calendar
    (``_on_observed_dates``) and ranked only among at least ``min_peers``
    peers on that calendar (``_rank_by_calendar``).
    """
    def raw(returns: pd.DataFrame) -> pd.DataFrame:
        short_mom = _zscore_ts(returns, short_window)
        long_mom = _zscore_ts(returns, long_window)
        vol_5d = returns.rolling(short_window, min_periods=5).std()
        vol_zscore = _zscore_ts(vol_5d, vol_zscore_window)
        high_vol = vol_zscore > vol_threshold
        return pd.DataFrame(
            np.where(high_vol, short_mom, long_mom),
            index=returns.index,
            columns=returns.columns,
        )

    signal, calendars = _on_observed_dates(prices, raw)
    return _rank_by_calendar(signal, calendars, min_peers)


def dual_horizon_equity(
    prices: pd.DataFrame,
    short_window: int = 10,
    medium_window: int = 20,
    short_weight: float = 1.5,
    medium_weight: float = 1.0,
    min_peers: int = MIN_RANK_PEERS,
) -> pd.DataFrame:
    """
    Dual Horizon Momentum — equity-tuned variant.

    Longer lookbacks (10d/20d vs 5d/10d) capture equity momentum without
    the microstructure noise that dominates commodity futures at 5-day.
    Each ticker is computed on its own market calendar
    (``_on_observed_dates``) and ranked only among at least ``min_peers``
    peers on that calendar (``_rank_by_calendar``).
    """
    def raw(returns: pd.DataFrame) -> pd.DataFrame:
        z_short = _zscore_ts(returns, short_window)
        z_med = _zscore_ts(returns, medium_window)
        return short_weight * z_short + medium_weight * z_med

    signal, calendars = _on_observed_dates(prices, raw)
    return _rank_by_calendar(signal, calendars, min_peers)


def compute_all_signals(
    prices: pd.DataFrame,
    volume: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Compute all proven QuantaAlpha signals. Returns dict of signal name → ranked panel."""
    return {
        "vol_regime_adaptive": vol_regime_adaptive_momentum(prices),
        "dual_horizon_momentum": dual_horizon_momentum(prices),
        "trend_volume_gate": trend_volume_gate(prices, volume),
        "vol_price_divergence": vol_price_divergence(prices, volume),
    }


def compute_equity_signals(
    prices: pd.DataFrame,
    volume: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Equity-tuned signal suite. Use for GRID's stock+ETF universe."""
    return {
        "vol_regime_equity": vol_regime_adaptive_equity(prices),
        "dual_horizon_equity": dual_horizon_equity(prices),
        "trend_volume_gate": trend_volume_gate(prices, volume),
        "vol_price_divergence": vol_price_divergence(prices, volume),
    }
