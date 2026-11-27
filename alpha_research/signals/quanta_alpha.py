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


def _returns_on_own_dates(own: pd.DataFrame) -> pd.DataFrame:
    """Simple returns between a ticker's consecutive observations.

    A return is kept only when no NYSE session lies strictly between the two
    observations: Friday to Monday and Wednesday to Friday over Thanksgiving
    are one step, a missing session's data is not bridged (NaN, as before).
    """
    from ingestion.market_calendar import is_market_open

    returns = own.pct_change(fill_method=None)
    dates = [ts.date() for ts in own.index]
    for i in range(1, len(dates)):
        day = dates[i - 1] + timedelta(days=1)
        while day < dates[i]:
            if is_market_open(day):
                returns.iloc[i] = np.nan
                break
            day += timedelta(days=1)
    return returns


def _on_observed_dates(
    prices: pd.DataFrame,
    raw_fn: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    """Apply a returns transform to each ticker on that ticker's own dates.

    The price panel's index is the union of every ticker's observation dates,
    so it carries weekend and exchange-holiday rows for 24-hour tickers
    (crypto, GOLD) on which every equity is NaN. Computing an equity's returns
    and rolling windows on that index measures the first session after a
    closed day against a NaN: the value is NaN and the ticker is never ranked,
    so the equity momentum factors went silent on every Monday and every
    post-holiday session. Here each column's returns are computed on its own
    non-NaN rows (``_returns_on_own_dates``), ``raw_fn`` runs on them (its
    windows count that ticker's own observations), and the result is placed
    back on the shared index.
    """
    out: dict[str, pd.Series] = {}
    for col in prices.columns:
        own = prices[[col]].dropna()
        if own.empty:
            out[col] = pd.Series(np.nan, index=prices.index, dtype=float)
            continue
        out[col] = raw_fn(_returns_on_own_dates(own))[col].reindex(prices.index)
    return pd.DataFrame(out, index=prices.index, columns=prices.columns)


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
) -> pd.DataFrame:
    """
    Vol Regime Adaptive Momentum — equity-tuned variant.

    Equities mean-revert faster and have noisier short-term momentum than
    commodity futures.  Wider windows + lower vol threshold reduce whipsaw.
    Each ticker is computed on its own observed dates (``_on_observed_dates``)
    so a weekend or holiday row from a 24-hour ticker cannot blank it.
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

    return _rank_cross_section(_on_observed_dates(prices, raw))


def dual_horizon_equity(
    prices: pd.DataFrame,
    short_window: int = 10,
    medium_window: int = 20,
    short_weight: float = 1.5,
    medium_weight: float = 1.0,
) -> pd.DataFrame:
    """
    Dual Horizon Momentum — equity-tuned variant.

    Longer lookbacks (10d/20d vs 5d/10d) capture equity momentum without
    the microstructure noise that dominates commodity futures at 5-day.
    Each ticker is computed on its own observed dates (``_on_observed_dates``)
    so a weekend or holiday row from a 24-hour ticker cannot blank it.
    """
    def raw(returns: pd.DataFrame) -> pd.DataFrame:
        z_short = _zscore_ts(returns, short_window)
        z_med = _zscore_ts(returns, medium_window)
        return short_weight * z_short + medium_weight * z_med

    return _rank_cross_section(_on_observed_dates(prices, raw))


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
