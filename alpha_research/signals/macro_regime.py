"""
Macro regime signals that enhance cross-sectional alpha.

These signals use market-wide data (VIX, credit spreads, yield curve)
to create conditional features that the ensemble can use to time
when momentum vs mean-reversion signals are more reliable.

All signals return DataFrames aligned to the price panel index.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _broadcast_to_panel(
    series: pd.Series,
    panel_index: pd.DatetimeIndex,
    panel_columns: pd.Index,
) -> pd.DataFrame:
    """Broadcast a time series across all tickers in the panel."""
    # Deduplicate: keep last value per date
    deduped = series[~series.index.duplicated(keep="last")]
    aligned = deduped.reindex(panel_index).ffill(limit=5)
    return pd.DataFrame(
        np.tile(aligned.values[:, None], (1, len(panel_columns))),
        index=panel_index,
        columns=panel_columns,
    )


def _zscore_rolling(s: pd.Series, window: int) -> pd.Series:
    mu = s.rolling(window, min_periods=max(window // 2, 5)).mean()
    sigma = s.rolling(window, min_periods=max(window // 2, 5)).std()
    return (s - mu) / (sigma + 1e-8)


def vix_regime_signal(
    vix: pd.Series,
    panel_index: pd.DatetimeIndex,
    panel_columns: pd.Index,
    zscore_window: int = 60,
) -> pd.DataFrame:
    """
    VIX regime: z-scored VIX level.

    Low VIX → momentum works. High VIX → mean reversion works.
    Broadcast to full panel so ensemble can interact with per-ticker signals.
    """
    vix_z = _zscore_rolling(vix, zscore_window)
    return _broadcast_to_panel(vix_z, panel_index, panel_columns)


def vix_momentum_signal(
    vix: pd.Series,
    panel_index: pd.DatetimeIndex,
    panel_columns: pd.Index,
    fast: int = 5,
    slow: int = 20,
) -> pd.DataFrame:
    """
    VIX momentum: rate of change in fear.

    Rising VIX → risk-off. Falling VIX → risk-on.
    """
    vix_fast = vix.rolling(fast, min_periods=3).mean()
    vix_slow = vix.rolling(slow, min_periods=10).mean()
    vix_mom = (vix_fast - vix_slow) / (vix_slow + 1e-8)
    vix_mom_z = _zscore_rolling(vix_mom, 60)
    return _broadcast_to_panel(vix_mom_z, panel_index, panel_columns)


def credit_spread_signal(
    hy_spread: pd.Series,
    panel_index: pd.DatetimeIndex,
    panel_columns: pd.Index,
    zscore_window: int = 60,
) -> pd.DataFrame:
    """
    Credit spread regime: z-scored HY OAS spread.

    Tight spreads → risk-on, momentum favored.
    Wide/widening → stress, defensive positioning.
    """
    spread_z = _zscore_rolling(hy_spread, zscore_window)
    return _broadcast_to_panel(spread_z, panel_index, panel_columns)


def credit_momentum_signal(
    hy_spread: pd.Series,
    panel_index: pd.DatetimeIndex,
    panel_columns: pd.Index,
    window: int = 20,
) -> pd.DataFrame:
    """
    Credit spread momentum: 20-day change in HY spread.

    Tightening → bullish signal. Widening → bearish.
    """
    spread_chg = hy_spread.pct_change(window)
    spread_chg_z = _zscore_rolling(spread_chg, 60)
    return _broadcast_to_panel(spread_chg_z, panel_index, panel_columns)


def yield_curve_signal(
    yc_2s10s: pd.Series,
    panel_index: pd.DatetimeIndex,
    panel_columns: pd.Index,
    zscore_window: int = 120,
) -> pd.DataFrame:
    """
    Yield curve slope signal.

    Steepening → economic optimism → cyclicals outperform.
    Flattening/inversion → defensive positioning.
    """
    yc_z = _zscore_rolling(yc_2s10s, zscore_window)
    return _broadcast_to_panel(yc_z, panel_index, panel_columns)


def financial_stress_signal(
    stress: pd.Series,
    panel_index: pd.DatetimeIndex,
    panel_columns: pd.Index,
    zscore_window: int = 60,
) -> pd.DataFrame:
    """
    OFR Financial Stress Index signal.

    Low stress → momentum works. High stress → mean reversion.
    """
    stress_z = _zscore_rolling(stress, zscore_window)
    return _broadcast_to_panel(stress_z, panel_index, panel_columns)


def skew_signal(
    skew: pd.Series,
    panel_index: pd.DatetimeIndex,
    panel_columns: pd.Index,
    zscore_window: int = 60,
) -> pd.DataFrame:
    """
    SKEW index signal: tail risk pricing.

    High SKEW → hedging demand → potential reversal.
    Low SKEW → complacency.
    """
    skew_z = _zscore_rolling(skew, zscore_window)
    return _broadcast_to_panel(skew_z, panel_index, panel_columns)


def sector_dispersion_signal(
    prices: pd.DataFrame,
    window: int = 20,
) -> pd.DataFrame:
    """
    Cross-sectional return dispersion.

    High dispersion → alpha opportunity (stock-picking works).
    Low dispersion → correlation regime (macro-driven).
    Broadcast same value to all tickers.
    """
    returns = prices.pct_change(fill_method=None)

    # Cross-sectional std of returns per day
    disp = returns.std(axis=1)
    disp_z = _zscore_rolling(disp, 60)

    return pd.DataFrame(
        np.tile(disp_z.values[:, None], (1, len(prices.columns))),
        index=prices.index,
        columns=prices.columns,
    )


def relative_strength_signal(
    prices: pd.DataFrame,
    lookback: int = 20,
) -> pd.DataFrame:
    """
    Relative strength: each ticker's return rank vs peers.

    Pure cross-sectional momentum — winners keep winning.
    """
    returns = prices.pct_change(lookback, fill_method=None)
    return returns.rank(axis=1, pct=True)
