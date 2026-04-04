"""
Universal stock split adjuster for GRID price data.

This is a TOP-LEVEL data layer concern. Every price series that flows
through GRID must be split-adjusted before any downstream consumer
touches it — signals, backtests, models, scanners, everything.

GRID's resolved_series stores raw (unadjusted) prices from multiple
sources. When a stock splits N:1, the price drops ~(1-1/N) in one day.
This module detects those discontinuities and forward-adjusts ALL
pre-split prices so the series is continuous.

Forward adjustment (not backward):
  Pre-split price $200, 10:1 split → adjusted to $20.
  This preserves current-day prices as real market prices,
  and adjusts history to match the current share structure.

Usage:
  from alpha_research.data.split_adjuster import adjust_splits

  # Single series
  adjusted = adjust_splits(raw_prices)

  # Full panel (called automatically by panel_builder)
  panel = build_price_panel(engine)  # already adjusted
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
from loguru import logger as log


def detect_splits(
    prices: pd.Series,
    threshold: float = -0.40,
) -> list[dict]:
    """
    Detect stock splits by looking for single-day drops > threshold.

    Returns list of {date, ratio, pre_price, post_price} sorted
    chronologically.
    """
    if len(prices) < 2:
        return []

    pct_change = prices.pct_change()
    splits = []

    for dt, chg in pct_change.items():
        if pd.notna(chg) and chg < threshold:
            idx = prices.index.get_loc(dt)
            if isinstance(idx, slice):
                idx = idx.start
            prev_idx = idx - 1
            if prev_idx >= 0:
                pre = float(prices.iloc[prev_idx])
                post = float(prices.iloc[idx])
                if post > 0:
                    ratio = round(pre / post)
                    if ratio >= 2:
                        splits.append({
                            "date": dt,
                            "ratio": ratio,
                            "pre_price": pre,
                            "post_price": post,
                            "adjustment_factor": post / pre,
                        })

    return splits


def adjust_splits(
    prices: pd.Series,
    threshold: float = -0.40,
) -> pd.Series:
    """
    Forward-adjust a price series for stock splits.

    Detects split dates and multiplies all pre-split prices by the
    adjustment factor so the series is continuous. Handles multiple
    sequential splits (e.g., TSLA 5:1 in 2020 then 3:1 in 2022).

    Returns a new Series with adjusted prices. Current-day price is
    unchanged — history is scaled down to match.
    """
    if len(prices) < 2:
        return prices.copy()

    splits = detect_splits(prices, threshold)
    if not splits:
        return prices.copy()

    adjusted = prices.copy().astype(float)

    # Process splits in reverse chronological order so adjustments compound
    for split in reversed(splits):
        split_date = split["date"]
        factor = split["adjustment_factor"]

        # Adjust all prices BEFORE the split date
        mask = adjusted.index < split_date
        adjusted.loc[mask] = adjusted.loc[mask] * factor

    n_splits = len(splits)
    ticker = prices.name or "unknown"
    total_adj = np.prod([s["adjustment_factor"] for s in splits])
    log.debug(
        "Split-adjusted {t}: {n} split(s), total factor {f:.6f}",
        t=ticker, n=n_splits, f=total_adj,
    )

    return adjusted


def adjust_panel(
    panel: pd.DataFrame,
    threshold: float = -0.40,
) -> pd.DataFrame:
    """
    Split-adjust every column in a price panel.

    This is the main entry point for panel_builder integration.
    Applies adjust_splits() to each ticker column independently.
    """
    if panel.empty:
        return panel.copy()

    adjusted = panel.copy()
    for col in adjusted.columns:
        series = adjusted[col].dropna()
        if len(series) < 2:
            continue
        series.name = col
        adj = adjust_splits(series, threshold)
        adjusted[col] = adj

    return adjusted


def detect_panel_splits(
    panel: pd.DataFrame,
    threshold: float = -0.40,
) -> dict[str, list[dict]]:
    """
    Detect splits across all tickers in a panel.

    Returns {ticker: [split_info, ...]} for tickers with splits.
    Useful for auditing data quality.
    """
    result = {}
    for col in panel.columns:
        series = panel[col].dropna()
        if len(series) < 2:
            continue
        splits = detect_splits(series, threshold)
        if splits:
            result[col] = splits
    return result


# Legacy API — keep for backward compat with bottom_detector_monitor
def get_post_split_series(
    prices: pd.Series,
    threshold: float = -0.40,
) -> pd.Series:
    """Return split-adjusted full series (legacy name, now adjusts instead of truncating)."""
    return adjust_splits(prices, threshold)


def compute_real_drawdown(prices: pd.Series) -> dict:
    """Compute drawdown metrics on split-adjusted data."""
    clean = adjust_splits(prices)
    raw_splits = detect_splits(prices)

    if clean.empty:
        return {"error": "no data"}

    current = float(clean.iloc[-1])
    current_date = clean.index[-1]
    ath = float(clean.max())
    ath_date = clean.idxmax()
    atl = float(clean.min())
    atl_date = clean.idxmin()
    drawdown = (current / ath - 1) * 100 if ath > 0 else 0

    mom_5d = ((current / float(clean.iloc[-min(5, len(clean))])) - 1) * 100 if len(clean) > 5 else 0
    mom_30d = ((current / float(clean.iloc[-min(30, len(clean))])) - 1) * 100 if len(clean) > 30 else 0
    mom_90d = ((current / float(clean.iloc[-min(90, len(clean))])) - 1) * 100 if len(clean) > 90 else 0

    return {
        "current": current,
        "current_date": current_date,
        "ath": ath,
        "ath_date": ath_date,
        "atl": atl,
        "atl_date": atl_date,
        "drawdown_pct": drawdown,
        "mom_5d": mom_5d,
        "mom_30d": mom_30d,
        "mom_90d": mom_90d,
        "has_split": len(raw_splits) > 0,
        "last_split_date": raw_splits[-1]["date"] if raw_splits else None,
        "last_split_ratio": raw_splits[-1]["ratio"] if raw_splits else None,
        "n_points": len(clean),
    }
