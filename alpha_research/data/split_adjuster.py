"""
Heuristic stock split adjuster for price series that carry NO basis metadata.

When a stock splits N:1, the as-traded price drops ~(1-1/N) in one day.
This module detects such discontinuities from the prices alone and
forward-adjusts ALL pre-split prices so the series is continuous.

Forward adjustment (not backward):
  Pre-split price $200, 10:1 split → adjusted to $20.
  This preserves current-day prices as real market prices,
  and adjusts history to match the current share structure.

Scope (narrowed 2026-11-08, docs/reference/PRICE_PANEL_BASIS.md):

* ``alpha_research.data.panel_builder.build_price_panel`` NO LONGER runs
  this module. The ``{ticker}_full`` features it reads are on the
  ``adj_close`` basis (already split- and dividend-adjusted as of each
  vintage's pull) and the panel re-bases each vintage with the *recorded*
  split events (``store.observations.read_splits``), which is exact and
  idempotent. Running a drop detector on top of that panel is what turned
  raw/adjusted vintage flips and holiday junk prints into compounded
  "splits" (XLP first-to-last ratio 8e201).
* Callers that still have only an unlabelled price series
  (``conviction_scorer``, ``compute_real_drawdown``) keep using
  ``adjust_splits``. For them ``detect_splits`` now refuses a drop that
  does not persist: a candidate whose price climbs back above the
  threshold line within ``confirm_days`` is a data discontinuity, not a
  corporate action, and is skipped with a warning instead of rescaling
  the whole history. A real split's new level persists.

Usage:
  from alpha_research.data.split_adjuster import adjust_splits

  adjusted = adjust_splits(raw_prices)
"""

from __future__ import annotations


import numpy as np
import pandas as pd
from loguru import logger as log

#: Calendar days after a candidate drop in which a price back above the
#: threshold line disproves the split. Bars beyond this window carry no
#: evidence either way (a stock may genuinely double in a year), so a
#: candidate with no later bars inside the window is accepted as before.
DEFAULT_CONFIRM_DAYS = 10
#: Bar-count fallback for series without a DatetimeIndex.
DEFAULT_CONFIRM_BARS = 5


def _reverts(
    prices: pd.Series,
    idx: int,
    pre: float,
    threshold: float,
    confirm_days: int,
    confirm_bars: int,
) -> bool:
    """True if a later bar inside the confirmation window climbs back above
    the threshold line ``pre * (1 + threshold)`` -- the drop did not persist."""
    later = prices.iloc[idx + 1:]
    if later.empty:
        return False
    if isinstance(prices.index, pd.DatetimeIndex):
        cutoff = prices.index[idx] + pd.Timedelta(days=confirm_days)
        later = later[later.index <= cutoff]
    else:
        later = later.iloc[:confirm_bars]
    if later.empty:
        return False
    line = pre * (1.0 + threshold)
    return bool((later.astype(float) > line).any())


def detect_splits(
    prices: pd.Series,
    threshold: float = -0.40,
    *,
    confirm_days: int = DEFAULT_CONFIRM_DAYS,
    confirm_bars: int = DEFAULT_CONFIRM_BARS,
) -> list[dict]:
    """
    Detect stock splits by looking for single-day drops > threshold whose
    new level persists.

    A drop is a split only when the rounded pre/post ratio is >= 2 AND no
    bar within ``confirm_days`` calendar days after it (``confirm_bars``
    bars for a non-datetime index) climbs back above the threshold line.
    A reverting drop is a glitch (a wrong-basis vintage, a holiday junk
    print) and is logged and skipped; adjusting history for it would
    multiply every earlier price by the glitch and never undo it.

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
                        if _reverts(prices, idx, pre, threshold, confirm_days, confirm_bars):
                            log.warning(
                                "split_adjuster {t}: {d} drop {pre:.4g} -> {post:.4g} "
                                "reverts within {w} days; data discontinuity, not a "
                                "split -- history NOT adjusted",
                                t=prices.name or "unknown", d=dt, pre=pre, post=post,
                                w=confirm_days,
                            )
                            continue
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

    Applies adjust_splits() to each ticker column independently. Not used
    by ``panel_builder.build_price_panel`` since 2026-11-08 (see module
    docstring); kept for callers with unlabelled panels.
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
