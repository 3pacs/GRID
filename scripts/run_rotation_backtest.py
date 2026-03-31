#!/usr/bin/env python3
"""
Walk-forward backtest for Adaptive Rotation Strategy on GRID data.

Iterates weekly over 3 years of history, recording weights, regimes,
and computing portfolio returns with transaction costs.
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger as log

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine
from alpha_research.data.panel_builder import build_price_panel
from alpha_research.strategies.adaptive_rotation import (
    PositionState,
    run_rotation,
)


REBALANCE_DAYS = 5  # weekly rebalance
COST_BPS = 10.0     # 10 bps round-trip
LOOKBACK_YEARS = 3


def main() -> None:
    engine = get_engine()

    end_date = date.today()
    start_date = end_date - timedelta(days=LOOKBACK_YEARS * 365)

    # Pre-fetch full price panel for return calculation
    log.info("Loading price panel {s} → {e}...", s=start_date, e=end_date)
    full_prices = build_price_panel(engine, start_date=start_date, end_date=end_date)
    if full_prices.empty:
        log.error("No price data available")
        return

    full_prices = full_prices.ffill(limit=5)
    daily_returns = full_prices.pct_change()

    # Generate rebalance dates (every REBALANCE_DAYS trading days)
    trading_dates = full_prices.index.tolist()
    # Skip first 6 months for warm-up
    warmup_end = start_date + timedelta(days=180)
    rebalance_dates = [
        d for i, d in enumerate(trading_dates)
        if d.date() >= warmup_end and i % REBALANCE_DAYS == 0
    ]

    log.info("{n} rebalance dates from {s} to {e}",
             n=len(rebalance_dates), s=rebalance_dates[0].date(), e=rebalance_dates[-1].date())

    # Walk-forward
    positions: dict[str, PositionState] = {}
    current_weights: dict[str, float] = {}
    portfolio_returns: list[float] = []
    portfolio_dates: list = []
    regime_log: list[dict] = []
    turnover_log: list[float] = []

    rebalance_set = set(rebalance_dates)
    active_dates = [d for d in trading_dates if d >= rebalance_dates[0]]

    for dt in active_dates:
        if dt in rebalance_set:
            try:
                result = run_rotation(engine, as_of_date=dt.date(), positions=positions)
                old_weights = current_weights.copy()
                current_weights = result.weights

                # Compute turnover
                all_tickers = set(old_weights) | set(current_weights)
                turnover = sum(
                    abs(current_weights.get(t, 0) - old_weights.get(t, 0))
                    for t in all_tickers
                ) / 2.0
                turnover_log.append(turnover)

                # Update position states for stop tracking
                for ticker, w in current_weights.items():
                    if ticker in full_prices.columns:
                        price = float(full_prices.loc[dt, ticker]) if dt in full_prices.index else 0
                        if price > 0:
                            if ticker in positions:
                                positions[ticker].update_peak(price, dt.date())
                            else:
                                positions[ticker] = PositionState(
                                    ticker=ticker,
                                    entry_price=price,
                                    entry_date=dt.date(),
                                    peak_price=price,
                                    peak_date=dt.date(),
                                )

                # Remove stopped tickers from positions
                for t in result.stopped_tickers:
                    positions.pop(t, None)

                regime_log.append({
                    "date": dt.date(),
                    "regime": result.regime.label,
                    "spy_trend": result.regime.spy_trend,
                    "vix_zscore": result.regime.vix_zscore,
                    "active_groups": result.active_groups,
                    "weights": current_weights.copy(),
                    "turnover": turnover,
                })
            except Exception as e:
                log.warning("Rotation failed on {d}: {e}", d=dt.date(), e=str(e))

        # Daily portfolio return
        day_ret = 0.0
        if dt in daily_returns.index:
            for ticker, w in current_weights.items():
                if ticker in daily_returns.columns:
                    r = daily_returns.loc[dt, ticker]
                    if not np.isnan(r):
                        day_ret += w * r

        # Subtract transaction costs on rebalance days
        if dt in rebalance_set and turnover_log:
            day_ret -= turnover_log[-1] * (COST_BPS / 10_000)

        portfolio_returns.append(day_ret)
        portfolio_dates.append(dt)

    # Compute metrics
    ret_series = pd.Series(portfolio_returns, index=portfolio_dates)
    cumulative = (1 + ret_series).cumprod()

    total_return = float(cumulative.iloc[-1] - 1) * 100
    ann_return = float((cumulative.iloc[-1]) ** (252 / len(ret_series)) - 1) * 100
    ann_vol = float(ret_series.std() * np.sqrt(252)) * 100
    sharpe = ann_return / ann_vol if ann_vol > 0 else 0

    # Max drawdown
    running_max = cumulative.cummax()
    drawdown = (cumulative - running_max) / running_max
    max_dd = float(drawdown.min()) * 100

    # Calmar
    calmar = ann_return / abs(max_dd) if max_dd != 0 else 0

    # Average turnover
    avg_turnover = float(np.mean(turnover_log)) if turnover_log else 0

    # Regime distribution
    regime_counts: dict[str, int] = {}
    for r in regime_log:
        label = r["regime"]
        regime_counts[label] = regime_counts.get(label, 0) + 1

    # SPY benchmark
    spy_ret = daily_returns.get("SPY")
    if spy_ret is not None:
        spy_subset = spy_ret.reindex(portfolio_dates).fillna(0)
        spy_cum = (1 + spy_subset).cumprod()
        spy_total = float(spy_cum.iloc[-1] - 1) * 100
        spy_ann = float(spy_cum.iloc[-1] ** (252 / len(spy_subset)) - 1) * 100
    else:
        spy_total = spy_ann = 0.0

    log.info("=" * 60)
    log.info("ADAPTIVE ROTATION WALK-FORWARD BACKTEST")
    log.info("=" * 60)
    log.info("Period: {s} → {e} ({n} trading days)",
             s=portfolio_dates[0].date(), e=portfolio_dates[-1].date(), n=len(ret_series))
    log.info("")
    log.info("PORTFOLIO:")
    log.info("  Total Return:    {r:+.1f}%", r=total_return)
    log.info("  Ann. Return:     {r:+.1f}%", r=ann_return)
    log.info("  Ann. Volatility: {v:.1f}%", v=ann_vol)
    log.info("  Sharpe Ratio:    {s:.2f}", s=sharpe)
    log.info("  Max Drawdown:    {d:.1f}%", d=max_dd)
    log.info("  Calmar Ratio:    {c:.2f}", c=calmar)
    log.info("  Avg Turnover:    {t:.1f}%", t=avg_turnover * 100)
    log.info("")
    log.info("SPY BENCHMARK:")
    log.info("  Total Return:    {r:+.1f}%", r=spy_total)
    log.info("  Ann. Return:     {r:+.1f}%", r=spy_ann)
    log.info("")
    log.info("REGIME DISTRIBUTION:")
    for label, count in sorted(regime_counts.items()):
        log.info("  {l}: {c} rebalances", l=label, c=count)
    log.info("")
    log.info("{n} rebalance events, {s} stopped tickers total",
             n=len(regime_log),
             s=sum(len(r.get("stopped_tickers", [])) for r in regime_log if isinstance(r.get("stopped_tickers"), list)))


if __name__ == "__main__":
    main()
