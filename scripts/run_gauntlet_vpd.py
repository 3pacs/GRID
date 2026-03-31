#!/usr/bin/env python3
"""
Run the full 5-test False Discovery Gauntlet on vol_price_divergence.

Tests: permutation, deflated Sharpe, subsample stability, decay, CV consistency.
"""

from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

from loguru import logger as log

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine
from alpha_research.data.panel_builder import build_price_panel, build_returns_panel
from alpha_research.signals.quanta_alpha import vol_price_divergence
from alpha_research.validation.gauntlet import run_gauntlet
from alpha_research.validation.metrics import compute_signal_metrics


def main() -> None:
    engine = get_engine()
    end_date = date.today()
    start_date = end_date - timedelta(days=365 * 3)

    log.info("Loading price panel {s} → {e}...", s=start_date, e=end_date)
    prices = build_price_panel(engine, start_date=start_date, end_date=end_date)
    if prices.empty:
        log.error("No price data")
        return

    prices = prices.ffill(limit=5)
    log.info("Panel: {r} rows × {c} tickers", r=len(prices), c=len(prices.columns))

    # Compute signal
    log.info("Computing vol_price_divergence signal...")
    signal = vol_price_divergence(prices)

    # Forward returns (1-day)
    forward_returns = prices.pct_change().shift(-1)

    # Drop rows with insufficient data
    common_idx = signal.dropna(how="all").index.intersection(
        forward_returns.dropna(how="all").index
    )
    signal = signal.loc[common_idx]
    forward_returns = forward_returns.loc[common_idx]

    log.info("Signal computed: {n} dates, {t} tickers", n=len(signal), t=len(signal.columns))

    # Quick metrics first
    log.info("")
    log.info("QUICK METRICS:")
    metrics = compute_signal_metrics(signal, forward_returns)
    log.info("  Mean RankIC:     {ic:.4f}", ic=metrics["mean_rank_ic"])
    log.info("  ICIR:            {ir:.4f}", ir=metrics["rank_icir"])
    log.info("  Net Sharpe:      {s:.4f}", s=metrics["sharpe_net"])
    log.info("  Ann. Return:     {r:.4f}", r=metrics["annualized_return"])
    log.info("  Avg Turnover:    {t:.4f}", t=metrics["turnover"])
    log.info("")

    # Run gauntlet (this takes a while — 1000 permutations)
    log.info("Running 5-test False Discovery Gauntlet (1000 permutations)...")
    log.info("This will take 2-5 minutes...")
    result = run_gauntlet(
        signal,
        forward_returns,
        n_models_tested=4,  # we tested 4 signals
        top_n=5,
        cost_bps=10.0,
        n_permutations=1000,
        n_subsample_splits=20,
    )

    log.info("")
    log.info("=" * 60)
    log.info("GAUNTLET RESULT: {v}", v=result.verdict)
    log.info("=" * 60)
    log.info("")
    log.info("1. PERMUTATION TEST:")
    log.info("   p-value: {p:.4f} (threshold: < 0.05)", p=result.permutation_p)
    log.info("   Passed:  {ok}", ok=result.permutation_passed)
    log.info("")
    log.info("2. DEFLATED SHARPE RATIO:")
    log.info("   Observed Sharpe:  {s:.4f}", s=result.observed_sharpe)
    log.info("   Threshold:        {t:.4f}", t=result.deflated_threshold)
    log.info("   Passed:           {ok}", ok=result.deflated_sharpe_passed)
    log.info("")
    log.info("3. SUBSAMPLE STABILITY:")
    log.info("   Stability: {s:.1f}% (threshold: > 50%)", s=result.subsample_stability * 100)
    log.info("   Passed:    {ok}", ok=result.subsample_passed)
    log.info("")
    log.info("4. DECAY ANALYSIS:")
    log.info("   Monotonic: {ok}", ok=result.decay_monotonic)
    if "decay_ics" in result.details:
        for h, ic in result.details["decay_ics"].items():
            log.info("   Horizon {h}d: IC={ic:.4f}", h=h, ic=ic)
    log.info("")
    log.info("5. CV CONSISTENCY:")
    log.info("   Consistency: {c:.1f}% (threshold: >= 75%)", c=result.cv_consistency * 100)
    log.info("   Passed:      {ok}", ok=result.cv_passed)
    log.info("")
    log.info("Net Sharpe: {s:.4f} (threshold > 1.4 for production: {ok})",
             s=result.details.get("net_sharpe", 0),
             ok=result.details.get("passes_net_sharpe_threshold", False))
    log.info("")
    log.info("VERDICT: {v}", v=result.verdict)


if __name__ == "__main__":
    main()
