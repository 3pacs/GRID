#!/usr/bin/env python3
"""
Run LightGBM ensemble on all QuantaAlpha factor signals.

Builds signal panels from GRID's resolved_series, computes forward returns,
trains LightGBM, and reports train/val Sharpe + feature importance.
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
from alpha_research.data.panel_builder import build_price_panel, build_volume_panel
from alpha_research.signals.quanta_alpha import compute_equity_signals
from alpha_research.ensemble import train_ensemble
from alpha_research.validation.metrics import compute_signal_metrics


def main() -> None:
    engine = get_engine()

    end_date = date.today()
    start_date = end_date - timedelta(days=365 * 3)

    log.info("Loading price panel {s} → {e}...", s=start_date, e=end_date)
    prices = build_price_panel(engine, start_date=start_date, end_date=end_date)
    if prices.empty:
        log.error("No price data available")
        return

    # Need at least 10 tickers with decent coverage
    coverage = prices.notna().sum()
    good_tickers = coverage[coverage > len(prices) * 0.5].index.tolist()
    if len(good_tickers) < 10:
        log.warning("Only {n} tickers with >50% coverage, using all {t}",
                     n=len(good_tickers), t=len(prices.columns))
        good_tickers = prices.columns.tolist()

    prices = prices[good_tickers].ffill(limit=5)
    log.info("Price panel: {d} dates × {t} tickers", d=len(prices), t=len(prices.columns))

    # Volume panel (may be sparse)
    volume = build_volume_panel(engine, start_date=start_date, end_date=end_date)
    if not volume.empty:
        volume = volume.reindex(columns=good_tickers).ffill(limit=5)
        log.info("Volume panel: {d} dates × {t} tickers", d=len(volume), t=len(volume.columns))
    else:
        log.info("No volume data — using price-derived proxy")
        volume = None

    # Compute all equity-tuned signals
    log.info("Computing equity signals...")
    signals = compute_equity_signals(prices, volume)
    log.info("Signals computed: {s}", s=list(signals.keys()))

    # Forward returns (5-day)
    forward_returns = prices.pct_change(periods=5, fill_method=None).shift(-5)

    # Individual signal metrics
    log.info("")
    log.info("=" * 60)
    log.info("INDIVIDUAL SIGNAL METRICS")
    log.info("=" * 60)

    for name, sig in signals.items():
        metrics = compute_signal_metrics(sig, forward_returns)
        log.info(
            "{n:30s} | Sharpe {s:.2f} | RankIC {ic:.4f} | MaxDD {dd:.1%} | Calmar {c:.2f} | n={nd}",
            n=name, s=metrics["sharpe_net"], ic=metrics["mean_rank_ic"],
            dd=metrics["max_drawdown"], c=metrics["calmar"], nd=metrics["n_days"],
        )

    # Train ensemble
    log.info("")
    log.info("=" * 60)
    log.info("LIGHTGBM ENSEMBLE")
    log.info("=" * 60)

    try:
        result = train_ensemble(
            signal_panels=signals,
            forward_returns=forward_returns,
            train_frac=0.7,
            n_rounds=300,
            early_stopping=30,
        )

        log.info("")
        log.info("RESULTS:")
        log.info("  Train Sharpe: {s:.3f}", s=result.train_sharpe)
        log.info("  Val Sharpe:   {s:.3f}", s=result.val_sharpe)
        log.info("  N features:   {n}", n=result.n_features)
        log.info("  N train:      {n}", n=result.n_train_samples)
        log.info("")
        log.info("FEATURE IMPORTANCE:")
        for feat, imp in sorted(result.feature_importance.items(), key=lambda x: -x[1]):
            bar = "█" * int(imp * 50)
            log.info("  {f:30s} {i:.1%}  {b}", f=feat, i=imp, b=bar)

    except Exception as e:
        log.error("Ensemble training failed: {e}", e=str(e))
        raise


if __name__ == "__main__":
    main()
