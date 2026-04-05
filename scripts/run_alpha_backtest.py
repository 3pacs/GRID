"""
Run QuantaAlpha signals against real GRID data and report metrics.

Usage: python3 scripts/run_alpha_backtest.py
"""

from datetime import date, timedelta
import sys
sys.path.insert(0, ".")

from sqlalchemy import create_engine
from config import settings

from loguru import logger as log
from alpha_research.data.panel_builder import build_price_panel, get_available_tickers
from alpha_research.signals.quanta_alpha import compute_all_signals
from alpha_research.validation.metrics import compute_signal_metrics


def main():
    engine = create_engine(settings.DB_URL)

    tickers = get_available_tickers(engine)
    log.info("Available tickers: {}", len(tickers))

    # Use 3-year backtest window
    end = date(2026, 3, 25)
    start = date(2023, 1, 3)  # align with equity trading days
    train_end = date(2025, 3, 25)  # ~2 years train, 1 year test

    log.info("Building price panel: {} to {}...", start, end)
    prices = build_price_panel(engine, start_date=start, end_date=end)
    log.info("Raw panel shape: {} (dates x tickers)", prices.shape)

    if prices.empty:
        log.error("No price data found.")
        return

    # Forward-fill within each ticker (weekends/holidays), then drop sparse
    prices = prices.ffill(limit=5)

    # Drop tickers with < 500 observations (need at least 2 years)
    obs_count = prices.notna().sum()
    good_tickers = obs_count[obs_count >= 500].index.tolist()
    prices = prices[good_tickers].dropna(how="all")
    log.info("Tickers with >= 500 obs: {}", len(good_tickers))
    log.info("Final panel: {}", prices.shape)

    returns = prices.pct_change()

    # Compute all signals
    log.info("Computing signals...")
    signals = compute_all_signals(prices)

    # Split into train/test
    train_mask = prices.index <= str(train_end)
    test_mask = prices.index > str(train_end)

    log.info("Train: {} days, Test: {} days", train_mask.sum(), test_mask.sum())

    # Evaluate each signal
    log.info("=" * 80)
    log.info("{:<30s} {:<8s} {:>8s} {:>8s} {:>12s} {:>10s} {:>10s} {:>8s}",
             "Signal", "Split", "RankIC", "ICIR", "Sharpe(net)", "Return", "MaxDD", "Pass1.4")
    log.info("=" * 80)

    for name, signal in signals.items():
        for split_name, mask in [("TRAIN", train_mask), ("TEST", test_mask)]:
            sig_split = signal.loc[mask]
            ret_split = returns.loc[mask]

            if len(sig_split) < 60:
                log.info("{:<30s} {:<8s} insufficient data", name, split_name)
                continue

            metrics = compute_signal_metrics(sig_split, ret_split, top_n=5, cost_bps=10)

            pass_str = "YES" if metrics["passes_threshold"] else "no"
            log.info(
                "{:<30s} {:<8s} {:>8.4f} {:>8.4f} {:>12.4f} {:>9.2%} {:>9.2%} {:>8s}",
                name, split_name,
                metrics['mean_rank_ic'], metrics['rank_icir'],
                metrics['sharpe_net'], metrics['annualized_return'],
                metrics['max_drawdown'], pass_str,
            )

    log.info("=" * 80)
    log.info("Threshold: val net Sharpe > 1.4 required for OOS confidence")
    log.info("Budget: 64% RankIC shrinkage from train to test is normal")


if __name__ == "__main__":
    main()
