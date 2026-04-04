"""
Run QuantaAlpha signals against real GRID data and report metrics.

Usage: python3 scripts/run_alpha_backtest.py
"""

from datetime import date, timedelta
import sys
sys.path.insert(0, ".")

from sqlalchemy import create_engine
from config import settings

from alpha_research.data.panel_builder import build_price_panel, get_available_tickers
from alpha_research.signals.quanta_alpha import compute_all_signals
from alpha_research.validation.metrics import compute_signal_metrics


def main():
    engine = create_engine(settings.DB_URL)

    tickers = get_available_tickers(engine)
    print(f"Available tickers: {len(tickers)}")

    # Use 3-year backtest window
    end = date(2026, 3, 25)
    start = date(2023, 1, 3)  # align with equity trading days
    train_end = date(2025, 3, 25)  # ~2 years train, 1 year test

    print(f"Building price panel: {start} to {end}...")
    prices = build_price_panel(engine, start_date=start, end_date=end)
    print(f"Raw panel shape: {prices.shape} (dates x tickers)")

    if prices.empty:
        print("ERROR: No price data found.")
        return

    # Forward-fill within each ticker (weekends/holidays), then drop sparse
    prices = prices.ffill(limit=5)

    # Drop tickers with < 500 observations (need at least 2 years)
    obs_count = prices.notna().sum()
    good_tickers = obs_count[obs_count >= 500].index.tolist()
    prices = prices[good_tickers].dropna(how="all")
    print(f"Tickers with >= 500 obs: {len(good_tickers)}")
    print(f"Final panel: {prices.shape}")

    returns = prices.pct_change()

    # Compute all signals
    print("\nComputing signals...")
    signals = compute_all_signals(prices)

    # Split into train/test
    train_mask = prices.index <= str(train_end)
    test_mask = prices.index > str(train_end)

    print(f"Train: {train_mask.sum()} days, Test: {test_mask.sum()} days")

    # Evaluate each signal
    print("\n" + "=" * 80)
    print(f"{'Signal':<30s} {'Split':<8s} {'RankIC':>8s} {'ICIR':>8s} "
          f"{'Sharpe(net)':>12s} {'Return':>10s} {'MaxDD':>10s} {'Pass1.4':>8s}")
    print("=" * 80)

    for name, signal in signals.items():
        for split_name, mask in [("TRAIN", train_mask), ("TEST", test_mask)]:
            sig_split = signal.loc[mask]
            ret_split = returns.loc[mask]

            if len(sig_split) < 60:
                print(f"{name:<30s} {split_name:<8s} insufficient data")
                continue

            metrics = compute_signal_metrics(sig_split, ret_split, top_n=5, cost_bps=10)

            pass_str = "YES" if metrics["passes_threshold"] else "no"
            print(
                f"{name:<30s} {split_name:<8s} "
                f"{metrics['mean_rank_ic']:>8.4f} "
                f"{metrics['rank_icir']:>8.4f} "
                f"{metrics['sharpe_net']:>12.4f} "
                f"{metrics['annualized_return']:>9.2%} "
                f"{metrics['max_drawdown']:>9.2%} "
                f"{pass_str:>8s}"
            )

    print("\n" + "=" * 80)
    print("Threshold: val net Sharpe > 1.4 required for OOS confidence")
    print("Budget: 64% RankIC shrinkage from train to test is normal")


if __name__ == "__main__":
    main()
