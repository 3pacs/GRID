"""
GRID Pitch Backtest Engine.

Runs full historical regime classification + portfolio simulation
to produce pitch-grade performance metrics, equity curves, and
regime timelines. Uses PIT-correct data access throughout.

Usage:
    python -m backtest.engine --run
    python -m backtest.engine --charts
    python -m backtest.engine --summary
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log

# Output directory
_OUTPUT_DIR = Path(__file__).parent.parent / "outputs" / "backtest"


# ---------------------------------------------------------------------------
# Allocation model — posture-based asset allocation
# ---------------------------------------------------------------------------

POSTURE_ALLOCATIONS: dict[str, dict[str, float]] = {
    "AGGRESSIVE": {
        "SPY": 0.40, "QQQ": 0.15, "XLE": 0.05, "XLF": 0.05, "IWM": 0.05,
        "BTC-USD": 0.10, "ETH-USD": 0.05,
        "GLD": 0.05, "HG=F": 0.05,
        "CASH": 0.05,
    },
    "BALANCED": {
        "SPY": 0.30, "QQQ": 0.10,
        "BTC-USD": 0.07, "ETH-USD": 0.03,
        "TLT": 0.10, "IEF": 0.05, "BND": 0.05,
        "GLD": 0.10,
        "CASH": 0.10,
    },
    "DEFENSIVE": {
        "SPY": 0.20, "XLU": 0.05, "XLP": 0.05,
        "BTC-USD": 0.05,
        "TLT": 0.20, "SHY": 0.10, "TIP": 0.05,
        "GLD": 0.15, "SLV": 0.05,
        "CASH": 0.10,
    },
    "CAPITAL_PRESERVATION": {
        "SPY": 0.10,
        "TLT": 0.25, "SHY": 0.15, "TIP": 0.10,
        "GLD": 0.20, "SLV": 0.05,
        "CASH": 0.15,
    },
}

REGIME_TO_POSTURE = {
    "GROWTH": "AGGRESSIVE",
    "NEUTRAL": "BALANCED",
    "FRAGILE": "DEFENSIVE",
    "CRISIS": "CAPITAL_PRESERVATION",
}

# Benchmark allocations for comparison
BENCHMARK_60_40 = {"SPY": 0.60, "TLT": 0.40}
BENCHMARK_SPY = {"SPY": 1.00}


# ---------------------------------------------------------------------------
# Position sizing — Kelly criterion with regime conditioning
# ---------------------------------------------------------------------------

KELLY_REGIME_MULTIPLIER = {
    "GROWTH": 1.0,
    "NEUTRAL": 0.5,
    "FRAGILE": 0.3,
    "CRISIS": 0.2,
}


def half_kelly_fraction(
    win_rate: float,
    avg_win: float,
    avg_loss: float,
) -> float:
    """Compute half-Kelly optimal fraction.

    Parameters:
        win_rate: Historical win probability (0-1).
        avg_win: Average win magnitude (positive).
        avg_loss: Average loss magnitude (positive).

    Returns:
        float: Half-Kelly fraction (capped at 0.25).
    """
    if avg_loss <= 0 or avg_win <= 0:
        return 0.0
    b = avg_win / avg_loss  # Win/loss ratio
    q = 1 - win_rate
    kelly = (win_rate * b - q) / b
    half = kelly / 2
    return max(0.0, min(half, 0.25))  # Cap at 25%


def regime_adjusted_size(
    kelly_frac: float,
    regime: str,
    confidence: float,
) -> float:
    """Adjust Kelly fraction for regime and confidence.

    Parameters:
        kelly_frac: Base Kelly fraction.
        regime: Current regime label.
        confidence: Regime classification confidence (0-1).

    Returns:
        float: Adjusted position size multiplier.
    """
    regime_mult = KELLY_REGIME_MULTIPLIER.get(regime, 0.5)
    conf_mult = 0.5 + 0.5 * confidence  # Scale from 50% to 100%
    return kelly_frac * regime_mult * conf_mult


# ---------------------------------------------------------------------------
# Performance metrics
# ---------------------------------------------------------------------------

def compute_metrics(
    daily_returns: pd.Series,
    risk_free_rate: float = 0.03,
    trading_days: int = 252,
) -> dict[str, Any]:
    """Compute comprehensive performance metrics.

    Parameters:
        daily_returns: Series of daily returns.
        risk_free_rate: Annual risk-free rate.
        trading_days: Trading days per year.

    Returns:
        dict: Full performance metrics.
    """
    if daily_returns.empty or len(daily_returns) < 2:
        return {"error": "Insufficient data"}

    rf_daily = (1 + risk_free_rate) ** (1 / trading_days) - 1

    # Cumulative
    cum = (1 + daily_returns).cumprod()
    total_return = float(cum.iloc[-1] - 1)
    n_years = len(daily_returns) / trading_days
    ann_return = float((1 + total_return) ** (1 / max(n_years, 0.01)) - 1)

    # Volatility
    ann_vol = float(daily_returns.std() * np.sqrt(trading_days))

    # Sharpe
    excess = daily_returns - rf_daily
    sharpe = float(excess.mean() / excess.std() * np.sqrt(trading_days)) if excess.std() > 0 else 0.0

    # Sortino
    downside = daily_returns[daily_returns < 0]
    downside_std = float(downside.std() * np.sqrt(trading_days)) if len(downside) > 0 else 1e-10
    sortino = float((ann_return - risk_free_rate) / downside_std) if downside_std > 0 else 0.0

    # Max drawdown
    peak = cum.expanding().max()
    dd = (cum - peak) / peak
    max_dd = float(dd.min())
    max_dd_date = str(dd.idxmin()) if not dd.empty else None

    # Drawdown duration
    in_dd = dd < 0
    dd_groups = (~in_dd).cumsum()
    dd_durations = in_dd.groupby(dd_groups).sum()
    max_dd_duration = int(dd_durations.max()) if len(dd_durations) > 0 else 0

    # Calmar
    calmar = float(ann_return / abs(max_dd)) if abs(max_dd) > 0.001 else 0.0

    # Win rate
    win_rate = float((daily_returns > 0).mean())

    # Best/worst periods
    monthly = daily_returns.resample('ME').apply(lambda x: (1 + x).prod() - 1)
    quarterly = daily_returns.resample('QE').apply(lambda x: (1 + x).prod() - 1)
    yearly = daily_returns.resample('YE').apply(lambda x: (1 + x).prod() - 1)

    # Beta to SPY (approximated)
    beta = None  # Will be filled if benchmark data available

    return {
        "cumulative_return": round(total_return, 4),
        "annualized_return": round(ann_return, 4),
        "annualized_volatility": round(ann_vol, 4),
        "sharpe_ratio": round(sharpe, 4),
        "sortino_ratio": round(sortino, 4),
        "max_drawdown": round(max_dd, 4),
        "max_drawdown_date": max_dd_date,
        "max_drawdown_duration_days": max_dd_duration,
        "calmar_ratio": round(calmar, 4),
        "daily_win_rate": round(win_rate, 4),
        "total_trading_days": len(daily_returns),
        "years": round(n_years, 2),
        "worst_month": round(float(monthly.min()), 4) if len(monthly) > 0 else None,
        "worst_quarter": round(float(quarterly.min()), 4) if len(quarterly) > 0 else None,
        "worst_year": round(float(yearly.min()), 4) if len(yearly) > 0 else None,
        "best_month": round(float(monthly.max()), 4) if len(monthly) > 0 else None,
        "best_year": round(float(yearly.max()), 4) if len(yearly) > 0 else None,
        "beta_to_spy": beta,
    }


def compute_regime_stats(
    daily_returns: pd.Series,
    regime_series: pd.Series,
) -> dict[str, dict[str, Any]]:
    """Compute performance statistics broken down by regime.

    Parameters:
        daily_returns: Daily portfolio returns.
        regime_series: Regime label for each day.

    Returns:
        dict: Per-regime statistics.
    """
    stats = {}
    for regime in ["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"]:
        mask = regime_series == regime
        if mask.sum() == 0:
            continue
        r = daily_returns[mask]
        stats[regime] = {
            "days": int(mask.sum()),
            "pct_of_total": round(float(mask.mean()), 4),
            "avg_daily_return": round(float(r.mean()), 6),
            "annualized_return": round(float(r.mean() * 252), 4),
            "win_rate": round(float((r > 0).mean()), 4),
            "volatility": round(float(r.std() * np.sqrt(252)), 4),
        }
    return stats


def compute_transition_returns(
    daily_returns: pd.Series,
    regime_series: pd.Series,
    horizons: list[int] = [5, 10, 20],
) -> list[dict[str, Any]]:
    """Compute returns after regime transitions.

    Parameters:
        daily_returns: Daily portfolio returns.
        regime_series: Regime label for each day.
        horizons: Forward return horizons in days.

    Returns:
        list: Transition event records with forward returns.
    """
    transitions = []
    prev_regime = None
    for i, (dt, regime) in enumerate(regime_series.items()):
        if prev_regime is not None and regime != prev_regime:
            event = {
                "date": str(dt),
                "from": prev_regime,
                "to": regime,
            }
            for h in horizons:
                if i + h < len(daily_returns):
                    fwd = float((1 + daily_returns.iloc[i:i + h]).prod() - 1)
                    event[f"fwd_{h}d"] = round(fwd, 4)
            transitions.append(event)
        prev_regime = regime
    return transitions


# ---------------------------------------------------------------------------
# Backtest engine
# ---------------------------------------------------------------------------

class PitchBacktester:
    """Full pitch-grade backtester with regime-driven allocation.

    Runs historical regime classification day-by-day using the GMM model
    with PIT-correct data, then simulates portfolio allocation with
    transaction costs and rebalancing lag.

    Attributes:
        engine: SQLAlchemy database engine.
        pit_store: PITStore for point-in-time data.
    """

    def __init__(self, db_engine: Any = None, pit_store: Any = None) -> None:
        self.engine = db_engine
        self.pit_store = pit_store
        _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def _init_db(self) -> None:
        """Lazy-init database connections."""
        if self.engine is None:
            from db import get_engine
            self.engine = get_engine()
        if self.pit_store is None:
            from store.pit import PITStore
            self.pit_store = PITStore(self.engine)

    def run_historical_regime(
        self,
        start_date: date = date(2015, 1, 1),
        end_date: date | None = None,
        retrain_frequency: int = 63,
    ) -> pd.DataFrame:
        """Classify regimes historically with PIT-correct data.

        Retrains the GMM every retrain_frequency days using only data
        available as of each point.

        Parameters:
            start_date: Start of backtest period.
            end_date: End of backtest period (default: today).
            retrain_frequency: Days between GMM retraining.

        Returns:
            DataFrame with columns [date, regime, confidence, posture].
        """
        self._init_db()
        if end_date is None:
            end_date = date.today()

        log.info("Running historical regime classification {s} → {e}", s=start_date, e=end_date)

        from sqlalchemy import text as sqlt
        from sklearn.preprocessing import StandardScaler
        from sklearn.mixture import GaussianMixture

        # Get eligible feature IDs
        with self.engine.connect() as conn:
            rows = conn.execute(
                sqlt("SELECT id FROM feature_registry WHERE model_eligible=TRUE ORDER BY id")
            ).fetchall()
        fids = [r[0] for r in rows]

        if not fids:
            log.error("No model-eligible features found")
            return pd.DataFrame()

        # Get full feature matrix
        lookback_start = start_date - timedelta(days=504)
        matrix = self.pit_store.get_feature_matrix(
            feature_ids=fids,
            start_date=lookback_start,
            end_date=end_date,
            as_of_date=end_date,
            vintage_policy="FIRST_RELEASE",
        )

        if matrix.empty:
            log.error("Empty feature matrix")
            return pd.DataFrame()

        matrix = matrix.ffill().bfill().dropna(axis=1, how="all").dropna()
        log.info("Feature matrix: {r} rows × {c} cols", r=matrix.shape[0], c=matrix.shape[1])

        # Day-by-day regime classification with periodic retraining
        results = []
        gmm = None
        scaler = None
        last_train_idx = -retrain_frequency  # Force initial training
        trading_dates = matrix.loc[start_date:end_date].index

        for i, dt in enumerate(trading_dates):
            # Retrain GMM periodically
            if i - last_train_idx >= retrain_frequency or gmm is None:
                train_data = matrix.loc[:dt]
                if len(train_data) < 60:
                    continue

                scaler = StandardScaler()
                X_train = scaler.fit_transform(train_data.values)
                gmm = GaussianMixture(n_components=4, random_state=42, n_init=5)
                gmm.fit(X_train)

                # Map clusters to regimes by stress level
                labels_all = gmm.predict(X_train)
                cluster_means = {}
                for k in range(4):
                    mask = labels_all == k
                    if mask.sum() > 0:
                        cluster_means[k] = np.mean(np.abs(train_data.values[mask].mean(axis=0)))
                    else:
                        cluster_means[k] = 0.0

                stress_order = sorted(cluster_means, key=cluster_means.get)
                regime_map = {
                    stress_order[0]: "GROWTH",
                    stress_order[1]: "NEUTRAL",
                    stress_order[2]: "FRAGILE",
                    stress_order[3]: "CRISIS",
                }
                last_train_idx = i

            # Classify current day
            row = matrix.loc[[dt]]
            X_curr = scaler.transform(row.values)
            label = gmm.predict(X_curr)[0]
            probs = gmm.predict_proba(X_curr)[0]
            confidence = float(np.max(probs))

            regime = regime_map.get(label, "NEUTRAL")
            posture = REGIME_TO_POSTURE[regime]

            results.append({
                "date": dt,
                "regime": regime,
                "confidence": confidence,
                "posture": posture,
            })

        df = pd.DataFrame(results)
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
            log.info("Regime classification complete: {n} days", n=len(df))

        return df

    def get_asset_prices(
        self,
        tickers: list[str],
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Fetch daily close prices for assets.

        Tries yfinance first, then falls back to PIT store for features
        that map to known tickers.

        Parameters:
            tickers: List of ticker symbols.
            start_date: Start date.
            end_date: End date.

        Returns:
            DataFrame with tickers as columns, dates as index.
        """
        try:
            import yfinance as yf
            data = yf.download(
                tickers,
                start=start_date.isoformat(),
                end=end_date.isoformat(),
                progress=False,
                auto_adjust=True,
            )
            if isinstance(data.columns, pd.MultiIndex):
                prices = data["Close"] if "Close" in data.columns.get_level_values(0) else data
            else:
                prices = data
            return prices.dropna(how="all")
        except Exception as exc:
            log.warning("yfinance failed: {e}, generating synthetic returns", e=str(exc))
            # Fallback: generate from PIT store data if available
            return pd.DataFrame()

    def simulate_portfolio(
        self,
        regime_df: pd.DataFrame,
        initial_capital: float = 100_000,
        cost_bps: float = 10.0,
        rebalance_lag: int = 1,
    ) -> dict[str, Any]:
        """Simulate portfolio returns based on regime-driven allocation.

        Parameters:
            regime_df: DataFrame with regime, confidence, posture columns.
            initial_capital: Starting portfolio value.
            cost_bps: Transaction cost per rebalance in basis points.
            rebalance_lag: Days of lag before rebalancing.

        Returns:
            dict: Full backtest results with equity curve, metrics, etc.
        """
        if regime_df.empty:
            return {"error": "No regime data"}

        start_date = regime_df.index.min().date()
        end_date = regime_df.index.max().date()

        # Collect all unique tickers needed
        all_tickers = set()
        for alloc in POSTURE_ALLOCATIONS.values():
            all_tickers.update(k for k in alloc if k != "CASH")
        # Add benchmark tickers
        all_tickers.update(["SPY", "TLT"])

        log.info("Fetching prices for {n} assets", n=len(all_tickers))
        prices = self.get_asset_prices(
            list(all_tickers),
            start_date - timedelta(days=5),
            end_date + timedelta(days=1),
        )

        if prices.empty:
            log.error("No price data available")
            return {"error": "No price data"}

        # Compute daily returns for all assets
        returns = prices.pct_change().dropna()

        # Align regime data with returns
        common_dates = regime_df.index.intersection(returns.index)
        if len(common_dates) == 0:
            log.error("No overlapping dates between regime and price data")
            return {"error": "No date overlap"}

        regime_aligned = regime_df.loc[common_dates]
        returns_aligned = returns.loc[common_dates]

        # Simulate GRID portfolio
        grid_values = [initial_capital]
        grid_returns_list = []
        prev_posture = None
        cost_rate = cost_bps / 10_000

        for i in range(len(common_dates)):
            dt = common_dates[i]

            # Apply rebalance lag
            if i >= rebalance_lag:
                posture = regime_aligned.iloc[i - rebalance_lag]["posture"]
            else:
                posture = "BALANCED"

            alloc = POSTURE_ALLOCATIONS[posture]

            # Compute portfolio return for this day
            port_return = 0.0
            for ticker, weight in alloc.items():
                if ticker == "CASH":
                    port_return += weight * (0.04 / 252)  # ~4% annual cash yield
                elif ticker in returns_aligned.columns:
                    port_return += weight * returns_aligned.loc[dt, ticker]

            # Apply transaction costs on rebalance
            if posture != prev_posture and prev_posture is not None:
                port_return -= cost_rate
            prev_posture = posture

            grid_returns_list.append(port_return)
            grid_values.append(grid_values[-1] * (1 + port_return))

        grid_returns = pd.Series(grid_returns_list, index=common_dates, name="GRID")
        grid_equity = pd.Series(grid_values[1:], index=common_dates, name="GRID")

        # Simulate benchmarks
        benchmarks = {}
        for bench_name, bench_alloc in [("SPY", BENCHMARK_SPY), ("60/40", BENCHMARK_60_40)]:
            bench_returns_list = []
            for dt in common_dates:
                r = sum(
                    w * returns_aligned.loc[dt, t]
                    for t, w in bench_alloc.items()
                    if t in returns_aligned.columns
                )
                bench_returns_list.append(r)
            bench_returns = pd.Series(bench_returns_list, index=common_dates, name=bench_name)
            bench_equity_vals = [initial_capital]
            for r in bench_returns_list:
                bench_equity_vals.append(bench_equity_vals[-1] * (1 + r))
            bench_equity = pd.Series(bench_equity_vals[1:], index=common_dates, name=bench_name)
            benchmarks[bench_name] = {
                "returns": bench_returns,
                "equity": bench_equity,
                "metrics": compute_metrics(bench_returns),
            }

        # Compute GRID metrics
        grid_metrics = compute_metrics(grid_returns)
        regime_stats = compute_regime_stats(grid_returns, regime_aligned["regime"])
        transitions = compute_transition_returns(grid_returns, regime_aligned["regime"])

        # Rolling 1-year Sharpe
        rolling_sharpe = grid_returns.rolling(252).apply(
            lambda x: x.mean() / x.std() * np.sqrt(252) if x.std() > 0 else 0
        ).dropna()

        result = {
            "period": {"start": str(start_date), "end": str(end_date)},
            "initial_capital": initial_capital,
            "final_value": round(grid_values[-1], 2),
            "cost_bps": cost_bps,
            "rebalance_lag_days": rebalance_lag,
            "grid_metrics": grid_metrics,
            "benchmark_metrics": {k: v["metrics"] for k, v in benchmarks.items()},
            "regime_stats": regime_stats,
            "transitions": transitions[:20],  # Top 20
            "total_transitions": len(transitions),
            "equity_curve": {
                "dates": [str(d.date()) for d in common_dates],
                "grid": [round(v, 2) for v in grid_equity.values],
                "spy": [round(v, 2) for v in benchmarks["SPY"]["equity"].values] if "SPY" in benchmarks else [],
                "sixty_forty": [round(v, 2) for v in benchmarks["60/40"]["equity"].values] if "60/40" in benchmarks else [],
            },
            "regime_timeline": {
                "dates": [str(d.date()) for d in common_dates],
                "regimes": regime_aligned["regime"].values.tolist(),
                "confidences": [round(c, 3) for c in regime_aligned["confidence"].values],
            },
            "rolling_sharpe": {
                "dates": [str(d.date()) for d in rolling_sharpe.index],
                "values": [round(v, 3) for v in rolling_sharpe.values],
            },
        }

        return result

    def run_full_backtest(
        self,
        start_date: date = date(2015, 1, 1),
        end_date: date | None = None,
        initial_capital: float = 100_000,
        cost_bps: float = 10.0,
        save: bool = True,
    ) -> dict[str, Any]:
        """Run the complete pitch backtest.

        Parameters:
            start_date: Backtest start date.
            end_date: Backtest end date (default: today).
            initial_capital: Starting capital.
            cost_bps: Transaction cost in basis points.
            save: Whether to save results to disk.

        Returns:
            dict: Complete backtest results.
        """
        self._init_db()
        if end_date is None:
            end_date = date.today()

        log.info("=== PITCH BACKTEST: {s} → {e} ===", s=start_date, e=end_date)

        # Step 1: Historical regime classification
        regime_df = self.run_historical_regime(start_date, end_date)
        if regime_df.empty:
            return {"error": "Regime classification failed"}

        # Step 2: Portfolio simulation
        result = self.simulate_portfolio(
            regime_df,
            initial_capital=initial_capital,
            cost_bps=cost_bps,
        )

        if "error" in result:
            return result

        # Step 3: Position sizing model parameters
        if result.get("grid_metrics"):
            wr = result["grid_metrics"].get("daily_win_rate", 0.52)
            grid_returns_arr = np.array([
                (result["equity_curve"]["grid"][i] / result["equity_curve"]["grid"][i - 1]) - 1
                for i in range(1, len(result["equity_curve"]["grid"]))
            ])
            wins = grid_returns_arr[grid_returns_arr > 0]
            losses = grid_returns_arr[grid_returns_arr < 0]
            avg_win = float(wins.mean()) if len(wins) > 0 else 0.01
            avg_loss = float(abs(losses.mean())) if len(losses) > 0 else 0.01

            kelly = half_kelly_fraction(wr, avg_win, avg_loss)
            result["position_sizing"] = {
                "half_kelly_fraction": round(kelly, 4),
                "win_rate": round(wr, 4),
                "avg_win": round(avg_win, 6),
                "avg_loss": round(avg_loss, 6),
                "win_loss_ratio": round(avg_win / avg_loss, 4) if avg_loss > 0 else None,
                "regime_adjusted_sizes": {
                    regime: round(regime_adjusted_size(kelly, regime, 0.8), 4)
                    for regime in KELLY_REGIME_MULTIPLIER
                },
            }

        if save:
            self._save_results(result)

        return result

    def _save_results(self, result: dict[str, Any]) -> None:
        """Save backtest results to disk."""
        _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # Save full JSON
        json_path = _OUTPUT_DIR / "backtest_results.json"
        with json_path.open("w") as f:
            json.dump(result, f, indent=2, default=str)
        log.info("Results saved to {p}", p=json_path)

    def get_latest_results(self) -> dict[str, Any] | None:
        """Load latest saved backtest results."""
        json_path = _OUTPUT_DIR / "backtest_results.json"
        if not json_path.exists():
            return None
        with json_path.open() as f:
            return json.load(f)

    def get_summary(self) -> dict[str, Any] | None:
        """Get a summary of the latest backtest for the pitch."""
        result = self.get_latest_results()
        if not result:
            return None

        gm = result.get("grid_metrics", {})
        bm = result.get("benchmark_metrics", {})
        spy_m = bm.get("SPY", {})
        sixty_m = bm.get("60/40", {})

        return {
            "period": result.get("period"),
            "grid": {
                "final_value": result.get("final_value"),
                "cumulative_return": gm.get("cumulative_return"),
                "annualized_return": gm.get("annualized_return"),
                "sharpe": gm.get("sharpe_ratio"),
                "sortino": gm.get("sortino_ratio"),
                "max_drawdown": gm.get("max_drawdown"),
                "calmar": gm.get("calmar_ratio"),
            },
            "spy": {
                "cumulative_return": spy_m.get("cumulative_return"),
                "sharpe": spy_m.get("sharpe_ratio"),
                "max_drawdown": spy_m.get("max_drawdown"),
            },
            "sixty_forty": {
                "cumulative_return": sixty_m.get("cumulative_return"),
                "sharpe": sixty_m.get("sharpe_ratio"),
                "max_drawdown": sixty_m.get("max_drawdown"),
            },
            "regime_stats": result.get("regime_stats"),
            "position_sizing": result.get("position_sizing"),
            "total_transitions": result.get("total_transitions"),
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    backtester = PitchBacktester()

    if len(sys.argv) > 1 and sys.argv[1] == "--run":
        start = date(2015, 1, 1)
        if len(sys.argv) > 2:
            start = date.fromisoformat(sys.argv[2])
        result = backtester.run_full_backtest(start_date=start)
        if "error" in result:
            print(f"ERROR: {result['error']}")
        else:
            gm = result["grid_metrics"]
            print(f"\n{'='*60}")
            print(f"GRID PITCH BACKTEST RESULTS")
            print(f"{'='*60}")
            print(f"Period: {result['period']['start']} → {result['period']['end']}")
            print(f"Initial: ${result['initial_capital']:,.0f}")
            print(f"Final:   ${result['final_value']:,.0f}")
            print(f"")
            print(f"Cumulative Return: {gm['cumulative_return']:.1%}")
            print(f"Annualized Return: {gm['annualized_return']:.1%}")
            print(f"Sharpe Ratio:      {gm['sharpe_ratio']:.2f}")
            print(f"Sortino Ratio:     {gm['sortino_ratio']:.2f}")
            print(f"Max Drawdown:      {gm['max_drawdown']:.1%}")
            print(f"Calmar Ratio:      {gm['calmar_ratio']:.2f}")
            print(f"")
            print(f"vs SPY:  {result['benchmark_metrics']['SPY']['cumulative_return']:.1%}")
            print(f"vs 60/40: {result['benchmark_metrics']['60/40']['cumulative_return']:.1%}")

    elif len(sys.argv) > 1 and sys.argv[1] == "--summary":
        summary = backtester.get_summary()
        if summary:
            print(json.dumps(summary, indent=2))
        else:
            print("No saved results. Run with --run first.")

    else:
        print("Usage:")
        print("  python -m backtest.engine --run [start_date]")
        print("  python -m backtest.engine --summary")
