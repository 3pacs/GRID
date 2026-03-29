"""
GRID — Key strategies from Kakushadze & Serur (2018) '151 Trading Strategies'.

Implements the most relevant quantitative strategies adapted for GRID's
PIT-correct data access and existing options/regime detection infrastructure.

Strategies implemented:
    1. Mean reversion via z-score of deviation from moving average (Ch.3 §3.9)
    2. Pairs trading via Engle-Granger cointegration (Ch.3 §3.8)
    3. Cross-sectional price momentum with 12-1 month lookback (Ch.3 §3.1)
    4. Volatility risk premium — implied vs realised vol spread (Ch.7 §7.4)
    5. Ornstein-Uhlenbeck mean reversion (Ch.3 / optimisation paper)
    6. Sector momentum rotation — risk-adjusted (Ch.3 + Ch.4 ETFs)

Reference:
    Kakushadze, Z. & Serur, J.A. (2018). 151 Trading Strategies.
    SSRN: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3247865
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore


# ---------------------------------------------------------------------------
# Signal dataclass
# ---------------------------------------------------------------------------

@dataclass
class StrategySignal:
    """A directional signal produced by one of the 151 strategies."""

    strategy: str           # e.g. "mean_reversion", "pairs", "momentum"
    ticker: str
    direction: str          # "LONG" or "SHORT"
    strength: float         # signal strength 0–1
    entry_price: float | None
    stop_loss: float | None
    target: float | None
    metadata: dict = field(default_factory=dict)
    generated_at: str = ""

    def __post_init__(self) -> None:
        if not self.generated_at:
            self.generated_at = datetime.utcnow().isoformat()


# ---------------------------------------------------------------------------
# Strategy engine
# ---------------------------------------------------------------------------

class Strategy151Engine:
    """Key strategies from Kakushadze & Serur (2018) '151 Trading Strategies'.

    Each strategy method accepts pre-loaded price data (DataFrames) so that
    callers can supply any data source.  The ``run_all_strategies`` entry
    point loads data from the database and delegates to each strategy.

    Attributes:
        engine: SQLAlchemy engine for database queries.
        pit_store: GRID point-in-time store for PIT-correct reads.
    """

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        self.engine = db_engine
        self.pit_store = pit_store
        log.info("Strategy151Engine initialised")

    # ------------------------------------------------------------------
    # Data loading helpers
    # ------------------------------------------------------------------

    def _load_price_data(
        self,
        tickers: list[str],
        as_of_date: date,
        lookback_days: int = 504,
    ) -> pd.DataFrame:
        """Load daily close prices from ``market_daily``.

        Returns a DataFrame with dates as index and tickers as columns.
        """
        start_date = as_of_date - timedelta(days=lookback_days)
        query = text("""
            SELECT ticker, signal_date AS date, close_price AS close
            FROM market_daily
            WHERE ticker = ANY(:tickers)
              AND signal_date BETWEEN :start AND :end
            ORDER BY signal_date
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={
                    "tickers": tickers,
                    "start": start_date,
                    "end": as_of_date,
                })
        except Exception as exc:
            log.error("Failed to load price data: {e}", e=str(exc))
            return pd.DataFrame()

        if df.empty:
            return pd.DataFrame()

        return df.pivot(index="date", columns="ticker", values="close")

    def _load_iv_data(
        self,
        tickers: list[str],
        as_of_date: date,
        lookback_days: int = 252,
    ) -> pd.DataFrame:
        """Load implied volatility from ``options_daily_signals``."""
        start_date = as_of_date - timedelta(days=lookback_days)
        query = text("""
            SELECT ticker, signal_date, iv_atm
            FROM options_daily_signals
            WHERE ticker = ANY(:tickers)
              AND signal_date BETWEEN :start AND :end
            ORDER BY signal_date
        """)
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, params={
                    "tickers": tickers,
                    "start": start_date,
                    "end": as_of_date,
                })
        except Exception as exc:
            log.error("Failed to load IV data: {e}", e=str(exc))
            return pd.DataFrame()

        if df.empty:
            return pd.DataFrame()

        return df.pivot(index="signal_date", columns="ticker", values="iv_atm")

    # ------------------------------------------------------------------
    # Strategy 1: Mean reversion via z-score (Ch.3 §3.9)
    # ------------------------------------------------------------------

    def mean_reversion_scan(
        self,
        prices: pd.DataFrame,
        window: int = 20,
        entry_z: float = 2.0,
        exit_z: float = 0.5,
    ) -> list[StrategySignal]:
        """Mean reversion via z-score of deviation from moving average.

        For each ticker, computes::

            spread = price - SMA(price, window)
            zscore = spread / rolling_std(spread, window)

        Generates LONG when zscore < -entry_z, SHORT when zscore > entry_z.

        Parameters:
            prices: DataFrame with dates as index, tickers as columns.
            window: Lookback window for the moving average.
            entry_z: Z-score threshold for entry.
            exit_z: Z-score threshold for exit (metadata only).

        Returns:
            List of strategy signals for tickers currently in entry zone.
        """
        if prices.empty or len(prices) < window * 2:
            log.warning("Insufficient data for mean reversion scan")
            return []

        signals: list[StrategySignal] = []
        ma = prices.rolling(window).mean()
        spread = prices - ma
        std = spread.rolling(window).std()
        zscore = spread / std.replace(0, np.nan)

        latest_z = zscore.iloc[-1]
        latest_p = prices.iloc[-1]

        for ticker in prices.columns:
            z = latest_z.get(ticker)
            p = latest_p.get(ticker)
            if z is None or np.isnan(z) or p is None or np.isnan(p):
                continue

            if z < -entry_z:
                strength = min(1.0, abs(z) / (entry_z * 2))
                signals.append(StrategySignal(
                    strategy="mean_reversion",
                    ticker=ticker,
                    direction="LONG",
                    strength=strength,
                    entry_price=float(p),
                    stop_loss=float(p * 0.95),
                    target=float(ma.iloc[-1].get(ticker, p)),
                    metadata={
                        "zscore": round(float(z), 4),
                        "ma_window": window,
                        "entry_z": entry_z,
                        "exit_z": exit_z,
                    },
                ))
            elif z > entry_z:
                strength = min(1.0, abs(z) / (entry_z * 2))
                signals.append(StrategySignal(
                    strategy="mean_reversion",
                    ticker=ticker,
                    direction="SHORT",
                    strength=strength,
                    entry_price=float(p),
                    stop_loss=float(p * 1.05),
                    target=float(ma.iloc[-1].get(ticker, p)),
                    metadata={
                        "zscore": round(float(z), 4),
                        "ma_window": window,
                        "entry_z": entry_z,
                        "exit_z": exit_z,
                    },
                ))

        log.info(
            "Mean reversion scan: {n} signals from {t} tickers",
            n=len(signals), t=len(prices.columns),
        )
        return signals

    # ------------------------------------------------------------------
    # Strategy 2: Pairs trading via cointegration (Ch.3 §3.8)
    # ------------------------------------------------------------------

    def _find_cointegrated_pairs(
        self,
        prices: pd.DataFrame,
        p_threshold: float = 0.05,
    ) -> list[tuple[str, str, float]]:
        """Find cointegrated pairs using the Engle-Granger test.

        Returns list of (ticker_a, ticker_b, p_value) sorted by p-value.
        """
        try:
            from statsmodels.tsa.stattools import coint
        except ImportError:
            log.warning("statsmodels not installed — skipping cointegration")
            return []

        tickers = prices.columns.tolist()
        pairs: list[tuple[str, str, float]] = []

        for i in range(len(tickers)):
            for j in range(i + 1, len(tickers)):
                s1 = prices[tickers[i]].dropna()
                s2 = prices[tickers[j]].dropna()
                common = s1.index.intersection(s2.index)
                if len(common) < 60:
                    continue
                try:
                    _, p_value, _ = coint(s1.loc[common], s2.loc[common])
                    if p_value < p_threshold:
                        pairs.append((tickers[i], tickers[j], float(p_value)))
                except Exception:
                    continue

        return sorted(pairs, key=lambda x: x[2])

    def pairs_trading_scan(
        self,
        prices: pd.DataFrame,
        lookback: int = 252,
        entry_z: float = 2.0,
        exit_z: float = 0.5,
        p_threshold: float = 0.05,
    ) -> list[StrategySignal]:
        """Pairs trading via Engle-Granger cointegration.

        For each cointegrated pair (A, B):
          1. Compute hedge ratio beta = cov(A,B) / var(B)
          2. Spread = A - beta * B
          3. Z-score the spread
          4. Signal: LONG spread if z < -entry_z, SHORT if z > entry_z

        Parameters:
            prices: DataFrame with dates as index, tickers as columns.
            lookback: Lookback window for the cointegration test.
            entry_z: Z-score entry threshold.
            exit_z: Z-score exit threshold.
            p_threshold: Maximum p-value for the cointegration test.

        Returns:
            List of strategy signals.
        """
        if prices.empty or len(prices) < lookback:
            log.warning("Insufficient data for pairs trading scan")
            return []

        # Use most recent `lookback` rows
        recent = prices.iloc[-lookback:]
        pairs = self._find_cointegrated_pairs(recent, p_threshold)

        if not pairs:
            log.info("No cointegrated pairs found")
            return []

        log.info("Found {n} cointegrated pairs", n=len(pairs))
        signals: list[StrategySignal] = []

        for ticker_a, ticker_b, p_val in pairs:
            a = recent[ticker_a].dropna()
            b = recent[ticker_b].dropna()
            common = a.index.intersection(b.index)
            if len(common) < 60:
                continue

            ac = a.loc[common]
            bc = b.loc[common]

            # Hedge ratio via OLS
            beta = float(np.cov(ac, bc)[0, 1] / np.var(bc))
            spread = ac - beta * bc
            z = float((spread.iloc[-1] - spread.mean()) / max(spread.std(), 1e-10))

            if abs(z) < entry_z:
                continue

            direction = "LONG" if z < -entry_z else "SHORT"
            strength = min(1.0, abs(z) / (entry_z * 2))

            signals.append(StrategySignal(
                strategy="pairs_trading",
                ticker=f"{ticker_a}/{ticker_b}",
                direction=direction,
                strength=strength,
                entry_price=None,
                stop_loss=None,
                target=None,
                metadata={
                    "ticker_a": ticker_a,
                    "ticker_b": ticker_b,
                    "hedge_ratio": round(beta, 4),
                    "zscore": round(z, 4),
                    "p_value": round(p_val, 6),
                    "spread_mean": round(float(spread.mean()), 4),
                    "spread_std": round(float(spread.std()), 4),
                },
            ))

        log.info("Pairs trading scan: {n} signals", n=len(signals))
        return signals

    # ------------------------------------------------------------------
    # Strategy 3: Cross-sectional momentum (Ch.3 §3.1)
    # ------------------------------------------------------------------

    def cross_sectional_momentum(
        self,
        prices: pd.DataFrame,
        formation: int = 252,
        skip: int = 21,
        top_n: int = 10,
    ) -> list[StrategySignal]:
        """Cross-sectional price momentum with 12-1 month lookback.

        Ranks stocks by ``R(t-skip, t-formation)`` and generates LONG for
        top_n winners and SHORT for bottom top_n losers.

        Parameters:
            prices: DataFrame with dates as index, tickers as columns.
            formation: Formation period in trading days (default 252 = 12m).
            skip: Days to skip at the recent end (default 21 = 1 month).
            top_n: Number of tickers in each leg.

        Returns:
            List of strategy signals.
        """
        if prices.empty or len(prices) < formation + skip:
            log.warning("Insufficient data for momentum scan")
            return []

        # Momentum return: price(t-skip) / price(t-formation) - 1
        p_now = prices.iloc[-1 - skip]
        p_past = prices.iloc[-formation]

        returns = (p_now / p_past - 1).dropna()
        if len(returns) < 2:
            return []

        ranked = returns.sort_values(ascending=False)
        n = min(top_n, len(ranked) // 2)
        if n == 0:
            return []

        signals: list[StrategySignal] = []
        latest_prices = prices.iloc[-1]

        # Long winners
        for ticker in ranked.head(n).index:
            p = latest_prices.get(ticker)
            if p is None or np.isnan(p):
                continue
            signals.append(StrategySignal(
                strategy="momentum",
                ticker=ticker,
                direction="LONG",
                strength=min(1.0, abs(float(ranked[ticker])) / 0.5),
                entry_price=float(p),
                stop_loss=float(p * 0.90),
                target=float(p * 1.10),
                metadata={
                    "momentum_return": round(float(ranked[ticker]), 4),
                    "formation_days": formation,
                    "skip_days": skip,
                    "rank": int(list(ranked.index).index(ticker)) + 1,
                },
            ))

        # Short losers
        for ticker in ranked.tail(n).index:
            p = latest_prices.get(ticker)
            if p is None or np.isnan(p):
                continue
            signals.append(StrategySignal(
                strategy="momentum",
                ticker=ticker,
                direction="SHORT",
                strength=min(1.0, abs(float(ranked[ticker])) / 0.5),
                entry_price=float(p),
                stop_loss=float(p * 1.10),
                target=float(p * 0.90),
                metadata={
                    "momentum_return": round(float(ranked[ticker]), 4),
                    "formation_days": formation,
                    "skip_days": skip,
                    "rank": int(list(ranked.index).index(ticker)) + 1,
                },
            ))

        log.info(
            "Momentum scan: {n} signals ({l} long, {s} short)",
            n=len(signals),
            l=sum(1 for s in signals if s.direction == "LONG"),
            s=sum(1 for s in signals if s.direction == "SHORT"),
        )
        return signals

    # ------------------------------------------------------------------
    # Strategy 4: Volatility risk premium (Ch.7 §7.4)
    # ------------------------------------------------------------------

    def volatility_risk_premium(
        self,
        prices: pd.DataFrame,
        iv_data: pd.DataFrame | None = None,
        rv_window: int = 21,
        vrp_threshold: float = 1.5,
    ) -> list[StrategySignal]:
        """Harvest the spread between implied and realised volatility.

        VRP = IV - RV, where RV = sqrt(252) * std(daily_returns, rv_window).

        Parameters:
            prices: Close prices (dates x tickers).
            iv_data: Implied volatility panel.  If None, loads from DB.
            rv_window: Window for realised vol calculation.
            vrp_threshold: Z-score threshold for VRP signal.

        Returns:
            List of strategy signals.
        """
        if prices.empty or len(prices) < rv_window + 10:
            return []

        # Compute realised vol
        log_ret = np.log(prices / prices.shift(1))
        rv = log_ret.rolling(rv_window).std() * np.sqrt(252)

        if iv_data is None or iv_data.empty:
            log.warning("No IV data available — skipping VRP strategy")
            return []

        # Align IV and RV
        common_dates = rv.index.intersection(iv_data.index)
        common_tickers = rv.columns.intersection(iv_data.columns)
        if len(common_dates) < 30 or len(common_tickers) == 0:
            return []

        rv_aligned = rv.loc[common_dates, common_tickers]
        iv_aligned = iv_data.loc[common_dates, common_tickers]

        vrp = iv_aligned - rv_aligned

        # Z-score the VRP
        vrp_mean = vrp.rolling(63, min_periods=20).mean()
        vrp_std = vrp.rolling(63, min_periods=20).std().replace(0, np.nan)
        vrp_z = (vrp - vrp_mean) / vrp_std

        latest_z = vrp_z.iloc[-1]
        latest_vrp = vrp.iloc[-1]
        signals: list[StrategySignal] = []

        for ticker in common_tickers:
            z = latest_z.get(ticker)
            v = latest_vrp.get(ticker)
            if z is None or np.isnan(z) or v is None or np.isnan(v):
                continue

            if z > vrp_threshold:
                # IV rich relative to RV — sell vol
                signals.append(StrategySignal(
                    strategy="volatility_risk_premium",
                    ticker=ticker,
                    direction="SHORT",
                    strength=min(1.0, abs(float(z)) / (vrp_threshold * 2)),
                    entry_price=None,
                    stop_loss=None,
                    target=None,
                    metadata={
                        "vrp": round(float(v), 4),
                        "vrp_zscore": round(float(z), 4),
                        "iv": round(float(iv_aligned.iloc[-1][ticker]), 4),
                        "rv": round(float(rv_aligned.iloc[-1][ticker]), 4),
                        "action": "sell_straddle",
                    },
                ))
            elif z < -vrp_threshold:
                # IV cheap relative to RV — buy vol
                signals.append(StrategySignal(
                    strategy="volatility_risk_premium",
                    ticker=ticker,
                    direction="LONG",
                    strength=min(1.0, abs(float(z)) / (vrp_threshold * 2)),
                    entry_price=None,
                    stop_loss=None,
                    target=None,
                    metadata={
                        "vrp": round(float(v), 4),
                        "vrp_zscore": round(float(z), 4),
                        "iv": round(float(iv_aligned.iloc[-1][ticker]), 4),
                        "rv": round(float(rv_aligned.iloc[-1][ticker]), 4),
                        "action": "buy_protection",
                    },
                ))

        log.info("VRP scan: {n} signals", n=len(signals))
        return signals

    # ------------------------------------------------------------------
    # Strategy 5: Ornstein-Uhlenbeck mean reversion
    # ------------------------------------------------------------------

    @staticmethod
    def _estimate_ou_params(series: pd.Series) -> dict[str, float]:
        """Estimate Ornstein-Uhlenbeck parameters from log prices.

        Fits an AR(1) model: y_t = a + b * y_{t-1} + epsilon
        Then derives OU parameters:
            theta = -ln(b) / dt
            mu = a / (1 - b)
            half_life = ln(2) / theta * 252  (in trading days)

        Returns dict with keys: theta, mu, sigma, half_life_days, b.
        """
        log_prices = np.log(series.dropna().values)
        if len(log_prices) < 30:
            return {"theta": 0, "mu": 0, "sigma": 0, "half_life_days": np.inf, "b": 1}

        y = log_prices[1:]
        x = log_prices[:-1]
        n = len(x)

        # AR(1) OLS: y = a + b*x
        sx = np.sum(x)
        sy = np.sum(y)
        sxy = np.sum(x * y)
        sxx = np.sum(x ** 2)

        denom = n * sxx - sx ** 2
        if abs(denom) < 1e-15:
            return {"theta": 0, "mu": 0, "sigma": 0, "half_life_days": np.inf, "b": 1}

        b = (n * sxy - sx * sy) / denom
        a = (sy - b * sx) / n
        residuals = y - a - b * x
        sigma_eq = float(np.std(residuals))

        dt = 1.0 / 252.0

        if b <= 0 or b >= 1:
            # Not mean-reverting
            return {
                "theta": 0.0,
                "mu": float(np.mean(log_prices)),
                "sigma": sigma_eq,
                "half_life_days": np.inf,
                "b": float(b),
            }

        theta = -np.log(b) / dt
        mu = a / (1 - b)
        half_life = np.log(2) / theta * 252  # in trading days

        b_sq = b ** 2
        if abs(1 - b_sq) > 1e-10:
            sigma = sigma_eq * np.sqrt(2 * theta / (1 - b_sq))
        else:
            sigma = sigma_eq

        return {
            "theta": float(theta),
            "mu": float(mu),
            "sigma": float(sigma),
            "half_life_days": float(half_life),
            "b": float(b),
        }

    def ou_mean_reversion(
        self,
        prices: pd.DataFrame,
        lookback: int = 252,
        min_half_life: float = 5.0,
        max_half_life: float = 120.0,
        entry_sigma: float = 1.5,
    ) -> list[StrategySignal]:
        """Ornstein-Uhlenbeck based mean reversion trading.

        For each ticker, fits OU parameters and generates signals when
        the log-price deviates from the equilibrium mu by more than
        ``entry_sigma * sigma / sqrt(2 * theta)``.

        Parameters:
            prices: Close prices (dates x tickers).
            lookback: Lookback window for OU estimation.
            min_half_life: Minimum half-life in trading days (filter noise).
            max_half_life: Maximum half-life in trading days (filter trends).
            entry_sigma: Number of equilibrium-sigmas for entry.

        Returns:
            List of strategy signals.
        """
        if prices.empty or len(prices) < lookback:
            return []

        signals: list[StrategySignal] = []
        recent = prices.iloc[-lookback:]

        for ticker in prices.columns:
            series = recent[ticker].dropna()
            if len(series) < 60:
                continue

            params = self._estimate_ou_params(series)
            hl = params["half_life_days"]

            # Filter: only trade mean-reverting with sensible half-life
            if hl < min_half_life or hl > max_half_life or params["theta"] <= 0:
                continue

            # Current deviation from equilibrium
            current_log = np.log(float(series.iloc[-1]))
            dev = current_log - params["mu"]
            eq_sigma = params["sigma"] / np.sqrt(2 * params["theta"]) if params["theta"] > 0 else np.inf
            if eq_sigma <= 0 or np.isinf(eq_sigma):
                continue

            z = dev / eq_sigma
            p = float(series.iloc[-1])

            if abs(z) < entry_sigma:
                continue

            direction = "SHORT" if z > 0 else "LONG"
            strength = min(1.0, abs(z) / (entry_sigma * 2))

            # Target: mean price
            target_price = float(np.exp(params["mu"]))
            # Stop: 2x deviation in the wrong direction
            if direction == "LONG":
                stop_price = float(p * np.exp(-abs(dev)))
            else:
                stop_price = float(p * np.exp(abs(dev)))

            signals.append(StrategySignal(
                strategy="ou_mean_reversion",
                ticker=ticker,
                direction=direction,
                strength=strength,
                entry_price=p,
                stop_loss=stop_price,
                target=target_price,
                metadata={
                    "theta": round(params["theta"], 4),
                    "mu": round(params["mu"], 6),
                    "sigma": round(params["sigma"], 6),
                    "half_life_days": round(hl, 1),
                    "deviation_sigma": round(float(z), 4),
                    "ar1_coeff": round(params["b"], 4),
                },
            ))

        log.info("OU mean reversion: {n} signals", n=len(signals))
        return signals

    # ------------------------------------------------------------------
    # Strategy 6: Sector momentum rotation (Ch.3 + Ch.4)
    # ------------------------------------------------------------------

    def sector_rotation(
        self,
        prices: pd.DataFrame,
        lookback_months: tuple[int, ...] = (1, 3, 6, 12),
        weights: tuple[float, ...] | None = None,
        top_n: int = 3,
    ) -> list[StrategySignal]:
        """Risk-adjusted sector momentum rotation.

        For each sector/ticker, computes a weighted momentum score across
        multiple lookback windows, divided by realised volatility.

        Parameters:
            prices: Close prices (dates x tickers/sectors).
            lookback_months: Momentum windows in months (approx. 21 days/month).
            weights: Weight per lookback. Default: equal weight.
            top_n: Number of sectors to go long/short.

        Returns:
            List of strategy signals.
        """
        if prices.empty:
            return []

        if weights is None:
            weights = tuple(1.0 / len(lookback_months) for _ in lookback_months)

        # Convert months to approximate trading days
        lookback_days = [m * 21 for m in lookback_months]
        max_lb = max(lookback_days)

        if len(prices) < max_lb + 21:
            log.warning("Insufficient data for sector rotation")
            return []

        # Compute momentum score per ticker
        scores: dict[str, float] = {}
        for ticker in prices.columns:
            s = prices[ticker].dropna()
            if len(s) < max_lb:
                continue

            total_score = 0.0
            for lb, w in zip(lookback_days, weights):
                if len(s) < lb:
                    continue
                ret = float(s.iloc[-1] / s.iloc[-lb] - 1)
                total_score += w * ret

            # Risk-adjust by realised vol
            ret_series = s.pct_change().dropna()
            vol = float(ret_series.iloc[-63:].std()) * np.sqrt(252) if len(ret_series) > 63 else 1.0
            if vol > 0:
                scores[ticker] = total_score / vol
            else:
                scores[ticker] = 0.0

        if len(scores) < 2:
            return []

        ranked = pd.Series(scores).sort_values(ascending=False)
        n = min(top_n, len(ranked) // 2)
        if n == 0:
            return []

        signals: list[StrategySignal] = []
        latest_prices = prices.iloc[-1]

        # Long top sectors
        for ticker in ranked.head(n).index:
            p = latest_prices.get(ticker)
            if p is None or np.isnan(p):
                continue
            signals.append(StrategySignal(
                strategy="sector_rotation",
                ticker=ticker,
                direction="LONG",
                strength=min(1.0, abs(float(ranked[ticker])) / 2.0),
                entry_price=float(p),
                stop_loss=None,
                target=None,
                metadata={
                    "risk_adj_score": round(float(ranked[ticker]), 4),
                    "rank": int(list(ranked.index).index(ticker)) + 1,
                    "lookback_months": list(lookback_months),
                },
            ))

        # Short bottom sectors
        for ticker in ranked.tail(n).index:
            p = latest_prices.get(ticker)
            if p is None or np.isnan(p):
                continue
            signals.append(StrategySignal(
                strategy="sector_rotation",
                ticker=ticker,
                direction="SHORT",
                strength=min(1.0, abs(float(ranked[ticker])) / 2.0),
                entry_price=float(p),
                stop_loss=None,
                target=None,
                metadata={
                    "risk_adj_score": round(float(ranked[ticker]), 4),
                    "rank": int(list(ranked.index).index(ticker)) + 1,
                    "lookback_months": list(lookback_months),
                },
            ))

        log.info(
            "Sector rotation: {n} signals ({l} long, {s} short)",
            n=len(signals),
            l=sum(1 for s in signals if s.direction == "LONG"),
            s=sum(1 for s in signals if s.direction == "SHORT"),
        )
        return signals

    # ------------------------------------------------------------------
    # Composite scoring
    # ------------------------------------------------------------------

    def generate_composite_score(
        self,
        signals: dict[str, list[StrategySignal]],
    ) -> pd.DataFrame:
        """Combine signals across strategies into ticker-level composite scores.

        Each signal contributes ``+strength`` for LONG, ``-strength`` for
        SHORT.  The composite is the mean across all contributing strategies.

        Parameters:
            signals: Mapping of strategy_name -> list of signals.

        Returns:
            DataFrame with columns: ticker, composite_score, n_strategies,
            direction, strategies.
        """
        ticker_scores: dict[str, list[float]] = {}
        ticker_strategies: dict[str, list[str]] = {}

        for strategy_name, sigs in signals.items():
            for sig in sigs:
                t = sig.ticker
                score = sig.strength if sig.direction == "LONG" else -sig.strength
                ticker_scores.setdefault(t, []).append(score)
                ticker_strategies.setdefault(t, []).append(strategy_name)

        if not ticker_scores:
            return pd.DataFrame(
                columns=["ticker", "composite_score", "n_strategies", "direction", "strategies"]
            )

        rows = []
        for ticker, scores in ticker_scores.items():
            avg = float(np.mean(scores))
            rows.append({
                "ticker": ticker,
                "composite_score": round(avg, 4),
                "n_strategies": len(scores),
                "direction": "LONG" if avg > 0 else "SHORT",
                "strategies": ticker_strategies[ticker],
            })

        df = pd.DataFrame(rows).sort_values("composite_score", ascending=False, key=abs)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Run all strategies
    # ------------------------------------------------------------------

    def run_all_strategies(
        self,
        tickers: list[str],
        as_of_date: date,
        lookback_days: int = 504,
        persist: bool = True,
    ) -> dict[str, list[StrategySignal]]:
        """Run all strategies and return combined signals.

        Parameters:
            tickers: Ticker symbols to analyse.
            as_of_date: PIT boundary date.
            lookback_days: Calendar days of history.
            persist: Whether to save results to analytical_snapshots.

        Returns:
            Dict mapping strategy name to list of signals.
        """
        log.info(
            "Running all 151 strategies — {n} tickers, as_of={d}",
            n=len(tickers), d=as_of_date,
        )

        prices = self._load_price_data(tickers, as_of_date, lookback_days)
        if prices.empty:
            log.warning("No price data — aborting strategy scan")
            return {}

        iv_data = self._load_iv_data(tickers, as_of_date)

        results: dict[str, list[StrategySignal]] = {}

        # Strategy 1: Mean reversion
        results["mean_reversion"] = self.mean_reversion_scan(prices)

        # Strategy 2: Pairs trading
        results["pairs_trading"] = self.pairs_trading_scan(prices)

        # Strategy 3: Cross-sectional momentum
        results["momentum"] = self.cross_sectional_momentum(prices)

        # Strategy 4: Volatility risk premium
        results["volatility_risk_premium"] = self.volatility_risk_premium(
            prices, iv_data=iv_data,
        )

        # Strategy 5: OU mean reversion
        results["ou_mean_reversion"] = self.ou_mean_reversion(prices)

        # Strategy 6: Sector rotation
        results["sector_rotation"] = self.sector_rotation(prices)

        total = sum(len(v) for v in results.values())
        log.info("Strategy scan complete — {n} total signals", n=total)

        if persist:
            self._persist_results(results, as_of_date)

        return results

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _persist_results(
        self,
        results: dict[str, list[StrategySignal]],
        as_of_date: date,
    ) -> int | None:
        """Save strategy scan results to analytical_snapshots."""
        from store.snapshots import AnalyticalSnapshotStore

        # Serialise signals for JSON storage
        payload: dict[str, Any] = {}
        for strategy_name, sigs in results.items():
            payload[strategy_name] = [
                {
                    "ticker": s.ticker,
                    "direction": s.direction,
                    "strength": s.strength,
                    "metadata": s.metadata,
                }
                for s in sigs
            ]

        total = sum(len(v) for v in results.values())
        try:
            snap = AnalyticalSnapshotStore(self.engine)
            return snap.save_snapshot(
                category="strategy151",
                payload=payload,
                as_of_date=as_of_date,
                metrics={"n_signals": total, "n_strategies": len(results)},
            )
        except Exception as exc:
            log.error("Failed to persist strategy results: {e}", e=str(exc))
            return None
