"""
GRID WorldQuant 101 Formulaic Alphas engine.

Implements the 101 Formulaic Alphas from Kakushadze, Z. (2016) adapted for
GRID's architecture.  These alphas operate on OHLCV panel data (tickers as
columns, dates as index) and produce cross-sectional signals with typical
holding periods of 0.6–6.4 days.

Reference:
    Kakushadze, Z. (2016). 101 Formulaic Alphas.
    SSRN: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2701346
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from scipy.stats import rankdata
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.pit import PITStore


# ---------------------------------------------------------------------------
# Time-series operators
# ---------------------------------------------------------------------------

def ts_sum(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Rolling sum over *window* periods."""
    return df.rolling(window, min_periods=max(1, window // 2)).sum()


def sma(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Simple moving average."""
    return df.rolling(window, min_periods=max(1, window // 2)).mean()


def stddev(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Rolling standard deviation."""
    return df.rolling(window, min_periods=max(1, window // 2)).std()


def correlation(x: pd.DataFrame, y: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Rolling Pearson correlation between *x* and *y*."""
    return x.rolling(window, min_periods=max(1, window // 2)).corr(y)


def covariance(x: pd.DataFrame, y: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Rolling covariance between *x* and *y*."""
    return x.rolling(window, min_periods=max(1, window // 2)).cov(y)


def ts_rank(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Rolling time-series percentile rank (value between 0 and 1)."""
    return df.rolling(window, min_periods=max(1, window // 2)).apply(
        lambda x: rankdata(x)[-1] / len(x), raw=True,
    )


def ts_min(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Rolling minimum."""
    return df.rolling(window, min_periods=max(1, window // 2)).min()


def ts_max(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Rolling maximum."""
    return df.rolling(window, min_periods=max(1, window // 2)).max()


def delta(df: pd.DataFrame, period: int = 1) -> pd.DataFrame:
    """First difference with given *period*."""
    return df.diff(period)


def delay(df: pd.DataFrame, period: int = 1) -> pd.DataFrame:
    """Lag (shift forward) by *period* rows."""
    return df.shift(period)


def rank(df: pd.DataFrame) -> pd.DataFrame:
    """Cross-sectional percentile rank (across tickers for each date)."""
    return df.rank(axis=1, pct=True)


def scale(df: pd.DataFrame, k: int = 1) -> pd.DataFrame:
    """Scale rows so that ``sum(abs(values)) == k``."""
    return df.mul(k).div(df.abs().sum(axis=1), axis=0)


def ts_argmax(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Rolling position of the maximum value (1-indexed)."""
    return df.rolling(window, min_periods=max(1, window // 2)).apply(
        np.argmax, raw=True,
    ) + 1


def ts_argmin(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Rolling position of the minimum value (1-indexed)."""
    return df.rolling(window, min_periods=max(1, window // 2)).apply(
        np.argmin, raw=True,
    ) + 1


def decay_linear(df: pd.DataFrame, period: int = 10) -> pd.DataFrame:
    """Linear-weighted moving average with increasing weights."""
    weights = np.arange(1, period + 1, dtype=float)
    weights /= weights.sum()
    return df.rolling(period, min_periods=max(1, period // 2)).apply(
        lambda x: np.dot(x[-len(weights):], weights[-len(x):]) if len(x) >= len(weights) else np.nan,
        raw=True,
    )


def product(df: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """Rolling product."""
    return df.rolling(window, min_periods=max(1, window // 2)).apply(np.prod, raw=True)


def signed_power(df: pd.DataFrame, exp: float) -> pd.DataFrame:
    """Element-wise ``sign(x) * |x| ** exp``."""
    return df.apply(lambda x: np.sign(x) * (np.abs(x) ** exp))


# ---------------------------------------------------------------------------
# Sanitisation helper
# ---------------------------------------------------------------------------

def _sanitize(df: pd.DataFrame) -> pd.DataFrame:
    """Replace inf/-inf with 0 and fill remaining NaN with 0."""
    return df.replace([np.inf, -np.inf], 0.0).fillna(0.0)


# ---------------------------------------------------------------------------
# Alpha101Engine
# ---------------------------------------------------------------------------

class Alpha101Engine:
    """WorldQuant 101 Formulaic Alphas adapted for GRID.

    Reference: Kakushadze, Z. (2016). 101 Formulaic Alphas.

    These alphas operate on OHLCV data and produce cross-sectional signals
    with holding periods of approximately 0.6-6.4 days.

    Typical usage::

        engine = Alpha101Engine(db_engine, pit_store)
        results = engine.run_alpha_scan(
            tickers=["AAPL", "MSFT", "GOOG"],
            as_of_date=date(2026, 3, 28),
        )

    Attributes:
        engine: SQLAlchemy engine for database queries.
        pit_store: GRID point-in-time store for PIT-correct reads.
    """

    # Alpha categories for grouping
    MOMENTUM_ALPHAS: list[int] = [
        1, 7, 9, 10, 12, 19, 20, 24, 29, 30, 34, 35, 37, 39, 43, 46,
        49, 51, 52, 101,
    ]
    MEAN_REVERSION_ALPHAS: list[int] = [
        3, 4, 5, 6, 8, 11, 14, 15, 16, 17, 18, 22, 23, 25, 26, 27, 28,
        32, 33, 40, 41, 42, 44, 45, 47, 50, 53, 54, 55, 60,
    ]
    VOLUME_ALPHAS: list[int] = [
        2, 13, 36, 38, 61, 62, 64, 65, 66, 68, 71, 72, 73, 74, 75, 77,
        78, 81, 83, 84, 85, 86, 88, 92, 94, 95, 96, 98, 99,
    ]

    # All alphas that have an implementation in this class
    IMPLEMENTED_ALPHAS: list[int] = [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 13, 17, 20, 24, 30, 33, 34,
        35, 37, 41, 42, 43, 44, 52, 53, 54, 101,
    ]

    def __init__(self, db_engine: Engine, pit_store: PITStore) -> None:
        """Initialise the Alpha101 engine.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            pit_store: GRID point-in-time store instance.
        """
        self.engine = db_engine
        self.pit_store = pit_store
        log.info("Alpha101Engine initialised")

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_ohlcv(
        self,
        tickers: list[str],
        as_of_date: date,
        lookback_days: int = 504,
    ) -> dict[str, pd.DataFrame]:
        """Load OHLCV data for the requested tickers.

        Queries ``market_daily`` for the date range
        ``[as_of_date - lookback_days, as_of_date]`` and pivots into panel
        format (one DataFrame per field, dates as index, tickers as columns).

        Parameters:
            tickers: List of ticker symbols.
            as_of_date: PIT boundary date (inclusive).
            lookback_days: Calendar days of history to load.

        Returns:
            dict mapping field name ('open', 'high', 'low', 'close',
            'volume', 'vwap', 'returns') to a ``pd.DataFrame``.  Empty dict
            when no data is found.
        """
        start_date = as_of_date - timedelta(days=lookback_days)

        query = text("""
            SELECT ticker, signal_date, open_price, high_price, low_price,
                   close_price, volume, vwap
            FROM market_daily
            WHERE ticker = ANY(:tickers)
              AND signal_date BETWEEN :start AND :end
            ORDER BY signal_date
        """)

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(
                    query,
                    conn,
                    params={
                        "tickers": tickers,
                        "start": start_date,
                        "end": as_of_date,
                    },
                )
        except Exception as exc:
            log.error("Failed to load OHLCV data: {e}", e=str(exc))
            return {}

        if df.empty:
            log.warning(
                "No OHLCV data found for {n} tickers between {s} and {e}",
                n=len(tickers), s=start_date, e=as_of_date,
            )
            return {}

        log.info(
            "Loaded {rows} OHLCV rows for {t} tickers",
            rows=len(df), t=df["ticker"].nunique(),
        )

        # Pivot into panel format: dates x tickers
        result: dict[str, pd.DataFrame] = {}
        col_map = [
            ("open", "open_price"),
            ("high", "high_price"),
            ("low", "low_price"),
            ("close", "close_price"),
            ("volume", "volume"),
            ("vwap", "vwap"),
        ]
        for col, src in col_map:
            result[col] = df.pivot(
                index="signal_date", columns="ticker", values=src,
            )

        # Compute simple returns
        result["returns"] = result["close"].pct_change()

        # Approximate VWAP if the column is entirely null
        if result["vwap"].isna().all().all():
            log.warning("VWAP missing — approximating as (H+L+C)/3")
            result["vwap"] = (
                result["high"] + result["low"] + result["close"]
            ) / 3

        return result

    # ------------------------------------------------------------------
    # Individual alpha implementations
    # ------------------------------------------------------------------

    def _alpha001(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#1 — conditional volatility rank (momentum)."""
        inner = o["close"].copy()
        inner[o["returns"] < 0] = stddev(o["returns"], 20)
        return rank(ts_argmax(signed_power(inner, 2.0), 5))

    def _alpha002(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#2 — volume-price correlation."""
        return -1 * correlation(
            rank(delta(np.log(o["volume"].replace(0, 1)), 2)),
            rank((o["close"] - o["open"]) / o["open"].replace(0, np.nan)),
            6,
        )

    def _alpha003(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#3 — open-volume decorrelation."""
        return -1 * correlation(rank(o["open"]), rank(o["volume"]), 10)

    def _alpha004(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#4 — low rank momentum."""
        return -1 * ts_rank(rank(o["low"]), 9)

    def _alpha005(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#5 — VWAP deviation."""
        return (
            rank(o["open"] - ts_sum(o["vwap"], 10) / 10)
            * (-1 * rank(o["close"] - o["vwap"]).abs())
        )

    def _alpha006(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#6 — open-volume correlation."""
        return -1 * correlation(o["open"], o["volume"], 10)

    def _alpha007(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#7 — conditional momentum."""
        adv20 = sma(o["volume"], 20)
        alpha = (
            -1 * ts_rank(delta(o["close"], 7).abs(), 60)
            * np.sign(delta(o["close"], 7))
        )
        alpha[adv20 >= o["volume"]] = -1
        return alpha

    def _alpha008(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#8 — open-returns interaction."""
        product_now = ts_sum(o["open"], 5) * ts_sum(o["returns"], 5)
        return -1 * rank(product_now - delay(product_now, 10))

    def _alpha009(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#9 — conditional close delta."""
        d = delta(o["close"], 1)
        cond1 = ts_min(d, 5) > 0
        cond2 = ts_max(d, 5) < 0
        alpha = -1 * d
        alpha[cond1 | cond2] = d
        return alpha

    def _alpha010(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#10 — conditional close delta (window=4)."""
        d = delta(o["close"], 1)
        cond1 = ts_min(d, 4) > 0
        cond2 = ts_max(d, 4) < 0
        alpha = -1 * d
        alpha[cond1 | cond2] = d
        return alpha

    def _alpha012(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#12 — volume-change price reversal."""
        return np.sign(delta(o["volume"], 1)) * (-1 * delta(o["close"], 1))

    def _alpha013(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#13 — close-volume covariance rank."""
        return -1 * rank(covariance(rank(o["close"]), rank(o["volume"]), 5))

    def _alpha017(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#17 — multi-factor momentum."""
        adv20 = sma(o["volume"], 20)
        return (
            -1
            * rank(ts_rank(o["close"], 10))
            * rank(delta(delta(o["close"], 1), 1))
            * rank(ts_rank(o["volume"] / adv20.replace(0, np.nan), 5))
        )

    def _alpha020(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#20 — triple open-gap."""
        return (
            -1
            * rank(o["open"] - delay(o["high"], 1))
            * rank(o["open"] - delay(o["close"], 1))
            * rank(o["open"] - delay(o["low"], 1))
        )

    def _alpha024(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#24 — SMA deviation momentum."""
        cond = (
            delta(sma(o["close"], 100), 100)
            / delay(o["close"], 100).replace(0, np.nan)
        ) <= 0.05
        alpha = -1 * delta(o["close"], 3)
        alpha[cond] = -1 * (o["close"] - ts_min(o["close"], 100))
        return alpha

    def _alpha030(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#30 — sign persistence x volume."""
        d = delta(o["close"], 1)
        inner = np.sign(d) + np.sign(delay(d, 1)) + np.sign(delay(d, 2))
        vol_ratio = ts_sum(o["volume"], 5) / ts_sum(o["volume"], 20).replace(0, np.nan)
        return (1.0 - rank(inner)) * vol_ratio

    def _alpha033(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#33 — open / close ratio."""
        return rank(-1 + o["open"] / o["close"].replace(0, np.nan))

    def _alpha034(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#34 — volatility regime + momentum."""
        inner = stddev(o["returns"], 2) / stddev(o["returns"], 5).replace(0, np.nan)
        inner = inner.replace([np.inf, -np.inf], 1.0).fillna(1.0)
        return rank(2 - rank(inner) - rank(delta(o["close"], 1)))

    def _alpha035(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#35 — volume-price-return triple."""
        return (
            ts_rank(o["volume"], 32)
            * (1 - ts_rank(o["close"] + o["high"] - o["low"], 16))
            * (1 - ts_rank(o["returns"], 32))
        )

    def _alpha037(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#37 — lagged open-close correlation."""
        return (
            rank(correlation(delay(o["open"] - o["close"], 1), o["close"], 200))
            + rank(o["open"] - o["close"])
        )

    def _alpha041(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#41 — geometric mean vs VWAP."""
        return (o["high"] * o["low"]).pow(0.5) - o["vwap"]

    def _alpha042(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#42 — VWAP-close normalised."""
        denom = (o["vwap"] + o["close"]).replace(0, np.nan)
        return rank(o["vwap"] - o["close"]) / rank(denom)

    def _alpha043(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#43 — volume ratio x close delta."""
        adv20 = sma(o["volume"], 20)
        return (
            ts_rank(o["volume"] / adv20.replace(0, np.nan), 20)
            * ts_rank(-1 * delta(o["close"], 7), 8)
        )

    def _alpha044(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#44 — high-volume correlation."""
        return -1 * correlation(o["high"], rank(o["volume"]), 5)

    def _alpha052(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#52 — long-term momentum x volume."""
        long_ret = (
            ts_sum(o["returns"], 240) - ts_sum(o["returns"], 20)
        ) / 220
        return (
            (-1 * delta(ts_min(o["low"], 5), 5))
            * rank(long_ret)
            * ts_rank(o["volume"], 5)
        )

    def _alpha053(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#53 — intraday position change."""
        inner = (o["close"] - o["low"]).replace(0, 0.0001)
        return -1 * delta(
            ((o["close"] - o["low"]) - (o["high"] - o["close"])) / inner, 9,
        )

    def _alpha054(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#54 — low-close-high-open relationship."""
        inner = (o["low"] - o["high"]).replace(0, -0.0001)
        return (
            -1
            * (o["low"] - o["close"])
            * (o["open"] ** 5)
            / (inner * (o["close"].replace(0, np.nan) ** 5))
        )

    def _alpha101(self, o: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Alpha#101 — intraday momentum."""
        hl_range = (o["high"] - o["low"]) + 0.001
        return (o["close"] - o["open"]) / hl_range

    # ------------------------------------------------------------------
    # Dispatch and aggregation
    # ------------------------------------------------------------------

    def compute_alpha(
        self,
        alpha_num: int,
        ohlcv: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Compute a single alpha by number.

        Parameters:
            alpha_num: Alpha identifier (1-101).
            ohlcv: Panel data dict as returned by :meth:`_load_ohlcv`.

        Returns:
            DataFrame with tickers as columns and dates as index.

        Raises:
            ValueError: If the requested alpha is not implemented.
        """
        method_name = f"_alpha{alpha_num:03d}"
        method = getattr(self, method_name, None)
        if method is None:
            raise ValueError(
                f"Alpha#{alpha_num} is not implemented "
                f"(available: {self.IMPLEMENTED_ALPHAS})"
            )
        try:
            result = method(ohlcv)
            return _sanitize(result)
        except Exception as exc:
            log.warning(
                "Alpha#{n} computation failed: {e}", n=alpha_num, e=str(exc),
            )
            # Return zeros with same shape as close
            return pd.DataFrame(
                0.0,
                index=ohlcv["close"].index,
                columns=ohlcv["close"].columns,
            )

    def compute_all_alphas(
        self,
        ohlcv: dict[str, pd.DataFrame],
        alpha_nums: list[int] | None = None,
    ) -> dict[int, pd.DataFrame]:
        """Compute multiple alphas.

        Parameters:
            ohlcv: Panel data dict as returned by :meth:`_load_ohlcv`.
            alpha_nums: Specific alpha numbers to compute.  Defaults to all
                implemented alphas.

        Returns:
            dict mapping alpha number to its result DataFrame.
        """
        nums = alpha_nums if alpha_nums is not None else self.IMPLEMENTED_ALPHAS
        results: dict[int, pd.DataFrame] = {}

        for n in nums:
            if f"_alpha{n:03d}" not in dir(self):
                log.warning("Alpha#{n} not implemented — skipping", n=n)
                continue
            results[n] = self.compute_alpha(n, ohlcv)

        log.info("Computed {k}/{t} alphas", k=len(results), t=len(nums))
        return results

    def compute_composite_signal(
        self,
        ohlcv: dict[str, pd.DataFrame],
        category: str = "all",
    ) -> pd.DataFrame:
        """Combine alphas within a category into an equal-weighted composite.

        Parameters:
            ohlcv: Panel data dict as returned by :meth:`_load_ohlcv`.
            category: One of ``'momentum'``, ``'mean_reversion'``,
                ``'volume'``, or ``'all'``.

        Returns:
            Composite signal DataFrame (dates x tickers).

        Raises:
            ValueError: If *category* is unrecognised.
        """
        category_map: dict[str, list[int]] = {
            "momentum": self.MOMENTUM_ALPHAS,
            "mean_reversion": self.MEAN_REVERSION_ALPHAS,
            "volume": self.VOLUME_ALPHAS,
            "all": self.IMPLEMENTED_ALPHAS,
        }
        if category not in category_map:
            raise ValueError(
                f"Unknown category '{category}'. "
                f"Choose from: {list(category_map.keys())}"
            )

        target_nums = [
            n for n in category_map[category]
            if n in self.IMPLEMENTED_ALPHAS
        ]

        if not target_nums:
            log.warning(
                "No implemented alphas in category '{c}'", c=category,
            )
            return pd.DataFrame(
                0.0,
                index=ohlcv["close"].index,
                columns=ohlcv["close"].columns,
            )

        alphas = self.compute_all_alphas(ohlcv, alpha_nums=target_nums)

        # Stack and mean — each alpha contributes equally
        stacked = pd.concat(alphas.values(), axis=0)
        composite = stacked.groupby(stacked.index).mean()

        log.info(
            "Composite signal for '{c}': {n} alphas, {d} dates, {t} tickers",
            c=category,
            n=len(alphas),
            d=len(composite),
            t=len(composite.columns),
        )
        return _sanitize(composite)

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _persist_results(self, results: dict[str, Any], as_of_date: date) -> int | None:
        """Save alpha scan results to analytical_snapshots.

        Parameters:
            results: Scan output dict containing alphas, composites, and
                metadata.
            as_of_date: The decision date the scan was run for.

        Returns:
            Snapshot row ID, or None on failure.
        """
        from store.snapshots import AnalyticalSnapshotStore

        snap = AnalyticalSnapshotStore(self.engine)
        return snap.save_snapshot(
            category="alpha101",
            payload=results,
            as_of_date=as_of_date,
            metrics={"n_alphas": len(results.get("alphas", {}))},
        )

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run_alpha_scan(
        self,
        tickers: list[str],
        as_of_date: date,
        lookback_days: int = 504,
        persist: bool = True,
    ) -> dict[str, Any]:
        """Run a full alpha scan: load data, compute alphas, produce composites.

        Parameters:
            tickers: Ticker symbols to scan.
            as_of_date: PIT boundary date — no data beyond this date is used.
            lookback_days: Calendar days of history for the OHLCV load.
            persist: Whether to save results to ``analytical_snapshots``.

        Returns:
            dict with keys:
            - ``tickers``: list of tickers in the scan
            - ``as_of_date``: the decision date
            - ``alphas``: dict mapping alpha number to the most recent
              cross-sectional values (dict of ticker -> value)
            - ``composites``: dict mapping category name to latest values
            - ``snapshot_id``: database row ID (if persisted)
        """
        log.info(
            "Alpha scan starting — {n} tickers, as_of={d}, lookback={lb}d",
            n=len(tickers), d=as_of_date, lb=lookback_days,
        )

        ohlcv = self._load_ohlcv(tickers, as_of_date, lookback_days)
        if not ohlcv:
            log.warning("No data loaded — aborting alpha scan")
            return {
                "tickers": tickers,
                "as_of_date": as_of_date.isoformat(),
                "alphas": {},
                "composites": {},
                "snapshot_id": None,
            }

        # Compute all implemented alphas
        all_alphas = self.compute_all_alphas(ohlcv)

        # Latest cross-sectional values per alpha
        alpha_latest: dict[int, dict[str, float]] = {}
        for num, df in all_alphas.items():
            if not df.empty:
                alpha_latest[num] = df.iloc[-1].to_dict()

        # Composite signals per category
        composites: dict[str, dict[str, float]] = {}
        for cat in ("momentum", "mean_reversion", "volume", "all"):
            comp = self.compute_composite_signal(ohlcv, category=cat)
            if not comp.empty:
                composites[cat] = comp.iloc[-1].to_dict()

        results: dict[str, Any] = {
            "tickers": tickers,
            "as_of_date": as_of_date.isoformat(),
            "alphas": alpha_latest,
            "composites": composites,
            "snapshot_id": None,
        }

        if persist:
            try:
                snap_id = self._persist_results(results, as_of_date)
                results["snapshot_id"] = snap_id
                log.info("Alpha scan persisted — snapshot_id={id}", id=snap_id)
            except Exception as exc:
                log.error("Failed to persist alpha scan: {e}", e=str(exc))

        log.info(
            "Alpha scan complete — {n} alphas computed for {t} tickers",
            n=len(all_alphas), t=len(tickers),
        )
        return results
