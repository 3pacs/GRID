"""
Adaptive Rotation Strategy — adapted from FinRL-X.

The only live-verified system in the entire research sweep:
  Paper Trading (Oct 2025 - Mar 2026): Sharpe 1.96, +19.76% vs SPY -2.51%
  Backtest (Jan 2018 - Oct 2025): Sharpe 1.10, 22.32% annualized

Architecture:
  1. Regime detection: 26-week SPY trend + VIX state → risk-on/neutral/risk-off
  2. Fast risk-off: 3-day SPY drawdown < -3% OR VIX z-score > 3.0
  3. Group selection: Information Ratio vs QQQ per group, max 2 active
  4. Intra-group: 12-week risk-adjusted return, robust Z-score
  5. Risk: 5% absolute stop, 10% trailing stop, 20-day cooldown
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy.engine import Engine

from alpha_research.data.panel_builder import build_price_panel, get_vix_series

# ── Asset Groups (tickers GRID has data for) ──────────────────────────

ASSET_GROUPS = {
    "growth_tech": {
        "tickers": ["AAPL", "MSFT", "NVDA", "META", "AMZN", "GOOGL", "TSLA"],
        "max_positions": 2,
    },
    "real_assets": {
        "tickers": ["XOM", "CVX", "GLD", "SLV", "XLE"],
        "max_positions": 2,
    },
    "defensive": {
        "tickers": ["TLT", "XLU", "XLV", "XLB", "XLI"],
        "max_positions": 2,
    },
}

FALLBACK_TICKERS = ["SPY", "QQQ", "GLD", "XLU", "XLV"]
MAX_ACTIVE_GROUPS = 2
BENCHMARK = "QQQ"

# ── Regime Parameters ─────────────────────────────────────────────────

TREND_WEEKS = 26
VIX_ZSCORE_THRESHOLD = 3.0
DRAWDOWN_THRESHOLD = -0.03  # -3%
DRAWDOWN_WINDOW = 3  # days
FAST_RISK_OFF_DURATION = 10  # days
FAST_RISK_OFF_CASH_FLOOR = 0.50
RANKING_WEEKS = 12

# ── Risk Parameters ───────────────────────────────────────────────────

ABSOLUTE_STOP = 0.05  # 5%
TRAILING_STOP = 0.10  # 10%
COOLDOWN_DAYS = 20


@dataclass(frozen=True)
class RegimeState:
    """The strategy's current regime read.

    ``available=False`` (label "unavailable") means the inputs required to
    compute a genuine regime were missing or too short — NOT that risk-off
    or neutral was detected. ``spy_trend``/``vix_zscore`` are ``None`` in
    that case; a caller must never treat that ``None`` as ``0.0`` (see
    VERIFIED FACT 4 — the old code fabricated exactly that zero, which
    detect_regime's own thresholds then read as a calm/neutral market).
    """

    label: Literal["risk-on", "neutral", "risk-off", "unavailable"]
    spy_trend: float | None  # 26-week return
    vix_zscore: float | None
    fast_risk_off: bool
    max_groups: int
    cash_floor: float
    available: bool = True
    reason: str | None = None


@dataclass(frozen=True)
class GroupScore:
    name: str
    info_ratio: float
    top_tickers: list[str]
    ticker_scores: dict[str, float]


@dataclass
class PositionState:
    ticker: str
    entry_date: date
    entry_price: float
    peak_price: float
    peak_date: date
    cooldown_until: date | None = None

    def update_peak(self, price: float, dt: date) -> None:
        if price > self.peak_price:
            self.peak_price = price
            self.peak_date = dt


@dataclass(frozen=True)
class RotationResult:
    weights: dict[str, float]
    regime: RegimeState
    active_groups: list[str]
    group_scores: list[GroupScore]
    stopped_tickers: list[str]
    as_of_date: date


def detect_regime(
    spy_prices: pd.Series,
    vix_series: pd.Series,
    as_of_date: date,
) -> RegimeState:
    """
    26-week SPY trend + VIX z-score → regime label.
    Fast risk-off: 3-day drawdown < -3% OR VIX z-score > 3.0.

    Returns an unavailable RegimeState (label="unavailable", available=False)
    when there isn't enough SPY history (need >=130 obs for the 26-week
    trend) or VIX history (need >=20 obs for the z-score) to compute a
    genuine reading. The old behaviour of defaulting spy_trend/vix_zscore to
    0.0 silently manufactured a calm/neutral-looking regime out of missing
    data (VERIFIED FACT 4) — an absent input must say so, not read as zero.
    """
    reasons = []
    if len(spy_prices) < 130:
        reasons.append(f"SPY history has {len(spy_prices)} obs, need >= 130 for the 26-week trend")
    if len(vix_series) < 20:
        reasons.append(f"VIX history has {len(vix_series)} obs, need >= 20 for the z-score")

    if reasons:
        reason = "; ".join(reasons)
        log.warning("Regime unavailable: {r}", r=reason)
        return RegimeState(
            label="unavailable",
            spy_trend=None,
            vix_zscore=None,
            fast_risk_off=False,
            max_groups=0,
            cash_floor=1.0,
            available=False,
            reason=reason,
        )

    # 26-week (~130 trading days) SPY trend
    spy_trend = float(spy_prices.iloc[-1] / spy_prices.iloc[-130] - 1)

    # VIX z-score (20-day)
    vix_20d = vix_series.iloc[-20:]
    vix_zscore = float(
        (vix_series.iloc[-1] - vix_20d.mean()) / (vix_20d.std() + 1e-8)
    )

    # Fast risk-off check
    fast_risk_off = False
    if len(spy_prices) >= DRAWDOWN_WINDOW:
        recent = spy_prices.iloc[-DRAWDOWN_WINDOW:]
        drawdown_3d = float(recent.iloc[-1] / recent.iloc[0] - 1)
        if drawdown_3d < DRAWDOWN_THRESHOLD:
            fast_risk_off = True
    if vix_zscore > VIX_ZSCORE_THRESHOLD:
        fast_risk_off = True

    # Regime label
    if fast_risk_off:
        label = "risk-off"
    elif spy_trend > 0.05 and vix_zscore < 1.0:
        label = "risk-on"
    elif spy_trend < -0.05 or vix_zscore > 2.0:
        label = "risk-off"
    else:
        label = "neutral"

    # Regime-dependent parameters
    regime_params = {
        "risk-on": {"max_groups": 2, "cash_floor": 0.0},
        "neutral": {"max_groups": 2, "cash_floor": 0.20},
        "risk-off": {"max_groups": 1, "cash_floor": FAST_RISK_OFF_CASH_FLOOR},
    }
    params = regime_params[label]

    return RegimeState(
        label=label,
        spy_trend=round(spy_trend, 4),
        vix_zscore=round(vix_zscore, 4),
        fast_risk_off=fast_risk_off,
        max_groups=params["max_groups"],
        cash_floor=params["cash_floor"],
    )


def score_groups(
    prices: pd.DataFrame,
    benchmark_prices: pd.Series,
    ranking_weeks: int = RANKING_WEEKS,
) -> list[GroupScore]:
    """
    Score each asset group by Information Ratio vs benchmark.
    Within each group, rank tickers by 12-week risk-adjusted return.
    """
    trading_days = ranking_weeks * 5
    scores = []

    for group_name, group_def in ASSET_GROUPS.items():
        available = [t for t in group_def["tickers"] if t in prices.columns]
        if not available:
            continue

        group_prices = prices[available].iloc[-trading_days:]
        bench = benchmark_prices.iloc[-trading_days:]

        if len(group_prices) < 20 or len(bench) < 20:
            continue

        # Group return = equal-weighted average of members
        group_returns = group_prices.pct_change(fill_method=None).mean(axis=1)
        bench_returns = bench.pct_change(fill_method=None)

        excess = group_returns - bench_returns
        excess = excess.dropna()

        if len(excess) < 10 or excess.std() == 0:
            ir = 0.0
        else:
            ir = float(excess.mean() / excess.std() * np.sqrt(252))

        # Intra-group ranking: risk-adjusted return (robust z-score)
        ticker_scores = {}
        for ticker in available:
            t_prices = group_prices[ticker].dropna()
            if len(t_prices) < 20:
                continue
            t_ret = t_prices.pct_change(fill_method=None).dropna()
            if t_ret.std() == 0:
                continue
            # Robust z-score: median absolute deviation based
            med = t_ret.median()
            mad = (t_ret - med).abs().median()
            if mad == 0:
                z = float(t_ret.mean() / (t_ret.std() + 1e-8))
            else:
                z = float((t_ret.mean() - med) / (mad * 1.4826 + 1e-8))
            ticker_scores[ticker] = min(z, 20.0)  # cap at 20

        top = sorted(ticker_scores, key=ticker_scores.get, reverse=True)
        top = top[: group_def["max_positions"]]

        scores.append(
            GroupScore(
                name=group_name,
                info_ratio=round(ir, 4),
                top_tickers=top,
                ticker_scores=ticker_scores,
            )
        )

    scores.sort(key=lambda g: g.info_ratio, reverse=True)
    return scores


def check_stops(
    positions: dict[str, PositionState],
    current_prices: dict[str, float],
    as_of_date: date,
) -> list[str]:
    """Check absolute and trailing stops. Returns list of stopped tickers."""
    stopped = []
    for ticker, pos in positions.items():
        if ticker not in current_prices:
            continue

        price = current_prices[ticker]
        pos.update_peak(price, as_of_date)

        # Absolute stop: -5% from entry
        if price < pos.entry_price * (1 - ABSOLUTE_STOP):
            stopped.append(ticker)
            continue

        # Trailing stop: -10% from peak
        if price < pos.peak_price * (1 - TRAILING_STOP):
            stopped.append(ticker)

    return stopped


def run_rotation(
    engine: Engine,
    as_of_date: date | None = None,
    positions: dict[str, PositionState] | None = None,
) -> RotationResult:
    """
    Run one iteration of the Adaptive Rotation Strategy.

    Returns target weights and metadata.
    """
    if as_of_date is None:
        as_of_date = date.today()
    if positions is None:
        positions = {}

    # Build price data (need 26 weeks + buffer)
    start = as_of_date - timedelta(days=365)

    # Get all tickers we need
    all_tickers = set()
    for g in ASSET_GROUPS.values():
        all_tickers.update(g["tickers"])
    all_tickers.update(["SPY", "QQQ"])
    all_tickers.update(FALLBACK_TICKERS)

    prices = build_price_panel(
        engine,
        tickers=list(all_tickers),
        start_date=start,
        end_date=as_of_date,
    )

    if prices.empty or "SPY" not in prices.columns:
        reason = (
            "build_price_panel returned no rows"
            if prices.empty
            else "SPY missing from the price panel"
        )
        log.warning("Rotation inputs unavailable: {r} — returning no target weights", r=reason)
        return RotationResult(
            weights={},
            regime=RegimeState(
                label="unavailable",
                spy_trend=None,
                vix_zscore=None,
                fast_risk_off=False,
                max_groups=0,
                cash_floor=1.0,
                available=False,
                reason=reason,
            ),
            active_groups=[],
            group_scores=[],
            stopped_tickers=[],
            as_of_date=as_of_date,
        )

    prices = prices.ffill(limit=5)

    # Pull the real VIX series PIT-correctly and hand it to detect_regime as
    # found — including empty. detect_regime() needs >=20 obs to compute a
    # genuine z-score and returns an explicit unavailable regime otherwise
    # (see its docstring). VERIFIED FACT 2: a previous revision of this
    # function propped up an empty read with a fabricated single-value
    # series instead — that fallback path is gone; see
    # tests/test_adaptive_rotation.py's
    # test_run_rotation_returns_unavailable_regime_when_vix_missing.
    vix_series = get_vix_series(
        engine,
        start_date=as_of_date - timedelta(days=60),
        end_date=as_of_date,
        as_of_date=as_of_date,
    )

    # 1. Detect regime
    regime = detect_regime(prices["SPY"], vix_series, as_of_date)

    if not regime.available:
        log.warning(
            "Rotation regime unavailable ({r}) — returning no target weights",
            r=regime.reason,
        )
        return RotationResult(
            weights={},
            regime=regime,
            active_groups=[],
            group_scores=[],
            stopped_tickers=[],
            as_of_date=as_of_date,
        )

    # 2. Check stops on existing positions
    current_prices_dict = {}
    for ticker in positions:
        if ticker in prices.columns:
            last_price = prices[ticker].dropna().iloc[-1]
            current_prices_dict[ticker] = float(last_price)
    stopped = check_stops(positions, current_prices_dict, as_of_date)

    # 3. Score groups
    benchmark = prices[BENCHMARK] if BENCHMARK in prices.columns else prices["SPY"]
    group_scores = score_groups(prices, benchmark)

    # 4. Select top groups
    active_groups = group_scores[: min(regime.max_groups, MAX_ACTIVE_GROUPS)]

    # 5. Build weights
    weights: dict[str, float] = {}
    investable = 1.0 - regime.cash_floor

    if not active_groups:
        # Fallback: equal weight across safe assets
        avail_fallback = [t for t in FALLBACK_TICKERS if t in prices.columns]
        if avail_fallback:
            w = investable / len(avail_fallback)
            weights = {t: w for t in avail_fallback}
    else:
        weight_per_group = investable / len(active_groups)
        for group in active_groups:
            tickers_in_group = [
                t for t in group.top_tickers
                if t not in stopped  # exclude stopped tickers
                and t in prices.columns
            ]
            if not tickers_in_group:
                continue
            w = weight_per_group / len(tickers_in_group)
            for t in tickers_in_group:
                weights[t] = w

    return RotationResult(
        weights=weights,
        regime=regime,
        active_groups=[g.name for g in active_groups],
        group_scores=group_scores,
        stopped_tickers=stopped,
        as_of_date=as_of_date,
    )
