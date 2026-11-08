"""High-Sharpe ensemble — walk-forward top-k selection over GRID's existing rule-based strategies.

Paper only. Nothing here places an order, writes to the GRID database, or touches
the decision journal. The module answers one pre-registered question
(``docs/paper_log/high-sharpe-ensemble-v1-preregistration.md``): does picking the
few best of GRID's existing strategies by their *trailing out-of-sample* Sharpe,
shrunk toward zero and penalised for correlation, beat SPY buy-and-hold net of
costs when evaluated strictly walk-forward?

Design (all fixed in the registration before the first look):

* **Candidates** are deterministic rules that already exist in GRID — the adaptive
  rotation strategy, three of the ``151 Trading Strategies`` scans, two QuantaAlpha
  rank signals and the VIX/MA exposure scalar — each turned into target weights
  with a fixed, simple construction (:data:`CANDIDATES`). No parameter here is fit.
* **Point in time.** Every candidate sees only prices and VIX observed on or before
  the decision date (:class:`DecisionContext`, enforced by :class:`PanelPITStore`
  for the rotation strategy's own reads). Realised returns use the prices that
  followed. The price vintages are the ones GRID held on the run date, declared in
  the registration: backfilled history carries its backfill date as
  ``release_date``, so a strict release-date read would see nothing before 2025.
* **Selection** every ``selection_every`` segments: trailing-window annualised
  Sharpe of each candidate's realised net returns, shrunk by ``n / (n + n0)``,
  greedy top-``k`` with a penalty of ``corr_penalty`` × max correlation to the
  candidates already picked, only candidates with shrunk Sharpe above zero.
* **Weighting** is inverse trailing volatility over the selected candidates,
  summing to one. The ensemble's underlying weights are the weighted sum of the
  selected candidates' weights, so costs are charged once, on the underlying
  turnover, at ``cost_bps_per_side``.
* **Evaluation**: mean net excess return over SPY with a circular block bootstrap
  lower bound (the primary criterion), the deflated Sharpe ratio for the declared
  number of trials (:func:`alpha_research.validation.gauntlet.deflated_sharpe_ratio`)
  and the probability of backtest overfitting over the candidate matrix
  (:func:`alpha_research.validation.overfitting.probability_of_backtest_overfitting`).
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Sequence

import numpy as np
import pandas as pd
from loguru import logger as log
from scipy import stats

from alpha_research.validation.gauntlet import deflated_sharpe_ratio, trial_sharpe_variance
from alpha_research.validation.metrics import max_drawdown
from alpha_research.validation.overfitting import (
    OverfitInputError,
    probability_of_backtest_overfitting,
)
from analysis.offline_research_proof import autocorrelation_block
from store.pit import PITStore

# ── Registration ─────────────────────────────────────────────────────────────

#: The pre-registrations this module implements, one per candidate set. Each file's
#: LF-normalised sha256 is pinned here; :func:`registration_sha256` recomputes it and
#: the tests compare the two. Re-pin only before that set's look; afterwards a change
#: is a new set. v1's look was spent on 2026-11-08T07:26Z (FAIL).
REGISTRATIONS: dict[str, tuple[str, str]] = {
    "v1": (
        "docs/paper_log/high-sharpe-ensemble-v1-preregistration.md",
        "4c84ba9d3c36d92c32ae7a7d7bf7590115c33b415e4c3b8e859ebf5264a0cbc7",
    ),
    "v6": (
        "docs/paper_log/high-sharpe-ensemble-v6-preregistration.md",
        "37bedb1a862f782d8efd99abc336492607ecd03ac3d0a2916a23187c5ff8cc1a",
    ),
    "v5": (
        "docs/paper_log/high-sharpe-ensemble-v5-preregistration.md",
        "4c168b17317514aeec65298e28d8175bf04e1a17b68082e812d39a8459aab0de",
    ),
    "v4": (
        "docs/paper_log/high-sharpe-ensemble-v4-preregistration.md",
        "53ecca3881596128813d46ef9d0738bcbc93e1426a87d67427546a7dccb5ef89",
    ),
    "v3": (
        "docs/paper_log/high-sharpe-ensemble-v3-preregistration.md",
        "1670eae74e9c0f8bc9a538e81a250db3b38e8a856cab5d6863aedce592db29b5",
    ),
    "v2": (
        "docs/paper_log/high-sharpe-ensemble-v2-preregistration.md",
        "2e805286f124d068d1f3571c0822e915856f02ddab9aeb196326e09935ea376e",
    ),
}
PREREG_PATH, PREREG_SHA256 = REGISTRATIONS["v1"]

ENSEMBLE_VERSION = "high-sharpe-ensemble/6"

#: Trials spent before each set's look: 240 rotation-sweep configs + v1's 7 candidates
#: + v1's ensemble = 248; every later set adds its candidates and its ensemble.
TRIALS_BEFORE: dict[str, int] = {"v1": 240, "v2": 248, "v3": 261, "v4": 270, "v5": 278, "v6": 280}


def lf_sha256(data: bytes) -> str:
    """sha256 of *data* with CRLF normalised to LF (house convention)."""
    return hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()


def registration_sha256(repo_root: str | Path | None = None, set_name: str = "v1") -> str:
    """sha256 of a set's registration file as it sits in *repo_root* (default: this repo)."""
    root = Path(repo_root) if repo_root is not None else Path(__file__).resolve().parent.parent
    return lf_sha256((root / REGISTRATIONS[set_name][0]).read_bytes())


# ── Universe and candidates ─────────────────────────────────────────────────

#: Every ticker any candidate may hold. Fixed in the registration.
UNIVERSE: tuple[str, ...] = (
    # adaptive rotation groups + benchmark
    "AAPL", "MSFT", "NVDA", "META", "AMZN", "GOOGL", "TSLA",
    "XOM", "CVX", "GLD", "SLV", "XLE",
    "TLT", "XLU", "XLV", "XLB", "XLI",
    "SPY", "QQQ",
    # broad / sector / rates / credit ETFs with resolved_series history
    "IWM", "DIA", "XLF", "XLK", "XLP", "XLY",
    "IEF", "SHY", "HYG", "LQD", "USO",
)

#: Sector ETFs the sector-rotation candidate ranks (SPDR select sectors with history to 2016).
SECTOR_ETFS: tuple[str, ...] = ("XLE", "XLU", "XLV", "XLB", "XLI", "XLF", "XLK", "XLP", "XLY")

BENCHMARK = "SPY"
VIX_NAME = "VIX"


@dataclass(frozen=True)
class DecisionContext:
    """Everything a candidate may look at on a decision date.

    ``prices`` and ``vix`` end at ``as_of`` (inclusive); nothing later is present.
    ``engine`` and ``pit`` are only needed by :func:`adaptive_rotation_weights`.
    """

    as_of: date
    prices: pd.DataFrame
    vix: pd.Series
    engine: Any = None
    pit: PITStore | None = None


CandidateFn = Callable[[DecisionContext], dict[str, float]]


def _clean_weights(weights: dict[str, float], allowed: Sequence[str]) -> dict[str, float]:
    """Drop unknown tickers and non-finite weights; cap gross exposure at 1."""
    out = {
        str(t): float(w)
        for t, w in weights.items()
        if t in allowed and w is not None and math.isfinite(float(w)) and abs(float(w)) > 1e-12
    }
    gross = sum(abs(w) for w in out.values())
    if gross > 1.0 + 1e-9:
        out = {t: w / gross for t, w in out.items()}
    return out


def _long_short_equal(longs: Sequence[str], shorts: Sequence[str]) -> dict[str, float]:
    """0.5 gross per leg, equal weight within a leg; a missing leg leaves cash."""
    weights: dict[str, float] = {}
    if longs:
        for t in longs:
            weights[t] = weights.get(t, 0.0) + 0.5 / len(longs)
    if shorts:
        for t in shorts:
            weights[t] = weights.get(t, 0.0) - 0.5 / len(shorts)
    return weights


def _signals_to_weights(signals: Sequence[Any]) -> dict[str, float]:
    longs = [s.ticker for s in signals if s.direction == "LONG"]
    shorts = [s.ticker for s in signals if s.direction == "SHORT"]
    return _long_short_equal(longs, shorts)


def s151_engine(ctx: DecisionContext) -> Any:
    """A ``Strategy151Engine`` whose data loaders are never called (we pass panels)."""
    from trading.strategy151 import Strategy151Engine

    return Strategy151Engine(ctx.engine, ctx.pit)  # type: ignore[arg-type]


def s151_xs_momentum_weights(ctx: DecisionContext) -> dict[str, float]:
    """12-1 cross-sectional momentum, top 5 long / bottom 5 short (strategy151 §3.1)."""
    prices = ctx.prices.drop(columns=[c for c in (VIX_NAME,) if c in ctx.prices.columns])
    return _signals_to_weights(s151_engine(ctx).cross_sectional_momentum(prices, top_n=5))


def s151_mean_reversion_weights(ctx: DecisionContext) -> dict[str, float]:
    """20-day z-score mean reversion, entry |z| > 2 (strategy151 §3.9)."""
    prices = ctx.prices.drop(columns=[c for c in (VIX_NAME,) if c in ctx.prices.columns])
    return _signals_to_weights(s151_engine(ctx).mean_reversion_scan(prices))


def s151_sector_rotation_weights(ctx: DecisionContext) -> dict[str, float]:
    """Risk-adjusted multi-horizon sector momentum, top 3 long / bottom 3 short."""
    cols = [t for t in SECTOR_ETFS if t in ctx.prices.columns]
    return _signals_to_weights(s151_engine(ctx).sector_rotation(ctx.prices[cols], top_n=3))


def _quintile_long_short(rank_signal: pd.DataFrame) -> dict[str, float]:
    """Long the top quintile and short the bottom quintile of the last signal row."""
    if rank_signal.empty:
        return {}
    row = rank_signal.iloc[-1].dropna()
    if len(row) < 5:
        return {}
    n = max(1, len(row) // 5)
    ordered = row.sort_values()
    return _long_short_equal(list(ordered.index[-n:]), list(ordered.index[:n]))


def qa_dual_horizon_weights(ctx: DecisionContext) -> dict[str, float]:
    """QuantaAlpha dual-horizon momentum (equity-tuned), quintile long/short."""
    from alpha_research.signals.quanta_alpha import dual_horizon_equity

    prices = ctx.prices.drop(columns=[c for c in (VIX_NAME,) if c in ctx.prices.columns])
    return _quintile_long_short(dual_horizon_equity(prices.tail(120)))


def qa_vol_regime_weights(ctx: DecisionContext) -> dict[str, float]:
    """QuantaAlpha vol-regime adaptive momentum (equity-tuned), quintile long/short."""
    from alpha_research.signals.quanta_alpha import vol_regime_adaptive_equity

    prices = ctx.prices.drop(columns=[c for c in (VIX_NAME,) if c in ctx.prices.columns])
    return _quintile_long_short(vol_regime_adaptive_equity(prices.tail(160)))


VIX_MA_WINDOW = 20  # alpha_research.signals.exposure_scaler.MA_WINDOW


def vix_ma_exposure_spy_weights(ctx: DecisionContext) -> dict[str, float]:
    """SPY scaled by the VIX/MA(20) exposure scalar ``clip(1 - (vix/ma - 1), 0, 1)``."""
    vix = ctx.vix.dropna()
    if len(vix) < VIX_MA_WINDOW:
        return {}
    ma = float(vix.tail(VIX_MA_WINDOW).mean())
    if ma <= 0:
        return {}
    scalar = float(np.clip(1.0 - (float(vix.iloc[-1]) / ma - 1.0), 0.0, 1.0))
    return {BENCHMARK: scalar} if scalar > 0 else {}


def adaptive_rotation_weights(ctx: DecisionContext) -> dict[str, float]:
    """``adaptive_rotation.run_rotation`` with default constants, reading through ``ctx.pit``."""
    from alpha_research.strategies import adaptive_rotation as ar

    if ctx.pit is None:
        raise ValueError("adaptive_rotation needs a PanelPITStore in the decision context")
    result = ar.run_rotation(
        ctx.engine, as_of_date=ctx.as_of, positions={}, pit=ctx.pit, data_as_of=ctx.pit.data_as_of,
    )
    return dict(result.weights)


#: The registered universe of candidate strategies, in registration order.
CANDIDATES: dict[str, CandidateFn] = {
    "adaptive_rotation": adaptive_rotation_weights,
    "s151_xs_momentum": s151_xs_momentum_weights,
    "s151_mean_reversion": s151_mean_reversion_weights,
    "s151_sector_rotation": s151_sector_rotation_weights,
    "qa_dual_horizon_equity": qa_dual_horizon_weights,
    "qa_vol_regime_equity": qa_vol_regime_weights,
    "vix_ma_exposure_spy": vix_ma_exposure_spy_weights,
}


# ── v2: trend and dual momentum (registered 2026-11-08, cumulative trials 248 before) ──

#: Asset-class ETFs for time-series momentum and dual momentum.
V2_ASSETS: tuple[str, ...] = ("SPY", "QQQ", "IWM", "EFA", "EEM", "TLT", "IEF", "GLD", "DBC")
#: The eleven SPDR select sectors (XLC and XLRE are younger; the coverage rule decides).
V2_SECTORS: tuple[str, ...] = ("XLE", "XLU", "XLV", "XLB", "XLI", "XLF", "XLK", "XLP", "XLY", "XLC", "XLRE")
V2_TBILL = "SHY"   # absolute-momentum hurdle (T-bill proxy with history to 2014)
V2_BOND = "IEF"    # dual momentum's defensive leg (AGG has no history before 2020)
UNIVERSE_V2: tuple[str, ...] = tuple(dict.fromkeys(V2_ASSETS + V2_SECTORS + (V2_TBILL,)))

MOM_FORMATION = 252   # 12 months of sessions
MOM_SKIP = 21         # skip the most recent month
MA_WINDOW_10M = 210   # 10-month simple moving average in sessions


def momentum_12_1(prices: pd.DataFrame) -> pd.Series:
    """Return from 252 sessions ago to 21 sessions ago (Jegadeesh–Titman 12-1), per column."""
    if len(prices) < MOM_FORMATION + 1:
        return pd.Series(dtype=float)
    p_now = prices.iloc[-1 - MOM_SKIP]
    p_past = prices.iloc[-1 - MOM_FORMATION]
    return (p_now / p_past - 1.0).dropna()


def above_10m_ma(prices: pd.DataFrame) -> pd.Series:
    """True where the last close is above its 210-session simple moving average."""
    if len(prices) < MA_WINDOW_10M:
        return pd.Series(dtype=bool)
    ma = prices.iloc[-MA_WINDOW_10M:].mean()
    last = prices.iloc[-1]
    return (last > ma).reindex(prices.columns).fillna(False)


def _present(ctx: DecisionContext, names: Sequence[str]) -> list[str]:
    return [t for t in names if t in ctx.prices.columns]


def tsmom_assets_weights(ctx: DecisionContext, ma_filter: bool = False) -> dict[str, float]:
    """Long each asset-class ETF whose 12-1 momentum is positive, 1/N of the full set each."""
    cols = _present(ctx, V2_ASSETS)
    mom = momentum_12_1(ctx.prices[cols])
    if mom.empty:
        return {}
    keep = mom[mom > 0].index
    if ma_filter:
        ok = above_10m_ma(ctx.prices[cols])
        keep = [t for t in keep if bool(ok.get(t, False))]
    return {t: 1.0 / len(V2_ASSETS) for t in keep}


def tsmom_sectors_top3_weights(ctx: DecisionContext, ma_filter: bool = False) -> dict[str, float]:
    """Top 3 sectors by 12-1 momentum, equal weight, only those with positive momentum."""
    cols = _present(ctx, V2_SECTORS)
    mom = momentum_12_1(ctx.prices[cols])
    if mom.empty:
        return {}
    top = mom.sort_values(ascending=False).head(3)
    keep = list(top[top > 0].index)
    if ma_filter:
        ok = above_10m_ma(ctx.prices[cols])
        keep = [t for t in keep if bool(ok.get(t, False))]
    return {t: 1.0 / 3.0 for t in keep}


def gem_dual_momentum_weights(ctx: DecisionContext) -> dict[str, float]:
    """Antonacci GEM: SPY vs EFA by 12-month return; hold the winner if it beats SHY, else IEF."""
    cols = _present(ctx, ("SPY", "EFA", V2_TBILL, V2_BOND))
    if len(ctx.prices) < MOM_FORMATION + 1 or not {"SPY", V2_TBILL, V2_BOND} <= set(cols):
        return {}
    ret12 = (ctx.prices[cols].iloc[-1] / ctx.prices[cols].iloc[-1 - MOM_FORMATION] - 1.0).dropna()
    equities = [t for t in ("SPY", "EFA") if t in ret12.index]
    if not equities:
        return {}
    winner = max(equities, key=lambda t: float(ret12[t]))
    if float(ret12[winner]) > float(ret12.get(V2_TBILL, 0.0)):
        return {winner: 1.0}
    return {V2_BOND: 1.0}


def ma10_spy_weights(ctx: DecisionContext) -> dict[str, float]:
    """Faber's rule: SPY when above its 10-month SMA, else cash."""
    if BENCHMARK not in ctx.prices.columns:
        return {}
    ok = above_10m_ma(ctx.prices[[BENCHMARK]])
    return {BENCHMARK: 1.0} if bool(ok.get(BENCHMARK, False)) else {}


class Cadenced:
    """Re-decide only on the first decision date of each new calendar period; hold otherwise.

    ``period`` is a pandas period alias ("M" for calendar month). Stateful across the
    walk-forward's date loop, so build a fresh instance per run (see :func:`candidate_set`).
    """

    def __init__(self, fn: CandidateFn, period: str = "M") -> None:
        self.fn = fn
        self.period = period
        self._last_period: Any = None
        self._weights: dict[str, float] = {}

    def __call__(self, ctx: DecisionContext) -> dict[str, float]:
        current = pd.Timestamp(ctx.as_of).to_period(self.period)
        if current != self._last_period:
            self._weights = dict(self.fn(ctx))
            self._last_period = current
        return dict(self._weights)


def v2_candidates() -> dict[str, CandidateFn]:
    """The twelve registered v2 arms: six rules × {monthly, weekly}; fresh state per call."""
    rules: dict[str, CandidateFn] = {
        "tsmom_12_1_assets": lambda ctx: tsmom_assets_weights(ctx, ma_filter=False),
        "tsmom_12_1_assets_ma10": lambda ctx: tsmom_assets_weights(ctx, ma_filter=True),
        "tsmom_12_1_sectors_top3": lambda ctx: tsmom_sectors_top3_weights(ctx, ma_filter=False),
        "tsmom_12_1_sectors_top3_ma10": lambda ctx: tsmom_sectors_top3_weights(ctx, ma_filter=True),
        "gem_dual_momentum": gem_dual_momentum_weights,
        "ma10_spy": ma10_spy_weights,
    }
    out: dict[str, CandidateFn] = {}
    for name, fn in rules.items():
        out[f"{name}_M"] = Cadenced(fn, "M")
        out[f"{name}_W"] = fn
    return out


# ── v3: volatility-managed and risk parity (registered 2026-11-08, 261 trials before) ──

V3_RISK_PARITY: tuple[str, ...] = ("SPY", "TLT", "GLD", "DBC")
UNIVERSE_V3: tuple[str, ...] = ("SPY", "QQQ", "TLT", "IEF", "GLD", "DBC")
VOL_TARGET_ANNUAL = 0.10      # Moreira–Muir style target, 10 % a year
VOL_WINDOW = 20               # sessions of realised volatility for the target
RP_VOL_WINDOW = 60            # sessions for inverse-volatility risk parity
MAX_LEVERAGE = 1.0            # paper book never borrows


def realised_vol(prices: pd.DataFrame, window: int) -> pd.Series:
    """Annualised standard deviation of daily log returns over the last ``window`` sessions."""
    if len(prices) < window + 1:
        return pd.Series(dtype=float)
    rets = np.log(prices.iloc[-(window + 1):]).diff().dropna()
    return rets.std(ddof=1) * math.sqrt(252)


def vol_target_weights(ctx: DecisionContext, ticker: str) -> dict[str, float]:
    """``min(target / realised vol, 1)`` in ``ticker``, cash remainder."""
    if ticker not in ctx.prices.columns:
        return {}
    vol = realised_vol(ctx.prices[[ticker]], VOL_WINDOW)
    v = float(vol.get(ticker, float("nan")))
    if not math.isfinite(v) or v <= 0:
        return {}
    return {ticker: float(min(VOL_TARGET_ANNUAL / v, MAX_LEVERAGE))}


def risk_parity_weights(ctx: DecisionContext) -> dict[str, float]:
    """Inverse 60-session volatility over SPY/TLT/GLD/DBC, fully invested (sums to one)."""
    cols = _present(ctx, V3_RISK_PARITY)
    vol = realised_vol(ctx.prices[cols], RP_VOL_WINDOW).dropna()
    vol = vol[vol > 0]
    if vol.empty:
        return {}
    inv = 1.0 / vol
    return {t: float(w) for t, w in (inv / inv.sum()).items()}


def sixty_forty_vol_scaled_weights(ctx: DecisionContext) -> dict[str, float]:
    """60 % SPY / 40 % IEF, scaled by ``min(target / realised portfolio vol, 1)``."""
    if not {"SPY", "IEF"} <= set(ctx.prices.columns):
        return {}
    if len(ctx.prices) < VOL_WINDOW + 1:
        return {}
    rets = np.log(ctx.prices[["SPY", "IEF"]].iloc[-(VOL_WINDOW + 1):]).diff().dropna()
    port = 0.6 * rets["SPY"] + 0.4 * rets["IEF"]
    v = float(port.std(ddof=1) * math.sqrt(252))
    if not math.isfinite(v) or v <= 0:
        return {}
    scale = float(min(VOL_TARGET_ANNUAL / v, MAX_LEVERAGE))
    return {"SPY": 0.6 * scale, "IEF": 0.4 * scale}


def v3_candidates() -> dict[str, CandidateFn]:
    """The eight registered v3 arms: four rules × {monthly, weekly}; fresh state per call."""
    rules: dict[str, CandidateFn] = {
        "vol_target_spy": lambda ctx: vol_target_weights(ctx, "SPY"),
        "vol_target_qqq": lambda ctx: vol_target_weights(ctx, "QQQ"),
        "risk_parity_4": risk_parity_weights,
        "sixty_forty_vol_scaled": sixty_forty_vol_scaled_weights,
    }
    out: dict[str, CandidateFn] = {}
    for name, fn in rules.items():
        out[f"{name}_M"] = Cadenced(fn, "M")
        out[f"{name}_W"] = fn
    return out


# ── v4: short-horizon mean reversion on SPY / QQQ, daily segments (270 trials before) ──

UNIVERSE_V4: tuple[str, ...] = ("SPY", "QQQ")
RSI_PERIOD = 2
RSI_ENTRY = 10.0
RSI_EXIT = 60.0
TREND_MA = 200
DOWN_DAYS = 3
TOM_DAYS_BEFORE = 4   # turn of month: last 4 sessions of a month ...
TOM_DAYS_AFTER = 3    # ... and the first 3 of the next


def rsi(prices: pd.Series, period: int = RSI_PERIOD) -> float:
    """Wilder-free simple RSI over the last ``period`` changes (Connors' RSI(2) uses this form)."""
    d = prices.diff().dropna().iloc[-period:]
    if len(d) < period:
        return float("nan")
    up = float(d[d > 0].sum())
    down = float(-d[d < 0].sum())
    if up + down == 0:
        return 50.0
    return 100.0 * up / (up + down)


def rsi2_weights(ctx: DecisionContext, ticker: str) -> dict[str, float]:
    """Connors RSI(2): long when RSI(2) < 10 above the 200-day MA; stay long until RSI(2) > 60.

    Stateless form: the position is on if the most recent entry (RSI below 10 above the MA) is
    more recent than the most recent exit (RSI above 60), looking back 10 sessions.
    """
    if ticker not in ctx.prices.columns:
        return {}
    px = ctx.prices[ticker].dropna()
    if len(px) < TREND_MA + 12:
        return {}
    ma = float(px.iloc[-TREND_MA:].mean())
    last_entry = last_exit = None
    for back in range(10, -1, -1):
        window = px.iloc[: len(px) - back]
        r = rsi(window)
        above = float(window.iloc[-1]) > float(window.iloc[-TREND_MA:].mean()) if back else float(px.iloc[-1]) > ma
        if r < RSI_ENTRY and above:
            last_entry = back
        if r > RSI_EXIT:
            last_exit = back
    if last_entry is not None and (last_exit is None or last_entry < last_exit):
        return {ticker: 1.0}
    return {}


def three_down_weights(ctx: DecisionContext, ticker: str) -> dict[str, float]:
    """Long after three consecutive lower closes above the 200-day MA; flat once the close exceeds the prior close."""
    if ticker not in ctx.prices.columns:
        return {}
    px = ctx.prices[ticker].dropna()
    if len(px) < TREND_MA + DOWN_DAYS + 2:
        return {}
    above = float(px.iloc[-1]) > float(px.iloc[-TREND_MA:].mean())
    d = px.diff().dropna()
    if above and bool((d.iloc[-DOWN_DAYS:] < 0).all()):
        return {ticker: 1.0}
    # still in: entered within the last 5 sessions and no up close since the entry
    for back in range(1, 6):
        seg = d.iloc[: len(d) - back]
        if len(seg) >= DOWN_DAYS and bool((seg.iloc[-DOWN_DAYS:] < 0).all()) and bool((d.iloc[len(d) - back :] <= 0).all()):
            return {ticker: 1.0}
    return {}


def turn_of_month_weights(ctx: DecisionContext, ticker: str) -> dict[str, float]:
    """Long over the last 4 and first 3 sessions of each calendar month (Ariel / Lakonishok–Smidt)."""
    if ticker not in ctx.prices.columns:
        return {}
    idx = ctx.prices.index
    if len(idx) < 30:
        return {}
    today = idx[-1]
    month_sessions = idx[(idx.year == today.year) & (idx.month == today.month)]
    pos = int(month_sessions.get_loc(today)) + 1          # 1-based session number within the month
    if pos <= TOM_DAYS_AFTER:
        return {ticker: 1.0}
    # last 4 sessions: count remaining business days in the month from the NYSE-ish bdate calendar
    month_end = (today + pd.offsets.MonthEnd(0)).normalize()
    remaining = len(pd.bdate_range(today, month_end)) - 1  # business days after today in this month
    if remaining < TOM_DAYS_BEFORE:
        return {ticker: 1.0}
    return {}


def v4_candidates() -> dict[str, CandidateFn]:
    """The six registered v4 arms, all daily (no cadence wrapper)."""
    return {
        "rsi2_spy": lambda ctx: rsi2_weights(ctx, "SPY"),
        "rsi2_qqq": lambda ctx: rsi2_weights(ctx, "QQQ"),
        "three_down_spy": lambda ctx: three_down_weights(ctx, "SPY"),
        "three_down_qqq": lambda ctx: three_down_weights(ctx, "QQQ"),
        "turn_of_month_spy": lambda ctx: turn_of_month_weights(ctx, "SPY"),
        "turn_of_month_qqq": lambda ctx: turn_of_month_weights(ctx, "QQQ"),
    }


#: v4 runs on daily segments: 252 per year, trailing 252, re-select every 21, and the look
#: runs the selector at 20 bp (primary) and 10 bp (secondary) — two ensemble configurations.
V4_CONFIG: dict[str, Any] = {"rebalance": "B", "periods_per_year": 252, "trailing": 252, "selection_every": 21}


# ── v5: combinations of the arms that beat SPY's Sharpe in their own look (278 before) ──

#: Arms whose out-of-sample annualised Sharpe exceeded SPY buy-and-hold's on the same segments
#: in their own registered look (weekly sets v1–v3; no v4 arm qualified). Fixed from the records
#: in docs/reports/high-sharpe-ensemble-v{1,2,3}-look-*.json; a test re-derives this list.
V5_QUALIFIED: dict[str, str] = {
    "adaptive_rotation": "v1",
    "vix_ma_exposure_spy": "v1",
    "tsmom_12_1_assets_ma10_M": "v2",
    "tsmom_12_1_assets_ma10_W": "v2",
    "ma10_spy_W": "v2",
    "vol_target_spy_M": "v3",
    "vol_target_spy_W": "v3",
    "vol_target_qqq_M": "v3",
    "vol_target_qqq_W": "v3",
    "risk_parity_4_M": "v3",
    "risk_parity_4_W": "v3",
    "sixty_forty_vol_scaled_M": "v3",
    "sixty_forty_vol_scaled_W": "v3",
}
UNIVERSE_V5: tuple[str, ...] = tuple(dict.fromkeys(UNIVERSE + V2_ASSETS + UNIVERSE_V3))

#: v5 primary: hold every qualified arm, inverse-volatility weighted, no selection
#: (k = all, no Sharpe floor, no correlation penalty); secondary: the v1 selector unchanged.
V5_PRIMARY: dict[str, Any] = {"k": len(V5_QUALIFIED), "min_shrunk_sharpe": -1e9, "corr_penalty": 0.0}
V5_SECONDARY: dict[str, Any] = {}


def v5_candidates() -> dict[str, CandidateFn]:
    """The thirteen qualified arms, built from their own sets' factories (fresh state)."""
    pools = {"v1": dict(CANDIDATES), "v2": v2_candidates(), "v3": v3_candidates()}
    return {name: pools[origin][name] for name, origin in V5_QUALIFIED.items()}


# ── v6: regime-conditional selection over every weekly arm of v1–v3 (280 before) ──

#: Regime cells of trading.regime_ensemble; each arm × cell is a trial for the deflated Sharpe.
V6_CELLS = 6
#: Long-sample arms (trading.regime_ensemble.long_sample_candidates) counted in the same set.
V6_LONG_SAMPLE_ARMS = 8


def v6_candidates() -> dict[str, CandidateFn]:
    """Every weekly arm registered in v1, v2 and v3 (27), fresh state per call."""
    out: dict[str, CandidateFn] = dict(CANDIDATES)
    out.update(v2_candidates())
    out.update(v3_candidates())
    return out


def v6_trials_added() -> int:
    """27 ETF arms × 6 cells + 8 long-sample arms × 6 cells + 2 ensembles (ETF conditional, long-sample conditional)."""
    return len(v6_candidates()) * V6_CELLS + V6_LONG_SAMPLE_ARMS * V6_CELLS + 2


@dataclass(frozen=True)
class CandidateSet:
    """One registered set: its universe, candidates, trial accounting and verdict rule."""

    name: str
    universe: tuple[str, ...]
    factory: Callable[[], dict[str, CandidateFn]]
    trials_before: int
    criterion: str  # "v1_pass" or "watch"

    def candidates(self) -> dict[str, CandidateFn]:
        return self.factory()

    count_candidates: bool = True  # False when every arm was already counted in an earlier set

    @property
    def n_trials_cumulative(self) -> int:
        """Trials before this set + this set's new candidates + this set's ensemble configuration(s)."""
        new_arms = len(self.factory()) if self.count_candidates else 0
        return self.trials_before + new_arms + self.extra_ensembles

    extra_ensembles: int = 1
    config_overrides: dict[str, Any] = field(default_factory=dict)
    secondary_overrides: dict[str, Any] | None = None  # a second, pre-stated ensemble configuration

    def config(self, **overrides: Any) -> "EnsembleConfig":
        merged = {**self.config_overrides, **overrides}
        return EnsembleConfig(n_trials_declared=self.n_trials_cumulative, criterion=self.criterion, **merged)


def candidate_set(name: str) -> CandidateSet:
    if name == "v1":
        return CandidateSet("v1", UNIVERSE, lambda: dict(CANDIDATES), TRIALS_BEFORE["v1"], "v1_pass")
    if name == "v2":
        return CandidateSet("v2", UNIVERSE_V2, v2_candidates, TRIALS_BEFORE["v2"], "watch")
    if name == "v3":
        return CandidateSet("v3", UNIVERSE_V3, v3_candidates, TRIALS_BEFORE["v3"], "watch")
    if name == "v6":
        return CandidateSet(
            "v6", UNIVERSE_V5, v6_candidates, TRIALS_BEFORE["v6"], "watch",
            extra_ensembles=v6_trials_added(), count_candidates=False,
            config_overrides={"regime_min_obs": 26},
        )
    if name == "v5":
        return CandidateSet(
            "v5", UNIVERSE_V5, v5_candidates, TRIALS_BEFORE["v5"], "watch",
            extra_ensembles=2, config_overrides=V5_PRIMARY, secondary_overrides=V5_SECONDARY,
            count_candidates=False,
        )
    if name == "v4":
        return CandidateSet(
            "v4", UNIVERSE_V4, v4_candidates, TRIALS_BEFORE["v4"], "watch",
            extra_ensembles=2, config_overrides={**V4_CONFIG, "cost_bps_per_side": 20.0},
        )
    raise KeyError(f"unknown candidate set {name!r}")


# ── Point-in-time panel store ────────────────────────────────────────────────


class PanelPITStore(PITStore):
    """A ``PITStore`` that answers from an in-memory, already-loaded price panel.

    ``run_rotation`` reads prices through ``build_price_panel(..., pit=...)`` and VIX
    through ``get_vix_series(..., pit=...)``; both call ``feature_ids_by_name`` and
    then ``get_feature_matrix`` / ``get_pit``. This store serves those calls from the
    panel and refuses any read whose ``end_date`` lies after the decision date it
    was positioned at (:meth:`position`), so the rotation strategy cannot see
    forward prices even by accident. ``data_as_of`` is the vintage date of the
    panel, recorded for provenance.
    """

    def __init__(self, panel: pd.DataFrame, vix: pd.Series, *, data_as_of: date) -> None:
        super().__init__(None)  # type: ignore[arg-type]
        self.panel = panel.sort_index()
        self.vix = vix.sort_index()
        self.data_as_of = data_as_of
        self.decision_date: date | None = None
        names = [f"{t.lower()}_full" for t in self.panel.columns] + ["vix_spot"]
        self._name_to_id = {n: i + 1 for i, n in enumerate(names)}
        self._id_to_col = {i + 1: t for i, t in enumerate(self.panel.columns)}
        self._vix_id = self._name_to_id["vix_spot"]
        self.reads = 0

    def position(self, decision_date: date) -> None:
        self.decision_date = decision_date

    def _guard(self, end_date: date) -> None:
        if self.decision_date is not None and end_date > self.decision_date:
            raise ValueError(
                f"LOOKAHEAD: read to {end_date} requested at decision date {self.decision_date}"
            )

    def feature_ids_by_name(self, names: list[str]) -> dict[str, int]:
        return {n: self._name_to_id[n] for n in names if n in self._name_to_id}

    def get_feature_matrix(
        self,
        feature_ids: list[int],
        start_date: date,
        end_date: date,
        as_of_date: date,
        vintage_policy: str = "FIRST_RELEASE",
    ) -> pd.DataFrame:
        self._guard(end_date)
        self.reads += 1
        cols = [self._id_to_col[i] for i in feature_ids if i in self._id_to_col]
        frame = self.panel.loc[str(start_date):str(end_date), cols]
        frame = frame.rename(columns={t: self._name_to_id[f"{t.lower()}_full"] for t in cols})
        frame.index.name = "obs_date"
        return frame

    def get_pit(
        self,
        feature_ids: list[int],
        as_of_date: date,
        vintage_policy: str = "LATEST_AS_OF",
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        end = end_date or as_of_date
        self._guard(end)
        self.reads += 1
        if list(feature_ids) != [self._vix_id]:
            raise ValueError("PanelPITStore.get_pit serves the VIX feature only")
        s = self.vix.loc[str(start_date) if start_date else None : str(end)].dropna()
        obs = [d.date() for d in s.index]
        return pd.DataFrame(
            {
                "feature_id": self._vix_id,
                "obs_date": obs,
                "value": s.to_numpy(dtype=float),
                "release_date": obs,
                "vintage_date": obs,
            }
        )


# ── Walk-forward engine ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class EnsembleConfig:
    """Every knob, fixed by the registration. ``as_dict`` is written into the report."""

    k: int = 3
    trailing: int = 52  # segments in the selection window
    selection_every: int = 4  # re-select every N segments
    shrink_n0: int = 52
    corr_penalty: float = 0.5
    min_shrunk_sharpe: float = 0.0
    cost_bps_per_side: float = 10.0
    periods_per_year: int = 52
    rebalance: str = "W-FRI"
    bootstrap_reps: int = 10_000
    seed: int = 20261108
    alpha: float = 0.05
    n_trials_declared: int = 248  # 7 candidates + 1 ensemble + 240 rotation sweep configs
    dsr_min: float = 0.95
    max_abs_segment_return: float = 0.5  # a single-name segment return beyond this is a data fault
    criterion: str = "v1_pass"  # "v1_pass" (v1 registration) or "watch" (v2 onward)
    regime_min_obs: int = 26  # regime-conditional selection needs this many segments in the cell, else falls back

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def rebalance_dates(index: pd.DatetimeIndex, start: date, end: date, rule: str = "W-FRI") -> list[date]:
    """Last trading day of each ``rule`` period (pandas offset alias) inside ``[start, end]``."""
    idx = index[(index >= pd.Timestamp(start)) & (index <= pd.Timestamp(end))]
    if len(idx) == 0:
        return []
    marks = pd.Series(idx, index=idx).resample(rule).max().dropna()
    return [pd.Timestamp(d).date() for d in marks.to_numpy()]


def segment_returns(prices: pd.DataFrame, dates: Sequence[date], cap: float) -> pd.DataFrame:
    """Per-ticker return over each segment ``[dates[i], dates[i+1]]``; rows = segment start.

    Returns beyond ``±cap`` are set to NaN (a data fault, reported) rather than
    clipped, so a bad print cannot manufacture a 50 % week.
    """
    stamps = pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
    p = prices.reindex(stamps)
    r = p.shift(-1) / p - 1.0
    r = r.iloc[:-1]
    r = r.where(r.abs() <= cap)
    return r


def path_returns(
    weights: pd.DataFrame, seg_ret: pd.DataFrame, cost_bps_per_side: float
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Gross, net and turnover series for a weight path (rows = segment start dates).

    Gross = Σ w·r over the segment; turnover = Σ|w_t − w_{t−1}| (first row Σ|w_0|);
    cost = turnover × bps / 1e4. Missing (NaN) returns on a held name count as zero
    and are reported by the caller as faults.
    """
    w = weights.reindex(index=seg_ret.index, columns=seg_ret.columns).fillna(0.0)
    r = seg_ret.reindex(columns=w.columns).fillna(0.0)
    gross = (w * r).sum(axis=1)
    prev = w.shift(1).fillna(0.0)
    turnover = (w - prev).abs().sum(axis=1)
    net = gross - turnover * cost_bps_per_side / 1e4
    return gross, net, turnover


def annualised_sharpe(returns: pd.Series, periods_per_year: int) -> float:
    r = returns.dropna()
    if len(r) < 2 or float(r.std(ddof=1)) == 0.0:
        return 0.0
    return float(r.mean() / r.std(ddof=1) * math.sqrt(periods_per_year))


def shrunk_sharpe(returns: pd.Series, n0: int, periods_per_year: int) -> float:
    """Annualised Sharpe times ``n / (n + n0)``: a fixed shrinkage toward zero."""
    n = int(returns.dropna().shape[0])
    if n == 0:
        return 0.0
    return annualised_sharpe(returns, periods_per_year) * n / (n + n0)


def select_top_k(
    trailing: pd.DataFrame, k: int, n0: int, corr_penalty: float, min_shrunk: float, periods_per_year: int
) -> list[str]:
    """Greedy top-k by shrunk Sharpe with a correlation penalty against the picks so far."""
    scores = {c: shrunk_sharpe(trailing[c], n0, periods_per_year) for c in trailing.columns}
    eligible = [c for c, s in scores.items() if s > min_shrunk]
    if not eligible:
        return []
    corr = trailing[eligible].corr().fillna(0.0)
    chosen: list[str] = []
    while eligible and len(chosen) < k:
        best, best_score = None, -math.inf
        for c in eligible:
            penalty = max((float(corr.loc[c, p]) for p in chosen), default=0.0)
            adjusted = scores[c] - corr_penalty * max(penalty, 0.0)
            if adjusted > best_score:
                best, best_score = c, adjusted
        if best is None or best_score <= min_shrunk:
            break
        chosen.append(best)
        eligible.remove(best)
    return chosen


def inverse_vol_weights(trailing: pd.DataFrame, selected: Sequence[str]) -> dict[str, float]:
    """Weights ∝ 1 / trailing std over the selected candidates, summing to one."""
    if not selected:
        return {}
    vols = {c: float(trailing[c].dropna().std(ddof=1)) for c in selected}
    inv = {c: (1.0 / v if v and math.isfinite(v) and v > 0 else 0.0) for c, v in vols.items()}
    total = sum(inv.values())
    if total <= 0:
        return {c: 1.0 / len(selected) for c in selected}
    return {c: v / total for c, v in inv.items()}


@dataclass
class EnsembleRun:
    """Everything the walk-forward produced; ``report`` turns it into numbers."""

    config: EnsembleConfig
    dates: list[date]
    seg_ret: pd.DataFrame
    candidate_weights: dict[str, pd.DataFrame]
    candidate_net: pd.DataFrame
    candidate_gross: pd.DataFrame
    candidate_turnover: pd.DataFrame
    ensemble_weights: pd.DataFrame  # rows = segment start, cols = candidates (allocation a_c)
    ensemble_underlying: pd.DataFrame
    ensemble_net: pd.Series
    ensemble_gross: pd.Series
    ensemble_turnover: pd.Series
    equal_weight_net: pd.Series
    benchmark_net: pd.Series
    selections: list[dict[str, Any]] = field(default_factory=list)
    faults: dict[str, Any] = field(default_factory=dict)
    regime_path: pd.Series | None = None  # cell label in force at each segment start (as known then)

    @property
    def oos_index(self) -> pd.DatetimeIndex:
        """Every segment after the warm-up, cash segments included."""
        return self.seg_ret.index[self.config.trailing :]


def compute_candidate_paths(
    prices: pd.DataFrame,
    vix: pd.Series,
    dates: Sequence[date],
    candidates: dict[str, CandidateFn],
    *,
    engine: Any = None,
    pit: PanelPITStore | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, int]]:
    """Target weights of every candidate on every decision date, each from data ≤ that date."""
    paths: dict[str, list[pd.Series]] = {n: [] for n in candidates}
    errors: dict[str, int] = {n: 0 for n in candidates}
    cols = list(prices.columns)
    for d in dates:
        ts = pd.Timestamp(d)
        ctx = DecisionContext(as_of=d, prices=prices.loc[:ts], vix=vix.loc[:ts], engine=engine, pit=pit)
        if pit is not None:
            pit.position(d)
        for name, fn in candidates.items():
            try:
                w = _clean_weights(fn(ctx), cols)
            except Exception as exc:  # noqa: BLE001 - one bad date must not kill the run
                errors[name] += 1
                log.warning("candidate {n} failed on {d}: {e}", n=name, d=d, e=str(exc)[:200])
                w = {}
            paths[name].append(pd.Series(w, name=ts, dtype=float))
    frames = {
        n: pd.DataFrame(rows).reindex(columns=cols).fillna(0.0) if rows else pd.DataFrame(columns=cols)
        for n, rows in paths.items()
    }
    for n in frames:
        frames[n].index = pd.DatetimeIndex([pd.Timestamp(d) for d in dates[: len(frames[n])]])
    return frames, errors


def walk_forward(
    prices: pd.DataFrame,
    vix: pd.Series,
    start: date,
    end: date,
    config: EnsembleConfig,
    candidates: dict[str, CandidateFn] | None = None,
    *,
    engine: Any = None,
    pit: PanelPITStore | None = None,
    regime: pd.Series | None = None,
) -> EnsembleRun:
    """Run candidates and the selector over ``[start, end]`` on the given panel.

    ``regime`` (optional): cell labels indexed by the date each label became *known*. When
    given, the selector ranks candidates on their trailing returns inside the cell in force at
    the selection date (the last ``config.trailing`` such segments); a cell with fewer than
    ``config.regime_min_obs`` observed segments falls back to the unconditional trailing window
    and the selection record says so.
    """
    candidates = candidates or CANDIDATES
    dates = rebalance_dates(prices.index, start, end, config.rebalance)
    if len(dates) < config.trailing + 2:
        raise ValueError(f"{len(dates)} rebalance dates cannot cover a {config.trailing}-segment window")
    seg = segment_returns(prices, dates, config.max_abs_segment_return)
    weights, errors = compute_candidate_paths(prices, vix, dates[:-1], candidates, engine=engine, pit=pit)

    net, gross, turn = {}, {}, {}
    for name, w in weights.items():
        g, n, t = path_returns(w, seg, config.cost_bps_per_side)
        gross[name], net[name], turn[name] = g, n, t
    cand_net = pd.DataFrame(net)
    cand_gross = pd.DataFrame(gross)
    cand_turn = pd.DataFrame(turn)

    # Regime in force at each segment start: the latest label known on or before that date.
    regime_path: pd.Series | None = None
    if regime is not None:
        known = regime.dropna().sort_index()
        regime_path = pd.Series(
            [known.loc[: ts].iloc[-1] if len(known.loc[: ts]) else None for ts in seg.index], index=seg.index
        )

    # Selection loop: allocation decided at segment i from candidate net returns on segments < i.
    alloc = pd.DataFrame(0.0, index=seg.index, columns=list(candidates))
    selections: list[dict[str, Any]] = []
    current: dict[str, float] = {}
    for i in range(len(seg.index)):
        if i < config.trailing:
            continue
        if (i - config.trailing) % config.selection_every == 0:
            trailing = cand_net.iloc[i - config.trailing : i]
            cell, cell_n, fallback = None, None, False
            if regime_path is not None:
                cell = regime_path.iloc[i]
                in_cell = cand_net.iloc[:i][(regime_path.iloc[:i] == cell).to_numpy()]
                cell_n = int(len(in_cell))
                if cell is not None and cell_n >= config.regime_min_obs:
                    trailing = in_cell.iloc[-config.trailing :]
                else:
                    fallback = True
            picked = select_top_k(
                trailing, config.k, config.shrink_n0, config.corr_penalty,
                config.min_shrunk_sharpe, config.periods_per_year,
            )
            current = inverse_vol_weights(trailing, picked)
            selections.append(
                {
                    "date": seg.index[i].date().isoformat(),
                    "cell": cell,
                    "cell_n": cell_n,
                    "fallback": fallback,
                    "selected": picked,
                    "weights": {c: round(w, 6) for c, w in current.items()},
                    "shrunk_sharpe": {
                        c: round(shrunk_sharpe(trailing[c], config.shrink_n0, config.periods_per_year), 4)
                        for c in trailing.columns
                    },
                }
            )
        for c, a in current.items():
            alloc.iloc[i, alloc.columns.get_loc(c)] = a

    underlying = pd.DataFrame(0.0, index=seg.index, columns=seg.columns)
    for c in candidates:
        underlying = underlying.add(weights[c].reindex(index=seg.index).fillna(0.0).mul(alloc[c], axis=0), fill_value=0.0)
    ens_gross, ens_net, ens_turn = path_returns(underlying, seg, config.cost_bps_per_side)

    # Controls: 1/N over all candidates from the same start; SPY buy-and-hold (one entry cost).
    n_c = len(candidates)
    live = pd.Series(False, index=seg.index)
    live.iloc[config.trailing :] = True
    ew_alloc = pd.DataFrame(0.0, index=seg.index, columns=list(candidates))
    ew_alloc.loc[live, :] = 1.0 / n_c
    ew_underlying = pd.DataFrame(0.0, index=seg.index, columns=seg.columns)
    for c in candidates:
        ew_underlying = ew_underlying.add(
            weights[c].reindex(index=seg.index).fillna(0.0).mul(ew_alloc[c], axis=0), fill_value=0.0
        )
    _, ew_net, _ = path_returns(ew_underlying, seg, config.cost_bps_per_side)
    bench_w = pd.DataFrame(0.0, index=seg.index, columns=seg.columns)
    bench_w.loc[live, BENCHMARK] = 1.0
    _, bench_net, _ = path_returns(bench_w, seg, config.cost_bps_per_side)

    faults = {
        "candidate_errors": errors,
        "segment_returns_nan": int(seg.isna().sum().sum()),
        "segment_returns_total": int(seg.size),
        "segments": int(len(seg.index)),
    }
    return EnsembleRun(
        config=config, dates=list(dates), seg_ret=seg, candidate_weights=weights,
        candidate_net=cand_net, candidate_gross=cand_gross, candidate_turnover=cand_turn,
        ensemble_weights=alloc, ensemble_underlying=underlying, ensemble_net=ens_net,
        ensemble_gross=ens_gross, ensemble_turnover=ens_turn, equal_weight_net=ew_net,
        benchmark_net=bench_net, selections=selections, faults=faults, regime_path=regime_path,
    )


# ── Evaluation ───────────────────────────────────────────────────────────────


def block_bootstrap_mean(
    x: np.ndarray, *, reps: int, seed: int, alpha: float, block: int | None = None
) -> dict[str, Any]:
    """Circular block bootstrap of the mean; one-sided ``1 - alpha`` lower bound.

    The block comes from :func:`analysis.offline_research_proof.autocorrelation_block`
    unless given. Returns the bound, the two-sided interval and the share of
    resamples with a non-positive mean (a bootstrap p-value for ``mean <= 0``).
    """
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    n = len(x)
    if n < 8:
        return {"n": n, "mean": float(np.mean(x)) if n else None, "lower_bound": None, "error": "n < 8"}
    basis: dict[str, Any] = {}
    if block is None:
        block, basis = autocorrelation_block(x)
    block = max(1, int(block))
    rng = np.random.default_rng(seed)
    n_blocks = math.ceil(n / block)
    starts = rng.integers(0, n, size=(reps, n_blocks))
    offsets = np.arange(block)
    idx = (starts[:, :, None] + offsets[None, None, :]).reshape(reps, -1)[:, :n] % n
    means = x[idx].mean(axis=1)
    return {
        "n": n,
        "mean": float(x.mean()),
        "block": block,
        "block_basis": basis,
        "reps": reps,
        "seed": seed,
        "alpha": alpha,
        "lower_bound": float(np.quantile(means, alpha)),
        "ci_two_sided": [float(np.quantile(means, alpha / 2)), float(np.quantile(means, 1 - alpha / 2))],
        "p_mean_le_zero": float(np.mean(means <= 0.0)),
    }


def series_summary(returns: pd.Series, periods_per_year: int) -> dict[str, Any]:
    r = returns.dropna()
    if r.empty:
        return {"n": 0}
    return {
        "n": int(len(r)),
        "mean_per_period": float(r.mean()),
        "std_per_period": float(r.std(ddof=1)) if len(r) > 1 else None,
        "sharpe_annualised": annualised_sharpe(r, periods_per_year),
        "annualised_return": float((1 + r).prod() ** (periods_per_year / len(r)) - 1),
        "total_return": float((1 + r).prod() - 1),
        "max_drawdown": float(max_drawdown(r)),
        "worst_period": float(r.min()),
        "share_positive": float((r > 0).mean()),
    }


def deflated_sharpe(ens: pd.Series, candidates: pd.DataFrame, n_trials: int, confidence: float) -> dict[str, Any]:
    """DSR of the ensemble's per-period returns against ``n_trials`` declared trials."""
    r = ens.dropna().to_numpy(dtype=float)
    if len(r) < 20:
        return {"error": "n < 20"}
    per_period = {c: annualised_sharpe(candidates[c], 1) for c in candidates.columns}
    var = trial_sharpe_variance(list(per_period.values())) if len(per_period) > 1 else 0.0
    sr = float(np.mean(r) / np.std(r, ddof=1)) if np.std(r, ddof=1) > 0 else 0.0
    try:
        result = deflated_sharpe_ratio(
            sr, int(n_trials), int(len(r)), float(var),
            float(stats.skew(r)), float(stats.kurtosis(r, fisher=False)), confidence,
        )
    except ValueError as exc:
        return {"error": str(exc), "sharpe_per_period": sr, "trial_sharpe_variance": float(var)}
    return {
        "dsr": result.dsr,
        "passed": result.passed,
        "sharpe_per_period": result.sharpe,
        "sr0_per_period": result.sr0,
        "expected_max_z": result.expected_max_z,
        "n_trials": result.n_trials,
        "n_observations": result.n_observations,
        "trial_sharpe_variance": result.trial_sharpe_variance,
        "skewness": result.skewness,
        "kurtosis": result.kurtosis,
        "candidate_sharpe_per_period": per_period,
    }


def pbo_over_candidates(candidates: pd.DataFrame) -> dict[str, Any]:
    """PBO (CSCV, 16 partitions) over the candidate return matrix, if it can be formed."""
    m = candidates.dropna(how="any").copy()
    if m.index.tz is None:
        m.index = m.index.tz_localize("UTC")
    m.columns = [str(c) for c in m.columns]
    try:
        return probability_of_backtest_overfitting(m, 16).as_dict()
    except OverfitInputError as exc:
        return {"error": str(exc)}


def evaluate(run: EnsembleRun) -> dict[str, Any]:
    """Numbers for the report, including the registration's single pass criterion."""
    cfg = run.config
    oos = run.oos_index
    ens = run.ensemble_net.loc[oos]
    bench = run.benchmark_net.loc[oos]
    ew = run.equal_weight_net.loc[oos]
    excess = (ens - bench).dropna()
    boot = block_bootstrap_mean(excess.to_numpy(), reps=cfg.bootstrap_reps, seed=cfg.seed, alpha=cfg.alpha)
    cand_oos = run.candidate_net.loc[oos]
    cand_summary = {c: series_summary(cand_oos[c], cfg.periods_per_year) for c in cand_oos.columns}
    cand_excess_boot = {
        c: block_bootstrap_mean(
            (cand_oos[c] - bench).dropna().to_numpy(), reps=cfg.bootstrap_reps, seed=cfg.seed,
            alpha=cfg.alpha / max(1, len(cand_oos.columns)),
        )
        for c in cand_oos.columns
    }
    best_single = max(cand_summary, key=lambda c: cand_summary[c].get("sharpe_annualised", -math.inf)) if cand_summary else None
    dsr = deflated_sharpe(ens, cand_oos, cfg.n_trials_declared, cfg.dsr_min)
    pbo = pbo_over_candidates(cand_oos)
    primary = bool(boot.get("lower_bound") is not None and boot["lower_bound"] > 0)
    dsr_ok = bool(dsr.get("passed", False))
    v1_verdict = "PASS" if (primary and dsr_ok) else "FAIL"
    ens_summary = series_summary(ens, cfg.periods_per_year)
    spy_summary = series_summary(bench, cfg.periods_per_year)
    sharpe_beats_spy = bool(ens_summary.get("sharpe_annualised", -math.inf) > spy_summary.get("sharpe_annualised", math.inf))
    dsr_above_half = bool((dsr.get("dsr") or 0.0) > 0.5)
    dd_no_worse = bool(ens_summary.get("max_drawdown", -math.inf) >= spy_summary.get("max_drawdown", math.inf))
    watch_pass = primary and sharpe_beats_spy and dsr_above_half and dd_no_worse
    watch = {
        "rule": "ensemble OOS Sharpe > SPY buy-and-hold on the same segments AND one-sided 95% block-bootstrap "
                "lower bound of mean weekly net excess over SPY > 0 after costs AND deflated Sharpe > 0.5 "
                "(observed per-period Sharpe above the expected maximum at the cumulative trial count) "
                "AND ensemble max drawdown no worse than SPY's",
        "sharpe_beats_spy": sharpe_beats_spy,
        "excess_lower_bound_positive": primary,
        "dsr_above_half": dsr_above_half,
        "drawdown_no_worse_than_spy": dd_no_worse,
        "n_trials_cumulative": cfg.n_trials_declared,
        "verdict": "PASS" if watch_pass else "FAIL",
    }
    verdict = watch["verdict"] if cfg.criterion == "watch" else v1_verdict
    turnover_ann = float(run.ensemble_turnover.loc[oos].mean() * cfg.periods_per_year) if len(oos) else None
    time_in_cash = float((run.ensemble_weights.loc[oos].abs().sum(axis=1) == 0).mean()) if len(oos) else None
    selection_counts: dict[str, int] = {}
    for s in run.selections:
        for c in s["selected"]:
            selection_counts[c] = selection_counts.get(c, 0) + 1
    by_cell: dict[str, Any] = {}
    if run.regime_path is not None:
        cells = run.regime_path.loc[oos]
        for cell in sorted({c for c in cells.dropna().unique()}):
            mask = (cells == cell).to_numpy()
            idx = oos[mask]
            e_c, b_c = ens.loc[idx], bench.loc[idx]
            ex_c = (e_c - b_c).dropna()
            by_cell[str(cell)] = {
                "n_segments": int(len(idx)),
                "share": float(mask.mean()) if len(mask) else None,
                "ensemble": series_summary(e_c, cfg.periods_per_year),
                "spy": series_summary(b_c, cfg.periods_per_year),
                "equal_weight": series_summary(ew.loc[idx], cfg.periods_per_year),
                "excess_vs_spy_bootstrap": block_bootstrap_mean(
                    ex_c.to_numpy(), reps=cfg.bootstrap_reps, seed=cfg.seed, alpha=cfg.alpha
                ) if len(ex_c) >= 8 else {"n": int(len(ex_c)), "error": "n < 8"},
                "candidates": {c: series_summary(cand_oos.loc[idx, c], cfg.periods_per_year) for c in cand_oos.columns},
            }
        fallbacks = [s for s in run.selections if s.get("fallback")]
        by_cell["_selection_fallback_share"] = float(len(fallbacks) / len(run.selections)) if run.selections else None
    return {
        "version": ENSEMBLE_VERSION,
        "config": cfg.as_dict(),
        "by_regime_cell": by_cell,
        "oos_period": {
            "first_segment": oos.min().date().isoformat() if len(oos) else None,
            "last_segment": oos.max().date().isoformat() if len(oos) else None,
            "n_segments": int(len(oos)),
        },
        "ensemble": series_summary(ens, cfg.periods_per_year),
        "ensemble_gross": series_summary(run.ensemble_gross.loc[oos], cfg.periods_per_year),
        "ensemble_turnover_annualised": turnover_ann,
        "ensemble_share_in_cash": time_in_cash,
        "spy_buy_and_hold": series_summary(bench, cfg.periods_per_year),
        "equal_weight_all_candidates": series_summary(ew, cfg.periods_per_year),
        "best_single_in_hindsight": {"name": best_single, **(cand_summary.get(best_single, {}) if best_single else {})},
        "candidates": cand_summary,
        "candidate_excess_vs_spy_bootstrap_bonferroni": cand_excess_boot,
        "candidate_turnover_annualised": {
            c: float(run.candidate_turnover.loc[oos, c].mean() * cfg.periods_per_year) for c in cand_oos.columns
        },
        "candidate_correlation_oos": cand_oos.corr().round(3).to_dict(),
        "selection_counts": selection_counts,
        "n_selections": len(run.selections),
        "excess_vs_spy_bootstrap": boot,
        "deflated_sharpe": dsr,
        "pbo_candidates": pbo,
        "pass_criterion": {
            "rule": "block-bootstrap one-sided 95% lower bound of mean weekly net excess return vs SPY > 0 "
                    "AND deflated Sharpe > 0.95 at the declared trial count",
            "excess_lower_bound_positive": primary,
            "dsr_passed": dsr_ok,
            "verdict": verdict,
            "v1_pass_verdict": v1_verdict,
            "criterion": cfg.criterion,
        },
        "watch_criterion": watch,
        "faults": run.faults,
    }


def report_markdown(report: dict[str, Any], *, title: str, data_basis: str) -> str:
    """A short human summary; the JSON is the record."""
    e, b, ew = report["ensemble"], report["spy_buy_and_hold"], report["equal_weight_all_candidates"]
    best = report["best_single_in_hindsight"]
    boot = report["excess_vs_spy_bootstrap"]
    dsr = report["deflated_sharpe"]
    pbo = report["pbo_candidates"]
    pc = report["pass_criterion"]

    def f(v: Any, nd: int = 3) -> str:
        return "—" if v is None or (isinstance(v, float) and not math.isfinite(v)) else f"{v:.{nd}f}"

    watch = report.get("watch_criterion", {})
    rule = watch["rule"] if pc.get("criterion") == "watch" else pc["rule"]
    lines = [
        f"# {title}",
        "",
        f"Verdict: **{pc['verdict']}** — {rule}.",
        "",
        f"Cumulative trials counted in the deflated Sharpe: {report['config']['n_trials_declared']}. "
        f"Watch criterion parts: Sharpe beats SPY {watch.get('sharpe_beats_spy')}, excess LB > 0 "
        f"{watch.get('excess_lower_bound_positive')}, DSR > 0.5 {watch.get('dsr_above_half')}, "
        f"drawdown no worse {watch.get('drawdown_no_worse_than_spy')}.",
        "",
        f"Out-of-sample segments: {report['oos_period']['n_segments']} "
        f"{'daily' if report['config']['periods_per_year'] == 252 else 'weekly'}, "
        f"{report['oos_period']['first_segment']} → {report['oos_period']['last_segment']}. {data_basis}",
        "",
        "| series | Sharpe (ann.) | ann. return | max DD | total | n |",
        "|---|---|---|---|---|---|",
        f"| ensemble (net) | {f(e.get('sharpe_annualised'))} | {f(e.get('annualised_return'))} | {f(e.get('max_drawdown'))} | {f(e.get('total_return'))} | {e.get('n')} |",
        f"| SPY buy-and-hold | {f(b.get('sharpe_annualised'))} | {f(b.get('annualised_return'))} | {f(b.get('max_drawdown'))} | {f(b.get('total_return'))} | {b.get('n')} |",
        f"| 1/N all candidates | {f(ew.get('sharpe_annualised'))} | {f(ew.get('annualised_return'))} | {f(ew.get('max_drawdown'))} | {f(ew.get('total_return'))} | {ew.get('n')} |",
        f"| best single in hindsight ({best.get('name')}) | {f(best.get('sharpe_annualised'))} | {f(best.get('annualised_return'))} | {f(best.get('max_drawdown'))} | {f(best.get('total_return'))} | {best.get('n')} |",
        "",
        f"Mean weekly net excess vs SPY: {f(boot.get('mean'), 5)}; one-sided 95 % lower bound "
        f"{f(boot.get('lower_bound'), 5)} (block {boot.get('block')}, {boot.get('reps')} resamples, seed {boot.get('seed')}); "
        f"P(mean ≤ 0) = {f(boot.get('p_mean_le_zero'))}.",
        f"Deflated Sharpe: {f(dsr.get('dsr'))} at n_trials = {dsr.get('n_trials')} (SR0 per period {f(dsr.get('sr0_per_period'), 4)}, "
        f"observed per period {f(dsr.get('sharpe_per_period'), 4)}) → {'pass' if dsr.get('passed') else 'fail'}.",
        f"PBO over the candidate matrix: {f(pbo.get('pbo'))} ({pbo.get('n_combinations')} CSCV splits); "
        f"P(OOS loss of the in-sample best) = {f(pbo.get('prob_oos_loss'))}.",
        f"Ensemble turnover {f(report['ensemble_turnover_annualised'], 2)}×/yr; in cash {f(report['ensemble_share_in_cash'])} of segments.",
        "",
        f"## Candidates (same out-of-sample segments, net of {report['config']['cost_bps_per_side']:g} bp/side)",
        "",
        "| candidate | Sharpe | ann. return | max DD | turnover/yr | excess-vs-SPY LB (Bonferroni) | times selected |",
        "|---|---|---|---|---|---|---|",
    ]
    for c, s in report["candidates"].items():
        lb = report["candidate_excess_vs_spy_bootstrap_bonferroni"][c].get("lower_bound")
        lines.append(
            f"| {c} | {f(s.get('sharpe_annualised'))} | {f(s.get('annualised_return'))} | {f(s.get('max_drawdown'))} | "
            f"{f(report['candidate_turnover_annualised'].get(c), 2)} | {f(lb, 5)} | {report['selection_counts'].get(c, 0)} |"
        )
    lines += ["", f"Faults: {report['faults']}", ""]
    return "\n".join(lines)


def frame_to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """JSON-safe rows of a date-indexed frame (dates ISO, floats rounded)."""
    out = []
    for ts, row in frame.iterrows():
        d = ts.date().isoformat() if isinstance(ts, (pd.Timestamp, datetime)) else str(ts)
        out.append({"date": d, **{str(k): (None if pd.isna(v) else round(float(v), 8)) for k, v in row.items()}})
    return out


__all__ = [
    "BENCHMARK", "CANDIDATES", "Cadenced", "CandidateSet", "REGISTRATIONS", "TRIALS_BEFORE", "UNIVERSE_V2",
    "UNIVERSE_V3", "UNIVERSE_V4", "UNIVERSE_V5", "V5_QUALIFIED", "candidate_set", "v2_candidates", "v3_candidates",
    "v4_candidates", "v5_candidates", "v6_candidates", "v6_trials_added", "DecisionContext", "EnsembleConfig", "EnsembleRun", "PREREG_PATH",
    "PREREG_SHA256", "PanelPITStore", "UNIVERSE", "annualised_sharpe", "block_bootstrap_mean",
    "compute_candidate_paths", "deflated_sharpe", "evaluate", "frame_to_records", "inverse_vol_weights",
    "path_returns", "pbo_over_candidates", "rebalance_dates", "registration_sha256", "report_markdown",
    "segment_returns", "select_top_k", "shrunk_sharpe", "walk_forward",
]
