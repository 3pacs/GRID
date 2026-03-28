"""
GRID Trend Tracker — Divergence Analysis for Market Trends.

Applies the same divergence-detection lens used in cross_reference.py,
but aimed at market *trends* rather than government statistics.

Six trend categories:
    1. Momentum Shifts   — features crossing key moving averages (golden/death crosses)
    2. Regime Transitions — GROWTH / FRAGILE / CRISIS shift detection with leading indicators
    3. Sector Rotation    — relative strength changes across 1W / 1M / 3M windows
    4. Volatility Regime  — VIX term structure, realized vs implied vol divergence
    5. Liquidity Trend    — Fed net liquidity, reverse repo, TGA direction
    6. Correlation Breakdown — historically correlated assets diverging

Pipeline:
    analyze_trends(engine, lookback_days=90) orchestrates all six categories,
    scores each trend by direction/strength/confidence, generates a narrative
    synthesis, and returns a TrendReport dict.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ─────────────────────────────────────────────────────────────

TREND_CATEGORIES = [
    "momentum", "regime", "sector_rotation",
    "volatility", "liquidity", "correlation",
]

DIRECTION_BULLISH = "bullish"
DIRECTION_BEARISH = "bearish"
DIRECTION_NEUTRAL = "neutral"
DIRECTION_TRANSITIONING = "transitioning"

# Moving average windows (business days)
MA_SHORT = 50
MA_LONG = 200

# Minimum strength threshold to report a trend
MIN_STRENGTH = 0.2

# Minimum data points required for valid analysis
MIN_OBSERVATIONS = 20


# ── Data Classes ──────────────────────────────────────────────────────────

@dataclass
class Trend:
    """A single detected market trend."""

    name: str
    category: str
    direction: str                  # bullish / bearish / neutral / transitioning
    strength: float                 # 0-1
    description: str
    supporting_evidence: list[str] = field(default_factory=list)
    contradicting_evidence: list[str] = field(default_factory=list)
    implications: list[str] = field(default_factory=list)
    tickers_affected: list[str] = field(default_factory=list)
    started: str = ""
    confidence: float = 0.5

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TrendReport:
    """Complete trend analysis report."""

    trends: list[Trend]
    category_summaries: dict[str, dict]
    narrative: str
    generated_at: str

    def to_dict(self) -> dict:
        return {
            "trends": [t.to_dict() for t in self.trends],
            "category_summaries": self.category_summaries,
            "narrative": self.narrative,
            "generated_at": self.generated_at,
        }


# ── Helpers ───────────────────────────────────────────────────────────────

def _safe_float(val: Any, default: float = 0.0) -> float:
    """Convert a value to float, returning default on failure."""
    if val is None:
        return default
    try:
        v = float(val)
        return default if (math.isnan(v) or math.isinf(v)) else v
    except (TypeError, ValueError):
        return default


def _clamp(val: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, val))


def _query_feature_series(
    engine: Engine,
    feature_name: str,
    lookback_days: int = 252,
) -> pd.Series:
    """Pull a PIT-correct feature time series from resolved_series."""
    try:
        cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
        query = text("""
            SELECT DISTINCT ON (obs_date) obs_date, value
            FROM resolved_series
            WHERE feature_id IN (
                SELECT id FROM feature_catalog WHERE name = :fname
            )
            AND obs_date >= :cutoff
            ORDER BY obs_date, release_date DESC
        """).bindparams(fname=feature_name, cutoff=cutoff)
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, parse_dates=["obs_date"])
        if df.empty:
            return pd.Series(dtype=float)
        return df.set_index("obs_date")["value"].sort_index().astype(float)
    except Exception as exc:
        log.debug("Failed to query feature {f}: {e}", f=feature_name, e=str(exc))
        return pd.Series(dtype=float)


def _query_latest_regime(engine: Engine) -> dict:
    """Get the current regime state and recent transitions."""
    try:
        query = text("""
            SELECT regime_label, confidence, detected_at
            FROM regime_history
            ORDER BY detected_at DESC
            LIMIT 10
        """)
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        if df.empty:
            return {"current": "UNKNOWN", "history": []}
        return {
            "current": df.iloc[0]["regime_label"],
            "confidence": _safe_float(df.iloc[0].get("confidence"), 0.5),
            "history": df.to_dict("records"),
        }
    except Exception as exc:
        log.debug("Failed to query regime: {e}", e=str(exc))
        return {"current": "UNKNOWN", "history": []}


def _compute_ma_cross(series: pd.Series, short: int = MA_SHORT, long_: int = MA_LONG) -> dict:
    """Detect moving average crossovers."""
    if len(series) < long_ + 5:
        return {"cross": None}
    ma_short = series.rolling(short).mean()
    ma_long = series.rolling(long_).mean()

    diff = ma_short - ma_long
    diff_clean = diff.dropna()
    if len(diff_clean) < 2:
        return {"cross": None}

    current_above = diff_clean.iloc[-1] > 0
    prev_above = diff_clean.iloc[-2] > 0

    if current_above and not prev_above:
        return {"cross": "golden", "short_ma": ma_short.iloc[-1], "long_ma": ma_long.iloc[-1]}
    elif not current_above and prev_above:
        return {"cross": "death", "short_ma": ma_short.iloc[-1], "long_ma": ma_long.iloc[-1]}

    # Check recent (last 10 days) for proximity to cross
    recent_diff = diff_clean.tail(10)
    if len(recent_diff) > 0 and abs(recent_diff.iloc[-1]) / (abs(ma_long.iloc[-1]) + 1e-9) < 0.01:
        return {
            "cross": "approaching",
            "direction": "golden" if diff_clean.diff().iloc[-1] > 0 else "death",
            "short_ma": ma_short.iloc[-1],
            "long_ma": ma_long.iloc[-1],
        }

    return {"cross": None, "above": current_above, "short_ma": ma_short.iloc[-1], "long_ma": ma_long.iloc[-1]}


def _relative_strength(series: pd.Series, window: int) -> float:
    """Compute simple relative strength: return over window."""
    if len(series) < window + 1:
        return 0.0
    recent = series.iloc[-1]
    past = series.iloc[-window - 1]
    if past == 0:
        return 0.0
    return (recent - past) / abs(past)


# ── Category Analyzers ────────────────────────────────────────────────────

def _analyze_momentum(engine: Engine, lookback_days: int) -> list[Trend]:
    """Detect momentum shifts via MA crossovers for key features."""
    trends = []

    # Key features to track for momentum
    momentum_features = {
        "sp500_close": {"label": "S&P 500", "tickers": ["SPY", "ES=F"]},
        "nasdaq_close": {"label": "NASDAQ", "tickers": ["QQQ", "NQ=F"]},
        "xlk_close": {"label": "Tech Sector (XLK)", "tickers": ["XLK", "AAPL", "MSFT", "NVDA"]},
        "xle_close": {"label": "Energy Sector (XLE)", "tickers": ["XLE", "XOM", "CVX"]},
        "xlf_close": {"label": "Financials (XLF)", "tickers": ["XLF", "JPM", "BAC"]},
        "eem_close": {"label": "Emerging Markets", "tickers": ["EEM", "FXI"]},
        "btc_usd": {"label": "Bitcoin", "tickers": ["BTC", "GBTC"]},
        "gold_close": {"label": "Gold", "tickers": ["GLD", "GC=F"]},
        "tlt_close": {"label": "Long Treasuries", "tickers": ["TLT", "ZN=F"]},
        "dxy_close": {"label": "US Dollar Index", "tickers": ["UUP", "DX=F"]},
    }

    for feat_name, meta in momentum_features.items():
        series = _query_feature_series(engine, feat_name, max(lookback_days, 252))
        if len(series) < MA_LONG + 10:
            continue

        cross_info = _compute_ma_cross(series)
        cross_type = cross_info.get("cross")

        if cross_type in ("golden", "death"):
            is_golden = cross_type == "golden"
            direction = DIRECTION_BULLISH if is_golden else DIRECTION_BEARISH

            # RSI-like metric: pct of up days in last 14
            recent = series.tail(14)
            up_days = (recent.diff().dropna() > 0).sum()
            rsi_approx = up_days / max(len(recent.diff().dropna()), 1) * 100

            # Volume proxy: recent volatility relative to historical
            vol_recent = series.tail(20).pct_change().std()
            vol_hist = series.tail(60).pct_change().std()
            vol_ratio = vol_recent / (vol_hist + 1e-9)

            strength = _clamp(0.6 + (0.2 if vol_ratio > 1.2 else 0))
            confidence = _clamp(0.5 + (0.15 if vol_ratio > 1.3 else 0))

            supporting = []
            contradicting = []

            if is_golden:
                supporting.append(f"50-day MA crossed above 200-day MA")
                supporting.append(f"RSI proxy at {rsi_approx:.0f}")
                if vol_ratio > 1.2:
                    supporting.append("Volume elevated on breakout")
                if rsi_approx > 70:
                    contradicting.append(f"RSI proxy ({rsi_approx:.0f}) suggests overbought")
            else:
                supporting.append(f"50-day MA crossed below 200-day MA")
                supporting.append(f"RSI proxy at {rsi_approx:.0f}")
                if vol_ratio > 1.2:
                    supporting.append("Volume elevated on breakdown")
                if rsi_approx < 30:
                    contradicting.append(f"RSI proxy ({rsi_approx:.0f}) suggests oversold — bounce possible")

            cross_label = "Golden Cross" if is_golden else "Death Cross"
            trends.append(Trend(
                name=f"{meta['label']} {cross_label}",
                category="momentum",
                direction=direction,
                strength=strength,
                description=(
                    f"{meta['label']} 50-day MA crossed "
                    f"{'above' if is_golden else 'below'} 200-day MA"
                ),
                supporting_evidence=supporting,
                contradicting_evidence=contradicting,
                implications=[
                    f"{'Bullish' if is_golden else 'Bearish'} momentum signal for {meta['label']}",
                    f"Watch for follow-through in coming sessions",
                ],
                tickers_affected=meta["tickers"],
                started=str(series.index[-1].date()) if hasattr(series.index[-1], 'date') else str(date.today()),
                confidence=confidence,
            ))

        elif cross_type == "approaching":
            approaching_dir = cross_info.get("direction", "golden")
            trends.append(Trend(
                name=f"{meta['label']} Approaching {'Golden' if approaching_dir == 'golden' else 'Death'} Cross",
                category="momentum",
                direction=DIRECTION_TRANSITIONING,
                strength=0.4,
                description=f"{meta['label']} MAs converging — cross likely within days",
                supporting_evidence=[f"MA spread narrowing rapidly"],
                contradicting_evidence=[],
                implications=[f"Watch for confirmation of {'golden' if approaching_dir == 'golden' else 'death'} cross"],
                tickers_affected=meta["tickers"],
                started=str(date.today()),
                confidence=0.4,
            ))

    return trends


def _analyze_regime(engine: Engine, lookback_days: int) -> list[Trend]:
    """Detect regime transitions: GROWTH / FRAGILE / CRISIS."""
    trends = []
    regime_info = _query_latest_regime(engine)
    current_regime = regime_info.get("current", "UNKNOWN")
    history = regime_info.get("history", [])

    if len(history) < 2:
        return trends

    # Check for recent regime change
    prev_regime = history[1].get("regime_label", "UNKNOWN") if len(history) > 1 else "UNKNOWN"
    confidence = _safe_float(regime_info.get("confidence"), 0.5)

    if current_regime != prev_regime and current_regime != "UNKNOWN":
        direction_map = {
            "GROWTH": DIRECTION_BULLISH,
            "FRAGILE": DIRECTION_TRANSITIONING,
            "CRISIS": DIRECTION_BEARISH,
        }
        direction = direction_map.get(current_regime, DIRECTION_NEUTRAL)

        # Determine strength based on confidence and transition type
        transition_severity = {
            ("GROWTH", "FRAGILE"): 0.5,
            ("GROWTH", "CRISIS"): 0.9,
            ("FRAGILE", "GROWTH"): 0.6,
            ("FRAGILE", "CRISIS"): 0.8,
            ("CRISIS", "GROWTH"): 0.7,
            ("CRISIS", "FRAGILE"): 0.5,
        }
        strength = transition_severity.get((prev_regime, current_regime), 0.5)

        # Leading indicators: check VIX, credit spreads, yield curve
        supporting = []
        contradicting = []

        vix = _query_feature_series(engine, "vix_close", 60)
        if len(vix) > 5:
            vix_level = _safe_float(vix.iloc[-1])
            vix_5d_chg = _safe_float(vix.iloc[-1] - vix.iloc[-5]) if len(vix) >= 5 else 0
            if vix_level > 25:
                supporting.append(f"VIX elevated at {vix_level:.1f}")
            elif vix_level < 15:
                supporting.append(f"VIX calm at {vix_level:.1f}")
            if abs(vix_5d_chg) > 3:
                supporting.append(f"VIX moved {vix_5d_chg:+.1f} pts in 5 days")

        yield_curve = _query_feature_series(engine, "us_yield_10y_2y_spread", 60)
        if len(yield_curve) > 5:
            spread = _safe_float(yield_curve.iloc[-1])
            if spread < 0:
                supporting.append(f"Yield curve inverted at {spread:.2f}%")
            elif spread > 0.5:
                contradicting.append(f"Yield curve steepening at {spread:.2f}%")

        detected_str = ""
        detected_at = history[0].get("detected_at")
        if detected_at:
            if isinstance(detected_at, str):
                detected_str = detected_at[:10]
            elif hasattr(detected_at, 'strftime'):
                detected_str = detected_at.strftime("%Y-%m-%d")

        trends.append(Trend(
            name=f"Regime Shift: {prev_regime} -> {current_regime}",
            category="regime",
            direction=direction,
            strength=strength,
            description=(
                f"Market regime transitioning from {prev_regime} to {current_regime}. "
                f"Confidence: {confidence:.0%}"
            ),
            supporting_evidence=supporting,
            contradicting_evidence=contradicting,
            implications=[
                f"Portfolio positioning should shift to {current_regime.lower()} playbook",
                f"Risk management {'tightening' if current_regime in ('FRAGILE', 'CRISIS') else 'can loosen'}",
            ],
            tickers_affected=["SPY", "QQQ", "TLT", "GLD", "VXX"],
            started=detected_str or str(date.today()),
            confidence=confidence,
        ))
    elif current_regime != "UNKNOWN":
        # No transition but report current regime health
        trends.append(Trend(
            name=f"Regime: {current_regime} (Holding)",
            category="regime",
            direction={
                "GROWTH": DIRECTION_BULLISH,
                "FRAGILE": DIRECTION_TRANSITIONING,
                "CRISIS": DIRECTION_BEARISH,
            }.get(current_regime, DIRECTION_NEUTRAL),
            strength=_clamp(confidence * 0.6),
            description=f"Market remains in {current_regime} regime with {confidence:.0%} confidence",
            supporting_evidence=[f"Regime confidence: {confidence:.0%}"],
            contradicting_evidence=[],
            implications=[f"Current {current_regime.lower()} positioning remains appropriate"],
            tickers_affected=["SPY"],
            started=str(date.today()),
            confidence=confidence,
        ))

    return trends


def _analyze_sector_rotation(engine: Engine, lookback_days: int) -> list[Trend]:
    """Detect sector rotation patterns: where money is flowing TO and FROM."""
    trends = []

    sectors = {
        "xlk_close": {"label": "Technology (XLK)", "ticker": "XLK"},
        "xle_close": {"label": "Energy (XLE)", "ticker": "XLE"},
        "xlf_close": {"label": "Financials (XLF)", "ticker": "XLF"},
        "xlv_close": {"label": "Healthcare (XLV)", "ticker": "XLV"},
        "xli_close": {"label": "Industrials (XLI)", "ticker": "XLI"},
        "xlp_close": {"label": "Staples (XLP)", "ticker": "XLP"},
        "xlu_close": {"label": "Utilities (XLU)", "ticker": "XLU"},
        "xlb_close": {"label": "Materials (XLB)", "ticker": "XLB"},
        "xlre_close": {"label": "Real Estate (XLRE)", "ticker": "XLRE"},
        "xly_close": {"label": "Discretionary (XLY)", "ticker": "XLY"},
        "xlc_close": {"label": "Communications (XLC)", "ticker": "XLC"},
    }

    # Compute relative strength over 1W (5), 1M (21), 3M (63)
    windows = {"1W": 5, "1M": 21, "3M": 63}
    sector_rs = {}

    for feat, meta in sectors.items():
        series = _query_feature_series(engine, feat, max(lookback_days, 90))
        if len(series) < 63:
            continue
        rs = {}
        for label, w in windows.items():
            rs[label] = _relative_strength(series, w)
        sector_rs[feat] = {"meta": meta, "rs": rs, "series": series}

    if len(sector_rs) < 3:
        return trends

    # Identify gainers and losers by 1M relative strength
    sorted_by_1m = sorted(sector_rs.items(), key=lambda x: x[1]["rs"].get("1M", 0), reverse=True)

    # Top gainers
    for feat, info in sorted_by_1m[:3]:
        rs = info["rs"]
        if rs.get("1M", 0) > 0.02:  # At least 2% relative gain
            # Is 1M stronger than 3M? Accelerating.
            accelerating = rs.get("1M", 0) > rs.get("3M", 0) / 3
            trends.append(Trend(
                name=f"{info['meta']['label']} Gaining Strength",
                category="sector_rotation",
                direction=DIRECTION_BULLISH,
                strength=_clamp(abs(rs["1M"]) * 5),
                description=(
                    f"{info['meta']['label']} relative strength: "
                    f"1W {rs.get('1W', 0):+.1%}, 1M {rs.get('1M', 0):+.1%}, 3M {rs.get('3M', 0):+.1%}"
                    + (" — accelerating" if accelerating else "")
                ),
                supporting_evidence=[
                    f"1M return: {rs.get('1M', 0):+.1%}",
                    f"1W return: {rs.get('1W', 0):+.1%}",
                ] + (["Momentum accelerating vs 3M trend"] if accelerating else []),
                contradicting_evidence=[],
                implications=[f"Money rotating INTO {info['meta']['label']}"],
                tickers_affected=[info["meta"]["ticker"]],
                started=str(date.today() - timedelta(days=21)),
                confidence=_clamp(0.4 + abs(rs["1M"]) * 3),
            ))

    # Bottom losers
    for feat, info in sorted_by_1m[-3:]:
        rs = info["rs"]
        if rs.get("1M", 0) < -0.02:
            decelerating = rs.get("1M", 0) < rs.get("3M", 0) / 3
            trends.append(Trend(
                name=f"{info['meta']['label']} Losing Strength",
                category="sector_rotation",
                direction=DIRECTION_BEARISH,
                strength=_clamp(abs(rs["1M"]) * 5),
                description=(
                    f"{info['meta']['label']} relative strength: "
                    f"1W {rs.get('1W', 0):+.1%}, 1M {rs.get('1M', 0):+.1%}, 3M {rs.get('3M', 0):+.1%}"
                    + (" — decelerating" if decelerating else "")
                ),
                supporting_evidence=[
                    f"1M return: {rs.get('1M', 0):+.1%}",
                    f"1W return: {rs.get('1W', 0):+.1%}",
                ] + (["Momentum decelerating vs 3M trend"] if decelerating else []),
                contradicting_evidence=[],
                implications=[f"Money rotating OUT OF {info['meta']['label']}"],
                tickers_affected=[info["meta"]["ticker"]],
                started=str(date.today() - timedelta(days=21)),
                confidence=_clamp(0.4 + abs(rs["1M"]) * 3),
            ))

    return trends


def _analyze_volatility(engine: Engine, lookback_days: int) -> list[Trend]:
    """Analyze VIX term structure and realized vs implied vol divergence."""
    trends = []

    vix = _query_feature_series(engine, "vix_close", lookback_days)
    vix3m = _query_feature_series(engine, "vix3m_close", lookback_days)
    sp500 = _query_feature_series(engine, "sp500_close", lookback_days)

    # VIX term structure: contango vs backwardation
    if len(vix) > 5 and len(vix3m) > 5:
        vix_now = _safe_float(vix.iloc[-1])
        vix3m_now = _safe_float(vix3m.iloc[-1])

        if vix_now > 0 and vix3m_now > 0:
            term_ratio = vix_now / vix3m_now
            in_backwardation = term_ratio > 1.05

            if in_backwardation:
                trends.append(Trend(
                    name="VIX Term Structure in Backwardation",
                    category="volatility",
                    direction=DIRECTION_BEARISH,
                    strength=_clamp((term_ratio - 1.0) * 3),
                    description=(
                        f"VIX ({vix_now:.1f}) trading above VIX3M ({vix3m_now:.1f}) — "
                        f"ratio {term_ratio:.2f}. Near-term fear exceeds medium-term."
                    ),
                    supporting_evidence=[
                        f"VIX/VIX3M ratio: {term_ratio:.2f}",
                        "Backwardation signals elevated near-term stress",
                    ],
                    contradicting_evidence=(
                        ["Backwardation can be transient around events"] if term_ratio < 1.15 else []
                    ),
                    implications=[
                        "Short-vol strategies at risk",
                        "Hedging costs elevated",
                        "Potential for mean reversion if catalyst passes",
                    ],
                    tickers_affected=["VXX", "UVXY", "SVXY", "SPY"],
                    started=str(date.today()),
                    confidence=_clamp(0.5 + (term_ratio - 1.0) * 2),
                ))
            elif term_ratio < 0.85:
                trends.append(Trend(
                    name="VIX Term Structure Deep Contango",
                    category="volatility",
                    direction=DIRECTION_BULLISH,
                    strength=_clamp((1.0 - term_ratio) * 2),
                    description=(
                        f"VIX ({vix_now:.1f}) well below VIX3M ({vix3m_now:.1f}) — "
                        f"ratio {term_ratio:.2f}. Markets complacent."
                    ),
                    supporting_evidence=[
                        f"VIX/VIX3M ratio: {term_ratio:.2f}",
                        "Deep contango = low near-term fear",
                    ],
                    contradicting_evidence=["Complacency can precede sharp corrections"],
                    implications=[
                        "Short-vol strategies profitable",
                        "Low hedging costs — good time to add protection",
                    ],
                    tickers_affected=["VXX", "UVXY", "SVXY", "SPY"],
                    started=str(date.today()),
                    confidence=_clamp(0.4 + (1.0 - term_ratio)),
                ))

    # Realized vs implied vol divergence
    if len(sp500) > 30 and len(vix) > 5:
        realized_vol = sp500.pct_change().tail(20).std() * (252 ** 0.5) * 100
        implied_vol = _safe_float(vix.iloc[-1])

        if realized_vol > 0 and implied_vol > 0:
            vol_premium = implied_vol - realized_vol
            vol_ratio = implied_vol / realized_vol

            if vol_ratio > 1.5:
                trends.append(Trend(
                    name="Implied Vol Premium Elevated",
                    category="volatility",
                    direction=DIRECTION_BEARISH,
                    strength=_clamp((vol_ratio - 1.0) * 0.5),
                    description=(
                        f"VIX ({implied_vol:.1f}) trading {vol_premium:.1f} pts above "
                        f"realized vol ({realized_vol:.1f}). Markets pricing more risk than visible."
                    ),
                    supporting_evidence=[
                        f"Implied/realized ratio: {vol_ratio:.2f}",
                        f"Vol premium: {vol_premium:.1f} pts",
                    ],
                    contradicting_evidence=["Some premium is normal — mean is ~1.3x"],
                    implications=["Option sellers have edge if premium normalizes"],
                    tickers_affected=["SPY", "VXX"],
                    started=str(date.today()),
                    confidence=_clamp(0.4 + (vol_ratio - 1.3) * 0.5),
                ))
            elif vol_ratio < 0.8:
                trends.append(Trend(
                    name="Realized Vol Exceeding Implied",
                    category="volatility",
                    direction=DIRECTION_BEARISH,
                    strength=_clamp((1.0 - vol_ratio) * 2),
                    description=(
                        f"Realized vol ({realized_vol:.1f}) exceeding VIX ({implied_vol:.1f}). "
                        f"Markets underpricing actual risk."
                    ),
                    supporting_evidence=[
                        f"Implied/realized ratio: {vol_ratio:.2f}",
                        "Market moving more than options imply",
                    ],
                    contradicting_evidence=[],
                    implications=[
                        "Options are cheap relative to actual moves",
                        "Good time to buy protection",
                    ],
                    tickers_affected=["SPY", "VXX"],
                    started=str(date.today()),
                    confidence=_clamp(0.5 + (1.0 - vol_ratio)),
                ))

    return trends


def _analyze_liquidity(engine: Engine, lookback_days: int) -> list[Trend]:
    """Analyze Fed net liquidity, reverse repo, and TGA trends."""
    trends = []

    # Fed balance sheet / net liquidity
    fed_bs = _query_feature_series(engine, "fed_total_assets", lookback_days)
    rrp = _query_feature_series(engine, "fed_reverse_repo", lookback_days)
    tga = _query_feature_series(engine, "treasury_general_account", lookback_days)

    # Net liquidity = Fed Balance Sheet - Reverse Repo - TGA
    if len(fed_bs) > 10:
        fed_direction = "expanding" if fed_bs.iloc[-1] > fed_bs.iloc[-10] else "contracting"
        fed_chg = _safe_float(fed_bs.iloc[-1] - fed_bs.iloc[-10])
        fed_pct = fed_chg / abs(_safe_float(fed_bs.iloc[-10], 1)) * 100

        direction = DIRECTION_BULLISH if fed_direction == "expanding" else DIRECTION_BEARISH
        trends.append(Trend(
            name=f"Fed Balance Sheet {fed_direction.title()}",
            category="liquidity",
            direction=direction,
            strength=_clamp(abs(fed_pct) * 0.5),
            description=(
                f"Fed balance sheet {fed_direction} by {fed_pct:+.2f}% over last 10 readings. "
                f"{'Injecting' if fed_direction == 'expanding' else 'Draining'} liquidity."
            ),
            supporting_evidence=[
                f"10-period change: {fed_pct:+.2f}%",
                f"Current level: ${_safe_float(fed_bs.iloc[-1])/1e12:.2f}T" if _safe_float(fed_bs.iloc[-1]) > 1e9 else f"Current level: {_safe_float(fed_bs.iloc[-1]):.0f}",
            ],
            contradicting_evidence=[],
            implications=[
                f"Liquidity {'tailwind' if fed_direction == 'expanding' else 'headwind'} for risk assets",
            ],
            tickers_affected=["SPY", "QQQ", "BTC", "TLT"],
            started=str(date.today() - timedelta(days=10)),
            confidence=0.6,
        ))

    if len(rrp) > 10:
        rrp_direction = "declining" if rrp.iloc[-1] < rrp.iloc[-10] else "rising"
        rrp_chg = _safe_float(rrp.iloc[-1] - rrp.iloc[-10])

        # Declining RRP = liquidity entering markets (bullish)
        direction = DIRECTION_BULLISH if rrp_direction == "declining" else DIRECTION_BEARISH
        trends.append(Trend(
            name=f"Reverse Repo {rrp_direction.title()}",
            category="liquidity",
            direction=direction,
            strength=_clamp(abs(rrp_chg) / (abs(_safe_float(rrp.iloc[-10], 1)) + 1e-9) * 3),
            description=(
                f"Reverse repo facility {rrp_direction}. "
                f"{'Money leaving RRP and entering markets' if rrp_direction == 'declining' else 'Money flowing into RRP from markets'}."
            ),
            supporting_evidence=[f"RRP trend: {rrp_direction} over 10 readings"],
            contradicting_evidence=[],
            implications=[
                f"{'Positive' if rrp_direction == 'declining' else 'Negative'} for market liquidity",
            ],
            tickers_affected=["SPY", "TLT"],
            started=str(date.today() - timedelta(days=10)),
            confidence=0.55,
        ))

    if len(tga) > 10:
        tga_direction = "drawing down" if tga.iloc[-1] < tga.iloc[-10] else "building"
        direction = DIRECTION_BULLISH if tga_direction == "drawing down" else DIRECTION_BEARISH

        trends.append(Trend(
            name=f"Treasury General Account {tga_direction.title()}",
            category="liquidity",
            direction=direction,
            strength=0.4,
            description=(
                f"TGA is {tga_direction}. "
                f"{'Spending injects liquidity into banking system' if tga_direction == 'drawing down' else 'Issuance drains reserves from banking system'}."
            ),
            supporting_evidence=[f"TGA trend: {tga_direction}"],
            contradicting_evidence=[],
            implications=[
                f"{'Positive' if tga_direction == 'drawing down' else 'Negative'} for bank reserves and market liquidity",
            ],
            tickers_affected=["SPY", "TLT", "XLF"],
            started=str(date.today() - timedelta(days=10)),
            confidence=0.5,
        ))

    return trends


def _analyze_correlations(engine: Engine, lookback_days: int) -> list[Trend]:
    """Detect correlation breakdowns between historically correlated assets."""
    trends = []

    pairs = [
        ("btc_usd", "nasdaq_close", "BTC vs NASDAQ", ["BTC", "QQQ"], 0.6),
        ("gold_close", "tlt_close", "Gold vs Treasuries", ["GLD", "TLT"], 0.4),
        ("sp500_close", "eem_close", "S&P 500 vs EM", ["SPY", "EEM"], 0.5),
        ("dxy_close", "gold_close", "Dollar vs Gold", ["UUP", "GLD"], -0.4),
        ("xle_close", "crude_oil_close", "Energy Sector vs Crude", ["XLE", "USO"], 0.7),
    ]

    for feat_a, feat_b, label, tickers, expected_corr in pairs:
        series_a = _query_feature_series(engine, feat_a, lookback_days)
        series_b = _query_feature_series(engine, feat_b, lookback_days)

        if len(series_a) < 30 or len(series_b) < 30:
            continue

        # Align on dates
        combined = pd.concat([series_a, series_b], axis=1, join="inner")
        if len(combined) < 20:
            continue

        combined.columns = ["a", "b"]
        returns = combined.pct_change().dropna()
        if len(returns) < 20:
            continue

        # Rolling 30-day correlation
        rolling_corr = returns["a"].rolling(30).corr(returns["b"])
        if rolling_corr.dropna().empty:
            continue

        current_corr = _safe_float(rolling_corr.iloc[-1])
        hist_corr = _safe_float(returns["a"].corr(returns["b"]))

        # Check for significant divergence from expected
        corr_diff = abs(current_corr - expected_corr)

        if corr_diff > 0.4:
            decoupling = current_corr * expected_corr < 0  # Sign flip
            trends.append(Trend(
                name=f"{label} {'Decoupling' if decoupling else 'Correlation Breakdown'}",
                category="correlation",
                direction=DIRECTION_TRANSITIONING,
                strength=_clamp(corr_diff),
                description=(
                    f"{label}: current 30-day correlation is {current_corr:.2f}, "
                    f"expected ~{expected_corr:.2f}. "
                    f"{'Assets have decoupled — sign flipped.' if decoupling else f'Correlation has weakened significantly.'}"
                ),
                supporting_evidence=[
                    f"Current rolling correlation: {current_corr:.2f}",
                    f"Historical full-period correlation: {hist_corr:.2f}",
                    f"Expected correlation: {expected_corr:.2f}",
                ],
                contradicting_evidence=(
                    ["Short-term divergences often revert within weeks"] if corr_diff < 0.6 else []
                ),
                implications=[
                    f"Pair trades based on {label} correlation may fail",
                    f"Potential regime shift in inter-asset dynamics",
                ],
                tickers_affected=tickers,
                started=str(date.today() - timedelta(days=30)),
                confidence=_clamp(0.4 + corr_diff * 0.5),
            ))

    return trends


# ── Category Summaries ────────────────────────────────────────────────────

def _build_category_summaries(trends: list[Trend]) -> dict[str, dict]:
    """Build a summary for each of the 6 trend categories."""
    summaries = {}

    category_labels = {
        "momentum": "Momentum",
        "regime": "Regime",
        "sector_rotation": "Sector Rotation",
        "volatility": "Volatility",
        "liquidity": "Liquidity",
        "correlation": "Correlations",
    }

    for cat in TREND_CATEGORIES:
        cat_trends = [t for t in trends if t.category == cat]

        if not cat_trends:
            summaries[cat] = {
                "label": category_labels.get(cat, cat),
                "direction": DIRECTION_NEUTRAL,
                "strength": 0.0,
                "headline": "No significant trends detected",
                "trend_count": 0,
            }
            continue

        # Aggregate direction: majority vote weighted by strength
        bullish_weight = sum(t.strength for t in cat_trends if t.direction == DIRECTION_BULLISH)
        bearish_weight = sum(t.strength for t in cat_trends if t.direction == DIRECTION_BEARISH)
        trans_weight = sum(t.strength for t in cat_trends if t.direction == DIRECTION_TRANSITIONING)

        if bullish_weight > bearish_weight and bullish_weight > trans_weight:
            direction = DIRECTION_BULLISH
        elif bearish_weight > bullish_weight and bearish_weight > trans_weight:
            direction = DIRECTION_BEARISH
        elif trans_weight > 0:
            direction = DIRECTION_TRANSITIONING
        else:
            direction = DIRECTION_NEUTRAL

        avg_strength = sum(t.strength for t in cat_trends) / len(cat_trends)

        # Pick the strongest trend as headline
        strongest = max(cat_trends, key=lambda t: t.strength)

        summaries[cat] = {
            "label": category_labels.get(cat, cat),
            "direction": direction,
            "strength": round(avg_strength, 2),
            "headline": strongest.name,
            "trend_count": len(cat_trends),
        }

    return summaries


# ── Narrative ─────────────────────────────────────────────────────────────

def _generate_narrative(trends: list[Trend], summaries: dict) -> str:
    """Generate a rule-based narrative synthesis of all trends."""
    if not trends:
        return "No significant trends detected in the current analysis window."

    bullish_cats = [k for k, v in summaries.items() if v["direction"] == DIRECTION_BULLISH]
    bearish_cats = [k for k, v in summaries.items() if v["direction"] == DIRECTION_BEARISH]
    trans_cats = [k for k, v in summaries.items() if v["direction"] == DIRECTION_TRANSITIONING]

    lines = []

    # Overall tone
    if len(bullish_cats) > len(bearish_cats) + 1:
        lines.append(
            "The trend landscape is broadly constructive. "
            f"{len(bullish_cats)} of 6 categories are leaning bullish, "
            "suggesting a convergence of positive signals."
        )
    elif len(bearish_cats) > len(bullish_cats) + 1:
        lines.append(
            "The trend landscape is deteriorating. "
            f"{len(bearish_cats)} of 6 categories are leaning bearish, "
            "suggesting mounting headwinds."
        )
    elif len(trans_cats) >= 3:
        lines.append(
            "Markets are in transition. Multiple categories showing mixed signals "
            "suggests we are at an inflection point where conviction is low and "
            "the next directional move has not yet been determined."
        )
    else:
        lines.append(
            "The trend picture is mixed. Bullish and bearish signals are roughly balanced, "
            "suggesting a choppy, range-bound environment until a catalyst tips the scales."
        )

    # Key callouts — strongest trends
    strong_trends = sorted(trends, key=lambda t: t.strength, reverse=True)[:3]
    for t in strong_trends:
        lines.append(
            f"[{t.category.upper()}] {t.name}: {t.description}"
        )

    # Convergence check
    all_directions = [t.direction for t in trends if t.strength > 0.5]
    if all_directions and len(set(all_directions)) == 1:
        d = all_directions[0]
        lines.append(
            f"HIGH CONVICTION: All strong trends are aligned {d}. "
            f"This convergence historically precedes a significant directional move."
        )
    elif len(set(all_directions)) > 1 and len(all_directions) > 3:
        lines.append(
            "DIVERGENCE WARNING: Strong trends are pointing in different directions. "
            "This internal conflict often resolves with elevated volatility."
        )

    return "\n\n".join(lines)


# ── Main Entry Point ──────────────────────────────────────────────────────

def analyze_trends(engine: Engine, lookback_days: int = 90) -> dict:
    """Run all trend analyses and return a complete TrendReport.

    Parameters
    ----------
    engine : SQLAlchemy Engine
        Database connection for PIT queries.
    lookback_days : int
        How far back to look for trend detection (default 90 days).

    Returns
    -------
    dict
        Serialised TrendReport with trends, summaries, and narrative.
    """
    log.info("Running trend analysis with {d}-day lookback", d=lookback_days)

    all_trends: list[Trend] = []

    # Run each category analyzer
    analyzers = [
        ("momentum", _analyze_momentum),
        ("regime", _analyze_regime),
        ("sector_rotation", _analyze_sector_rotation),
        ("volatility", _analyze_volatility),
        ("liquidity", _analyze_liquidity),
        ("correlation", _analyze_correlations),
    ]

    for name, analyzer_fn in analyzers:
        try:
            category_trends = analyzer_fn(engine, lookback_days)
            all_trends.extend(category_trends)
            log.debug("Trend category {c}: {n} trends detected", c=name, n=len(category_trends))
        except Exception as exc:
            log.warning("Trend analyzer {c} failed: {e}", c=name, e=str(exc))

    # Filter out very weak trends
    all_trends = [t for t in all_trends if t.strength >= MIN_STRENGTH]

    # Build summaries and narrative
    summaries = _build_category_summaries(all_trends)
    narrative = _generate_narrative(all_trends, summaries)

    report = TrendReport(
        trends=all_trends,
        category_summaries=summaries,
        narrative=narrative,
        generated_at=datetime.now(timezone.utc).isoformat(),
    )

    log.info("Trend analysis complete: {n} trends detected", n=len(all_trends))
    return report.to_dict()
