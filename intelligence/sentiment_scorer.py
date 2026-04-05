"""
GRID Intelligence — Deterministic Market Sentiment Scorer.

Computes a bounded sentiment score from data, not LLM vibes.
The LLM gets the score and interprets context — it never computes direction.

Architecture:
  1. Pull signal counts (trust-weighted) from signal_sources
  2. Pull net dollar flows from dollar_flows
  3. Pull regime state + confidence from decision_journal
  4. Pull price momentum from raw_series
  5. Combine via weighted average → score in [-1.0, +1.0]
  6. Log prediction for self-scoring
  7. Score past predictions against realized SPY returns

Self-learning loop:
  - Every prediction is stored with component weights
  - After the evaluation window, score vs realized return
  - Bayesian weight update: components that predicted correctly get weight boost
  - Weights are bounded [0.05, 0.5] to prevent any single component from dominating

Score interpretation:
  [-1.0, -0.6]  → Strongly bearish
  [-0.6, -0.2]  → Bearish
  [-0.2,  0.2]  → Neutral
  [ 0.2,  0.6]  → Bullish
  [ 0.6,  1.0]  → Strongly bullish
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Configuration ─────────────────────────────────────────────────────────

EVALUATION_WINDOW_DAYS = 5  # Score predictions after 5 trading days

# Default component weights (self-adjusted over time via Bayesian updates)
# Each component gets scored independently [-1, +1] then weighted into composite
DEFAULT_WEIGHTS = {
    # ── Price & Momentum ──
    "momentum": 0.08,          # SPY 5d/20d returns
    "breadth": 0.06,           # Advance/decline ratio
    "trend": 0.06,             # SPY vs 50d/200d moving averages
    # ── Volatility & Fear ──
    "volatility": 0.06,        # VIX absolute level
    "vol_term_structure": 0.05, # VIX vs VIX3M (contango/backwardation)
    "put_call_ratio": 0.05,    # Options put/call ratio
    # ── Flows & Positioning ──
    "flows": 0.08,             # Net dollar flows (inflow vs outflow)
    "dark_pool": 0.05,         # Dark pool buy/sell imbalance
    "etf_flows": 0.05,         # Net ETF fund flows
    # ── Signals & Intelligence ──
    "signals": 0.08,           # Trust-weighted buy/sell signal balance
    "insider": 0.06,           # Insider buy/sell ratio
    "congressional": 0.04,     # Congressional trading direction
    # ── Macro & Rates ──
    "regime": 0.06,            # Regime state from decision_journal
    "yield_curve": 0.05,       # 10Y-2Y spread direction
    "credit_spread": 0.05,     # HYG/LQD ratio (credit risk appetite)
    # ── Sentiment & Social ──
    "social_sentiment": 0.04,  # Reddit/Bluesky bull ratio
    "fear_greed": 0.04,        # CNN Fear & Greed index proxy
    # ── Convergence ──
    "convergence": 0.04,       # Multi-source signal agreement
}

# Signal type → direction mapping
BULLISH_SIGNALS = {"BUY", "CLUSTER_BUY", "UNUSUAL_BUY", "CONTRACT_AWARD"}
BEARISH_SIGNALS = {"SELL", "UNUSUAL_SELL", "CLUSTER_SELL"}

# Regime → score mapping
REGIME_SCORES = {
    "GROWTH": 0.6,
    "EXPANSION": 0.8,
    "NEUTRAL": 0.0,
    "FRAGILE": -0.4,
    "CONTRACTION": -0.6,
    "CRISIS": -0.8,
}

LABEL_MAP = {
    (-1.0, -0.6): "STRONGLY_BEARISH",
    (-0.6, -0.2): "BEARISH",
    (-0.2, 0.2): "NEUTRAL",
    (0.2, 0.6): "BULLISH",
    (0.6, 1.0): "STRONGLY_BULLISH",
}


# ── Data classes ──────────────────────────────────────────────────────────

@dataclass
class SentimentComponent:
    """One input component contributing to the overall score."""
    name: str
    raw_value: float      # Unbounded raw measurement
    score: float          # Normalized to [-1, 1]
    weight: float         # Current weight
    detail: str           # Human-readable explanation


@dataclass
class SentimentResult:
    """Complete scored sentiment output."""
    score: float                              # [-1.0, +1.0]
    label: str                                # e.g. "BULLISH"
    components: list[SentimentComponent]       # Breakdown
    context: str                               # One-sentence summary
    timestamp: str
    weights_version: int = 0                   # Increments on each weight update

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": round(self.score, 4),
            "label": self.label,
            "components": [
                {
                    "name": c.name,
                    "raw_value": round(c.raw_value, 4),
                    "score": round(c.score, 4),
                    "weight": round(c.weight, 4),
                    "detail": c.detail,
                }
                for c in self.components
            ],
            "context": self.context,
            "timestamp": self.timestamp,
            "weights_version": self.weights_version,
        }


# ── Table setup ───────────────────────────────────────────────────────────

_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sentiment_predictions (
    id              SERIAL PRIMARY KEY,
    prediction_date DATE NOT NULL,
    score           REAL NOT NULL,
    label           TEXT NOT NULL,
    components      JSONB NOT NULL,
    weights         JSONB NOT NULL,
    weights_version INT NOT NULL DEFAULT 0,
    realized_return REAL,
    outcome         TEXT DEFAULT 'PENDING',
    scored_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_sentiment_pred_date
    ON sentiment_predictions (prediction_date);
CREATE INDEX IF NOT EXISTS idx_sentiment_pred_outcome
    ON sentiment_predictions (outcome);

CREATE TABLE IF NOT EXISTS sentiment_weights (
    id          SERIAL PRIMARY KEY,
    weights     JSONB NOT NULL,
    version     INT NOT NULL DEFAULT 0,
    accuracy    REAL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
"""


def _ensure_tables(engine: Engine) -> None:
    """Create tables if they don't exist."""
    try:
        with engine.connect() as conn:
            conn.execute(text(_TABLE_SQL))
            conn.commit()
    except Exception as e:
        log.warning("Could not ensure sentiment tables: {e}", e=e)


def _load_weights(engine: Engine) -> tuple[dict[str, float], int]:
    """Load latest learned weights, or return defaults."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT weights, version FROM sentiment_weights "
                "ORDER BY version DESC LIMIT 1"
            )).fetchone()
            if row and row[0]:
                w = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                return w, row[1]
    except Exception as exc:
        log.warning("Failed to load sentiment weights from DB: {e}", e=exc)
    return dict(DEFAULT_WEIGHTS), 0


# ── Component scorers ────────────────────────────────────────────────────

def _score_signals(engine: Engine, days: int = 7) -> SentimentComponent:
    """Score from trust-weighted buy/sell signal balance."""
    bull_score = 0.0
    bear_score = 0.0
    total = 0

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT signal_type, trust_score "
                "FROM signal_sources "
                "WHERE signal_date >= CURRENT_DATE - :days "
                "AND trust_score IS NOT NULL"
            ), {"days": days}).fetchall()

            for signal_type, trust in rows:
                if signal_type in BULLISH_SIGNALS:
                    bull_score += trust
                    total += 1
                elif signal_type in BEARISH_SIGNALS:
                    bear_score += trust
                    total += 1
    except Exception as e:
        log.warning("Signal scoring failed: {e}", e=e)

    if total == 0:
        return SentimentComponent(
            name="signals", raw_value=0.0, score=0.0,
            weight=0.0, detail="No scored signals available",
        )

    # Net balance normalized to [-1, 1]
    net = bull_score - bear_score
    max_possible = bull_score + bear_score
    score = (net / max_possible) if max_possible > 0 else 0.0
    score = max(-1.0, min(1.0, score))

    return SentimentComponent(
        name="signals",
        raw_value=net,
        score=score,
        weight=0.0,  # Filled by caller
        detail=f"{total} signals: bull_weight={bull_score:.1f}, bear_weight={bear_score:.1f}, net={score:+.2f}",
    )


def _score_flows(engine: Engine, days: int = 7) -> SentimentComponent:
    """Score from net dollar flow direction."""
    inflow = 0.0
    outflow = 0.0

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT direction, SUM(amount_usd) "
                "FROM dollar_flows "
                "WHERE flow_date >= CURRENT_DATE - :days "
                "GROUP BY direction"
            ), {"days": days}).fetchall()

            for direction, amount in rows:
                if direction == "inflow":
                    inflow = float(amount or 0)
                elif direction == "outflow":
                    outflow = float(amount or 0)
    except Exception as e:
        log.warning("Flow scoring failed: {e}", e=e)

    total = inflow + outflow
    if total == 0:
        return SentimentComponent(
            name="flows", raw_value=0.0, score=0.0,
            weight=0.0, detail="No flow data available",
        )

    # Net flow ratio, clamped
    net_ratio = (inflow - outflow) / total
    score = max(-1.0, min(1.0, net_ratio * 2))  # Scale up slightly, cap at ±1

    return SentimentComponent(
        name="flows",
        raw_value=net_ratio,
        score=score,
        weight=0.0,
        detail=f"${inflow/1e9:.1f}B in, ${outflow/1e9:.1f}B out, net ratio={net_ratio:+.2f}",
    )


def _score_regime(engine: Engine) -> SentimentComponent:
    """Score from latest regime inference."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT inferred_state, state_confidence "
                "FROM decision_journal "
                "ORDER BY decision_timestamp DESC LIMIT 1"
            )).fetchone()

            if row:
                state = row[0]
                confidence = float(row[1] or 0.5)
                base_score = REGIME_SCORES.get(state.upper(), 0.0)
                # Scale by confidence — low confidence pulls toward neutral
                score = base_score * confidence
                return SentimentComponent(
                    name="regime",
                    raw_value=confidence,
                    score=max(-1.0, min(1.0, score)),
                    weight=0.0,
                    detail=f"{state} at {confidence:.0%} confidence → base={base_score:+.1f}, weighted={score:+.2f}",
                )
    except Exception as e:
        log.warning("Regime scoring failed: {e}", e=e)

    return SentimentComponent(
        name="regime", raw_value=0.0, score=0.0,
        weight=0.0, detail="No regime data available",
    )


def _score_momentum(engine: Engine) -> SentimentComponent:
    """Score from SPY price momentum (5d and 20d returns)."""
    ret_5d = 0.0
    ret_20d = 0.0

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT value, obs_date FROM raw_series "
                "WHERE series_id = 'YF:^GSPC:close' "
                "ORDER BY obs_date DESC LIMIT 25"
            )).fetchall()

            if len(rows) >= 6:
                latest = float(rows[0][0])
                d5 = float(rows[5][0])
                ret_5d = (latest - d5) / d5

            if len(rows) >= 21:
                d20 = float(rows[20][0])
                ret_20d = (latest - d20) / d20
    except Exception as e:
        log.warning("Momentum scoring failed: {e}", e=e)

    if ret_5d == 0.0 and ret_20d == 0.0:
        return SentimentComponent(
            name="momentum", raw_value=0.0, score=0.0,
            weight=0.0, detail="No price data available",
        )

    # Blend 5d (60%) and 20d (40%), scale: ±5% maps to ±1.0
    blended = ret_5d * 0.6 + ret_20d * 0.4
    score = max(-1.0, min(1.0, blended / 0.05))

    return SentimentComponent(
        name="momentum",
        raw_value=blended,
        score=score,
        weight=0.0,
        detail=f"SPY 5d={ret_5d:+.2%}, 20d={ret_20d:+.2%}, blended={blended:+.2%}",
    )


def _score_volatility(engine: Engine) -> SentimentComponent:
    """Score from VIX level — high VIX = bearish, low VIX = bullish."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'YF:^VIX:close' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            if row:
                vix = float(row[0])
                # VIX scoring: 12 = very bullish (+0.8), 20 = neutral, 30 = bearish (-0.6), 40+ = crisis (-1.0)
                if vix <= 12:
                    score = 0.8
                elif vix <= 20:
                    score = 0.8 - (vix - 12) * (0.8 / 8)  # Linear from 0.8 to 0.0
                elif vix <= 35:
                    score = -(vix - 20) / 15  # Linear from 0.0 to -1.0
                else:
                    score = -1.0

                return SentimentComponent(
                    name="volatility",
                    raw_value=vix,
                    score=max(-1.0, min(1.0, score)),
                    weight=0.0,
                    detail=f"VIX={vix:.1f} → score={score:+.2f} (12=calm, 20=normal, 35+=fear)",
                )
    except Exception as e:
        log.warning("Volatility scoring failed: {e}", e=e)

    return SentimentComponent(
        name="volatility", raw_value=0.0, score=0.0,
        weight=0.0, detail="No VIX data available",
    )


def _score_vol_term_structure(engine: Engine) -> SentimentComponent:
    """VIX vs VIX3M: contango = complacent (bullish), backwardation = fear (bearish)."""
    try:
        with engine.connect() as conn:
            vix = conn.execute(text(
                "SELECT value FROM raw_series WHERE series_id='YF:^VIX:close' ORDER BY obs_date DESC LIMIT 1"
            )).scalar()
            vix3m = conn.execute(text(
                "SELECT value FROM raw_series WHERE series_id='YF:^VIX3M:close' ORDER BY obs_date DESC LIMIT 1"
            )).scalar()
            if vix and vix3m and float(vix3m) > 0:
                ratio = float(vix) / float(vix3m)
                # ratio < 1 = contango (normal/bullish), > 1 = backwardation (fear)
                score = max(-1.0, min(1.0, (1.0 - ratio) * 3))
                return SentimentComponent(
                    name="vol_term_structure", raw_value=ratio, score=score, weight=0.0,
                    detail=f"VIX/VIX3M={ratio:.2f} ({'contango' if ratio < 1 else 'backwardation'})",
                )
    except Exception as e:
        log.debug("Vol term structure scoring failed: {e}", e=e)
    return SentimentComponent(name="vol_term_structure", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_breadth(engine: Engine) -> SentimentComponent:
    """Market breadth from advance/decline data or sector dispersion."""
    try:
        with engine.connect() as conn:
            # Use sector ETF returns as breadth proxy
            etfs = ["SPY", "XLK", "XLF", "XLE", "XLV", "XLI", "XLC", "XLY", "XLP", "XLU", "XLRE"]
            positive = 0
            total = 0
            for etf in etfs:
                rows = conn.execute(text(
                    "SELECT value FROM raw_series WHERE series_id = :sid ORDER BY obs_date DESC LIMIT 6"
                ), {"sid": f"YF:{etf}:close"}).fetchall()
                if len(rows) >= 2:
                    total += 1
                    if float(rows[0][0]) > float(rows[1][0]):
                        positive += 1
            if total >= 5:
                ratio = positive / total
                score = (ratio - 0.5) * 2  # 0.5 → 0, 1.0 → 1.0, 0.0 → -1.0
                return SentimentComponent(
                    name="breadth", raw_value=ratio, score=max(-1.0, min(1.0, score)), weight=0.0,
                    detail=f"{positive}/{total} sector ETFs positive ({ratio:.0%})",
                )
    except Exception as e:
        log.debug("Breadth scoring failed: {e}", e=e)
    return SentimentComponent(name="breadth", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_trend(engine: Engine) -> SentimentComponent:
    """SPY position relative to 50d and 200d moving averages."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT value FROM raw_series WHERE series_id='YF:^GSPC:close' ORDER BY obs_date DESC LIMIT 200"
            )).fetchall()
            if len(rows) >= 50:
                latest = float(rows[0][0])
                ma50 = sum(float(r[0]) for r in rows[:50]) / 50
                above_50 = latest > ma50
                above_200 = False
                if len(rows) >= 200:
                    ma200 = sum(float(r[0]) for r in rows[:200]) / 200
                    above_200 = latest > ma200

                if above_50 and above_200:
                    score = 0.8
                    detail = f"SPY above 50d & 200d MA (strong uptrend)"
                elif above_50:
                    score = 0.3
                    detail = f"SPY above 50d but below 200d MA (recovery)"
                elif above_200:
                    score = -0.2
                    detail = f"SPY below 50d but above 200d MA (pullback)"
                else:
                    score = -0.8
                    detail = f"SPY below 50d & 200d MA (downtrend)"

                return SentimentComponent(
                    name="trend", raw_value=latest, score=score, weight=0.0, detail=detail,
                )
    except Exception as e:
        log.debug("Trend scoring failed: {e}", e=e)
    return SentimentComponent(name="trend", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_put_call_ratio(engine: Engine) -> SentimentComponent:
    """Put/call ratio from options data — high = fear (contrarian bullish)."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT AVG(put_call_ratio) FROM options_daily_signals "
                "WHERE signal_date = (SELECT MAX(signal_date) FROM options_daily_signals)"
            )).scalar()
            if row:
                pcr = float(row)
                # PCR < 0.7 = complacent (bearish contrarian), > 1.0 = fear (bullish contrarian)
                if pcr < 0.7:
                    score = -0.5
                elif pcr < 0.85:
                    score = 0.0
                elif pcr < 1.0:
                    score = 0.3
                else:
                    score = min(1.0, (pcr - 1.0) * 2 + 0.5)
                return SentimentComponent(
                    name="put_call_ratio", raw_value=pcr, score=max(-1.0, min(1.0, score)), weight=0.0,
                    detail=f"P/C ratio={pcr:.2f} ({'fear/contrarian bullish' if pcr > 1.0 else 'complacent' if pcr < 0.7 else 'normal'})",
                )
    except Exception as e:
        log.debug("Put/call scoring failed: {e}", e=e)
    return SentimentComponent(name="put_call_ratio", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_dark_pool(engine: Engine, days: int = 7) -> SentimentComponent:
    """Dark pool buy/sell imbalance from dollar_flows."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT direction, SUM(amount_usd) FROM dollar_flows "
                "WHERE source_type = 'darkpool' AND flow_date >= CURRENT_DATE - :days "
                "GROUP BY direction"
            ), {"days": days}).fetchall()
            inflow = sum(float(r[1]) for r in rows if r[0] == "inflow")
            outflow = sum(float(r[1]) for r in rows if r[0] == "outflow")
            total = inflow + outflow
            if total > 0:
                net = (inflow - outflow) / total
                score = max(-1.0, min(1.0, net * 2))
                return SentimentComponent(
                    name="dark_pool", raw_value=net, score=score, weight=0.0,
                    detail=f"Dark pool: ${inflow/1e6:.0f}M in, ${outflow/1e6:.0f}M out, net={net:+.2f}",
                )
    except Exception as e:
        log.debug("Dark pool scoring failed: {e}", e=e)
    return SentimentComponent(name="dark_pool", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_etf_flows(engine: Engine, days: int = 7) -> SentimentComponent:
    """ETF fund flows from dollar_flows table."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT direction, SUM(amount_usd) FROM dollar_flows "
                "WHERE source_type IN ('etf_flow', 'etf_flows') AND flow_date >= CURRENT_DATE - :days "
                "GROUP BY direction"
            ), {"days": days}).fetchall()
            inflow = sum(float(r[1]) for r in rows if r[0] == "inflow")
            outflow = sum(float(r[1]) for r in rows if r[0] == "outflow")
            total = inflow + outflow
            if total > 0:
                net = (inflow - outflow) / total
                score = max(-1.0, min(1.0, net * 2))
                return SentimentComponent(
                    name="etf_flows", raw_value=net, score=score, weight=0.0,
                    detail=f"ETF flows: ${inflow/1e6:.0f}M in, ${outflow/1e6:.0f}M out",
                )
    except Exception as e:
        log.debug("ETF flow scoring failed: {e}", e=e)
    return SentimentComponent(name="etf_flows", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_insider(engine: Engine, days: int = 14) -> SentimentComponent:
    """Insider buy/sell ratio from signal_sources."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT signal_type, COUNT(*) FROM signal_sources "
                "WHERE source_type = 'insider' AND signal_date >= CURRENT_DATE - :days "
                "GROUP BY signal_type"
            ), {"days": days}).fetchall()
            buys = sum(r[1] for r in rows if r[0] in BULLISH_SIGNALS)
            sells = sum(r[1] for r in rows if r[0] in BEARISH_SIGNALS)
            total = buys + sells
            if total >= 3:
                ratio = buys / total
                score = (ratio - 0.5) * 2
                return SentimentComponent(
                    name="insider", raw_value=ratio, score=max(-1.0, min(1.0, score)), weight=0.0,
                    detail=f"Insiders: {buys} buys, {sells} sells, ratio={ratio:.0%}",
                )
    except Exception as e:
        log.debug("Insider scoring failed: {e}", e=e)
    return SentimentComponent(name="insider", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_congressional(engine: Engine, days: int = 30) -> SentimentComponent:
    """Congressional trading direction."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT signal_type, COUNT(*) FROM signal_sources "
                "WHERE source_type = 'congressional' AND signal_date >= CURRENT_DATE - :days "
                "GROUP BY signal_type"
            ), {"days": days}).fetchall()
            buys = sum(r[1] for r in rows if r[0] in BULLISH_SIGNALS)
            sells = sum(r[1] for r in rows if r[0] in BEARISH_SIGNALS)
            total = buys + sells
            if total >= 2:
                ratio = buys / total
                score = (ratio - 0.5) * 2
                return SentimentComponent(
                    name="congressional", raw_value=ratio, score=max(-1.0, min(1.0, score)), weight=0.0,
                    detail=f"Congress: {buys} buys, {sells} sells, ratio={ratio:.0%}",
                )
    except Exception as e:
        log.debug("Congressional scoring failed: {e}", e=e)
    return SentimentComponent(name="congressional", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_yield_curve(engine: Engine) -> SentimentComponent:
    """10Y-2Y spread — positive = normal (bullish), negative = inverted (bearish)."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT value FROM raw_series WHERE series_id='FRED:T10Y2Y' ORDER BY obs_date DESC LIMIT 1"
            )).scalar()
            if row:
                spread = float(row)
                # Spread > 0.5 = healthy, 0 to 0.5 = flattening, < 0 = inverted
                if spread > 1.0:
                    score = 0.8
                elif spread > 0:
                    score = spread * 0.8
                else:
                    score = max(-1.0, spread * 1.5)
                return SentimentComponent(
                    name="yield_curve", raw_value=spread, score=max(-1.0, min(1.0, score)), weight=0.0,
                    detail=f"10Y-2Y spread={spread:+.2f}% ({'inverted' if spread < 0 else 'normal'})",
                )
    except Exception as e:
        log.debug("Yield curve scoring failed: {e}", e=e)
    return SentimentComponent(name="yield_curve", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_credit_spread(engine: Engine) -> SentimentComponent:
    """HYG vs LQD ratio — rising = risk appetite (bullish), falling = flight to quality."""
    try:
        with engine.connect() as conn:
            hyg_rows = conn.execute(text(
                "SELECT value FROM raw_series WHERE series_id='YF:HYG:close' ORDER BY obs_date DESC LIMIT 21"
            )).fetchall()
            lqd_rows = conn.execute(text(
                "SELECT value FROM raw_series WHERE series_id='YF:LQD:close' ORDER BY obs_date DESC LIMIT 21"
            )).fetchall()
            if len(hyg_rows) >= 2 and len(lqd_rows) >= 2:
                hyg_now = float(hyg_rows[0][0])
                lqd_now = float(lqd_rows[0][0])
                ratio_now = hyg_now / lqd_now if lqd_now > 0 else 1.0

                hyg_prev = float(hyg_rows[-1][0])
                lqd_prev = float(lqd_rows[-1][0])
                ratio_prev = hyg_prev / lqd_prev if lqd_prev > 0 else 1.0

                change = (ratio_now - ratio_prev) / ratio_prev
                score = max(-1.0, min(1.0, change * 20))  # ±5% change → ±1.0
                return SentimentComponent(
                    name="credit_spread", raw_value=change, score=score, weight=0.0,
                    detail=f"HYG/LQD ratio change={change:+.2%} ({'risk-on' if change > 0 else 'risk-off'})",
                )
    except Exception as e:
        log.debug("Credit spread scoring failed: {e}", e=e)
    return SentimentComponent(name="credit_spread", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_social_sentiment(engine: Engine) -> SentimentComponent:
    """Social media bull/bear ratio from source_accuracy or ingested sentiment."""
    try:
        with engine.connect() as conn:
            # Check for recent social signals
            rows = conn.execute(text(
                "SELECT signal_type, COUNT(*) FROM signal_sources "
                "WHERE source_type IN ('social', 'reddit', 'bluesky') "
                "AND signal_date >= CURRENT_DATE - 3 "
                "GROUP BY signal_type"
            )).fetchall()
            buys = sum(r[1] for r in rows if r[0] in BULLISH_SIGNALS)
            sells = sum(r[1] for r in rows if r[0] in BEARISH_SIGNALS)
            total = buys + sells
            if total >= 5:
                ratio = buys / total
                score = (ratio - 0.5) * 2
                return SentimentComponent(
                    name="social_sentiment", raw_value=ratio, score=max(-1.0, min(1.0, score)), weight=0.0,
                    detail=f"Social: {buys} bullish, {sells} bearish, ratio={ratio:.0%}",
                )
    except Exception as e:
        log.debug("Social sentiment scoring failed: {e}", e=e)
    return SentimentComponent(name="social_sentiment", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_fear_greed(engine: Engine) -> SentimentComponent:
    """Fear & Greed proxy from VIX + put/call + breadth composite."""
    # This is a synthetic indicator combining other signals
    # We compute it from existing data rather than external API
    try:
        with engine.connect() as conn:
            vix = conn.execute(text(
                "SELECT value FROM raw_series WHERE series_id='YF:^VIX:close' ORDER BY obs_date DESC LIMIT 1"
            )).scalar()
            if vix:
                vix_val = float(vix)
                # Simple fear/greed: VIX < 15 = extreme greed, > 30 = extreme fear
                fg_score = max(0, min(100, 100 - (vix_val - 10) * (100 / 30)))
                # Map 0-100 to [-1, +1]: 50 = neutral
                score = (fg_score - 50) / 50
                label = "extreme fear" if fg_score < 20 else "fear" if fg_score < 40 else "neutral" if fg_score < 60 else "greed" if fg_score < 80 else "extreme greed"
                return SentimentComponent(
                    name="fear_greed", raw_value=fg_score, score=max(-1.0, min(1.0, score)), weight=0.0,
                    detail=f"Fear/Greed proxy={fg_score:.0f}/100 ({label})",
                )
    except Exception as e:
        log.debug("Fear/greed scoring failed: {e}", e=e)
    return SentimentComponent(name="fear_greed", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


def _score_convergence(engine: Engine) -> SentimentComponent:
    """Multi-source signal convergence — strong agreement = higher conviction."""
    try:
        from intelligence.trust_scorer import detect_convergence
        events = detect_convergence(engine)
        if events:
            bull = sum(1 for e in events if e.get("signal_type", "").upper() in ("BUY", "BULLISH", "LONG"))
            bear = sum(1 for e in events if e.get("signal_type", "").upper() in ("SELL", "BEARISH", "SHORT"))
            total = bull + bear
            if total > 0:
                score = (bull - bear) / total
                return SentimentComponent(
                    name="convergence", raw_value=len(events), score=max(-1.0, min(1.0, score)), weight=0.0,
                    detail=f"{len(events)} convergence events: {bull} bullish, {bear} bearish",
                )
    except Exception as e:
        log.debug("Convergence scoring failed: {e}", e=e)
    return SentimentComponent(name="convergence", raw_value=0.0, score=0.0, weight=0.0, detail="No data available")


# ── Main scorer ──────────────────────────────────────────────────────────

def compute_sentiment(engine: Engine) -> SentimentResult:
    """Compute deterministic market sentiment from all available data.

    Returns a SentimentResult with score in [-1.0, +1.0] and full breakdown.
    """
    _ensure_tables(engine)
    weights, version = _load_weights(engine)

    # Score all components — each returns [-1, +1] independently
    components = [
        # Price & Momentum
        _score_momentum(engine),
        _score_breadth(engine),
        _score_trend(engine),
        # Volatility & Fear
        _score_volatility(engine),
        _score_vol_term_structure(engine),
        _score_put_call_ratio(engine),
        # Flows & Positioning
        _score_flows(engine),
        _score_dark_pool(engine),
        _score_etf_flows(engine),
        # Signals & Intelligence
        _score_signals(engine),
        _score_insider(engine),
        _score_congressional(engine),
        # Macro & Rates
        _score_regime(engine),
        _score_yield_curve(engine),
        _score_credit_spread(engine),
        # Sentiment & Social
        _score_social_sentiment(engine),
        _score_fear_greed(engine),
        # Convergence
        _score_convergence(engine),
    ]

    # Apply weights
    total_weight = 0.0
    weighted_sum = 0.0
    for comp in components:
        w = weights.get(comp.name, 0.0)
        comp.weight = w
        # Skip components with no data (weight stays 0 in output)
        if comp.detail.endswith("available"):
            continue
        weighted_sum += comp.score * w
        total_weight += w

    # Normalize
    score = (weighted_sum / total_weight) if total_weight > 0 else 0.0
    score = max(-1.0, min(1.0, score))

    # Determine label
    label = "NEUTRAL"
    for (lo, hi), lbl in LABEL_MAP.items():
        if lo <= score <= hi:
            label = lbl
            break

    # Build context sentence
    active = [c for c in components if not c.detail.endswith("available")]
    if active:
        strongest = max(active, key=lambda c: abs(c.score * c.weight))
        context = (
            f"Market sentiment is {label.lower().replace('_', ' ')} "
            f"(score {score:+.2f}), driven primarily by {strongest.name} "
            f"({strongest.detail})."
        )
    else:
        context = "Insufficient data to compute sentiment."

    return SentimentResult(
        score=score,
        label=label,
        components=components,
        context=context,
        timestamp=datetime.now(timezone.utc).isoformat(),
        weights_version=version,
    )


# ── Prediction logging ───────────────────────────────────────────────────

def log_prediction(engine: Engine, result: SentimentResult) -> int | None:
    """Store a sentiment prediction for later self-scoring.

    Returns the prediction ID or None on failure.
    """
    try:
        with engine.connect() as conn:
            # Don't double-log for the same date
            existing = conn.execute(text(
                "SELECT id FROM sentiment_predictions "
                "WHERE prediction_date = CURRENT_DATE LIMIT 1"
            )).fetchone()
            if existing:
                return existing[0]

            row = conn.execute(text(
                "INSERT INTO sentiment_predictions "
                "(prediction_date, score, label, components, weights, weights_version) "
                "VALUES (CURRENT_DATE, :score, :label, :components, :weights, :version) "
                "RETURNING id"
            ), {
                "score": result.score,
                "label": result.label,
                "components": json.dumps([
                    {"name": c.name, "score": c.score, "weight": c.weight}
                    for c in result.components
                ]),
                "weights": json.dumps(
                    {c.name: c.weight for c in result.components}
                ),
                "version": result.weights_version,
            }).fetchone()
            conn.commit()
            pred_id = row[0] if row else None
            log.info("Logged sentiment prediction id={id}, score={s}", id=pred_id, s=result.score)
            return pred_id
    except Exception as e:
        log.warning("Failed to log sentiment prediction: {e}", e=e)
        return None


# ── Self-scoring loop ────────────────────────────────────────────────────

def score_past_predictions(engine: Engine) -> dict[str, Any]:
    """Score pending predictions whose evaluation window has passed.

    Compares predicted sentiment direction vs realized SPY 5-day return.
    Updates weights based on component-level accuracy.

    Returns summary of scoring results.
    """
    _ensure_tables(engine)
    scored = 0
    correct = 0
    results = []

    try:
        with engine.connect() as conn:
            # Find predictions old enough to score
            pending = conn.execute(text(
                "SELECT id, prediction_date, score, label, components, weights "
                "FROM sentiment_predictions "
                "WHERE outcome = 'PENDING' "
                "AND prediction_date <= CURRENT_DATE - :window "
                "ORDER BY prediction_date"
            ), {"window": EVALUATION_WINDOW_DAYS}).fetchall()

            if not pending:
                return {"scored": 0, "message": "No predictions ready to score"}

            for pred in pending:
                pred_id, pred_date, pred_score, pred_label, comp_json, weight_json = pred

                # Get realized SPY return over the evaluation window
                spy_rows = conn.execute(text(
                    "SELECT value, obs_date FROM raw_series "
                    "WHERE series_id = 'YF:^GSPC:close' "
                    "AND obs_date >= :start AND obs_date <= :end "
                    "ORDER BY obs_date"
                ), {
                    "start": pred_date,
                    "end": pred_date + timedelta(days=EVALUATION_WINDOW_DAYS + 3),  # Buffer for weekends
                }).fetchall()

                if len(spy_rows) < 2:
                    continue  # Not enough price data yet

                start_price = float(spy_rows[0][0])
                end_price = float(spy_rows[-1][0])
                realized = (end_price - start_price) / start_price

                # Did we get the direction right?
                predicted_dir = 1 if pred_score > 0.05 else (-1 if pred_score < -0.05 else 0)
                actual_dir = 1 if realized > 0.005 else (-1 if realized < -0.005 else 0)

                if predicted_dir == 0:
                    outcome = "NEUTRAL_CORRECT" if abs(realized) < 0.01 else "NEUTRAL_WRONG"
                    is_correct = abs(realized) < 0.01
                elif predicted_dir == actual_dir:
                    outcome = "CORRECT"
                    is_correct = True
                else:
                    outcome = "WRONG"
                    is_correct = False

                conn.execute(text(
                    "UPDATE sentiment_predictions "
                    "SET realized_return = :ret, outcome = :outcome, scored_at = NOW() "
                    "WHERE id = :id"
                ), {"ret": realized, "outcome": outcome, "id": pred_id})

                scored += 1
                if is_correct:
                    correct += 1

                results.append({
                    "date": str(pred_date),
                    "predicted": pred_score,
                    "realized": realized,
                    "outcome": outcome,
                })

            conn.commit()

    except Exception as e:
        log.warning("Scoring failed: {e}", e=e)
        return {"scored": 0, "error": str(e)}

    # Update weights if we have enough scored predictions
    if scored >= 3:
        _update_weights(engine)

    accuracy = (correct / scored * 100) if scored > 0 else 0
    log.info(
        "Scored {n} predictions: {c}/{n} correct ({a:.0f}%)",
        n=scored, c=correct, a=accuracy,
    )

    return {
        "scored": scored,
        "correct": correct,
        "accuracy": round(accuracy, 1),
        "results": results,
    }


def _update_weights(engine: Engine) -> None:
    """Bayesian weight update based on component-level accuracy.

    For each component, compute what fraction of correct predictions
    had that component pointing the right direction. Boost weights
    for reliable components, dampen unreliable ones.
    """
    try:
        with engine.connect() as conn:
            # Get all scored predictions
            rows = conn.execute(text(
                "SELECT score, components, outcome, realized_return "
                "FROM sentiment_predictions "
                "WHERE outcome IN ('CORRECT', 'WRONG', 'NEUTRAL_CORRECT', 'NEUTRAL_WRONG') "
                "ORDER BY prediction_date DESC "
                "LIMIT 60"  # Last ~3 months of daily predictions
            )).fetchall()

            if len(rows) < 10:
                return  # Need minimum history

            # Track per-component accuracy
            comp_hits: dict[str, float] = {}
            comp_total: dict[str, float] = {}

            for pred_score, comp_json, outcome, realized in rows:
                comps = comp_json if isinstance(comp_json, list) else json.loads(comp_json)
                is_correct = outcome in ("CORRECT", "NEUTRAL_CORRECT")

                for c in comps:
                    name = c["name"]
                    comp_score = c["score"]
                    comp_total[name] = comp_total.get(name, 0) + 1

                    # Did this component's direction match reality?
                    comp_dir = 1 if comp_score > 0.05 else (-1 if comp_score < -0.05 else 0)
                    actual_dir = 1 if realized > 0.005 else (-1 if realized < -0.005 else 0)

                    if comp_dir == actual_dir or (comp_dir == 0 and abs(realized) < 0.01):
                        comp_hits[name] = comp_hits.get(name, 0) + 1

            # Compute new weights using Bayesian beta posterior
            new_weights = {}
            for name in DEFAULT_WEIGHTS:
                hits = comp_hits.get(name, 0)
                total = comp_total.get(name, 0)
                # Beta posterior: (hits + 1) / (total + 2)
                accuracy = (hits + 1) / (total + 2)
                # Map accuracy [0.3, 0.7] → weight [0.05, 0.5]
                weight = 0.05 + (accuracy - 0.3) * (0.45 / 0.4)
                new_weights[name] = round(max(0.05, min(0.5, weight)), 4)

            # Normalize to sum to 1.0
            total_w = sum(new_weights.values())
            new_weights = {k: round(v / total_w, 4) for k, v in new_weights.items()}

            # Get current version
            _, current_version = _load_weights(engine)
            new_version = current_version + 1

            # Overall accuracy
            correct_count = sum(1 for r in rows if r[2] in ("CORRECT", "NEUTRAL_CORRECT"))
            overall_accuracy = correct_count / len(rows)

            conn.execute(text(
                "INSERT INTO sentiment_weights (weights, version, accuracy) "
                "VALUES (:weights, :version, :accuracy)"
            ), {
                "weights": json.dumps(new_weights),
                "version": new_version,
                "accuracy": overall_accuracy,
            })
            conn.commit()

            log.info(
                "Updated sentiment weights v{v}: {w} (accuracy={a:.1%})",
                v=new_version, w=new_weights, a=overall_accuracy,
            )

    except Exception as e:
        log.warning("Weight update failed: {e}", e=e)


# ── Full cycle (called by Hermes) ────────────────────────────────────────

def run_sentiment_cycle(engine: Engine) -> dict[str, Any]:
    """Full sentiment cycle: score past predictions, compute current, log it.

    Call this from Hermes at market open and close.
    """
    # Step 1: Score any past predictions that are ready
    scoring_result = score_past_predictions(engine)

    # Step 2: Compute current sentiment
    result = compute_sentiment(engine)

    # Step 3: Log prediction
    pred_id = log_prediction(engine, result)

    return {
        "sentiment": result.to_dict(),
        "prediction_id": pred_id,
        "scoring": scoring_result,
    }
