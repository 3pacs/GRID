"""ALPHA-8 / task #111 — Market-implied probability comparator.

Every oracle prediction has a probability the market is also pricing
(via options IV, prediction markets like Polymarket/Kalshi, or futures-
implied probabilities). When GRID's prediction diverges from the market
consensus, that divergence is either:

    1. **Edge** — the model sees something the market doesn't yet, OR
    2. **Warning** — the market knows something the model doesn't.

The comparator surfaces the divergence as a feature so the recommender
can choose: amplify Kelly when GRID disagrees with a thinly-traded
contract (likely edge), shrink Kelly when GRID disagrees with a
liquid contract (likely the model is wrong).

This module focuses on the OPTIONS-implied path because that's the one
GRID has rich data for (``options_daily_signals``). Polymarket / Kalshi
ingestion is queued separately as CAT-191; this module's API has a
``polymarket_implied`` parameter that's optional today.

Public API
----------
    options_implied_probability(ticker, target_move_pct, days_to_expiry, engine)
        → MarketImpliedProb{prob, source, raw, ticker, computed_at}

    compare_to_oracle(oracle_prob, market_prob)
        → DivergenceReport{divergence, edge_direction, severity}

All functions are pure where possible — DB I/O is isolated to the
options reader. Statistical math is closed-form Black-Scholes.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class MarketImpliedProb:
    """One market-implied probability snapshot."""

    ticker: str
    prob: float                # Implied probability in [0, 1]
    source: str                # 'options_iv' / 'polymarket' / 'futures' / 'unknown'
    target_move_pct: float     # The move size this prob corresponds to
    horizon_days: int          # Days to expiry (or contract resolution)
    raw_iv: float | None = None  # Raw at-the-money IV used for the calc
    spot: float | None = None    # Spot price used
    computed_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "prob": round(self.prob, 4),
            "source": self.source,
            "target_move_pct": round(self.target_move_pct, 4),
            "horizon_days": self.horizon_days,
            "raw_iv": round(self.raw_iv, 4) if self.raw_iv is not None else None,
            "spot": self.spot,
            "computed_at": self.computed_at.isoformat() if self.computed_at else None,
        }


@dataclass(frozen=True)
class DivergenceReport:
    """One oracle-vs-market divergence event."""

    oracle_prob: float
    market_prob: float
    divergence: float          # Signed: oracle - market, in [-1, 1]
    edge_direction: str         # 'oracle_higher' / 'oracle_lower' / 'aligned'
    severity: str               # 'aligned' / 'mild' / 'moderate' / 'extreme'
    confidence_multiplier: float  # Suggested oracle-confidence multiplier

    def to_dict(self) -> dict[str, Any]:
        return {
            "oracle_prob": round(self.oracle_prob, 4),
            "market_prob": round(self.market_prob, 4),
            "divergence": round(self.divergence, 4),
            "edge_direction": self.edge_direction,
            "severity": self.severity,
            "confidence_multiplier": round(self.confidence_multiplier, 4),
        }


# ── Closed-form options-implied probability ────────────────────────────────


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf — no scipy dependency."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def options_implied_probability_from_iv(
    *,
    spot: float,
    target_price: float,
    iv: float,
    days_to_expiry: int,
    risk_free_rate: float = 0.05,
) -> float:
    """Probability of spot reaching ``target_price`` within ``days_to_expiry``.

    Uses the lognormal model implicit in Black-Scholes:

        P(S_T >= K) = N(d2)   for K above current spot
        P(S_T <= K) = N(-d2)  for K below current spot

    where d2 = (ln(S/K) + (r - σ²/2) × T) / (σ × √T).

    This is a CLOSED-FORM market-implied probability that requires no
    external probability source — it falls straight out of the IV surface
    we already store in ``options_daily_signals``.
    """
    if days_to_expiry <= 0 or iv <= 0 or spot <= 0 or target_price <= 0:
        return 0.5  # Degenerate input → no information
    T = days_to_expiry / 365.0
    sigma_sqrt_t = iv * math.sqrt(T)
    if sigma_sqrt_t == 0:
        return 1.0 if spot >= target_price else 0.0
    d2 = (math.log(spot / target_price) + (risk_free_rate - 0.5 * iv * iv) * T) / sigma_sqrt_t
    if target_price >= spot:
        return _norm_cdf(d2)  # Probability of hitting above target
    return 1.0 - _norm_cdf(d2)  # Probability of hitting below target


def options_implied_probability(
    engine: Engine,
    ticker: str,
    *,
    target_move_pct: float,
    horizon_days: int = 30,
    risk_free_rate: float = 0.05,
) -> MarketImpliedProb | None:
    """Read the latest options snapshot for ``ticker`` and compute the
    market-implied probability of a move of ``target_move_pct`` (signed).

    Positive ``target_move_pct`` → P(spot rises by that pct).
    Negative ``target_move_pct`` → P(spot falls by that pct).

    Returns None when no usable options data is available.
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT spot_price, iv_atm, signal_date
                    FROM options_daily_signals
                    WHERE ticker = :t
                      AND iv_atm IS NOT NULL
                      AND spot_price IS NOT NULL
                    ORDER BY signal_date DESC
                    LIMIT 1
                    """
                ),
                {"t": ticker.upper()},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug("options_implied read failed for {t}: {e}", t=ticker, e=str(exc))
        return None

    if row is None:
        return None

    spot = float(row[0] or 0)
    iv = float(row[1] or 0)
    if spot <= 0 or iv <= 0:
        return None

    target_price = spot * (1.0 + target_move_pct)
    prob = options_implied_probability_from_iv(
        spot=spot,
        target_price=target_price,
        iv=iv,
        days_to_expiry=horizon_days,
        risk_free_rate=risk_free_rate,
    )

    return MarketImpliedProb(
        ticker=ticker.upper(),
        prob=prob,
        source="options_iv",
        target_move_pct=target_move_pct,
        horizon_days=horizon_days,
        raw_iv=iv,
        spot=spot,
        computed_at=datetime.now(timezone.utc),
    )


# ── Divergence comparator ──────────────────────────────────────────────────


# Severity thresholds on |divergence|
_DIV_MILD = 0.05
_DIV_MODERATE = 0.15
_DIV_EXTREME = 0.30


def compare_to_oracle(oracle_prob: float, market_prob: float) -> DivergenceReport:
    """Compare an oracle probability to a market-implied probability.

    Returns a :class:`DivergenceReport` with severity bucketing and a
    suggested confidence multiplier. The multiplier convention:

      - ``aligned`` (|div| < 5pp)        → 1.00x (no change)
      - ``mild`` (5..15pp)                → 1.05x (small edge boost)
      - ``moderate`` (15..30pp)           → 1.10x (real edge OR warning)
      - ``extreme`` (>30pp)               → 0.85x (suspect — shrink)

    The extreme bucket SHRINKS rather than amplifies because a 30pp+
    divergence usually means the model is wrong, not that it's found
    historic alpha. Operator can override this in the recommender.
    """
    o = max(0.0, min(1.0, float(oracle_prob)))
    m = max(0.0, min(1.0, float(market_prob)))
    div = o - m
    abs_div = abs(div)

    if abs_div < _DIV_MILD:
        severity = "aligned"
        mult = 1.00
        direction = "aligned"
    elif abs_div < _DIV_MODERATE:
        severity = "mild"
        mult = 1.05
        direction = "oracle_higher" if div > 0 else "oracle_lower"
    elif abs_div < _DIV_EXTREME:
        severity = "moderate"
        mult = 1.10
        direction = "oracle_higher" if div > 0 else "oracle_lower"
    else:
        severity = "extreme"
        mult = 0.85
        direction = "oracle_higher" if div > 0 else "oracle_lower"

    return DivergenceReport(
        oracle_prob=o,
        market_prob=m,
        divergence=div,
        edge_direction=direction,
        severity=severity,
        confidence_multiplier=mult,
    )
