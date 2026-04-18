"""CAT-182 — Consensus crowdedness detector.

When GRID's prediction aligns with a heavily-crowded trade (high short
interest on the loser, high long exposure on the winner, heavy media
attention), that alignment is a WARNING sign, not a confirmation — the
easy money has already moved. The recommender should discount Kelly
when crowdedness is high, even when all the fundamental signals agree.

Inputs
------
  1. Short interest ratio       — high SI on the ticker GRID says will DROP
                                  means the trade is crowded short
  2. Institutional ownership    — high concentration + high exposure
  3. Analyst recommendation avg — every street analyst already at Buy
  4. Media mention velocity     — news article volume over last 7d
  5. Options positioning skew   — put/call OI on the directional bet

Each input gets a 0..1 crowdedness subscore; the final composite is the
weighted average. When crowdedness > 0.7 AND the oracle's direction
matches the crowd's direction, the recommender should apply a 0.80x
confidence dampening (the "consensus crowd" penalty).

Why this matters (Tier A catalog #182): the alpha has to come from
somewhere. If GRID and the crowd agree AND the crowd is already heavily
positioned, there's nobody left to provide the incremental flow that
would realize the thesis. The trade's expected value is close to the
crowd's cost basis, not your entry.

All functions are pure of DB semantics — they take an engine and return
structured dataclasses. Missing inputs fall through gracefully with a
"partial_crowdedness" flag.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Crowdedness component configuration ──────────────────────────────────

# Weight each component contributes to the composite score.
# Sum should be 1.0. Short interest + institutional get the lion's share
# because they're the most directly measurable.
_WEIGHTS: dict[str, float] = {
    "short_interest": 0.30,
    "institutional": 0.25,
    "analyst_consensus": 0.15,
    "media_velocity": 0.15,
    "options_skew": 0.15,
}

# Thresholds that signal a FULL crowdedness score (1.0) for each component.
# Below the threshold the score interpolates linearly from 0.
_FULL_CROWD: dict[str, float] = {
    "short_interest": 0.25,        # short interest > 25% of float
    "institutional": 0.85,          # >85% institutional ownership
    "analyst_consensus": 4.5,       # >4.5 on 1-5 scale (5=strong buy)
    "media_velocity": 50.0,         # >50 articles/week
    "options_skew": 2.0,            # put/call OI ratio >2 or <0.5
}

# When composite exceeds this threshold, the detector flags "crowded"
_CROWDED_THRESHOLD = 0.70

# Confidence multipliers: applied only when oracle direction AGREES with crowd
_DAMPING_CROWDED = 0.80    # Agree with crowded crowd → shrink
_DAMPING_NEUTRAL = 1.00    # Disagree or uncrowded → no change


# ── Data classes ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CrowdednessComponent:
    """One leg of the crowdedness composite."""

    label: str
    raw_value: float
    normalized: float            # 0..1
    weight: float

    @property
    def contribution(self) -> float:
        return self.normalized * self.weight

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "raw_value": round(self.raw_value, 4),
            "normalized": round(self.normalized, 4),
            "weight": self.weight,
            "contribution": round(self.contribution, 4),
        }


@dataclass(frozen=True)
class CrowdednessResult:
    """Ticker-level crowdedness snapshot."""

    ticker: str
    as_of: date
    score: float                 # 0..1 composite
    is_crowded: bool              # score >= _CROWDED_THRESHOLD
    crowd_direction: str | None   # 'bullish' / 'bearish' / None
    components: list[CrowdednessComponent]
    missing: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "as_of": self.as_of.isoformat(),
            "score": round(self.score, 4),
            "is_crowded": self.is_crowded,
            "crowd_direction": self.crowd_direction,
            "components": [c.to_dict() for c in self.components],
            "missing": list(self.missing),
        }


@dataclass(frozen=True)
class CrowdednessPenalty:
    """The multiplier the recommender should apply."""

    ticker: str
    oracle_direction: str
    crowd_direction: str | None
    crowdedness_score: float
    aligned: bool                    # oracle direction == crowd direction
    multiplier: float                 # applied to oracle confidence
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "oracle_direction": self.oracle_direction,
            "crowd_direction": self.crowd_direction,
            "crowdedness_score": round(self.crowdedness_score, 4),
            "aligned": self.aligned,
            "multiplier": round(self.multiplier, 4),
            "reason": self.reason,
        }


# ── Pure-function helpers ────────────────────────────────────────────────


def _normalize(raw: float, threshold: float) -> float:
    """Linear 0..1 interpolation capped at 1."""
    if threshold <= 0 or raw <= 0:
        return 0.0
    return min(1.0, raw / threshold)


def _normalize_options_skew(pcr: float) -> float:
    """Put/call OI ratio — crowded in EITHER direction (puts OR calls).

    Normal PCR is around 0.8..1.2. Extreme values in either direction
    indicate crowded positioning.
    """
    if pcr <= 0:
        return 0.0
    # Distance from 1.0 (neutral) in log space, capped at threshold
    from math import log
    distance = abs(log(pcr))
    threshold_log = log(_FULL_CROWD["options_skew"])
    return min(1.0, distance / threshold_log) if threshold_log > 0 else 0.0


def compose_crowdedness(
    *,
    ticker: str,
    short_interest: float | None = None,
    institutional_pct: float | None = None,
    analyst_rating_avg: float | None = None,
    media_articles_week: int | None = None,
    put_call_oi_ratio: float | None = None,
    as_of: date | None = None,
) -> CrowdednessResult:
    """Pure-function composer — no DB I/O.

    Each input is optional; missing inputs are tracked in the result and
    the composite is computed over the present components only.
    """
    if as_of is None:
        as_of = date.today()

    components: list[CrowdednessComponent] = []
    missing: list[str] = []

    # Short interest
    if short_interest is not None:
        norm = _normalize(short_interest, _FULL_CROWD["short_interest"])
        components.append(CrowdednessComponent(
            label="short_interest",
            raw_value=float(short_interest),
            normalized=norm,
            weight=_WEIGHTS["short_interest"],
        ))
    else:
        missing.append("short_interest")

    # Institutional concentration
    if institutional_pct is not None:
        norm = _normalize(institutional_pct, _FULL_CROWD["institutional"])
        components.append(CrowdednessComponent(
            label="institutional",
            raw_value=float(institutional_pct),
            normalized=norm,
            weight=_WEIGHTS["institutional"],
        ))
    else:
        missing.append("institutional")

    # Analyst consensus (1-5 scale; higher = more buy)
    if analyst_rating_avg is not None:
        norm = _normalize(analyst_rating_avg, _FULL_CROWD["analyst_consensus"])
        components.append(CrowdednessComponent(
            label="analyst_consensus",
            raw_value=float(analyst_rating_avg),
            normalized=norm,
            weight=_WEIGHTS["analyst_consensus"],
        ))
    else:
        missing.append("analyst_consensus")

    # Media velocity
    if media_articles_week is not None:
        norm = _normalize(float(media_articles_week), _FULL_CROWD["media_velocity"])
        components.append(CrowdednessComponent(
            label="media_velocity",
            raw_value=float(media_articles_week),
            normalized=norm,
            weight=_WEIGHTS["media_velocity"],
        ))
    else:
        missing.append("media_velocity")

    # Options skew
    if put_call_oi_ratio is not None and put_call_oi_ratio > 0:
        norm = _normalize_options_skew(put_call_oi_ratio)
        components.append(CrowdednessComponent(
            label="options_skew",
            raw_value=float(put_call_oi_ratio),
            normalized=norm,
            weight=_WEIGHTS["options_skew"],
        ))
    else:
        missing.append("options_skew")

    if not components:
        return CrowdednessResult(
            ticker=ticker,
            as_of=as_of,
            score=0.0,
            is_crowded=False,
            crowd_direction=None,
            components=[],
            missing=missing,
        )

    # Renormalize weights to actual present components
    total_weight = sum(c.weight for c in components)
    if total_weight <= 0:
        score = 0.0
    else:
        score = sum(c.contribution for c in components) / total_weight

    # Infer crowd direction — short interest dominant → bearish crowd;
    # analyst consensus + institutional dominant → bullish crowd.
    # When neither dominates, None.
    crowd_direction: str | None = None
    short_comp = next((c for c in components if c.label == "short_interest"), None)
    bullish_comps = [
        c for c in components
        if c.label in ("institutional", "analyst_consensus") and c.normalized > 0.6
    ]
    if short_comp and short_comp.normalized > 0.6:
        crowd_direction = "bearish"
    elif len(bullish_comps) >= 1:
        crowd_direction = "bullish"

    # Options skew can OVERRIDE — extreme put/call ratio says more than
    # the general direction heuristics.
    opts = next((c for c in components if c.label == "options_skew"), None)
    if opts and opts.normalized > 0.7:
        if opts.raw_value > 1.0:
            crowd_direction = "bearish"  # heavy puts = bearish crowd
        else:
            crowd_direction = "bullish"  # heavy calls = bullish crowd

    return CrowdednessResult(
        ticker=ticker,
        as_of=as_of,
        score=score,
        is_crowded=score >= _CROWDED_THRESHOLD,
        crowd_direction=crowd_direction,
        components=components,
        missing=missing,
    )


def compute_penalty(
    crowdedness: CrowdednessResult,
    oracle_direction: str,
) -> CrowdednessPenalty:
    """Return the confidence multiplier to apply to an oracle prediction.

    Penalty triggers only when:
      1. crowdedness.is_crowded is True (score >= _CROWDED_THRESHOLD), AND
      2. oracle_direction matches crowdedness.crowd_direction

    Otherwise the multiplier is 1.0 (no change).
    """
    o_dir = (oracle_direction or "").lower()
    c_dir = crowdedness.crowd_direction
    aligned = False
    multiplier = _DAMPING_NEUTRAL
    reason = "not crowded" if not crowdedness.is_crowded else "direction mismatch"

    if crowdedness.is_crowded and c_dir and o_dir == c_dir:
        aligned = True
        multiplier = _DAMPING_CROWDED
        reason = (
            f"oracle and crowd both {o_dir}; crowdedness "
            f"{crowdedness.score:.2f} ≥ threshold — shrink Kelly"
        )

    return CrowdednessPenalty(
        ticker=crowdedness.ticker,
        oracle_direction=o_dir,
        crowd_direction=c_dir,
        crowdedness_score=crowdedness.score,
        aligned=aligned,
        multiplier=multiplier,
        reason=reason,
    )


# ── DB I/O ───────────────────────────────────────────────────────────────


def _read_short_interest(engine: Engine, ticker: str) -> float | None:
    """Read latest short interest ratio from institutional_holdings-adjacent data."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT value
                    FROM raw_series
                    WHERE series_id = :sid
                    ORDER BY obs_date DESC
                    LIMIT 1
                    """
                ),
                {"sid": f"finra_short_interest:{ticker.upper()}"},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug("short_interest read failed for {t}: {e}", t=ticker, e=str(exc))
        return None
    return float(row[0]) if row else None


def _read_media_velocity(engine: Engine, ticker: str) -> int | None:
    """Count news_articles mentions in the last 7 days."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM news_articles
                    WHERE :ticker = ANY(tickers)
                      AND published_at >= NOW() - INTERVAL '7 days'
                    """
                ),
                {"ticker": ticker.upper()},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug("media_velocity read failed for {t}: {e}", t=ticker, e=str(exc))
        return None
    return int(row[0]) if row else None


def _read_options_pcr(engine: Engine, ticker: str) -> float | None:
    """Read latest put/call OI ratio from options_daily_signals."""
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT put_call_ratio
                    FROM options_daily_signals
                    WHERE ticker = :t
                      AND put_call_ratio IS NOT NULL
                    ORDER BY signal_date DESC
                    LIMIT 1
                    """
                ),
                {"t": ticker.upper()},
            ).fetchone()
    except Exception as exc:  # noqa: BLE001
        log.debug("options_pcr read failed for {t}: {e}", t=ticker, e=str(exc))
        return None
    return float(row[0]) if row else None


def compute_crowdedness(
    engine: Engine,
    ticker: str,
) -> CrowdednessResult:
    """Read every available crowdedness signal and return the composite."""
    return compose_crowdedness(
        ticker=ticker,
        short_interest=_read_short_interest(engine, ticker),
        institutional_pct=None,        # TODO: wire when 13F aggregator lands
        analyst_rating_avg=None,        # TODO: wire when analyst_ratings lands
        media_articles_week=_read_media_velocity(engine, ticker),
        put_call_oi_ratio=_read_options_pcr(engine, ticker),
    )
