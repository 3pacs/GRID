"""
GRID Oracle — Signal Aggregator.

Combines typed signals from the signal_registry into a single directional
view for a given model and point-in-time.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from loguru import logger as log


class WeightMode(str, Enum):
    EQUAL = "equal"
    TRUST_WEIGHTED = "trust_weighted"
    RECENCY_WEIGHTED = "recency_weighted"
    LEARNED = "learned"


@dataclass(frozen=True)
class WeightConfig:
    mode: str = WeightMode.EQUAL
    trust_decay_half_life_days: float = 90.0
    min_weight: float = 0.1
    max_weight: float = 3.0
    family_weights: dict[str, float] | None = None

    def __post_init__(self) -> None:
        if self.trust_decay_half_life_days <= 0:
            raise ValueError("trust_decay_half_life_days must be positive")
        if self.min_weight < 0:
            raise ValueError("min_weight must be >= 0")
        if self.max_weight <= self.min_weight:
            raise ValueError("max_weight must be > min_weight")


@dataclass(frozen=True)
class AggregatedSignal:
    direction: str
    strength: float
    confidence: float
    coherence: float
    signal_count: int
    bullish_count: int
    bearish_count: int
    neutral_count: int
    top_contributors: list[dict[str, Any]]
    as_of: datetime


def _decay_factor(age_days: float, half_life_days: float) -> float:
    return math.exp(-math.log(2.0) * age_days / half_life_days)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _age_days(valid_from: datetime, as_of: datetime) -> float:
    delta = as_of - valid_from
    return max(0.0, delta.total_seconds() / 86_400.0)


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


class SignalAggregator:

    def aggregate(
        self,
        signals: list[dict[str, Any]],
        config: WeightConfig,
        as_of: datetime,
        trust_scores: dict[str, float] | None = None,
    ) -> AggregatedSignal:
        if not signals:
            return self._empty(as_of)

        as_of_utc = _ensure_utc(as_of)

        # PIT filter
        valid: list[dict[str, Any]] = []
        for sig in signals:
            vf = _ensure_utc(sig.get("valid_from"))
            if vf is None or vf > as_of_utc:
                continue
            vu = sig.get("valid_until")
            if vu is not None:
                vu_utc = _ensure_utc(vu)
                if vu_utc <= as_of_utc:
                    continue
            valid.append(sig)

        if not valid:
            return self._empty(as_of)

        # Compute weights
        weighted: list[dict[str, Any]] = []
        for sig in valid:
            base = self._base_weight(sig, config, as_of_utc, trust_scores)
            if config.family_weights:
                override = config.family_weights.get(sig.get("source_module", ""))
                if override is not None:
                    base *= override
            eff_weight = _clamp(base, config.min_weight, config.max_weight)
            weighted.append({**sig, "_eff_weight": eff_weight})

        total_weight = sum(e["_eff_weight"] for e in weighted)
        if total_weight == 0.0:
            return self._empty(as_of)

        # Directional vote
        weighted_bull = weighted_bear = confidence_wsum = 0.0
        bullish_count = bearish_count = neutral_count = 0

        for entry in weighted:
            direction = entry.get("direction", "neutral")
            w = entry["_eff_weight"]
            strength = float(entry.get("strength", entry.get("value", 0.5)))
            confidence = float(entry.get("confidence", 0.5))
            confidence_wsum += confidence * w
            if direction == "bullish":
                weighted_bull += w * abs(strength)
                bullish_count += 1
            elif direction == "bearish":
                weighted_bear += w * abs(strength)
                bearish_count += 1
            else:
                neutral_count += 1

        if weighted_bull > weighted_bear:
            net_direction = "bullish"
        elif weighted_bear > weighted_bull:
            net_direction = "bearish"
        else:
            net_direction = "neutral"

        strength_val = abs(weighted_bull - weighted_bear) / total_weight
        avg_confidence = confidence_wsum / total_weight
        directional = bullish_count + bearish_count
        coherence = max(bullish_count, bearish_count) / directional if directional > 0 else 0.0

        top_contributors = [
            {
                "source_module": e.get("source_module"),
                "signal_type": e.get("signal_type"),
                "direction": e.get("direction"),
                "confidence": round(float(e.get("confidence", 0.0)), 4),
                "effective_weight": round(e["_eff_weight"], 4),
            }
            for e in sorted(weighted, key=lambda x: -x["_eff_weight"])[:5]
        ]

        return AggregatedSignal(
            direction=net_direction,
            strength=round(strength_val, 4),
            confidence=round(avg_confidence, 4),
            coherence=round(coherence, 4),
            signal_count=len(weighted),
            bullish_count=bullish_count,
            bearish_count=bearish_count,
            neutral_count=neutral_count,
            top_contributors=top_contributors,
            as_of=as_of,
        )

    def _base_weight(self, sig, config, as_of_utc, trust_scores):
        mode = config.mode
        if mode == WeightMode.EQUAL:
            return 1.0
        if mode == WeightMode.TRUST_WEIGHTED:
            src = sig.get("source_module", "")
            return float((trust_scores or {}).get(src, sig.get("trust_score", 0.5)))
        if mode == WeightMode.RECENCY_WEIGHTED:
            vf = _ensure_utc(sig.get("valid_from"))
            if vf is None:
                return 1.0
            return _decay_factor(_age_days(vf, as_of_utc), config.trust_decay_half_life_days)
        if mode == WeightMode.LEARNED:
            src = sig.get("source_module", "")
            trust = float((trust_scores or {}).get(src, sig.get("trust_score", 0.5)))
            vf = _ensure_utc(sig.get("valid_from"))
            decay = _decay_factor(_age_days(vf, as_of_utc), config.trust_decay_half_life_days) if vf else 1.0
            return trust * decay
        return 1.0

    @staticmethod
    def _empty(as_of):
        return AggregatedSignal(
            direction="neutral", strength=0.0, confidence=0.0, coherence=0.0,
            signal_count=0, bullish_count=0, bearish_count=0, neutral_count=0,
            top_contributors=[], as_of=as_of,
        )
