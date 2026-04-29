"""ALPHA-10 / task #113 — Ensemble disagreement as a meta-feature.

The oracle's EnsemblePredictor aggregates votes from 6 model heads
(flow_momentum, regime_contrarian, options_flow, cross_asset, news_energy,
timeseries_enhanced — plus holder_overlap, fundamental, contagion added by
later waves). Their aggregate direction is already rolled up into
``EnsemblePrediction.coherence`` — but coherence is a BLUNT metric:

  coherence = max(bullish_count, bearish_count) / directional_count

That tells you how many heads agree. It does NOT tell you:

  1. How CONFIDENT the disagreeing heads are (two weak bears vs four weak
     bulls is different from two *certain* bears vs four weak bulls).
  2. How the per-head confidence DISTRIBUTES — a tight cluster of 0.6s is
     less informative than a bimodal 0.9/0.2 split.
  3. Whether the disagreement is stable across horizons — if three heads
     disagree at 7d but all six agree at 30d, that's a timing signal,
     not a thesis disagreement.

This module exposes three vote-level statistics:

  ``directional_entropy``  Shannon entropy of the bullish/bearish/neutral
                            distribution, weighted by vote_weight.
  ``confidence_variance``   Variance of per-head confidence across the
                            ensemble.
  ``disagreement_score``    Composite in [0, 1] blending the above —
                            0 = unanimous + uniform confidence, 1 = split +
                            bimodal confidence.

The score is intended as a CONFIDENCE MULTIPLIER: when disagreement is
high, the recommender should shrink Kelly (same pattern as ALPHA-4's
catalyst dampening). The oracle stamps it on ``EnsemblePrediction`` in a
follow-up wiring edit — this module owns the math so the math is
auditable in isolation.

All functions are pure — no DB I/O.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Sequence


@dataclass
class DisagreementMetrics:
    """Per-prediction disagreement snapshot."""

    directional_entropy: float   # 0..log2(3) ≈ 1.585
    confidence_variance: float    # 0..0.25 (for confidence in [0,1])
    disagreement_score: float     # 0..1 composite
    n_votes: int                  # number of contributing model heads
    directional_split: dict[str, float]  # {bullish, bearish, neutral} mass

    def to_dict(self) -> dict[str, Any]:
        return {
            "directional_entropy": round(self.directional_entropy, 4),
            "confidence_variance": round(self.confidence_variance, 6),
            "disagreement_score": round(self.disagreement_score, 4),
            "n_votes": self.n_votes,
            "directional_split": {
                k: round(v, 4) for k, v in self.directional_split.items()
            },
        }


# Shannon entropy over 3 outcomes saturates at log2(3) ≈ 1.585.
_MAX_ENTROPY = math.log2(3)

# Confidence variance for a [0,1] variable is bounded by 0.25 (reached
# when half the heads are at 0 and half at 1). We normalize by 0.25 so
# the variance term stays in [0, 1] alongside the entropy term.
_MAX_CONF_VARIANCE = 0.25

# Composite weighting — entropy dominates because it captures the
# direction split, which matters more than confidence spread for
# trade-sizing decisions.
_ENTROPY_WEIGHT = 0.65
_VARIANCE_WEIGHT = 0.35


def _vote_weight_of(vote: dict[str, Any]) -> float:
    """Pull ``vote_weight`` with a safe fallback to a uniform mass."""
    w = vote.get("vote_weight")
    if w is None:
        return 1.0
    try:
        return max(float(w), 0.0)
    except (TypeError, ValueError):
        return 1.0


def _direction_mass(votes: Sequence[dict[str, Any]]) -> dict[str, float]:
    """Return normalized directional mass across the 3 outcomes.

    Weights are the vote_weight field (hit_rate × confidence × bucket_weight
    from oracle/engine.py::EnsemblePredictor.predict). Missing / zero
    weights fall back to uniform 1.0.
    """
    mass = {"bullish": 0.0, "bearish": 0.0, "neutral": 0.0}
    for v in votes:
        d = str(v.get("direction", "neutral")).lower()
        if d not in mass:
            d = "neutral"
        mass[d] += _vote_weight_of(v)
    total = sum(mass.values())
    if total <= 0:
        return {"bullish": 0.0, "bearish": 0.0, "neutral": 1.0}
    return {k: v / total for k, v in mass.items()}


def directional_entropy(votes: Sequence[dict[str, Any]]) -> float:
    """Shannon entropy of the weighted directional split.

    Returns 0 when every head agrees on a single direction, and log2(3) ≈
    1.585 when the three directions are perfectly balanced.
    """
    split = _direction_mass(votes)
    ent = 0.0
    for p in split.values():
        if p > 0:
            ent -= p * math.log2(p)
    return ent


def confidence_variance(votes: Sequence[dict[str, Any]]) -> float:
    """Variance of per-head ``confidence`` across the ensemble.

    Missing confidences are treated as 0 (conservative — a head that
    didn't report confidence gets no weight in the spread statistic).
    """
    confs = []
    for v in votes:
        c = v.get("confidence")
        if c is None:
            confs.append(0.0)
        else:
            try:
                confs.append(max(0.0, min(1.0, float(c))))
            except (TypeError, ValueError):
                confs.append(0.0)
    if not confs:
        return 0.0
    mean = sum(confs) / len(confs)
    if len(confs) == 1:
        return 0.0
    return sum((c - mean) ** 2 for c in confs) / len(confs)


def disagreement_score(votes: Sequence[dict[str, Any]]) -> float:
    """Composite disagreement score in [0, 1].

    Blends normalized directional entropy (65%) and confidence variance
    (35%). Zero when the ensemble is unanimous with uniform confidence.
    Higher values mean the recommender should shrink Kelly.
    """
    if not votes:
        return 0.0
    ent = directional_entropy(votes) / _MAX_ENTROPY
    var = confidence_variance(votes) / _MAX_CONF_VARIANCE
    score = _ENTROPY_WEIGHT * ent + _VARIANCE_WEIGHT * var
    return max(0.0, min(1.0, score))


def compute_metrics(votes: Sequence[dict[str, Any]]) -> DisagreementMetrics:
    """One-shot rollup of every public metric in this module."""
    if not votes:
        return DisagreementMetrics(
            directional_entropy=0.0,
            confidence_variance=0.0,
            disagreement_score=0.0,
            n_votes=0,
            directional_split={"bullish": 0.0, "bearish": 0.0, "neutral": 1.0},
        )
    return DisagreementMetrics(
        directional_entropy=directional_entropy(votes),
        confidence_variance=confidence_variance(votes),
        disagreement_score=disagreement_score(votes),
        n_votes=len(votes),
        directional_split=_direction_mass(votes),
    )
