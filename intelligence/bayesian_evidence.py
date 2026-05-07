"""CAT-178 — Bayesian evidence combiner.

The current oracle combines signals via a weighted vote:

    score = Σ weight_i × direction_i × confidence_i

That's OK but loses Bayesian information — a signal that says "70% bullish"
isn't just a directional vote, it's a likelihood distribution that should
update a prior.

This module implements the explicit Bayesian accumulator:

    log_odds_posterior = log_odds_prior + Σ log_likelihood_ratio_i

where LLR_i = log(p(evidence_i | hypothesis) / p(evidence_i | not hypothesis)).
For a signal reporting confidence c ∈ [0, 1] about a binary outcome:

    LLR_i = log(c / (1 - c)) = logit(c)

So combining signals is just summing logits. This is mathematically
equivalent to the weighted vote when weights equal 1 and signals are
independent, but exposes two features the vote path cannot:

  1. EXPLICIT PRIORS — start from a base rate (e.g. historical hit rate)
     not from 50/50
  2. CORRELATION PENALTY — when two signals are correlated, their combined
     LLR is LESS than the sum (we use a correlation-adjusted effective n)

Why this matters (Tier A catalog #178): without correlation adjustment,
having 6 options signals all say "bullish" would move the posterior as
hard as having 6 INDEPENDENT signals agree — but they're really one
signal counted six times. The correlation penalty is the fix.

All functions are pure — no DB I/O.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Sequence


# Numerical safety bounds for logit conversion
_MIN_PROB = 1e-6
_MAX_PROB = 1.0 - _MIN_PROB


@dataclass(frozen=True)
class EvidenceItem:
    """One piece of evidence feeding the Bayesian accumulator."""

    name: str
    probability: float          # P(hypothesis | this signal alone), in [0, 1]
    weight: float = 1.0          # multiplier on the LLR (≤1 shrinks)
    family: str = ""             # for correlation grouping

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "probability": round(self.probability, 4),
            "weight": self.weight,
            "family": self.family,
        }


@dataclass(frozen=True)
class BayesianResult:
    """Output of the Bayesian combiner."""

    prior: float                 # P(hypothesis) before any evidence
    posterior: float              # P(hypothesis | all evidence)
    log_odds_prior: float
    log_odds_posterior: float
    evidence: list[EvidenceItem]
    n_effective: float            # correlation-adjusted effective count
    family_shares: dict[str, int]  # count of items per family

    def to_dict(self) -> dict[str, Any]:
        return {
            "prior": round(self.prior, 4),
            "posterior": round(self.posterior, 4),
            "log_odds_prior": round(self.log_odds_prior, 4),
            "log_odds_posterior": round(self.log_odds_posterior, 4),
            "n_effective": round(self.n_effective, 4),
            "family_shares": dict(self.family_shares),
            "evidence": [e.to_dict() for e in self.evidence],
        }


# ── Core math ────────────────────────────────────────────────────────────


def logit(p: float) -> float:
    """Clamped logit — returns log(p / (1 - p)).

    Saturates at ±log(1e6) ≈ ±13.8 to avoid infinities when callers pass
    0 or 1. The clamp is symmetric so log_odds stay finite.
    """
    p_clamped = max(_MIN_PROB, min(_MAX_PROB, float(p)))
    return math.log(p_clamped / (1.0 - p_clamped))


def sigmoid(log_odds: float) -> float:
    """Inverse of logit — maps log-odds back to probability."""
    if log_odds > 500:
        return 1.0
    if log_odds < -500:
        return 0.0
    return 1.0 / (1.0 + math.exp(-log_odds))


def _effective_n(family_counts: dict[str, int]) -> float:
    """Compute the correlation-adjusted effective number of independent signals.

    Signals WITHIN a family are treated as perfectly correlated — we count
    them as a single piece of evidence. Signals ACROSS families are treated
    as independent.

    Examples:
        {options: 4, insider: 1}       → effective = 2 (one options, one insider)
        {options: 4, insider: 2}       → effective = 2
        {options: 1, insider: 1, news: 1} → effective = 3

    Items without a family get counted individually (one each), since we
    can't prove they're correlated with anything.
    """
    if not family_counts:
        return 0.0
    effective = 0.0
    for family, count in family_counts.items():
        if not family:
            # Unnamed family items count individually
            effective += count
        else:
            # Same-family items collapse to one
            effective += 1.0 if count > 0 else 0.0
    return effective


def combine_evidence(
    evidence: Sequence[EvidenceItem],
    *,
    prior: float = 0.5,
    correlation_adjust: bool = True,
) -> BayesianResult:
    """Combine a list of EvidenceItems into a single posterior probability.

    Parameters
    ----------
    evidence:
        Sequence of :class:`EvidenceItem`. Order-independent.
    prior:
        P(hypothesis) before any evidence. Defaults to 0.5 (no prior).
    correlation_adjust:
        When True, signals within the same ``family`` are treated as
        perfectly correlated and only contribute ONE vote at the
        family-average logit. When False, each signal contributes
        independently (equivalent to the naive Bayes combiner).

    Returns a :class:`BayesianResult` with prior + posterior + LLR
    breakdown.
    """
    log_odds_prior = logit(prior)
    family_counts: dict[str, int] = {}

    if not evidence:
        return BayesianResult(
            prior=prior,
            posterior=prior,
            log_odds_prior=log_odds_prior,
            log_odds_posterior=log_odds_prior,
            evidence=[],
            n_effective=0.0,
            family_shares={},
        )

    # Group by family (empty-string families stay ungrouped)
    grouped: dict[str, list[EvidenceItem]] = {}
    for item in evidence:
        key = item.family or f"__solo_{item.name}"
        grouped.setdefault(key, []).append(item)
        if item.family:
            family_counts[item.family] = family_counts.get(item.family, 0) + 1

    total_llr = 0.0

    if correlation_adjust:
        # Per-family: compute the family-average logit, then add it ONCE
        for key, items in grouped.items():
            avg_logit = sum(logit(i.probability) * i.weight for i in items) / len(items)
            total_llr += avg_logit
    else:
        # Naive: sum all LLRs, no correlation penalty
        for item in evidence:
            total_llr += logit(item.probability) * item.weight

    log_odds_posterior = log_odds_prior + total_llr
    posterior = sigmoid(log_odds_posterior)

    n_eff = _effective_n(family_counts) if correlation_adjust else float(len(evidence))

    # Include ungrouped items in n_eff too
    ungrouped_count = sum(1 for i in evidence if not i.family)
    if correlation_adjust:
        n_eff += ungrouped_count

    return BayesianResult(
        prior=prior,
        posterior=posterior,
        log_odds_prior=log_odds_prior,
        log_odds_posterior=log_odds_posterior,
        evidence=list(evidence),
        n_effective=n_eff,
        family_shares=family_counts,
    )


# ── Convenience builders ─────────────────────────────────────────────────


def from_oracle_votes(
    votes: Sequence[dict[str, Any]],
    *,
    prior: float = 0.5,
    correlation_adjust: bool = True,
) -> BayesianResult:
    """Shim from oracle EnsemblePredictor vote dicts → BayesianResult.

    Each vote dict is expected to have:
        name (str) - the model head name
        confidence (float) - the head's confidence in its own direction
        direction (str) - 'bullish' / 'bearish' / 'neutral'
        family (str, optional) - signal family for correlation grouping

    Neutral votes are dropped. Bearish votes have their probability
    inverted (1 - confidence) so the result is the P(bullish).
    """
    evidence: list[EvidenceItem] = []
    for v in votes:
        direction = str(v.get("direction", "")).lower()
        if direction not in ("bullish", "bearish"):
            continue
        try:
            conf = float(v.get("confidence") or 0.5)
        except (TypeError, ValueError):
            conf = 0.5
        conf = max(0.0, min(1.0, conf))
        # For bearish votes, invert confidence: a bearish vote at 0.8
        # confidence means P(bullish) = 0.2
        prob_bullish = conf if direction == "bullish" else 1.0 - conf
        evidence.append(EvidenceItem(
            name=str(v.get("name") or v.get("model_name") or ""),
            probability=prob_bullish,
            weight=1.0,
            family=str(v.get("family") or ""),
        ))

    return combine_evidence(
        evidence, prior=prior, correlation_adjust=correlation_adjust,
    )
