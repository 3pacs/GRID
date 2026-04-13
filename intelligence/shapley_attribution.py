"""ALPHA-9 / task #112 — Shapley-value attribution per prediction.

For every oracle prediction, compute how much each model head (or each
signal family) contributed to the final score. The Shapley value is the
unique fair attribution: each contributor's value is its marginal
contribution averaged over every possible ordering of the other
contributors.

Why this matters
----------------
The recommender currently treats every prediction as monolithic — confidence
× direction × Kelly. But two predictions with the same confidence can be
very different in fragility:

  Prediction A: 6 model heads each contribute ~17% of the score.
  Prediction B: 1 model head contributes 80%, the other 5 contribute 4% each.

Prediction B is one-leg-fragile — if that one head is wrong, the whole
prediction collapses. The fragility multiplier is a confidence dampening
that the recommender uses to shrink Kelly on B vs A.

Computational cost
------------------
Exact Shapley = exponential in n (we'd need 2^n permutations). With 6-10
model heads that's 64-1024 — trivially cheap. We use the exact formula
for n ≤ 12 and the leave-one-out approximation (n+1 evaluations) for
larger ensembles. The downstream consumer never needs to know which
path was used.

All functions are pure — no DB I/O.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import combinations
from typing import Any, Callable, Sequence


# Switch from exact to leave-one-out approximation above this many contributors
_EXACT_MAX_N = 12

# Fragility threshold — Herfindahl index above this triggers a dampening
# 1.0 = single contributor owns everything; 1/n = perfectly distributed
_FRAGILE_HERFINDAHL = 0.40


@dataclass(frozen=True)
class ShapleyAttribution:
    """Per-contributor attribution + fragility metrics for one prediction."""

    contributions: dict[str, float]   # contributor → marginal value
    total: float                       # sum of contributions (≈ ensemble score)
    herfindahl: float                  # concentration index (1/n .. 1)
    top_contributor: str               # name of the largest contributor
    top_share: float                   # 0..1 — how much the top owns
    fragility_multiplier: float        # ≤1.0 — confidence shrink factor
    n: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "contributions": {
                k: round(v, 4) for k, v in self.contributions.items()
            },
            "total": round(self.total, 4),
            "herfindahl": round(self.herfindahl, 4),
            "top_contributor": self.top_contributor,
            "top_share": round(self.top_share, 4),
            "fragility_multiplier": round(self.fragility_multiplier, 4),
            "n": self.n,
        }


# ── Exact Shapley over a value function ────────────────────────────────────


def shapley_exact(
    contributors: Sequence[str],
    value_fn: Callable[[Sequence[str]], float],
) -> dict[str, float]:
    """Exact Shapley values for a small set of contributors.

    ``value_fn`` is a coalition value function: ``value_fn(subset) → score``.
    For an ensemble where each model head contributes a vote_weight, the
    natural value function is ``sum(vote_weight for c in subset)`` — but
    callers can pass anything (e.g. a non-linear ensemble combiner).

    Returns a dict mapping each contributor to its Shapley value. Sum of
    Shapley values equals ``value_fn(full_set) - value_fn(empty)``.
    """
    n = len(contributors)
    if n == 0:
        return {}

    # Cache coalition values to avoid recomputation
    cache: dict[frozenset[str], float] = {}

    def v(subset: frozenset[str]) -> float:
        if subset not in cache:
            cache[subset] = value_fn(tuple(sorted(subset)))
        return cache[subset]

    out: dict[str, float] = {c: 0.0 for c in contributors}
    contributors_list = list(contributors)

    # Sum over all subsets S not containing i:
    #   shapley[i] = Σ |S|! * (n - |S| - 1)! / n!   *   (v(S ∪ {i}) - v(S))
    factorial = [math.factorial(k) for k in range(n + 1)]
    n_fact = factorial[n]

    for i, contributor in enumerate(contributors_list):
        others = contributors_list[:i] + contributors_list[i + 1:]
        for k in range(n):
            for subset in combinations(others, k):
                s_set = frozenset(subset)
                s_with = frozenset((*subset, contributor))
                marginal = v(s_with) - v(s_set)
                weight = factorial[k] * factorial[n - k - 1] / n_fact
                out[contributor] += weight * marginal

    return out


def shapley_leave_one_out(
    contributors: Sequence[str],
    value_fn: Callable[[Sequence[str]], float],
) -> dict[str, float]:
    """Leave-one-out approximation for large ensembles (n > _EXACT_MAX_N).

    Each contributor's value is ``v(full) - v(full - {i})`` rescaled so the
    sum equals the full coalition value. Cheap (n+1 evaluations) and a
    reasonable approximation when contributions are roughly additive.
    """
    full = list(contributors)
    if not full:
        return {}
    full_value = value_fn(full)
    if full_value == 0:
        return {c: 0.0 for c in full}

    raw: dict[str, float] = {}
    for c in full:
        without = [x for x in full if x != c]
        raw[c] = full_value - value_fn(without)

    # Rescale so sum equals full_value
    total = sum(raw.values()) or 1.0
    scale = full_value / total
    return {c: v * scale for c, v in raw.items()}


def attribute_votes(
    votes: Sequence[dict[str, Any]],
    *,
    weight_field: str = "vote_weight",
    name_field: str = "model_name",
) -> ShapleyAttribution:
    """Compute Shapley attribution over an ensemble vote list.

    Each vote dict must have a ``weight_field`` (default ``vote_weight``)
    and ``name_field`` (default ``model_name``). The value function is
    additive: ``v(S) = sum(weight for v in S)``.

    Returns a :class:`ShapleyAttribution` with per-head contributions, the
    Herfindahl concentration index, and a fragility multiplier suitable
    for confidence dampening.
    """
    if not votes:
        return ShapleyAttribution(
            contributions={}, total=0.0, herfindahl=0.0,
            top_contributor="", top_share=0.0,
            fragility_multiplier=1.0, n=0,
        )

    # Build name → weight map (deduplicate by name; if dupes, sum)
    weights: dict[str, float] = {}
    for v in votes:
        name = str(v.get(name_field, f"vote_{id(v)}"))
        try:
            w = float(v.get(weight_field) or 0.0)
        except (TypeError, ValueError):
            w = 0.0
        weights[name] = weights.get(name, 0.0) + max(0.0, w)

    contributors = list(weights.keys())
    n = len(contributors)

    # Additive value function — sum of selected weights
    def value_fn(subset: Sequence[str]) -> float:
        return sum(weights[c] for c in subset)

    if n <= _EXACT_MAX_N:
        contributions = shapley_exact(contributors, value_fn)
    else:
        contributions = shapley_leave_one_out(contributors, value_fn)

    total = sum(contributions.values())
    if total <= 0:
        return ShapleyAttribution(
            contributions=contributions, total=0.0, herfindahl=0.0,
            top_contributor="", top_share=0.0,
            fragility_multiplier=1.0, n=n,
        )

    # Normalize to shares
    shares = {c: v / total for c, v in contributions.items()}

    # Herfindahl index — sum of squared shares
    herfindahl = sum(s * s for s in shares.values())

    # Top contributor
    top = max(shares, key=shares.get)
    top_share = shares[top]

    # Fragility multiplier — quadratic shrink as Herfindahl rises above
    # the threshold. At threshold → 1.0 (no dampening). At max (1.0) → 0.5.
    if herfindahl <= _FRAGILE_HERFINDAHL:
        fragility = 1.0
    else:
        excess = (herfindahl - _FRAGILE_HERFINDAHL) / (1.0 - _FRAGILE_HERFINDAHL)
        fragility = max(0.5, 1.0 - 0.5 * excess)

    return ShapleyAttribution(
        contributions=contributions,
        total=total,
        herfindahl=herfindahl,
        top_contributor=top,
        top_share=top_share,
        fragility_multiplier=fragility,
        n=n,
    )
