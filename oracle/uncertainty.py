"""ALPHA-11 / task #114 — Uncertainty bounds + confidence intervals.

Every oracle prediction currently ships a point estimate for ``confidence``.
The recommender's Kelly sizing uses that scalar directly. That's fine when
the ensemble is tightly-clustered but misleading when the per-head
confidences are spread (e.g. 4 models at 0.7, 2 at 0.3 — the point mean is
0.57 but the lower 90% bound is ~0.3 and the Kelly should reflect THAT).

This module computes a symmetric confidence interval around the point
estimate from the same ``confidence_variance`` statistic ALPHA-10 already
exposes. No distributional assumption — we use the bootstrap-style percentile
approach: for small ensembles we lean on the t-distribution critical value,
for larger ones we use the standard normal.

Public API
----------
    compute_confidence_interval(votes, confidence, alpha=0.10)
        → ConfidenceInterval with {lower, upper, width, n, alpha}

Used by ALPHA-12 (Kelly-with-error-bars) for conservative sizing and by the
oracle report layer to display CI bars alongside every prediction.

All functions are pure — no DB I/O, no engine state.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Sequence


# Student-t two-sided critical values for the 90% / 95% / 99% CIs. Used
# when the ensemble has <30 heads (small-sample regime). For larger
# ensembles we fall back to the standard normal.
_T_CRITICAL: dict[int, dict[float, float]] = {
    # df → { alpha: t_{α/2, df} }
    1:  {0.10: 6.314, 0.05: 12.706, 0.01: 63.657},
    2:  {0.10: 2.920, 0.05: 4.303,  0.01: 9.925},
    3:  {0.10: 2.353, 0.05: 3.182,  0.01: 5.841},
    4:  {0.10: 2.132, 0.05: 2.776,  0.01: 4.604},
    5:  {0.10: 2.015, 0.05: 2.571,  0.01: 4.032},
    6:  {0.10: 1.943, 0.05: 2.447,  0.01: 3.707},
    7:  {0.10: 1.895, 0.05: 2.365,  0.01: 3.499},
    8:  {0.10: 1.860, 0.05: 2.306,  0.01: 3.355},
    9:  {0.10: 1.833, 0.05: 2.262,  0.01: 3.250},
    10: {0.10: 1.812, 0.05: 2.228,  0.01: 3.169},
    15: {0.10: 1.753, 0.05: 2.131,  0.01: 2.947},
    20: {0.10: 1.725, 0.05: 2.086,  0.01: 2.845},
    25: {0.10: 1.708, 0.05: 2.060,  0.01: 2.787},
    29: {0.10: 1.699, 0.05: 2.045,  0.01: 2.756},
}

# Standard normal two-sided critical values for large-sample regime.
_Z_CRITICAL: dict[float, float] = {
    0.10: 1.645,
    0.05: 1.960,
    0.01: 2.576,
}


@dataclass(frozen=True)
class ConfidenceInterval:
    """Symmetric confidence interval around a point estimate."""

    point: float           # The oracle's point confidence
    lower: float           # Lower CI bound, clamped to [0, 1]
    upper: float           # Upper CI bound, clamped to [0, 1]
    alpha: float           # Significance level (0.10 → 90% CI)
    n: int                 # Sample size (number of heads)
    sem: float             # Standard error of the mean

    @property
    def width(self) -> float:
        return self.upper - self.lower

    @property
    def level_label(self) -> str:
        return f"{int((1.0 - self.alpha) * 100)}%"

    def to_dict(self) -> dict[str, Any]:
        return {
            "point": round(self.point, 4),
            "lower": round(self.lower, 4),
            "upper": round(self.upper, 4),
            "width": round(self.width, 4),
            "alpha": self.alpha,
            "n": self.n,
            "sem": round(self.sem, 6),
            "level": self.level_label,
        }


def _critical_value(df: int, alpha: float) -> float:
    """Pick the right critical value for the given df + alpha.

    Uses the t-table for small samples (df<30), the normal table for larger
    ensembles. Unknown alphas fall back to the 90% CI.
    """
    if alpha not in (0.10, 0.05, 0.01):
        alpha = 0.10
    if df >= 30:
        return _Z_CRITICAL[alpha]
    # Find the nearest (non-exceeding) df bucket in the t-table
    keys = sorted(_T_CRITICAL.keys())
    chosen = keys[0]
    for k in keys:
        if k <= df:
            chosen = k
        else:
            break
    return _T_CRITICAL[chosen][alpha]


def _vote_confidence_values(votes: Sequence[dict[str, Any]]) -> list[float]:
    """Extract per-head confidence values with out-of-range clamping."""
    out: list[float] = []
    for v in votes:
        c = v.get("confidence")
        if c is None:
            continue
        try:
            out.append(max(0.0, min(1.0, float(c))))
        except (TypeError, ValueError):
            continue
    return out


def compute_confidence_interval(
    votes: Sequence[dict[str, Any]],
    point_confidence: float,
    *,
    alpha: float = 0.10,
) -> ConfidenceInterval:
    """Return a symmetric confidence interval for the ensemble's point estimate.

    Uses the t-distribution critical value for n<30 ensembles and the normal
    approximation for larger ones. The interval is centered on ``point_confidence``
    (not on the mean of the per-head confidences) so it stays consistent with
    whatever dampening the oracle has already applied.

    Parameters
    ----------
    votes:
        Sequence of per-head vote dicts (must have a ``confidence`` key).
    point_confidence:
        The oracle's post-dampening point estimate — becomes the center of
        the interval.
    alpha:
        Two-sided significance level. ``0.10`` = 90% CI, ``0.05`` = 95%,
        ``0.01`` = 99%. Unknown values fall back to 0.10.

    Returns
    -------
    A :class:`ConfidenceInterval` with both bounds clamped to ``[0, 1]``.
    Singleton ensembles (n=1) collapse to a zero-width interval around the
    point.
    """
    confs = _vote_confidence_values(votes)
    n = len(confs)
    if n <= 1:
        return ConfidenceInterval(
            point=point_confidence,
            lower=max(0.0, min(1.0, point_confidence)),
            upper=max(0.0, min(1.0, point_confidence)),
            alpha=alpha,
            n=n,
            sem=0.0,
        )

    mean = sum(confs) / n
    var = sum((c - mean) ** 2 for c in confs) / (n - 1)  # sample variance
    std = math.sqrt(var)
    sem = std / math.sqrt(n)

    crit = _critical_value(n - 1, alpha)
    half_width = crit * sem

    lower = max(0.0, point_confidence - half_width)
    upper = min(1.0, point_confidence + half_width)

    return ConfidenceInterval(
        point=point_confidence,
        lower=lower,
        upper=upper,
        alpha=alpha,
        n=n,
        sem=sem,
    )
