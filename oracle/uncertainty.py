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

S10 (2026-11-06) adds coverage-backed intervals next to it (exploratory,
not used for sizing):

    split_conformal_interval(p_hat, sorted_scores, alpha=0.10)
    AdaptiveConformal(alpha=0.10, gamma=ACI_GAMMA).interval(...) / .update(...)
        → ConformalInterval with {lower, upper, abstain, stance, ...}

``abstain`` is True when the interval contains 0.5.

All functions are pure — no DB I/O, no engine state.
"""
from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

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


# ── S10 (2026-11-06) — split-conformal + adaptive conformal intervals ─────────
#
# ``compute_confidence_interval`` above measures ensemble *disagreement* only:
# six heads that all say 0.7 give a zero-width interval even if calls at 0.7
# historically land 45% of the time. The functions below give an interval with
# a finite-sample coverage statement instead, built from scored history:
#
# * ``p_hat`` is a recalibrated probability that the call's direction is right
#   (``oracle.calibration`` fits it on an earlier time block).
# * The nonconformity score of a scored row is ``|y - p_hat|`` with ``y`` the
#   0/1 directional outcome. For a binary label that is the same as the
#   least-ambiguous-set score ``1 - p_hat(y)``, so the interval
#   ``[p_hat - q, p_hat + q]`` and the label set ``{y : |y - p_hat| <= q}`` are
#   one object: the interval contains 1 iff the set contains "right", contains
#   0 iff it contains "wrong", and contains 0.5 iff it cannot exclude either
#   label better than a coin flip -- the abstain signal.
# * Split conformal (static ``q``) is exactly valid only under
#   exchangeability. Oracle outcomes are clustered by expiry date and drift
#   over time, so the exploratory job also runs adaptive conformal inference
#   (Gibbs & Candes 2021, "Adaptive Conformal Inference Under Distribution
#   Shift"): the working miscoverage level is nudged after every observed
#   outcome, ``alpha_t+1 = alpha_t + gamma * (alpha - err_t)``.
#
# The step size gamma is fixed here, before any evaluation: 0.005, the value
# Gibbs & Candes use for their main experiments. It is never tuned on the
# held-out block. Their Proposition 4.1 bound on long-run miscoverage,
# ``|mean(err) - alpha| <= (max(alpha_1, 1 - alpha_1) + gamma) / (gamma * T)``,
# is reported next to every evaluation so the reader can see how much T the
# guarantee needs.
#
# None of this is wired into live sizing: ``engine.py`` and the options
# recommender keep calling ``compute_confidence_interval``. The conformal
# interval is exploratory output (``oracle_calibration_exploratory``).

ACI_GAMMA: float = 0.005
CONFORMAL_ALPHA: float = 0.10
ABSTAIN_THRESHOLD: float = 0.5


def binary_residual_score(p_hat: float, y: float) -> float:
    """Nonconformity score ``|y - p_hat|`` for a 0/1 outcome."""
    return abs(float(y) - float(p_hat))


def conformal_quantile(sorted_scores: Sequence[float], alpha: float) -> float:
    """Finite-sample split-conformal quantile of ascending ``sorted_scores``.

    Returns the ``ceil((n + 1)(1 - alpha))``-th smallest score. When that rank
    exceeds ``n`` (too few scores, or ``alpha <= 0``) the quantile is
    ``inf`` -- the interval is the whole of ``[0, 1]``. ``alpha >= 1`` gives 0
    (the interval collapses to the point). Callers pass a pre-sorted list so
    an online loop can keep it sorted with ``bisect.insort``.
    """
    n = len(sorted_scores)
    if alpha >= 1.0:
        return 0.0
    if alpha <= 0.0 or n == 0:
        return math.inf
    k = math.ceil((n + 1) * (1.0 - alpha))
    if k > n:
        return math.inf
    if k < 1:
        return 0.0
    return float(sorted_scores[k - 1])


@dataclass(frozen=True)
class ConformalInterval:
    """Conformal interval for the probability that a call's direction is right."""

    point: float           # recalibrated p_hat
    lower: float           # clamped to [0, 1]
    upper: float           # clamped to [0, 1]
    alpha: float           # nominal miscoverage
    alpha_used: float      # level actually used (== alpha for split; alpha_t for ACI)
    q: float               # conformal score quantile (inf -> full interval)
    n_cal: int             # number of calibration scores behind q
    method: str            # 'split' or 'aci'
    official: bool = False  # always False: exploratory output only

    @property
    def abstain(self) -> bool:
        """True when the interval cannot rule out a coin flip (contains 0.5)."""
        return self.lower <= ABSTAIN_THRESHOLD <= self.upper

    @property
    def stance(self) -> str:
        """'abstain', 'take' (interval above 0.5) or 'fade' (interval below 0.5)."""
        if self.abstain:
            return "abstain"
        return "take" if self.lower > ABSTAIN_THRESHOLD else "fade"

    def covers(self, y: float) -> bool:
        return self.lower <= float(y) <= self.upper

    def to_dict(self) -> dict[str, Any]:
        return {
            "point": round(self.point, 4),
            "lower": round(self.lower, 4),
            "upper": round(self.upper, 4),
            "alpha": self.alpha,
            "alpha_used": round(self.alpha_used, 6),
            "q": None if math.isinf(self.q) else round(self.q, 6),
            "n_cal": self.n_cal,
            "method": self.method,
            "abstain": self.abstain,
            "stance": self.stance,
            "official": False,
            "level": f"{round((1.0 - self.alpha) * 100)}%",
        }


def conformal_interval_from_quantile(
    p_hat: float,
    q: float,
    *,
    alpha: float,
    alpha_used: float | None = None,
    n_cal: int,
    method: str = "split",
) -> ConformalInterval:
    """Build ``[p_hat - q, p_hat + q]`` clamped to ``[0, 1]``."""
    p = max(0.0, min(1.0, float(p_hat)))
    if math.isinf(q):
        lower, upper = 0.0, 1.0
    else:
        lower, upper = max(0.0, p - q), min(1.0, p + q)
    return ConformalInterval(
        point=p, lower=lower, upper=upper, alpha=alpha,
        alpha_used=alpha if alpha_used is None else alpha_used,
        q=q, n_cal=n_cal, method=method,
    )


def split_conformal_interval(
    p_hat: float,
    sorted_scores: Sequence[float],
    *,
    alpha: float = CONFORMAL_ALPHA,
) -> ConformalInterval:
    """Split-conformal interval from a fixed, ascending calibration score set."""
    q = conformal_quantile(sorted_scores, alpha)
    return conformal_interval_from_quantile(
        p_hat, q, alpha=alpha, n_cal=len(sorted_scores), method="split",
    )


class AdaptiveConformal:
    """Adaptive conformal inference state (Gibbs & Candes 2021).

    ``alpha_t`` starts at the nominal ``alpha`` and moves by
    ``gamma * (alpha - err)`` after each observed outcome: a miss
    (``err = 1``) lowers ``alpha_t`` and widens later intervals, a cover
    raises it. ``alpha_t`` is deliberately not clamped -- the paper's
    guarantee depends on it being free to leave ``[0, 1]`` (below 0 the
    interval is the whole range; above 1 it is the point).
    """

    def __init__(self, alpha: float = CONFORMAL_ALPHA, gamma: float = ACI_GAMMA) -> None:
        if not 0.0 < alpha < 1.0:
            raise ValueError("alpha must be in (0, 1)")
        if not gamma > 0.0:
            raise ValueError("gamma must be positive")
        self.alpha = float(alpha)
        self.gamma = float(gamma)
        self.alpha_t = float(alpha)
        self.n_updates = 0
        self.n_errors = 0

    def interval(self, p_hat: float, sorted_scores: Sequence[float]) -> ConformalInterval:
        q = conformal_quantile(sorted_scores, self.alpha_t)
        return conformal_interval_from_quantile(
            p_hat, q, alpha=self.alpha, alpha_used=self.alpha_t,
            n_cal=len(sorted_scores), method="aci",
        )

    def update(self, covered: bool) -> float:
        err = 0.0 if covered else 1.0
        self.alpha_t += self.gamma * (self.alpha - err)
        self.n_updates += 1
        self.n_errors += int(err)
        return self.alpha_t

    def miscoverage_bound(self, alpha_1: float | None = None) -> float | None:
        """Gibbs & Candes Prop. 4.1 bound on ``|mean(err) - alpha|`` after T updates."""
        if self.n_updates == 0:
            return None
        a1 = self.alpha if alpha_1 is None else alpha_1
        return (max(a1, 1.0 - a1) + self.gamma) / (self.gamma * self.n_updates)
