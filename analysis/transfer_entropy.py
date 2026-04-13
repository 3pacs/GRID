"""CAT-111 — Transfer entropy discovery engine.

Transfer entropy quantifies the reduction in uncertainty about series Y's
next value given the past of series X, beyond what Y's own past provides.
Unlike correlation or Granger causality, it's distribution-free and
captures non-linear lead-lag dependencies.

Formula (first-order, discretized):

    TE(X→Y) = Σ p(y_t+1, y_t, x_t) × log[
                  p(y_t+1 | y_t, x_t)
                  ──────────────────
                  p(y_t+1 | y_t)
              ]

Interpretation: TE(X→Y) is the number of bits of information about
Y_{t+1} that X_t provides ABOVE AND BEYOND Y_t. If TE(X→Y) >> TE(Y→X),
then X leads Y.

Why this matters (Tier A catalog #111): GRID's existing
``analysis/backtest_scanner.scan_all_pairs`` uses linear correlation,
which misses regime-switching relationships and non-linear lead-lag.
Transfer entropy catches them — at the cost of needing a discretizer
and enough data to estimate the 3-dimensional joint probability.

We use quantile-based discretization (default 4 bins per series) and
first-order history (lag=1). Higher orders are queued as a follow-up
(they need much more data).

All functions are pure — no DB I/O.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Sequence

import numpy as np
from loguru import logger as log


# ── Defaults ─────────────────────────────────────────────────────────────

# Number of discrete bins for quantile discretization. 4 = quartiles.
_DEFAULT_BINS = 4

# Minimum observations required for a reliable TE estimate
_MIN_OBSERVATIONS = 30

# Number of lags to scan in lead-lag analysis
_MAX_LAG = 10


# ── Data classes ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TransferEntropyResult:
    """One directional TE(source → target) measurement."""

    source_name: str
    target_name: str
    lag: int
    te_bits: float               # TE in bits (log base 2)
    symmetric_te: float           # TE(target → source) for direction check
    n_observations: int
    bins: int

    @property
    def is_directional(self) -> bool:
        """True when source leads target (TE(X→Y) > TE(Y→X) by margin)."""
        return self.te_bits > self.symmetric_te * 1.5

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "target_name": self.target_name,
            "lag": self.lag,
            "te_bits": round(self.te_bits, 6),
            "symmetric_te": round(self.symmetric_te, 6),
            "n_observations": self.n_observations,
            "bins": self.bins,
            "is_directional": self.is_directional,
        }


@dataclass(frozen=True)
class LeadLagScan:
    """Lead-lag scan result across multiple lags."""

    source_name: str
    target_name: str
    results: list[TransferEntropyResult]
    best_lag: int | None
    best_te: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "target_name": self.target_name,
            "best_lag": self.best_lag,
            "best_te": round(self.best_te, 6),
            "results": [r.to_dict() for r in self.results],
        }


# ── Discretization ───────────────────────────────────────────────────────


def quantile_discretize(
    series: Sequence[float],
    bins: int = _DEFAULT_BINS,
) -> np.ndarray:
    """Convert a continuous series to integer bin labels [0, bins-1].

    Uses empirical quantiles so each bin has roughly equal population —
    this makes the TE estimate more robust to outliers than fixed-width
    bins. Ties are broken by numpy's default.
    """
    arr = np.asarray(series, dtype=float)
    arr = arr[np.isfinite(arr)]
    if len(arr) == 0:
        return np.array([], dtype=int)
    if bins <= 1:
        return np.zeros(len(arr), dtype=int)
    # np.quantile gives bin EDGES; we want the thresholds INSIDE
    edges = np.quantile(arr, np.linspace(0, 1, bins + 1))
    # Use np.searchsorted to assign each value to a bin
    labels = np.searchsorted(edges[1:-1], arr, side="right")
    return labels.astype(int)


# ── Transfer entropy core math ───────────────────────────────────────────


def transfer_entropy(
    source: Sequence[float],
    target: Sequence[float],
    *,
    lag: int = 1,
    bins: int = _DEFAULT_BINS,
) -> float:
    """Estimate TE(source → target) at a given lag, in bits.

    Returns 0.0 when the input is too short or degenerate. The estimate
    is a plug-in maximum likelihood over discretized states — biased
    toward overestimation at low sample sizes, which we mitigate via
    the _MIN_OBSERVATIONS gate.
    """
    src = quantile_discretize(source, bins)
    tgt = quantile_discretize(target, bins)

    # Trim to common length after discretization
    n = min(len(src), len(tgt))
    if n < _MIN_OBSERVATIONS + lag:
        return 0.0

    src = src[:n]
    tgt = tgt[:n]

    # Build (y_{t+1}, y_t, x_t) triples
    y_next = tgt[lag:]
    y_past = tgt[:n - lag]
    x_past = src[:n - lag]

    n_samples = len(y_next)
    if n_samples == 0:
        return 0.0

    # Joint counts
    joint_counts: dict[tuple[int, int, int], int] = {}
    yy_counts: dict[tuple[int, int], int] = {}   # (y_next, y_past)
    yx_counts: dict[tuple[int, int], int] = {}   # (y_past, x_past)
    y_past_counts: dict[int, int] = {}

    for i in range(n_samples):
        yn = int(y_next[i])
        yp = int(y_past[i])
        xp = int(x_past[i])
        joint_counts[(yn, yp, xp)] = joint_counts.get((yn, yp, xp), 0) + 1
        yy_counts[(yn, yp)] = yy_counts.get((yn, yp), 0) + 1
        yx_counts[(yp, xp)] = yx_counts.get((yp, xp), 0) + 1
        y_past_counts[yp] = y_past_counts.get(yp, 0) + 1

    # TE = Σ p(yn, yp, xp) × log[ p(yn | yp, xp) / p(yn | yp) ]
    te = 0.0
    for (yn, yp, xp), count in joint_counts.items():
        p_joint = count / n_samples
        p_yn_given_yp_xp = count / yx_counts[(yp, xp)]
        p_yn_given_yp = yy_counts[(yn, yp)] / y_past_counts[yp]
        if p_yn_given_yp_xp > 0 and p_yn_given_yp > 0:
            te += p_joint * math.log2(p_yn_given_yp_xp / p_yn_given_yp)

    return max(0.0, te)  # TE is always non-negative in principle


def pair_transfer_entropy(
    source_name: str,
    source: Sequence[float],
    target_name: str,
    target: Sequence[float],
    *,
    lag: int = 1,
    bins: int = _DEFAULT_BINS,
) -> TransferEntropyResult:
    """Compute both TE(source→target) and TE(target→source) at a given lag."""
    te_forward = transfer_entropy(source, target, lag=lag, bins=bins)
    te_reverse = transfer_entropy(target, source, lag=lag, bins=bins)
    n_obs = min(len(source), len(target))
    return TransferEntropyResult(
        source_name=source_name,
        target_name=target_name,
        lag=lag,
        te_bits=te_forward,
        symmetric_te=te_reverse,
        n_observations=n_obs,
        bins=bins,
    )


def scan_lead_lag(
    source_name: str,
    source: Sequence[float],
    target_name: str,
    target: Sequence[float],
    *,
    max_lag: int = _MAX_LAG,
    bins: int = _DEFAULT_BINS,
) -> LeadLagScan:
    """Scan multiple lags to find the best source→target TE.

    Returns a :class:`LeadLagScan` with results at each lag and the
    best (highest TE) lag identified. Reports ``best_lag=None`` when
    no lag produced a non-zero TE.
    """
    results: list[TransferEntropyResult] = []
    best_lag: int | None = None
    best_te = 0.0

    for lag in range(1, max_lag + 1):
        r = pair_transfer_entropy(
            source_name, source, target_name, target,
            lag=lag, bins=bins,
        )
        results.append(r)
        if r.te_bits > best_te:
            best_te = r.te_bits
            best_lag = lag

    return LeadLagScan(
        source_name=source_name,
        target_name=target_name,
        results=results,
        best_lag=best_lag,
        best_te=best_te,
    )


def discover_leaders(
    series_map: dict[str, Sequence[float]],
    *,
    max_lag: int = 5,
    bins: int = _DEFAULT_BINS,
    min_directional_bits: float = 0.05,
) -> list[LeadLagScan]:
    """For every ordered pair (A, B) in series_map, compute TE(A→B) and
    return only the directional pairs (A leads B with TE ≥ min threshold).

    This is the discovery entry point — feed it a dict of {name: series}
    and it returns the list of "A predicts B" findings ranked by TE.
    Symmetric pairs (A↔B with similar TE both ways) are filtered out.
    """
    findings: list[LeadLagScan] = []
    names = list(series_map.keys())
    for i, src_name in enumerate(names):
        for j, tgt_name in enumerate(names):
            if i == j:
                continue
            scan = scan_lead_lag(
                src_name, series_map[src_name],
                tgt_name, series_map[tgt_name],
                max_lag=max_lag, bins=bins,
            )
            if scan.best_lag is None or scan.best_te < min_directional_bits:
                continue
            # Require asymmetry — reject if reverse TE is too close
            best_result = scan.results[scan.best_lag - 1]
            if best_result.te_bits <= best_result.symmetric_te * 1.5:
                continue
            findings.append(scan)

    findings.sort(key=lambda s: -s.best_te)
    return findings
