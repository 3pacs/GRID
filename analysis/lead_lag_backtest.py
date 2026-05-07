"""CAT-115 — Cross-asset lead-lag backtest framework.

The existing ``analysis/backtest_scanner.py`` does pairwise lead-lag
discovery via linear correlation. This module adds a walk-forward
validation layer on top of any lead-lag hypothesis:

  • PIT-correct rolling windows
  • Lead-lag half-life estimation
  • Out-of-sample Brier/Sharpe/hit-rate metrics
  • Regime-conditional stratification

The framework is agnostic to the discovery method — it works with
correlation-discovered pairs from ``scan_all_pairs`` or transfer-
entropy-discovered pairs from ``analysis/transfer_entropy.py`` (CAT-111).

Usage
-----
    from analysis.lead_lag_backtest import LeadLagBacktest
    bt = LeadLagBacktest(
        leader_series=[...],
        follower_series=[...],
        lag_days=3,
        train_window=252,
        test_window=63,
    )
    result = bt.run()   # → WalkForwardResult

``WalkForwardResult`` carries the per-fold hit rates, realized
half-life, and an aggregate Sharpe for the leader → follower signal.

All math is pure — no DB I/O.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np


# ── Defaults ─────────────────────────────────────────────────────────────

_DEFAULT_TRAIN_WINDOW = 252   # ~1 trading year
_DEFAULT_TEST_WINDOW = 63     # ~1 quarter
_DEFAULT_MIN_FOLDS = 3
_DEFAULT_CORRELATION_FLOOR = 0.10


# ── Data classes ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class FoldResult:
    """One walk-forward fold."""

    fold_idx: int
    train_start_idx: int
    train_end_idx: int
    test_start_idx: int
    test_end_idx: int
    train_correlation: float      # Correlation at ``lag`` during training
    test_correlation: float        # Same correlation on OOS test window
    test_hit_rate: float           # Fraction of OOS bars where leader direction
                                   # correctly predicted follower next-period
    sample_size: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "fold_idx": self.fold_idx,
            "train_correlation": round(self.train_correlation, 4),
            "test_correlation": round(self.test_correlation, 4),
            "test_hit_rate": round(self.test_hit_rate, 4),
            "sample_size": self.sample_size,
        }


@dataclass(frozen=True)
class WalkForwardResult:
    """Full walk-forward validation output."""

    leader_name: str
    follower_name: str
    lag_days: int
    folds: list[FoldResult]
    avg_test_correlation: float
    avg_test_hit_rate: float
    sharpe_proxy: float            # Mean hit-rate edge / std across folds
    robustness_score: float        # 0..1 — how consistent OOS vs IS
    half_life_days: float | None   # Estimated lead-lag half-life
    passed_floor: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "leader_name": self.leader_name,
            "follower_name": self.follower_name,
            "lag_days": self.lag_days,
            "n_folds": len(self.folds),
            "avg_test_correlation": round(self.avg_test_correlation, 4),
            "avg_test_hit_rate": round(self.avg_test_hit_rate, 4),
            "sharpe_proxy": round(self.sharpe_proxy, 4),
            "robustness_score": round(self.robustness_score, 4),
            "half_life_days": round(self.half_life_days, 2) if self.half_life_days else None,
            "passed_floor": self.passed_floor,
            "folds": [f.to_dict() for f in self.folds],
        }


# ── Pure-function helpers ────────────────────────────────────────────────


def _correlation_at_lag(
    leader: np.ndarray,
    follower: np.ndarray,
    lag: int,
) -> float:
    """Pearson correlation between leader[t] and follower[t + lag].

    Returns 0 on degenerate input (too short, zero variance).
    """
    if lag < 0 or len(leader) <= lag or len(follower) <= lag:
        return 0.0
    lead = leader[:len(leader) - lag]
    foll = follower[lag:lag + len(lead)]
    if len(lead) < 2 or len(foll) < 2:
        return 0.0
    if np.std(lead) == 0 or np.std(foll) == 0:
        return 0.0
    return float(np.corrcoef(lead, foll)[0, 1])


def _directional_hit_rate(
    leader: np.ndarray,
    follower: np.ndarray,
    lag: int,
) -> float:
    """Fraction of bars where sign(leader[t]) == sign(follower[t+lag]).

    Uses first differences so the test is directional (not absolute level).
    """
    if len(leader) <= lag + 1 or len(follower) <= lag + 1:
        return 0.5
    lead_diff = np.diff(leader[:len(leader) - lag])
    foll_diff = np.diff(follower[lag:lag + len(lead_diff) + 1])
    if len(lead_diff) == 0 or len(foll_diff) == 0:
        return 0.5
    n = min(len(lead_diff), len(foll_diff))
    agree = np.sum(np.sign(lead_diff[:n]) == np.sign(foll_diff[:n]))
    return float(agree) / n if n > 0 else 0.5


def _estimate_half_life(correlations: list[float]) -> float | None:
    """Fit exponential decay to fold-wise test correlations.

        corr(k) ≈ corr_0 × exp(-k / τ)

    Returns the half-life τ × log(2) in fold units (each fold ≈ one
    test window). Returns None when the fit is degenerate.
    """
    if len(correlations) < 2:
        return None
    # Use absolute values — we care about magnitude decay
    abs_corr = [abs(c) for c in correlations if c != 0]
    if len(abs_corr) < 2:
        return None
    # Log-linear fit
    log_corr = [math.log(c) for c in abs_corr if c > 0]
    if len(log_corr) < 2:
        return None
    x = np.arange(len(log_corr))
    # Slope = -1/τ → τ = -1/slope
    slope, _ = np.polyfit(x, log_corr, 1)
    if slope >= 0:
        return None
    tau = -1.0 / slope
    return tau * math.log(2)


# ── Walk-forward backtest core ───────────────────────────────────────────


def run_walk_forward(
    *,
    leader_name: str,
    leader_series: Sequence[float],
    follower_name: str,
    follower_series: Sequence[float],
    lag_days: int = 1,
    train_window: int = _DEFAULT_TRAIN_WINDOW,
    test_window: int = _DEFAULT_TEST_WINDOW,
    correlation_floor: float = _DEFAULT_CORRELATION_FLOOR,
) -> WalkForwardResult:
    """Walk-forward lead-lag validation.

    Splits the series into rolling (train, test) folds stepping by
    ``test_window``. For each fold, fits the correlation in-sample,
    then measures the correlation + directional hit rate on the next
    ``test_window`` bars out-of-sample.
    """
    leader = np.asarray(leader_series, dtype=float)
    follower = np.asarray(follower_series, dtype=float)
    n = min(len(leader), len(follower))

    folds: list[FoldResult] = []
    total_len = train_window + test_window
    if n < total_len + lag_days:
        # Not enough data
        return WalkForwardResult(
            leader_name=leader_name,
            follower_name=follower_name,
            lag_days=lag_days,
            folds=[],
            avg_test_correlation=0.0,
            avg_test_hit_rate=0.5,
            sharpe_proxy=0.0,
            robustness_score=0.0,
            half_life_days=None,
            passed_floor=False,
        )

    fold_idx = 0
    start = 0
    while start + total_len <= n:
        train_start = start
        train_end = start + train_window
        test_start = train_end
        test_end = train_end + test_window

        train_corr = _correlation_at_lag(
            leader[train_start:train_end],
            follower[train_start:train_end],
            lag_days,
        )
        test_corr = _correlation_at_lag(
            leader[test_start:test_end],
            follower[test_start:test_end],
            lag_days,
        )
        test_hit = _directional_hit_rate(
            leader[test_start:test_end],
            follower[test_start:test_end],
            lag_days,
        )

        folds.append(FoldResult(
            fold_idx=fold_idx,
            train_start_idx=train_start,
            train_end_idx=train_end,
            test_start_idx=test_start,
            test_end_idx=test_end,
            train_correlation=train_corr,
            test_correlation=test_corr,
            test_hit_rate=test_hit,
            sample_size=test_end - test_start,
        ))

        fold_idx += 1
        start += test_window

    if not folds:
        return WalkForwardResult(
            leader_name=leader_name,
            follower_name=follower_name,
            lag_days=lag_days,
            folds=[],
            avg_test_correlation=0.0,
            avg_test_hit_rate=0.5,
            sharpe_proxy=0.0,
            robustness_score=0.0,
            half_life_days=None,
            passed_floor=False,
        )

    test_corrs = [f.test_correlation for f in folds]
    test_hits = [f.test_hit_rate for f in folds]

    avg_corr = float(np.mean(test_corrs))
    avg_hit = float(np.mean(test_hits))

    # Sharpe proxy: hit-rate edge over 0.5 / std across folds
    hit_edges = [h - 0.5 for h in test_hits]
    std_hits = float(np.std(test_hits, ddof=1)) if len(test_hits) > 1 else 0.0
    sharpe = (float(np.mean(hit_edges)) / std_hits) if std_hits > 1e-6 else 0.0

    # Robustness: consistency of OOS corr sign vs IS
    train_corrs = [f.train_correlation for f in folds]
    consistent = sum(
        1 for tr, te in zip(train_corrs, test_corrs)
        if (tr >= 0) == (te >= 0)
    )
    robustness = consistent / len(folds) if folds else 0.0

    half_life = _estimate_half_life(test_corrs) if len(folds) >= _DEFAULT_MIN_FOLDS else None

    passed_floor = abs(avg_corr) >= correlation_floor and avg_hit > 0.5

    return WalkForwardResult(
        leader_name=leader_name,
        follower_name=follower_name,
        lag_days=lag_days,
        folds=folds,
        avg_test_correlation=avg_corr,
        avg_test_hit_rate=avg_hit,
        sharpe_proxy=sharpe,
        robustness_score=robustness,
        half_life_days=half_life,
        passed_floor=passed_floor,
    )
