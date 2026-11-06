"""Overfitting checks over a full trial matrix: CPCV, PBO (CSCV) and the deflated Sharpe.

W2-3 / S11 (2026-11-06). A research candidate is one column of a *trial
matrix*: per-period returns (rows: a common, tz-aware date index; columns:
every trial the research loop ever measured). Selection bias lives in that
matrix, not in the candidate's own series, so every check here takes the
whole matrix. This module knows nothing about ledgers; the S11 gate
(:func:`analysis.ledger_steered_exploration.record_overfit_gate`) decides
which columns the matrix must hold and records every result.

Checks
------
* **CPCV**, combinatorial purged cross-validation (López de Prado 2018,
  ch. 12): the rows are cut into ``N`` contiguous groups; every choice of
  ``k`` test groups is one split (``C(N, k)``; 45 for N=10, k=2) and the test
  groups recombine into ``k / N * C(N, k)`` = ``C(N-1, k-1)`` full backtest
  paths (9). Training rows whose label span ``[i, label_end[i]]`` overlaps a
  test span are *purged*; training rows within ``embargo`` rows after a test
  run's last label end are *embargoed*. :func:`cpcv_selection` runs the
  research loop's own fitting step (pick the trial with the best training
  Sharpe) on every split and reports the out-of-sample Sharpe of each path.
* **PBO**, the probability of backtest overfitting by combinatorially
  symmetric cross-validation (Bailey, Borwein, López de Prado & Zhu 2017): the
  rows are cut into ``S`` (even) blocks; every half of them is an in-sample
  set and its complement the out-of-sample set (``C(S, S/2)``; 12,870 for
  S=16). PBO is the share of splits whose in-sample best trial ranks at or
  below the out-of-sample median (logit ``<= 0``). With overlapping labels the
  last ``gap`` rows of every block are dropped, so no label spans two blocks
  (the split stays symmetric).
* **DSR**, #979's corrected deflated Sharpe ratio
  (:func:`alpha_research.validation.gauntlet.deflated_sharpe_ratio`): per-period
  Sharpe, ``n_trials`` from the caller's ledger (every declared trial, not
  just the matrix columns) and the *cross-trial* variance of the matrix
  columns' Sharpe ratios. With overlapping labels the observation count is
  deflated to ``T // (overlap_depth + 1)``.

The embargo reuses #979's dependence sizing
(:func:`analysis.offline_research_proof.autocorrelation_block` on the
matrix's cross-trial mean return): ``max(ceil(embargo_fraction * T),
block - 1)``.

Pre-registered thresholds (:class:`OverfitGateConfig`): PBO < 0.5 and every
candidate trial's DSR > 0.95. Thresholds may be made stricter, never looser,
and the structural settings (groups, partitions, embargo fraction) are fixed.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from itertools import combinations, pairwise
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from alpha_research.validation.gauntlet import (
    DSR_CONFIDENCE,
    DeflatedSharpe,
    deflated_sharpe_ratio,
    trial_sharpe_variance,
)
from analysis.offline_research_proof import autocorrelation_block

GATE_VERSION = "s11-overfit-gate-v1"
PBO_MAX = 0.5  # pass iff PBO < PBO_MAX
DSR_MIN = DSR_CONFIDENCE  # pass iff DSR > DSR_MIN (0.95)
CPCV_GROUPS = 10
CPCV_TEST_GROUPS = 2
CSCV_PARTITIONS = 16
EMBARGO_FRACTION = 0.01
# Rows each CSCV block must keep after its gap (a Sharpe needs a few points).
MIN_ROWS_PER_PARTITION = 4
CSCV_CHUNK = 2048


class OverfitInputError(ValueError):
    """The trial matrix (or a setting) cannot support the checks."""


@dataclass(frozen=True)
class OverfitGateConfig:
    """Pre-registered gate settings (S11). Thresholds may only get stricter."""

    pbo_max: float = PBO_MAX
    dsr_min: float = DSR_MIN
    cscv_partitions: int = CSCV_PARTITIONS
    cpcv_groups: int = CPCV_GROUPS
    cpcv_test_groups: int = CPCV_TEST_GROUPS
    embargo_fraction: float = EMBARGO_FRACTION

    def validate(self) -> None:
        if not (math.isfinite(self.pbo_max) and 0 < self.pbo_max <= PBO_MAX):
            raise OverfitInputError(f"pbo_max must be in (0, {PBO_MAX}] (stricter only)")
        if not (math.isfinite(self.dsr_min) and DSR_MIN <= self.dsr_min < 1):
            raise OverfitInputError(f"dsr_min must be in [{DSR_MIN}, 1) (stricter only)")
        fixed = (
            self.cscv_partitions,
            self.cpcv_groups,
            self.cpcv_test_groups,
            self.embargo_fraction,
        )
        if fixed != (CSCV_PARTITIONS, CPCV_GROUPS, CPCV_TEST_GROUPS, EMBARGO_FRACTION):
            raise OverfitInputError(
                "partitions, groups and embargo fraction are pre-registered and fixed"
            )

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# --- shared helpers -----------------------------------------------------------------


def group_bounds(n_obs: int, n_groups: int) -> list[tuple[int, int]]:
    """``n_groups`` contiguous ``[start, stop)`` row ranges covering ``n_obs`` rows."""
    if n_groups < 2 or n_obs < n_groups:
        raise OverfitInputError(f"cannot cut {n_obs} rows into {n_groups} groups")
    edges = np.linspace(0, n_obs, n_groups + 1).round().astype(int)
    return [(int(a), int(b)) for a, b in pairwise(edges)]


def label_ends(
    n_obs: int, overlap_depth: int = 0, label_end: Sequence[int] | np.ndarray | None = None
) -> np.ndarray:
    """Last row each observation's label depends on (inclusive).

    Default ``i + overlap_depth`` (capped at the last row): a label built from
    ``overlap_depth + 1`` consecutive rows. ``label_end`` overrides it per row.
    """
    if label_end is None:
        if overlap_depth < 0:
            raise OverfitInputError("overlap_depth must be >= 0")
        return np.minimum(np.arange(n_obs) + int(overlap_depth), n_obs - 1)
    ends = np.asarray(label_end)
    if ends.shape != (n_obs,) or not np.issubdtype(ends.dtype, np.integer):
        raise OverfitInputError("label_end must hold one integer row index per observation")
    if np.any(ends < np.arange(n_obs)) or np.any(ends >= n_obs):
        raise OverfitInputError("label_end[i] must lie in [i, n_obs)")
    return ends.astype(int)


def per_period_sharpe(values: np.ndarray, axis: int = 0) -> np.ndarray:
    """Mean / sample std along ``axis``; 0 where the std is 0."""
    mean = values.mean(axis=axis)
    std = values.std(axis=axis, ddof=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        out = np.where(std > 0, mean / np.where(std > 0, std, 1.0), 0.0)
    return out


def _sharpe_from_moments(count: np.ndarray, s1: np.ndarray, s2: np.ndarray) -> np.ndarray:
    count = count.reshape(-1, 1) if count.ndim == 1 and s1.ndim == 2 else count
    mean = s1 / count
    var = (s2 - s1 * s1 / count) / (count - 1)
    var = np.maximum(var, 0.0)
    std = np.sqrt(var)
    scale = np.maximum(np.abs(mean), 1.0) * 1e-12
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(std > scale, mean / np.where(std > scale, std, 1.0), 0.0)


def validate_matrix(matrix: pd.DataFrame) -> np.ndarray:
    """A trial matrix: tz-aware, unique, increasing dates; unique columns; finite values."""
    if not isinstance(matrix, pd.DataFrame):
        raise OverfitInputError("the trial matrix must be a DataFrame")
    index = matrix.index
    if (
        not isinstance(index, pd.DatetimeIndex)
        or index.tz is None
        or not index.is_monotonic_increasing
        or not index.is_unique
    ):
        raise OverfitInputError("trial matrix rows need a unique, increasing, tz-aware index")
    if not matrix.columns.is_unique or not all(isinstance(c, str) for c in matrix.columns):
        raise OverfitInputError("trial matrix columns must be unique trial-id strings")
    try:
        values = matrix.to_numpy(dtype=float)
    except (TypeError, ValueError) as exc:
        raise OverfitInputError(f"trial matrix values must be numeric: {exc}") from exc
    if not np.all(np.isfinite(values)):
        raise OverfitInputError(
            "trial matrix has non-finite values (record an abstention as a 0 return)"
        )
    return values


def matrix_sha256(matrix: pd.DataFrame) -> str:
    """sha256 over the column ids, the ISO row stamps and the float64 values."""
    head = json.dumps(
        [list(map(str, matrix.columns)), [ts.isoformat() for ts in matrix.index]],
        separators=(",", ":"),
    ).encode()
    values = np.ascontiguousarray(matrix.to_numpy(dtype=np.float64))
    return hashlib.sha256(head + b"\n" + values.tobytes()).hexdigest()


# --- CPCV ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CPCVSplit:
    test_groups: tuple[int, ...]
    train: np.ndarray  # row indices used for fitting
    test: np.ndarray  # row indices of the test groups (never purged)
    purged: np.ndarray  # training rows dropped for label overlap with a test span
    embargoed: np.ndarray  # training rows dropped by the embargo (and not purged)


def _test_runs(test_groups: tuple[int, ...], bounds: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Maximal contiguous ``[start, stop)`` row runs made of adjacent test groups."""
    runs: list[list[int]] = []
    for g in sorted(test_groups):
        a, b = bounds[g]
        if runs and runs[-1][1] == a:
            runs[-1][1] = b
        else:
            runs.append([a, b])
    return [(a, b) for a, b in runs]


def cpcv_splits(
    n_obs: int,
    n_groups: int = CPCV_GROUPS,
    n_test_groups: int = CPCV_TEST_GROUPS,
    *,
    overlap_depth: int = 0,
    label_end: Sequence[int] | np.ndarray | None = None,
    embargo: int = 0,
) -> list[CPCVSplit]:
    """Every CPCV split, in ``itertools.combinations(range(n_groups), k)`` order.

    Observation ``j``'s label spans rows ``[j, label_end[j]]``. For each
    contiguous run of test rows ``[a, b]`` with last label end ``e = max
    label_end[a..b]``, a training row is purged when its span overlaps
    ``[a, e]`` (``j <= e`` and ``label_end[j] >= a``), and embargoed when it
    is not purged and ``e < j <= e + embargo``.
    """
    if not 1 <= n_test_groups < n_groups:
        raise OverfitInputError("need 1 <= n_test_groups < n_groups")
    if embargo < 0:
        raise OverfitInputError("embargo must be >= 0")
    bounds = group_bounds(n_obs, n_groups)
    ends = label_ends(n_obs, overlap_depth, label_end)
    rows = np.arange(n_obs)
    splits = []
    for test_groups in combinations(range(n_groups), n_test_groups):
        is_test = np.zeros(n_obs, dtype=bool)
        for g in test_groups:
            is_test[bounds[g][0] : bounds[g][1]] = True
        purge = np.zeros(n_obs, dtype=bool)
        embargo_mask = np.zeros(n_obs, dtype=bool)
        for a, b in _test_runs(test_groups, bounds):
            last_end = int(ends[a:b].max())
            purge |= (rows <= last_end) & (ends >= a)
            embargo_mask |= (rows > last_end) & (rows <= last_end + embargo)
        purge &= ~is_test
        embargo_mask &= ~is_test & ~purge
        train = ~is_test & ~purge & ~embargo_mask
        splits.append(
            CPCVSplit(
                test_groups=tuple(test_groups),
                train=rows[train],
                test=rows[is_test],
                purged=rows[purge],
                embargoed=rows[embargo_mask],
            )
        )
    return splits


def cpcv_paths(n_groups: int = CPCV_GROUPS, n_test_groups: int = CPCV_TEST_GROUPS) -> np.ndarray:
    """Path assembly: ``paths[p, g]`` is the split whose test set supplies group ``g`` on path ``p``.

    Each group is tested in ``C(N-1, k-1)`` splits; path ``p`` takes the
    ``p``-th of them, so there are ``C(N-1, k-1)`` = ``k / N * C(N, k)`` paths.
    """
    combos = list(combinations(range(n_groups), n_test_groups))
    per_group = [[s for s, c in enumerate(combos) if g in c] for g in range(n_groups)]
    n_paths = math.comb(n_groups - 1, n_test_groups - 1)
    return np.array(
        [[per_group[g][p] for g in range(n_groups)] for p in range(n_paths)], dtype=int
    )


@dataclass(frozen=True)
class CPCVResult:
    n_splits: int
    n_paths: int
    embargo: int
    overlap_depth: int
    path_sharpes: tuple[float, ...]  # per-period OOS Sharpe of select-best-on-train, per path
    mean_path_sharpe: float
    positive_path_fraction: float
    selected: tuple[str, ...]  # the trial chosen on each split's training rows
    candidate_selected_fraction: float | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "n_splits": self.n_splits,
            "n_paths": self.n_paths,
            "embargo": self.embargo,
            "overlap_depth": self.overlap_depth,
            "path_sharpes": list(self.path_sharpes),
            "mean_path_sharpe": self.mean_path_sharpe,
            "positive_path_fraction": self.positive_path_fraction,
            "selections": dict(sorted(_count(self.selected).items())),
            "candidate_selected_fraction": self.candidate_selected_fraction,
        }


def _count(items: Sequence[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for item in items:
        out[item] = out.get(item, 0) + 1
    return out


def cpcv_selection(
    matrix: pd.DataFrame,
    *,
    n_groups: int = CPCV_GROUPS,
    n_test_groups: int = CPCV_TEST_GROUPS,
    overlap_depth: int = 0,
    embargo: int = 0,
    candidates: Sequence[str] = (),
) -> CPCVResult:
    """CPCV of the research loop's selection step over the whole trial matrix.

    On every split the trial with the best (per-period) Sharpe on the purged,
    embargoed training rows is selected; its returns on the split's test
    groups fill those groups on the paths that use this split. Reports each
    path's out-of-sample Sharpe and how often a candidate was the one selected.
    """
    values = validate_matrix(matrix)
    n_obs = len(values)
    columns = list(matrix.columns)
    splits = cpcv_splits(
        n_obs, n_groups, n_test_groups, overlap_depth=overlap_depth, embargo=embargo
    )
    bounds = group_bounds(n_obs, n_groups)
    chosen = []
    for split in splits:
        if len(split.train) < 2:
            raise OverfitInputError("a CPCV split kept fewer than 2 training rows")
        chosen.append(int(np.argmax(per_period_sharpe(values[split.train], axis=0))))
    paths = cpcv_paths(n_groups, n_test_groups)
    sharpes = []
    for path in paths:
        series = np.concatenate(
            [values[bounds[g][0] : bounds[g][1], chosen[path[g]]] for g in range(n_groups)]
        )
        sharpes.append(float(per_period_sharpe(series)))
    selected = tuple(columns[c] for c in chosen)
    wanted = set(candidates)
    return CPCVResult(
        n_splits=len(splits),
        n_paths=len(paths),
        embargo=int(embargo),
        overlap_depth=int(overlap_depth),
        path_sharpes=tuple(sharpes),
        mean_path_sharpe=float(np.mean(sharpes)),
        positive_path_fraction=float(np.mean(np.asarray(sharpes) > 0)),
        selected=selected,
        candidate_selected_fraction=(
            float(np.mean([s in wanted for s in selected])) if wanted else None
        ),
    )


# --- PBO (CSCV) ---------------------------------------------------------------------


@dataclass(frozen=True)
class PBOResult:
    pbo: float  # share of splits whose IS-best trial has OOS logit <= 0
    n_combinations: int
    n_partitions: int
    n_trials: int
    gap: int
    rows_per_partition_min: int
    logit_median: float
    prob_oos_loss: float  # share of splits whose IS-best trial has OOS Sharpe < 0
    degradation_slope: float  # OLS slope of OOS on IS Sharpe of the IS-best trial

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def probability_of_backtest_overfitting(
    matrix: pd.DataFrame | np.ndarray,
    n_partitions: int = CSCV_PARTITIONS,
    *,
    gap: int = 0,
    chunk: int = CSCV_CHUNK,
) -> PBOResult:
    """PBO by combinatorially symmetric cross-validation (Bailey et al. 2017).

    ``matrix``: T x N per-period returns of every trial. Performance is the
    per-period Sharpe. For each of the ``C(S, S/2)`` in-sample halves, ``n*``
    is the best in-sample trial, ``w`` its out-of-sample relative rank
    ``rank / (N + 1)`` (average ranks for ties) and ``logit = ln(w / (1 - w))``;
    PBO is the share of halves with ``logit <= 0``. The last ``gap`` rows of
    every block are dropped first (overlap purge plus embargo).
    """
    values = (
        validate_matrix(matrix) if isinstance(matrix, pd.DataFrame) else np.asarray(matrix, float)
    )
    if values.ndim != 2 or not np.all(np.isfinite(values)):
        raise OverfitInputError("PBO needs a finite T x N matrix")
    n_obs, n_trials = values.shape
    if n_trials < 2:
        raise OverfitInputError("PBO needs at least 2 trials")
    if n_partitions < 2 or n_partitions % 2:
        raise OverfitInputError("CSCV needs an even number of partitions >= 2")
    if gap < 0:
        raise OverfitInputError("gap must be >= 0")
    bounds = group_bounds(n_obs, n_partitions)
    kept = [(a, b - gap) for a, b in bounds]
    rows_min = min(b - a for a, b in kept)
    if rows_min < MIN_ROWS_PER_PARTITION:
        raise OverfitInputError(
            f"{n_obs} rows in {n_partitions} partitions with gap {gap} leave "
            f"{rows_min} rows in a partition (< {MIN_ROWS_PER_PARTITION})"
        )
    count = np.array([b - a for a, b in kept], dtype=float)
    s1 = np.stack([values[a:b].sum(axis=0) for a, b in kept])
    s2 = np.stack([(values[a:b] ** 2).sum(axis=0) for a, b in kept])
    total = (count.sum(), s1.sum(axis=0), s2.sum(axis=0))
    combos = np.array(list(combinations(range(n_partitions), n_partitions // 2)), dtype=int)
    logits, is_best, oos_best = [], [], []
    for start in range(0, len(combos), max(1, int(chunk))):
        part = combos[start : start + chunk]
        member = np.zeros((len(part), n_partitions))
        member[np.arange(len(part))[:, None], part] = 1.0
        is_n = member @ count
        is_s1, is_s2 = member @ s1, member @ s2
        is_sr = _sharpe_from_moments(is_n, is_s1, is_s2)
        oos_sr = _sharpe_from_moments(total[0] - is_n, total[1] - is_s1, total[2] - is_s2)
        best = np.argmax(is_sr, axis=1)
        rows = np.arange(len(part))
        ranks = stats.rankdata(oos_sr, axis=1)[rows, best]
        omega = ranks / (n_trials + 1)
        logits.append(np.log(omega / (1 - omega)))
        is_best.append(is_sr[rows, best])
        oos_best.append(oos_sr[rows, best])
    logit = np.concatenate(logits)
    x, y = np.concatenate(is_best), np.concatenate(oos_best)
    slope = float(np.polyfit(x, y, 1)[0]) if np.ptp(x) > 0 else 0.0
    return PBOResult(
        pbo=float(np.mean(logit <= 0)),
        n_combinations=len(combos),
        n_partitions=int(n_partitions),
        n_trials=int(n_trials),
        gap=int(gap),
        rows_per_partition_min=int(rows_min),
        logit_median=float(np.median(logit)),
        prob_oos_loss=float(np.mean(y < 0)),
        degradation_slope=slope,
    )


# --- DSR over the matrix ------------------------------------------------------------


def matrix_deflated_sharpe(
    matrix: pd.DataFrame,
    candidate: str,
    *,
    n_trials: int,
    overlap_depth: int = 0,
    confidence: float = DSR_MIN,
) -> DeflatedSharpe:
    """#979's DSR for one matrix column.

    ``n_trials`` is the ledger's true trial count (it may exceed the matrix's
    columns: declared trials that could not be measured still count); the
    cross-trial Sharpe variance is taken over every matrix column. The
    observation count is ``T // (overlap_depth + 1)``.
    """
    values = validate_matrix(matrix)
    if candidate not in matrix.columns:
        raise OverfitInputError(f"candidate {candidate!r} is not a matrix column")
    if n_trials < matrix.shape[1]:
        raise OverfitInputError("n_trials cannot be below the matrix's trial count")
    series = matrix[candidate].to_numpy(dtype=float)
    sharpes = per_period_sharpe(values, axis=0)
    n_eff = len(series) // (int(overlap_depth) + 1)
    return deflated_sharpe_ratio(
        float(per_period_sharpe(series)),
        int(n_trials),
        int(n_eff),
        trial_sharpe_variance(sharpes) if len(sharpes) > 1 else 0.0,
        float(stats.skew(series)),
        float(stats.kurtosis(series, fisher=False)),
        confidence,
    )


def embargo_rows(
    matrix: pd.DataFrame, overlap_depth: int = 0, fraction: float = EMBARGO_FRACTION
) -> tuple[int, dict[str, Any]]:
    """``max(ceil(fraction * T), block - 1)``; ``block`` from #979's dependence sizing.

    The block is :func:`autocorrelation_block` of the cross-trial mean return
    (floor ``overlap_depth + 1``), so the embargo is never below the overlap depth.
    """
    values = validate_matrix(matrix)
    base = math.ceil(fraction * len(values))
    block, basis = autocorrelation_block(values.mean(axis=1), int(overlap_depth))
    return max(base, int(block) - 1), {
        "fraction_rows": base,
        "dependence_block": int(block),
        "block_basis": basis,
    }


# --- the gate's numbers -------------------------------------------------------------


def evaluate_trial_matrix(
    matrix: pd.DataFrame,
    candidates: Sequence[str],
    *,
    n_trials: int,
    overlap_depth: int = 0,
    config: OverfitGateConfig | None = None,
) -> dict[str, Any]:
    """PBO, CPCV and per-candidate DSR over the full matrix, with the pass/fail verdict.

    Passes iff ``PBO < pbo_max`` and every candidate's ``DSR > dsr_min``.
    Raises :class:`OverfitInputError` when the matrix cannot support the checks.
    """
    config = config or OverfitGateConfig()
    config.validate()
    if overlap_depth < 0 or int(overlap_depth) != overlap_depth:
        raise OverfitInputError("overlap_depth must be a non-negative integer")
    if not candidates:
        raise OverfitInputError("no candidate trials")
    validate_matrix(matrix)
    if matrix.shape[1] < 2:
        raise OverfitInputError("PBO needs at least 2 trials in the matrix")
    embargo, embargo_basis = embargo_rows(matrix, overlap_depth, config.embargo_fraction)
    pbo = probability_of_backtest_overfitting(
        matrix, config.cscv_partitions, gap=int(overlap_depth) + embargo
    )
    cpcv = cpcv_selection(
        matrix,
        n_groups=config.cpcv_groups,
        n_test_groups=config.cpcv_test_groups,
        overlap_depth=int(overlap_depth),
        embargo=embargo,
        candidates=candidates,
    )
    reasons = []
    pbo_passed = pbo.pbo < config.pbo_max
    if not pbo_passed:
        reasons.append(f"PBO {pbo.pbo:.4f} >= {config.pbo_max}")
    dsr: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        try:
            result = matrix_deflated_sharpe(
                matrix,
                candidate,
                n_trials=n_trials,
                overlap_depth=int(overlap_depth),
                confidence=config.dsr_min,
            )
        except ValueError as exc:  # e.g. non-positive Sharpe variance from extreme moments
            dsr[candidate] = {"dsr": None, "passed": False, "error": str(exc)}
            reasons.append(f"DSR of {candidate[:12]} unavailable: {exc}")
            continue
        dsr[candidate] = {
            "dsr": result.dsr,
            "passed": result.passed,
            "sharpe_per_period": result.sharpe,
            "sr0_per_period": result.sr0,
            "expected_max_z": result.expected_max_z,
            "n_trials": result.n_trials,
            "n_observations": result.n_observations,
            "trial_sharpe_variance": result.trial_sharpe_variance,
            "skewness": result.skewness,
            "kurtosis": result.kurtosis,
        }
        if not result.passed:
            reasons.append(f"DSR of {candidate[:12]} {result.dsr:.4f} <= {config.dsr_min}")
    return {
        "passed": not reasons,
        "reasons": reasons,
        "pbo": pbo.as_dict(),
        "pbo_passed": pbo_passed,
        "dsr": dsr,
        "dsr_passed": all(d["passed"] for d in dsr.values()),
        "cpcv": cpcv.as_dict(),
        "embargo": embargo,
        "embargo_basis": embargo_basis,
        "overlap_depth": int(overlap_depth),
    }
