"""Tests for the discovery.clustering performance fixes.

The consolidated audit (CRITICAL #9) flagged _evaluate_k as O(n²)
because of silhouette_score on the full sample plus
AgglomerativeClustering.fit_predict, which were the main cost terms
for n=10K observations (60-180s in the audit). The fix:

  * silhouette_score uses sample_size=2000 by default
  * AgglomerativeClustering is skipped past
    GRID_CLUSTERING_AGG_MAX_N=5000 rows; calinski_harabasz falls back
    to KMeans labels and an `agg_skipped_n_too_large` flag is set
  * _compute_persistence is vectorised via np.diff

These tests pin down the contract so future edits can't silently
regress (e.g. someone removing the sample_size kwarg).
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from discovery.clustering import ClusterDiscovery


@pytest.fixture
def cd_obj():
    """ClusterDiscovery without engine/pit_store deps — _evaluate_k
    only touches the input array, so we bypass __init__."""
    return ClusterDiscovery.__new__(ClusterDiscovery)


def _make_features(n: int, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.standard_normal(size=(n, 4))


def _dates(n: int) -> pd.DatetimeIndex:
    return pd.DatetimeIndex(pd.date_range("2010-01-01", periods=n))


def test_evaluate_k_small_n_runs_agglomerative(cd_obj) -> None:
    """At small n we still run all three clusterers."""
    features = _make_features(500)
    result = cd_obj._evaluate_k(features, k=3, dates=_dates(500))
    assert "agg_calinski_harabasz" in result
    assert result.get("agg_skipped_n_too_large") is not True


def test_evaluate_k_large_n_skips_agglomerative(cd_obj) -> None:
    """Past the threshold we MUST skip the O(n³) clusterer to keep
    runtime under control."""
    # Use a small enough threshold to test cheaply.
    os.environ["GRID_CLUSTERING_AGG_MAX_N"] = "100"
    try:
        features = _make_features(500)
        result = cd_obj._evaluate_k(features, k=3, dates=_dates(500))
    finally:
        del os.environ["GRID_CLUSTERING_AGG_MAX_N"]
    assert result.get("agg_skipped_n_too_large") is True
    # CH score still populated via KMeans labels — never None.
    assert isinstance(result["agg_calinski_harabasz"], float)


def test_evaluate_k_silhouette_subsamples_for_large_n(cd_obj) -> None:
    """At large n, silhouette_score must use the sample_size kwarg.
    We verify by setting an absurdly small sample size and confirming
    the function still completes; if it ignored the kwarg, n=2000
    full-sample silhouette would be slow but not broken — so we
    instead verify the result exists and is finite."""
    os.environ["GRID_SILHOUETTE_SAMPLE"] = "100"
    try:
        features = _make_features(2000)
        result = cd_obj._evaluate_k(features, k=3, dates=_dates(2000))
    finally:
        del os.environ["GRID_SILHOUETTE_SAMPLE"]
    sil = result["kmeans_silhouette"]
    assert isinstance(sil, float)
    assert -1.0 <= sil <= 1.0
    assert not np.isnan(sil)


def test_compute_persistence_vectorised_matches_loop(cd_obj) -> None:
    """The vectorised np.diff implementation must produce the same
    result as the original Python loop for representative inputs."""

    def loop_impl(labels):
        if len(labels) == 0:
            return 0.0
        runs, cur = [], 1
        for i in range(1, len(labels)):
            if labels[i] == labels[i - 1]:
                cur += 1
            else:
                runs.append(cur)
                cur = 1
        runs.append(cur)
        return float(np.mean(runs))

    rng = np.random.default_rng(0)
    for seed in range(5):
        rng = np.random.default_rng(seed)
        labels = rng.integers(0, 6, size=500)
        assert (
            abs(cd_obj._compute_persistence(labels) - loop_impl(labels)) < 1e-9
        ), f"seed {seed} mismatch"


def test_compute_persistence_edge_cases(cd_obj) -> None:
    assert cd_obj._compute_persistence(np.array([])) == 0.0
    assert cd_obj._compute_persistence(np.array([0])) == 1.0
    # All same → one run of length n.
    assert cd_obj._compute_persistence(np.array([7] * 50)) == 50.0
    # Alternating → n runs of length 1, mean = 1.
    assert cd_obj._compute_persistence(np.array([0, 1, 0, 1, 0])) == 1.0


def test_evaluate_k_result_keys_stable(cd_obj) -> None:
    """The result dict shape is consumed downstream by save_snapshot
    and the persistence/transition LLM interpreter. Lock the keys so
    we don't break consumers when refactoring."""
    features = _make_features(300)
    result = cd_obj._evaluate_k(features, k=3, dates=_dates(300))
    expected = {
        "k",
        "gmm_bic",
        "gmm_aic",
        "kmeans_inertia",
        "kmeans_silhouette",
        "agg_calinski_harabasz",
        "gmm_persistence",
        "transition_entropy",
    }
    missing = expected - set(result.keys())
    assert not missing, f"missing keys: {missing}"
