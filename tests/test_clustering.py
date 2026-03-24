"""Tests for discovery/clustering.py — ClusterDiscovery.

All tests use mocked database and PIT store fixtures; no real PostgreSQL required.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from discovery.clustering import ClusterDiscovery


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cluster_discovery(
    mock_engine: MagicMock,
    mock_pit_store: MagicMock,
) -> ClusterDiscovery:
    """Create a ClusterDiscovery with mocked dependencies."""
    return ClusterDiscovery(db_engine=mock_engine, pit_store=mock_pit_store)


# ---------------------------------------------------------------------------
# Tests: Feature matrix edge cases
# ---------------------------------------------------------------------------

class TestClusterDiscoveryMatrix:
    """Tests for run_cluster_discovery edge cases."""

    def test_empty_feature_matrix(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Empty feature matrix returns an error dict."""
        mock_pit_store.get_feature_matrix.return_value = pd.DataFrame()

        # Wire engine to return some feature IDs (matrix emptiness is the trigger)
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1,), (2,)]
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        cd = _make_cluster_discovery(mock_engine, mock_pit_store)
        result = cd.run_cluster_discovery(n_components=3)
        assert "error" in result

    def test_insufficient_data_rows(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Matrix with fewer than 30 rows returns an error."""
        dates = pd.date_range("2020-01-01", periods=20, freq="B")
        matrix = pd.DataFrame(
            np.random.default_rng(1).standard_normal((20, 3)),
            index=dates,
            columns=[1, 2, 3],
        )
        mock_pit_store.get_feature_matrix.return_value = matrix

        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1,), (2,), (3,)]
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        cd = _make_cluster_discovery(mock_engine, mock_pit_store)
        result = cd.run_cluster_discovery(n_components=2)
        assert "error" in result
        assert "Insufficient" in result["error"]


# ---------------------------------------------------------------------------
# Tests: _compute_persistence
# ---------------------------------------------------------------------------

class TestComputePersistence:
    """Tests for ClusterDiscovery._compute_persistence."""

    def test_compute_persistence_single_run(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """All same label: one run of length 4 -> persistence=4.0."""
        cd = _make_cluster_discovery(mock_engine, mock_pit_store)
        result = cd._compute_persistence(np.array([0, 0, 0, 0]))
        assert result == 4.0

    def test_compute_persistence_alternating(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Alternating labels: each run is length 1 -> persistence=1.0."""
        cd = _make_cluster_discovery(mock_engine, mock_pit_store)
        result = cd._compute_persistence(np.array([0, 1, 0, 1]))
        assert result == 1.0

    def test_compute_persistence_empty(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Empty labels array -> persistence=0.0."""
        cd = _make_cluster_discovery(mock_engine, mock_pit_store)
        result = cd._compute_persistence(np.array([]))
        assert result == 0.0


# ---------------------------------------------------------------------------
# Tests: _compute_transition_matrix
# ---------------------------------------------------------------------------

class TestComputeTransitionMatrix:
    """Tests for ClusterDiscovery._compute_transition_matrix."""

    def test_compute_transition_matrix(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Transition matrix rows should each sum to 1."""
        cd = _make_cluster_discovery(mock_engine, mock_pit_store)
        labels = np.array([0, 0, 1, 1, 0])
        trans = cd._compute_transition_matrix(labels, k=2)

        assert trans.shape == (2, 2)
        # Each row sums to 1
        np.testing.assert_allclose(trans.sum(axis=1), [1.0, 1.0])

    def test_transition_matrix_unobserved_state(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """State 2 never appears as a 'from' state -> gets uniform row."""
        cd = _make_cluster_discovery(mock_engine, mock_pit_store)
        labels = np.array([0, 1, 0, 1])
        trans = cd._compute_transition_matrix(labels, k=3)

        assert trans.shape == (3, 3)
        # State 2 row should be uniform (1/3 each)
        np.testing.assert_allclose(trans[2], [1.0 / 3, 1.0 / 3, 1.0 / 3])
        # All rows sum to 1
        np.testing.assert_allclose(trans.sum(axis=1), [1.0, 1.0, 1.0])


# ---------------------------------------------------------------------------
# Tests: _evaluate_k
# ---------------------------------------------------------------------------

class TestEvaluateK:
    """Tests for ClusterDiscovery._evaluate_k."""

    def test_evaluate_k_returns_metrics(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """_evaluate_k should return a dict with expected metric keys."""
        cd = _make_cluster_discovery(mock_engine, mock_pit_store)

        rng = np.random.default_rng(42)
        features = rng.standard_normal((100, 3))
        dates = pd.date_range("2020-01-01", periods=100, freq="B")

        result = cd._evaluate_k(features, k=3, dates=dates)

        assert result["k"] == 3
        assert "gmm_bic" in result
        assert "kmeans_silhouette" in result
        assert isinstance(result["gmm_bic"], float)
        assert isinstance(result["kmeans_silhouette"], float)
        assert "gmm_persistence" in result
        assert "transition_entropy" in result
