"""
Tests for the GRID Hyperspace integration layer.

All tests are designed to pass whether or not Hyperspace is running.
They verify graceful degradation when the node is unavailable and
correct logic for deduplication.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from hyperspace.client import HyperspaceClient
from hyperspace.embeddings import GRIDEmbeddings
from hyperspace.monitor import HyperspaceMonitor
from hyperspace.reasoner import GRIDReasoner


# ---------------------------------------------------------------------------
# Client graceful degradation
# ---------------------------------------------------------------------------

class TestClientGracefulDegradation:
    """Verify the client handles unavailability without raising."""

    def test_client_handles_unavailable_gracefully(self):
        """Client on a wrong port should report unavailable and return None."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        assert client.is_available is False

        # chat should return None, not raise
        result = client.chat([{"role": "user", "content": "test"}])
        assert result is None

        # embed should return None, not raise
        result = client.embed(["test text"])
        assert result is None

    def test_health_check_structure(self):
        """health_check should return a well-formed dict even when unavailable."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        result = client.health_check()

        assert isinstance(result, dict)
        assert "available" in result
        assert "latency_ms" in result
        assert "models" in result
        assert "endpoint" in result
        assert result["available"] is False

    def test_get_available_models_returns_empty(self):
        """get_available_models should return empty list when unavailable."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        models = client.get_available_models()
        assert isinstance(models, list)
        assert len(models) == 0


# ---------------------------------------------------------------------------
# Embeddings graceful degradation
# ---------------------------------------------------------------------------

class TestEmbeddingsGracefulDegradation:
    """Verify embeddings return None gracefully when node is down."""

    def test_embeddings_returns_none_gracefully(self):
        """semantic_similarity_matrix should return None without raising."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        embedder = GRIDEmbeddings(client)

        result = embedder.semantic_similarity_matrix(["feat_a", "feat_b"])
        assert result is None

    def test_embed_features_returns_none(self):
        """embed_features should return None without raising."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        embedder = GRIDEmbeddings(client)

        result = embedder.embed_features(["feat_a", "feat_b"])
        assert result is None

    def test_find_similar_returns_empty(self):
        """find_similar_features should return empty list without raising."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        embedder = GRIDEmbeddings(client)

        result = embedder.find_similar_features("credit stress", ["a", "b"])
        assert isinstance(result, list)
        assert len(result) == 0

    def test_embed_hypothesis_returns_none(self):
        """embed_hypothesis should return None without raising."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        embedder = GRIDEmbeddings(client)

        result = embedder.embed_hypothesis("test hypothesis")
        assert result is None


# ---------------------------------------------------------------------------
# Reasoner graceful degradation
# ---------------------------------------------------------------------------

class TestReasonerGracefulDegradation:
    """Verify reasoner returns None gracefully when node is down."""

    def test_reasoner_returns_none_gracefully(self):
        """explain_relationship should return None without raising."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        reasoner = GRIDReasoner(client)

        result = reasoner.explain_relationship("a", "b", "test pattern")
        assert result is None

    def test_generate_hypotheses_returns_none(self):
        """generate_hypothesis_candidates should return None without raising."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        reasoner = GRIDReasoner(client)

        result = reasoner.generate_hypothesis_candidates("test pattern")
        assert result is None

    def test_critique_returns_none(self):
        """critique_backtest_result should return None without raising."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        reasoner = GRIDReasoner(client)

        result = reasoner.critique_backtest_result(
            "test hypothesis", "sharpe", 1.5, 1.0, 10
        )
        assert result is None


# ---------------------------------------------------------------------------
# Monitor graceful degradation
# ---------------------------------------------------------------------------

class TestMonitorGracefulDegradation:
    """Verify monitor handles offline node without raising."""

    def test_monitor_handles_offline(self):
        """is_earning should return False without raising."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        monitor = HyperspaceMonitor(client)

        result = monitor.is_earning()
        assert result is False

    def test_tail_log_returns_empty(self):
        """tail_log should return empty list if no log exists."""
        client = HyperspaceClient(
            base_url="http://localhost:9999/v1",
            timeout=2,
        )
        monitor = HyperspaceMonitor(client)

        # May return lines if a log file happens to exist, but should not raise
        result = monitor.tail_log(10)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Hypothesis deduplication logic (no network needed)
# ---------------------------------------------------------------------------

class TestHypothesisDedupLogic:
    """Test dedup logic using mocked embeddings (no network required)."""

    def test_dedup_identifies_near_duplicate(self):
        """Nearly identical statements should be flagged as duplicates."""
        client = MagicMock(spec=HyperspaceClient)
        client.is_available = True

        embedder = GRIDEmbeddings(client)

        # Mock embed to return controlled vectors
        # Near-duplicate: vectors pointing in nearly the same direction
        # Different: vector pointing in a different direction
        near_dup_vec = [1.0, 0.0, 0.0]
        original_vec = [0.99, 0.1, 0.0]  # Very close to near_dup
        different_vec = [0.0, 0.0, 1.0]  # Orthogonal

        client.embed.return_value = [
            near_dup_vec,   # new_statement
            original_vec,   # first existing (near-duplicate)
            different_vec,  # second existing (different)
        ]

        is_dup, match = embedder.hypothesis_dedup_check(
            new_statement="Yield curve inversion predicts recession within 12 months",
            existing_statements=[
                "Inverted yield curve is a leading indicator of recession within one year",
                "VIX term structure backwardation signals near-term equity risk",
            ],
            threshold=0.92,
        )

        # The near_dup_vec and original_vec have cosine similarity ~0.995
        assert is_dup is True
        assert match == "Inverted yield curve is a leading indicator of recession within one year"

    def test_dedup_clears_different_statement(self):
        """A genuinely different statement should not be flagged."""
        client = MagicMock(spec=HyperspaceClient)
        client.is_available = True

        embedder = GRIDEmbeddings(client)

        # New statement vector is orthogonal to all existing
        client.embed.return_value = [
            [0.0, 0.0, 1.0],   # new (orthogonal)
            [1.0, 0.0, 0.0],   # existing 1
            [0.0, 1.0, 0.0],   # existing 2
        ]

        is_dup, match = embedder.hypothesis_dedup_check(
            new_statement="Copper-gold ratio predicts manufacturing PMI direction",
            existing_statements=[
                "Yield curve inversion predicts recession",
                "Credit spreads widen before equity drawdowns",
            ],
            threshold=0.92,
        )

        # Orthogonal vectors have cosine similarity ~0
        assert is_dup is False

    def test_dedup_handles_empty_existing(self):
        """Empty existing list should return not duplicate."""
        client = MagicMock(spec=HyperspaceClient)
        client.is_available = True

        embedder = GRIDEmbeddings(client)

        is_dup, match = embedder.hypothesis_dedup_check(
            new_statement="test",
            existing_statements=[],
        )

        assert is_dup is False
        assert match is None
