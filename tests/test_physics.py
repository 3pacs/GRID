"""
Tests for the physics verification and news momentum modules.

Tests cover:
- MarketPhysicsVerifier.verify_all graceful error handling
- Individual check behavior with empty/missing data
- NewsMomentumAnalyzer with mock data
- News momentum check integration in verify suite
- Endpoint-level error handling
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from physics.momentum import MomentumResult, NewsMomentumAnalyzer
from physics.verify import MarketPhysicsVerifier, VerificationResult


# ---------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------


@pytest.fixture
def mock_engine():
    """Return a mock SQLAlchemy engine with basic connect() support."""
    engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.fetchone.return_value = None
    mock_result.fetchall.return_value = []
    mock_result.scalar.return_value = 0
    mock_conn.execute.return_value = mock_result

    engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    return engine


@pytest.fixture
def mock_pit_store():
    """Return a mock PITStore returning empty DataFrames by default."""
    pit = MagicMock()
    pit.get_pit.return_value = pd.DataFrame(
        columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"]
    )
    pit.get_feature_matrix.return_value = pd.DataFrame()
    return pit


# ---------------------------------------------------------------
# VerificationResult tests
# ---------------------------------------------------------------


class TestVerificationResult:
    def test_to_dict(self) -> None:
        vr = VerificationResult(
            check_name="test",
            passed=True,
            score=0.95,
            details={"key": "value"},
            warnings=["warn1"],
        )
        d = vr.to_dict()
        assert d["check_name"] == "test"
        assert d["passed"] is True
        assert d["score"] == 0.95
        assert d["details"] == {"key": "value"}
        assert d["warnings"] == ["warn1"]


# ---------------------------------------------------------------
# MarketPhysicsVerifier tests
# ---------------------------------------------------------------


class TestVerifyAll:
    def test_verify_all_returns_summary(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """verify_all should return results for all checks plus a _summary."""
        verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
        results = verifier.verify_all(date(2024, 1, 15))

        assert "_summary" in results
        summary = results["_summary"]
        assert "total_checks" in summary
        assert "passed" in summary
        assert "failed" in summary
        assert summary["total_checks"] >= 6
        assert summary["as_of_date"] == "2024-01-15"

    def test_verify_all_handles_check_exception(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """If a single check raises, it should be caught and reported."""
        verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)

        # Force one check to raise
        verifier.check_conservation = MagicMock(
            side_effect=RuntimeError("DB connection lost")
        )

        results = verifier.verify_all(date(2024, 1, 15))

        # Should still have results for all checks
        assert "_summary" in results
        assert "conservation" in results
        assert results["conservation"]["passed"] is False
        assert "DB connection lost" in results["conservation"]["details"]["error"]

    def test_verify_all_includes_news_momentum(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """verify_all should include the news_momentum check."""
        verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
        results = verifier.verify_all(date(2024, 6, 1))

        assert "news_momentum" in results

    def test_check_conservation_no_flow_features(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Conservation check with no flow features should skip gracefully."""
        verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
        result = verifier.check_conservation(date(2024, 1, 1))

        assert result.check_name == "conservation"
        assert result.passed is True

    def test_check_limiting_cases_no_data(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Limiting cases with no data should pass (nothing to violate)."""
        verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
        result = verifier.check_limiting_cases(date(2024, 1, 1))

        assert result.check_name == "limiting_cases"
        # With no data, cases_tested=0, score=1.0
        assert result.passed is True

    def test_check_numerical_stability_empty_db(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Numerical stability with empty DB should pass."""
        verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
        result = verifier.check_numerical_stability(date(2024, 1, 1))

        assert result.check_name == "numerical_stability"
        assert result.passed is True


# ---------------------------------------------------------------
# NewsMomentumAnalyzer tests
# ---------------------------------------------------------------


class TestNewsMomentumAnalyzer:
    def test_analyze_no_gdelt_features(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Should return unavailable when no GDELT features exist."""
        analyzer = NewsMomentumAnalyzer(mock_engine, mock_pit_store)
        result = analyzer.analyze(date(2024, 6, 1))

        assert result.available is False
        assert result.sentiment_trend == "unavailable"
        assert result.momentum_direction == "unavailable"
        assert result.energy_state == "unavailable"

    def test_analyze_with_data(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Should compute momentum metrics when data is available."""
        # Mock feature registry lookup
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1, "gdelt_tone_usa")]
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Create synthetic tone data
        dates = pd.date_range("2024-01-01", periods=60, freq="D")
        tone_data = np.sin(np.linspace(0, 4 * np.pi, 60)) * 2 + 1
        matrix = pd.DataFrame({1: tone_data}, index=dates)
        mock_pit_store.get_feature_matrix.return_value = matrix

        analyzer = NewsMomentumAnalyzer(mock_engine, mock_pit_store)
        result = analyzer.analyze(date(2024, 3, 1))

        assert result.available is True
        assert result.sentiment_trend in ("rising", "falling", "neutral")
        assert result.momentum_direction in ("accelerating", "decelerating", "stable")
        assert result.energy_state in ("high", "medium", "low")
        assert "trend" in result.details
        assert "momentum" in result.details
        assert "energy" in result.details

    def test_analyze_insufficient_data(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Should return unavailable when data has too few points."""
        # Mock feature registry lookup
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1, "gdelt_tone_usa")]
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Only 3 data points
        dates = pd.date_range("2024-01-01", periods=3, freq="D")
        matrix = pd.DataFrame({1: [1.0, 2.0, 3.0]}, index=dates)
        mock_pit_store.get_feature_matrix.return_value = matrix

        analyzer = NewsMomentumAnalyzer(mock_engine, mock_pit_store)
        result = analyzer.analyze(date(2024, 1, 4))

        assert result.available is False

    def test_momentum_result_to_dict(self) -> None:
        """MomentumResult.to_dict should serialize correctly."""
        mr = MomentumResult(
            available=True,
            sentiment_trend="rising",
            momentum_direction="accelerating",
            energy_state="high",
            details={"key": "val"},
            warnings=["w1"],
        )
        d = mr.to_dict()
        assert d["available"] is True
        assert d["sentiment_trend"] == "rising"
        assert d["energy_state"] == "high"
        assert d["details"]["key"] == "val"

    def test_resolve_feature_ids_db_error(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """Should return empty dict on DB error, not crash."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = RuntimeError("connection refused")
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        analyzer = NewsMomentumAnalyzer(mock_engine, mock_pit_store)
        ids = analyzer._resolve_feature_ids(["gdelt_tone_usa"])

        assert ids == {}


# ---------------------------------------------------------------
# check_news_momentum integration
# ---------------------------------------------------------------


class TestCheckNewsMomentum:
    def test_news_momentum_no_data(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """News momentum check should pass with score=0.5 when no data."""
        verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
        result = verifier.check_news_momentum(date(2024, 6, 1))

        assert result.check_name == "news_momentum"
        assert result.passed is True
        assert result.score == 0.5

    def test_news_momentum_with_data(
        self, mock_engine: MagicMock, mock_pit_store: MagicMock
    ) -> None:
        """News momentum check should produce valid result with good data."""
        # Mock feature registry returning gdelt_tone_usa
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [(1, "gdelt_tone_usa")]
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        # Provide realistic-looking tone data
        dates = pd.date_range("2024-03-01", periods=60, freq="D")
        tone_data = np.random.default_rng(42).normal(2.0, 0.5, 60)
        matrix = pd.DataFrame({1: tone_data}, index=dates)
        mock_pit_store.get_feature_matrix.return_value = matrix

        verifier = MarketPhysicsVerifier(mock_engine, mock_pit_store)
        result = verifier.check_news_momentum(date(2024, 5, 1))

        assert result.check_name == "news_momentum"
        assert result.score > 0.0
        assert "sentiment_trend" in result.details
