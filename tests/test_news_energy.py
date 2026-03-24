"""
Tests for physics/news_energy.py — News Energy Decomposition Engine.

Tests cover energy computations, coherence, cross-correlation, and the
static methods without requiring a live database.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from physics.news_energy import NewsEnergyEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def random_series() -> pd.Series:
    """Generate a random walk series for testing."""
    np.random.seed(42)
    n = 100
    returns = np.random.randn(n) * 0.01
    prices = 100 * np.exp(np.cumsum(returns))
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    return pd.Series(prices, index=idx, name="test_series")


@pytest.fixture
def trending_series() -> pd.Series:
    """Generate a consistently trending series."""
    np.random.seed(42)
    n = 100
    trend = np.linspace(0, 2, n)
    noise = np.random.randn(n) * 0.1
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    return pd.Series(trend + noise + 100, index=idx, name="trending")


@pytest.fixture
def flat_series() -> pd.Series:
    """Generate a flat (mean-reverting) series."""
    np.random.seed(42)
    n = 100
    noise = np.random.randn(n) * 0.01
    idx = pd.date_range("2025-01-01", periods=n, freq="B")
    return pd.Series(100 + noise, index=idx, name="flat")


# ---------------------------------------------------------------------------
# Kinetic energy tests
# ---------------------------------------------------------------------------


class TestKineticEnergy:
    def test_returns_series(self, random_series: pd.Series) -> None:
        ke = NewsEnergyEngine._kinetic_energy(random_series, window=5)
        assert isinstance(ke, pd.Series)
        assert len(ke) == len(random_series)

    def test_non_negative(self, random_series: pd.Series) -> None:
        ke = NewsEnergyEngine._kinetic_energy(random_series, window=5)
        valid = ke.dropna()
        assert (valid >= 0).all(), "Kinetic energy must be non-negative"

    def test_trending_has_higher_ke(
        self, trending_series: pd.Series, flat_series: pd.Series
    ) -> None:
        ke_trend = NewsEnergyEngine._kinetic_energy(trending_series, window=5)
        ke_flat = NewsEnergyEngine._kinetic_energy(flat_series, window=5)
        # Trending series should have higher mean KE
        assert ke_trend.dropna().mean() > ke_flat.dropna().mean()

    def test_short_series(self) -> None:
        """Short series should not crash."""
        s = pd.Series([1.0, 2.0, 3.0])
        ke = NewsEnergyEngine._kinetic_energy(s, window=5)
        assert isinstance(ke, pd.Series)


# ---------------------------------------------------------------------------
# Potential energy tests
# ---------------------------------------------------------------------------


class TestPotentialEnergy:
    def test_returns_series(self, random_series: pd.Series) -> None:
        pe = NewsEnergyEngine._potential_energy(random_series, window=21)
        assert isinstance(pe, pd.Series)
        assert len(pe) == len(random_series)

    def test_non_negative(self, random_series: pd.Series) -> None:
        pe = NewsEnergyEngine._potential_energy(random_series, window=21)
        valid = pe.dropna()
        assert (valid >= 0).all(), "Potential energy must be non-negative"


# ---------------------------------------------------------------------------
# Coherence tests
# ---------------------------------------------------------------------------


class TestCoherence:
    def test_all_positive_changes(self) -> None:
        """All sources increasing should give high coherence."""
        changes = {
            "src_a": pd.Series([0.1, 0.2, 0.3]),
            "src_b": pd.Series([0.05, 0.1, 0.15]),
            "src_c": pd.Series([0.2, 0.3, 0.4]),
        }
        result = NewsEnergyEngine._compute_coherence(changes)
        assert result["coherence"] == 1.0
        assert result["dominant_direction"] == "increasing"
        assert len(result["aligned_sources"]) == 3

    def test_all_negative_changes(self) -> None:
        changes = {
            "src_a": pd.Series([-0.1, -0.2, -0.3]),
            "src_b": pd.Series([-0.05, -0.1, -0.15]),
        }
        result = NewsEnergyEngine._compute_coherence(changes)
        assert result["coherence"] == 1.0
        assert result["dominant_direction"] == "decreasing"

    def test_mixed_changes(self) -> None:
        """Mixed directions should give lower coherence."""
        changes = {
            "src_a": pd.Series([0.1, 0.2, 0.3]),
            "src_b": pd.Series([-0.1, -0.2, -0.3]),
        }
        result = NewsEnergyEngine._compute_coherence(changes)
        assert result["coherence"] == 0.5

    def test_empty_input(self) -> None:
        result = NewsEnergyEngine._compute_coherence({})
        assert result["coherence"] == 0.0
        assert result["n_sources"] == 0


# ---------------------------------------------------------------------------
# Cross-correlation tests
# ---------------------------------------------------------------------------


class TestCrossCorrelate:
    def test_identical_series(self) -> None:
        """Identical series should have correlation near 1 at lag 0."""
        idx = pd.date_range("2025-01-01", periods=50, freq="B")
        s = pd.Series(np.random.randn(50), index=idx)
        result = NewsEnergyEngine._cross_correlate(s, s)
        assert result["peak_correlation"] > 0.9
        assert result["optimal_lag"] == 0

    def test_insufficient_data(self) -> None:
        s1 = pd.Series([1.0, 2.0])
        s2 = pd.Series([3.0, 4.0])
        result = NewsEnergyEngine._cross_correlate(s1, s2)
        assert result["direction"] == "insufficient_data"

    def test_lagged_correlation(self) -> None:
        """A shifted series should show lag in the result."""
        np.random.seed(42)
        n = 100
        idx = pd.date_range("2025-01-01", periods=n, freq="B")
        base = np.random.randn(n)
        s1 = pd.Series(base, index=idx)
        # Shift by 3 periods
        s2 = pd.Series(np.roll(base, 3), index=idx)
        result = NewsEnergyEngine._cross_correlate(s1, s2)
        # Should detect some lag
        assert abs(result["optimal_lag"]) <= 5


# ---------------------------------------------------------------------------
# Empty result tests
# ---------------------------------------------------------------------------


class TestEmptyResult:
    def test_structure(self) -> None:
        result = NewsEnergyEngine._empty_result("test reason")
        assert result["total_news_energy"] == 0.0
        assert result["summary"] == "test reason"
        assert result["energy_by_source"] == []
        assert result["force_vector"] == []


# ---------------------------------------------------------------------------
# Summary builder tests
# ---------------------------------------------------------------------------


class TestBuildSummary:
    def test_low_energy(self) -> None:
        summary = NewsEnergyEngine._build_summary(
            energy_by_source=[],
            total_energy=1.0,
            coherence={"coherence": 0.5, "dominant_direction": "mixed"},
            force_vector=[],
            regime_signal={"equilibrium": True, "violations": 0, "violating_sources": []},
        )
        assert "LOW" in summary

    def test_elevated_energy(self) -> None:
        summary = NewsEnergyEngine._build_summary(
            energy_by_source=[],
            total_energy=15.0,
            coherence={"coherence": 0.9, "dominant_direction": "increasing"},
            force_vector=[{"feature": "gdelt_tone", "energy": 5.0, "direction_label": "increasing"}],
            regime_signal={"equilibrium": False, "violations": 2,
                           "violating_sources": ["crucix_conflict", "gdelt_tone"]},
        )
        assert "ELEVATED" in summary
        assert "REGIME SHIFT" in summary
        assert "HIGH" in summary  # coherence
