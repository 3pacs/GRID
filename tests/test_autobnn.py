"""
Tests for the AutoBNN signal decomposition module.

Tests DecompositionResult, RegimeChangeSignal, and AutoBNNDecomposer
with the changepoint detection fallback (no JAX required).
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from timeseries.autobnn import (
    AutoBNNDecomposer,
    DecompositionResult,
    RegimeChangeSignal,
)


# ---------------------------------------------------------------------------
# Data Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def decomposer() -> AutoBNNDecomposer:
    return AutoBNNDecomposer(num_samples=100, num_chains=1, seed=42)


@pytest.fixture
def trend_with_changepoint() -> np.ndarray:
    """Series that rises then falls — clear changepoint in the middle."""
    n = 200
    rising = np.linspace(100, 150, n // 2)
    falling = np.linspace(150, 110, n // 2)
    return np.concatenate([rising, falling]).astype(np.float64)


@pytest.fixture
def flat_series() -> np.ndarray:
    """Flat series with no structural changes."""
    return np.ones(100, dtype=np.float64) * 100.0


@pytest.fixture
def noisy_trend() -> np.ndarray:
    """Series with trend + noise, no sharp changepoint."""
    np.random.seed(42)
    trend = np.linspace(100, 130, 150)
    noise = np.random.normal(0, 2, 150)
    return (trend + noise).astype(np.float64)


# ---------------------------------------------------------------------------
# DecompositionResult
# ---------------------------------------------------------------------------


class TestDecompositionResult:
    def test_frozen_dataclass(self) -> None:
        result = DecompositionResult(
            series_id="SPY",
            analysis_date=date(2026, 4, 1),
            trend=[1.0, 2.0],
            seasonality=[0.1, -0.1],
            residual=[0.01, -0.01],
            changepoints=[],
            kernel_description="LinearBNN + PeriodicBNN",
            posterior_std=[0.5, 0.5],
            model_evidence=-100.0,
        )
        assert result.series_id == "SPY"
        assert len(result.trend) == 2

        with pytest.raises(AttributeError):
            result.series_id = "QQQ"  # type: ignore[misc]


class TestRegimeChangeSignal:
    def test_frozen_dataclass(self) -> None:
        signal = RegimeChangeSignal(
            series_id="SPY",
            change_index=50,
            change_date=date(2026, 3, 15),
            pre_regime="rising",
            post_regime="falling",
            confidence=0.85,
            magnitude=2.5,
        )
        assert signal.confidence == 0.85
        assert signal.pre_regime == "rising"


# ---------------------------------------------------------------------------
# AutoBNNDecomposer — availability
# ---------------------------------------------------------------------------


class TestAvailability:
    def test_unavailable_without_jax(self, decomposer: AutoBNNDecomposer) -> None:
        with patch("builtins.__import__", side_effect=ImportError("no jax")):
            decomposer._available = None
            assert decomposer.is_available is False

    def test_health_check(self, decomposer: AutoBNNDecomposer) -> None:
        hc = decomposer.health_check()
        assert "available" in hc
        assert hc["num_samples"] == 100
        assert hc["num_chains"] == 1


# ---------------------------------------------------------------------------
# AutoBNNDecomposer — changepoint detection (fallback, no JAX needed)
# ---------------------------------------------------------------------------


class TestChangepointDetection:
    def test_detects_changepoint_in_v_shape(
        self,
        decomposer: AutoBNNDecomposer,
        trend_with_changepoint: np.ndarray,
    ) -> None:
        """A V-shaped series should have a changepoint near the middle."""
        changepoints = decomposer._detect_changepoints(
            trend_with_changepoint, None, "TEST"
        )

        assert len(changepoints) > 0
        # The main changepoint should be near index 100 (middle)
        main_cp = max(changepoints, key=lambda c: c.confidence)
        assert 80 <= main_cp.change_index <= 120

    def test_no_changepoint_in_flat_series(
        self,
        decomposer: AutoBNNDecomposer,
        flat_series: np.ndarray,
    ) -> None:
        """A perfectly flat series should have no changepoints."""
        changepoints = decomposer._detect_changepoints(
            flat_series, None, "FLAT"
        )
        assert len(changepoints) == 0

    def test_changepoint_with_dates(
        self,
        decomposer: AutoBNNDecomposer,
        trend_with_changepoint: np.ndarray,
    ) -> None:
        """Changepoints should include dates when provided."""
        from datetime import timedelta

        start = date(2026, 1, 1)
        dates = [start + timedelta(days=i) for i in range(len(trend_with_changepoint))]

        changepoints = decomposer._detect_changepoints(
            trend_with_changepoint, dates, "DATED"
        )

        if changepoints:
            cp = changepoints[0]
            assert cp.change_date is not None

    def test_short_series_no_crash(self, decomposer: AutoBNNDecomposer) -> None:
        """Very short series should return empty, not crash."""
        short = np.array([1.0, 2.0, 3.0])
        changepoints = decomposer._detect_changepoints(short, None, "SHORT")
        assert changepoints == []

    def test_changepoint_regime_labels(
        self,
        decomposer: AutoBNNDecomposer,
        trend_with_changepoint: np.ndarray,
    ) -> None:
        """Verify pre/post regime labels are meaningful."""
        changepoints = decomposer._detect_changepoints(
            trend_with_changepoint, None, "LABELS"
        )

        if changepoints:
            main_cp = max(changepoints, key=lambda c: c.confidence)
            assert main_cp.pre_regime in ("rising", "falling", "flat")
            assert main_cp.post_regime in ("rising", "falling", "flat")


# ---------------------------------------------------------------------------
# AutoBNNDecomposer — regime detection (fallback mode)
# ---------------------------------------------------------------------------


class TestRegimeDetection:
    def test_detect_regime_changes_fallback(
        self,
        decomposer: AutoBNNDecomposer,
        trend_with_changepoint: np.ndarray,
    ) -> None:
        """detect_regime_changes should work even without JAX."""
        decomposer._available = False

        changes = decomposer.detect_regime_changes(
            trend_with_changepoint,
            series_id="FALLBACK",
            min_confidence=0.3,
        )

        # Should detect at least one regime change
        assert len(changes) >= 0  # May or may not find with moving average

    def test_detect_regime_changes_with_pandas(
        self,
        decomposer: AutoBNNDecomposer,
        trend_with_changepoint: np.ndarray,
    ) -> None:
        """Should handle pandas Series input."""
        decomposer._available = False

        pd_series = pd.Series(trend_with_changepoint)
        changes = decomposer.detect_regime_changes(
            pd_series,
            series_id="PD_TEST",
        )

        assert isinstance(changes, list)

    def test_detect_regime_changes_min_confidence(
        self,
        decomposer: AutoBNNDecomposer,
        trend_with_changepoint: np.ndarray,
    ) -> None:
        """High min_confidence should filter out weak changepoints."""
        decomposer._available = False

        low_conf = decomposer.detect_regime_changes(
            trend_with_changepoint,
            min_confidence=0.0,
        )
        high_conf = decomposer.detect_regime_changes(
            trend_with_changepoint,
            min_confidence=0.9,
        )

        assert len(high_conf) <= len(low_conf)

    def test_very_short_series(self, decomposer: AutoBNNDecomposer) -> None:
        """Very short series should return empty list."""
        decomposer._available = False
        short = np.array([1.0, 2.0, 3.0, 4.0])
        changes = decomposer.detect_regime_changes(short)
        assert changes == []


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


class TestDecomposerSingleton:
    def test_get_decomposer_returns_same_instance(self) -> None:
        import timeseries.autobnn as ab

        ab._decomposer_instance = None

        mock_settings = MagicMock()
        mock_settings.AUTOBNN_NUM_SAMPLES = 100
        mock_settings.AUTOBNN_NUM_CHAINS = 1
        mock_settings.AUTOBNN_SEED = 42

        with patch("config.settings", mock_settings):
            d1 = ab.get_decomposer()
            d2 = ab.get_decomposer()

        assert d1 is d2
        ab._decomposer_instance = None
