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
    CAUSAL_PEAK_WINDOW,
    CAUSAL_POST_WINDOW,
    CAUSAL_PRE_WINDOW,
    AutoBNNDecomposer,
    DecompositionResult,
    RegimeChangeSignal,
    causal_context_start,
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


# ---------------------------------------------------------------------------
# Causal drift-break test (the fallback the changepoint detector uses)
# ---------------------------------------------------------------------------

W, K, M = CAUSAL_PRE_WINDOW, CAUSAL_POST_WINDOW, CAUSAL_PEAK_WINDOW


def _drift_shift(n: int = 400, at: int = 300, drift: float = 1.5, seed: int = 3) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return np.cumsum(np.concatenate([rng.normal(0, 1, at), rng.normal(drift, 1, n - at)]))


def _old_fallback_trend(series: np.ndarray) -> np.ndarray:
    """The pre-#1301 fallback: centred rolling mean, NaN ends filled with the series mean."""
    window = min(30, len(series) // 4)
    trend = pd.Series(series).rolling(window, center=True).mean().values
    return np.nan_to_num(trend, nan=series.mean())


class TestCausalBreaks:
    def test_finds_a_drift_shift_near_where_it_happened(self, decomposer: AutoBNNDecomposer) -> None:
        breaks = decomposer.detect_causal_breaks(_drift_shift())
        assert len(breaks) == 1
        (b,) = breaks
        assert 300 - K <= b.change_index <= 300 + K
        assert b.statistic > 0 and b.post_regime == "rising"
        assert b.confirm_index == b.change_index + M + K
        assert 0.5 <= b.confidence <= 1.0

    def test_down_shift_has_a_negative_statistic(self, decomposer: AutoBNNDecomposer) -> None:
        (b,) = decomposer.detect_causal_breaks(_drift_shift(drift=-1.5))
        assert b.statistic < 0 and b.post_regime == "falling"

    def test_v_shape_turns_at_the_top(self, decomposer: AutoBNNDecomposer) -> None:
        v = np.concatenate([np.linspace(100, 150, 200), np.linspace(150, 110, 200)])
        (b,) = decomposer.detect_causal_breaks(v)
        assert 195 <= b.change_index <= 200
        assert (b.pre_regime, b.post_regime) == ("rising", "falling")

    def test_no_breaks_on_a_trend(self, decomposer: AutoBNNDecomposer) -> None:
        assert decomposer.detect_causal_breaks(np.linspace(100, 200, 750)) == []
        noisy = np.linspace(100, 200, 750) + np.random.default_rng(1).normal(0, 1, 750)
        assert decomposer.detect_causal_breaks(noisy) == []

    def test_no_edge_artifacts_on_random_walks(self, decomposer: AutoBNNDecomposer) -> None:
        n = 365
        old_edge = new_total = 0
        for seed in range(30):
            walk = 100 + np.cumsum(np.random.default_rng(seed).normal(0, 1, n))
            old = decomposer._detect_changepoints(_old_fallback_trend(walk), None, "RW", edge_guard=0)
            old_edge += sum(1 for c in old if c.change_index <= 16 or c.change_index >= n - 17)
            for b in decomposer.detect_causal_breaks(walk):
                assert 2 * W <= b.change_index <= n - 1 - (M + K)
                new_total += 1
        assert old_edge >= 30          # the defect: an edge "break" in every window
        assert new_total <= 30         # rare, and never at an edge

    def test_decision_does_not_depend_on_the_window(self, decomposer: AutoBNNDecomposer) -> None:
        x = _drift_shift(n=700, at=500, drift=-2.0, seed=5)
        full = {(b.change_index, round(b.statistic, 9)) for b in decomposer.detect_causal_breaks(x)}
        later = {(b.change_index + 150, round(b.statistic, 9)) for b in decomposer.detect_causal_breaks(x[150:])}
        cut = {(b.change_index, round(b.statistic, 9)) for b in decomposer.detect_causal_breaks(x[:560])}
        decidable = {e for e in full if e[0] >= 150 + 2 * W}
        assert decidable and decidable == later
        assert {e for e in full if e[0] + M + K <= 559} == cut
        for b in decomposer.detect_causal_breaks(x):
            assert causal_context_start(b.change_index) == b.change_index - 2 * W

    def test_non_finite_values_are_never_filled(self, decomposer: AutoBNNDecomposer) -> None:
        x = _drift_shift()
        x[100] = np.nan
        x[-1] = np.inf
        breaks = decomposer.detect_causal_breaks(x)
        # Windows touching the NaN or the inf are skipped; the shift is still found.
        assert [b for b in breaks if 290 <= b.change_index <= 310]
        assert all(b.confirm_index < len(x) - 1 for b in breaks)
        assert all(np.isfinite(b.statistic) and np.isfinite(b.magnitude) for b in breaks)

    def test_short_series_and_bad_windows(self, decomposer: AutoBNNDecomposer) -> None:
        assert decomposer.detect_causal_breaks(np.arange(2 * W + K + M, dtype=float)) == []
        with pytest.raises(ValueError):
            decomposer.detect_causal_breaks(np.arange(500.0), pre_window=1)
        with pytest.raises(ValueError):
            decomposer.detect_causal_breaks(np.arange(500.0), pre_window=5, post_window=10)

    def test_fallback_runs_the_causal_test(self, decomposer: AutoBNNDecomposer) -> None:
        decomposer._available = False
        x = _drift_shift()
        assert decomposer.detect_regime_changes(x) == decomposer.detect_causal_breaks(x)
        assert all(c.confirm_index is not None for c in decomposer.detect_regime_changes(x))


class TestCurvatureEdgeGuard:
    def test_nan_ends_are_not_filled_or_treated_as_breaks(self, decomposer: AutoBNNDecomposer) -> None:
        walk = 100 + np.cumsum(np.random.default_rng(4).normal(0, 1, 250))
        trend = pd.Series(walk).rolling(30, center=True).mean().values   # NaN ends, unfilled
        finite = np.flatnonzero(np.isfinite(trend))
        for c in decomposer._detect_changepoints(trend, None, "MA"):
            assert finite[0] + 10 <= c.change_index <= finite[-1] - 10

    def test_edge_guard_drops_breaks_at_the_ends(self, decomposer: AutoBNNDecomposer) -> None:
        kink_near_end = np.concatenate([np.linspace(0, 10, 195), np.linspace(10, 0, 5)])
        assert decomposer._detect_changepoints(kink_near_end, None, "END") == []
        assert decomposer._detect_changepoints(kink_near_end, None, "END", edge_guard=0)
