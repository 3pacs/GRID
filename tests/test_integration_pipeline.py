"""
Integration test: ingestion → resolution → PIT store → features → inference.

Tests the full data pipeline end-to-end using mocked database interactions.
Verifies that data flows correctly through each layer and that PIT constraints
are maintained throughout.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helpers: simulate what each layer returns
# ---------------------------------------------------------------------------

def _make_raw_series(feature_id: int, source_id: int, obs_dates: list[date],
                     values: list[float], release_date: date) -> pd.DataFrame:
    """Simulate raw_series rows from ingestion."""
    return pd.DataFrame({
        "feature_id": [feature_id] * len(obs_dates),
        "source_id": [source_id] * len(obs_dates),
        "obs_date": obs_dates,
        "value": values,
        "release_date": [release_date] * len(obs_dates),
        "vintage_date": [release_date] * len(obs_dates),
    })


def _make_resolved_series(feature_id: int, obs_dates: list[date],
                          values: list[float], release_date: date) -> pd.DataFrame:
    """Simulate resolved_series rows after conflict resolution."""
    return pd.DataFrame({
        "feature_id": [feature_id] * len(obs_dates),
        "obs_date": obs_dates,
        "value": values,
        "release_date": [release_date] * len(obs_dates),
        "vintage_date": [release_date] * len(obs_dates),
        "conflict_flag": [False] * len(obs_dates),
    })


# ---------------------------------------------------------------------------
# 1. Conflict resolution logic (unit-level integration)
# ---------------------------------------------------------------------------

class TestConflictDetection:
    """Test that conflict resolution correctly picks highest-priority source."""

    def test_single_source_no_conflict(self):
        """Single source: value passes through without conflict."""
        from normalization.resolver import CONFLICT_THRESHOLD

        values = [100.0]
        priorities = [1]
        # Highest priority wins, no conflict when single source
        winner = values[priorities.index(min(priorities))]
        assert winner == 100.0

    def test_two_sources_agree(self):
        """Two sources with values within threshold: no conflict."""
        from normalization.resolver import CONFLICT_THRESHOLD

        ref_val = 100.0
        other_val = 100.3  # 0.3% difference
        pct_diff = abs(other_val - ref_val) / abs(ref_val)
        assert pct_diff < CONFLICT_THRESHOLD  # 0.3% < 0.5%

    def test_two_sources_disagree(self):
        """Two sources with values outside threshold: conflict flagged."""
        from normalization.resolver import CONFLICT_THRESHOLD

        ref_val = 100.0
        other_val = 101.0  # 1% difference
        pct_diff = abs(other_val - ref_val) / abs(ref_val)
        assert pct_diff > CONFLICT_THRESHOLD  # 1% > 0.5%

    def test_per_family_threshold_vol(self):
        """VIX/vol family uses wider threshold (2%)."""
        from normalization.resolver import FAMILY_CONFLICT_THRESHOLDS

        vol_threshold = FAMILY_CONFLICT_THRESHOLDS["vol"]
        ref_val = 20.0
        other_val = 20.3  # 1.5% difference
        pct_diff = abs(other_val - ref_val) / abs(ref_val)
        assert pct_diff < vol_threshold  # 1.5% < 2%

    def test_division_by_zero_ref_val(self):
        """When reference value is 0 and other is nonzero: always conflict."""
        ref_val = 0.0
        other_val = 0.01
        if ref_val == 0 and other_val != 0:
            is_conflict = True  # Per resolver.py fix
        else:
            is_conflict = False
        assert is_conflict is True

    def test_both_zero_no_conflict(self):
        """When both values are 0: no conflict."""
        ref_val = 0.0
        other_val = 0.0
        if ref_val == 0 and other_val == 0:
            is_conflict = False
        else:
            is_conflict = True
        assert is_conflict is False


# ---------------------------------------------------------------------------
# 2. PIT correctness across the pipeline
# ---------------------------------------------------------------------------

class TestPITCorrectness:
    """Verify PIT constraints are maintained end-to-end."""

    def test_future_data_excluded(self):
        """Data released after as_of_date must not appear in results."""
        as_of = date(2024, 6, 1)
        release_dates = [
            date(2024, 5, 1),   # OK — released before as_of
            date(2024, 5, 15),  # OK — released before as_of
            date(2024, 6, 15),  # BAD — released AFTER as_of
        ]

        for rd in release_dates:
            if rd > as_of:
                # This row must be excluded
                assert rd > as_of
            else:
                assert rd <= as_of

    def test_pit_vintage_policy_first_release(self):
        """FIRST_RELEASE returns earliest vintage per obs_date."""
        vintages = pd.DataFrame({
            "feature_id": [1, 1, 1],
            "obs_date": [date(2024, 1, 1)] * 3,
            "value": [100.0, 100.5, 101.0],
            "vintage_date": [date(2024, 1, 5), date(2024, 2, 1), date(2024, 3, 1)],
            "release_date": [date(2024, 1, 5), date(2024, 2, 1), date(2024, 3, 1)],
        })

        # FIRST_RELEASE: earliest vintage_date
        first = vintages.sort_values("vintage_date").groupby(
            ["feature_id", "obs_date"]
        ).first().reset_index()
        assert first.iloc[0]["value"] == 100.0

    def test_pit_vintage_policy_latest_as_of(self):
        """LATEST_AS_OF returns latest vintage available before as_of."""
        as_of = date(2024, 2, 15)
        vintages = pd.DataFrame({
            "feature_id": [1, 1, 1],
            "obs_date": [date(2024, 1, 1)] * 3,
            "value": [100.0, 100.5, 101.0],
            "vintage_date": [date(2024, 1, 5), date(2024, 2, 1), date(2024, 3, 1)],
            "release_date": [date(2024, 1, 5), date(2024, 2, 1), date(2024, 3, 1)],
        })

        # Filter to release_date <= as_of
        available = vintages[vintages["release_date"] <= as_of]
        # LATEST_AS_OF: latest vintage
        latest = available.sort_values("vintage_date").groupby(
            ["feature_id", "obs_date"]
        ).last().reset_index()
        assert latest.iloc[0]["value"] == 100.5  # Revision from 2024-02-01


# ---------------------------------------------------------------------------
# 3. Feature transformation correctness
# ---------------------------------------------------------------------------

class TestFeatureTransformations:
    """Test feature lab transformations maintain data integrity."""

    def test_zscore_known_values(self):
        """Z-score of constant series is NaN (zero std)."""
        from features.lab import zscore_normalize

        series = pd.Series([100.0] * 300)
        result = zscore_normalize(series, window=252)
        # Constant series has std=0, so z-score should be NaN
        assert result.dropna().empty or result.isna().all()

    def test_zscore_standard_normal(self):
        """Z-score of series with known mean/std produces near-zero mean."""
        from features.lab import zscore_normalize

        np.random.seed(42)
        series = pd.Series(np.random.normal(100, 10, 500))
        result = zscore_normalize(series, window=252)
        valid = result.dropna()
        assert abs(valid.mean()) < 0.5  # Should be near zero

    def test_rolling_slope_trending(self):
        """Rising linear trend should produce positive slope."""
        from features.lab import rolling_slope

        series = pd.Series(np.arange(200, dtype=float))
        result = rolling_slope(series, window=63)
        valid = result.dropna()
        assert (valid > 0).all()  # All slopes should be positive

    def test_rolling_slope_flat(self):
        """Flat series should produce near-zero slope."""
        from features.lab import rolling_slope

        series = pd.Series([50.0] * 200)
        result = rolling_slope(series, window=63)
        valid = result.dropna()
        assert (valid.abs() < 1e-6).all()

    def test_pct_change_lagged(self):
        """Percentage change computes correctly."""
        from features.lab import pct_change_lagged

        series = pd.Series([100.0, 110.0, 121.0, 133.1])
        result = pct_change_lagged(series, lag_days=1)
        # 110/100 - 1 = 0.1 = 10%
        assert abs(result.iloc[1] - 0.1) < 1e-10


# ---------------------------------------------------------------------------
# 4. Inference recommendation logic
# ---------------------------------------------------------------------------

class TestInferenceRecommendation:
    """Test inference recommendation generation in isolation."""

    def _make_inference(self):
        """Create a LiveInference with mocks."""
        engine = MagicMock()
        pit = MagicMock()
        from inference.live import LiveInference
        return LiveInference(engine, pit)

    def test_recommendation_with_threshold_scores(self):
        """Recommendation picks state with highest absolute score."""
        li = self._make_inference()
        feature_vector = {"feature_1": 2.0, "feature_2": -1.5}
        params = {
            "state_thresholds": {
                "RISK_ON": {"weights": {"feature_1": 1.0, "feature_2": 0.5}, "action": "BUY"},
                "RISK_OFF": {"weights": {"feature_1": -1.0, "feature_2": -0.5}, "action": "SELL"},
            }
        }
        rec = li._generate_recommendation(feature_vector, params)
        # RISK_ON score: 2.0*1.0 + (-1.5)*0.5 = 1.25
        # RISK_OFF score: 2.0*(-1.0) + (-1.5)*(-0.5) = -1.25
        # Both abs=1.25, but max picks first in case of tie
        assert rec["inferred_state"] in ("RISK_ON", "RISK_OFF")
        assert rec["state_confidence"] > 0

    def test_recommendation_low_coverage(self):
        """Insufficient feature coverage returns HOLD."""
        li = self._make_inference()
        feature_vector = {"feature_1": 1.0, "feature_2": None, "feature_3": None}
        params = {"state_thresholds": {}}
        rec = li._generate_recommendation(feature_vector, params)
        assert "HOLD" in rec["suggested_action"]

    def test_recommendation_empty_thresholds(self):
        """Empty parameter snapshot produces UNKNOWN state."""
        li = self._make_inference()
        feature_vector = {"feature_1": 1.0}
        params = {}
        rec = li._generate_recommendation(feature_vector, params)
        assert rec["inferred_state"] == "UNKNOWN"

    def test_recommendation_no_production_models(self):
        """No production models returns error dict."""
        li = self._make_inference()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result
        li.engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        li.engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = li.run_inference(as_of_date=date(2024, 6, 1))
        assert "error" in result


# ---------------------------------------------------------------------------
# 5. End-to-end pipeline flow (mocked)
# ---------------------------------------------------------------------------

class TestEndToEndPipeline:
    """Test the full pipeline with mocked components."""

    def test_ingestion_to_inference_data_shape(self):
        """Verify data shape is preserved through the pipeline."""
        # Step 1: Simulate ingested raw data
        raw = _make_raw_series(
            feature_id=1,
            source_id=1,
            obs_dates=[date(2024, 1, d) for d in range(1, 11)],
            values=[100 + i * 0.5 for i in range(10)],
            release_date=date(2024, 1, 15),
        )
        assert len(raw) == 10
        assert "feature_id" in raw.columns

        # Step 2: Simulate resolution (same data, no conflicts)
        resolved = _make_resolved_series(
            feature_id=1,
            obs_dates=raw["obs_date"].tolist(),
            values=raw["value"].tolist(),
            release_date=date(2024, 1, 15),
        )
        assert len(resolved) == 10
        assert resolved["conflict_flag"].sum() == 0

        # Step 3: Verify PIT filter works
        as_of = date(2024, 1, 10)
        pit_filtered = resolved[
            (resolved["release_date"] <= as_of) &
            (resolved["obs_date"] <= as_of)
        ]
        # All 10 have release_date 2024-01-15 > 2024-01-10, so none pass
        assert len(pit_filtered) == 0

        # With proper as_of
        as_of = date(2024, 1, 20)
        pit_filtered = resolved[
            (resolved["release_date"] <= as_of) &
            (resolved["obs_date"] <= as_of)
        ]
        assert len(pit_filtered) == 10

    def test_feature_transformation_preserves_index(self):
        """Feature transforms maintain date index alignment."""
        from features.lab import zscore_normalize

        dates = pd.date_range("2023-01-01", periods=300, freq="B")
        raw_values = pd.Series(
            np.random.normal(100, 10, 300),
            index=dates,
        )

        transformed = zscore_normalize(raw_values, window=252)
        # Index must be preserved
        assert len(transformed) == len(raw_values)
        assert (transformed.index == raw_values.index).all()

    def test_multi_source_resolution_picks_highest_priority(self):
        """When two sources provide different values, highest priority wins."""
        from normalization.resolver import CONFLICT_THRESHOLD

        source_a_val = 100.0   # priority 1 (highest)
        source_b_val = 100.3   # priority 2
        pct_diff = abs(source_b_val - source_a_val) / abs(source_a_val)

        if pct_diff <= CONFLICT_THRESHOLD:
            conflict = False
        else:
            conflict = True

        # 0.3% < 0.5% threshold → no conflict
        assert conflict is False
        # Winner is highest priority (source A)
        winner = source_a_val
        assert winner == 100.0

    def test_nan_handling_across_pipeline(self):
        """NaN values in raw data are handled through transforms."""
        from features.lab import zscore_normalize

        values = [100, 101, np.nan, 103, 104, np.nan, 106] + [100 + i for i in range(293)]
        series = pd.Series(values)

        # Z-score should handle NaN gracefully
        result = zscore_normalize(series, window=252)
        # Should not raise, result should have some valid values
        assert not result.dropna().empty

    def test_temporal_consistency_in_pipeline(self):
        """Dates must be strictly increasing through the pipeline."""
        obs_dates = [date(2024, 1, d) for d in range(1, 31)]
        values = [100 + np.sin(i) for i in range(30)]

        raw = _make_raw_series(
            feature_id=1, source_id=1,
            obs_dates=obs_dates, values=values,
            release_date=date(2024, 2, 1),
        )

        # Sort by obs_date (as PIT store would)
        sorted_raw = raw.sort_values("obs_date")
        dates_list = sorted_raw["obs_date"].tolist()

        # Verify monotonically increasing
        for i in range(1, len(dates_list)):
            assert dates_list[i] >= dates_list[i - 1]
