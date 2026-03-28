"""Unit tests for intelligence/prediction_calibration.py PredictionCalibrationChecker."""

from __future__ import annotations

import math
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from intelligence.cross_reference import (
    CrossRefCheck,
    MINOR_DIVERGENCE_THRESHOLD,
    MAJOR_DIVERGENCE_THRESHOLD,
    CONTRADICTION_THRESHOLD,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_engine():
    """Create a mock SQLAlchemy engine that returns configurable rows."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._conn = conn  # expose for test configuration
    return engine


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestAssessDivergence:
    def test_consistent(self):
        from intelligence.prediction_calibration import _assess_divergence

        assert _assess_divergence(0.5) == "consistent"

    def test_minor(self):
        from intelligence.prediction_calibration import _assess_divergence

        assert _assess_divergence(1.5) == "minor_divergence"

    def test_major(self):
        from intelligence.prediction_calibration import _assess_divergence

        assert _assess_divergence(2.5) == "major_divergence"

    def test_contradiction(self):
        from intelligence.prediction_calibration import _assess_divergence

        assert _assess_divergence(3.5) == "contradiction"

    def test_negative_z_score(self):
        from intelligence.prediction_calibration import _assess_divergence

        assert _assess_divergence(-2.5) == "major_divergence"

    def test_exact_thresholds(self):
        from intelligence.prediction_calibration import _assess_divergence

        assert _assess_divergence(MINOR_DIVERGENCE_THRESHOLD) == "minor_divergence"
        assert _assess_divergence(MAJOR_DIVERGENCE_THRESHOLD) == "major_divergence"
        assert _assess_divergence(CONTRADICTION_THRESHOLD) == "contradiction"


class TestComputeZScore:
    def test_basic(self):
        from intelligence.prediction_calibration import _compute_z_score

        assert _compute_z_score(10, 5, 2.5) == 2.0

    def test_zero_std(self):
        from intelligence.prediction_calibration import _compute_z_score

        assert _compute_z_score(10, 5, 0) == 0.0

    def test_none_std(self):
        from intelligence.prediction_calibration import _compute_z_score

        assert _compute_z_score(10, 5, None) == 0.0

    def test_nan_input(self):
        from intelligence.prediction_calibration import _compute_z_score

        assert _compute_z_score(float("nan"), 5, 2.5) == 0.0

    def test_inf_input(self):
        from intelligence.prediction_calibration import _compute_z_score

        assert _compute_z_score(float("inf"), 5, 2.5) == 0.0

    def test_negative_z(self):
        from intelligence.prediction_calibration import _compute_z_score

        assert _compute_z_score(0, 5, 2.5) == -2.0


# ---------------------------------------------------------------------------
# Cross-platform divergence tests
# ---------------------------------------------------------------------------


class TestCrossPlatformDivergence:
    def test_detects_divergence(self, mock_engine):
        """Flags when same event has different prices across platforms."""
        today = date.today()
        mock_engine._conn.execute.return_value.fetchall.return_value = [
            ("pmxt.polymarket.fed_rate_cut.yes", 0.70, today),
            ("pmxt.kalshi.fed_rate_cut.yes", 0.45, today),
        ]

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_cross_platform()

        assert len(checks) >= 1
        check = checks[0]
        assert isinstance(check, CrossRefCheck)
        assert check.category == "prediction_calibration"
        assert "fed_rate_cut" in check.name
        assert check.actual_divergence > 0

    def test_no_divergence_when_prices_close(self, mock_engine):
        """No flags when prices are within tolerance."""
        today = date.today()
        mock_engine._conn.execute.return_value.fetchall.return_value = [
            ("pmxt.polymarket.fed_rate_cut.yes", 0.70, today),
            ("pmxt.kalshi.fed_rate_cut.yes", 0.72, today),
        ]

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_cross_platform()

        # Diff is 0.02, below _MIN_PRICE_DIFF of 0.05
        assert len(checks) == 0

    def test_handles_single_platform(self, mock_engine):
        """No divergence when only one platform has data."""
        today = date.today()
        mock_engine._conn.execute.return_value.fetchall.return_value = [
            ("pmxt.polymarket.fed_rate_cut.yes", 0.70, today),
        ]

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_cross_platform()

        assert len(checks) == 0

    def test_handles_empty_data(self, mock_engine):
        """Returns empty list when no pmxt data exists."""
        mock_engine._conn.execute.return_value.fetchall.return_value = []

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_cross_platform()

        assert checks == []

    def test_skips_nan_values(self, mock_engine):
        """Skips rows with NaN values."""
        today = date.today()
        mock_engine._conn.execute.return_value.fetchall.return_value = [
            ("pmxt.polymarket.fed_rate_cut.yes", float("nan"), today),
            ("pmxt.kalshi.fed_rate_cut.yes", 0.70, today),
        ]

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_cross_platform()

        # Only one valid platform, so no cross-platform comparison
        assert len(checks) == 0

    def test_handles_db_exception(self, mock_engine):
        """Returns empty list on database error."""
        mock_engine._conn.execute.side_effect = Exception("DB error")

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_cross_platform()

        assert checks == []

    def test_multiple_events(self, mock_engine):
        """Handles multiple events with different divergence levels."""
        today = date.today()
        mock_engine._conn.execute.return_value.fetchall.return_value = [
            # Large divergence
            ("pmxt.polymarket.recession.yes", 0.80, today),
            ("pmxt.kalshi.recession.yes", 0.30, today),
            # Small divergence (below threshold)
            ("pmxt.polymarket.oil_spike.yes", 0.50, today),
            ("pmxt.kalshi.oil_spike.yes", 0.52, today),
        ]

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_cross_platform()

        # Only the large divergence should be flagged
        assert len(checks) >= 1
        names = [c.name for c in checks]
        assert any("recession" in n for n in names)


# ---------------------------------------------------------------------------
# Fundamental divergence tests
# ---------------------------------------------------------------------------


class TestFundamentalDivergence:
    def _setup_fetch_calls(self, mock_engine, pmxt_rows, regime_rows):
        """Configure mock engine to return different data for different queries."""
        call_count = [0]
        original_conn = mock_engine._conn

        def side_effect(*args, **kwargs):
            result = MagicMock()
            call_count[0] += 1

            # First call: regime signals (REGIME:%), second call: pmxt data
            if call_count[0] <= 2:
                # Check if this is a regime query or pmxt query based on
                # the params
                if args and hasattr(args[0], 'text'):
                    query_text = str(args[0])
                else:
                    query_text = str(args[0]) if args else ""

                # Return regime rows for first batch, pmxt for second
                if call_count[0] == 1:
                    result.fetchall.return_value = regime_rows
                else:
                    result.fetchall.return_value = pmxt_rows
            else:
                result.fetchall.return_value = pmxt_rows

            return result

        original_conn.execute = MagicMock(side_effect=side_effect)

    def test_detects_fundamental_divergence(self, mock_engine):
        """Flags when prediction markets disagree with GRID regime."""
        today = date.today()
        regime_rows = [
            ("REGIME:recession_prob", 0.60),
        ]
        pmxt_rows = [
            ("pmxt.polymarket.recession_odds.yes", 0.15, today),
        ]

        self._setup_fetch_calls(mock_engine, pmxt_rows, regime_rows)

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_fundamental()

        assert len(checks) >= 1
        check = checks[0]
        assert check.category == "prediction_calibration"
        assert "recession" in check.name

    def test_no_divergence_when_aligned(self, mock_engine):
        """No flags when prediction markets and regime agree."""
        today = date.today()
        regime_rows = [
            ("REGIME:recession_prob", 0.50),
        ]
        pmxt_rows = [
            ("pmxt.polymarket.recession_odds.yes", 0.48, today),
        ]

        self._setup_fetch_calls(mock_engine, pmxt_rows, regime_rows)

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_fundamental()

        # Diff is 0.02, below threshold
        assert len(checks) == 0

    def test_handles_no_regime_signals(self, mock_engine):
        """Returns empty list when no regime signals exist."""
        mock_engine._conn.execute.return_value.fetchall.return_value = []

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_fundamental()

        assert checks == []


# ---------------------------------------------------------------------------
# run_checks integration
# ---------------------------------------------------------------------------


class TestRunChecks:
    def test_combines_all_checks(self, mock_engine):
        """run_checks returns results from both check types."""
        today = date.today()
        # Return cross-platform divergence data
        mock_engine._conn.execute.return_value.fetchall.return_value = [
            ("pmxt.polymarket.fed_rate_cut.yes", 0.80, today),
            ("pmxt.kalshi.fed_rate_cut.yes", 0.30, today),
        ]

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.run_checks()

        assert isinstance(checks, list)
        # Should have at least cross-platform checks
        for check in checks:
            assert isinstance(check, CrossRefCheck)

    def test_handles_partial_failure(self, mock_engine):
        """run_checks continues if one check type fails."""
        mock_engine._conn.execute.side_effect = [
            Exception("cross-platform failed"),
            MagicMock(fetchall=MagicMock(return_value=[])),
        ]

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        # Should not raise
        checks = checker.run_checks()
        assert isinstance(checks, list)

    def test_returns_empty_on_no_data(self, mock_engine):
        """run_checks returns empty list when no data is available."""
        mock_engine._conn.execute.return_value.fetchall.return_value = []

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.run_checks()

        assert checks == []


# ---------------------------------------------------------------------------
# Parse series_id tests
# ---------------------------------------------------------------------------


class TestParseSeriesId:
    def test_valid_series_id(self, mock_engine):
        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        parsed = checker._parse_series_id("pmxt.polymarket.fed_cut.yes")

        assert parsed["platform"] == "polymarket"
        assert parsed["event_slug"] == "fed_cut"
        assert parsed["outcome"] == "yes"

    def test_short_series_id(self, mock_engine):
        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        parsed = checker._parse_series_id("pmxt.polymarket")

        assert parsed["platform"] == ""
        assert parsed["event_slug"] == ""

    def test_series_with_dots_in_slug(self, mock_engine):
        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        parsed = checker._parse_series_id("pmxt.kalshi.fed.rate.cut.yes")

        assert parsed["platform"] == "kalshi"
        assert parsed["event_slug"] == "fed"
        # Everything after the third dot goes into outcome
        assert parsed["outcome"] == "rate.cut.yes"


# ---------------------------------------------------------------------------
# CrossRefCheck dataclass fields
# ---------------------------------------------------------------------------


class TestCrossRefCheckIntegrity:
    def test_check_has_all_required_fields(self, mock_engine):
        """Verify produced checks have all CrossRefCheck fields populated."""
        today = date.today()
        mock_engine._conn.execute.return_value.fetchall.return_value = [
            ("pmxt.polymarket.fed_rate_cut.yes", 0.80, today),
            ("pmxt.kalshi.fed_rate_cut.yes", 0.30, today),
        ]

        from intelligence.prediction_calibration import PredictionCalibrationChecker

        checker = PredictionCalibrationChecker(engine=mock_engine)
        checks = checker.check_cross_platform()

        if checks:
            check = checks[0]
            assert check.name is not None
            assert check.category is not None
            assert check.official_source is not None
            assert check.physical_source is not None
            assert isinstance(check.official_value, float)
            assert isinstance(check.physical_value, float)
            assert isinstance(check.actual_divergence, float)
            assert check.assessment in (
                "consistent", "minor_divergence",
                "major_divergence", "contradiction",
            )
            assert isinstance(check.confidence, float)
            assert 0 <= check.confidence <= 1
            assert check.checked_at is not None
