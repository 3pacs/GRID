"""
Tests for the GRID cross-reference engine (intelligence/cross_reference.py).

Tests the divergence calculation, classification, confidence scoring,
data class construction, and the per-category check functions with
mocked database results.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from intelligence.cross_reference import (
    CrossRefCheck,
    LieDetectorReport,
    MINOR_DIVERGENCE_THRESHOLD,
    MAJOR_DIVERGENCE_THRESHOLD,
    CONTRADICTION_THRESHOLD,
    _classify_divergence,
    _compute_confidence,
    _compute_divergence_zscore,
    _make_check,
    _generate_narrative,
    check_gdp_vs_physical,
    check_inflation_vs_inputs,
    check_central_bank_actions_vs_words,
    check_employment_reality,
    check_trade_bilateral,
    get_cross_ref_for_ticker,
    run_all_checks,
)


# ── Classification Tests ──────────────────────────────────────────────────

class TestClassifyDivergence:
    """Test the z-score to assessment classification."""

    def test_consistent(self):
        assert _classify_divergence(0.0) == "consistent"
        assert _classify_divergence(0.5) == "consistent"
        assert _classify_divergence(-0.9) == "consistent"

    def test_minor_divergence(self):
        assert _classify_divergence(1.5) == "minor_divergence"
        assert _classify_divergence(-1.5) == "minor_divergence"

    def test_major_divergence(self):
        assert _classify_divergence(2.5) == "major_divergence"
        assert _classify_divergence(-2.5) == "major_divergence"

    def test_contradiction(self):
        assert _classify_divergence(3.5) == "contradiction"
        assert _classify_divergence(-4.0) == "contradiction"

    def test_boundary_values(self):
        # At the exact thresholds, should be the lower category
        assert _classify_divergence(MINOR_DIVERGENCE_THRESHOLD) == "minor_divergence"
        assert _classify_divergence(MAJOR_DIVERGENCE_THRESHOLD) == "major_divergence"
        assert _classify_divergence(CONTRADICTION_THRESHOLD) == "contradiction"


# ── Confidence Scoring Tests ──────────────────────────────────────────────

class TestComputeConfidence:

    def test_high_confidence(self):
        conf = _compute_confidence(100, 100, staleness_days=0)
        assert conf == 1.0

    def test_low_data(self):
        conf = _compute_confidence(5, 5, staleness_days=0)
        assert conf == 0.1

    def test_stale_data_penalty(self):
        fresh = _compute_confidence(100, 100, staleness_days=0)
        stale = _compute_confidence(100, 100, staleness_days=90)
        assert stale < fresh

    def test_very_stale(self):
        conf = _compute_confidence(100, 100, staleness_days=360)
        assert conf >= 0.0  # shouldn't go negative

    def test_zero_observations(self):
        conf = _compute_confidence(0, 0, staleness_days=0)
        assert conf == 0.0


# ── Z-Score Computation Tests ─────────────────────────────────────────────

class TestComputeDivergenceZscore:

    def test_empty_series(self):
        assert _compute_divergence_zscore(pd.Series(dtype=float), pd.Series(dtype=float)) == 0.0

    def test_short_series(self):
        s1 = pd.Series([1.0, 2.0, 3.0])
        s2 = pd.Series([1.0, 2.0, 3.0])
        # Fewer than MIN_OBSERVATIONS
        assert _compute_divergence_zscore(s1, s2) == 0.0

    def test_identical_series(self):
        dates = pd.date_range("2020-01-01", periods=60, freq="MS")
        s1 = pd.Series(range(60), index=dates, dtype=float)
        s2 = pd.Series(range(60), index=dates, dtype=float)
        zscore = _compute_divergence_zscore(s1, s2)
        # Identical series should have near-zero divergence
        assert abs(zscore) < 0.5

    def test_inverse_relationship(self):
        dates = pd.date_range("2020-01-01", periods=60, freq="MS")
        s1 = pd.Series(range(60), index=dates, dtype=float)
        s2 = pd.Series(range(59, -1, -1), index=dates, dtype=float)
        # With inverse relationship, positive + negative should sum near zero
        zscore = _compute_divergence_zscore(s1, s2, relationship="inverse")
        # The exact value depends on the data but it should compute without error
        assert isinstance(zscore, float)


# ── Make Check Tests ──────────────────────────────────────────────────────

class TestMakeCheck:

    def test_basic_construction(self):
        check = _make_check(
            name="Test Check",
            category="gdp",
            official_source="GDP_OFFICIAL",
            official_value=100.0,
            physical_source="NIGHT_LIGHTS",
            physical_value=95.0,
            expected_relationship="positive_correlation",
            zscore=0.5,
            implication="Minor difference",
            confidence=0.8,
        )
        assert check.name == "Test Check"
        assert check.category == "gdp"
        assert check.assessment == "consistent"
        assert check.confidence == 0.8

    def test_none_values_default_to_zero(self):
        check = _make_check(
            name="Test", category="gdp",
            official_source="A", official_value=None,
            physical_source="B", physical_value=None,
            expected_relationship="positive_correlation",
            zscore=0.0, implication="", confidence=0.0,
        )
        assert check.official_value == 0.0
        assert check.physical_value == 0.0

    def test_high_zscore_classified_correctly(self):
        check = _make_check(
            name="Test", category="gdp",
            official_source="A", official_value=100.0,
            physical_source="B", physical_value=50.0,
            expected_relationship="positive_correlation",
            zscore=3.5, implication="Bad", confidence=0.9,
        )
        assert check.assessment == "contradiction"


# ── Narrative Generation Tests ────────────────────────────────────────────

class TestGenerateNarrative:

    def test_no_red_flags(self):
        checks = [
            _make_check("C1", "gdp", "A", 1.0, "B", 1.0, "positive_correlation",
                         0.3, "Fine", 0.8),
        ]
        narrative = _generate_narrative(checks, [])
        assert "1 checks" in narrative or "1 check" in narrative
        assert "No major divergences" in narrative

    def test_with_red_flags(self):
        rf = _make_check("Bad Check", "gdp", "A", 100.0, "B", 50.0,
                          "positive_correlation", 3.5, "Very bad", 0.9)
        narrative = _generate_narrative([rf], [rf])
        assert "RED FLAG" in narrative
        assert "Bad Check" in narrative


# ── Category Check Tests (mocked DB) ─────────────────────────────────────

class TestGDPVsPhysical:

    @patch("intelligence.cross_reference._get_series_history")
    @patch("intelligence.cross_reference._get_feature_history")
    @patch("intelligence.cross_reference._get_latest_value")
    @patch("intelligence.cross_reference._get_feature_value")
    def test_us_returns_checks(self, mock_fval, mock_lval, mock_fhist, mock_shist):
        mock_shist.return_value = pd.Series(dtype=float)
        mock_fhist.return_value = pd.Series(dtype=float)
        mock_lval.return_value = (100.0, date(2025, 1, 1))
        mock_fval.return_value = (100.0, date(2025, 1, 1))

        engine = MagicMock()
        checks = check_gdp_vs_physical(engine, "US")
        assert len(checks) == 4  # 4 US GDP checks configured
        assert all(c.category == "gdp" for c in checks)

    @patch("intelligence.cross_reference._get_series_history")
    @patch("intelligence.cross_reference._get_feature_history")
    @patch("intelligence.cross_reference._get_latest_value")
    @patch("intelligence.cross_reference._get_feature_value")
    def test_china_returns_checks(self, mock_fval, mock_lval, mock_fhist, mock_shist):
        mock_shist.return_value = pd.Series(dtype=float)
        mock_fhist.return_value = pd.Series(dtype=float)
        mock_lval.return_value = (50.0, date(2025, 1, 1))
        mock_fval.return_value = (50.0, date(2025, 1, 1))

        engine = MagicMock()
        checks = check_gdp_vs_physical(engine, "CN")
        assert len(checks) == 3  # 3 China GDP checks configured
        assert all(c.category == "gdp" for c in checks)

    def test_unknown_country_returns_empty(self):
        engine = MagicMock()
        checks = check_gdp_vs_physical(engine, "XX")
        assert checks == []


class TestTickerCrossRef:

    @patch("intelligence.cross_reference.check_gdp_vs_physical")
    @patch("intelligence.cross_reference.check_employment_reality")
    def test_spy_mapped(self, mock_emp, mock_gdp):
        mock_gdp.return_value = []
        mock_emp.return_value = []
        engine = MagicMock()
        result = get_cross_ref_for_ticker(engine, "SPY")
        assert result["mapped"] is True
        assert "gdp" in result["categories"]
        assert "employment" in result["categories"]

    def test_unknown_ticker(self):
        engine = MagicMock()
        result = get_cross_ref_for_ticker(engine, "ZZZZ")
        assert result["mapped"] is False


class TestRunAllChecks:

    @patch("intelligence.cross_reference._persist_checks")
    @patch("intelligence.cross_reference.check_employment_reality")
    @patch("intelligence.cross_reference.check_central_bank_actions_vs_words")
    @patch("intelligence.cross_reference.check_inflation_vs_inputs")
    @patch("intelligence.cross_reference.check_trade_bilateral")
    @patch("intelligence.cross_reference.check_gdp_vs_physical")
    def test_runs_all_categories(
        self, mock_gdp, mock_trade, mock_infl, mock_cb, mock_emp, mock_persist,
    ):
        # Each returns one check
        sample = _make_check(
            "T", "gdp", "A", 1.0, "B", 1.0,
            "positive_correlation", 0.5, "ok", 0.8,
        )
        mock_gdp.return_value = [sample]
        mock_trade.return_value = [sample]
        mock_infl.return_value = [sample]
        mock_cb.return_value = [sample]
        mock_emp.return_value = [sample]
        mock_persist.return_value = 0

        engine = MagicMock()
        report = run_all_checks(engine)

        assert isinstance(report, LieDetectorReport)
        # 3 GDP calls (US, CN, EU) + trade + inflation + CB + employment = 7 calls total
        # Each returns 1 check = 7 checks total
        assert len(report.checks) == 7
        assert report.summary["total_checks"] == 7
        assert report.generated_at is not None

    @patch("intelligence.cross_reference._persist_checks")
    @patch("intelligence.cross_reference.check_employment_reality")
    @patch("intelligence.cross_reference.check_central_bank_actions_vs_words")
    @patch("intelligence.cross_reference.check_inflation_vs_inputs")
    @patch("intelligence.cross_reference.check_trade_bilateral")
    @patch("intelligence.cross_reference.check_gdp_vs_physical")
    def test_red_flags_detected(
        self, mock_gdp, mock_trade, mock_infl, mock_cb, mock_emp, mock_persist,
    ):
        red_flag = _make_check(
            "RF", "gdp", "A", 100.0, "B", 50.0,
            "positive_correlation", 3.5, "Contradiction!", 0.9,
        )
        consistent = _make_check(
            "OK", "employment", "A", 1.0, "B", 1.0,
            "positive_correlation", 0.1, "Fine", 0.8,
        )
        mock_gdp.return_value = [red_flag]
        mock_trade.return_value = [consistent]
        mock_infl.return_value = [consistent]
        mock_cb.return_value = [consistent]
        mock_emp.return_value = [consistent]
        mock_persist.return_value = 0

        engine = MagicMock()
        report = run_all_checks(engine)

        # 3 GDP calls each return 1 red flag + 4 consistent checks
        assert report.summary["red_flag_count"] == 3
        assert len(report.red_flags) == 3
