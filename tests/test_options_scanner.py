"""Tests for discovery/options_scanner.py — OptionsScanner and MispricingOpportunity.

All tests are pure unit tests with no database dependency.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from discovery.options_scanner import MispricingOpportunity, OptionsScanner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_scanner(mock_engine: MagicMock) -> OptionsScanner:
    """Create an OptionsScanner with a mock engine."""
    return OptionsScanner(db_engine=mock_engine, lookback_days=252)


def _empty_history() -> pd.DataFrame:
    """Return an empty history DataFrame."""
    return pd.DataFrame()


def _make_opportunity(payoff: float = 50.0, **kwargs) -> MispricingOpportunity:
    """Create a MispricingOpportunity with sensible defaults."""
    defaults = dict(
        ticker="TEST",
        scan_date=date(2024, 1, 15),
        score=7.0,
        estimated_payoff_multiple=payoff,
        direction="CALL",
        thesis="Test thesis",
    )
    defaults.update(kwargs)
    return MispricingOpportunity(**defaults)


# ---------------------------------------------------------------------------
# Tests: MispricingOpportunity.is_100x
# ---------------------------------------------------------------------------

class TestMispricingOpportunity:
    """Tests for the MispricingOpportunity dataclass."""

    def test_mispricing_opportunity_is_100x(self) -> None:
        """Payoff >= 100 should flag is_100x as True."""
        opp = _make_opportunity(payoff=100.0)
        assert opp.is_100x is True

        opp2 = _make_opportunity(payoff=500.0)
        assert opp2.is_100x is True

    def test_mispricing_opportunity_not_100x(self) -> None:
        """Payoff < 100 should flag is_100x as False."""
        opp = _make_opportunity(payoff=99.9)
        assert opp.is_100x is False

        opp2 = _make_opportunity(payoff=0.0)
        assert opp2.is_100x is False


# ---------------------------------------------------------------------------
# Tests: _score_pcr
# ---------------------------------------------------------------------------

class TestScorePCR:
    """Tests for OptionsScanner._score_pcr."""

    def test_score_pcr_extreme_high(self, mock_engine: MagicMock) -> None:
        """High put/call ratio (2.0) -> positive score, direction CALL."""
        scanner = _make_scanner(mock_engine)
        current = {"put_call_ratio": 2.0}
        score, direction = scanner._score_pcr(current, _empty_history())
        assert score > 0
        assert direction == "CALL"

    def test_score_pcr_extreme_low(self, mock_engine: MagicMock) -> None:
        """Low put/call ratio (0.3) -> positive score, direction PUT."""
        scanner = _make_scanner(mock_engine)
        current = {"put_call_ratio": 0.3}
        score, direction = scanner._score_pcr(current, _empty_history())
        assert score > 0
        assert direction == "PUT"

    def test_score_pcr_normal(self, mock_engine: MagicMock) -> None:
        """Normal put/call ratio (1.0) -> score 0."""
        scanner = _make_scanner(mock_engine)
        current = {"put_call_ratio": 1.0}
        score, direction = scanner._score_pcr(current, _empty_history())
        assert score == 0


# ---------------------------------------------------------------------------
# Tests: _score_iv_skew
# ---------------------------------------------------------------------------

class TestScoreIVSkew:
    """Tests for OptionsScanner._score_iv_skew."""

    def test_score_iv_skew_extreme(self, mock_engine: MagicMock) -> None:
        """High IV skew (2.0) -> positive score, direction CALL."""
        scanner = _make_scanner(mock_engine)
        current = {"iv_skew": 2.0}
        score, direction = scanner._score_iv_skew(current, _empty_history())
        assert score > 0
        assert direction == "CALL"

    def test_score_iv_skew_collapsed(self, mock_engine: MagicMock) -> None:
        """Collapsed IV skew (0.7) -> positive score, direction PUT."""
        scanner = _make_scanner(mock_engine)
        current = {"iv_skew": 0.7}
        score, direction = scanner._score_iv_skew(current, _empty_history())
        assert score > 0
        assert direction == "PUT"


# ---------------------------------------------------------------------------
# Tests: _score_max_pain_divergence
# ---------------------------------------------------------------------------

class TestScoreMaxPainDivergence:
    """Tests for OptionsScanner._score_max_pain_divergence."""

    def test_score_max_pain_divergence_above(self, mock_engine: MagicMock) -> None:
        """Spot above max pain -> direction PUT (expect pullback)."""
        scanner = _make_scanner(mock_engine)
        current = {"spot_price": 110.0, "max_pain": 100.0}
        score, direction = scanner._score_max_pain_divergence(current)
        assert score > 0
        assert direction == "PUT"

    def test_score_max_pain_divergence_below(self, mock_engine: MagicMock) -> None:
        """Spot below max pain -> direction CALL (expect rally)."""
        scanner = _make_scanner(mock_engine)
        current = {"spot_price": 90.0, "max_pain": 100.0}
        score, direction = scanner._score_max_pain_divergence(current)
        assert score > 0
        assert direction == "CALL"

    def test_score_max_pain_no_divergence(self, mock_engine: MagicMock) -> None:
        """Spot at max pain -> score 0."""
        scanner = _make_scanner(mock_engine)
        current = {"spot_price": 100.0, "max_pain": 100.0}
        score, direction = scanner._score_max_pain_divergence(current)
        assert score == 0


# ---------------------------------------------------------------------------
# Tests: _score_term_structure
# ---------------------------------------------------------------------------

class TestScoreTermStructure:
    """Tests for OptionsScanner._score_term_structure."""

    def test_score_term_structure_inverted(self, mock_engine: MagicMock) -> None:
        """Negative slope (-0.1) -> positive score."""
        scanner = _make_scanner(mock_engine)
        current = {"term_structure_slope": -0.1}
        score, direction = scanner._score_term_structure(current)
        assert score > 0


# ---------------------------------------------------------------------------
# Tests: _score_oi_concentration
# ---------------------------------------------------------------------------

class TestScoreOIConcentration:
    """Tests for OptionsScanner._score_oi_concentration."""

    def test_score_oi_concentration(self, mock_engine: MagicMock) -> None:
        """Concentration of 0.25 (above threshold 0.15) -> positive score."""
        scanner = _make_scanner(mock_engine)
        current = {"oi_concentration": 0.25}
        score = scanner._score_oi_concentration(current)
        assert score > 0


# ---------------------------------------------------------------------------
# Tests: _score_gamma_squeeze
# ---------------------------------------------------------------------------

class TestScoreGammaSqueeze:
    """Tests for OptionsScanner._score_gamma_squeeze."""

    def test_score_gamma_squeeze(self, mock_engine: MagicMock) -> None:
        """High concentration + high divergence + high OI -> positive score."""
        scanner = _make_scanner(mock_engine)
        current = {
            "oi_concentration": 0.20,
            "spot_price": 110.0,
            "max_pain": 100.0,
            "total_oi": 100000,
        }
        score = scanner._score_gamma_squeeze(current)
        assert score > 0


# ---------------------------------------------------------------------------
# Tests: _estimate_payoff_multiple
# ---------------------------------------------------------------------------

class TestEstimatePayoffMultiple:
    """Tests for OptionsScanner._estimate_payoff_multiple."""

    def test_estimate_payoff_multiple(self, mock_engine: MagicMock) -> None:
        """Should return a positive number."""
        scanner = _make_scanner(mock_engine)
        current = {
            "iv_atm": 0.25,
            "spot_price": 100.0,
            "max_pain": 90.0,
        }
        payoff = scanner._estimate_payoff_multiple(current, 7.0, "CALL")
        assert payoff > 0


# ---------------------------------------------------------------------------
# Tests: _build_thesis
# ---------------------------------------------------------------------------

class TestBuildThesis:
    """Tests for OptionsScanner._build_thesis."""

    def test_build_thesis_includes_active_signals(
        self, mock_engine: MagicMock
    ) -> None:
        """Thesis string should be non-empty when active signals exist."""
        scanner = _make_scanner(mock_engine)
        signals = {
            "pcr": {"score": 8, "direction": "CALL", "value": 2.1},
            "iv_skew": {"score": 2, "direction": "", "value": 1.0},
            "max_pain_div": {"score": 0, "direction": "", "value": None},
            "term_structure": {"score": 0, "direction": "", "value": None},
            "oi_concentration": {"score": 0, "value": None},
            "iv_percentile": {"score": 0, "direction": "", "value": None},
            "gamma_squeeze": {"score": 0},
        }
        current = {"spot_price": 100.0, "max_pain": 95.0}
        thesis = scanner._build_thesis("SPY", signals, "CALL", current)
        assert len(thesis) > 0
        assert "SPY" in thesis


# ---------------------------------------------------------------------------
# Tests: format_report
# ---------------------------------------------------------------------------

class TestFormatReport:
    """Tests for OptionsScanner.format_report."""

    def test_format_report_empty(self, mock_engine: MagicMock) -> None:
        """Empty opportunities list produces the 'No mispricing' message."""
        scanner = _make_scanner(mock_engine)
        result = scanner.format_report([])
        assert result == "No mispricing opportunities found."

    def test_format_report_with_opportunities(
        self, mock_engine: MagicMock
    ) -> None:
        """Report should contain the ticker and score for each opportunity."""
        scanner = _make_scanner(mock_engine)
        opp = _make_opportunity(
            payoff=150.0,
            ticker="AAPL",
            score=8.5,
            spot_price=180.0,
            iv_atm=0.30,
        )
        report = scanner.format_report([opp])
        assert "AAPL" in report
        assert "8.5" in report
        assert "150x" in report
