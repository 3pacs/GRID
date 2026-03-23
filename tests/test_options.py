"""Tests for options ingestion and mispricing scanner."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# ingestion/options.py — signal computation helpers
# ---------------------------------------------------------------------------

from ingestion.options import (
    compute_iv_skew,
    compute_max_pain,
    _compute_atm_iv,
    _compute_wing_iv,
    _compute_oi_concentration,
)


class TestComputeMaxPain:
    """Tests for max pain calculation."""

    def _make_df(self, strikes, ois):
        return pd.DataFrame({"strike": strikes, "openInterest": ois})

    def test_basic_max_pain(self):
        calls = self._make_df([90, 100, 110], [100, 200, 50])
        puts = self._make_df([90, 100, 110], [50, 200, 100])
        result = compute_max_pain(calls, puts, 100.0)
        assert result is not None
        assert isinstance(result, float)

    def test_empty_calls(self):
        calls = pd.DataFrame(columns=["strike", "openInterest"])
        puts = self._make_df([90, 100], [100, 200])
        assert compute_max_pain(calls, puts, 100.0) is None

    def test_empty_puts(self):
        calls = self._make_df([90, 100], [100, 200])
        puts = pd.DataFrame(columns=["strike", "openInterest"])
        assert compute_max_pain(calls, puts, 100.0) is None

    def test_max_pain_is_at_highest_oi(self):
        """Max pain should tend toward the strike with highest combined OI."""
        calls = self._make_df([95, 100, 105], [10, 1000, 10])
        puts = self._make_df([95, 100, 105], [10, 1000, 10])
        result = compute_max_pain(calls, puts, 100.0)
        assert result == 100.0

    def test_nan_oi_handled(self):
        calls = self._make_df([95, 100, 105], [np.nan, 100, 50])
        puts = self._make_df([95, 100, 105], [50, np.nan, 100])
        result = compute_max_pain(calls, puts, 100.0)
        assert result is not None


class TestComputeIVSkew:
    """Tests for IV skew calculation."""

    def test_basic_skew(self):
        puts = pd.DataFrame({
            "strike": [85, 88, 90, 97, 100, 103],
            "impliedVolatility": [0.45, 0.42, 0.40, 0.30, 0.28, 0.29],
        })
        result = compute_iv_skew(puts, spot_price=100.0)
        assert result is not None
        assert result > 1.0  # OTM puts should have higher IV

    def test_empty_puts(self):
        assert compute_iv_skew(pd.DataFrame(), 100.0) is None

    def test_no_iv_column(self):
        puts = pd.DataFrame({"strike": [90, 100]})
        assert compute_iv_skew(puts, 100.0) is None

    def test_no_otm_puts(self):
        """All puts are ATM — no OTM puts to compare."""
        puts = pd.DataFrame({
            "strike": [98, 99, 100, 101],
            "impliedVolatility": [0.30, 0.29, 0.28, 0.30],
        })
        result = compute_iv_skew(puts, spot_price=100.0)
        assert result is None  # No OTM puts in 85-92% range


class TestComputeATMIV:
    """Tests for ATM IV calculation."""

    def test_basic_atm_iv(self):
        calls = pd.DataFrame({
            "strike": [98, 99, 100, 101, 102],
            "impliedVolatility": [0.28, 0.27, 0.26, 0.27, 0.28],
        })
        puts = pd.DataFrame({
            "strike": [98, 99, 100, 101, 102],
            "impliedVolatility": [0.29, 0.28, 0.27, 0.28, 0.29],
        })
        result = _compute_atm_iv(calls, puts, 100.0)
        assert result is not None
        assert 0.25 < result < 0.32

    def test_empty_dataframes(self):
        assert _compute_atm_iv(pd.DataFrame(), pd.DataFrame(), 100.0) is None


class TestOIConcentration:
    """Tests for OI concentration calculation."""

    def test_basic(self):
        calls = pd.DataFrame({"openInterest": [100, 500, 50]})
        puts = pd.DataFrame({"openInterest": [80, 60, 40]})
        result = _compute_oi_concentration(calls, puts, total_oi=830)
        assert result is not None
        assert abs(result - 500 / 830) < 0.01

    def test_zero_total_oi(self):
        calls = pd.DataFrame({"openInterest": [0]})
        puts = pd.DataFrame({"openInterest": [0]})
        assert _compute_oi_concentration(calls, puts, total_oi=0) is None


# ---------------------------------------------------------------------------
# discovery/options_scanner.py — OptionsScanner
# ---------------------------------------------------------------------------

from discovery.options_scanner import MispricingOpportunity, OptionsScanner


class TestMispricingOpportunity:
    """Tests for the MispricingOpportunity dataclass."""

    def test_is_100x_true(self):
        opp = MispricingOpportunity(
            ticker="SPY", scan_date=date.today(), score=8.5,
            estimated_payoff_multiple=150.0, direction="CALL",
            thesis="Test thesis",
        )
        assert opp.is_100x is True

    def test_is_100x_false(self):
        opp = MispricingOpportunity(
            ticker="SPY", scan_date=date.today(), score=5.0,
            estimated_payoff_multiple=50.0, direction="PUT",
            thesis="Test thesis",
        )
        assert opp.is_100x is False

    def test_is_100x_boundary(self):
        opp = MispricingOpportunity(
            ticker="SPY", scan_date=date.today(), score=7.0,
            estimated_payoff_multiple=100.0, direction="CALL",
            thesis="Boundary test",
        )
        assert opp.is_100x is True


class TestOptionsScannerScoring:
    """Tests for individual signal scoring methods (no DB required)."""

    def _make_scanner(self):
        """Create scanner with mock engine."""
        engine = MagicMock()
        return OptionsScanner(engine, lookback_days=252)

    def test_pcr_extreme_high(self):
        scanner = self._make_scanner()
        score, direction = scanner._score_pcr(
            {"put_call_ratio": 2.5}, pd.DataFrame()
        )
        assert score >= 5
        assert direction == "CALL"  # Contrarian: extreme put buying → buy calls

    def test_pcr_extreme_low(self):
        scanner = self._make_scanner()
        score, direction = scanner._score_pcr(
            {"put_call_ratio": 0.2}, pd.DataFrame()
        )
        assert score >= 5
        assert direction == "PUT"  # Extreme complacency → buy puts

    def test_pcr_normal(self):
        scanner = self._make_scanner()
        score, _ = scanner._score_pcr(
            {"put_call_ratio": 0.9}, pd.DataFrame()
        )
        assert score == 0

    def test_pcr_none(self):
        scanner = self._make_scanner()
        score, _ = scanner._score_pcr({"put_call_ratio": None}, pd.DataFrame())
        assert score == 0

    def test_iv_skew_extreme(self):
        scanner = self._make_scanner()
        score, direction = scanner._score_iv_skew(
            {"iv_skew": 1.8}, pd.DataFrame()
        )
        assert score >= 5
        assert direction == "CALL"

    def test_iv_skew_collapsed(self):
        scanner = self._make_scanner()
        score, direction = scanner._score_iv_skew(
            {"iv_skew": 0.7}, pd.DataFrame()
        )
        assert score >= 5
        assert direction == "PUT"

    def test_max_pain_large_divergence(self):
        scanner = self._make_scanner()
        score, direction = scanner._score_max_pain_divergence(
            {"spot_price": 500, "max_pain": 450}
        )
        assert score >= 3
        assert direction == "PUT"  # Spot above max pain → expect pullback

    def test_max_pain_small_divergence(self):
        scanner = self._make_scanner()
        score, _ = scanner._score_max_pain_divergence(
            {"spot_price": 500, "max_pain": 495}
        )
        assert score == 0

    def test_term_structure_inverted(self):
        scanner = self._make_scanner()
        score, _ = scanner._score_term_structure(
            {"term_structure_slope": -0.10, "put_call_ratio": 1.2}
        )
        assert score >= 3

    def test_term_structure_normal(self):
        scanner = self._make_scanner()
        score, _ = scanner._score_term_structure(
            {"term_structure_slope": 0.05}
        )
        assert score == 0

    def test_oi_concentration_high(self):
        scanner = self._make_scanner()
        score = scanner._score_oi_concentration(
            {"oi_concentration": 0.25}
        )
        assert score >= 3

    def test_oi_concentration_normal(self):
        scanner = self._make_scanner()
        score = scanner._score_oi_concentration(
            {"oi_concentration": 0.05}
        )
        assert score == 0

    def test_iv_percentile_cheap(self):
        scanner = self._make_scanner()
        history = pd.DataFrame({
            "iv_atm": np.linspace(0.15, 0.50, 100)
        })
        # Current IV is very low
        score, direction = scanner._score_iv_percentile(
            {"iv_atm": 0.12}, history
        )
        assert score >= 5
        assert direction == "CALL"  # Cheap vol → buy options

    def test_iv_percentile_rich(self):
        scanner = self._make_scanner()
        history = pd.DataFrame({
            "iv_atm": np.linspace(0.15, 0.50, 100)
        })
        score, direction = scanner._score_iv_percentile(
            {"iv_atm": 0.55}, history
        )
        assert score >= 3

    def test_gamma_squeeze_potential(self):
        scanner = self._make_scanner()
        score = scanner._score_gamma_squeeze({
            "oi_concentration": 0.20,
            "spot_price": 500,
            "max_pain": 460,
            "total_oi": 100000,
        })
        assert score >= 3

    def test_gamma_squeeze_no_setup(self):
        scanner = self._make_scanner()
        score = scanner._score_gamma_squeeze({
            "oi_concentration": 0.05,
            "spot_price": 500,
            "max_pain": 498,
            "total_oi": 10000,
        })
        assert score == 0


class TestPayoffEstimation:
    """Tests for payoff multiple estimation."""

    def _make_scanner(self):
        engine = MagicMock()
        return OptionsScanner(engine)

    def test_low_iv_high_score_gives_high_payoff(self):
        scanner = self._make_scanner()
        payoff = scanner._estimate_payoff_multiple(
            {"iv_atm": 0.12, "spot_price": 500, "max_pain": 450},
            composite_score=8.0,
            direction="CALL",
        )
        assert payoff >= 50  # Low IV + high score = big potential

    def test_high_iv_low_score_gives_low_payoff(self):
        scanner = self._make_scanner()
        payoff = scanner._estimate_payoff_multiple(
            {"iv_atm": 0.60, "spot_price": 500, "max_pain": 495},
            composite_score=3.0,
            direction="PUT",
        )
        assert payoff < 50  # High IV + low score = limited potential

    def test_payoff_capped_at_1000(self):
        scanner = self._make_scanner()
        payoff = scanner._estimate_payoff_multiple(
            {"iv_atm": 0.01, "spot_price": 500, "max_pain": 300},
            composite_score=10.0,
            direction="CALL",
        )
        assert payoff <= 1000

    def test_missing_iv_fallback(self):
        scanner = self._make_scanner()
        payoff = scanner._estimate_payoff_multiple(
            {"iv_atm": None, "spot_price": 500},
            composite_score=7.0,
            direction="CALL",
        )
        assert payoff > 0


class TestFormatReport:
    """Tests for report formatting."""

    def _make_scanner(self):
        engine = MagicMock()
        return OptionsScanner(engine)

    def test_empty_report(self):
        scanner = self._make_scanner()
        assert scanner.format_report([]) == "No mispricing opportunities found."

    def test_report_with_opportunities(self):
        scanner = self._make_scanner()
        opps = [
            MispricingOpportunity(
                ticker="SPY", scan_date=date(2026, 3, 23), score=8.5,
                estimated_payoff_multiple=150.0, direction="CALL",
                thesis="Test thesis", confidence="HIGH",
                spot_price=500.0, iv_atm=0.15,
                strikes=[550, 575, 600],
            ),
        ]
        report = scanner.format_report(opps)
        assert "SPY" in report
        assert "100x+" in report
        assert "150x" in report
        assert "HIGH" in report


class TestTargetStrikes:
    """Tests for target strike selection."""

    def _make_scanner(self):
        engine = MagicMock()
        return OptionsScanner(engine)

    def test_call_strikes_above_spot(self):
        scanner = self._make_scanner()
        strikes = scanner._get_target_strikes(
            "SPY", date.today(), "CALL",
            {"spot_price": 500},
        )
        assert len(strikes) == 3
        assert all(s > 500 for s in strikes)

    def test_put_strikes_below_spot(self):
        scanner = self._make_scanner()
        strikes = scanner._get_target_strikes(
            "SPY", date.today(), "PUT",
            {"spot_price": 500},
        )
        assert len(strikes) == 3
        assert all(s < 500 for s in strikes)

    def test_no_spot_returns_empty(self):
        scanner = self._make_scanner()
        strikes = scanner._get_target_strikes(
            "SPY", date.today(), "CALL",
            {"spot_price": 0},
        )
        assert strikes == []
