"""Tests for debate protocol enhancements (conviction scoring + round scaling)."""

from unittest.mock import patch

import pytest

from agents.adapter import compute_conviction_score
from agents.config import scale_debate_rounds


class TestConvictionScore:
    """Test compute_conviction_score()."""

    def test_buy_with_bullish_debate_returns_high_score(self):
        parsed = {
            "final_decision": "BUY",
            "bull_bear_debate": {
                "summary": "Very bullish outlook with strong upside opportunity. Long position recommended.",
            },
            "risk_assessment": {"assessment": "Moderate risk, manageable."},
        }
        score = compute_conviction_score(parsed)
        assert score > 0.6

    def test_buy_with_bearish_debate_returns_low_score(self):
        parsed = {
            "final_decision": "BUY",
            "bull_bear_debate": {
                "summary": "Bearish signals dominate. Downside risk is high. Weak fundamentals.",
            },
            "risk_assessment": {},
        }
        score = compute_conviction_score(parsed)
        assert score < 0.5

    def test_hold_returns_neutral(self):
        parsed = {
            "final_decision": "HOLD",
            "bull_bear_debate": {"summary": "Mixed signals, no clear direction."},
            "risk_assessment": {},
        }
        score = compute_conviction_score(parsed)
        assert score == 0.5

    def test_sell_with_bearish_debate_returns_high_score(self):
        parsed = {
            "final_decision": "SELL",
            "bull_bear_debate": {
                "summary": "Strong bearish case. Downside risk elevated. Weak outlook.",
            },
            "risk_assessment": {},
        }
        score = compute_conviction_score(parsed)
        assert score > 0.6

    def test_high_risk_warnings_penalise_score(self):
        parsed = {
            "final_decision": "BUY",
            "bull_bear_debate": {
                "summary": "Bullish with upside opportunity and strong momentum.",
            },
            "risk_assessment": {
                "assessment": "HIGH RISK — EXTREME volatility expected. AVOID overleveraging.",
            },
        }
        score = compute_conviction_score(parsed)
        # Should be penalised by risk flags
        assert score < 0.9

    def test_empty_debate_returns_neutral(self):
        parsed = {
            "final_decision": "BUY",
            "bull_bear_debate": {},
            "risk_assessment": {},
        }
        score = compute_conviction_score(parsed)
        assert score == 0.5

    def test_string_debate_handled(self):
        parsed = {
            "final_decision": "BUY",
            "bull_bear_debate": "Bullish outlook with strong opportunity",
            "risk_assessment": "Low risk",
        }
        score = compute_conviction_score(parsed)
        assert 0.0 <= score <= 1.0

    def test_score_always_in_range(self):
        for decision in ("BUY", "SELL", "HOLD"):
            parsed = {
                "final_decision": decision,
                "bull_bear_debate": {"x": "random text with no keywords"},
                "risk_assessment": {"y": "more text"},
            }
            score = compute_conviction_score(parsed)
            assert 0.0 <= score <= 1.0


class TestScaleDebateRounds:
    """Test debate round scaling based on position size."""

    @patch("agents.config.settings")
    def test_zero_position_returns_min(self, mock_settings):
        mock_settings.AGENTS_MIN_DEBATE_ROUNDS = 1
        mock_settings.AGENTS_MAX_DEBATE_ROUNDS = 5
        mock_settings.AGENTS_DEBATE_SCALE_THRESHOLD = 0.2
        assert scale_debate_rounds(0.0) == 1

    @patch("agents.config.settings")
    def test_full_threshold_returns_max(self, mock_settings):
        mock_settings.AGENTS_MIN_DEBATE_ROUNDS = 1
        mock_settings.AGENTS_MAX_DEBATE_ROUNDS = 5
        mock_settings.AGENTS_DEBATE_SCALE_THRESHOLD = 0.2
        assert scale_debate_rounds(0.2) == 5

    @patch("agents.config.settings")
    def test_half_threshold_returns_midpoint(self, mock_settings):
        mock_settings.AGENTS_MIN_DEBATE_ROUNDS = 1
        mock_settings.AGENTS_MAX_DEBATE_ROUNDS = 5
        mock_settings.AGENTS_DEBATE_SCALE_THRESHOLD = 0.2
        result = scale_debate_rounds(0.1)
        assert 2 <= result <= 4  # midpoint area

    @patch("agents.config.settings")
    def test_over_threshold_caps_at_max(self, mock_settings):
        mock_settings.AGENTS_MIN_DEBATE_ROUNDS = 1
        mock_settings.AGENTS_MAX_DEBATE_ROUNDS = 5
        mock_settings.AGENTS_DEBATE_SCALE_THRESHOLD = 0.2
        assert scale_debate_rounds(0.5) == 5

    @patch("agents.config.settings")
    def test_zero_threshold_returns_min(self, mock_settings):
        mock_settings.AGENTS_MIN_DEBATE_ROUNDS = 1
        mock_settings.AGENTS_MAX_DEBATE_ROUNDS = 5
        mock_settings.AGENTS_DEBATE_SCALE_THRESHOLD = 0.0
        assert scale_debate_rounds(0.1) == 1
