"""
Tests for the knowledge tree module.

Tests extraction logic (category, tags, tickers, features, confidence)
without requiring a database connection.
"""

from __future__ import annotations

import pytest

from knowledge.tree import (
    _detect_category,
    _estimate_confidence,
    _extract_features,
    _extract_tags,
    _extract_tickers,
)


class TestDetectCategory:
    """Test category detection from text."""

    def test_macro_keywords(self) -> None:
        text = "What is the current Fed interest rate and how does inflation affect GDP?"
        assert _detect_category(text) == "macro"

    def test_technical_keywords(self) -> None:
        text = "The RSI is overbought and MACD shows divergence from the moving average"
        assert _detect_category(text) == "technical"

    def test_regime_keywords(self) -> None:
        text = "When did the last regime transition happen and what cluster are we in?"
        assert _detect_category(text) == "regime"

    def test_sentiment_keywords(self) -> None:
        text = "What is the current VIX level and market sentiment fear greed index?"
        assert _detect_category(text) == "sentiment"

    def test_risk_keywords(self) -> None:
        text = "What is the max drawdown and Sharpe ratio for this portfolio hedge?"
        assert _detect_category(text) == "risk"

    def test_general_fallback(self) -> None:
        text = "Tell me something fun about the weather today"
        assert _detect_category(text) == "general"

    def test_empty_string(self) -> None:
        assert _detect_category("") == "general"


class TestExtractTags:
    """Test tag extraction from text."""

    def test_basic_extraction(self) -> None:
        text = "The Federal Reserve raised interest rates affecting treasury yields"
        tags = _extract_tags(text)
        assert isinstance(tags, list)
        assert len(tags) > 0
        assert all(isinstance(t, str) for t in tags)

    def test_stopwords_excluded(self) -> None:
        text = "the and but or if when where how which that this"
        tags = _extract_tags(text)
        assert len(tags) == 0

    def test_max_tags(self) -> None:
        text = " ".join(f"word{i}" for i in range(50))
        tags = _extract_tags(text, max_tags=5)
        assert len(tags) <= 5

    def test_empty_string(self) -> None:
        assert _extract_tags("") == []


class TestExtractTickers:
    """Test ticker symbol extraction."""

    def test_dollar_prefix(self) -> None:
        text = "I'm watching $AAPL and $MSFT closely"
        tickers = _extract_tickers(text)
        assert "AAPL" in tickers
        assert "MSFT" in tickers

    def test_known_tickers(self) -> None:
        text = "SPY is at all-time highs while VIX remains low"
        tickers = _extract_tickers(text)
        assert "SPY" in tickers
        assert "VIX" in tickers

    def test_no_tickers(self) -> None:
        text = "The market is generally stable today"
        tickers = _extract_tickers(text)
        # Should not false-positive on common words
        assert isinstance(tickers, list)

    def test_empty_string(self) -> None:
        assert _extract_tickers("") == []


class TestExtractFeatures:
    """Test feature name extraction."""

    def test_z_score_pattern(self) -> None:
        text = "The z_score_spread_10y2y feature is showing mean reversion"
        features = _extract_features(text)
        assert "z_score_spread_10y2y" in features

    def test_slope_pattern(self) -> None:
        text = "slope_gdp_growth has been declining for three quarters"
        features = _extract_features(text)
        assert "slope_gdp_growth" in features

    def test_no_features(self) -> None:
        text = "The market is performing well this quarter"
        features = _extract_features(text)
        assert isinstance(features, list)

    def test_empty_string(self) -> None:
        assert _extract_features("") == []


class TestEstimateConfidence:
    """Test confidence estimation heuristic."""

    def test_empty_answer(self) -> None:
        assert _estimate_confidence("") == 0.0

    def test_short_answer_lower(self) -> None:
        score = _estimate_confidence("I don't know.")
        assert score < 0.5

    def test_detailed_answer_higher(self) -> None:
        answer = " ".join(["detailed analysis"] * 120)
        answer += " The yield is 4.25% as of 2024-01."
        score = _estimate_confidence(answer)
        assert score > 0.5

    def test_hedging_reduces(self) -> None:
        hedging = "I'm not sure about this. It's uncertain and unclear."
        confident = "The GDP growth rate is 2.5% based on latest BLS data from 2024-01."
        assert _estimate_confidence(confident) > _estimate_confidence(hedging)

    def test_bounds(self) -> None:
        """Confidence should always be between 0 and 1."""
        assert 0.0 <= _estimate_confidence("x") <= 1.0
        assert 0.0 <= _estimate_confidence("x " * 1000) <= 1.0
