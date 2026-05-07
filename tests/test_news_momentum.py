"""Tests for intelligence.news_momentum — sentiment momentum signals."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from intelligence.news_momentum import (
    MomentumCalculator,
    MomentumSignal,
    SentimentSnapshot,
    SentimentTimeSeries,
    DivergenceDetector,
)


# ── Fixtures ─────────────────────────────────────────────────────────────

def _make_snapshot(
    ticker: str = "AAPL",
    window_hours: int = 24,
    article_count: int = 10,
    bullish: int = 6,
    bearish: int = 2,
    neutral: int = 2,
    weighted_score: float = 0.3,
) -> SentimentSnapshot:
    """Build a test SentimentSnapshot."""
    total = bullish + bearish + neutral
    return SentimentSnapshot(
        ticker=ticker,
        window_hours=window_hours,
        article_count=article_count,
        bullish_count=bullish,
        bearish_count=bearish,
        neutral_count=neutral,
        avg_confidence=0.7,
        sentiment_score=round((bullish - bearish) / total, 4) if total else 0.0,
        weighted_score=weighted_score,
    )


# ── MomentumCalculator Tests ────────────────────────────────────────────

class TestMomentumCalculator:
    """Tests for the MomentumCalculator class."""

    def setup_method(self) -> None:
        self.calc = MomentumCalculator()

    def test_velocity_bullish_acceleration(self) -> None:
        """Short window more bullish than long → positive velocity."""
        short = _make_snapshot(weighted_score=0.5)
        long = _make_snapshot(weighted_score=0.1, window_hours=168)
        velocity = self.calc.compute_velocity(short, long)
        assert velocity > 0
        assert velocity == pytest.approx(0.4, abs=0.001)

    def test_velocity_bearish_acceleration(self) -> None:
        """Short window more bearish than long → negative velocity."""
        short = _make_snapshot(weighted_score=-0.3)
        long = _make_snapshot(weighted_score=0.2, window_hours=168)
        velocity = self.calc.compute_velocity(short, long)
        assert velocity < 0
        assert velocity == pytest.approx(-0.5, abs=0.001)

    def test_velocity_flat(self) -> None:
        """Same score across windows → zero velocity."""
        short = _make_snapshot(weighted_score=0.3)
        long = _make_snapshot(weighted_score=0.3, window_hours=168)
        velocity = self.calc.compute_velocity(short, long)
        assert velocity == pytest.approx(0.0, abs=0.001)

    def test_acceleration_positive(self) -> None:
        """Velocity increasing → positive acceleration."""
        short = _make_snapshot(weighted_score=0.6)
        medium = _make_snapshot(weighted_score=0.3, window_hours=72)
        long = _make_snapshot(weighted_score=0.2, window_hours=168)
        accel = self.calc.compute_acceleration(short, medium, long)
        # velocity_recent = 0.6 - 0.3 = 0.3
        # velocity_prior = 0.3 - 0.2 = 0.1
        # acceleration = 0.3 - 0.1 = 0.2
        assert accel == pytest.approx(0.2, abs=0.001)

    def test_acceleration_negative(self) -> None:
        """Velocity decreasing → negative acceleration."""
        short = _make_snapshot(weighted_score=0.3)
        medium = _make_snapshot(weighted_score=0.3, window_hours=72)
        long = _make_snapshot(weighted_score=0.0, window_hours=168)
        accel = self.calc.compute_acceleration(short, medium, long)
        # velocity_recent = 0.3 - 0.3 = 0.0
        # velocity_prior = 0.3 - 0.0 = 0.3
        # acceleration = 0.0 - 0.3 = -0.3
        assert accel == pytest.approx(-0.3, abs=0.001)

    def test_classify_accelerating_bullish(self) -> None:
        """High positive velocity + acceleration → ACCELERATING bullish."""
        sig_type, direction = self.calc.classify_momentum(
            velocity=0.3, acceleration=0.2, short_score=0.4,
        )
        assert sig_type == "ACCELERATING"
        assert direction == "bullish"

    def test_classify_decelerating(self) -> None:
        """Positive velocity + negative acceleration → DECELERATING."""
        sig_type, direction = self.calc.classify_momentum(
            velocity=0.3, acceleration=-0.2, short_score=0.4,
        )
        assert sig_type == "DECELERATING"
        assert direction == "bullish"

    def test_classify_steady(self) -> None:
        """Low velocity + low acceleration → STEADY."""
        sig_type, direction = self.calc.classify_momentum(
            velocity=0.05, acceleration=0.01, short_score=0.0,
        )
        assert sig_type == "STEADY"
        assert direction == "neutral"

    def test_classify_bearish_direction(self) -> None:
        """Negative short score → bearish direction."""
        _, direction = self.calc.classify_momentum(
            velocity=-0.3, acceleration=-0.2, short_score=-0.3,
        )
        assert direction == "bearish"


# ── SentimentTimeSeries Tests ────────────────────────────────────────────

class TestSentimentTimeSeries:
    """Tests for the SentimentTimeSeries query builder."""

    def test_snapshot_with_data(self) -> None:
        """Snapshot computes correct scores from mock DB rows."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        # Simulate 5 bullish (conf 0.8), 3 bearish (conf 0.6), 2 neutral (conf 0.5)
        rows = (
            [("BULLISH", 0.8)] * 5 +
            [("BEARISH", 0.6)] * 3 +
            [("NEUTRAL", 0.5)] * 2
        )
        mock_conn.execute.return_value.fetchall.return_value = rows

        ts = SentimentTimeSeries(mock_engine)
        snap = ts.get_snapshot("AAPL", 24)

        assert snap is not None
        assert snap.ticker == "AAPL"
        assert snap.article_count == 10
        assert snap.bullish_count == 5
        assert snap.bearish_count == 3
        assert snap.neutral_count == 2
        # sentiment_score = (5-3)/10 = 0.2
        assert snap.sentiment_score == pytest.approx(0.2, abs=0.01)

    def test_snapshot_empty_returns_none(self) -> None:
        """No articles → None."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.fetchall.return_value = []

        ts = SentimentTimeSeries(mock_engine)
        assert ts.get_snapshot("AAPL", 24) is None


# ── MomentumSignal Tests ────────────────────────────────────────────────

class TestMomentumSignal:
    """Tests for MomentumSignal data class."""

    def test_to_dict(self) -> None:
        """Signal serializes to dict correctly."""
        signal = MomentumSignal(
            signal_id="test123",
            ticker="AAPL",
            signal_type="DIVERGENCE",
            direction="bullish",
            magnitude=0.7,
            sentiment_velocity=0.3,
            sentiment_acceleration=0.1,
            price_direction="down",
            price_pct=-2.5,
            confidence=0.8,
            short_score=0.4,
            medium_score=0.2,
            long_score=0.1,
            article_count=15,
            evidence=["test evidence"],
        )
        d = signal.to_dict()
        assert d["ticker"] == "AAPL"
        assert d["signal_type"] == "DIVERGENCE"
        assert d["magnitude"] == 0.7
        assert "computed_at" in d


# ── DivergenceDetector Tests ─────────────────────────────────────────────

class TestDivergenceDetector:
    """Tests for divergence detection logic."""

    def test_divergence_price_up_sentiment_down(self) -> None:
        """Price up + sentiment deteriorating → divergence."""
        mock_engine = MagicMock()
        detector = DivergenceDetector(mock_engine)

        with patch.object(detector, "get_price_direction", return_value=("up", 3.5)):
            is_div, price_dir, price_pct = detector.detect_divergence(
                "AAPL", sentiment_velocity=-0.5, sentiment_direction="bearish",
            )
            assert is_div is True
            assert price_dir == "up"

    def test_divergence_price_down_sentiment_up(self) -> None:
        """Price down + sentiment improving → divergence."""
        mock_engine = MagicMock()
        detector = DivergenceDetector(mock_engine)

        with patch.object(detector, "get_price_direction", return_value=("down", -4.0)):
            is_div, price_dir, price_pct = detector.detect_divergence(
                "AAPL", sentiment_velocity=0.5, sentiment_direction="bullish",
            )
            assert is_div is True
            assert price_dir == "down"

    def test_no_divergence_aligned(self) -> None:
        """Price and sentiment moving same direction → no divergence."""
        mock_engine = MagicMock()
        detector = DivergenceDetector(mock_engine)

        with patch.object(detector, "get_price_direction", return_value=("up", 2.0)):
            is_div, _, _ = detector.detect_divergence(
                "AAPL", sentiment_velocity=0.3, sentiment_direction="bullish",
            )
            assert is_div is False

    def test_no_divergence_no_price_data(self) -> None:
        """No price data available → no divergence."""
        mock_engine = MagicMock()
        detector = DivergenceDetector(mock_engine)

        with patch.object(detector, "get_price_direction", return_value=(None, None)):
            is_div, price_dir, price_pct = detector.detect_divergence(
                "AAPL", sentiment_velocity=-0.5, sentiment_direction="bearish",
            )
            assert is_div is False
            assert price_dir is None
