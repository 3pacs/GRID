"""Tests for intelligence.deal_detector — M&A / deal detection engine."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from intelligence.deal_detector import (
    DealClassifier,
    DealSignal,
    DEAL_STAGES,
    DEAL_TYPES,
    TYPICAL_IMPACT_BPS,
)


# ── DealClassifier Tests ────────────────────────────────────────────────

class TestDealClassifier:
    """Tests for the DealClassifier pattern matching."""

    def setup_method(self) -> None:
        self.classifier = DealClassifier()

    # -- Deal Type Detection --

    def test_detect_acquisition(self) -> None:
        """Acquisition language is detected."""
        signal = self.classifier.classify(
            title="Apple to acquire AI startup for $2 billion",
            summary="Apple Inc. announced plans to acquire a leading AI company.",
            tickers=["AAPL"],
            source="reuters",
        )
        assert signal is not None
        assert signal.deal_type == "ACQUISITION"

    def test_detect_merger(self) -> None:
        """Merger language is detected."""
        signal = self.classifier.classify(
            title="Sprint and T-Mobile agree to merge in $26 billion deal",
            summary="The two companies will combine to form a stronger competitor.",
            tickers=["TMUS"],
            source="cnbc",
        )
        assert signal is not None
        assert signal.deal_type == "MERGER"

    def test_detect_takeover_bid(self) -> None:
        """Hostile takeover language is detected."""
        signal = self.classifier.classify(
            title="Activist investor launches hostile bid for company",
            summary="An unsolicited offer has been made to acquire the firm.",
            tickers=["XYZ"],
            source="bloomberg",
        )
        assert signal is not None
        assert signal.deal_type == "TAKEOVER_BID"

    def test_detect_partnership(self) -> None:
        """Partnership/JV language is detected."""
        signal = self.classifier.classify(
            title="Google and Samsung form strategic partnership on AI chips",
            summary="The two tech giants announced a joint venture.",
            tickers=["GOOGL"],
            source="techcrunch",
        )
        assert signal is not None
        assert signal.deal_type == "PARTNERSHIP"

    def test_detect_ipo(self) -> None:
        """IPO language is detected."""
        signal = self.classifier.classify(
            title="Stripe files for initial public offering",
            summary="The payments company plans to go public via direct listing.",
            tickers=[],
            source="wsj",
        )
        assert signal is not None
        assert signal.deal_type == "IPO"

    def test_detect_buyback(self) -> None:
        """Buyback language is detected."""
        signal = self.classifier.classify(
            title="Apple announces $110 billion share repurchase program",
            summary="The company will buy back shares over the next 3 years.",
            tickers=["AAPL"],
            source="reuters",
        )
        assert signal is not None
        assert signal.deal_type == "BUYBACK"

    def test_detect_spinoff(self) -> None:
        """Spinoff language is detected."""
        signal = self.classifier.classify(
            title="GE to spin off its healthcare division",
            summary="General Electric plans a spin-off of GE HealthCare.",
            tickers=["GE"],
            source="cnbc",
        )
        assert signal is not None
        assert signal.deal_type == "SPINOFF"

    def test_no_deal_detected(self) -> None:
        """Non-deal news returns None."""
        signal = self.classifier.classify(
            title="Apple reports strong iPhone sales in China",
            summary="Revenue beat analyst expectations.",
            tickers=["AAPL"],
            source="reuters",
        )
        assert signal is None

    # -- Stage Detection --

    def test_detect_rumor_stage(self) -> None:
        """Rumor language → RUMOR stage."""
        signal = self.classifier.classify(
            title="Company rumored to be exploring acquisition of rival",
            summary="Sources say the firm is in early discussions.",
            tickers=["XYZ"],
            source="bloomberg",
        )
        assert signal is not None
        assert signal.stage == "RUMOR"

    def test_detect_confirmed_stage(self) -> None:
        """Confirmed language → CONFIRMED stage."""
        signal = self.classifier.classify(
            title="Company confirms acquisition of tech startup",
            summary="The company announced a definitive agreement.",
            tickers=["XYZ"],
            source="reuters",
        )
        assert signal is not None
        assert signal.stage == "CONFIRMED"

    def test_detect_failed_stage(self) -> None:
        """Failed language → FAILED stage."""
        signal = self.classifier.classify(
            title="Merger talks collapse between the two companies",
            summary="The proposed merger has failed after regulatory concerns.",
            tickers=["XYZ", "ABC"],
            source="wsj",
        )
        assert signal is not None
        assert signal.stage == "FAILED"

    # -- Dollar Value Extraction --

    def test_extract_billion_value(self) -> None:
        """Billion dollar values are extracted."""
        signal = self.classifier.classify(
            title="Microsoft to acquire Activision for $69 billion",
            summary="",
            tickers=["MSFT", "ATVI"],
            source="reuters",
        )
        assert signal is not None
        assert signal.deal_value_usd == pytest.approx(69e9, rel=0.01)

    def test_extract_million_value(self) -> None:
        """Million dollar values are extracted."""
        signal = self.classifier.classify(
            title="Startup acquired for $500 million",
            summary="",
            tickers=[],
            source="techcrunch",
        )
        assert signal is not None
        assert signal.deal_value_usd == pytest.approx(500e6, rel=0.01)

    def test_no_value_in_text(self) -> None:
        """No dollar value → None."""
        signal = self.classifier.classify(
            title="Two companies agree to merge",
            summary="Terms of the merger were not disclosed.",
            tickers=[],
            source="reuters",
        )
        assert signal is not None
        assert signal.deal_value_usd is None

    # -- Probability & Direction --

    def test_rumor_low_probability(self) -> None:
        """Rumors have low probability."""
        signal = self.classifier.classify(
            title="Company rumored to be considering acquisition",
            summary="",
            tickers=["XYZ"],
            source="bloomberg",
        )
        assert signal is not None
        assert signal.probability <= 0.2

    def test_confirmed_high_probability(self) -> None:
        """Confirmed deals have high probability."""
        signal = self.classifier.classify(
            title="Company confirms definitive agreement to acquire rival",
            summary="The signed agreement is for $5 billion.",
            tickers=["XYZ"],
            source="reuters",
        )
        assert signal is not None
        assert signal.probability >= 0.7

    def test_acquisition_bullish_direction(self) -> None:
        """Acquisitions are bullish for target."""
        signal = self.classifier.classify(
            title="Big Corp acquires Small Corp for premium",
            summary="",
            tickers=["SMLL"],
            source="reuters",
        )
        assert signal is not None
        assert signal.direction == "bullish"

    def test_failed_deal_bearish(self) -> None:
        """Failed deals are bearish."""
        signal = self.classifier.classify(
            title="Merger has failed after regulatory review",
            summary="The acquisition was abandoned due to antitrust concerns.",
            tickers=["XYZ"],
            source="reuters",
        )
        assert signal is not None
        assert signal.direction == "bearish"

    # -- Ticker Extraction --

    def test_extract_tickers_from_text(self) -> None:
        """$TICKER and (NASDAQ: TICKER) are extracted."""
        signal = self.classifier.classify(
            title="$AAPL considering acquiring $TSLA division",
            summary="(NASDAQ: AAPL) in talks with (NASDAQ: TSLA).",
            tickers=[],
            source="reuters",
        )
        assert signal is not None
        assert "AAPL" in signal.tickers
        assert "TSLA" in signal.tickers

    # -- Confidence --

    def test_high_confidence_with_details(self) -> None:
        """More details (value, tickers, confirmed) → higher confidence."""
        signal = self.classifier.classify(
            title="Apple confirms acquisition of AI startup for $2 billion",
            summary="The definitive agreement was signed today. Apple (NASDAQ: AAPL) will pay in cash.",
            tickers=["AAPL"],
            source="reuters",
        )
        assert signal is not None
        assert signal.confidence >= 0.7

    def test_serialization(self) -> None:
        """DealSignal.to_dict serializes correctly."""
        signal = self.classifier.classify(
            title="Company acquires rival for $1 billion",
            summary="",
            tickers=["XYZ"],
            source="reuters",
        )
        assert signal is not None
        d = signal.to_dict()
        assert "deal_type" in d
        assert "detected_at" in d
        assert isinstance(d["detected_at"], str)


# ── Constants Sanity Checks ──────────────────────────────────────────────

class TestDealConstants:
    """Verify deal type/stage constants are consistent."""

    def test_all_deal_types_have_impact(self) -> None:
        """Every deal type has a typical impact value."""
        for dt in DEAL_TYPES:
            assert dt in TYPICAL_IMPACT_BPS

    def test_stage_ordering(self) -> None:
        """Stages have monotonically increasing order."""
        assert DEAL_STAGES["RUMOR"] < DEAL_STAGES["REPORTED"]
        assert DEAL_STAGES["REPORTED"] < DEAL_STAGES["CONFIRMED"]
        assert DEAL_STAGES["CONFIRMED"] < DEAL_STAGES["REGULATORY"]
        assert DEAL_STAGES["REGULATORY"] < DEAL_STAGES["CLOSED"]
