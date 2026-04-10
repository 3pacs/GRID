"""Tests for intelligence.business_news_parser — structured event extraction."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from intelligence.business_news_parser import (
    BusinessNewsParser,
    BusinessEvent,
    EVENT_CATEGORIES,
)


# ── Fixtures ─────────────────────────────────────────────────────────────

class MockEngine:
    """Minimal mock engine that satisfies __init__ without DB."""

    def begin(self):
        return self

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, *args, **kwargs):
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return None


# ── Pattern Detection Tests ──────────────────────────────────────────────

class TestBusinessNewsParserPatterns:
    """Test that the regex patterns match expected headlines."""

    def setup_method(self) -> None:
        self.parser = BusinessNewsParser(MockEngine())

    # -- Executive Changes --

    def test_ceo_appointment(self) -> None:
        events = self.parser.parse_article(
            title="Apple names John Smith as new CEO",
            summary="", tickers=["AAPL"], source="reuters",
        )
        assert any(e.category == "executive_change" for e in events)

    def test_ceo_departure(self) -> None:
        events = self.parser.parse_article(
            title="CEO steps down amid controversy",
            summary="The chief executive resigned effective immediately.",
            tickers=["XYZ"], source="cnbc",
        )
        ceo_events = [e for e in events if e.category == "executive_change"]
        assert len(ceo_events) >= 1
        assert ceo_events[0].direction == "bearish"

    # -- Product Launches --

    def test_product_launch(self) -> None:
        events = self.parser.parse_article(
            title="Tesla unveils new Model Y refresh at event",
            summary="The company introduced its updated electric vehicle.",
            tickers=["TSLA"], source="reuters",
        )
        assert any(e.category == "product_launch" for e in events)

    def test_fda_approval(self) -> None:
        events = self.parser.parse_article(
            title="FDA approves Pfizer's new cancer drug",
            summary="Regulatory approval was granted for the novel therapy.",
            tickers=["PFE"], source="reuters",
        )
        product_events = [e for e in events if e.category == "product_launch"]
        assert len(product_events) >= 1
        assert product_events[0].direction == "bullish"

    # -- Restructuring --

    def test_layoffs(self) -> None:
        events = self.parser.parse_article(
            title="Meta to cut 10,000 jobs in latest round of layoffs",
            summary="The company will eliminate positions across divisions.",
            tickers=["META"], source="wsj",
        )
        assert any(e.category == "restructuring" for e in events)

    def test_restructuring(self) -> None:
        events = self.parser.parse_article(
            title="Company announces major restructuring plan",
            summary="The firm will consolidate operations.",
            tickers=["XYZ"], source="reuters",
        )
        assert any(e.category == "restructuring" for e in events)

    # -- Earnings --

    def test_earnings_beat(self) -> None:
        events = self.parser.parse_article(
            title="Amazon beats earnings estimates by wide margin",
            summary="Revenue exceeded consensus expectations.",
            tickers=["AMZN"], source="cnbc",
        )
        earnings_events = [e for e in events if e.category == "earnings_surprise"]
        assert len(earnings_events) >= 1
        assert earnings_events[0].direction == "bullish"

    def test_earnings_miss(self) -> None:
        events = self.parser.parse_article(
            title="Intel misses revenue estimates, shares fall",
            summary="Results fell short of analyst expectations.",
            tickers=["INTC"], source="reuters",
        )
        earnings_events = [e for e in events if e.category == "earnings_surprise"]
        assert len(earnings_events) >= 1
        assert earnings_events[0].direction == "bearish"

    # -- Guidance --

    def test_raised_guidance(self) -> None:
        events = self.parser.parse_article(
            title="Nvidia raises full-year guidance above consensus",
            summary="The company increased its outlook for the fiscal year.",
            tickers=["NVDA"], source="reuters",
        )
        guidance_events = [e for e in events if e.category == "guidance_change"]
        assert len(guidance_events) >= 1
        assert guidance_events[0].direction == "bullish"

    def test_lowered_guidance(self) -> None:
        events = self.parser.parse_article(
            title="FedEx cuts guidance citing weak demand",
            summary="The company lowered its full-year forecast.",
            tickers=["FDX"], source="cnbc",
        )
        guidance_events = [e for e in events if e.category == "guidance_change"]
        assert len(guidance_events) >= 1
        assert guidance_events[0].direction == "bearish"

    # -- Contracts --

    def test_government_contract(self) -> None:
        events = self.parser.parse_article(
            title="Lockheed wins $5 billion Pentagon contract for F-35",
            summary="The defense contract was awarded by the DOD.",
            tickers=["LMT"], source="reuters",
        )
        assert any(e.category == "contract_win" for e in events)

    # -- Analyst Actions --

    def test_upgrade(self) -> None:
        events = self.parser.parse_article(
            title="Goldman upgrades Apple to buy with $250 target",
            summary="The analyst raised to overweight.",
            tickers=["AAPL"], source="cnbc",
        )
        assert any(e.category == "analyst_action" for e in events)

    # -- Credit Events --

    def test_bankruptcy(self) -> None:
        events = self.parser.parse_article(
            title="Company files for chapter 11 bankruptcy protection",
            summary="The firm defaulted on its debt obligations.",
            tickers=["XYZ"], source="reuters",
        )
        credit_events = [e for e in events if e.category == "credit_event"]
        assert len(credit_events) >= 1
        assert credit_events[0].direction == "bearish"

    # -- Regulatory --

    def test_regulatory_investigation(self) -> None:
        events = self.parser.parse_article(
            title="DOJ launches antitrust probe into tech giant",
            summary="The FTC is investigating anti-competitive practices.",
            tickers=["GOOGL"], source="wsj",
        )
        assert any(e.category == "regulatory_action" for e in events)

    # -- No Match --

    def test_no_match_returns_empty(self) -> None:
        events = self.parser.parse_article(
            title="Weather forecast sunny for the weekend",
            summary="No rain expected.",
            tickers=[], source="weather.com",
        )
        assert len(events) == 0

    # -- Multiple Events --

    def test_multiple_events_single_article(self) -> None:
        """An article can trigger multiple categories."""
        events = self.parser.parse_article(
            title="Apple beats earnings and raises guidance, CEO announces buyback",
            summary="Revenue exceeded estimates. The company raised its full-year outlook and announced a $100 billion share repurchase program.",
            tickers=["AAPL"], source="reuters",
        )
        categories = {e.category for e in events}
        # Should detect at least earnings + guidance
        assert len(categories) >= 2

    # -- Dollar Value Extraction --

    def test_dollar_value_extraction(self) -> None:
        events = self.parser.parse_article(
            title="Company wins $3.5 billion contract",
            summary="The award totals $3.5 billion over five years.",
            tickers=["XYZ"], source="reuters",
        )
        events_with_value = [e for e in events if e.dollar_value is not None]
        assert len(events_with_value) >= 1
        assert events_with_value[0].dollar_value == pytest.approx(3.5e9, rel=0.01)


# ── BusinessEvent Tests ──────────────────────────────────────────────────

class TestBusinessEvent:
    """Test BusinessEvent data class."""

    def test_to_dict(self) -> None:
        event = BusinessEvent(
            event_id="test123",
            category="earnings_surprise",
            tickers=["AAPL"],
            headline="Apple beats estimates",
            description="Revenue exceeded consensus.",
            source="reuters",
            direction="bullish",
            estimated_bps=300,
            horizon="short",
            dollar_value=None,
            confidence=0.8,
            published_at=datetime(2026, 4, 10, tzinfo=timezone.utc),
        )
        d = event.to_dict()
        assert d["category"] == "earnings_surprise"
        assert d["direction"] == "bullish"
        assert d["published_at"] == "2026-04-10T00:00:00+00:00"


# ── Constants Sanity ─────────────────────────────────────────────────────

class TestEventConstants:
    """Verify event category constants are well-formed."""

    def test_all_categories_have_required_fields(self) -> None:
        for cat_name, cat_info in EVENT_CATEGORIES.items():
            assert "typical_bps" in cat_info, f"{cat_name} missing typical_bps"
            assert "horizon" in cat_info, f"{cat_name} missing horizon"
            assert "description" in cat_info, f"{cat_name} missing description"
            assert cat_info["typical_bps"] > 0
