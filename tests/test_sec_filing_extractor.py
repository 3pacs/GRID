"""Tests for intelligence.sec_filing_extractor — SEC 8-K fact extraction."""

from __future__ import annotations

from datetime import date

import pytest

from intelligence.sec_filing_extractor import (
    SECFilingExtractor,
    MaterialFact,
    ITEM_TYPES,
)


# ── Fixtures ─────────────────────────────────────────────────────────────

class MockEngine:
    """Minimal mock engine for init without DB."""

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

    def scalar(self):
        return 0


# ── Item Extraction Tests ────────────────────────────────────────────────

class TestSECFilingExtractor:
    """Tests for the SECFilingExtractor pattern matching."""

    def setup_method(self) -> None:
        self.extractor = SECFilingExtractor(MockEngine())

    def test_extract_acquisition_item(self) -> None:
        """Item 2.01 (acquisition) is detected from filing text."""
        text = (
            "Item 2.01 Completion of Acquisition or Disposition of Assets. "
            "On April 5, 2026, the Company completed the acquisition of "
            "Target Corp for $3.5 billion in cash. The transaction was "
            "approved by shareholders on March 15, 2026."
        )
        facts = self.extractor.extract_from_text("AAPL", date(2026, 4, 5), text)
        assert len(facts) >= 1
        fact = facts[0]
        assert fact.item_number == "2.01"
        assert fact.item_name == "Acquisition or Disposition"
        assert len(fact.dollar_values) >= 1
        assert 3.5e9 in fact.dollar_values

    def test_extract_earnings_item(self) -> None:
        """Item 2.02 (earnings) is detected."""
        text = (
            "Item 2.02 Results of Operations and Financial Condition. "
            "Revenue for Q1 2026 was $95.3 billion, up 12% year over year. "
            "Earnings per share of $2.15 exceeded consensus of $2.00."
        )
        facts = self.extractor.extract_from_text("AAPL", date(2026, 4, 1), text)
        assert len(facts) >= 1
        assert facts[0].item_number == "2.02"
        assert len(facts[0].dollar_values) >= 1
        assert len(facts[0].percentages) >= 1

    def test_extract_restructuring_item(self) -> None:
        """Item 2.05 (restructuring) is detected."""
        text = (
            "Item 2.05 Costs Associated with Exit or Disposal Activities. "
            "The Company announced a workforce reduction of approximately "
            "5,000 employees, representing 8% of its global workforce. "
            "The restructuring charge is estimated at $350 million."
        )
        facts = self.extractor.extract_from_text("XYZ", date(2026, 3, 15), text)
        assert len(facts) >= 1
        assert facts[0].item_number == "2.05"
        assert facts[0].direction == "bearish"

    def test_extract_officer_change(self) -> None:
        """Item 5.02 (officer departure) is detected."""
        text = (
            "Item 5.02 Departure of Directors or Certain Officers; "
            "Election of Directors; Appointment of Certain Officers. "
            "John Smith has resigned as CEO effective immediately. "
            "Jane Doe has been appointed as interim CEO."
        )
        facts = self.extractor.extract_from_text("XYZ", date(2026, 4, 1), text)
        assert len(facts) >= 1
        assert facts[0].item_number == "5.02"

    def test_extract_bankruptcy(self) -> None:
        """Item 1.03 (bankruptcy) is detected."""
        text = (
            "Item 1.03 Bankruptcy or Receivership. "
            "On April 8, 2026, the Company filed a voluntary petition "
            "for relief under Chapter 11 of the United States Bankruptcy Code."
        )
        facts = self.extractor.extract_from_text("FAIL", date(2026, 4, 8), text)
        assert len(facts) >= 1
        assert facts[0].item_number == "1.03"
        assert facts[0].direction == "bearish"
        assert facts[0].estimated_bps == 1000

    def test_extract_impairment(self) -> None:
        """Item 2.06 (material impairment) is detected."""
        text = (
            "Item 2.06 Material Impairments. "
            "The Company recorded a goodwill impairment charge of $2.1 billion "
            "related to its media segment."
        )
        facts = self.extractor.extract_from_text("WBD", date(2026, 2, 1), text)
        assert len(facts) >= 1
        assert facts[0].item_number == "2.06"
        assert facts[0].direction == "bearish"

    def test_extract_multiple_items(self) -> None:
        """Filing with multiple items extracts all of them."""
        text = (
            "Item 2.02 Results of Operations. Revenue was $10 billion. "
            "Item 5.02 Officer Changes. The CFO resigned. Jane was appointed. "
            "Item 8.01 Other Events. The company announced a stock split."
        )
        facts = self.extractor.extract_from_text("XYZ", date(2026, 4, 1), text)
        item_numbers = {f.item_number for f in facts}
        assert "2.02" in item_numbers
        assert "5.02" in item_numbers
        assert "8.01" in item_numbers

    def test_no_items_fallback_to_keywords(self) -> None:
        """Text without item numbers falls back to keyword classification."""
        text = (
            "The company completed the acquisition of Small Corp today. "
            "The purchase price was $500 million in cash and stock."
        )
        facts = self.extractor.extract_from_text("BIG", date(2026, 4, 1), text)
        # Should match via keyword fallback
        assert len(facts) >= 1

    def test_short_text_returns_empty(self) -> None:
        """Very short text returns no facts."""
        facts = self.extractor.extract_from_text("XYZ", date(2026, 4, 1), "Short.")
        assert len(facts) == 0


# ── Dollar/Percentage Extraction Tests ───────────────────────────────────

class TestValueExtraction:
    """Test dollar and percentage extraction helpers."""

    def setup_method(self) -> None:
        self.extractor = SECFilingExtractor(MockEngine())

    def test_extract_billions(self) -> None:
        values = self.extractor._extract_dollars("The deal was valued at $3.5 billion.")
        assert 3.5e9 in values

    def test_extract_millions(self) -> None:
        values = self.extractor._extract_dollars("Revenue of $250 million this quarter.")
        assert 250e6 in values

    def test_extract_multiple_values(self) -> None:
        text = "Revenue was $10 billion and net income was $2.5 billion."
        values = self.extractor._extract_dollars(text)
        assert len(values) >= 2

    def test_extract_percentages(self) -> None:
        values = self.extractor._extract_percentages("Revenue grew 15% year over year.")
        assert 15.0 in values

    def test_extract_entities(self) -> None:
        entities = self.extractor._extract_entities(
            "Target Corp Inc. and Acquirer Holdings LLC announced the deal."
        )
        assert len(entities) >= 1


# ── Direction Inference Tests ────────────────────────────────────────────

class TestDirectionInference:
    """Test market direction inference from text."""

    def setup_method(self) -> None:
        self.extractor = SECFilingExtractor(MockEngine())

    def test_bullish_text(self) -> None:
        direction = self.extractor._infer_direction(
            "2.02",
            "Revenue growth was strong and exceeded record levels. The company beat estimates.",
        )
        assert direction == "bullish"

    def test_bearish_text(self) -> None:
        direction = self.extractor._infer_direction(
            "2.02",
            "Revenue declined significantly. The company missed estimates and faces layoff restructuring.",
        )
        assert direction == "bearish"

    def test_default_direction_from_item(self) -> None:
        """Items with non-neutral default use that default."""
        direction = self.extractor._infer_direction("1.03", "Filing for relief.")
        assert direction == "bearish"


# ── MaterialFact Tests ───────────────────────────────────────────────────

class TestMaterialFact:
    """Test MaterialFact data class."""

    def test_to_dict(self) -> None:
        fact = MaterialFact(
            fact_id="test123",
            ticker="AAPL",
            filing_date=date(2026, 4, 1),
            item_number="2.01",
            item_name="Acquisition or Disposition",
            description="Completed acquisition of XYZ.",
            direction="neutral",
            estimated_bps=500,
            dollar_values=[3.5e9],
            percentages=[],
            key_entities=["XYZ Corp"],
            confidence=0.8,
            raw_text="Item 2.01 ...",
        )
        d = fact.to_dict()
        assert d["ticker"] == "AAPL"
        assert d["item_number"] == "2.01"
        assert d["filing_date"] == "2026-04-01"
        assert d["dollar_values"] == [3.5e9]


# ── ITEM_TYPES Sanity ────────────────────────────────────────────────────

class TestItemTypes:
    """Verify ITEM_TYPES constants are well-formed."""

    def test_all_items_have_required_fields(self) -> None:
        for item_num, info in ITEM_TYPES.items():
            assert "name" in info, f"Item {item_num} missing name"
            assert "direction" in info, f"Item {item_num} missing direction"
            assert "typical_bps" in info, f"Item {item_num} missing typical_bps"
            assert info["direction"] in ("bullish", "bearish", "neutral")
