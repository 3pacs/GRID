"""Tests for the active Obsidian agent."""

from __future__ import annotations

import pytest


class TestExtractEntities:
    def test_extracts_cashtag_tickers(self):
        from intelligence.obsidian_agent import extract_entities
        text = "Watching $NVDA and $AAPL for earnings momentum. Also $BTC looking strong."
        entities = extract_entities(text)
        assert "NVDA" in entities["tickers"]
        assert "AAPL" in entities["tickers"]
        assert "BTC" in entities["tickers"]

    def test_no_tickers_returns_empty(self):
        from intelligence.obsidian_agent import extract_entities
        entities = extract_entities("No market references here.")
        assert entities["tickers"] == []

    def test_extracts_contextual_tickers(self):
        from intelligence.obsidian_agent import extract_entities
        text = "NVDA up 5% after $MSFT earnings beat"
        entities = extract_entities(text)
        assert "NVDA" in entities["tickers"]
        assert "MSFT" in entities["tickers"]


class TestPriorityRanking:
    def test_urgent_sorts_first(self):
        from intelligence.obsidian_agent import rank_for_review
        items = [
            {"agent_flags": {"priority": "low"}, "title": "A"},
            {"agent_flags": {"priority": "urgent"}, "title": "B"},
            {"agent_flags": {"priority": "high"}, "title": "C"},
        ]
        ranked = rank_for_review(items)
        assert ranked[0]["title"] == "B"
        assert ranked[1]["title"] == "C"
        assert ranked[2]["title"] == "A"

    def test_missing_priority_sorts_last(self):
        from intelligence.obsidian_agent import rank_for_review
        items = [
            {"agent_flags": {}, "title": "No Priority"},
            {"agent_flags": {"priority": "high"}, "title": "High"},
        ]
        ranked = rank_for_review(items)
        assert ranked[0]["title"] == "High"


class TestShouldEscalateToPaid:
    def test_low_confidence_triggers_escalation(self):
        from intelligence.obsidian_agent import should_escalate_to_paid
        result = {"confidence": 0.3, "coherent": False}
        assert should_escalate_to_paid(result) is True

    def test_high_confidence_no_escalation(self):
        from intelligence.obsidian_agent import should_escalate_to_paid
        result = {"confidence": 0.9, "coherent": True}
        assert should_escalate_to_paid(result) is False

    def test_incoherent_triggers_regardless_of_confidence(self):
        from intelligence.obsidian_agent import should_escalate_to_paid
        result = {"confidence": 0.9, "coherent": False}
        assert should_escalate_to_paid(result) is True
