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


class TestLearningLoop:
    def test_compute_preferences_from_actions(self):
        from intelligence.obsidian_agent import compute_preferences
        actions = [
            {"domain": "tools", "status": "approved", "tags": ["quantization"], "relevance": 8},
            {"domain": "tools", "status": "approved", "tags": ["quantization"], "relevance": 7},
            {"domain": "tools", "status": "rejected", "tags": ["scraping"], "relevance": 4},
            {"domain": "tools", "status": "rejected", "tags": ["scraping"], "relevance": 3},
            {"domain": "alpha", "status": "approved", "tags": ["options"], "relevance": 9},
        ]
        prefs = compute_preferences(actions)
        assert prefs["domain_approval_rate"]["tools"] == 0.5
        assert prefs["domain_approval_rate"]["alpha"] == 1.0
        assert "quantization" in prefs["approved_tags"]
        assert "scraping" in prefs["rejected_tags"]
        assert prefs["min_relevance_threshold"] >= 4  # raised above rejected items

    def test_empty_actions_returns_defaults(self):
        from intelligence.obsidian_agent import compute_preferences
        prefs = compute_preferences([])
        assert prefs["min_relevance_threshold"] == 5
        assert prefs["domain_approval_rate"] == {}


class TestProactiveNotes:
    def test_build_proactive_note(self):
        from intelligence.obsidian_agent import build_proactive_note
        note = build_proactive_note(
            event_type="dark_pool_anomaly",
            title="NVDA Dark Pool 3x Volume",
            body="Unusual dark pool activity detected in NVDA. Volume 3x 30-day average.",
            domain="alpha",
            tags=["dark-pool", "NVDA"],
        )
        assert note["domain"] == "alpha"
        assert note["status"] == "inbox"
        assert note["title"] == "NVDA Dark Pool 3x Volume"
        assert "dark-pool" in note["frontmatter"]["tags"]
        assert note["frontmatter"]["confidence"] == "derived"
        assert note["frontmatter"]["source"] == "dark_pool_anomaly"
        assert note["agent_flags"]["needs_human_review"] is True

    def test_build_regime_change_note(self):
        from intelligence.obsidian_agent import build_proactive_note
        note = build_proactive_note(
            event_type="regime_change",
            title="Regime Shift: Risk-On to Risk-Off",
            body="Regime classifier detected shift from risk-on to risk-off.",
            domain="intel",
            tags=["regime", "risk-off"],
            priority="high",
        )
        assert note["agent_flags"]["priority"] == "high"
        assert note["domain"] == "intel"
