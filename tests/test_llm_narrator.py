"""Tests for intelligence/llm_narrator.py."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from intelligence.llm_narrator import (
    MAX_WORDS,
    TARGET_WORDS,
    NarrativeReport,
    _count_words,
    _format_signal_line,
    _headline_from_verdict,
    _robustness_block,
    _top_signals_block,
    build_narrative_prompt,
    compose_template_narrative,
    narrate_trade,
)


# ── _count_words ─────────────────────────────────────────────────────────


class TestCountWords:
    def test_empty(self):
        assert _count_words("") == 0

    def test_simple(self):
        assert _count_words("hello world") == 2

    def test_whitespace_collapsed(self):
        assert _count_words("  a  b  c  ") == 3

    def test_newlines(self):
        assert _count_words("a\nb\n\nc") == 3


# ── _headline_from_verdict ───────────────────────────────────────────────


class TestHeadline:
    def test_long_high(self):
        assert _headline_from_verdict("high", "bullish", "TSM") == "LONG TSM — HIGH conviction"

    def test_short_medium(self):
        assert _headline_from_verdict("medium", "bearish", "NVDA") == "SHORT NVDA — MEDIUM conviction"

    def test_no_trade(self):
        assert _headline_from_verdict("no_trade", "bullish", "SPY") == "NO TRADE — SPY"

    def test_neutral_direction(self):
        assert _headline_from_verdict("low", "neutral", "QQQ") == "FLAT QQQ — LOW conviction"


# ── _format_signal_line / _top_signals_block ─────────────────────────────


def _evidence(source: str, weight: float, classification: str, brier: float | None = 0.1):
    card = None
    if brier is not None:
        card = SimpleNamespace(running_brier=brier, scored_count=100)
    return SimpleNamespace(
        signal_source=source,
        shapley_weight=weight,
        classification=classification,
        scorecard=card,
    )


class TestSignalBlock:
    def test_format_with_scorecard(self):
        line = _format_signal_line(_evidence("jodi_oil", 0.3, "strong", 0.08))
        assert "jodi_oil" in line
        assert "0.30" in line
        assert "strong" in line
        assert "0.080" in line

    def test_format_cold_start(self):
        line = _format_signal_line(_evidence("reddit", 0.2, "cold_start", brier=None))
        assert "cold-start" in line

    def test_top_block_sorted_desc(self):
        evs = [
            _evidence("weak", 0.1, "weak", 0.22),
            _evidence("strong", 0.5, "strong", 0.08),
            _evidence("mid", 0.3, "neutral", 0.15),
        ]
        block = _top_signals_block(evs, top_n=3)
        lines = [l for l in block.split("\n") if l.strip()]
        assert "strong" in lines[0]
        assert "mid" in lines[1]
        assert "weak" in lines[2]

    def test_top_block_truncates_to_n(self):
        evs = [_evidence(f"sig_{i}", 0.1 * (5 - i), "strong") for i in range(5)]
        block = _top_signals_block(evs, top_n=3)
        assert block.count("\n") == 2  # 3 lines → 2 newlines

    def test_top_block_empty(self):
        assert "no signal evidence" in _top_signals_block([])


# ── _robustness_block ────────────────────────────────────────────────────


class TestRobustness:
    def test_none_stress(self):
        assert _robustness_block(None) == "Stress test: not run"

    def test_robust(self):
        stress = SimpleNamespace(robustness_label="robust", robustness_score=0.95, break_count=0)
        line = _robustness_block(stress)
        assert "ROBUST" in line
        assert "0.95" in line
        assert "0 verdict-breaking" in line

    def test_fragile(self):
        stress = SimpleNamespace(robustness_label="fragile", robustness_score=0.3, break_count=5)
        line = _robustness_block(stress)
        assert "FRAGILE" in line
        assert "5" in line


# ── compose_template_narrative ───────────────────────────────────────────


def _provenance(
    verdict: str = "high",
    direction: str = "bullish",
    confidence: float = 0.82,
    aggregate: float = 1.25,
    crowd: bool = False,
    disagreement: float = 0.0,
    fragility: float = 1.0,
    red_team: float = 0.0,
    market_implied: float = 0.75,
    fudge_alerts: list | None = None,
    lever: str = "earnings_beat",
    actor: str = "TSMC",
    flow: str = "open",
):
    causation = SimpleNamespace(
        lever=lever, flow_direction=flow, actor=actor, complete=True,
    )
    return SimpleNamespace(
        ticker="TSM",
        direction=direction,
        verdict=verdict,
        confidence=confidence,
        confidence_lower=confidence - 0.1,
        confidence_upper=confidence + 0.1,
        horizon_days=7,
        aggregate_conviction=aggregate,
        regime="EXPANSION",
        fci_regime="EASY",
        signal_evidence=[
            _evidence("semi_book_to_bill", 0.3, "strong", 0.08),
            _evidence("flow_momentum", 0.25, "strong", 0.09),
            _evidence("taiwan_exports", 0.2, "strong", 0.10),
        ],
        fragility_multiplier=fragility,
        disagreement_score=disagreement,
        red_team_epistemic_risk=red_team,
        crowd_aligned=crowd,
        market_implied_prob=market_implied,
        shipping_fudge_alerts=fudge_alerts or [],
        causation=causation,
    )


class TestComposeTemplate:
    def test_happy_path_contains_key_facts(self):
        text = compose_template_narrative(_provenance())
        assert "TSM" in text
        assert "bullish" in text
        assert "HIGH" in text
        assert "0.82" in text
        assert "EXPANSION" in text
        assert "EASY" in text
        assert "earnings_beat" in text
        assert "TSMC" in text
        assert "semi_book_to_bill" in text

    def test_no_lever_warning(self):
        text = compose_template_narrative(
            _provenance(lever="", actor="unknown")
        )
        assert "WARNING" in text
        assert "no named lever" in text

    def test_penalty_stack_mentions_all_active(self):
        text = compose_template_narrative(
            _provenance(
                disagreement=0.3,
                fragility=0.6,
                red_team=0.3,
                crowd=True,
                fudge_alerts=[{"x": 1}],
            )
        )
        assert "disagreement" in text
        assert "fragility" in text
        assert "red-team" in text
        assert "crowd" in text
        assert "fudge" in text

    def test_no_penalties_clean_message(self):
        text = compose_template_narrative(_provenance())
        assert "No material penalties" in text or "all conviction layers align" in text

    def test_market_implied_positive_edge(self):
        text = compose_template_narrative(
            _provenance(confidence=0.85, market_implied=0.60)
        )
        assert "Positive edge" in text

    def test_market_implied_contrarian(self):
        text = compose_template_narrative(
            _provenance(confidence=0.55, market_implied=0.80)
        )
        assert "contrarian" in text.lower()

    def test_market_implied_aligned(self):
        text = compose_template_narrative(
            _provenance(confidence=0.78, market_implied=0.76)
        )
        assert "aligned" in text.lower()

    def test_stress_block_included(self):
        stress = SimpleNamespace(
            robustness_label="robust", robustness_score=0.95, break_count=0
        )
        text = compose_template_narrative(_provenance(), stress=stress)
        assert "ROBUST" in text

    def test_word_count_reasonable(self):
        text = compose_template_narrative(_provenance())
        words = _count_words(text)
        assert 80 <= words <= 400  # reasonable briefing length


# ── build_narrative_prompt ───────────────────────────────────────────────


class TestPrompt:
    def test_prompt_contains_template(self):
        prompt = build_narrative_prompt(_provenance())
        assert "PROVENANCE REPORT" in prompt
        assert "END PROVENANCE REPORT" in prompt
        assert "TSM" in prompt

    def test_prompt_mentions_word_target(self):
        prompt = build_narrative_prompt(_provenance())
        assert str(TARGET_WORDS) in prompt

    def test_prompt_no_invention_clause(self):
        prompt = build_narrative_prompt(_provenance())
        assert "do NOT invent" in prompt or "not invent" in prompt.lower()


# ── narrate_trade ────────────────────────────────────────────────────────


class TestNarrateTrade:
    def test_template_fallback_without_llm(self):
        report = narrate_trade(_provenance())
        assert report.source == "template"
        assert report.ticker == "TSM"
        assert report.word_count > 0
        assert "HIGH" in report.headline

    def test_llm_path_with_generate(self):
        mock_client = MagicMock()
        mock_client.generate.return_value = (
            "LONG TSM. Rising SEMI book-to-bill orders are the lever. "
            "TSMC foundry utilization above 85%. Target +10% over 7d. "
            "Stop below $207. Size 5% of account. Robust to stress."
        )
        report = narrate_trade(_provenance(), llm_client=mock_client)
        assert report.source == "llm"
        assert "TSM" in report.thesis
        assert "SEMI book-to-bill" in report.thesis

    def test_llm_failure_falls_back_to_template(self):
        # spec=['generate'] so MagicMock doesn't auto-create .chat and
        # accidentally satisfy the fallback path.
        mock_client = MagicMock(spec=["generate"])
        mock_client.generate.side_effect = RuntimeError("llm down")
        report = narrate_trade(_provenance(), llm_client=mock_client)
        assert report.source == "template"
        assert "TSM" in report.thesis

    def test_llm_empty_response_falls_back(self):
        mock_client = MagicMock()
        mock_client.generate.return_value = ""
        report = narrate_trade(_provenance(), llm_client=mock_client)
        assert report.source == "template"

    def test_llm_chat_fallback(self):
        mock_client = MagicMock(spec=["chat"])  # no .generate
        mock_client.chat.return_value = "LONG TSM via chat path."
        report = narrate_trade(_provenance(), llm_client=mock_client)
        assert report.source == "llm"
        assert "chat path" in report.thesis

    def test_llm_caps_word_count(self):
        mock_client = MagicMock()
        # 500-word response
        mock_client.generate.return_value = " ".join(["word"] * 500)
        report = narrate_trade(_provenance(), llm_client=mock_client)
        assert report.source == "llm"
        assert report.word_count <= MAX_WORDS + 1  # +1 for ellipsis marker

    def test_no_trade_headline(self):
        report = narrate_trade(_provenance(verdict="no_trade"))
        assert "NO TRADE" in report.headline

    def test_serialization_roundtrip(self):
        report = narrate_trade(_provenance())
        d = report.to_dict()
        for k in (
            "ticker", "headline", "thesis", "source",
            "word_count", "generated_at",
        ):
            assert k in d
