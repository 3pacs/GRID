"""Tests for intelligence.earnings_transcript_analyzer — tone and phrase analysis."""

from __future__ import annotations

from datetime import date

import pytest

from intelligence.earnings_transcript_analyzer import (
    ToneScorer,
    PhraseExtractor,
    SectionSplitter,
    TranscriptAnalysis,
)


# ── ToneScorer Tests ─────────────────────────────────────────────────────

class TestToneScorer:
    """Tests for the ToneScorer word-level analysis."""

    def setup_method(self) -> None:
        self.scorer = ToneScorer()

    def test_positive_tone(self) -> None:
        """Text with mostly positive words → positive tone."""
        text = (
            "We delivered strong results this quarter with robust growth "
            "and outstanding performance across all segments. Revenue "
            "momentum continued to accelerate."
        )
        result = self.scorer.score_text(text)
        assert result["tone"] > 0
        assert result["positive"] > result["negative"]

    def test_negative_tone(self) -> None:
        """Text with mostly negative words → negative tone."""
        text = (
            "We faced challenging headwinds this quarter with weak demand "
            "and uncertain market conditions. Revenue decline was driven "
            "by softness in our core business."
        )
        result = self.scorer.score_text(text)
        assert result["tone"] < 0
        assert result["negative"] > result["positive"]

    def test_neutral_tone(self) -> None:
        """Text with balanced or no sentiment words → near-zero tone."""
        text = "The company reported financial results for the quarter."
        result = self.scorer.score_text(text)
        assert abs(result["tone"]) < 0.5

    def test_empty_text(self) -> None:
        """Empty text returns zeroed scores."""
        result = self.scorer.score_text("")
        assert result["tone"] == 0.0
        assert result["word_count"] == 0

    def test_hedge_words_counted(self) -> None:
        """Hedge words are detected and counted."""
        text = (
            "We may see growth, and could potentially see improvement, "
            "assuming market conditions remain stable."
        )
        result = self.scorer.score_text(text)
        assert result["hedges"] > 0

    def test_classify_optimistic(self) -> None:
        assert self.scorer.classify_tone(0.5) == "optimistic"

    def test_classify_cautious(self) -> None:
        assert self.scorer.classify_tone(-0.15) == "cautious"

    def test_classify_defensive(self) -> None:
        assert self.scorer.classify_tone(-0.5) == "defensive"

    def test_classify_neutral(self) -> None:
        assert self.scorer.classify_tone(0.0) == "neutral"

    def test_classify_confident(self) -> None:
        assert self.scorer.classify_tone(0.15) == "confident"


# ── PhraseExtractor Tests ────────────────────────────────────────────────

class TestPhraseExtractor:
    """Tests for key phrase extraction."""

    def setup_method(self) -> None:
        self.extractor = PhraseExtractor()

    def test_extract_guidance(self) -> None:
        text = (
            "Revenue guidance for the full year is expected to be $50B. "
            "We raised our EPS guidance from $5 to $6."
        )
        phrases = self.extractor.extract_guidance(text)
        assert len(phrases) >= 1

    def test_extract_risks(self) -> None:
        text = (
            "The primary risk remains trade policy uncertainty. "
            "Supply chain disruption continues to be a headwind."
        )
        phrases = self.extractor.extract_risks(text)
        assert len(phrases) >= 1

    def test_extract_forward_looking(self) -> None:
        text = (
            "We expect to deliver strong growth in Q3. "
            "Looking ahead, we plan to expand into three new markets."
        )
        phrases = self.extractor.extract_forward(text)
        assert len(phrases) >= 1

    def test_extract_hedges(self) -> None:
        text = (
            "Subject to market conditions, we may pursue acquisitions. "
            "Depending on regulatory approval, the deal could close in Q4."
        )
        phrases = self.extractor.extract_hedges(text)
        assert len(phrases) >= 1

    def test_no_phrases_in_unrelated_text(self) -> None:
        text = "The weather was nice today and the birds were singing."
        assert len(self.extractor.extract_guidance(text)) == 0
        assert len(self.extractor.extract_risks(text)) == 0


# ── SectionSplitter Tests ────────────────────────────────────────────────

class TestSectionSplitter:
    """Tests for transcript section splitting."""

    def setup_method(self) -> None:
        self.splitter = SectionSplitter()

    def test_split_with_qa_marker(self) -> None:
        text = (
            "Good morning everyone. We had a great quarter. "
            "Revenue grew 20% year over year. "
            "Operator: Our first question comes from John at Morgan Stanley. "
            "Question and answer session begins now."
        )
        prepared, qa = self.splitter.split(text)
        # Should split at the Q&A marker
        assert len(prepared) > 0
        assert len(qa) > 0

    def test_no_qa_section(self) -> None:
        text = "This is a press release with no Q&A section at all."
        prepared, qa = self.splitter.split(text)
        assert prepared == text
        assert qa == ""


# ── TranscriptAnalysis Tests ─────────────────────────────────────────────

class TestTranscriptAnalysis:
    """Tests for the TranscriptAnalysis data class."""

    def test_to_dict(self) -> None:
        analysis = TranscriptAnalysis(
            analysis_id="test123",
            ticker="AAPL",
            filing_date=date(2026, 1, 15),
            overall_tone=0.3,
            prepared_remarks_tone=0.4,
            qa_tone=0.1,
            tone_label="confident",
            positive_count=25,
            negative_count=8,
            hedge_count=5,
            forward_looking_count=12,
            guidance_phrases=["Revenue guidance raised"],
            risk_phrases=["Trade risk"],
            forward_statements=["We expect growth"],
            hedge_phrases=["Subject to conditions"],
            tone_shift=0.15,
            prior_tone=0.15,
            word_count=5000,
            qa_word_count=2000,
            confidence=0.75,
        )
        d = analysis.to_dict()
        assert d["ticker"] == "AAPL"
        assert d["overall_tone"] == 0.3
        assert d["tone_label"] == "confident"
        assert d["filing_date"] == "2026-01-15"
        assert len(d["guidance_phrases"]) == 1
