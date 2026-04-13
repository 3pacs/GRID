"""CAT-152 — Risk factor novelty detector tests."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from intelligence.risk_factor_novelty import (
    _REWRITE_MIN_SIM,
    _SEVERITY_ELEVATED,
    _UNCHANGED_SIM,
    RiskFactorChange,
    RiskNoveltyResult,
    compute_novelty,
    detect_novelty,
    jaccard_similarity,
    ngrams,
    split_sentences,
    token_change_ratio,
    tokenize,
)


class TestTokenize:
    def test_basic(self):
        assert tokenize("Hello, World! 2025.") == ["hello", "world", "2025"]

    def test_empty(self):
        assert tokenize("") == []

    def test_none(self):
        assert tokenize(None) == []


class TestSplitSentences:
    def test_paragraph_split(self):
        text = """First sentence with many words here for the test.

Second paragraph starts here. And another sentence too to make it long enough.

Third one goes here also for the final test."""
        sents = split_sentences(text)
        assert len(sents) >= 2

    def test_short_sentences_filtered(self):
        text = "Too short. This one has enough tokens to be counted fine."
        sents = split_sentences(text)
        assert len(sents) == 1

    def test_empty(self):
        assert split_sentences("") == []
        assert split_sentences(None) == []


class TestNgramsJaccard:
    def test_identical_ngrams(self):
        tokens = ["a", "b", "c", "d", "e", "f"]
        g = ngrams(tokens, n=3)
        assert len(g) == 4
        assert jaccard_similarity(g, g) == 1.0

    def test_disjoint_zero(self):
        a = ngrams(["a", "b", "c", "d", "e"], n=3)
        b = ngrams(["x", "y", "z", "w", "v"], n=3)
        assert jaccard_similarity(a, b) == 0.0

    def test_short_sentence_fallback(self):
        g = ngrams(["a", "b"], n=5)
        assert len(g) == 1

    def test_empty_sets(self):
        assert jaccard_similarity(set(), set()) == 1.0
        assert jaccard_similarity({1}, set()) == 0.0


class TestTokenChangeRatio:
    def test_all_new(self):
        old = ["a", "b", "c"]
        new = ["x", "y", "z"]
        assert token_change_ratio(old, new) == 1.0

    def test_all_same(self):
        tokens = ["a", "b", "c"]
        assert token_change_ratio(tokens, tokens) == 0.0

    def test_half_changed(self):
        old = ["a", "b"]
        new = ["a", "z"]
        assert token_change_ratio(old, new) == 0.5

    def test_empty_new_zero(self):
        assert token_change_ratio(["a"], []) == 0.0


class TestDetectNovelty:
    def test_no_old_filing_all_new(self):
        new = (
            "Our business faces material risks from regulatory investigations and enforcement actions. "
            "We may be subject to increased tariffs on imports from China and other countries. "
            "Supply chain disruptions could materially affect our operating results going forward."
        )
        result = detect_novelty(
            ticker="ABC", new_filing_text=new, old_filing_text=None,
            new_filing_date=date(2026, 4, 1),
        )
        assert result.new_sentence_count == 3
        assert result.novelty_index == 1.0
        assert result.severity == "critical"

    def test_identical_filings_no_novelty(self):
        text = (
            "We face material risks from regulatory enforcement actions and litigation. "
            "Our business may be adversely impacted by global supply chain disruptions this year. "
            "Currency fluctuations against the US dollar could affect our international revenues significantly."
        )
        result = detect_novelty(
            ticker="ABC", new_filing_text=text, old_filing_text=text,
        )
        assert result.novelty_index == 0.0
        assert result.severity == "unchanged"
        assert result.new_sentence_count == 0
        assert result.rewritten_sentence_count == 0

    def test_one_new_sentence_added(self):
        old = (
            "We face material risks from regulatory enforcement actions and ongoing litigation matters. "
            "Our business may be adversely impacted by global supply chain disruptions this calendar year."
        )
        new = old + (
            " A new material risk factor has emerged regarding pending SEC investigations into accounting practices."
        )
        result = detect_novelty(
            ticker="ABC", new_filing_text=new, old_filing_text=old,
        )
        assert result.new_sentence_count >= 1

    def test_severity_critical(self):
        new = (
            "Risk one is brand new and significant for our business in every way. "
            "Risk two is also totally new and material to our revenue projections for this year. "
            "Risk three is a fresh disclosure that has never appeared in our previous filings before."
        )
        result = detect_novelty(
            ticker="ABC", new_filing_text=new, old_filing_text=None,
        )
        assert result.severity == "critical"

    def test_empty_new_filing(self):
        result = detect_novelty(
            ticker="ABC", new_filing_text="", old_filing_text="something",
        )
        assert result.novelty_index == 0.0
        assert result.severity == "unchanged"

    def test_to_dict_shape(self):
        result = detect_novelty(
            ticker="ABC",
            new_filing_text="We face risks from regulatory actions potentially today.",
            old_filing_text=None,
        )
        d = result.to_dict()
        for k in ("ticker", "new_filing_date", "old_filing_date",
                  "novelty_index", "severity", "changes"):
            assert k in d


class TestComputeNovelty:
    def test_no_filings_returns_none(self):
        eng = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        result_obj = MagicMock()
        result_obj.fetchall.return_value = []
        conn.execute.return_value = result_obj
        eng.connect.return_value = conn
        assert compute_novelty(eng, "AAPL") is None

    def test_single_filing_old_date_none(self):
        eng = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        result_obj = MagicMock()
        result_obj.fetchall.return_value = [(
            date(2026, 2, 15),
            "Our business faces new material risks from regulatory investigations today for real.",
        )]
        conn.execute.return_value = result_obj
        eng.connect.return_value = conn
        result = compute_novelty(eng, "XYZ")
        assert result is not None
        assert result.old_filing_date is None

    def test_two_filings_both_used(self):
        eng = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        result_obj = MagicMock()
        result_obj.fetchall.return_value = [
            (date(2026, 2, 15), "New filing risks from regulatory investigations and today."),
            (date(2025, 11, 15), "Old filing risks from supply chain disruptions also today."),
        ]
        conn.execute.return_value = result_obj
        eng.connect.return_value = conn
        result = compute_novelty(eng, "XYZ")
        assert result is not None
        assert result.old_filing_date == date(2025, 11, 15)

    def test_db_error_returns_none(self):
        eng = MagicMock()
        eng.connect.side_effect = RuntimeError("db down")
        assert compute_novelty(eng, "XYZ") is None
