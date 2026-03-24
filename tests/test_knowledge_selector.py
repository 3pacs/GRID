"""Tests for knowledge.selector — TF-IDF + orthogonality doc selection."""

import pytest

from knowledge.selector import (
    _tokenize,
    _tf,
    _cosine_sim,
    _tfidf_vector,
    _idf,
    select_knowledge,
    select_and_format,
)


# ---------------------------------------------------------------------------
# Tokenisation
# ---------------------------------------------------------------------------

class TestTokenize:
    def test_basic(self):
        tokens = _tokenize("GDP inflation rate forecast")
        assert "gdp" in tokens
        assert "inflation" in tokens
        assert "forecast" in tokens

    def test_stop_words_removed(self):
        tokens = _tokenize("the quick and the slow")
        assert "the" not in tokens
        assert "and" not in tokens
        assert "quick" in tokens

    def test_short_tokens_dropped(self):
        # Single-char tokens should be dropped (regex requires 2+)
        tokens = _tokenize("a b cd ef")
        assert "a" not in tokens
        assert "b" not in tokens
        assert "cd" in tokens

    def test_empty(self):
        assert _tokenize("") == []


# ---------------------------------------------------------------------------
# TF-IDF components
# ---------------------------------------------------------------------------

class TestTfIdf:
    def test_tf_normalised(self):
        tf = _tf(["gdp", "gdp", "inflation", "cpi"])
        assert tf["gdp"] == pytest.approx(0.5)
        assert tf["inflation"] == pytest.approx(0.25)

    def test_cosine_identical(self):
        vec = {"gdp": 1.0, "inflation": 0.5}
        assert _cosine_sim(vec, vec) == pytest.approx(1.0)

    def test_cosine_orthogonal(self):
        vec_a = {"gdp": 1.0}
        vec_b = {"vix": 1.0}
        assert _cosine_sim(vec_a, vec_b) == pytest.approx(0.0)

    def test_cosine_empty(self):
        assert _cosine_sim({}, {"gdp": 1.0}) == 0.0


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------

class TestSelectKnowledge:
    @pytest.fixture()
    def candidates(self):
        return {
            "macro_doc": "GDP inflation employment unemployment federal reserve monetary policy interest rates treasury yield",
            "regime_doc": "regime clustering transition hidden markov model risk-on risk-off volatility state phase",
            "crypto_doc": "bitcoin ethereum cryptocurrency blockchain defi staking mining hash rate mempool",
            "risk_doc": "risk management drawdown sharpe sortino VaR CVaR portfolio hedge tail risk stress test",
        }

    def test_selects_relevant(self, candidates):
        result = select_knowledge(
            "What is GDP growth and inflation outlook?",
            candidates,
        )
        names = [name for name, _, _ in result]
        assert names[0] == "macro_doc"  # most relevant

    def test_respects_budget(self, candidates):
        # Tiny budget should limit selections
        result = select_knowledge(
            "GDP inflation regime volatility risk",
            candidates,
            char_budget=150,  # only room for ~1 doc
        )
        assert len(result) <= 2

    def test_max_docs_limit(self, candidates):
        result = select_knowledge(
            "GDP inflation regime volatility risk crypto bitcoin",
            candidates,
            char_budget=100000,
            max_docs=2,
        )
        assert len(result) <= 2

    def test_empty_prompt(self, candidates):
        result = select_knowledge("", candidates)
        assert result == []

    def test_empty_candidates(self):
        result = select_knowledge("GDP inflation", {})
        assert result == []

    def test_orthogonality_prunes_redundant(self):
        # Two nearly identical docs — only one should be selected
        candidates = {
            "doc_a": "GDP inflation employment unemployment federal reserve monetary policy",
            "doc_b": "GDP inflation employment unemployment federal reserve monetary policy rates",
            "doc_c": "bitcoin ethereum cryptocurrency blockchain mining staking",
        }
        result = select_knowledge(
            "GDP inflation cryptocurrency bitcoin monetary policy",
            candidates,
            max_redundancy=0.85,
            char_budget=100000,
        )
        names = [n for n, _, _ in result]
        # Should not have both doc_a and doc_b (too similar)
        assert not ("doc_a" in names and "doc_b" in names)

    def test_scores_returned(self, candidates):
        result = select_knowledge(
            "regime transition volatility",
            candidates,
        )
        for name, content, score in result:
            assert isinstance(score, float)
            assert score >= 0


class TestSelectAndFormat:
    def test_returns_string(self):
        candidates = {
            "doc_a": "Content about GDP and inflation.",
            "doc_b": "Content about regime detection.",
        }
        result = select_and_format("GDP inflation outlook", candidates)
        assert isinstance(result, str)
        assert "GDP" in result

    def test_empty_returns_empty(self):
        assert select_and_format("", {}) == ""
