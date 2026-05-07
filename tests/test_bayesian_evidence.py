"""CAT-178 — Bayesian evidence combiner tests."""
from __future__ import annotations

import math


from intelligence.bayesian_evidence import (
    EvidenceItem,
    combine_evidence,
    from_oracle_votes,
    logit,
    sigmoid,
)


class TestLogitSigmoid:
    def test_logit_half_is_zero(self):
        assert abs(logit(0.5)) < 1e-6

    def test_sigmoid_zero_is_half(self):
        assert abs(sigmoid(0) - 0.5) < 1e-6

    def test_inverse_relationship(self):
        for p in [0.1, 0.3, 0.5, 0.7, 0.9]:
            assert abs(sigmoid(logit(p)) - p) < 1e-6

    def test_logit_clamps_zero(self):
        val = logit(0)
        assert math.isfinite(val)
        assert val < -10

    def test_logit_clamps_one(self):
        val = logit(1)
        assert math.isfinite(val)
        assert val > 10

    def test_sigmoid_large_positive_saturates(self):
        assert sigmoid(1000) == 1.0

    def test_sigmoid_large_negative_saturates(self):
        assert sigmoid(-1000) == 0.0


class TestCombineEvidenceBasic:
    def test_empty_returns_prior(self):
        result = combine_evidence([], prior=0.3)
        assert result.posterior == 0.3
        assert result.n_effective == 0.0

    def test_single_signal_shifts_posterior(self):
        evidence = [EvidenceItem("sig1", probability=0.8)]
        result = combine_evidence(evidence, prior=0.5)
        assert result.posterior > 0.5
        assert result.posterior < 0.9

    def test_neutral_signal_no_shift(self):
        evidence = [EvidenceItem("sig1", probability=0.5)]
        result = combine_evidence(evidence, prior=0.5)
        assert abs(result.posterior - 0.5) < 1e-6

    def test_multiple_agreeing_signals_accumulate(self):
        evidence = [
            EvidenceItem("a", probability=0.7),
            EvidenceItem("b", probability=0.7),
            EvidenceItem("c", probability=0.7),
        ]
        result = combine_evidence(evidence, prior=0.5)
        assert result.posterior > 0.7

    def test_disagreeing_signals_cancel(self):
        evidence = [
            EvidenceItem("a", probability=0.7),
            EvidenceItem("b", probability=0.3),
        ]
        result = combine_evidence(evidence, prior=0.5)
        assert abs(result.posterior - 0.5) < 0.01

    def test_strong_prior_resists_weak_evidence(self):
        evidence = [EvidenceItem("sig1", probability=0.55)]
        result = combine_evidence(evidence, prior=0.9)
        assert result.posterior > 0.85


class TestCorrelationAdjustment:
    def test_same_family_collapses_to_one(self):
        evidence = [
            EvidenceItem("a1", probability=0.8, family="options"),
            EvidenceItem("a2", probability=0.8, family="options"),
            EvidenceItem("a3", probability=0.8, family="options"),
            EvidenceItem("a4", probability=0.8, family="options"),
        ]
        result_adj = combine_evidence(evidence, prior=0.5, correlation_adjust=True)
        result_naive = combine_evidence(evidence, prior=0.5, correlation_adjust=False)
        assert result_naive.posterior > result_adj.posterior
        single = combine_evidence(
            [EvidenceItem("x", probability=0.8, family="options")],
            prior=0.5,
        )
        assert abs(result_adj.posterior - single.posterior) < 1e-3

    def test_different_families_independent(self):
        evidence = [
            EvidenceItem("a", probability=0.7, family="options"),
            EvidenceItem("b", probability=0.7, family="insider"),
            EvidenceItem("c", probability=0.7, family="news"),
        ]
        result = combine_evidence(evidence, prior=0.5, correlation_adjust=True)
        assert result.posterior > 0.8

    def test_mixed_families(self):
        evidence = [
            EvidenceItem("a", probability=0.7, family="options"),
            EvidenceItem("b", probability=0.7, family="options"),
            EvidenceItem("c", probability=0.7, family="insider"),
        ]
        result = combine_evidence(evidence, prior=0.5, correlation_adjust=True)
        assert result.n_effective == 2

    def test_ungrouped_items_independent(self):
        evidence = [
            EvidenceItem("a", probability=0.8),
            EvidenceItem("b", probability=0.8),
        ]
        result = combine_evidence(evidence, prior=0.5, correlation_adjust=True)
        assert result.posterior > 0.8

    def test_family_shares_tracked(self):
        evidence = [
            EvidenceItem("a", probability=0.7, family="options"),
            EvidenceItem("b", probability=0.7, family="options"),
            EvidenceItem("c", probability=0.7, family="insider"),
        ]
        result = combine_evidence(evidence, prior=0.5)
        assert result.family_shares["options"] == 2
        assert result.family_shares["insider"] == 1


class TestWeights:
    def test_weight_half_shrinks_effect(self):
        full = combine_evidence(
            [EvidenceItem("a", probability=0.8, weight=1.0)],
            prior=0.5,
        )
        half = combine_evidence(
            [EvidenceItem("a", probability=0.8, weight=0.5)],
            prior=0.5,
        )
        assert abs(half.posterior - 0.5) < abs(full.posterior - 0.5)

    def test_zero_weight_no_shift(self):
        result = combine_evidence(
            [EvidenceItem("a", probability=0.9, weight=0.0)],
            prior=0.5,
        )
        assert abs(result.posterior - 0.5) < 1e-6


class TestFromOracleVotes:
    def test_bullish_votes_pull_positive(self):
        votes = [
            {"name": "m1", "direction": "bullish", "confidence": 0.7, "family": "flows"},
            {"name": "m2", "direction": "bullish", "confidence": 0.6, "family": "options"},
        ]
        result = from_oracle_votes(votes, prior=0.5)
        assert result.posterior > 0.5

    def test_bearish_votes_inverted(self):
        votes = [
            {"name": "m1", "direction": "bearish", "confidence": 0.8, "family": "flows"},
        ]
        result = from_oracle_votes(votes, prior=0.5)
        assert result.posterior < 0.5

    def test_neutral_votes_dropped(self):
        votes = [
            {"name": "m1", "direction": "neutral", "confidence": 0.9, "family": "x"},
            {"name": "m2", "direction": "bullish", "confidence": 0.7, "family": "y"},
        ]
        result = from_oracle_votes(votes, prior=0.5)
        assert len(result.evidence) == 1

    def test_cancellation(self):
        votes = [
            {"name": "m1", "direction": "bullish", "confidence": 0.7, "family": "a"},
            {"name": "m2", "direction": "bearish", "confidence": 0.7, "family": "b"},
        ]
        result = from_oracle_votes(votes, prior=0.5)
        assert abs(result.posterior - 0.5) < 0.01


class TestDataclassSerialization:
    def test_result_to_dict(self):
        evidence = [EvidenceItem("a", probability=0.7, family="options")]
        result = combine_evidence(evidence, prior=0.5)
        d = result.to_dict()
        for k in ("prior", "posterior", "log_odds_prior", "log_odds_posterior",
                  "evidence", "n_effective", "family_shares"):
            assert k in d

    def test_evidence_item_to_dict(self):
        item = EvidenceItem("test", probability=0.6, weight=0.8, family="flows")
        d = item.to_dict()
        assert d["name"] == "test"
        assert d["probability"] == 0.6
        assert d["family"] == "flows"
