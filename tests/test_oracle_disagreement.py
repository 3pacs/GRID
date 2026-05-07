"""ALPHA-10 — ensemble disagreement tests.

Pure-function tests on oracle/disagreement.py math plus a small integration
check that EnsemblePrediction carries the new fields and predict() applies
the dampening.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _neutralize_sweep_multipliers():
    """No-op SWEEP multipliers so disagreement-specific tests see only disagreement."""
    fci_stub = MagicMock()
    fci_stub.score = 0.0
    fci_stub.regime = "NEUTRAL"
    shapley_stub = MagicMock()
    shapley_stub.fragility_multiplier = 1.0
    shapley_stub.top_contributor = ""
    shapley_stub.top_share = 0.0
    crowd_stub = MagicMock()
    crowd_stub.score = 0.0
    crowd_stub.crowd_direction = None
    penalty_stub = MagicMock()
    penalty_stub.multiplier = 1.0
    penalty_stub.aligned = False
    with patch("intelligence.financial_conditions_index.compute_fci",
               return_value=fci_stub), \
         patch("intelligence.shapley_attribution.attribute_votes",
               return_value=shapley_stub), \
         patch("intelligence.consensus_crowdedness.compute_crowdedness",
               return_value=crowd_stub), \
         patch("intelligence.consensus_crowdedness.compute_penalty",
               return_value=penalty_stub), \
         patch("intelligence.market_implied_prob.options_implied_probability",
               return_value=None):
        yield

from oracle.disagreement import (
    _MAX_CONF_VARIANCE,
    _MAX_ENTROPY,
    compute_metrics,
    confidence_variance,
    directional_entropy,
    disagreement_score,
)


# ── Directional entropy ───────────────────────────────────────────────────


class TestDirectionalEntropy:
    def test_unanimous_is_zero(self):
        votes = [
            {"direction": "bullish", "vote_weight": 1.0, "confidence": 0.6},
            {"direction": "bullish", "vote_weight": 1.0, "confidence": 0.5},
            {"direction": "bullish", "vote_weight": 1.0, "confidence": 0.7},
        ]
        assert directional_entropy(votes) == 0.0

    def test_fifty_fifty_bull_bear(self):
        votes = [
            {"direction": "bullish", "vote_weight": 1.0},
            {"direction": "bearish", "vote_weight": 1.0},
        ]
        # Binary 50/50 → 1 bit
        assert abs(directional_entropy(votes) - 1.0) < 1e-6

    def test_three_way_split_maxes(self):
        votes = [
            {"direction": "bullish", "vote_weight": 1.0},
            {"direction": "bearish", "vote_weight": 1.0},
            {"direction": "neutral", "vote_weight": 1.0},
        ]
        # log2(3) ≈ 1.585
        assert abs(directional_entropy(votes) - _MAX_ENTROPY) < 1e-6

    def test_weight_respects_vote_weight(self):
        # Heavy bull vote + light bear → entropy near 0
        heavy = [
            {"direction": "bullish", "vote_weight": 10.0},
            {"direction": "bearish", "vote_weight": 0.1},
        ]
        light_split = [
            {"direction": "bullish", "vote_weight": 1.0},
            {"direction": "bearish", "vote_weight": 1.0},
        ]
        assert directional_entropy(heavy) < directional_entropy(light_split)

    def test_unknown_direction_coerced_to_neutral(self):
        votes = [{"direction": "weird", "vote_weight": 1.0}]
        # Should not crash + entropy 0 (single direction, even if coerced)
        assert directional_entropy(votes) == 0.0

    def test_empty_votes(self):
        assert directional_entropy([]) == 0.0


# ── Confidence variance ───────────────────────────────────────────────────


class TestConfidenceVariance:
    def test_uniform_confidence_zero_variance(self):
        votes = [{"confidence": 0.5}] * 5
        assert confidence_variance(votes) == 0.0

    def test_bimodal_max_variance(self):
        votes = [
            {"confidence": 0.0},
            {"confidence": 1.0},
        ]
        # Variance of [0, 1] = 0.25
        assert abs(confidence_variance(votes) - 0.25) < 1e-6

    def test_missing_confidence_treated_as_zero(self):
        votes = [
            {"confidence": 1.0},
            {"confidence": None},
        ]
        # Mean = 0.5, variance = ((1-0.5)^2 + (0-0.5)^2) / 2 = 0.25
        assert abs(confidence_variance(votes) - 0.25) < 1e-6

    def test_clamps_out_of_range(self):
        votes = [{"confidence": 1.5}, {"confidence": -0.3}]
        # Clamped to [0, 1]: variance of [1.0, 0.0] = 0.25
        assert abs(confidence_variance(votes) - 0.25) < 1e-6

    def test_empty_votes(self):
        assert confidence_variance([]) == 0.0


# ── Disagreement score composite ──────────────────────────────────────────


class TestDisagreementScore:
    def test_unanimous_uniform_is_zero(self):
        votes = [{"direction": "bullish", "vote_weight": 1.0, "confidence": 0.6}] * 4
        assert disagreement_score(votes) == 0.0

    def test_three_way_split_with_bimodal_conf_is_high(self):
        votes = [
            {"direction": "bullish", "vote_weight": 1.0, "confidence": 0.9},
            {"direction": "bearish", "vote_weight": 1.0, "confidence": 0.1},
            {"direction": "neutral", "vote_weight": 1.0, "confidence": 0.9},
        ]
        score = disagreement_score(votes)
        # Max entropy × high variance → should be close to 1
        assert score > 0.8

    def test_score_capped_at_one(self):
        votes = [
            {"direction": "bullish", "vote_weight": 1.0, "confidence": 1.0},
            {"direction": "bearish", "vote_weight": 1.0, "confidence": 0.0},
            {"direction": "neutral", "vote_weight": 1.0, "confidence": 1.0},
        ]
        assert disagreement_score(votes) <= 1.0

    def test_empty_returns_zero(self):
        assert disagreement_score([]) == 0.0


# ── compute_metrics rollup ────────────────────────────────────────────────


class TestComputeMetrics:
    def test_metrics_shape(self):
        votes = [
            {"direction": "bullish", "vote_weight": 2.0, "confidence": 0.8},
            {"direction": "bearish", "vote_weight": 1.0, "confidence": 0.4},
        ]
        m = compute_metrics(votes)
        assert m.n_votes == 2
        assert 0 <= m.directional_entropy <= _MAX_ENTROPY
        assert 0 <= m.confidence_variance <= _MAX_CONF_VARIANCE
        assert 0 <= m.disagreement_score <= 1
        assert set(m.directional_split) == {"bullish", "bearish", "neutral"}
        assert abs(sum(m.directional_split.values()) - 1.0) < 1e-6

    def test_to_dict_roundtrip(self):
        votes = [{"direction": "bullish", "vote_weight": 1.0, "confidence": 0.5}]
        d = compute_metrics(votes).to_dict()
        assert "directional_entropy" in d
        assert "confidence_variance" in d
        assert "disagreement_score" in d
        assert d["n_votes"] == 1

    def test_empty_metrics(self):
        m = compute_metrics([])
        assert m.n_votes == 0
        assert m.directional_entropy == 0.0
        assert m.disagreement_score == 0.0


# ── EnsemblePrediction integration ────────────────────────────────────────


class TestEnsemblePredictionFields:
    def test_default_disagreement_zero(self):
        from oracle.engine import EnsemblePrediction
        ep = EnsemblePrediction(
            ticker="AAPL", direction="bullish", score=70, confidence=0.6,
            strength=0.4, coherence=0.8, model_count=5, level="meta",
            model_votes=[], as_of=datetime.now(timezone.utc), horizon=7,
        )
        assert ep.disagreement_score == 0.0
        assert ep.directional_entropy == 0.0


def _stub_predictor(vote_dirs):
    """Build an EnsemblePredictor stub with N heads producing the given directions."""
    from oracle.engine import EnsemblePredictor

    p = EnsemblePredictor.__new__(EnsemblePredictor)
    p.engine = MagicMock()
    p.factory = MagicMock()
    p.aggregator = MagicMock()

    models = []
    aggs = []
    for i, d in enumerate(vote_dirs):
        m = MagicMock()
        m.name = f"model_{i}"
        m.min_signals = 1
        m.weight_config = {}
        models.append(m)
        agg = MagicMock()
        agg.direction = d
        agg.strength = 0.6
        agg.confidence = 0.8
        agg.coherence = 0.9
        agg.signal_count = 3
        aggs.append(agg)

    p.factory.list_active_models.return_value = models
    p.factory.get_signals_for_model.return_value = [object()] * 3
    p.aggregator.aggregate.side_effect = aggs
    p._get_hit_rate = MagicMock(return_value=0.6)
    p._get_bucket_weight = MagicMock(return_value=1.0)
    return p


class TestPredictDisagreementDampening:
    def test_unanimous_no_dampening(self):
        p = _stub_predictor(["bullish"] * 4)
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps:
            ps.return_value = {"score": 0.0, "catalyst_type": None,
                               "nearest": None, "days_to_event": None,
                               "window_density": 0}
            result = p.predict("AAPL", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        assert result.disagreement_score == 0.0
        assert result.directional_entropy == 0.0
        # Confidence untouched (baseline 0.8)
        assert abs(result.confidence - 0.8) < 0.01

    def test_three_way_split_dampens_confidence(self):
        p = _stub_predictor(["bullish", "bearish", "neutral"])
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps:
            ps.return_value = {"score": 0.0, "catalyst_type": None,
                               "nearest": None, "days_to_event": None,
                               "window_density": 0}
            result = p.predict("AAPL", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        assert result.disagreement_score > 0.5
        assert result.directional_entropy > 1.0
        # Confidence dampened from baseline 0.8
        assert result.confidence < 0.6

    def test_disagreement_stacks_with_catalyst(self):
        p = _stub_predictor(["bullish", "bearish"])
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps:
            ps.return_value = {"score": 1.0, "catalyst_type": "fomc",
                               "nearest": object(), "days_to_event": 0,
                               "window_density": 1}
            result = p.predict("SPY", as_of=datetime(2026, 4, 29, tzinfo=timezone.utc))
        # Catalyst 50% × disagreement ~30% → compounded ~35% of baseline
        # baseline 0.8 → ~0.28 (below the catalyst-only 0.4)
        assert result.confidence < 0.35
        assert result.catalyst_proximity == 1.0
        assert result.disagreement_score > 0.3
