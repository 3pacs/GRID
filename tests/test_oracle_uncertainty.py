"""ALPHA-11 — uncertainty bounds + confidence interval tests.

Pure-function tests on oracle/uncertainty.py + a small integration check
that EnsemblePrediction carries the new lower/upper fields.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from oracle.uncertainty import (
    ConfidenceInterval,
    _critical_value,
    _vote_confidence_values,
    compute_confidence_interval,
)


class TestCriticalValue:
    def test_large_sample_uses_normal(self):
        assert _critical_value(50, 0.10) == 1.645
        assert _critical_value(50, 0.05) == 1.960

    def test_small_sample_uses_t(self):
        assert _critical_value(4, 0.05) == 2.776  # df=4 → 2.776
        assert _critical_value(9, 0.05) == 2.262

    def test_unknown_alpha_falls_back_to_90(self):
        v = _critical_value(100, 0.99)
        assert v == 1.645

    def test_df_below_table_uses_smallest(self):
        v = _critical_value(0, 0.05)
        assert v == 12.706  # df=1 entry


class TestVoteConfidenceValues:
    def test_clamps_out_of_range(self):
        votes = [{"confidence": 1.5}, {"confidence": -0.3}]
        assert _vote_confidence_values(votes) == [1.0, 0.0]

    def test_skips_missing_and_invalid(self):
        votes = [
            {"confidence": 0.5},
            {"confidence": None},
            {"confidence": "bad"},
            {"other": 1},
        ]
        assert _vote_confidence_values(votes) == [0.5]


class TestComputeConfidenceInterval:
    def test_singleton_zero_width(self):
        votes = [{"confidence": 0.6}]
        ci = compute_confidence_interval(votes, point_confidence=0.6)
        assert ci.lower == 0.6
        assert ci.upper == 0.6
        assert ci.width == 0
        assert ci.n == 1

    def test_uniform_confidence_zero_width(self):
        votes = [{"confidence": 0.5}] * 5
        ci = compute_confidence_interval(votes, point_confidence=0.5)
        assert ci.width == 0.0

    def test_bimodal_wide_interval(self):
        votes = [{"confidence": 0.2}, {"confidence": 0.8}] * 3
        ci = compute_confidence_interval(votes, point_confidence=0.5)
        assert ci.width > 0.1
        assert ci.lower < 0.5
        assert ci.upper > 0.5

    def test_clamped_to_zero_one(self):
        votes = [{"confidence": 0.0}, {"confidence": 1.0}] * 3
        ci = compute_confidence_interval(votes, point_confidence=0.95)
        assert ci.upper <= 1.0
        assert ci.lower >= 0.0

    def test_wider_at_smaller_sample(self):
        # Smaller ensembles should have wider intervals (t > z)
        votes_small = [{"confidence": 0.4}, {"confidence": 0.6}]
        votes_large = [{"confidence": 0.4}, {"confidence": 0.6}] * 20
        ci_small = compute_confidence_interval(votes_small, 0.5)
        ci_large = compute_confidence_interval(votes_large, 0.5)
        assert ci_small.width > ci_large.width

    def test_to_dict_shape(self):
        votes = [{"confidence": 0.3}, {"confidence": 0.7}]
        d = compute_confidence_interval(votes, 0.5).to_dict()
        for k in ("point", "lower", "upper", "width", "alpha", "n", "sem", "level"):
            assert k in d
        assert d["level"] == "90%"

    def test_empty_votes(self):
        ci = compute_confidence_interval([], 0.5)
        assert ci.lower == 0.5
        assert ci.upper == 0.5
        assert ci.n == 0


class TestEnsemblePredictionFields:
    def test_default_bounds_zero(self):
        from oracle.engine import EnsemblePrediction
        ep = EnsemblePrediction(
            ticker="AAPL", direction="bullish", score=70, confidence=0.6,
            strength=0.4, coherence=0.8, model_count=5, level="meta",
            model_votes=[], as_of=datetime.now(timezone.utc), horizon=7,
        )
        assert ep.confidence_lower == 0.0
        assert ep.confidence_upper == 0.0


def _stub_predictor(confs):
    from oracle.engine import EnsemblePredictor
    p = EnsemblePredictor.__new__(EnsemblePredictor)
    p.engine = MagicMock()
    p.factory = MagicMock()
    p.aggregator = MagicMock()

    models = []
    aggs = []
    for i, c in enumerate(confs):
        m = MagicMock()
        m.name = f"model_{i}"
        m.min_signals = 1
        m.weight_config = {}
        models.append(m)
        agg = MagicMock()
        agg.direction = "bullish"
        agg.strength = 0.6
        agg.confidence = c
        agg.coherence = 0.9
        agg.signal_count = 3
        aggs.append(agg)

    p.factory.list_active_models.return_value = models
    p.factory.get_signals_for_model.return_value = [object()] * 3
    p.aggregator.aggregate.side_effect = aggs
    p._get_hit_rate = MagicMock(return_value=0.6)
    p._get_bucket_weight = MagicMock(return_value=1.0)
    return p


class TestPredictBoundsWiring:
    def test_uniform_votes_narrow_bounds(self):
        p = _stub_predictor([0.7, 0.7, 0.7, 0.7])
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps, \
             patch("intelligence.liquidity_regime.classify_current_regime", side_effect=RuntimeError("no db")):
            ps.return_value = {"score": 0.0, "catalyst_type": None,
                               "nearest": None, "days_to_event": None,
                               "window_density": 0}
            result = p.predict("AAPL", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        assert abs(result.confidence_upper - result.confidence_lower) < 0.01

    def test_spread_votes_wider_bounds(self):
        p = _stub_predictor([0.2, 0.4, 0.6, 0.9])
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps, \
             patch("intelligence.liquidity_regime.classify_current_regime", side_effect=RuntimeError("no db")):
            ps.return_value = {"score": 0.0, "catalyst_type": None,
                               "nearest": None, "days_to_event": None,
                               "window_density": 0}
            result = p.predict("AAPL", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        assert result.confidence_upper > result.confidence_lower
        assert (result.confidence_upper - result.confidence_lower) > 0.1

    def test_bounds_bracket_point(self):
        p = _stub_predictor([0.3, 0.5, 0.7])
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps, \
             patch("intelligence.liquidity_regime.classify_current_regime", side_effect=RuntimeError("no db")):
            ps.return_value = {"score": 0.0, "catalyst_type": None,
                               "nearest": None, "days_to_event": None,
                               "window_density": 0}
            result = p.predict("AAPL", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        assert result.confidence_lower <= result.confidence
        assert result.confidence <= result.confidence_upper
