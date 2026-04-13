"""ALPHA-4 follow-up — confidence dampening from catalyst_aggregator into oracle.predict.

Locks the behavior: when an imminent high-impact catalyst is in the
prediction's horizon window, the EnsemblePrediction.confidence is dampened
by up to 50% AND the catalyst_proximity / catalyst_type fields are stamped
on the returned object. Direction + strength are untouched.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from oracle.engine import EnsemblePrediction


# ── Dataclass field defaults ──────────────────────────────────────────────


class TestEnsemblePredictionFields:
    def test_default_catalyst_proximity_zero(self):
        ep = EnsemblePrediction(
            ticker="AAPL", direction="bullish", score=70, confidence=0.6,
            strength=0.4, coherence=0.8, model_count=5, level="meta",
            model_votes=[], as_of=datetime.now(timezone.utc), horizon=7,
        )
        assert ep.catalyst_proximity == 0.0
        assert ep.catalyst_type is None

    def test_with_catalyst_fields(self):
        ep = EnsemblePrediction(
            ticker="AAPL", direction="bullish", score=70, confidence=0.3,
            strength=0.4, coherence=0.8, model_count=5, level="meta",
            model_votes=[], as_of=datetime.now(timezone.utc), horizon=7,
            catalyst_proximity=0.95, catalyst_type="fomc",
        )
        assert ep.catalyst_proximity == 0.95
        assert ep.catalyst_type == "fomc"


# ── Predict-path dampening ─────────────────────────────────────────────────


def _stub_predictor():
    """Build an EnsemblePredictor with stubbed factory/aggregator so we can
    exercise predict() without touching the real model loader."""
    from oracle.engine import EnsemblePredictor

    p = EnsemblePredictor.__new__(EnsemblePredictor)
    p.engine = MagicMock()
    p.factory = MagicMock()
    p.aggregator = MagicMock()

    # One model returning a confident bullish vote
    model = MagicMock()
    model.name = "test_model"
    model.min_signals = 1
    model.weight_config = {}
    p.factory.list_active_models.return_value = [model]
    p.factory.get_signals_for_model.return_value = [object()] * 3

    agg = MagicMock()
    agg.direction = "bullish"
    agg.strength = 0.6
    agg.confidence = 0.8
    agg.coherence = 0.9
    agg.signal_count = 3
    p.aggregator.aggregate.return_value = agg

    p._get_hit_rate = MagicMock(return_value=0.6)
    p._get_bucket_weight = MagicMock(return_value=1.0)
    return p


class TestPredictCatalystDampening:
    def test_no_catalyst_no_dampening(self):
        p = _stub_predictor()
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps:
            ps.return_value = {"score": 0.0, "catalyst_type": None,
                               "nearest": None, "days_to_event": None,
                               "window_density": 0}
            result = p.predict("AAPL", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        assert result.catalyst_proximity == 0.0
        assert result.catalyst_type is None
        # Baseline confidence with stub votes (vote_weight=0.48, sum*conf/tw):
        # 0.48 * 0.8 / 0.48 = 0.8 — not dampened
        assert abs(result.confidence - 0.8) < 0.01

    def test_imminent_fomc_halves_confidence(self):
        p = _stub_predictor()
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps:
            ps.return_value = {"score": 1.0, "catalyst_type": "fomc",
                               "nearest": object(), "days_to_event": 0,
                               "window_density": 1}
            result = p.predict("SPY", as_of=datetime(2026, 4, 29, tzinfo=timezone.utc))
        assert result.catalyst_proximity == 1.0
        assert result.catalyst_type == "fomc"
        # 1.0 proximity → multiply confidence by (1 - 0.5*1.0) = 0.5
        # baseline 0.8 → dampened 0.4
        assert abs(result.confidence - 0.4) < 0.01

    def test_partial_proximity_partial_dampening(self):
        p = _stub_predictor()
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps:
            ps.return_value = {"score": 0.4, "catalyst_type": "earnings",
                               "nearest": object(), "days_to_event": 5,
                               "window_density": 1}
            result = p.predict("AAPL", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        # 0.4 proximity → factor (1 - 0.5*0.4) = 0.8
        # baseline 0.8 → dampened 0.64
        assert abs(result.confidence - 0.64) < 0.01
        assert result.catalyst_type == "earnings"

    def test_direction_and_strength_unchanged_by_dampening(self):
        p = _stub_predictor()
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps:
            ps.return_value = {"score": 1.0, "catalyst_type": "fomc",
                               "nearest": object(), "days_to_event": 0,
                               "window_density": 1}
            result = p.predict("SPY", as_of=datetime(2026, 4, 29, tzinfo=timezone.utc))
        # Even with maximum dampening, direction stays bullish + strength
        # is still positive (the recommender will trade, just smaller)
        assert result.direction == "bullish"
        assert result.strength > 0

    def test_catalyst_aggregator_failure_non_fatal(self):
        p = _stub_predictor()
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps:
            ps.side_effect = RuntimeError("aggregator down")
            result = p.predict("AAPL", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        # Defensive try/except → proximity stays 0, predict still returns
        assert result.catalyst_proximity == 0.0
        assert result.direction == "bullish"
