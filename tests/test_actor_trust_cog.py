"""INTEL-2 — actor trust-or-cog classifier tests.

Pure-function tests on score_one_actor + the classification thresholds.
DB-touching paths are covered by integration tests elsewhere; this file
keeps the math auditable without requiring a live DB.
"""
from __future__ import annotations

from intelligence.actor_trust_cog import (
    _THRESHOLD_COG,
    _THRESHOLD_TRUST,
    _classify,
    _credibility_component,
    _centrality_component,
    _precision_component,
    score_one_actor,
)


# ── Component helpers ──────────────────────────────────────────────────────


class TestPrecisionComponent:
    def test_below_min_signals_returns_zero(self):
        c, b = _precision_component(correct=2, total=2, lead_days=10)
        assert c == 0.0
        assert "below min signal volume" in b["note"]

    def test_perfect_precision_with_lead_is_positive(self):
        c, _ = _precision_component(correct=10, total=10, lead_days=14)
        assert c > 0.5  # near max trust

    def test_zero_precision_with_lead_is_negative(self):
        c, _ = _precision_component(correct=0, total=10, lead_days=14)
        assert c < -0.5

    def test_50_percent_precision_is_zero(self):
        c, _ = _precision_component(correct=5, total=10, lead_days=7)
        # Centered at 0; lead factor amplifies but multiplied by 0.
        assert abs(c) < 0.05

    def test_negative_lead_inverts_sign(self):
        c, _ = _precision_component(correct=10, total=10, lead_days=-7)
        assert c < 0  # cog

    def test_zero_lead_caps_amplification(self):
        c, _ = _precision_component(correct=10, total=10, lead_days=0)
        # Centered=1.0, lead_factor=0, multiplier=0.5 → 0.5
        assert 0.4 <= c <= 0.6


class TestCentralityComponent:
    def test_zero_pagerank_zero_contribution(self):
        c, _ = _centrality_component(0.0)
        assert c == 0.0

    def test_high_pagerank_saturates_at_one(self):
        c, _ = _centrality_component(0.05)  # 5x cap
        assert c == 1.0

    def test_mid_pagerank_proportional(self):
        c, _ = _centrality_component(0.005)  # half of 0.01 cap
        assert 0.4 <= c <= 0.6

    def test_none_pagerank_zero(self):
        c, _ = _centrality_component(None)
        assert c == 0.0


class TestCredibilityComponent:
    def test_default_05_is_zero(self):
        c, _ = _credibility_component(0.5)
        assert c == 0.0

    def test_perfect_credibility_is_one(self):
        c, _ = _credibility_component(1.0)
        assert c == 1.0

    def test_zero_credibility_is_minus_one(self):
        c, _ = _credibility_component(0.0)
        assert c == -1.0


# ── Classifier ─────────────────────────────────────────────────────────────


class TestClassify:
    def test_high_score_is_trust(self):
        assert _classify(0.5) == "trust"
        assert _classify(_THRESHOLD_TRUST) == "trust"

    def test_low_score_is_cog(self):
        assert _classify(-0.5) == "cog"
        assert _classify(_THRESHOLD_COG) == "cog"

    def test_middle_score_is_mixed(self):
        assert _classify(0.0) == "mixed"
        assert _classify(0.1) == "mixed"
        assert _classify(-0.2) == "mixed"


# ── End-to-end pure-function score ─────────────────────────────────────────


class TestScoreOneActor:
    def test_high_signal_high_centrality_is_trust(self):
        s = score_one_actor(
            lever_id=1,
            name="Test Whale",
            category="institutional",
            correct_signals=18,
            total_signals=20,
            avg_lead_time_days=14,
            pagerank=0.02,
            credibility_score=0.9,
        )
        assert s.classification == "trust"
        assert s.score > _THRESHOLD_TRUST
        assert s.precision_component > 0
        assert s.centrality_component > 0
        assert s.credibility_component > 0

    def test_negative_lead_low_credibility_is_cog(self):
        s = score_one_actor(
            lever_id=2,
            name="Lagging Reactor",
            category="insider",
            correct_signals=2,
            total_signals=10,
            avg_lead_time_days=-7,
            pagerank=0.0,
            credibility_score=0.2,
        )
        assert s.classification == "cog"
        assert s.score < _THRESHOLD_COG

    def test_below_min_signals_unknown_path(self):
        s = score_one_actor(
            lever_id=3,
            name="Newcomer",
            category="insider",
            correct_signals=1,
            total_signals=2,
            avg_lead_time_days=7,
            pagerank=None,
            credibility_score=None,
        )
        # Precision component is forced to 0 → mixed/unknown territory.
        assert s.classification in ("mixed", "unknown")
        assert s.precision_component == 0.0

    def test_breakdown_inputs_present(self):
        s = score_one_actor(
            lever_id=4,
            name="Test",
            category="insider",
            correct_signals=10,
            total_signals=10,
            avg_lead_time_days=7,
            pagerank=0.005,
            credibility_score=0.7,
        )
        assert "precision" in s.inputs
        assert "centrality" in s.inputs
        assert "credibility" in s.inputs
        assert "weights" in s.inputs
        assert s.inputs["weights"]["precision"] == 0.60

    def test_score_clamped_to_range(self):
        # Even with all inputs maxed the score must stay in [-1, 1].
        s = score_one_actor(
            lever_id=5,
            name="Max",
            category="institutional",
            correct_signals=100,
            total_signals=100,
            avg_lead_time_days=100,
            pagerank=1.0,
            credibility_score=1.0,
        )
        assert -1.0 <= s.score <= 1.0
