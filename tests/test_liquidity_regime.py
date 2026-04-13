"""ALPHA-5 — liquidity regime classifier tests.

Pure-function tests on classify_from_series + apply_to_confidence. DB-touching
classify_current_regime path is exercised by a smoke run on the server.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from intelligence.liquidity_regime import (
    ALL_STATES,
    STATE_CONFIDENCE_MULTIPLIER,
    STATE_CRISIS,
    STATE_EXPANSION,
    STATE_EXPANSION_STRONG,
    STATE_NEUTRAL,
    STATE_TIGHTENING,
    _MIN_HISTORY_ROWS,
    apply_to_confidence,
    classify_from_series,
)


# ── Helpers ───────────────────────────────────────────────────────────────


def _history(n, base=5e12, trend=0.0):
    """Linearly trending history of n points."""
    return [base + i * trend for i in range(n)]


def _weekly_changes(n, mean=0.0, std=1e10):
    """Pseudorandom weekly changes with fixed mean/std."""
    import math
    out = []
    for i in range(n):
        out.append(mean + std * math.sin(i / 3.0))
    return out


_TODAY = date(2026, 4, 13)


# ── State assignment ──────────────────────────────────────────────────────


class TestClassifyFromSeries:
    def test_insufficient_history_neutral(self):
        r = classify_from_series(
            history=_history(10),
            weekly_changes=_weekly_changes(10),
            current_level=5e12,
            current_weekly=0.0,
            current_monthly=0.0,
            as_of=_TODAY,
        )
        assert r.state == STATE_NEUTRAL
        assert "insufficient history" in r.reason
        assert r.confidence_multiplier == STATE_CONFIDENCE_MULTIPLIER[STATE_NEUTRAL]

    def test_expansion_strong_at_high_pct_with_momentum(self):
        hist = _history(100, base=5e12, trend=1e10)  # rising
        weekly = [1e10] * 100
        r = classify_from_series(
            history=hist,
            weekly_changes=weekly,
            current_level=hist[-1] + 5e11,  # above the trend → high pct
            current_weekly=5e11,             # big positive jump → high z
            current_monthly=1e12,
            as_of=_TODAY,
        )
        assert r.state == STATE_EXPANSION_STRONG
        assert r.level_percentile >= 85
        assert r.weekly_change_z > 1.0

    def test_crisis_at_low_pct_with_down_momentum(self):
        hist = _history(100, base=5e12, trend=1e10)
        weekly = [1e10] * 100
        r = classify_from_series(
            history=hist,
            weekly_changes=weekly,
            current_level=hist[0] - 5e11,  # well below everything
            current_weekly=-5e11,           # big negative jump
            current_monthly=-1e12,
            as_of=_TODAY,
        )
        assert r.state == STATE_CRISIS
        assert r.level_percentile <= 10

    def test_tightening_below_40th_with_negative_monthly(self):
        hist = _history(100, base=5e12, trend=1e10)
        weekly = [1e10] * 100
        r = classify_from_series(
            history=hist,
            weekly_changes=weekly,
            current_level=hist[20],  # ~20th percentile
            current_weekly=-1e9,
            current_monthly=-1e10,    # negative 1m
            as_of=_TODAY,
        )
        assert r.state == STATE_TIGHTENING

    def test_expansion_above_60th_with_positive_monthly(self):
        hist = _history(100, base=5e12, trend=1e10)
        weekly = [1e10] * 100
        r = classify_from_series(
            history=hist,
            weekly_changes=weekly,
            current_level=hist[75],  # ~75th percentile (below STRONG cutoff of 85)
            current_weekly=1e9,
            current_monthly=1e10,
            as_of=_TODAY,
        )
        assert r.state == STATE_EXPANSION

    def test_neutral_default(self):
        hist = _history(100, base=5e12, trend=1e10)
        weekly = [1e10] * 100
        r = classify_from_series(
            history=hist,
            weekly_changes=weekly,
            current_level=hist[50],  # median
            current_weekly=0.0,
            current_monthly=0.0,
            as_of=_TODAY,
        )
        assert r.state == STATE_NEUTRAL

    def test_sample_size_in_result(self):
        hist = _history(100)
        r = classify_from_series(
            history=hist,
            weekly_changes=_weekly_changes(100),
            current_level=hist[-1],
            current_weekly=0.0,
            current_monthly=0.0,
            as_of=_TODAY,
        )
        assert r.sample_size == 100

    def test_to_dict_shape(self):
        hist = _history(50)
        r = classify_from_series(
            history=hist,
            weekly_changes=_weekly_changes(50),
            current_level=hist[-1],
            current_weekly=0.0,
            current_monthly=0.0,
            as_of=_TODAY,
        )
        d = r.to_dict()
        for key in (
            "state", "as_of", "net_liquidity", "level_percentile",
            "weekly_change", "weekly_change_z", "monthly_change",
            "confidence_multiplier", "sample_size", "reason",
        ):
            assert key in d


# ── apply_to_confidence ───────────────────────────────────────────────────


class TestApplyToConfidence:
    def test_crisis_shrinks(self):
        assert apply_to_confidence(0.5, STATE_CRISIS) < 0.5

    def test_tightening_shrinks(self):
        assert apply_to_confidence(0.5, STATE_TIGHTENING) < 0.5

    def test_neutral_noop(self):
        assert apply_to_confidence(0.5, STATE_NEUTRAL) == 0.5

    def test_expansion_amplifies(self):
        assert apply_to_confidence(0.5, STATE_EXPANSION) > 0.5

    def test_expansion_strong_amplifies_more(self):
        e = apply_to_confidence(0.5, STATE_EXPANSION)
        s = apply_to_confidence(0.5, STATE_EXPANSION_STRONG)
        assert s > e

    def test_clamps_to_one(self):
        assert apply_to_confidence(0.95, STATE_EXPANSION_STRONG) <= 1.0

    def test_clamps_to_zero(self):
        assert apply_to_confidence(-0.1, STATE_NEUTRAL) == 0.0

    def test_unknown_state_noop(self):
        assert apply_to_confidence(0.5, "UNKNOWN") == 0.5


# ── EnsemblePrediction integration ────────────────────────────────────────


class TestPredictLiquidityWiring:
    def _stub_predictor(self):
        from oracle.engine import EnsemblePredictor
        p = EnsemblePredictor.__new__(EnsemblePredictor)
        p.engine = MagicMock()
        p.factory = MagicMock()
        p.aggregator = MagicMock()

        m = MagicMock()
        m.name = "test_model"
        m.min_signals = 1
        m.weight_config = {}
        p.factory.list_active_models.return_value = [m]
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

    def test_neutral_regime_no_change(self):
        from datetime import datetime, timezone
        from intelligence.liquidity_regime import LiquidityRegimeResult
        p = self._stub_predictor()
        regime = LiquidityRegimeResult(
            state=STATE_NEUTRAL, as_of=_TODAY, net_liquidity=5e12,
            level_percentile=50.0, weekly_change=0.0, weekly_change_z=0.0,
            monthly_change=0.0, confidence_multiplier=1.0,
            sample_size=100, reason="neutral",
        )
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps, \
             patch("intelligence.liquidity_regime.classify_current_regime", return_value=regime):
            ps.return_value = {"score": 0.0, "catalyst_type": None,
                               "nearest": None, "days_to_event": None,
                               "window_density": 0}
            result = p.predict("SPY", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        assert result.liquidity_state == STATE_NEUTRAL
        assert abs(result.confidence - 0.8) < 0.01  # untouched

    def test_crisis_shrinks_confidence(self):
        from datetime import datetime, timezone
        from intelligence.liquidity_regime import LiquidityRegimeResult
        p = self._stub_predictor()
        regime = LiquidityRegimeResult(
            state=STATE_CRISIS, as_of=_TODAY, net_liquidity=4e12,
            level_percentile=5.0, weekly_change=-5e11, weekly_change_z=-3.0,
            monthly_change=-1e12, confidence_multiplier=0.60,
            sample_size=100, reason="crisis",
        )
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps, \
             patch("intelligence.liquidity_regime.classify_current_regime", return_value=regime):
            ps.return_value = {"score": 0.0, "catalyst_type": None,
                               "nearest": None, "days_to_event": None,
                               "window_density": 0}
            result = p.predict("SPY", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        assert result.liquidity_state == STATE_CRISIS
        # Baseline 0.8 × 0.60 = 0.48
        assert abs(result.confidence - 0.48) < 0.01

    def test_liquidity_failure_non_fatal(self):
        from datetime import datetime, timezone
        p = self._stub_predictor()
        with patch("intelligence.catalyst_aggregator.proximity_score") as ps, \
             patch("intelligence.liquidity_regime.classify_current_regime", side_effect=RuntimeError("no db")):
            ps.return_value = {"score": 0.0, "catalyst_type": None,
                               "nearest": None, "days_to_event": None,
                               "window_density": 0}
            result = p.predict("SPY", as_of=datetime(2026, 5, 1, tzinfo=timezone.utc))
        # Defensive try/except — liquidity_state stays empty, confidence untouched
        assert result.liquidity_state == ""
        assert abs(result.confidence - 0.8) < 0.01
