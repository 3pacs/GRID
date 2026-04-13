"""CAT-182 — consensus crowdedness detector tests."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from intelligence.consensus_crowdedness import (
    _CROWDED_THRESHOLD,
    _DAMPING_CROWDED,
    _DAMPING_NEUTRAL,
    CrowdednessPenalty,
    CrowdednessResult,
    _normalize,
    _normalize_options_skew,
    compose_crowdedness,
    compute_crowdedness,
    compute_penalty,
)


class TestNormalize:
    def test_zero_raw_zero(self):
        assert _normalize(0, 0.25) == 0.0

    def test_at_threshold_one(self):
        assert _normalize(0.25, 0.25) == 1.0

    def test_above_threshold_clamped(self):
        assert _normalize(0.50, 0.25) == 1.0

    def test_half_threshold_half(self):
        assert _normalize(0.125, 0.25) == 0.5


class TestNormalizeOptionsSkew:
    def test_neutral_zero(self):
        assert _normalize_options_skew(1.0) == 0.0

    def test_heavy_puts_high(self):
        v = _normalize_options_skew(2.0)
        assert v == 1.0

    def test_heavy_calls_high(self):
        v = _normalize_options_skew(0.5)
        assert v == 1.0

    def test_mild_skew_moderate(self):
        v = _normalize_options_skew(1.3)
        assert 0 < v < 0.5


class TestComposeCrowdedness:
    def test_no_inputs_zero(self):
        r = compose_crowdedness(ticker="AAPL")
        assert r.score == 0.0
        assert r.is_crowded is False
        assert len(r.missing) == 5
        assert r.crowd_direction is None

    def test_high_short_interest_bearish(self):
        r = compose_crowdedness(
            ticker="GME",
            short_interest=0.30,
        )
        assert r.crowd_direction == "bearish"
        assert r.components[0].normalized == 1.0

    def test_high_institutional_bullish(self):
        r = compose_crowdedness(
            ticker="NVDA",
            institutional_pct=0.90,
            analyst_rating_avg=4.7,
        )
        assert r.crowd_direction == "bullish"

    def test_heavy_calls_override_to_bullish(self):
        r = compose_crowdedness(
            ticker="TSLA",
            short_interest=0.10,
            put_call_oi_ratio=0.4,
        )
        assert r.crowd_direction == "bullish"

    def test_heavy_puts_override_to_bearish(self):
        r = compose_crowdedness(
            ticker="TSLA",
            institutional_pct=0.90,
            put_call_oi_ratio=2.5,
        )
        assert r.crowd_direction == "bearish"

    def test_composite_score_weighted_average(self):
        r = compose_crowdedness(
            ticker="AAPL",
            short_interest=0.25,
            institutional_pct=0.85,
            analyst_rating_avg=4.5,
            media_articles_week=50,
            put_call_oi_ratio=1.0,
        )
        assert 0.80 < r.score < 0.90

    def test_is_crowded_threshold(self):
        r_not = compose_crowdedness(
            ticker="AAPL",
            short_interest=0.10,
            institutional_pct=0.50,
        )
        assert r_not.is_crowded is False

        r_crowded = compose_crowdedness(
            ticker="GME",
            short_interest=0.30,
            institutional_pct=0.90,
            analyst_rating_avg=4.7,
            media_articles_week=60,
        )
        assert r_crowded.is_crowded is True

    def test_missing_components_tracked(self):
        r = compose_crowdedness(
            ticker="AAPL",
            short_interest=0.10,
        )
        assert "institutional" in r.missing
        assert "media_velocity" in r.missing


class TestComputePenalty:
    def test_not_crowded_no_damping(self):
        result = compose_crowdedness(
            ticker="AAPL", short_interest=0.05,
        )
        penalty = compute_penalty(result, "bullish")
        assert penalty.multiplier == _DAMPING_NEUTRAL
        assert penalty.aligned is False

    def test_crowded_aligned_dampens(self):
        result = compose_crowdedness(
            ticker="GME",
            short_interest=0.30,
            institutional_pct=0.90,
            analyst_rating_avg=4.7,
            media_articles_week=60,
        )
        assert result.is_crowded
        penalty = compute_penalty(result, "bearish")
        if result.crowd_direction == "bearish":
            assert penalty.multiplier == _DAMPING_CROWDED
            assert penalty.aligned is True

    def test_crowded_opposite_no_damping(self):
        result = compose_crowdedness(
            ticker="GME",
            short_interest=0.30,
            institutional_pct=0.90,
        )
        penalty = compute_penalty(result, "bullish")
        assert penalty.multiplier == _DAMPING_NEUTRAL
        assert penalty.aligned is False

    def test_crowdedness_penalty_to_dict(self):
        result = compose_crowdedness(ticker="AAPL", short_interest=0.10)
        penalty = compute_penalty(result, "bullish")
        d = penalty.to_dict()
        for k in ("ticker", "oracle_direction", "crowd_direction",
                  "crowdedness_score", "aligned", "multiplier", "reason"):
            assert k in d


class TestComputeCrowdednessDB:
    def test_db_paths_non_fatal(self):
        eng = MagicMock()
        eng.connect.side_effect = RuntimeError("db down")
        result = compute_crowdedness(eng, "AAPL")
        assert result.score == 0.0

    def test_partial_db_data(self):
        with patch("intelligence.consensus_crowdedness._read_short_interest", return_value=0.20), \
             patch("intelligence.consensus_crowdedness._read_media_velocity", return_value=40), \
             patch("intelligence.consensus_crowdedness._read_options_pcr", return_value=1.5):
            eng = MagicMock()
            result = compute_crowdedness(eng, "TSLA")
        assert len(result.components) == 3
        assert len(result.missing) == 2


class TestDataclassRoundtrip:
    def test_result_to_dict(self):
        r = compose_crowdedness(
            ticker="AAPL", short_interest=0.20,
        )
        d = r.to_dict()
        assert d["ticker"] == "AAPL"
        assert "components" in d
        assert "missing" in d
