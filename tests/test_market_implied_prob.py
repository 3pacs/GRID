"""ALPHA-8 — market-implied probability comparator tests."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock


from intelligence.market_implied_prob import (
    MarketImpliedProb,
    _norm_cdf,
    compare_to_oracle,
    options_implied_probability,
    options_implied_probability_from_iv,
)


class TestNormCdf:
    def test_zero_is_half(self):
        assert abs(_norm_cdf(0) - 0.5) < 1e-6

    def test_one_sigma(self):
        assert abs(_norm_cdf(1) - 0.8413) < 0.001

    def test_negative_one_sigma(self):
        assert abs(_norm_cdf(-1) - 0.1587) < 0.001


class TestOptionsImpliedFromIV:
    def test_at_money_target_near_half(self):
        # Target = spot, 30-day horizon, 30% IV → ~50% probability
        p = options_implied_probability_from_iv(
            spot=100, target_price=100, iv=0.30, days_to_expiry=30,
        )
        assert 0.45 < p < 0.55

    def test_higher_target_lower_prob(self):
        p_far = options_implied_probability_from_iv(
            spot=100, target_price=120, iv=0.30, days_to_expiry=30,
        )
        p_near = options_implied_probability_from_iv(
            spot=100, target_price=105, iv=0.30, days_to_expiry=30,
        )
        assert p_far < p_near
        assert p_far > 0
        assert p_near < 1

    def test_lower_target_returns_below_prob(self):
        p = options_implied_probability_from_iv(
            spot=100, target_price=90, iv=0.30, days_to_expiry=30,
        )
        # P(spot falls to 90 within 30d at 30% IV) — should be modest
        assert 0 < p < 0.5

    def test_zero_iv_collapses(self):
        # σ=0 means deterministic → at the spot
        p = options_implied_probability_from_iv(
            spot=100, target_price=100, iv=0, days_to_expiry=30,
        )
        # Degenerate input handling
        assert p in (0.0, 0.5, 1.0)

    def test_zero_dte_returns_neutral(self):
        p = options_implied_probability_from_iv(
            spot=100, target_price=110, iv=0.30, days_to_expiry=0,
        )
        assert p == 0.5

    def test_higher_iv_pulls_toward_half(self):
        p_low = options_implied_probability_from_iv(
            spot=100, target_price=120, iv=0.10, days_to_expiry=30,
        )
        p_high = options_implied_probability_from_iv(
            spot=100, target_price=120, iv=0.80, days_to_expiry=30,
        )
        # Higher vol → more probability mass at extremes
        assert p_high > p_low


class TestOptionsImpliedProbabilityRead:
    def _build_engine(self, row):
        eng = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        result = MagicMock()
        result.fetchone.return_value = row
        conn.execute.return_value = result
        eng.connect.return_value = conn
        return eng

    def test_reads_latest_snapshot(self):
        eng = self._build_engine((100.0, 0.30, "2026-04-13"))
        result = options_implied_probability(
            eng, "AAPL", target_move_pct=0.05, horizon_days=30,
        )
        assert result is not None
        assert result.ticker == "AAPL"
        assert result.source == "options_iv"
        assert 0 < result.prob < 1
        assert result.spot == 100.0

    def test_no_data_returns_none(self):
        eng = self._build_engine(None)
        result = options_implied_probability(
            eng, "ZZZ", target_move_pct=0.05, horizon_days=30,
        )
        assert result is None

    def test_zero_iv_returns_none(self):
        eng = self._build_engine((100.0, 0.0, "2026-04-13"))
        result = options_implied_probability(
            eng, "AAPL", target_move_pct=0.05, horizon_days=30,
        )
        assert result is None

    def test_db_error_returns_none(self):
        eng = MagicMock()
        eng.connect.side_effect = RuntimeError("db down")
        result = options_implied_probability(
            eng, "AAPL", target_move_pct=0.05, horizon_days=30,
        )
        assert result is None


class TestCompareToOracle:
    def test_aligned_no_change(self):
        r = compare_to_oracle(0.50, 0.52)
        assert r.severity == "aligned"
        assert r.confidence_multiplier == 1.00

    def test_mild_divergence_small_boost(self):
        r = compare_to_oracle(0.65, 0.55)
        assert r.severity == "mild"
        assert r.confidence_multiplier == 1.05
        assert r.edge_direction == "oracle_higher"

    def test_moderate_divergence_boost(self):
        r = compare_to_oracle(0.75, 0.55)
        assert r.severity == "moderate"
        assert r.confidence_multiplier == 1.10
        assert r.edge_direction == "oracle_higher"

    def test_extreme_divergence_shrinks(self):
        r = compare_to_oracle(0.90, 0.40)
        assert r.severity == "extreme"
        assert r.confidence_multiplier == 0.85
        assert r.edge_direction == "oracle_higher"

    def test_oracle_lower_direction(self):
        r = compare_to_oracle(0.30, 0.55)
        assert r.edge_direction == "oracle_lower"

    def test_clamping(self):
        r = compare_to_oracle(1.5, -0.5)
        assert r.oracle_prob == 1.0
        assert r.market_prob == 0.0

    def test_to_dict_shape(self):
        d = compare_to_oracle(0.7, 0.5).to_dict()
        for k in ("oracle_prob", "market_prob", "divergence",
                  "edge_direction", "severity", "confidence_multiplier"):
            assert k in d


class TestMarketImpliedProbDataclass:
    def test_to_dict_roundtrip(self):
        m = MarketImpliedProb(
            ticker="AAPL", prob=0.42, source="options_iv",
            target_move_pct=0.05, horizon_days=30, raw_iv=0.30, spot=100.0,
            computed_at=datetime(2026, 4, 13, tzinfo=timezone.utc),
        )
        d = m.to_dict()
        assert d["ticker"] == "AAPL"
        assert d["prob"] == 0.42
        assert d["source"] == "options_iv"
        assert d["raw_iv"] == 0.30
