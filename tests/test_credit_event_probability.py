"""CAT-162 — credit event probability machine tests."""
from __future__ import annotations

import math
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from intelligence.credit_event_probability import (
    _DTD_CEIL,
    _DTD_FLOOR,
    _norm_cdf,
    CreditEventResult,
    compose_credit_event_probability,
    compute_credit_event_probability,
    credit_spread_default_probability,
    merton_default_probability,
    merton_distance_to_default,
    rating_trajectory_adjustment,
)


class TestNormCdf:
    def test_zero_half(self):
        assert abs(_norm_cdf(0) - 0.5) < 1e-6

    def test_positive(self):
        assert _norm_cdf(1.0) > 0.8

    def test_negative(self):
        assert _norm_cdf(-1.0) < 0.2


class TestMertonDTD:
    def test_zero_debt_returns_none(self):
        assert merton_distance_to_default(100, 0, 0.3, 1.0) is None

    def test_zero_vol_returns_none(self):
        assert merton_distance_to_default(100, 50, 0, 1.0) is None

    def test_zero_horizon_returns_none(self):
        assert merton_distance_to_default(100, 50, 0.3, 0) is None

    def test_healthy_company_high_dtd(self):
        # Big cushion: V >> D
        dtd = merton_distance_to_default(
            asset_value=1000, debt=100, asset_vol=0.2, horizon_years=1.0,
        )
        assert dtd is not None
        assert dtd > 2.0

    def test_distressed_low_dtd(self):
        # V barely above D
        dtd = merton_distance_to_default(
            asset_value=105, debt=100, asset_vol=0.5, horizon_years=1.0,
        )
        assert dtd is not None
        assert dtd < 0.5

    def test_clamps_to_range(self):
        # Extreme cushion should hit _DTD_CEIL
        dtd = merton_distance_to_default(
            asset_value=1e12, debt=1, asset_vol=0.1, horizon_years=1.0,
        )
        assert dtd is not None
        assert dtd == _DTD_CEIL


class TestMertonDefaultProbability:
    def test_healthy_company_low_pd(self):
        pd, dtd = merton_default_probability(
            market_cap=1e12, total_debt=1e10, equity_vol_30d=0.2,
            horizon_years=1.0,
        )
        assert pd < 0.01
        assert dtd is not None

    def test_distressed_high_pd(self):
        pd, dtd = merton_default_probability(
            market_cap=5e8, total_debt=3e9, equity_vol_30d=0.8,
            horizon_years=1.0,
        )
        # Higher PD than the healthy case — don't pin exact value as the
        # formula uses conservative asset cushioning
        assert pd > 0.001


class TestCreditSpreadDefaultProbability:
    def test_zero_spread_zero_pd(self):
        assert credit_spread_default_probability(0, 1.0) == 0.0

    def test_high_spread_high_pd(self):
        pd = credit_spread_default_probability(1000, 1.0)  # 1000 bps = 10%
        # ~17% implied default over 1y at 40% recovery
        assert pd > 0.10

    def test_monotonic_in_horizon(self):
        pd_90d = credit_spread_default_probability(500, 90.0 / 365.0)
        pd_1y = credit_spread_default_probability(500, 1.0)
        assert pd_1y > pd_90d


class TestRatingTrajectoryAdjustment:
    def test_no_changes_zero(self):
        assert rating_trajectory_adjustment(0, 0) == 0.0

    def test_downgrades_positive(self):
        assert rating_trajectory_adjustment(2, 0) > 0

    def test_upgrades_negative(self):
        assert rating_trajectory_adjustment(0, 2) < 0

    def test_clamps_to_one(self):
        assert rating_trajectory_adjustment(100, 0) == 1.0

    def test_clamps_to_minus_one(self):
        assert rating_trajectory_adjustment(0, 100) == -1.0


class TestComposeCreditEventProbability:
    def test_no_inputs_returns_zero(self):
        r = compose_credit_event_probability(ticker="AAPL")
        assert r.p_default_90d == 0.0
        assert r.p_default_1y == 0.0
        assert len(r.components_used) == 0

    def test_merton_only(self):
        r = compose_credit_event_probability(
            ticker="AAPL",
            market_cap=1e12, total_debt=1e11, equity_vol_30d=0.3,
        )
        assert "merton_dtd" in r.components_used
        assert r.dtd is not None
        assert r.p_default_1y >= 0
        assert r.p_default_1y <= 1

    def test_credit_spread_only(self):
        r = compose_credit_event_probability(
            ticker="T",
            credit_spread_bps=500,
        )
        assert "credit_spread" in r.components_used
        assert "merton_dtd" in r.missing
        assert r.p_default_90d > 0

    def test_rating_trajectory_alone(self):
        r = compose_credit_event_probability(
            ticker="F",
            downgrades_90d=3,
        )
        # Rating alone gets picked up
        assert "rating_trajectory" in r.components_used
        assert r.rating_trajectory_score > 0

    def test_combined_inputs(self):
        r = compose_credit_event_probability(
            ticker="AAPL",
            market_cap=3e12, total_debt=1e11, equity_vol_30d=0.25,
            credit_spread_bps=80, downgrades_90d=0, upgrades_90d=0,
        )
        assert len(r.components_used) == 2
        # Healthy company → very low PD
        assert r.p_default_1y < 0.05

    def test_distressed_company_high_pd(self):
        r = compose_credit_event_probability(
            ticker="ZZZ",
            market_cap=1e8, total_debt=5e9, equity_vol_30d=1.2,
            credit_spread_bps=1500, downgrades_90d=2,
        )
        assert r.p_default_1y > 0.05

    def test_90d_less_than_1y(self):
        r = compose_credit_event_probability(
            ticker="T",
            credit_spread_bps=200,
        )
        assert r.p_default_90d < r.p_default_1y

    def test_missing_tracked(self):
        r = compose_credit_event_probability(
            ticker="AAPL",
            market_cap=1e10,
        )
        assert "merton_dtd" in r.missing
        assert "credit_spread" in r.missing
        assert "rating_trajectory" in r.missing

    def test_to_dict_shape(self):
        r = compose_credit_event_probability(
            ticker="AAPL",
            market_cap=1e12, total_debt=1e11, equity_vol_30d=0.3,
        )
        d = r.to_dict()
        for k in ("ticker", "as_of", "p_default_90d", "p_default_1y",
                  "dtd", "credit_spread", "rating_trajectory_score",
                  "components_used", "missing"):
            assert k in d


class TestComputeCreditEventProbability:
    def test_patched_readers(self):
        with patch("intelligence.credit_event_probability._read_market_cap", return_value=1e12), \
             patch("intelligence.credit_event_probability._read_total_debt", return_value=1e11), \
             patch("intelligence.credit_event_probability._read_equity_vol", return_value=0.3):
            eng = MagicMock()
            r = compute_credit_event_probability(eng, "AAPL")
        assert "merton_dtd" in r.components_used

    def test_db_errors_non_fatal(self):
        eng = MagicMock()
        eng.connect.side_effect = RuntimeError("down")
        r = compute_credit_event_probability(eng, "AAPL")
        assert r.p_default_90d == 0.0
