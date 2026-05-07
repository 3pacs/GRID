"""
Tests for the valuation module.

Tests intrinsic value calculations, milestone tracking, derivatives
support scoring, and composite model output.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from valuation.intrinsic import (
    FinancialInputs,
    IntrinsicValueEngine,
)
from valuation.milestones import Milestone
from valuation.derivatives_support import (
    DerivativesSupportResult,
    DealerPositioning,
    OptionsSentiment,
    ShortPositioning,
)


# ── FinancialInputs ─────────────────────────────────────────────────


class TestFinancialInputs:
    def test_create_minimal(self):
        fi = FinancialInputs(ticker="AAPL", filing_date=date(2026, 3, 31), period="Q1")
        assert fi.ticker == "AAPL"
        assert fi.total_assets is None

    def test_create_full(self):
        fi = FinancialInputs(
            ticker="MSFT",
            filing_date=date(2026, 3, 31),
            period="Q1",
            total_assets=400_000,
            total_equity=200_000,
            cash=50_000,
            total_debt=80_000,
            net_income=25_000,
            revenue=60_000,
            shares_outstanding=7_500,
            market_price=410.0,
            market_cap=3_075_000,
        )
        assert fi.total_equity == 200_000
        assert fi.shares_outstanding == 7_500


# ── IntrinsicValueEngine calculations ────────────────────────────────


class TestIntrinsicValueComputation:
    """Test the pure computation path (no DB)."""

    def _make_engine(self):
        return IntrinsicValueEngine(db_engine=MagicMock())

    def _sample_inputs(self) -> FinancialInputs:
        return FinancialInputs(
            ticker="TEST",
            filing_date=date(2026, 4, 1),
            period="TTM",
            total_assets=1_000,
            total_current_assets=400,
            total_current_liabilities=200,
            total_debt=300,
            cash=100,
            total_equity=500,
            intangible_assets=50,
            goodwill=30,
            inventory=60,
            receivables=80,
            revenue=800,
            net_income=120,
            ebitda=180,
            depreciation=40,
            operating_income=160,
            operating_cf=150,
            capex=-60,
            free_cf=90,
            market_price=50.0,
            shares_outstanding=20,
            market_cap=1_000,
            enterprise_value=1_200,
        )

    def test_book_value_ps(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        # equity / shares = 500 / 20 = 25.0
        assert result.book_value_ps == pytest.approx(25.0)

    def test_tangible_book_ps(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        # (500 - 50 - 30) / 20 = 420 / 20 = 21.0
        assert result.tangible_book_ps == pytest.approx(21.0)

    def test_ncav_ps(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        # total_liabilities = total_assets - total_equity = 1000 - 500 = 500
        # NCAV = current_assets - total_liabilities = 400 - 500 = -100
        # NCAV/share = -100 / 20 = -5.0
        assert result.ncav_ps == pytest.approx(-5.0)

    def test_net_cash_ps(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        # (100 - 300) / 20 = -200 / 20 = -10.0
        assert result.net_cash_ps == pytest.approx(-10.0)

    def test_liquidation_ps(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        # receivables*0.8 + inventory*0.5 + cash + other*0.2 - total_liab
        # other_current = 400 - 80 - 60 - 100 = 160
        # liq = 64 + 30 + 100 + 32 - 500 = -274
        # per share = -274 / 20 = -13.7
        assert result.liquidation_ps == pytest.approx(-13.7)

    def test_epv_ps(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        # normalized = operating_income * (1 - 0.21) = 160 * 0.79 = 126.4
        # EPV = 126.4 / 0.10 = 1264
        # + cash - debt = 1264 + 100 - 300 = 1064
        # per share = 1064 / 20 = 53.2
        assert result.epv_ps == pytest.approx(53.2)

    def test_owner_earnings_ps(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        # owner_earnings = net_income + depreciation - maintenance_capex
        # maintenance = |capex| * 0.70 = 60 * 0.70 = 42
        # OE = 120 + 40 - 42 = 118
        # per share = 118 / 20 = 5.9
        assert result.owner_earnings_ps == pytest.approx(5.9)

    def test_dcf_ps(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        # DCF uses free_cf=90 with 3% growth, 10% discount, 10 years
        # + net cash (100 - 300 = -200)
        assert result.dcf_ps is not None
        # Rough check: should be positive and reasonable
        assert result.dcf_ps > 0

    def test_ev_ebitda(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        # 1200 / 180 = 6.67
        assert result.ev_ebitda == pytest.approx(6.667, abs=0.01)

    def test_composite_range(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        # Should have low/mid/high from positive methods
        assert result.intrinsic_low is not None
        assert result.intrinsic_mid is not None
        assert result.intrinsic_high is not None
        assert result.intrinsic_low <= result.intrinsic_mid <= result.intrinsic_high

    def test_margin_of_safety(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        if result.intrinsic_mid and result.intrinsic_mid > 0:
            expected = (result.intrinsic_mid - 50.0) / result.intrinsic_mid
            assert result.margin_of_safety == pytest.approx(expected)

    def test_no_shares_returns_empty(self):
        engine = self._make_engine()
        inputs = FinancialInputs(
            ticker="EMPTY", filing_date=date(2026, 4, 1), period="TTM",
            total_equity=500, shares_outstanding=0,
        )
        result = engine.compute(inputs)
        assert result.book_value_ps is None

    def test_simple_dcf_static(self):
        # Test the static DCF method directly
        val = IntrinsicValueEngine._simple_dcf(100, growth_rate=0.03, discount_rate=0.10, years=10)
        assert val > 0
        # Should be roughly 14x-15x FCF for these assumptions
        assert 1300 < val < 1600

    def test_to_dict(self):
        engine = self._make_engine()
        inputs = self._sample_inputs()
        result = engine.compute(inputs)
        d = result.to_dict()
        assert d["ticker"] == "TEST"
        assert "book_value_ps" in d


# ── Milestone validation ─────────────────────────────────────────────


class TestMilestone:
    def test_valid_milestone(self):
        m = Milestone(
            ticker="AAPL",
            milestone_type="EARNINGS_GUIDANCE",
            announced_date=date(2026, 1, 15),
            description="Q1 2026 EPS guidance of $2.50",
            target_value=2.50,
            target_unit="EPS",
            probability=0.8,
        )
        assert m.status == "PENDING"
        assert m.probability == 0.8

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Invalid milestone_type"):
            Milestone(
                ticker="AAPL",
                milestone_type="INVALID",
                announced_date=date(2026, 1, 1),
                description="Test",
            )

    def test_invalid_status_raises(self):
        with pytest.raises(ValueError, match="Invalid status"):
            Milestone(
                ticker="AAPL",
                milestone_type="EARNINGS_GUIDANCE",
                announced_date=date(2026, 1, 1),
                description="Test",
                status="BAD_STATUS",
            )

    def test_invalid_probability_raises(self):
        with pytest.raises(ValueError, match="Probability must be 0-1"):
            Milestone(
                ticker="AAPL",
                milestone_type="RUMOR",
                announced_date=date(2026, 1, 1),
                description="Test",
                probability=1.5,
            )

    def test_all_valid_types(self):
        for mtype in [
            "EARNINGS_GUIDANCE", "REVENUE_GUIDANCE", "PRODUCT_LAUNCH",
            "EXPANSION", "M_AND_A", "REGULATORY", "COST_TARGET",
            "BUYBACK", "DIVIDEND", "DEBT_TARGET", "STRATEGIC", "RUMOR",
        ]:
            m = Milestone(
                ticker="X", milestone_type=mtype,
                announced_date=date(2026, 1, 1), description="Test",
            )
            assert m.milestone_type == mtype


# ── Short Positioning Scoring ────────────────────────────────────────


class TestShortPositioningScore:
    def test_low_short_interest_supportive(self):
        s = ShortPositioning(short_float_pct=1.5)
        assert s.score() >= 70

    def test_moderate_short_interest_pressure(self):
        s = ShortPositioning(short_float_pct=8.0)
        assert 30 <= s.score() <= 60

    def test_high_short_interest_pressure(self):
        s = ShortPositioning(short_float_pct=15.0)
        assert s.score() <= 50

    def test_extreme_short_squeeze_potential(self):
        s = ShortPositioning(short_float_pct=35.0, days_to_cover=8.0)
        # Extreme + high DTC should show squeeze potential
        assert s.score() >= 40

    def test_no_data_neutral(self):
        s = ShortPositioning()
        assert s.score() == 50.0

    def test_shorts_covering_supportive(self):
        s = ShortPositioning(short_float_pct=5.0, short_change_pct=-15.0)
        base = ShortPositioning(short_float_pct=5.0)
        assert s.score() > base.score()

    def test_shorts_increasing_pressure(self):
        s = ShortPositioning(short_float_pct=5.0, short_change_pct=15.0)
        base = ShortPositioning(short_float_pct=5.0)
        assert s.score() < base.score()


# ── Dealer Positioning Scoring ───────────────────────────────────────


class TestDealerPositioningScore:
    def test_long_gamma_supportive(self):
        d = DealerPositioning(gex_regime="LONG_GAMMA")
        assert d.score() >= 70

    def test_short_gamma_destabilizing(self):
        d = DealerPositioning(gex_regime="SHORT_GAMMA")
        assert d.score() <= 35

    def test_neutral_middle(self):
        d = DealerPositioning(gex_regime="NEUTRAL")
        assert 40 <= d.score() <= 60

    def test_no_data_neutral(self):
        d = DealerPositioning()
        assert d.score() == 50.0

    def test_near_put_wall_support(self):
        d = DealerPositioning(
            gex_regime="LONG_GAMMA",
            spot_price=100.0,
            put_wall=99.0,  # 1% below spot
        )
        assert d.score() > DealerPositioning(gex_regime="LONG_GAMMA").score()


# ── Options Sentiment Scoring ────────────────────────────────────────


class TestOptionsSentimentScore:
    def test_low_pcr_mildly_bullish(self):
        s = OptionsSentiment(put_call_ratio=0.6)
        assert s.score() > 50

    def test_high_pcr_bearish(self):
        s = OptionsSentiment(put_call_ratio=1.2)
        assert s.score() < 50

    def test_extreme_pcr_contrarian_bullish(self):
        s = OptionsSentiment(put_call_ratio=1.6)
        # Extreme fear should be contrarian bullish
        assert s.score() >= 50

    def test_high_iv_skew_bearish(self):
        s = OptionsSentiment(iv_skew=0.15)
        assert s.score() < 50

    def test_no_data_neutral(self):
        s = OptionsSentiment()
        assert s.score() == 50.0

    def test_below_max_pain_bullish(self):
        s = OptionsSentiment(max_pain_dist_pct=-6.0)
        assert s.score() > 55


# ── Composite Derivatives Support ────────────────────────────────────


class TestDerivativesSupportResult:
    def test_compute_composite_strong_support(self):
        result = DerivativesSupportResult(
            ticker="TEST", snap_date=date(2026, 4, 1),
            spot_price=100.0, intrinsic_mid=90.0,
            premium_to_intrinsic=0.11,
            short=ShortPositioning(short_float_pct=1.0),
            dealer=DealerPositioning(gex_regime="LONG_GAMMA"),
            options=OptionsSentiment(put_call_ratio=0.6),
        )
        result.compute_composite()
        assert result.derivatives_support_score > 60
        assert result.support_regime in ("STRONG_SUPPORT", "MILD_SUPPORT")

    def test_compute_composite_strong_pressure(self):
        result = DerivativesSupportResult(
            ticker="TEST", snap_date=date(2026, 4, 1),
            spot_price=100.0, intrinsic_mid=120.0,
            premium_to_intrinsic=-0.17,
            short=ShortPositioning(short_float_pct=18.0),
            dealer=DealerPositioning(gex_regime="SHORT_GAMMA"),
            options=OptionsSentiment(put_call_ratio=1.2, iv_skew=0.12),
        )
        result.compute_composite()
        assert result.derivatives_support_score < 40
        assert result.support_regime in ("STRONG_PRESSURE", "MILD_PRESSURE")

    def test_narrative_generated(self):
        result = DerivativesSupportResult(
            ticker="TEST", snap_date=date(2026, 4, 1),
            spot_price=100.0, intrinsic_mid=80.0,
            premium_to_intrinsic=0.25,
            short=ShortPositioning(short_float_pct=25.0, days_to_cover=5.0),
            dealer=DealerPositioning(gex_regime="SHORT_GAMMA"),
            options=OptionsSentiment(put_call_ratio=1.4),
        )
        result.compute_composite()
        assert len(result.narrative) > 0
        assert "short" in result.narrative.lower() or "gamma" in result.narrative.lower()

    def test_to_dict(self):
        result = DerivativesSupportResult(
            ticker="TEST", snap_date=date(2026, 4, 1),
            spot_price=100.0, intrinsic_mid=100.0,
            premium_to_intrinsic=0.0,
        )
        result.compute_composite()
        d = result.to_dict()
        assert d["ticker"] == "TEST"
        assert "derivatives_support_score" in d
        assert "support_regime" in d
