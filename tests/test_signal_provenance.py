"""Tests for intelligence/signal_provenance.py."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


from features.per_signal_brier import (
    SignalScorecard,
)
from intelligence.signal_provenance import (
    CausationChain,
    SignalEvidence,
    TradeProvenanceReport,
    _classify_evidence,
    _extract_causation,
    _extract_signal_contributions,
    _verdict_from_aggregate,
    build_provenance_report,
    compute_aggregate_conviction,
)


def _card(
    source: str,
    horizon: int,
    count: int,
    brier: float,
    weight: float,
    calibrated: bool = True,
) -> SignalScorecard:
    return SignalScorecard(
        signal_source=source,
        horizon_days=horizon,
        scored_count=count,
        running_brier=brier,
        running_ece=brier,
        hit_rate=0.6,
        last_updated=datetime.now(timezone.utc),
        is_calibrated=calibrated,
        conviction_weight=weight,
    )


# ── _classify_evidence ───────────────────────────────────────────────────


class TestClassifyEvidence:
    def test_none_is_no_history(self):
        assert _classify_evidence(None) == "no_history"

    def test_uncalibrated_is_cold_start(self):
        card = _card("x", 7, 5, 0.1, 1.0, calibrated=False)
        assert _classify_evidence(card) == "cold_start"

    def test_strong(self):
        card = _card("x", 7, 50, 0.06, 1.35)
        assert _classify_evidence(card) == "strong"

    def test_neutral(self):
        card = _card("x", 7, 50, 0.13, 0.9)
        assert _classify_evidence(card) == "neutral"

    def test_weak(self):
        card = _card("x", 7, 50, 0.21, 0.3)
        assert _classify_evidence(card) == "weak"

    def test_anti_predictive(self):
        card = _card("x", 7, 50, 0.26, 0.0)
        assert _classify_evidence(card) == "anti_predictive"


# ── compute_aggregate_conviction ─────────────────────────────────────────


class TestAggregateConviction:
    def test_empty_evidence_returns_zero(self):
        assert compute_aggregate_conviction([]) == 0.0

    def test_no_history_defaults_to_neutral_weight(self):
        ev = [SignalEvidence("x", 1.0, None, "no_history")]
        # 1.0 weight * 1.0 neutral = 1.0
        assert compute_aggregate_conviction(ev) == 1.0

    def test_single_strong_signal(self):
        card = _card("jodi_oil", 7, 50, 0.06, 1.5)
        ev = [SignalEvidence("jodi_oil", 1.0, card, "strong")]
        # 1.0 * 1.5 = 1.5, no penalties → 1.5 (clamp)
        assert compute_aggregate_conviction(ev) == 1.5

    def test_two_opposing_signals_average(self):
        strong = _card("jodi_oil", 7, 50, 0.06, 1.5)
        weak = _card("reddit_options_pulse", 7, 50, 0.22, 0.3)
        ev = [
            SignalEvidence("jodi_oil", 0.5, strong, "strong"),
            SignalEvidence("reddit_options_pulse", 0.5, weak, "weak"),
        ]
        # 0.5*1.5 + 0.5*0.3 = 0.9
        base = compute_aggregate_conviction(ev)
        assert abs(base - 0.9) < 0.001

    def test_disagreement_penalty(self):
        card = _card("jodi_oil", 7, 50, 0.06, 1.5)
        ev = [SignalEvidence("jodi_oil", 1.0, card, "strong")]
        clean = compute_aggregate_conviction(ev, disagreement_score=0.0)
        dampened = compute_aggregate_conviction(ev, disagreement_score=1.0)
        assert dampened < clean
        # Max 40% dampening at disagreement=1.0 → 1.5 * 0.6 = 0.9
        assert abs(dampened - 0.9) < 0.01

    def test_fragility_penalty(self):
        card = _card("jodi_oil", 7, 50, 0.06, 1.5)
        ev = [SignalEvidence("jodi_oil", 1.0, card, "strong")]
        clean = compute_aggregate_conviction(ev, fragility_multiplier=1.0)
        fragile = compute_aggregate_conviction(ev, fragility_multiplier=0.5)
        assert fragile < clean
        assert abs(fragile - 0.75) < 0.01  # 1.5 * 0.5 = 0.75

    def test_red_team_penalty(self):
        card = _card("jodi_oil", 7, 50, 0.06, 1.5)
        ev = [SignalEvidence("jodi_oil", 1.0, card, "strong")]
        clean = compute_aggregate_conviction(ev, red_team_epistemic_risk=0.0)
        risky = compute_aggregate_conviction(ev, red_team_epistemic_risk=1.0)
        assert risky < clean
        assert abs(risky - 0.75) < 0.01  # 1.5 * (1 - 0.5*1.0) = 0.75

    def test_fudge_alert_penalty(self):
        card = _card("jodi_oil", 7, 50, 0.06, 1.5)
        ev = [SignalEvidence("jodi_oil", 1.0, card, "strong")]
        clean = compute_aggregate_conviction(ev, fudge_alert_count=0)
        flagged = compute_aggregate_conviction(ev, fudge_alert_count=2)
        assert flagged < clean
        # 2 alerts → 1.0 - 2*0.15 = 0.7 → 1.5 * 0.7 = 1.05
        assert abs(flagged - 1.05) < 0.01

    def test_clamped_to_valid_range(self):
        card = _card("x", 7, 50, 0.06, 1.5)
        ev = [SignalEvidence("x", 2.0, card, "strong")]  # over-weighted
        result = compute_aggregate_conviction(ev)
        assert 0.0 <= result <= 1.5


# ── _verdict_from_aggregate ──────────────────────────────────────────────


class TestVerdict:
    def test_no_trade_at_low_conviction(self):
        assert _verdict_from_aggregate(0.1, 0.9) == "no_trade"
        assert _verdict_from_aggregate(0.29, 0.9) == "no_trade"

    def test_low_verdict(self):
        assert _verdict_from_aggregate(0.5, 0.8) == "low"
        assert _verdict_from_aggregate(0.9, 0.5) == "low"  # low confidence

    def test_high_verdict_calibrated_zone(self):
        # Post-2026-05-17b: HIGH gate is confidence in [0.55, 0.85] —
        # the calibrated zone. Excludes the saturated 0.9+ cluster
        # (612 of 677 trades at exactly 0.950, hit=16.8%, mean_pnl=-1.69%).
        assert _verdict_from_aggregate(1.2, 0.8) == "high"
        assert _verdict_from_aggregate(1.4, 0.75) == "high"
        assert _verdict_from_aggregate(1.0, 0.7) == "high"
        assert _verdict_from_aggregate(1.0, 0.55) == "high"
        assert _verdict_from_aggregate(1.0, 0.85) == "high"

    def test_saturated_confidence_falls_to_medium(self):
        # Confidence > 0.85 (the saturated 0.95-cap cluster) currently
        # lands MEDIUM — keeps HIGH bucket measurement honest until the
        # upstream confidence caps get replaced with per-model reliability.
        assert _verdict_from_aggregate(1.0, 0.90) == "medium"
        assert _verdict_from_aggregate(1.0, 0.95) == "medium"
        assert _verdict_from_aggregate(1.4, 0.99) == "medium"

    def test_medium_default(self):
        # Confidence just above the calibrated ceiling lands MEDIUM.
        assert _verdict_from_aggregate(0.8, 0.86) == "medium"
        # Conf in [0.55, 0.85] now lands HIGH; conf<0.55 still LOW.
        assert _verdict_from_aggregate(1.0, 0.55) == "high"
        assert _verdict_from_aggregate(1.0, 0.54) == "low"


# ── _extract_signal_contributions ────────────────────────────────────────


class TestExtractContributions:
    def test_empty_prediction_returns_empty(self):
        pred = SimpleNamespace(model_votes=[], shapley_top_contributor="")
        assert _extract_signal_contributions(pred) == {}

    def test_model_votes_sum_to_one(self):
        pred = SimpleNamespace(
            model_votes=[
                {"model_name": "flow_momentum", "weight": 2.0},
                {"model_name": "regime_contrarian", "weight": 3.0},
            ],
            shapley_top_contributor="",
            shapley_top_share=0.0,
        )
        result = _extract_signal_contributions(pred)
        assert abs(sum(result.values()) - 1.0) < 1e-9
        assert abs(result["flow_momentum"] - 0.4) < 1e-9
        assert abs(result["regime_contrarian"] - 0.6) < 1e-9

    def test_fallback_to_top_shapley_plus_other(self):
        pred = SimpleNamespace(
            model_votes=[],
            shapley_top_contributor="jodi_oil",
            shapley_top_share=0.6,
        )
        result = _extract_signal_contributions(pred)
        assert abs(result["jodi_oil"] - 0.6) < 1e-9
        assert abs(result["other"] - 0.4) < 1e-9

    def test_ignores_zero_or_negative_weights(self):
        pred = SimpleNamespace(
            model_votes=[
                {"model_name": "good", "weight": 1.0},
                {"model_name": "dead", "weight": 0.0},
                {"model_name": "broken", "weight": -0.5},
            ],
            shapley_top_contributor="",
            shapley_top_share=0.0,
        )
        result = _extract_signal_contributions(pred)
        assert "dead" not in result
        assert "broken" not in result
        assert result["good"] == 1.0


# ── _extract_causation ───────────────────────────────────────────────────


class TestExtractCausation:
    def test_bullish_prediction_open_flow(self):
        pred = SimpleNamespace(
            direction="bullish",
            catalyst_type="earnings",
            shapley_top_contributor="flow_momentum",
            liquidity_state="",
            fci_regime="",
        )
        causation = _extract_causation(pred)
        assert causation.flow_direction == "open"
        assert causation.lever == "earnings"
        assert causation.actor == "flow_momentum"
        assert causation.complete is True

    def test_bearish_prediction_close_flow(self):
        pred = SimpleNamespace(
            direction="bearish",
            catalyst_type="",
            liquidity_state="CRISIS",
            shapley_top_contributor="regime_contrarian",
            fci_regime="",
        )
        causation = _extract_causation(pred)
        assert causation.flow_direction == "close"
        assert causation.lever == "CRISIS"
        assert causation.complete is True

    def test_neutral_prediction_incomplete_chain(self):
        pred = SimpleNamespace(
            direction="neutral",
            catalyst_type="earnings",
            liquidity_state="",
            shapley_top_contributor="x",
            fci_regime="",
        )
        causation = _extract_causation(pred)
        assert causation.flow_direction == "neutral"
        assert causation.complete is False

    def test_missing_lever_incomplete(self):
        pred = SimpleNamespace(
            direction="bullish",
            catalyst_type="",
            liquidity_state="",
            shapley_top_contributor="flow_momentum",
            fci_regime="",
        )
        causation = _extract_causation(pred)
        assert causation.complete is False


# ── build_provenance_report integration ──────────────────────────────────


class TestBuildReport:
    def test_high_verdict_end_to_end(self):
        engine = MagicMock()
        # Mock scorecard returns (strong conviction for both signals)
        strong_card = _card("flow_momentum", 7, 50, 0.06, 1.5)
        other_card = _card("regime_contrarian", 7, 50, 0.07, 1.42)

        def fake_get(eng, source, h, regime=None):
            if source == "flow_momentum":
                return strong_card
            if source == "regime_contrarian":
                return other_card
            return None

        with patch(
            "intelligence.signal_provenance.get_scorecard_with_regime_fallback",
            side_effect=fake_get,
        ), patch(
            "intelligence.signal_provenance.get_lift_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.conviction_multiplier_for_bucket",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.scenario_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.null_hypothesis_penalty",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.get_aggregate_weight_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.contra_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.squeeze_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.arbitrage_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.convergence_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.money_flow_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance._recent_fudge_alerts",
            return_value=[],
        ):
            pred = SimpleNamespace(
                ticker="NVDA",
                direction="bullish",
                score=75,
                confidence=0.82,
                confidence_lower=0.72,
                confidence_upper=0.92,
                horizon=7,
                regime="EXPANSION",
                fci_regime="EASY",
                model_votes=[
                    {"model_name": "flow_momentum", "weight": 2.0},
                    {"model_name": "regime_contrarian", "weight": 3.0},
                ],
                shapley_top_contributor="regime_contrarian",
                shapley_top_share=0.6,
                fragility_multiplier=1.0,
                disagreement_score=0.0,
                crowd_aligned=False,
                market_implied_prob=0.78,
                catalyst_type="earnings",
                liquidity_state="EXPANSION",
            )
            report = build_provenance_report(
                engine,
                prediction=pred,
                red_team_epistemic_risk=0.0,
            )
        assert report.ticker == "NVDA"
        assert report.verdict == "high"
        assert len(report.signal_evidence) == 2
        assert report.aggregate_conviction >= 1.2

    def test_no_trade_when_red_team_high(self):
        engine = MagicMock()
        strong_card = _card("flow_momentum", 7, 50, 0.06, 1.5)

        with patch(
            "intelligence.signal_provenance.get_scorecard_with_regime_fallback",
            return_value=strong_card,
        ), patch(
            "intelligence.signal_provenance.get_lift_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.conviction_multiplier_for_bucket",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.scenario_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.null_hypothesis_penalty",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.get_aggregate_weight_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.contra_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.squeeze_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.arbitrage_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.convergence_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.money_flow_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance._recent_fudge_alerts",
            return_value=[],
        ):
            pred = SimpleNamespace(
                ticker="AAPL",
                direction="bullish",
                score=70,
                confidence=0.6,
                confidence_lower=0.5,
                confidence_upper=0.7,
                horizon=7,
                regime="NEUTRAL",
                fci_regime="",
                model_votes=[{"model_name": "flow_momentum", "weight": 1.0}],
                shapley_top_contributor="flow_momentum",
                shapley_top_share=0.9,
                fragility_multiplier=0.5,  # fragile
                disagreement_score=0.8,    # disagreeing models
                crowd_aligned=True,
                market_implied_prob=0.4,
                catalyst_type="",
                liquidity_state="",
            )
            report = build_provenance_report(
                engine,
                prediction=pred,
                red_team_epistemic_risk=0.9,  # red team screamed
            )
        # Heavy penalties should cascade to low verdict
        assert report.verdict in ("low", "no_trade")

    def test_fudge_alerts_penalize_conviction(self):
        engine = MagicMock()
        strong_card = _card("flow_momentum", 7, 50, 0.06, 1.5)

        fake_alerts = [
            {
                "name": "shipping_qingdao_x",
                "assessment": "major_divergence",
                "implication": "reported higher than observed",
                "divergence_zscore": 2.5,
                "confidence": 0.8,
                "checked_at": datetime.now(timezone.utc),
            }
        ]

        with patch(
            "intelligence.signal_provenance.get_scorecard_with_regime_fallback",
            return_value=strong_card,
        ), patch(
            "intelligence.signal_provenance.get_lift_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.conviction_multiplier_for_bucket",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.scenario_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.null_hypothesis_penalty",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.get_aggregate_weight_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.contra_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.squeeze_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.arbitrage_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.convergence_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance.money_flow_conviction_multiplier",
            return_value=1.0,
        ), patch(
            "intelligence.signal_provenance._recent_fudge_alerts",
            return_value=fake_alerts,
        ):
            pred = SimpleNamespace(
                ticker="BHP",
                direction="bullish",
                score=65,
                confidence=0.7,
                confidence_lower=0.6,
                confidence_upper=0.8,
                horizon=7,
                regime="NEUTRAL",
                fci_regime="",
                model_votes=[{"model_name": "flow_momentum", "weight": 1.0}],
                shapley_top_contributor="flow_momentum",
                shapley_top_share=0.9,
                fragility_multiplier=1.0,
                disagreement_score=0.0,
                crowd_aligned=False,
                market_implied_prob=0.6,
                catalyst_type="commodity_cycle",
                liquidity_state="",
            )
            report = build_provenance_report(
                engine,
                prediction=pred,
                red_team_epistemic_risk=0.0,
            )
        assert len(report.shipping_fudge_alerts) == 1
        # The fudge alert penalty should show in aggregate conviction
        assert report.aggregate_conviction < 1.5

    def test_serialization_roundtrip(self):
        card = _card("flow_momentum", 7, 50, 0.06, 1.5)
        ev = SignalEvidence("flow_momentum", 1.0, card, "strong")
        causation = CausationChain(
            lever="earnings", flow_direction="open",
            actor="flow_momentum", complete=True,
        )
        report = TradeProvenanceReport(
            ticker="NVDA",
            generated_at=datetime.now(timezone.utc).isoformat(),
            direction="bullish",
            score=75,
            confidence=0.82,
            confidence_lower=0.72,
            confidence_upper=0.92,
            horizon_days=7,
            regime="EXPANSION",
            fci_regime="EASY",
            signal_evidence=[ev],
            top_shapley_contributor="flow_momentum",
            top_shapley_share=0.9,
            fragility_multiplier=1.0,
            disagreement_score=0.0,
            crowd_aligned=False,
            market_implied_prob=0.78,
            red_team_epistemic_risk=0.0,
            shipping_fudge_alerts=[],
            causation=causation,
            cooccurrence_lift=1.0,
            regime_calibrated_signal_count=0,
            confidence_bucket_multiplier=1.0,
            scenario_multiplier=1.0,
            null_hypothesis_penalty=1.0,
            meta_learning_multiplier=1.0,
            contra_indicator_multiplier=1.0,
            squeeze_multiplier=1.0,
            arbitrage_multiplier=1.0,
            convergence_multiplier=1.0,
            money_flow_multiplier=1.0,
            memory_lesson_multiplier=1.0,
            aggregate_conviction=1.3,
            verdict="high",
        )
        d = report.to_dict()
        for k in (
            "ticker", "direction", "verdict", "aggregate_conviction",
            "signal_evidence", "causation", "shipping_fudge_alerts",
        ):
            assert k in d
        assert d["verdict"] == "high"
        assert d["causation"]["complete"] is True
        assert len(d["signal_evidence"]) == 1
