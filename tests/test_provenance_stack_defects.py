"""Regression tests for verified defects 1-6 in the conviction provenance stack.

Written test-first (failing before source fixes) using small synthetic inputs
and mocks (no database, no network):

  1. Honest coverage: adapters return None on no-data/error; empty/failing DB
     yields 0% layer coverage and demotes HIGH via the 60% coverage gate;
     signal_convergence_scanner's neutral 1.0 is handled on the provenance side;
     fudge_alerts DB failure is unavailable (None), not 0 alerts.
  2. No double counting of disagreement_score and fragility_multiplier in
     provenance (already applied to confidence in EnsemblePredictor.predict).
  3. Shipping fudge alerts gated to SHIPPING_EXPOSED_SECTORS; non-shipping
     tickers treat the layer as unavailable (None) with zero penalty.
  4. LLM red team is advisory only: stays on reports as text/fields with no
     numeric multiplier and no verdict veto (including when LLM is offline).
  5. Scorecard key-space mapping: oracle model_votes map to contributing
     signal names / families (matching bootstrap/Brier keys), not raw model
     names.
  6. Unwired edge_signal layer counts honestly as absent in coverage.
"""
from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from features.per_signal_brier import MIN_CALIBRATED_SAMPLES, SignalScorecard
from intelligence import decision_gateway as dg
from intelligence import llm_red_team
from intelligence import signal_cooccurrence as sc
from intelligence import signal_provenance as sp
from intelligence.confidence_bucket_tracker import (
    _reset_initialized_engines,
    conviction_multiplier_for_bucket,
    record_scored_prediction,
)
from intelligence.signal_convergence_scanner import ConvergenceReport
from tests.test_confidence_bucket_tracker import FakeEngine as BucketFakeEngine
from tests.test_signal_cooccurrence import FakeEngine as CoocFakeEngine


def _card(source: str, weight: float = 1.3, calibrated: bool = True) -> SignalScorecard:
    return SignalScorecard(
        signal_source=source,
        horizon_days=7,
        scored_count=50,
        running_brier=0.08,
        running_ece=0.06,
        hit_rate=0.65,
        last_updated=datetime.now(timezone.utc),
        is_calibrated=calibrated,
        conviction_weight=weight,
    )


def _prediction(
    *,
    ticker: str = "NVDA",
    direction: str = "bullish",
    confidence: float = 0.72,
    fragility_multiplier: float = 1.0,
    disagreement_score: float = 0.0,
    model_votes: list[dict[str, Any]] | None = None,
    signals: Any = None,
    signal_contributions: dict[str, float] | None = None,
) -> SimpleNamespace:
    votes = (
        model_votes
        if model_votes is not None
        else [{"model_name": "flow_momentum", "vote_weight": 1.0}]
    )
    ns = SimpleNamespace(
        ticker=ticker,
        direction=direction,
        score=72,
        confidence=confidence,
        confidence_lower=max(0.0, confidence - 0.1),
        confidence_upper=min(1.0, confidence + 0.1),
        horizon=7,
        regime="EXPANSION",
        fci_regime="EASY",
        model_votes=votes,
        shapley_top_contributor=votes[0]["model_name"] if votes else "",
        shapley_top_share=1.0 if votes else 0.0,
        fragility_multiplier=fragility_multiplier,
        disagreement_score=disagreement_score,
        crowd_aligned=False,
        market_implied_prob=0.65,
        catalyst_type="earnings",
        liquidity_state="EXPANSION",
    )
    if signals is not None:
        ns.signals = signals
    if signal_contributions is not None:
        ns.signal_contributions = signal_contributions
    return ns


def _failing_engine() -> MagicMock:
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("db unavailable")
    engine.begin.side_effect = RuntimeError("db unavailable")
    return engine


def _active_convergence(multiplier: float = 1.0) -> ConvergenceReport:
    return ConvergenceReport(
        ticker="NVDA",
        as_of="2026-11-21",
        window_days=7,
        target_direction="bullish",
        stream_signals=(),
        n_active_streams=2,
        n_aligned=2,
        n_opposed=0,
        convergence_score=0.55,
        conviction_multiplier=multiplier,
        evidence_chain=(),
        advisory="Active convergence",
        missing_stream_count=6,
    )


def _empty_convergence() -> ConvergenceReport:
    return ConvergenceReport(
        ticker="NVDA",
        as_of="2026-11-21",
        window_days=7,
        target_direction="bullish",
        stream_signals=(),
        n_active_streams=0,
        n_aligned=0,
        n_opposed=0,
        convergence_score=0.0,
        conviction_multiplier=1.0,
        evidence_chain=(),
        advisory="No alt-data streams fired",
        missing_stream_count=8,
    )


def _patch_all_non_fudge_adapters(stack: ExitStack, *, value: float | None = 1.0) -> None:
    fn = lambda *_a, **_k: value  # noqa: E731
    for name in (
        "get_lift_multiplier",
        "conviction_multiplier_for_bucket",
        "scenario_conviction_multiplier",
        "null_hypothesis_penalty",
        "get_aggregate_weight_multiplier",
        "contra_conviction_multiplier",
        "squeeze_conviction_multiplier",
        "arbitrage_conviction_multiplier",
        "money_flow_conviction_multiplier",
        "memory_lesson_conviction_multiplier",
    ):
        stack.enter_context(patch.object(sp, name, fn))
    conv_report = _empty_convergence() if value is None else _active_convergence(value)
    stack.enter_context(
        patch(
            "intelligence.signal_convergence_scanner.scan_convergence",
            return_value=conv_report,
        )
    )
    stack.enter_context(patch.object(sp, "build_condition_tuple", lambda **_k: ("t",)))


# ── Defect 1: Honest coverage across adapters and build_provenance_report ───


class TestDefect1HonestCoverage:
    def test_cooccurrence_adapter_returns_none_when_unavailable(self) -> None:
        empty = CoocFakeEngine()
        assert sc.get_lift_multiplier(empty, {"a": 0.5, "b": 0.5}) is None
        assert sc.get_lift_multiplier(empty, {"a": 0.5, "b": 0.05}) is None

        # Uncalibrated pair (< MIN_COOCCURRENCE_SAMPLES) -> unavailable
        sc.record_joint_prediction(
            empty, outcome=1.0, signal_contributions={"a": 0.5, "b": 0.5}
        )
        assert sc.get_lift_multiplier(empty, {"a": 0.5, "b": 0.5}) is None

        # Failing engine -> unavailable
        failing = CoocFakeEngine(should_fail=True)
        assert sc.get_lift_multiplier(failing, {"a": 0.5, "b": 0.5}) is None

        # Stale calibrated pair -> unavailable
        stale_eng = CoocFakeEngine()
        for _ in range(sc.MIN_COOCCURRENCE_SAMPLES):
            sc.record_joint_prediction(
                stale_eng, outcome=1.0, signal_contributions={"a": 0.5, "b": 0.5}
            )
        old = datetime.now(timezone.utc) - timedelta(days=sc.COOCCURRENCE_MAX_STALE_DAYS + 5)
        for row in stale_eng.pairs.values():
            row["last_updated"] = old
        assert sc.get_lift_multiplier(stale_eng, {"a": 0.5, "b": 0.5}) is None

    def test_confidence_bucket_adapter_returns_none_when_unavailable(self) -> None:
        _reset_initialized_engines()
        empty = BucketFakeEngine()
        assert conviction_multiplier_for_bucket(empty, confidence=0.75, horizon_days=7) is None
        assert conviction_multiplier_for_bucket(empty, confidence=0.30, horizon_days=7) is None

        # Cold-start (< MIN_CALIBRATED_SAMPLES) -> unavailable
        for _ in range(3):
            record_scored_prediction(empty, confidence=0.75, outcome=1.0, horizon_days=7)
        assert conviction_multiplier_for_bucket(empty, confidence=0.75, horizon_days=7) is None

        # DB failure -> unavailable
        failing = BucketFakeEngine(raise_on_execute=True)
        assert conviction_multiplier_for_bucket(failing, confidence=0.75, horizon_days=7) is None

        # Calibrated neutral bucket (gap == 0.0) -> genuinely 1.0 (not None)
        calibrated = BucketFakeEngine()
        calibrated.store[(7, 0.75, 0.80)] = {
            "n": MIN_CALIBRATED_SAMPLES * 2,
            "hits": MIN_CALIBRATED_SAMPLES * 2 * 0.75,
            "brier": 0.18,
            "last_updated": datetime.now(timezone.utc),
        }
        assert conviction_multiplier_for_bucket(calibrated, confidence=0.75, horizon_days=7) == 1.0

    def test_no_monkeypatch_identity_dispatch_in_signal_provenance(self) -> None:
        assert not [name for name in dir(sp) if name.startswith("_ORIG_")]
        assert not hasattr(sp, "convergence_conviction_multiplier")

    def test_upstream_adapters_return_none_on_no_data_or_failure(self) -> None:
        from datetime import date

        from intelligence import (
            contra_indicator_ensemble,
            historical_scenario_library,
            meta_learning_matrix,
            money_flow_adapter,
            null_hypothesis_forecaster,
            prediction_market_arbitrage,
            reasoning_bank,
            short_squeeze_composite,
        )

        failing = _failing_engine()
        today = date(2026, 11, 21)
        cond = meta_learning_matrix.build_condition_tuple(
            horizon_days=7,
            liquidity_regime="EXPANSION",
            fci_regime="EASY",
            vix_level=18.0,
        )

        assert (
            historical_scenario_library.scenario_conviction_multiplier(
                failing, as_of=today, horizon_days=7, direction="bullish"
            )
            is None
        )
        assert (
            null_hypothesis_forecaster.null_hypothesis_penalty(
                failing, horizon_days=7
            )
            is None
        )
        assert (
            meta_learning_matrix.get_aggregate_weight_multiplier(
                failing, signal_contributions={"a": 0.5}, condition=cond
            )
            is None
        )
        assert (
            contra_indicator_ensemble.contra_conviction_multiplier(
                failing, as_of=today, trade_direction="bullish"
            )
            is None
        )
        assert (
            short_squeeze_composite.squeeze_conviction_multiplier(
                failing, ticker="NVDA", as_of=today, trade_direction="bullish"
            )
            is None
        )
        assert (
            prediction_market_arbitrage.arbitrage_conviction_multiplier(
                failing,
                ticker="NVDA",
                as_of=today,
                direction="bullish",
                horizon_days=7,
                oracle_confidence=0.72,
            )
            is None
        )
        assert (
            money_flow_adapter.money_flow_conviction_multiplier(
                failing, as_of=today, trade_direction="bullish"
            )
            is None
        )
        assert (
            reasoning_bank.memory_lesson_conviction_multiplier(
                failing, fingerprint={"ticker": "NVDA", "direction": "bullish"}
            )
            is None
        )

    def test_failing_db_produces_zero_layer_coverage_and_demotes_high(self) -> None:
        # No adapter monkeypatching: exercise the real adapter calls against a
        # failing engine. Previously coverage was 15/16 (94%) because every
        # adapter caught its own exception and returned 1.0.
        engine = _failing_engine()
        pred = _prediction(ticker="XOM", confidence=0.72)

        with patch.object(
            sp,
            "get_scorecard_with_regime_fallback",
            lambda _eng, src, _h, _reg=None: _card(src, 1.3),
        ):
            report = sp.build_provenance_report(engine, prediction=pred)

        assert report.layers_present == 0
        # XOM is shipping-exposed (fudge_alerts applicable & failed -> False);
        # only unwired edge_signal is not_applicable and excluded from denominator.
        assert report.layers_total == len(sp.CONVICTION_LAYERS) - 1
        assert report.layer_coverage["edge_signal"] == "not_applicable"
        assert report.layer_coverage["fudge_alerts"] is False
        assert all(
            status is False
            for name, status in report.layer_coverage.items()
            if name != "edge_signal"
        )
        assert report.verdict == "low"
        assert "adjuster layers computed" in report.verdict_reason

    def test_convergence_empty_streams_neutral_handled_on_provenance_side(self) -> None:
        engine = _failing_engine()
        pred = _prediction(ticker="NVDA", confidence=0.72)
        empty_conv = ConvergenceReport(
            ticker="NVDA",
            as_of="2026-11-21",
            window_days=7,
            target_direction="bullish",
            stream_signals=(),
            n_active_streams=0,
            n_aligned=0,
            n_opposed=0,
            convergence_score=0.0,
            conviction_multiplier=1.0,
            evidence_chain=(),
            advisory="No alt-data streams fired",
            missing_stream_count=8,
        )
        active_conv = ConvergenceReport(
            ticker="NVDA",
            as_of="2026-11-21",
            window_days=7,
            target_direction="bullish",
            stream_signals=(),
            n_active_streams=2,
            n_aligned=2,
            n_opposed=0,
            convergence_score=0.55,
            conviction_multiplier=1.05,
            evidence_chain=(),
            advisory="Weak confirmation",
            missing_stream_count=6,
        )

        with patch(
            "intelligence.signal_convergence_scanner.scan_convergence",
            return_value=empty_conv,
        ):
            rep_empty = sp.build_provenance_report(engine, prediction=pred)
        assert rep_empty.layer_coverage["convergence"] is False
        assert rep_empty.convergence_multiplier == 1.0

        with patch(
            "intelligence.signal_convergence_scanner.scan_convergence",
            return_value=active_conv,
        ):
            rep_active = sp.build_provenance_report(engine, prediction=pred)
        assert rep_active.layer_coverage["convergence"] is True
        assert rep_active.convergence_multiplier == pytest.approx(1.05)

    def test_fudge_alerts_db_failure_returns_none_and_not_covered(self) -> None:
        engine = _failing_engine()
        assert sp._recent_fudge_alerts(engine, ticker="XOM") is None


# ── Defect 2: No double counting of disagreement and fragility ──────────────


class TestDefect2NoDoubleCounting:
    def test_disagreement_and_fragility_not_applied_in_aggregate_conviction(self) -> None:
        # Use a signal with no SIGNAL_WEIGHT_OVERRIDES entry (override = 1.0).
        ev = [sp.SignalEvidence("jodi_oil", 1.0, _card("jodi_oil", 1.3), "strong")]

        clean = sp.aggregate_conviction_with_coverage(
            ev, disagreement_score=0.0, fragility_multiplier=1.0
        )
        stressed = sp.aggregate_conviction_with_coverage(
            ev, disagreement_score=0.9, fragility_multiplier=0.5
        )
        assert stressed.value == pytest.approx(clean.value)
        assert stressed.value == pytest.approx(1.3)
        assert "disagreement" not in sp.CONVICTION_LAYERS
        assert "fragility" not in sp.CONVICTION_LAYERS
        assert "disagreement" not in stressed.layer_coverage
        assert "fragility" not in stressed.layer_coverage

    def test_disagreement_and_fragility_still_displayed_on_report(self) -> None:
        pred_clean = _prediction(
            ticker="NVDA",
            confidence=0.70,
            disagreement_score=0.0,
            fragility_multiplier=1.0,
        )
        pred_stressed = _prediction(
            ticker="NVDA",
            confidence=0.70,
            disagreement_score=0.65,
            fragility_multiplier=0.72,
        )
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_fallback",
                    lambda _eng, src, _h, _reg=None: _card(src, 1.25),
                )
            )
            rep_clean = sp.build_provenance_report(MagicMock(), prediction=pred_clean)
            rep_stressed = sp.build_provenance_report(MagicMock(), prediction=pred_stressed)

        assert rep_stressed.disagreement_score == pytest.approx(0.65)
        assert rep_stressed.fragility_multiplier == pytest.approx(0.72)
        # Neither disagreement nor fragility is multiplied into conviction again.
        # flow_momentum expands to equity(1.2x), flows(1.0x), breadth(1.0x), vol(0.3x)
        # -> mean override 0.875 -> 1.25 * 0.875 = 1.09375 for both clean & stressed.
        assert rep_stressed.aggregate_conviction == pytest.approx(rep_clean.aggregate_conviction)
        assert rep_stressed.aggregate_conviction == pytest.approx(1.25 * 0.875)


# ── Defect 3: Shipping fudge alerts restricted to SHIPPING_EXPOSED_SECTORS ──


class TestDefect3ShippingFudgeAlertsSectorScoped:
    def test_shipping_exposed_sectors_constant_defined(self) -> None:
        assert hasattr(sp, "SHIPPING_EXPOSED_SECTORS")
        sectors = {s.lower() for s in sp.SHIPPING_EXPOSED_SECTORS}
        for required in ("industrials", "energy", "materials"):
            assert required in sectors
        assert any("transport" in s for s in sectors)

    def test_non_shipping_ticker_skips_fudge_alerts_and_marks_unavailable(self) -> None:
        fake_alerts = [
            {"name": "port_x", "assessment": "major_divergence", "divergence_zscore": 3.0},
            {"name": "port_y", "assessment": "contradiction", "divergence_zscore": 2.8},
        ]
        # Engine that WOULD return 2 shipping alerts if queried.
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value.all.return_value = fake_alerts

        for non_shipping in ("NVDA", "JPM", "UNKNWN_TICKER"):
            assert sp._recent_fudge_alerts(engine, ticker=non_shipping) is None

        pred = _prediction(
            ticker="NVDA",
            confidence=0.72,
            signal_contributions={"jodi_oil": 1.0},
        )
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_fallback",
                    lambda _eng, src, _h, _reg=None: _card(src, 1.4),
                )
            )
            report = sp.build_provenance_report(engine, prediction=pred)

        assert report.shipping_fudge_alerts == []
        assert report.layer_coverage["fudge_alerts"] == "not_applicable"
        assert report.layer_coverage["edge_signal"] == "not_applicable"
        assert report.layers_total == len(sp.CONVICTION_LAYERS) - 2
        assert report.layers_present == report.layers_total
        assert report.to_dict()["layer_coverage"]["fudge_alerts"] == "not_applicable"
        assert report.aggregate_conviction == pytest.approx(1.4)

    def test_not_applicable_layers_excluded_from_coverage_denominator(self) -> None:
        # NVDA has 11 applicable layers (fudge_alerts & edge_signal are not_applicable).
        # With 7/11 present (63.6% >= 60%), verdict stays HIGH. Counting the two
        # non-applicable layers in the denominator would give 7/13 (53.8% < 60%) -> LOW.
        pred = _prediction(
            ticker="NVDA",
            confidence=0.72,
            signal_contributions={"jodi_oil": 1.0},
        )
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            for absent_adapter in (
                "scenario_conviction_multiplier",
                "null_hypothesis_penalty",
                "arbitrage_conviction_multiplier",
                "memory_lesson_conviction_multiplier",
            ):
                stack.enter_context(patch.object(sp, absent_adapter, lambda *_a, **_k: None))
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_fallback",
                    lambda _eng, src, _h, _reg=None: _card(src, 1.3),
                )
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        assert report.layers_total == 11
        assert report.layers_present == 7
        assert report.layer_coverage["fudge_alerts"] == "not_applicable"
        assert report.layer_coverage["edge_signal"] == "not_applicable"
        assert report.verdict == "high"

    def test_shipping_exposed_ticker_applies_fudge_alert_penalty(self) -> None:
        fake_alerts = [
            {"name": "port_x", "assessment": "major_divergence", "divergence_zscore": 3.0},
            {"name": "port_y", "assessment": "contradiction", "divergence_zscore": 2.8},
        ]
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value.all.return_value = fake_alerts

        for exposed in ("XOM", "UPS", "BHP", "ZIM"):
            res = sp._recent_fudge_alerts(engine, ticker=exposed)
            assert res is not None and len(res) == 2

        pred = _prediction(
            ticker="XOM",
            confidence=0.72,
            signal_contributions={"jodi_oil": 1.0},
        )
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_fallback",
                    lambda _eng, src, _h, _reg=None: _card(src, 1.4),
                )
            )
            report = sp.build_provenance_report(engine, prediction=pred)

        assert len(report.shipping_fudge_alerts) == 2
        assert report.layer_coverage["fudge_alerts"] is True
        # 2 alerts -> 1.0 - 2 * 0.15 = 0.70 -> 1.4 * 0.70 = 0.98
        assert report.aggregate_conviction == pytest.approx(0.98)


# ── Defect 4: LLM red team is advisory only ─────────────────────────────────


class TestDefect4RedTeamAdvisoryOnly:
    def test_red_team_none_preserved_on_report_when_not_run(self) -> None:
        pred = _prediction(
            ticker="NVDA",
            confidence=0.72,
            signal_contributions={"jodi_oil": 1.0},
        )
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_fallback",
                    lambda _eng, src, _h, _reg=None: _card(src, 1.3),
                )
            )
            rep_not_run = sp.build_provenance_report(
                MagicMock(), prediction=pred, red_team_epistemic_risk=None
            )
            rep_zero = sp.build_provenance_report(
                MagicMock(), prediction=pred, red_team_epistemic_risk=0.0
            )

        assert rep_not_run.red_team_epistemic_risk is None
        assert rep_not_run.to_dict()["red_team_epistemic_risk"] is None
        assert rep_zero.red_team_epistemic_risk == 0.0
        assert rep_zero.to_dict()["red_team_epistemic_risk"] == 0.0

    def test_red_team_has_no_numeric_multiplier_in_provenance(self) -> None:
        ev = [sp.SignalEvidence("jodi_oil", 1.0, _card("jodi_oil", 1.3), "strong")]
        low = sp.aggregate_conviction_with_coverage(ev, red_team_epistemic_risk=0.0)
        high = sp.aggregate_conviction_with_coverage(ev, red_team_epistemic_risk=1.0)
        assert high.value == pytest.approx(low.value)
        assert high.value == pytest.approx(1.3)
        assert "red_team" not in sp.CONVICTION_LAYERS
        assert "red_team" not in high.layer_coverage
        assert getattr(llm_red_team, "ADVISORY_ONLY", False) is True

    def test_combine_verdict_does_not_veto_on_high_or_missing_red_team(self) -> None:
        verdict_high_risk, _ = dg.combine_verdict(
            "high", 0.75, "robust", 0.95, red_team_risk=0.95
        )
        assert verdict_high_risk == "high"

        verdict_missing, _ = dg.combine_verdict(
            "high", 0.75, "robust", 0.95, red_team_risk=None
        )
        assert verdict_missing == "high"

    def test_should_i_trade_keeps_red_team_advisory_without_veto(self) -> None:
        engine = MagicMock()
        pred = _prediction(ticker="NVDA", confidence=0.78)
        prov = SimpleNamespace(
            ticker="NVDA",
            verdict="high",
            aggregate_conviction=1.3,
            red_team_epistemic_risk=0.95,
        )
        rt_report = llm_red_team.RedTeamReport(
            ticker="NVDA",
            prediction_score=0.78,
            counters=(
                llm_red_team.CounterArgument("Valuation risk", 0.95, 0.95, 0.95),
            ),
            epistemic_risk_score=0.95,
        )
        pattern = SimpleNamespace(confidence_signal=0.75)
        stress = SimpleNamespace(robustness_label="robust", robustness_score=0.95)
        ticket = SimpleNamespace(ticker="NVDA", verdict="high")

        with patch("oracle.engine.EnsemblePredictor") as MockPred, patch(
            "intelligence.llm_red_team.red_team_prediction",
            return_value=rt_report,
        ), patch(
            "intelligence.signal_provenance.build_provenance_report",
            return_value=prov,
        ), patch(
            "intelligence.pattern_library.build_pattern_match_report",
            return_value=pattern,
        ), patch(
            "intelligence.counterfactual_stress.run_stress_test",
            return_value=stress,
        ), patch(
            "trading.trade_ticket_generator.generate_ticket",
            return_value=ticket,
        ):
            MockPred.return_value.predict.return_value = pred
            resp = dg.should_i_trade(
                engine, "NVDA", account_size_usd=100_000, current_price=120.0
            )

        assert resp.unified_verdict == "high"
        assert resp.trade_ticket is ticket
        assert resp.red_team_report is rt_report

        # LLM offline must NOT block provenance or veto the quantitative verdict.
        with patch("oracle.engine.EnsemblePredictor") as MockPred, patch(
            "intelligence.llm_red_team.red_team_prediction",
            side_effect=RuntimeError("LLM offline"),
        ), patch(
            "intelligence.signal_provenance.build_provenance_report",
            return_value=prov,
        ), patch(
            "intelligence.pattern_library.build_pattern_match_report",
            return_value=pattern,
        ), patch(
            "intelligence.counterfactual_stress.run_stress_test",
            return_value=stress,
        ), patch(
            "trading.trade_ticket_generator.generate_ticket",
            return_value=ticket,
        ):
            MockPred.return_value.predict.return_value = pred
            resp_offline = dg.should_i_trade(
                engine, "NVDA", account_size_usd=100_000, current_price=120.0
            )

        assert resp_offline.unified_verdict == "high"
        assert resp_offline.trade_ticket is ticket
        assert "red_team" in resp_offline.stage_errors


# ── Defect 5: Scorecard key-space mapping ───────────────────────────────────


class TestDefect5ScorecardKeyMapping:
    def test_model_votes_map_to_signal_families_matching_brier_keys(self) -> None:
        # Brier scorecards are keyed by signal family (as written by
        # bootstrap_per_signal_brier.py), NOT by oracle model names.
        family_cards = {
            "equity": _card("equity", 1.3),
            "flows": _card("flows", 1.2),
            "breadth": _card("breadth", 1.1),
            "vol": _card("vol", 1.4),
            "rates": _card("rates", 1.2),
            "credit": _card("credit", 1.1),
            "macro": _card("macro", 1.3),
        }

        class _OracleModelsEngine:
            class _Conn:
                def __enter__(self):
                    return self

                def __exit__(self, *_a):
                    return False

                def execute(self, sql, params=None):
                    s = str(sql)
                    if "FROM oracle_models" in s:
                        rows = [
                            ("flow_momentum", ["equity", "flows", "breadth", "vol"], None),
                            ("regime_contrarian", ["rates", "credit", "vol", "macro"], None),
                        ]
                        res = MagicMock()
                        res.fetchall.return_value = rows
                        return res
                    res = MagicMock()
                    res.fetchall.return_value = []
                    res.mappings.return_value.all.return_value = []
                    return res

            def connect(self):
                return self._Conn()

        pred = _prediction(
            ticker="NVDA",
            confidence=0.72,
            model_votes=[
                {"model_name": "flow_momentum", "vote_weight": 0.4},
                {"model_name": "regime_contrarian", "vote_weight": 0.6},
            ],
        )

        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_fallback",
                    lambda _eng, src, _h, _reg=None: family_cards.get(src),
                )
            )
            report = sp.build_provenance_report(_OracleModelsEngine(), prediction=pred)

        sources = {ev.signal_source: ev for ev in report.signal_evidence}
        assert "flow_momentum" not in sources
        assert "regime_contrarian" not in sources
        assert set(sources.keys()) == set(family_cards.keys())
        # vol receives 0.4/4 + 0.6/4 = 0.25 of normalized weight.
        assert sources["vol"].shapley_weight == pytest.approx(0.25)
        assert report.evidence_coverage == pytest.approx(1.0)
        assert report.verdict == "high"

    def test_explicit_signal_contributions_and_feature_prefix_lookup(self) -> None:
        # When scorecards are stored under bare family names ("vol") while
        # signal_sources carry "feature:vol", lookup still matches.
        cards = {"pcr": _card("pcr", 1.2), "vol": _card("vol", 1.4)}
        pred = _prediction(
            ticker="NVDA",
            confidence=0.72,
            signal_contributions={"pcr": 0.6, "feature:vol": 0.4},
        )
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_fallback",
                    lambda _eng, src, _h, _reg=None: cards.get(src),
                )
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        assert report.evidence_coverage == pytest.approx(1.0)
        # 0.6 * 1.2 + 0.4 * 1.4 = 1.28
        assert report.aggregate_conviction == pytest.approx(1.28)


# ── Defect 6: Unwired edge_signal marked not_applicable & excluded from denominator ─


class TestDefect6EdgeSignalAbsence:
    def test_edge_signal_is_marked_not_applicable_and_excluded_from_denominator(self) -> None:
        pred = _prediction(ticker="XOM", confidence=0.72)
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            stack.enter_context(patch.object(sp, "_recent_fudge_alerts", lambda *_a, **_k: []))
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_fallback",
                    lambda _eng, src, _h, _reg=None: _card(src, 1.3),
                )
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        assert "edge_signal" in sp.CONVICTION_LAYERS
        assert report.layer_coverage["edge_signal"] == "not_applicable"
        assert report.to_dict()["layer_coverage"]["edge_signal"] == "not_applicable"
        assert report.layers_total == len(sp.CONVICTION_LAYERS) - 1
        assert report.layers_present == report.layers_total


# ── Follow-up: Scripts align with production math ───────────────────────────


class TestScriptsMatchProductionMath:
    def test_walk_forward_validate_does_not_apply_removed_penalties_or_fake_red_team(self) -> None:
        from scripts import walk_forward_validate as wfv

        created = datetime(2026, 3, 15, tzinfo=timezone.utc)
        base_row = {
            "id": "p1",
            "ticker": "NVDA",
            "created_at": created,
            "expiry": created + timedelta(days=7),
            "confidence": 0.72,
            "verdict": "hit",
            "model_name": None,
            "signals": {
                "direction": "bullish",
                "regime": "EXPANSION",
                "fci_regime": "EASY",
            },
            "signal_contributions": {"jodi_oil": 1.0},
            "model_weights": None,
        }
        stressed_row = dict(base_row)
        stressed_row["signals"] = {
            **base_row["signals"],
            "fragility_multiplier": 0.5,
            "disagreement_score": 0.9,
            "red_team_epistemic_risk": 0.95,
        }
        cards = {"jodi_oil": _card("jodi_oil", 1.3)}

        rep_base = wfv.build_time_frozen_provenance(base_row, cards)
        rep_stressed = wfv.build_time_frozen_provenance(stressed_row, cards)

        assert rep_base.red_team_epistemic_risk is None
        assert rep_stressed.red_team_epistemic_risk == pytest.approx(0.95)
        assert rep_stressed.aggregate_conviction == pytest.approx(rep_base.aggregate_conviction)
        assert rep_stressed.aggregate_conviction == pytest.approx(1.3)

    def test_call_a_trade_does_not_print_removed_penalties(self, capsys: pytest.CaptureFixture[str]) -> None:
        from scripts import call_a_trade

        rc = call_a_trade.main()
        out = capsys.readouterr().out
        assert rc == 0
        assert "Disagreement penalty:" not in out
        assert "Red-team penalty:" not in out
        assert "Fragility multiplier:  ×" not in out


# ── Codex HOLD 6011905360: regime-calibrated count and the builder's clock ──
#
# 1. The regime lookup falls back to the flat per-signal scorecard when the
#    prediction has no regime, or when the regime row is missing or thin.
#    Only scorecards from the regime-conditional table count toward
#    regime_calibrated_signal_count, and every evidence row reports which
#    table answered ("regime", "flat_fallback", "flat", or None).
# 2. The builder dates its layers from the prediction's own clock
#    (prediction.as_of), not date.today(). The undated layers (scorecards,
#    confidence bucket, null hypothesis, meta-learning, co-occurrence, memory,
#    fudge alerts) read current state, so the builder is live-only: a
#    prediction whose clock is outside the live window is refused before any
#    layer is read.


def _patch_calibration_tables(
    stack: ExitStack,
    *,
    regime_cards: dict[str, SignalScorecard],
    flat_cards: dict[str, SignalScorecard],
) -> dict[str, list[Any]]:
    """Stand in for the two scorecard tables behind the regime fallback.

    ``regime_cards`` holds the regime-conditional rows that are warm enough
    to return (a missing key is a missing or thin regime row); ``flat_cards``
    holds the flat per-signal scorecards.
    """
    calls: dict[str, list[Any]] = {"regime": [], "flat": []}

    def _regime(_eng: Any, src: str, _h: int, regime: str | None) -> SignalScorecard | None:
        calls["regime"].append((src, regime))
        return regime_cards.get(src)

    def _flat(_eng: Any, src: str, _h: int) -> SignalScorecard | None:
        calls["flat"].append(src)
        return flat_cards.get(src)

    stack.enter_context(
        patch("features.regime_conditional_brier.get_regime_conditional_scorecard", _regime)
    )
    stack.enter_context(patch("features.regime_conditional_brier.get_signal_scorecard", _flat))
    return calls


def _evidence_rows(report: Any) -> dict[str, dict[str, Any]]:
    return {row["signal_source"]: row for row in report.to_dict()["signal_evidence"]}


class TestCodexHold6011905360RegimeCalibratedCount:
    def test_missing_regime_reports_flat_source_and_counts_zero(self) -> None:
        pred = _prediction(signal_contributions={"jodi_oil": 0.6, "pcr": 0.4})
        pred.regime = ""
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            calls = _patch_calibration_tables(
                stack,
                regime_cards={"jodi_oil": _card("jodi_oil", 1.3), "pcr": _card("pcr", 1.2)},
                flat_cards={"jodi_oil": _card("jodi_oil", 1.1), "pcr": _card("pcr", 1.0)},
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        assert calls["regime"] == []  # no regime: the regime table is never read
        assert report.regime_calibrated_signal_count == 0
        rows = _evidence_rows(report)
        assert rows["jodi_oil"]["calibration_source"] == "flat"
        assert rows["pcr"]["calibration_source"] == "flat"
        assert all(ev.calibration_source == "flat" for ev in report.signal_evidence)

    def test_flat_fallback_is_reported_and_not_counted_as_regime_calibrated(self) -> None:
        # The prediction carries a regime, but neither signal has a warm
        # regime row, so both scorecards come from the flat fallback.
        pred = _prediction(signal_contributions={"jodi_oil": 0.6, "pcr": 0.4})
        assert pred.regime == "EXPANSION"
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            calls = _patch_calibration_tables(
                stack,
                regime_cards={},
                flat_cards={"jodi_oil": _card("jodi_oil", 1.3), "pcr": _card("pcr", 1.2)},
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        assert ("jodi_oil", "EXPANSION") in calls["regime"]
        assert report.regime_calibrated_signal_count == 0
        rows = _evidence_rows(report)
        assert rows["jodi_oil"]["calibration_source"] == "flat_fallback"
        assert rows["pcr"]["calibration_source"] == "flat_fallback"
        # The flat scorecard is still the evidence used (behaviour unchanged).
        assert rows["jodi_oil"]["scorecard"]["conviction_weight"] == pytest.approx(1.3)
        assert report.aggregate_conviction == pytest.approx(0.6 * 1.3 + 0.4 * 1.2)

    def test_only_regime_specific_hits_are_counted(self) -> None:
        pred = _prediction(
            signal_contributions={"jodi_oil": 0.5, "pcr": 0.3, "ghost_signal": 0.2}
        )
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            _patch_calibration_tables(
                stack,
                regime_cards={"jodi_oil": _card("jodi_oil", 1.3)},
                flat_cards={"jodi_oil": _card("jodi_oil", 0.9), "pcr": _card("pcr", 1.2)},
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        assert report.regime_calibrated_signal_count == 1
        assert report.to_dict()["regime_calibrated_signal_count"] == 1
        rows = _evidence_rows(report)
        assert rows["jodi_oil"]["calibration_source"] == "regime"
        assert rows["jodi_oil"]["scorecard"]["conviction_weight"] == pytest.approx(1.3)
        assert rows["pcr"]["calibration_source"] == "flat_fallback"
        assert rows["ghost_signal"]["calibration_source"] is None
        assert rows["ghost_signal"]["scorecard"] is None


_LIVE_NOW = datetime(2026, 3, 5, 15, 30, tzinfo=timezone.utc)

_DATED_ADAPTERS = (
    "scenario_conviction_multiplier",
    "contra_conviction_multiplier",
    "squeeze_conviction_multiplier",
    "arbitrage_conviction_multiplier",
    "money_flow_conviction_multiplier",
)


def _capture_dated_layers(stack: ExitStack) -> dict[str, Any]:
    """Patch every layer that takes a date and record the date it was given."""
    seen: dict[str, Any] = {}

    def _capture(name: str) -> Any:
        def _fn(*_a: Any, **kw: Any) -> float:
            seen[name] = kw.get("as_of")
            return 1.0

        return _fn

    for name in _DATED_ADAPTERS:
        stack.enter_context(patch.object(sp, name, _capture(name)))
    stack.enter_context(patch.object(sp, "_lookup_convergence", _capture("convergence")))
    return seen


def _spy_every_layer(stack: ExitStack) -> list[str]:
    """Patch every layer and scorecard read so any read is recorded."""
    reads: list[str] = []

    def _spy(name: str) -> Any:
        def _fn(*_a: Any, **_k: Any) -> Any:
            reads.append(name)
            return None

        return _fn

    for name in (
        "get_lift_multiplier",
        "conviction_multiplier_for_bucket",
        "null_hypothesis_penalty",
        "get_aggregate_weight_multiplier",
        "memory_lesson_conviction_multiplier",
        "_recent_fudge_alerts",
        "_lookup_convergence",
        "_resolve_signal_contributions",
        *_DATED_ADAPTERS,
    ):
        stack.enter_context(patch.object(sp, name, _spy(name)))
    stack.enter_context(
        patch("features.regime_conditional_brier.get_regime_conditional_scorecard", _spy("regime_card"))
    )
    stack.enter_context(
        patch("features.regime_conditional_brier.get_signal_scorecard", _spy("flat_card"))
    )
    return reads


class TestCodexHold6011905360PredictionClock:
    def test_dated_layers_use_the_prediction_clock_not_the_wall_clock(self) -> None:
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        pred.as_of = _LIVE_NOW - timedelta(minutes=10)
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            seen = _capture_dated_layers(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            _patch_calibration_tables(
                stack, regime_cards={}, flat_cards={"jodi_oil": _card("jodi_oil", 1.3)}
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        expected = _LIVE_NOW.date()
        assert set(seen) == {*_DATED_ADAPTERS, "convergence"}
        assert all(value == expected for value in seen.values()), seen
        assert report.generated_at == _LIVE_NOW.isoformat()

    def test_missing_clock_uses_the_builder_utc_date(self) -> None:
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        assert getattr(pred, "as_of", None) is None
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            seen = _capture_dated_layers(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            _patch_calibration_tables(
                stack, regime_cards={}, flat_cards={"jodi_oil": _card("jodi_oil", 1.3)}
            )
            sp.build_provenance_report(MagicMock(), prediction=pred)

        assert all(value == _LIVE_NOW.date() for value in seen.values()), seen

    def test_naive_clock_is_read_as_utc(self) -> None:
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        # 00:10 UTC on 2026-03-06, written without tzinfo.
        late_now = datetime(2026, 3, 6, 0, 20, tzinfo=timezone.utc)
        pred.as_of = datetime(2026, 3, 6, 0, 10)
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            seen = _capture_dated_layers(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: late_now, create=True))
            _patch_calibration_tables(
                stack, regime_cards={}, flat_cards={"jodi_oil": _card("jodi_oil", 1.3)}
            )
            sp.build_provenance_report(MagicMock(), prediction=pred)

        assert all(value == late_now.date() for value in seen.values()), seen

    @pytest.mark.parametrize(
        "clock",
        [
            pytest.param(_LIVE_NOW - timedelta(days=30), id="historical-replay"),
            pytest.param(_LIVE_NOW - timedelta(hours=2), id="stale-beyond-tolerance"),
            pytest.param(_LIVE_NOW + timedelta(days=2), id="future"),
            pytest.param(_LIVE_NOW.date() - timedelta(days=1), id="date-only-yesterday"),
            pytest.param("2026-03-05T15:30:00+00:00", id="string-clock"),
        ],
    )
    def test_clock_outside_live_window_is_refused_before_any_read(self, clock: Any) -> None:
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        pred.as_of = clock
        with ExitStack() as stack:
            reads = _spy_every_layer(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            with pytest.raises(ValueError, match="live-only"):
                sp.build_provenance_report(MagicMock(), prediction=pred)

        assert reads == []

    def test_historical_clock_fails_closed_through_the_decision_gateway(self) -> None:
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        pred.as_of = _LIVE_NOW - timedelta(days=30)
        with ExitStack() as stack:
            reads = _spy_every_layer(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            report, err = dg._run_provenance(MagicMock(), pred, None)

        assert report is None
        assert err is not None and "live-only" in err
        assert reads == []
