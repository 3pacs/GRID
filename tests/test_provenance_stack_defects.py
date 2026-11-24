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
  7. Codex HOLD 6011905360: only regime-table scorecards count as
     regime-calibrated (each row reports its calibration source), and the
     builder dates its layers from the prediction's clock and refuses a
     clock outside the live window.
  8. Codex review 6031562705: a zero-evidence fixture through the real
     decision gateway pins the outcome in each confidence band
     (``TestZeroEvidenceGatewayPin``).
  9. Codex review 6032024752, adopted by Anik on 2026-11-24: no ticket
     without eligible directional evidence, one verdict policy shared by
     provenance and the counterfactual stress test, and positive-control and
     exact-threshold tests (``TestEvidenceFloor``).
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from features.per_signal_brier import MIN_CALIBRATED_SAMPLES, SignalScorecard
from intelligence import counterfactual_stress as cs
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
from oracle.risk import CircuitBreaker
from tests.test_confidence_bucket_tracker import FakeEngine as BucketFakeEngine
from tests.test_signal_cooccurrence import FakeEngine as CoocFakeEngine
from trading.trade_ticket_generator import generate_ticket


def _card(source: str, weight: float = 1.3, calibrated: bool = True) -> SignalScorecard:
    return SignalScorecard(
        signal_source=source,
        horizon_days=7,
        scored_count=50,
        running_brier=0.08,
        running_ece=0.06,
        hit_rate=0.65,
        # Scored a day before any test's decision clock, so it is fenced.
        last_updated=datetime.now(timezone.utc) - timedelta(days=1),
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
        # The decision clock predict() stamps; builders refuse a missing one.
        as_of=datetime.now(timezone.utc),
    )
    if signals is not None:
        ns.signals = signals
    if signal_contributions is not None:
        ns.signal_contributions = signal_contributions
    return ns


def _fence_every_layer(stack: ExitStack) -> None:
    """Treat every adjuster layer as fenced to the decision clock.

    For tests of the coverage and multiplier mechanics, which predate the
    clock fence (Codex HOLD 6030789643) and exercise layers that are
    advisory in production. The real fence set is pinned by
    ``TestCodexHold6030789643EvidenceFence``.
    """
    stack.enter_context(
        patch.object(sp, "CLOCK_FENCED_LAYERS", frozenset(sp.CONVICTION_LAYERS))
    )


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
            "get_scorecard_with_regime_source",
            lambda _eng, src, _h, _reg=None, **_k: (_card(src, 1.3), "regime"),
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

        with ExitStack() as stack:
            _fence_every_layer(stack)
            stack.enter_context(patch(
                "intelligence.signal_convergence_scanner.scan_convergence",
                return_value=empty_conv,
            ))
            rep_empty = sp.build_provenance_report(engine, prediction=pred)
        assert rep_empty.layer_coverage["convergence"] is False
        assert rep_empty.convergence_multiplier == 1.0

        with ExitStack() as stack:
            _fence_every_layer(stack)
            stack.enter_context(patch(
                "intelligence.signal_convergence_scanner.scan_convergence",
                return_value=active_conv,
            ))
            rep_active = sp.build_provenance_report(engine, prediction=pred)
        assert rep_active.layer_coverage["convergence"] is True
        assert rep_active.convergence_multiplier == pytest.approx(1.05)

    def test_fudge_alerts_db_failure_returns_none_and_not_covered(self) -> None:
        engine = _failing_engine()
        clock = datetime.now(timezone.utc)
        assert sp._recent_fudge_alerts(engine, ticker="XOM", clock=clock) is None


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
                    "get_scorecard_with_regime_source",
                    lambda _eng, src, _h, _reg=None, **_k: (_card(src, 1.25), "regime"),
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

        clock = datetime.now(timezone.utc)
        for non_shipping in ("NVDA", "JPM", "UNKNWN_TICKER"):
            assert sp._recent_fudge_alerts(engine, ticker=non_shipping, clock=clock) is None

        pred = _prediction(
            ticker="NVDA",
            confidence=0.72,
            signal_contributions={"jodi_oil": 1.0},
        )
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            _fence_every_layer(stack)
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_source",
                    lambda _eng, src, _h, _reg=None, **_k: (_card(src, 1.4), "regime"),
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
            _fence_every_layer(stack)
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
                    "get_scorecard_with_regime_source",
                    lambda _eng, src, _h, _reg=None, **_k: (_card(src, 1.3), "regime"),
                )
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        assert report.layers_total == 11
        assert report.layers_present == 7
        assert report.layer_coverage["fudge_alerts"] == "not_applicable"
        assert report.layer_coverage["edge_signal"] == "not_applicable"
        assert report.verdict == "high"

    def test_shipping_exposed_ticker_applies_fudge_alert_penalty(self) -> None:
        checked = datetime.now(timezone.utc) - timedelta(hours=2)
        fake_alerts = [
            {"name": "port_x", "assessment": "major_divergence", "divergence_zscore": 3.0,
             "checked_at": checked},
            {"name": "port_y", "assessment": "contradiction", "divergence_zscore": 2.8,
             "checked_at": checked},
        ]
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value.all.return_value = fake_alerts

        clock = datetime.now(timezone.utc)
        for exposed in ("XOM", "UPS", "BHP", "ZIM"):
            res = sp._recent_fudge_alerts(engine, ticker=exposed, clock=clock)
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
                    "get_scorecard_with_regime_source",
                    lambda _eng, src, _h, _reg=None, **_k: (_card(src, 1.4), "regime"),
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
                    "get_scorecard_with_regime_source",
                    lambda _eng, src, _h, _reg=None, **_k: (_card(src, 1.3), "regime"),
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
            # One fenced in-direction card, so the evidence floor passes.
            direction="bullish",
            decision_clock="",
            signal_evidence=[
                sp.SignalEvidence("flow_momentum", 1.0, _card("flow_momentum"), "strong")
            ],
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
            _fence_every_layer(stack)
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_source",
                    lambda _eng, src, _h, _reg=None, **_k: (family_cards.get(src), "regime"),
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
                    "get_scorecard_with_regime_source",
                    lambda _eng, src, _h, _reg=None, *, aliases=(): (
                        next((cards[k] for k in (src, *aliases) if k in cards), None),
                        "regime",
                    ),
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
            _fence_every_layer(stack)
            stack.enter_context(patch.object(sp, "_recent_fudge_alerts", lambda *_a, **_k: []))
            stack.enter_context(
                patch.object(
                    sp,
                    "get_scorecard_with_regime_source",
                    lambda _eng, src, _h, _reg=None, **_k: (_card(src, 1.3), "regime"),
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
#    (prediction.as_of), not date.today(), and is live-only: a prediction
#    whose clock is outside the live window is refused before any layer is
#    read. (Round 2, below: a missing clock is refused too, and the layers
#    that read current state are advisory.)


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

    def test_naive_clock_is_read_as_utc(self) -> None:
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        # 00:10 UTC on 2026-03-06, written without tzinfo.
        late_now = datetime(2026, 3, 6, 0, 20, tzinfo=timezone.utc)
        pred.as_of = datetime(2026, 3, 6, 0, 10)  # noqa: DTZ001 - naive on purpose
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


# ── Codex HOLD 6030789643: alias priority and one validated decision clock ──
#
# 1. A signal's scorecard is looked up under every spelling of its key
#    (``jodi_oil`` / ``feature:jodi_oil``). A regime card on any alias wins
#    over a flat card on an earlier one.
# 2. The prediction's clock is validated asymmetrically: a future clock past
#    a 60 s skew, a clock older than the live window, a date-only clock and a
#    missing clock are all refused before any layer is read.
# 3. Only evidence bound by a timestamp at or before that clock moves a
#    number directional use reads. Everything else is computed and reported
#    as advisory, and the decision-bearing numbers take the neutral value.

_ADVISORY_LAYERS_NVDA = (
    "cooccurrence",
    "confidence_bucket",
    "scenario",
    "null_hypothesis",
    "meta_learning",
    "contra_indicator",
    "short_squeeze",
    "prediction_market_arb",
    "convergence",
    "money_flow",
    "memory_lesson",
)


def _card_at(source: str, weight: float, last_updated: Any) -> SignalScorecard:
    return SignalScorecard(
        signal_source=source,
        horizon_days=7,
        scored_count=50,
        running_brier=0.08,
        running_ece=0.06,
        hit_rate=0.65,
        last_updated=last_updated,
        is_calibrated=True,
        conviction_weight=weight,
    )


def _fudge_engine(rows: list[dict[str, Any]]) -> MagicMock:
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.mappings.return_value.all.return_value = rows
    return engine


class TestCodexHold6030789643AliasPriority:
    def test_regime_card_on_later_alias_beats_flat_card_on_earlier_alias(self) -> None:
        pred = _prediction(signal_contributions={"jodi_oil": 1.0})
        pred.as_of = _LIVE_NOW - timedelta(minutes=2)
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            calls = _patch_calibration_tables(
                stack,
                regime_cards={
                    "feature:jodi_oil": _card_at("feature:jodi_oil", 1.4, _LIVE_NOW - timedelta(days=1))
                },
                flat_cards={"jodi_oil": _card_at("jodi_oil", 0.9, _LIVE_NOW - timedelta(days=1))},
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        row = _evidence_rows(report)["jodi_oil"]
        assert row["calibration_source"] == "regime"
        assert row["scorecard"]["conviction_weight"] == pytest.approx(1.4)
        assert report.regime_calibrated_signal_count == 1
        assert calls["regime"] == [
            ("jodi_oil", "EXPANSION"),
            ("feature:jodi_oil", "EXPANSION"),
        ]
        assert calls["flat"] == []

    def test_prefixed_source_finds_regime_card_on_bare_alias(self) -> None:
        pred = _prediction(signal_contributions={"feature:vol": 1.0})
        pred.as_of = _LIVE_NOW - timedelta(minutes=2)
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            _patch_calibration_tables(
                stack,
                regime_cards={"vol": _card_at("vol", 1.4, _LIVE_NOW - timedelta(days=1))},
                flat_cards={"feature:vol": _card_at("feature:vol", 0.8, _LIVE_NOW - timedelta(days=1))},
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        row = _evidence_rows(report)["feature:vol"]
        assert row["calibration_source"] == "regime"
        assert row["scorecard"]["conviction_weight"] == pytest.approx(1.4)


class TestCodexHold6030789643DecisionClock:
    @pytest.mark.parametrize(
        "clock",
        [
            pytest.param(_LIVE_NOW + timedelta(seconds=61), id="future-just-past-skew"),
            pytest.param(_LIVE_NOW + timedelta(minutes=10), id="future-same-hour"),
            pytest.param(_LIVE_NOW + timedelta(hours=3), id="future-same-day"),
        ],
    )
    def test_future_stamp_is_refused_before_any_read(self, clock: datetime) -> None:
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        pred.as_of = clock
        with ExitStack() as stack:
            reads = _spy_every_layer(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            with pytest.raises(ValueError, match="future"):
                sp.build_provenance_report(MagicMock(), prediction=pred)

        assert reads == []

    def test_clock_within_the_future_skew_is_the_decision_clock(self) -> None:
        clock = _LIVE_NOW + timedelta(seconds=30)
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        pred.as_of = clock
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            _patch_calibration_tables(
                stack,
                regime_cards={},
                flat_cards={"jodi_oil": _card_at("jodi_oil", 1.3, _LIVE_NOW - timedelta(days=1))},
            )
            report = sp.build_provenance_report(MagicMock(), prediction=pred)

        assert sp.LIVE_CLOCK_MAX_FUTURE_SKEW <= timedelta(seconds=60)
        assert report.decision_clock == clock.isoformat()
        assert report.to_dict()["decision_clock"] == clock.isoformat()

    @pytest.mark.parametrize(
        "clock",
        [
            pytest.param(None, id="missing"),
            pytest.param(_LIVE_NOW.date(), id="date-only-today"),
        ],
    )
    def test_missing_or_date_only_clock_is_refused_for_directional_use(self, clock: Any) -> None:
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        pred.as_of = clock
        with ExitStack() as stack:
            reads = _spy_every_layer(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            with pytest.raises(ValueError, match="decision clock"):
                sp.build_provenance_report(MagicMock(), prediction=pred)

        assert reads == []

    def test_missing_clock_fails_closed_through_the_decision_gateway(self) -> None:
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        pred.as_of = None
        with ExitStack() as stack:
            reads = _spy_every_layer(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            report, err = dg._run_provenance(MagicMock(), pred, None)

        assert report is None
        assert err is not None and "decision clock" in err
        assert reads == []

    def test_clock_older_than_the_live_window_is_refused(self) -> None:
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        pred.as_of = _LIVE_NOW - sp.LIVE_CLOCK_MAX_AGE - timedelta(seconds=1)
        with ExitStack() as stack:
            reads = _spy_every_layer(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            with pytest.raises(ValueError, match="older than"):
                sp.build_provenance_report(MagicMock(), prediction=pred)

        assert reads == []


class TestCodexHold6030789643EvidenceFence:
    def _build(
        self,
        *,
        layer_value: float,
        cards: dict[str, SignalScorecard],
        contributions: dict[str, float],
        clock: datetime,
        ticker: str = "NVDA",
        engine: Any = None,
    ) -> Any:
        pred = _prediction(ticker=ticker, confidence=0.72, signal_contributions=contributions)
        pred.as_of = clock
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=layer_value)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW, create=True))
            _patch_calibration_tables(stack, regime_cards=cards, flat_cards={})
            return sp.build_provenance_report(engine or MagicMock(), prediction=pred)

    def test_only_the_fudge_alert_layer_is_fenced_to_the_clock(self) -> None:
        assert sp.CLOCK_FENCED_LAYERS == frozenset({"fudge_alerts"})
        assert set(_ADVISORY_LAYERS_NVDA) == (
            set(sp.CONVICTION_LAYERS) - sp.CLOCK_FENCED_LAYERS - {"edge_signal"}
        )

    def test_unfenced_layers_are_advisory_and_do_not_move_numbers(self) -> None:
        clock = _LIVE_NOW - timedelta(minutes=2)
        cards = {"jodi_oil": _card_at("jodi_oil", 1.3, _LIVE_NOW - timedelta(days=1))}
        quiet = self._build(layer_value=1.0, cards=cards, contributions={"jodi_oil": 1.0}, clock=clock)
        loud = self._build(layer_value=0.9, cards=cards, contributions={"jodi_oil": 1.0}, clock=clock)

        # Every number directional use reads is the same whether the
        # unfenced layers say 1.0 or 0.9.
        for field_name in (
            "aggregate_conviction",
            "verdict",
            "confidence",
            "score",
            "cooccurrence_lift",
            "confidence_bucket_multiplier",
            "scenario_multiplier",
            "null_hypothesis_penalty",
            "meta_learning_multiplier",
            "contra_indicator_multiplier",
            "squeeze_multiplier",
            "arbitrage_multiplier",
            "convergence_multiplier",
            "money_flow_multiplier",
            "memory_lesson_multiplier",
            "layers_present",
            "evidence_coverage",
        ):
            assert getattr(loud, field_name) == getattr(quiet, field_name), field_name
        assert loud.aggregate_conviction == pytest.approx(1.3)
        assert loud.scenario_multiplier == 1.0

        # ...and the unfenced values are still reported, marked advisory.
        assert loud.advisory_layers == {name: pytest.approx(0.9) for name in _ADVISORY_LAYERS_NVDA}
        for name in _ADVISORY_LAYERS_NVDA:
            assert loud.layer_coverage[name] == sp.LAYER_ADVISORY
        assert loud.layers_advisory == len(_ADVISORY_LAYERS_NVDA)
        assert loud.layers_present == 0
        assert loud.to_dict()["advisory_layers"]["scenario"] == pytest.approx(0.9)
        assert loud.to_dict()["layer_coverage"]["scenario"] == "advisory"

        # Directional use fails closed: HIGH needs layer coverage that only
        # unfenced evidence could supply, so the verdict is LOW and says why.
        assert loud.verdict == "low"
        assert "advisory" in loud.verdict_reason

    def test_same_hour_late_scorecard_is_advisory_not_decision_bearing(self) -> None:
        clock = _LIVE_NOW - timedelta(minutes=2)
        cards = {
            # Rewritten five minutes after the decision, same hour, same day.
            "jodi_oil": _card_at("jodi_oil", 1.4, clock + timedelta(minutes=5)),
            "pcr": _card_at("pcr", 1.2, clock - timedelta(minutes=5)),
        }
        report = self._build(
            layer_value=1.0,
            cards=cards,
            contributions={"jodi_oil": 0.5, "pcr": 0.5},
            clock=clock,
        )

        rows = _evidence_rows(report)
        assert rows["jodi_oil"]["classification"] == "advisory"
        assert rows["jodi_oil"]["scorecard"] is None
        assert rows["jodi_oil"]["advisory_scorecard"]["conviction_weight"] == pytest.approx(1.4)
        assert rows["pcr"]["classification"] == "strong"
        assert rows["pcr"]["scorecard"]["conviction_weight"] == pytest.approx(1.2)
        # The late card counts as no history: neutral weight, no coverage.
        assert report.aggregate_conviction == pytest.approx(0.5 * 1.0 + 0.5 * 1.2)
        assert report.evidence_coverage == pytest.approx(0.5)

    @pytest.mark.parametrize(
        "stamp",
        [
            pytest.param(None, id="no-stamp"),
            pytest.param(_LIVE_NOW.date(), id="date-only-stamp"),
        ],
    )
    def test_scorecard_without_a_timestamp_is_advisory(self, stamp: Any) -> None:
        report = self._build(
            layer_value=1.0,
            cards={"jodi_oil": _card_at("jodi_oil", 1.4, stamp)},
            contributions={"jodi_oil": 1.0},
            clock=_LIVE_NOW - timedelta(minutes=2),
        )
        assert _evidence_rows(report)["jodi_oil"]["classification"] == "advisory"
        assert report.aggregate_conviction == pytest.approx(1.0)
        assert report.evidence_coverage == 0.0

    def test_same_hour_late_fudge_alert_is_excluded(self) -> None:
        clock = _LIVE_NOW - timedelta(minutes=2)
        rows = [
            {"name": "before", "assessment": "major_divergence", "checked_at": clock - timedelta(minutes=5)},
            {"name": "late", "assessment": "contradiction", "checked_at": clock + timedelta(minutes=5)},
        ]
        engine = _fudge_engine(rows)
        alerts = sp._recent_fudge_alerts(engine, ticker="XOM", clock=clock)

        assert [a["name"] for a in alerts] == ["before"]
        sql, params = engine.connect.return_value.__enter__.return_value.execute.call_args[0]
        assert "checked_at <= :clock" in str(sql)
        assert params["clock"] == clock

    def test_fenced_fudge_alerts_still_move_conviction(self) -> None:
        clock = _LIVE_NOW - timedelta(minutes=2)
        engine = _fudge_engine(
            [
                {"name": "before", "assessment": "major_divergence", "checked_at": clock - timedelta(hours=3)},
                {"name": "late", "assessment": "contradiction", "checked_at": clock + timedelta(minutes=1)},
            ]
        )
        report = self._build(
            layer_value=1.0,
            cards={"jodi_oil": _card_at("jodi_oil", 1.4, _LIVE_NOW - timedelta(days=1))},
            contributions={"jodi_oil": 1.0},
            clock=clock,
            ticker="XOM",
            engine=engine,
        )
        assert [a["name"] for a in report.shipping_fudge_alerts] == ["before"]
        assert report.layer_coverage["fudge_alerts"] is True
        # One fenced alert -> 1.0 - 0.15 = 0.85.
        assert report.aggregate_conviction == pytest.approx(1.4 * 0.85)


# ── Codex reviews 6031562705 / 6032024752: the evidence floor ──────────────
#
# Codex scoped-ACCEPTed the clock fence and alias priority at 6296e9cb and
# held the live verdict rollout as a separate decision (6031562705): under
# the verdict policy then, HIGH was unreachable, and confidence above 0.85
# still landed at MEDIUM with no eligible provenance. The pin added at
# 8e33da83 drove the real ``should_i_trade`` path with a live prediction
# that has no eligible evidence and recorded each band's outcome. Codex's
# review of it (6032024752) recommended no ticket without eligible
# directional evidence, one policy shared by provenance and stress, and
# positive-control and exact-threshold tests. Anik adopted that evidence
# floor on 2026-11-24. ``TestZeroEvidenceGatewayPin`` now pins the outcomes
# under the floor, and ``TestEvidenceFloor`` holds the controls.

_ZERO_EVIDENCE_CLOCK = _LIVE_NOW - timedelta(minutes=2)
_ZERO_EVIDENCE_CONTRIBUTIONS: dict[str, float] = {
    "jodi_oil": 0.5,
    "pcr": 0.3,
    "ghost_signal": 0.2,
}
# What every one of the 11 unfenced layers answers: a non-neutral value, so
# a pinned neutral number shows the fence held, not that the layer was quiet.
_UNFENCED_LAYER_VALUE = 0.9


def _zero_evidence_cards() -> dict[str, SignalScorecard]:
    """Scorecards none of which is eligible at the decision clock.

    ``jodi_oil`` was rewritten five minutes after the clock, ``pcr`` has no
    stamp, and ``ghost_signal`` has no scorecard at all.
    """
    return {
        "jodi_oil": _card_at("jodi_oil", 1.4, _ZERO_EVIDENCE_CLOCK + timedelta(minutes=5)),
        "pcr": _card_at("pcr", 1.3, None),
    }


def _late_or_unstamped_fudge_engine() -> MagicMock:
    """An engine whose shipping fudge alerts are all ineligible.

    The statement binds ``checked_at <= :clock``, so a real database would
    not return the late row; the stub returns it to exercise the row fence
    behind the statement as well.
    """
    return _fudge_engine(
        [
            {
                "name": "late",
                "assessment": "contradiction",
                "checked_at": _ZERO_EVIDENCE_CLOCK + timedelta(minutes=1),
            },
            {"name": "unstamped", "assessment": "major_divergence", "checked_at": None},
        ]
    )


def _gateway_decision(
    *,
    confidence: float,
    ticker: str = "NVDA",
    cards: dict[str, SignalScorecard] | None = None,
    contributions: dict[str, float] | None = None,
) -> dg.DecisionResponse:
    """Run ``should_i_trade`` as production calls it, stubbing only the reads.

    Stubbed: the oracle (it returns the live prediction), the red-team LLM (a
    completed, advisory report), the pattern library (the degraded no-analog
    report, ``confidence_signal=0.0``), every adjuster read behind the 11
    unfenced layers, the two scorecard tables and, for ``XOM``, the fudge
    alert rows. Real: the gateway, ``build_provenance_report`` with its clock
    and fence, the counterfactual stress test and the ticket generator, given
    a fresh circuit breaker so no other test's breaker state can reach it.
    """
    pred = _prediction(
        ticker=ticker,
        confidence=confidence,
        signal_contributions=dict(contributions or _ZERO_EVIDENCE_CONTRIBUTIONS),
    )
    pred.as_of = _ZERO_EVIDENCE_CLOCK
    red_team = llm_red_team.RedTeamReport(
        ticker=ticker,
        prediction_score=confidence,
        counters=(llm_red_team.CounterArgument("Valuation risk", 0.2, 0.2, 0.2),),
        epistemic_risk_score=0.2,
    )
    engine = _late_or_unstamped_fudge_engine() if ticker == "XOM" else MagicMock()
    with ExitStack() as stack:
        _patch_all_non_fudge_adapters(stack, value=_UNFENCED_LAYER_VALUE)
        stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW))
        _patch_calibration_tables(
            stack,
            regime_cards=_zero_evidence_cards() if cards is None else cards,
            flat_cards={},
        )
        predictor = stack.enter_context(patch("oracle.engine.EnsemblePredictor"))
        predictor.return_value.predict.return_value = pred
        stack.enter_context(
            patch("intelligence.llm_red_team.red_team_prediction", return_value=red_team)
        )
        stack.enter_context(
            patch(
                "intelligence.pattern_library.build_pattern_match_report",
                return_value=SimpleNamespace(confidence_signal=0.0),
            )
        )
        stack.enter_context(
            patch("oracle.risk.get_global_circuit_breaker", lambda: CircuitBreaker())
        )
        return dg.should_i_trade(
            engine,
            ticker,
            account_size_usd=100_000,
            current_price=120.0,
            vol_30d=0.35,
        )




# The zero-evidence outcome in every confidence band before the floor (the
# pin at 8e33da83), kept as history: (provenance verdict, stress label and
# score, unified verdict, ticket).
_PRE_FLOOR_BAND_OUTCOMES: dict[str, tuple[str, str, float, str, bool]] = {
    "below-0.55": ("low", "robust", 1.0, "low", False),
    "0.55-0.85": ("low", "fragile", 0.0, "no_trade", False),
    "above-0.85": ("medium", "robust", 1.0, "medium", True),
}


def _pinned_band_outcome(confidence: float) -> tuple[str, str, bool]:
    """(provenance verdict, unified verdict, ticket) with zero evidence.

    Under the evidence floor every band is LOW with no ticket; before it the
    0.55-0.85 band was NO_TRADE and confidence above 0.85 was MEDIUM with a
    ticket (``_PRE_FLOOR_BAND_OUTCOMES``).
    """
    del confidence
    return "low", "low", False


def _eligible_count(report: Any) -> int:
    return sp.count_eligible_directional_evidence(
        report.signal_evidence,
        direction=report.direction,
        decision_clock=report.decision_clock,
    )


# (ticker, layers_present, layers_total, layer_coverage["fudge_alerts"])
_FUDGE_VARIANTS = [
    pytest.param("NVDA", 0, 11, sp.LAYER_NOT_APPLICABLE, id="fudge-not-applicable"),
    # A shipping ticker whose every alert is after the clock or unstamped:
    # the fenced layer is checked and finds nothing, so it counts as present.
    pytest.param("XOM", 1, 12, True, id="fudge-alerts-all-late-or-unstamped"),
]


class TestZeroEvidenceGatewayPin:
    """The zero-evidence gateway pin, under the adopted evidence floor.

    Codex review 6031562705 (at 6296e9cb) held the live verdict rollout:
    "Under current verdict policy HIGH is unreachable and confidence >0.85
    can still land at MEDIUM with no eligible provenance. Pin a
    zero-evidence gateway fixture and agree the required evidence floor
    before relying on this live."

    The fixture is a live prediction (a valid decision clock) with zero
    eligible evidence: every scorecard is absent, stamped after the clock or
    unstamped; no fudge alert is fenced to the clock; and all 11 unfenced
    layers are advisory. The pin added at 8e33da83 recorded the outcome in
    each band without endorsing it (``_PRE_FLOOR_BAND_OUTCOMES``): MEDIUM and
    a ticket from 0.8501 up. Codex review 6032024752 (at 8e33da83) confirmed
    that boundary and recommended no ticket without eligible directional
    evidence, one policy shared by provenance and stress, and tests of
    positive controls and the exact threshold. Anik adopted that floor
    first-hand on 2026-11-24, and the pin changed in the commit that
    implements it: zero eligible evidence is LOW with no ticket in every
    band, and the reason is recorded.
    """

    @pytest.mark.parametrize(
        ("ticker", "layers_present", "layers_total", "fudge_status"), _FUDGE_VARIANTS
    )
    @pytest.mark.parametrize(
        "confidence",
        [
            # Before the floor (``_PRE_FLOOR_BAND_OUTCOMES``): LOW below 0.55;
            # NO_TRADE from 0.55 to 0.85, after stress read every
            # perturbation as a flip; MEDIUM and a ticket above 0.85.
            pytest.param(0.40, id="below-0.55"),
            pytest.param(0.55, id="at-0.55"),
            pytest.param(0.72, id="0.55-0.85"),
            pytest.param(0.85, id="at-0.85"),
            pytest.param(0.8501, id="just-above-0.85"),
            pytest.param(0.92, id="above-0.85"),
        ],
    )
    def test_band_outcome_is_pinned(
        self,
        confidence: float,
        ticker: str,
        layers_present: int,
        layers_total: int,
        fudge_status: Any,
    ) -> None:
        resp = _gateway_decision(confidence=confidence, ticker=ticker)
        prov = resp.provenance_report
        assert prov is not None, resp.stage_errors

        # The fixture really is live and has zero eligible evidence.
        assert prov.decision_clock == _ZERO_EVIDENCE_CLOCK.isoformat()
        assert {ev.signal_source: ev.classification for ev in prov.signal_evidence} == {
            "jodi_oil": sp.EVIDENCE_ADVISORY,
            "pcr": sp.EVIDENCE_ADVISORY,
            "ghost_signal": "no_history",
        }
        assert all(ev.scorecard is None for ev in prov.signal_evidence)
        assert prov.shipping_fudge_alerts == []
        assert prov.layer_coverage["fudge_alerts"] == fudge_status
        assert prov.layer_coverage["edge_signal"] == sp.LAYER_NOT_APPLICABLE

        # Coverage, advisory layers and the neutral aggregate.
        assert len(_ADVISORY_LAYERS_NVDA) == 11
        assert prov.evidence_coverage == 0.0
        assert prov.layers_present == layers_present
        assert prov.layers_total == layers_total
        assert prov.layers_advisory == 11
        assert prov.advisory_layers == {
            name: pytest.approx(_UNFENCED_LAYER_VALUE) for name in _ADVISORY_LAYERS_NVDA
        }
        for name in _ADVISORY_LAYERS_NVDA:
            assert prov.layer_coverage[name] == sp.LAYER_ADVISORY
        assert prov.aggregate_conviction == pytest.approx(1.0)

        # The pinned outcome: LOW in every band, the floor named as the reason.
        assert prov.verdict == "low"
        assert prov.verdict_reason.startswith(sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE)
        assert "0 eligible" in prov.verdict_reason
        # Stress re-derives every perturbation under the same floor, so none
        # of them flips the LOW call (before the floor, 0.55-0.85 read as
        # fragile at 0.00 and was downgraded to NO_TRADE).
        assert resp.stress_report.robustness_label == "robust"
        assert resp.stress_report.robustness_score == pytest.approx(1.0)
        assert resp.stress_report.break_count == 0
        assert {p.new_verdict for p in resp.stress_report.perturbations} == {"low"}
        assert resp.unified_verdict == "low"
        assert any("pattern evidence unavailable" in r for r in resp.verdict_reasons)
        assert resp.trade_ticket is None
        assert set(resp.stage_errors) == {"trade_ticket"}
        assert resp.stage_errors["trade_ticket"] == "verdict=low below ticket threshold"
        assert "high" not in (prov.verdict, resp.unified_verdict)
        assert (prov.verdict, resp.unified_verdict, resp.trade_ticket is not None) == (
            _pinned_band_outcome(confidence)
        )
        assert _eligible_count(prov) == 0

        # The serialised response carries the same numbers.
        out = resp.to_dict()["provenance_report"]
        assert out["aggregate_conviction"] == pytest.approx(1.0)
        assert out["evidence_coverage"] == 0.0
        assert out["layers_present"] == layers_present
        assert out["layers_advisory"] == 11
        assert set(out["advisory_layers"]) == set(_ADVISORY_LAYERS_NVDA)
        assert out["verdict"] == "low"
        assert out["verdict_reason"].startswith(sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE)

    @pytest.mark.parametrize("ticker", ["NVDA", "XOM"])
    def test_high_never_appears_at_any_confidence(self, ticker: str) -> None:
        confidences = [round(0.30 + 0.05 * i, 2) for i in range(14)] + [0.8501]
        seen = {}
        for confidence in confidences:
            resp = _gateway_decision(confidence=confidence, ticker=ticker)
            seen[confidence] = (
                resp.provenance_report.verdict,
                resp.unified_verdict,
                resp.trade_ticket is not None,
            )

        assert all("high" not in outcome[:2] for outcome in seen.values()), seen
        assert seen == {c: _pinned_band_outcome(c) for c in confidences}


# ── The adopted evidence floor: positive controls and the exact threshold ──


def _fenced_card(source: str, weight: float, **overrides: Any) -> SignalScorecard:
    """A calibrated scorecard stamped a day before the decision clock."""
    card = _card_at(source, weight, _ZERO_EVIDENCE_CLOCK - timedelta(days=1))
    return replace(card, **overrides) if overrides else card


def _cards_with(**extra: SignalScorecard) -> dict[str, SignalScorecard]:
    """The zero-evidence cards with ``extra`` replacing or adding cards."""
    cards = _zero_evidence_cards()
    cards.update(extra)
    return cards


def _live_report(
    *,
    confidence: float,
    cards: dict[str, SignalScorecard],
    contributions: dict[str, float] | None = None,
) -> sp.TradeProvenanceReport:
    """``build_provenance_report`` on the zero-evidence fixture's clock."""
    pred = _prediction(
        ticker="NVDA",
        confidence=confidence,
        signal_contributions=dict(contributions or _ZERO_EVIDENCE_CONTRIBUTIONS),
    )
    pred.as_of = _ZERO_EVIDENCE_CLOCK
    with ExitStack() as stack:
        _patch_all_non_fudge_adapters(stack, value=_UNFENCED_LAYER_VALUE)
        stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW))
        _patch_calibration_tables(stack, regime_cards=cards, flat_cards={})
        return sp.build_provenance_report(MagicMock(), prediction=pred)


def _ticket(report: sp.TradeProvenanceReport) -> Any:
    with patch("oracle.risk.get_global_circuit_breaker", lambda: CircuitBreaker()):
        return generate_ticket(
            report, account_size_usd=100_000, current_price=120.0, vol_30d=0.35
        )


def _outcome(resp: dg.DecisionResponse) -> tuple[str, str, bool]:
    return (
        resp.provenance_report.verdict,
        resp.unified_verdict,
        resp.trade_ticket is not None,
    )


# Cards that are not eligible directional evidence on their own, each the
# only scorecard on the prediction (pcr and ghost_signal have none).
_INELIGIBLE_ALONE = [
    pytest.param(
        _card_at("jodi_oil", 1.3, _ZERO_EVIDENCE_CLOCK + timedelta(minutes=5)),
        sp.EVIDENCE_ADVISORY,
        id="late-same-hour",
    ),
    pytest.param(_card_at("jodi_oil", 1.3, None), sp.EVIDENCE_ADVISORY, id="unstamped"),
    pytest.param(
        _card_at("jodi_oil", 1.3, _ZERO_EVIDENCE_CLOCK.date()),
        sp.EVIDENCE_ADVISORY,
        id="date-only-stamp",
    ),
    pytest.param(
        replace(
            _card_at("jodi_oil", 1.0, _ZERO_EVIDENCE_CLOCK - timedelta(days=1)),
            scored_count=MIN_CALIBRATED_SAMPLES - 1,
            is_calibrated=False,
        ),
        "cold_start",
        id="fenced-cold-start",
    ),
]


class TestEvidenceFloor:
    """Positive controls and the exact threshold for the adopted floor.

    Eligible directional evidence (``sp.is_eligible_directional_evidence``)
    is an evidence row whose calibrated scorecard is stamped at or before
    the decision clock and is classified strong or neutral, behind a
    bullish or bearish call. A weak or anti-predictive card is against the
    direction its signal pushed. Fudge alerts dampen risk and are not
    directional evidence. The floor is one such row
    (``sp.MIN_ELIGIBLE_DIRECTIONAL_EVIDENCE``).
    """

    def test_the_floor_is_one_eligible_row_and_fudge_alerts_are_not_evidence(self) -> None:
        # Fenced fudge alerts on a shipping ticker are checked (the layer is
        # present) but add no eligible evidence.
        resp = _gateway_decision(confidence=0.92, ticker="XOM")
        assert resp.provenance_report.layer_coverage["fudge_alerts"] is True
        assert resp.trade_ticket is None
        assert _eligible_count(resp.provenance_report) == 0
        assert sp.MIN_ELIGIBLE_DIRECTIONAL_EVIDENCE == 1
        assert sp.DIRECTION_SUPPORTING_CLASSIFICATIONS == frozenset({"strong", "neutral"})

    @pytest.mark.parametrize("confidence", [0.8501, 0.92], ids=["just-above-0.85", "above-0.85"])
    def test_one_eligible_card_in_the_direction_still_gives_a_ticket(
        self, confidence: float
    ) -> None:
        resp = _gateway_decision(
            confidence=confidence, cards=_cards_with(jodi_oil=_fenced_card("jodi_oil", 1.3))
        )
        prov = resp.provenance_report
        assert _evidence_rows(prov)["jodi_oil"]["classification"] == "strong"
        assert prov.aggregate_conviction == pytest.approx(0.5 * 1.3 + 0.3 + 0.2)
        assert _outcome(resp) == ("medium", "medium", True)
        assert "saturated" in prov.verdict_reason
        assert resp.stress_report.robustness_label == "robust"
        assert resp.trade_ticket.verdict == "medium"
        assert resp.trade_ticket.direction == "bullish"
        assert resp.stage_errors == {}
        assert _eligible_count(prov) == 1

    @pytest.mark.parametrize(
        ("weight", "classification"),
        [
            # 0.5 x 0.45 + 0.5 = 0.725: conviction alone would still allow
            # MEDIUM and a ticket, so the floor is what refuses it.
            pytest.param(0.45, "weak", id="weak"),
            pytest.param(0.0, "anti_predictive", id="anti-predictive"),
        ],
    )
    def test_one_card_against_the_direction_gives_no_ticket(
        self, weight: float, classification: str
    ) -> None:
        resp = _gateway_decision(
            confidence=0.92, cards=_cards_with(jodi_oil=_fenced_card("jodi_oil", weight))
        )
        prov = resp.provenance_report
        assert _evidence_rows(prov)["jodi_oil"]["classification"] == classification
        assert prov.evidence_coverage == pytest.approx(0.5)
        assert _outcome(resp) == ("low", "low", False)
        assert prov.verdict_reason.startswith(sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE)
        assert _eligible_count(prov) == 0

    @pytest.mark.parametrize(("card", "classification"), _INELIGIBLE_ALONE)
    def test_advisory_late_unstamped_or_cold_start_card_alone_gives_no_ticket(
        self, card: SignalScorecard, classification: str
    ) -> None:
        resp = _gateway_decision(confidence=0.92, cards={"jodi_oil": card})
        prov = resp.provenance_report
        assert _evidence_rows(prov)["jodi_oil"]["classification"] == classification
        assert _outcome(resp) == ("low", "low", False)
        assert prov.verdict_reason.startswith(sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE)
        assert _eligible_count(prov) == 0

    @pytest.mark.parametrize(
        ("eligible", "confidence", "expected"),
        [
            pytest.param(0, 0.85, ("low", "low", False), id="0-eligible-at-0.85"),
            pytest.param(0, 0.8501, ("low", "low", False), id="0-eligible-at-0.8501"),
            # 0.85 is the calibrated zone, where HIGH needs layer coverage
            # that only advisory layers could supply: LOW, as before.
            pytest.param(1, 0.85, ("low", "low", False), id="1-eligible-at-0.85"),
            pytest.param(1, 0.8501, ("medium", "medium", True), id="1-eligible-at-0.8501"),
            pytest.param(2, 0.8501, ("medium", "medium", True), id="2-eligible-at-0.8501"),
        ],
    )
    def test_exact_threshold(
        self, eligible: int, confidence: float, expected: tuple[str, str, bool]
    ) -> None:
        extra = {
            "jodi_oil": _fenced_card("jodi_oil", 1.3),
            "pcr": _fenced_card("pcr", 1.2),
        }
        cards = _cards_with(**dict(list(extra.items())[:eligible]))
        resp = _gateway_decision(confidence=confidence, cards=cards)
        assert _outcome(resp) == expected
        assert resp.stress_report.robustness_label == "robust"
        assert _eligible_count(resp.provenance_report) == eligible
        if eligible == 0:
            assert resp.provenance_report.verdict_reason.startswith(
                sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE
            )

    def test_stress_imports_the_shared_policy(self) -> None:
        assert cs.decide_verdict is sp.decide_verdict

    @pytest.mark.parametrize("confidence", [0.85, 0.8501, 0.92])
    @pytest.mark.parametrize(
        "cards",
        [
            pytest.param(None, id="zero-evidence"),
            pytest.param(_cards_with(jodi_oil=_fenced_card("jodi_oil", 1.3)), id="one-in-direction"),
            pytest.param(_cards_with(jodi_oil=_fenced_card("jodi_oil", 0.45)), id="one-against"),
            *[
                pytest.param({"jodi_oil": p.values[0]}, id=f"alone-{p.id}")
                for p in _INELIGIBLE_ALONE
            ],
        ],
    )
    def test_stress_and_provenance_agree_on_the_floor(
        self, cards: dict[str, SignalScorecard] | None, confidence: float
    ) -> None:
        resp = _gateway_decision(confidence=confidence, cards=cards)
        prov = resp.provenance_report
        # Perturbing a row that holds no decision-bearing scorecard moves
        # neither conviction nor eligibility, so stress must reach the
        # provenance verdict there: it applies the same floor and coverage.
        by_source = {ev.signal_source: ev for ev in prov.signal_evidence}
        quiet = [
            p for p in resp.stress_report.perturbations
            if by_source[p.signal_source].scorecard is None
        ]
        assert quiet
        for p in quiet:
            assert p.new_aggregate_conviction == pytest.approx(prov.aggregate_conviction)
            assert (p.new_verdict, p.verdict_changed) == (prov.verdict, False)
        # Re-deriving the verdict with the shared policy reproduces it.
        assert sp.decide_verdict(
            prov.aggregate_conviction,
            prov.confidence,
            prov.signal_evidence,
            direction=prov.direction,
            decision_clock=prov.decision_clock,
            layer_coverage_ratio=prov.layers_present / prov.layers_total,
            layers_advisory=prov.layers_advisory,
        ) == (prov.verdict, prov.verdict_reason)

    def test_stress_floor_breaks_a_call_resting_on_one_marginal_card(self) -> None:
        # One fenced neutral card (Brier 0.15 -> weight 0.75) on a 0.2 share:
        # perturbed, it turns weak or anti-predictive, and the call loses its
        # only eligible row even though conviction stays at or above 0.80.
        card = _fenced_card("jodi_oil", 0.75, running_brier=0.15)
        report = _live_report(
            confidence=0.92,
            cards={"jodi_oil": card},
            contributions={"jodi_oil": 0.2, "pcr": 0.5, "ghost_signal": 0.3},
        )
        assert report.aggregate_conviction == pytest.approx(0.2 * 0.75 + 0.8)
        assert report.verdict == "medium"

        stress = cs.run_stress_test(report)
        broken = [p for p in stress.perturbations if p.signal_source == "jodi_oil"]
        assert [p.sigma for p in broken] == [-2.0, -1.0, 1.0]
        for p in broken:
            assert p.new_aggregate_conviction >= 0.8 - 1e-9
            assert (p.new_verdict, p.verdict_changed) == ("low", True)
        assert stress.break_count == 3
        assert stress.robustness_label == "fragile"
        assert _eligible_count(report) == 1

    def test_ticket_generator_refuses_a_medium_report_without_eligible_evidence(self) -> None:
        zero = _live_report(confidence=0.92, cards=_zero_evidence_cards())
        one = _live_report(
            confidence=0.92, cards=_cards_with(jodi_oil=_fenced_card("jodi_oil", 1.3))
        )
        # Forced to MEDIUM, as a report from another path could arrive.
        assert _ticket(replace(zero, verdict="medium")) is None
        assert _ticket(one) is not None

    def test_ticket_generator_rechecks_each_card_against_the_report_clock(self) -> None:
        report = _live_report(
            confidence=0.92, cards=_cards_with(jodi_oil=_fenced_card("jodi_oil", 1.3))
        )
        late = [
            replace(
                ev,
                scorecard=replace(
                    ev.scorecard, last_updated=_ZERO_EVIDENCE_CLOCK + timedelta(minutes=1)
                ),
            )
            if ev.scorecard is not None
            else ev
            for ev in report.signal_evidence
        ]
        assert _ticket(report) is not None
        assert _ticket(replace(report, signal_evidence=late)) is None
        assert _ticket(replace(report, decision_clock="not-a-clock")) is None

    def test_gateway_records_the_floor_when_a_medium_report_has_no_evidence(self) -> None:
        zero = replace(_live_report(confidence=0.92, cards=_zero_evidence_cards()), verdict="medium")
        with patch("oracle.risk.get_global_circuit_breaker", lambda: CircuitBreaker()):
            ticket, err = dg._run_trade_ticket(
                zero,
                account_size_usd=100_000,
                current_price=120.0,
                vol_30d=0.35,
                instrument="equity",
                unified_verdict="medium",
            )
        assert ticket is None
        assert err is not None and err.startswith(sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE)
