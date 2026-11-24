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
 10. Codex HOLD 6032982233: eligible evidence needs a signed input from the
     same decision (``TestCodexHold6032982233SignedInput``); actionable
     entry points need an explicit, timezone-aware decision instant
     (``TestCodexHold6032982233DecisionInstant``); clockless research
     reports are labelled and never ticket
     (``TestCodexHold6032982233ResearchReports``).
 11. Codex HOLD 6033471347, decided by Anik on 2026-11-24 ("Abstain now
     (B)"): eligible evidence needs a complete signed per-family receipt,
     which nothing in production builds, so no live report tickets; the
     aggregate head vote is labelled context and never counts; and a ticket
     must issue within a bounded age of its decision instant
     (``TestCodexHold6033471347SignedFamilyReceipts``). The positive paths
     elsewhere in this file are contract tests that pass test-constructed
     receipts (``_receipt``, ``_inject_receipts``).
"""
from __future__ import annotations

from contextlib import ExitStack
from dataclasses import fields, replace
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
    # The default head votes the call's direction (a signed input) and, when
    # explicit contributions are given, signs exactly those families.
    default_vote: dict[str, Any] = {
        "model_name": "flow_momentum",
        "vote_weight": 1.0,
        "direction": direction,
    }
    if signal_contributions:
        default_vote["signal_sources"] = list(signal_contributions)
    votes = model_votes if model_votes is not None else [default_vote]
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
        clock = datetime.now(timezone.utc) - timedelta(minutes=1)
        prov = SimpleNamespace(
            ticker="NVDA",
            verdict="high",
            aggregate_conviction=1.3,
            red_team_epistemic_risk=0.95,
            # A live report with one fenced card and a signed input in the
            # call's direction from the same decision: the floor passes.
            actionable=True,
            basis=sp.REPORT_BASIS_LIVE,
            direction="bullish",
            decision_clock=clock.isoformat(),
            signal_evidence=[
                sp.SignalEvidence(
                    "flow_momentum",
                    1.0,
                    _card("flow_momentum"),
                    "strong",
                    signed_input=sp.SignedInput(
                        "flow_momentum", "bullish", "NVDA", clock, 1.0, 0.0, ("flow_momentum",)
                    ),
                )
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
                {"model_name": "flow_momentum", "vote_weight": 0.4, "direction": "bullish"},
                {"model_name": "regime_contrarian", "vote_weight": 0.6, "direction": "bullish"},
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
        # A research report: no ticket since Codex HOLD 6032982233.
        assert rc == 1
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

    def test_naive_clock_is_refused(self) -> None:
        # Read as UTC until Codex HOLD 6032982233, which requires an
        # explicit, timezone-aware decision instant for actionable use.
        pred = _prediction(ticker="NVDA", signal_contributions={"jodi_oil": 1.0})
        late_now = datetime(2026, 3, 6, 0, 20, tzinfo=timezone.utc)
        pred.as_of = datetime(2026, 3, 6, 0, 10)  # noqa: DTZ001 - naive on purpose
        with ExitStack() as stack:
            _patch_all_non_fudge_adapters(stack, value=1.0)
            seen = _capture_dated_layers(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: late_now, create=True))
            with pytest.raises(ValueError, match="timezone-aware"):
                sp.build_provenance_report(MagicMock(), prediction=pred)

        assert seen == {}

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


def _receipt(
    family: str,
    call: str = "bullish",
    *,
    ticker: str = "NVDA",
    clock: datetime = _ZERO_EVIDENCE_CLOCK,
    heads: tuple[tuple[str, str], ...] | None = None,
    consumed: tuple[str, ...] | None = None,
    complete: Any = True,
) -> Any:
    """CONTRACT FIXTURE, not live: a signed per-family receipt.

    Nothing in production builds one (Codex HOLD 6033471347; Anik, "Abstain
    now (B)", 2026-11-24). Tests pass these to show the evidence floor is
    passable once an engine change exports receipts. The default is
    complete, signed ``call`` by its one contributing head, and bound to
    ``ticker`` and ``clock``. Built at call time, so the module still
    imports where the contract class is missing.
    """
    return sp.SignedFamilyReceipt(
        signal_source=family,
        direction=call,
        ticker=ticker,
        decision_clock=clock,
        consumed_signals=(f"{family}:obs-1",) if consumed is None else consumed,
        head_directions=(("head_a", call),) if heads is None else heads,
        complete_contributing_set=complete,
    )


def _receipts(
    *families: str,
    call: str = "bullish",
    ticker: str = "NVDA",
    clock: datetime = _ZERO_EVIDENCE_CLOCK,
) -> dict[str, Any]:
    """CONTRACT FIXTURE: one complete, agreeing receipt per family."""
    return {f: _receipt(f, call, ticker=ticker, clock=clock) for f in families}


def _inject_receipts(stack: ExitStack, receipts: dict[str, Any] | None) -> None:
    """CONTRACT FIXTURE: hand the live builder test-constructed receipts.

    Production's ``sp._family_receipts`` returns none; only a contract test
    replaces it, and only when it passes ``receipts``.
    """
    if receipts is not None:
        stack.enter_context(
            patch.object(sp, "_family_receipts", lambda *_a, **_k: dict(receipts))
        )


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
    receipts: dict[str, Any] | None = None,
) -> dg.DecisionResponse:
    """Run ``should_i_trade`` as production calls it, stubbing only the reads.

    Stubbed: the oracle (it returns the live prediction), the red-team LLM (a
    completed, advisory report), the pattern library (the degraded no-analog
    report, ``confidence_signal=0.0``), every adjuster read behind the 11
    unfenced layers, the two scorecard tables and, for ``XOM``, the fudge
    alert rows. Real: the gateway, ``build_provenance_report`` with its clock
    and fence, the counterfactual stress test and the ticket generator, given
    a fresh circuit breaker so no other test's breaker state can reach it.
    ``receipts`` makes it a contract test (``_inject_receipts``).
    """
    pred = _prediction(
        ticker=ticker,
        confidence=confidence,
        signal_contributions=dict(contributions or _ZERO_EVIDENCE_CONTRIBUTIONS),
    )
    pred.as_of = _ZERO_EVIDENCE_CLOCK
    return _run_gateway(pred, cards=cards, receipts=receipts)


def _run_gateway(
    pred: Any,
    *,
    cards: dict[str, SignalScorecard] | None = None,
    receipts: dict[str, Any] | None = None,
    engine: Any = None,
    flat_cards: dict[str, SignalScorecard] | None = None,
) -> dg.DecisionResponse:
    """``_gateway_decision``'s body for a caller-built live prediction."""
    ticker = pred.ticker
    confidence = pred.confidence
    red_team = llm_red_team.RedTeamReport(
        ticker=ticker,
        prediction_score=confidence,
        counters=(llm_red_team.CounterArgument("Valuation risk", 0.2, 0.2, 0.2),),
        epistemic_risk_score=0.2,
    )
    if engine is None:
        engine = _late_or_unstamped_fudge_engine() if ticker == "XOM" else MagicMock()
    with ExitStack() as stack:
        _patch_all_non_fudge_adapters(stack, value=_UNFENCED_LAYER_VALUE)
        _inject_receipts(stack, receipts)
        stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW))
        _patch_calibration_tables(
            stack,
            regime_cards=_zero_evidence_cards() if cards is None else cards,
            flat_cards=flat_cards or {},
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
    ticket (``_PRE_FLOOR_BAND_OUTCOMES``). Abstaining on receipts (round 7)
    leaves it LOW with no ticket.
    """
    del confidence
    return "low", "low", False


def _eligible_count(report: Any) -> int:
    return sp.count_eligible_directional_evidence(
        report.signal_evidence,
        direction=report.direction,
        decision_clock=report.decision_clock,
        ticker=report.ticker,
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

    Round 7 (Codex HOLD 6033471347; Anik, "Abstain now (B)", 2026-11-24):
    eligibility also needs a signed per-family receipt, which nothing in
    production builds. The pinned outcome is unchanged, LOW with no ticket in
    every band; every row now has no receipt, and the reason also records
    ``no_signed_family_receipt`` on all three rows.
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
        # Round 7: no row has a receipt, and the reason says so.
        assert all(ev.receipt is None for ev in prov.signal_evidence)
        assert f"{sp.NO_SIGNED_FAMILY_RECEIPT}: 3 of 3 rows" in prov.verdict_reason
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
        assert [row["receipt"] for row in out["signal_evidence"]] == [None, None, None]

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
    receipts: dict[str, Any] | None = None,
) -> sp.TradeProvenanceReport:
    """``build_provenance_report`` on the zero-evidence fixture's clock."""
    pred = _prediction(
        ticker="NVDA",
        confidence=confidence,
        signal_contributions=dict(contributions or _ZERO_EVIDENCE_CONTRIBUTIONS),
    )
    pred.as_of = _ZERO_EVIDENCE_CLOCK
    return _build_live(pred, cards=cards, receipts=receipts)


def _build_live(
    pred: Any,
    *,
    cards: dict[str, SignalScorecard],
    receipts: dict[str, Any] | None = None,
    engine: Any = None,
) -> sp.TradeProvenanceReport:
    """``build_provenance_report`` on a caller-built live prediction;
    ``receipts`` makes it a contract test (``_inject_receipts``)."""
    with ExitStack() as stack:
        _patch_all_non_fudge_adapters(stack, value=_UNFENCED_LAYER_VALUE)
        _inject_receipts(stack, receipts)
        stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW))
        _patch_calibration_tables(stack, regime_cards=cards, flat_cards={})
        return sp.build_provenance_report(
            MagicMock() if engine is None else engine, prediction=pred
        )


def _ticket_at(report: Any, now: datetime) -> Any:
    """``generate_ticket`` with the wall clock at ``now``."""
    with ExitStack() as stack:
        stack.enter_context(
            patch("oracle.risk.get_global_circuit_breaker", lambda: CircuitBreaker())
        )
        stack.enter_context(patch.object(sp, "_utc_now", lambda: now))
        return generate_ticket(
            report, account_size_usd=100_000, current_price=120.0, vol_30d=0.35
        )


def _ticket(report: Any) -> Any:
    """``generate_ticket`` at the fixtures' wall clock, two minutes after
    their decision clock (inside the issuance bound)."""
    return _ticket_at(report, _LIVE_NOW)


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

    Round 7 (Codex HOLD 6033471347; Anik, "Abstain now (B)", 2026-11-24): an
    eligible row also needs a complete signed per-family receipt, which
    nothing in production builds. The positive controls here are contract
    tests: they pass test-constructed receipts (``_receipts``). The same
    fixtures without receipts, the live case, are LOW with no ticket.
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
        cards = _cards_with(jodi_oil=_fenced_card("jodi_oil", 1.3))
        # Live (no receipt): the card alone abstains.
        live = _gateway_decision(confidence=confidence, cards=cards)
        assert _outcome(live) == ("low", "low", False)
        assert _eligible_count(live.provenance_report) == 0
        # Contract: the same card with a complete, agreeing receipt.
        resp = _gateway_decision(
            confidence=confidence, cards=cards, receipts=_receipts("jodi_oil")
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
        # Contract: a receipt does not rescue a card against the direction.
        resp = _gateway_decision(
            confidence=0.92,
            cards=_cards_with(jodi_oil=_fenced_card("jodi_oil", weight)),
            receipts=_receipts("jodi_oil"),
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
        # Contract: a receipt does not rescue an ineligible card.
        resp = _gateway_decision(
            confidence=0.92, cards={"jodi_oil": card}, receipts=_receipts("jodi_oil")
        )
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
        # Contract: every family carries a complete, agreeing receipt, so the
        # count is the cards' (pcr's unstamped card and ghost_signal's
        # missing one stay ineligible).
        resp = _gateway_decision(
            confidence=confidence, cards=cards, receipts=_receipts(*_ZERO_EVIDENCE_CONTRIBUTIONS)
        )
        assert _outcome(resp) == expected
        assert resp.stress_report.robustness_label == "robust"
        assert _eligible_count(resp.provenance_report) == eligible
        if eligible == 0:
            assert resp.provenance_report.verdict_reason.startswith(
                sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE
            )

    def test_stress_imports_the_shared_policy(self) -> None:
        assert cs.decide_verdict is sp.decide_verdict

    # Live (no receipts) and contract (a receipt on every family).
    @pytest.mark.parametrize("receipted", [False, True], ids=["live", "contract-receipts"])
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
        self, cards: dict[str, SignalScorecard] | None, confidence: float, receipted: bool
    ) -> None:
        resp = _gateway_decision(
            confidence=confidence,
            cards=cards,
            receipts=_receipts(*_ZERO_EVIDENCE_CONTRIBUTIONS) if receipted else None,
        )
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
            ticker=prov.ticker,
            decision_clock=prov.decision_clock,
            layer_coverage_ratio=prov.layers_present / prov.layers_total,
            layers_advisory=prov.layers_advisory,
        ) == (prov.verdict, prov.verdict_reason)

    def test_stress_floor_breaks_a_call_resting_on_one_marginal_card(self) -> None:
        # One fenced neutral card (Brier 0.15 -> weight 0.75) on a 0.2 share:
        # perturbed, it turns weak or anti-predictive, and the call loses its
        # only eligible row even though conviction stays at or above 0.80.
        card = _fenced_card("jodi_oil", 0.75, running_brier=0.15)
        # Contract: jodi_oil carries a complete, agreeing receipt.
        report = _live_report(
            confidence=0.92,
            cards={"jodi_oil": card},
            contributions={"jodi_oil": 0.2, "pcr": 0.5, "ghost_signal": 0.3},
            receipts=_receipts("jodi_oil"),
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
        live_one = _live_report(
            confidence=0.92, cards=_cards_with(jodi_oil=_fenced_card("jodi_oil", 1.3))
        )
        # Contract: the same card with a complete, agreeing receipt.
        one = _live_report(
            confidence=0.92,
            cards=_cards_with(jodi_oil=_fenced_card("jodi_oil", 1.3)),
            receipts=_receipts("jodi_oil"),
        )
        # Forced to MEDIUM, as a report from another path could arrive.
        assert _ticket(replace(zero, verdict="medium")) is None
        assert _ticket(replace(live_one, verdict="medium")) is None
        assert _ticket(one) is not None

    def test_ticket_generator_rechecks_each_card_against_the_report_clock(self) -> None:
        # Contract: jodi_oil carries a complete, agreeing receipt.
        report = _live_report(
            confidence=0.92,
            cards=_cards_with(jodi_oil=_fenced_card("jodi_oil", 1.3)),
            receipts=_receipts("jodi_oil"),
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
        ticket, err = _gateway_ticket_stage(zero)
        assert ticket is None
        assert err is not None and err.startswith(sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE)
        assert f"{sp.NO_SIGNED_FAMILY_RECEIPT}: 3 of 3 rows" in err


# ── Codex HOLD 6032982233: a signed input from the decision, and its instant ─
#
# Codex review 6032982233 (at dba5ef01) held the adopted floor on two
# points. (1) A scorecard is historical skill, not an observation made at
# the decision: the same positive-weight calibrated card passed unchanged
# behind a bullish and a bearish call. Eligible evidence now also needs a
# signed input for the same signal family, bound to the report's ticker and
# decision clock, whose sign agrees with the call. The live prediction
# carries one: each oracle head vote in ``model_votes`` has the direction
# ``predict(ticker, as_of)`` computed from that head's signals at the clock,
# and a family's signed input is the direction of the votes that weight it.
# (2) A missing, blank or date-only clock skipped the time fence, so a
# constructed MEDIUM report with a card stamped after the decision could
# reach a ticket. Actionable entry points now need an explicit,
# timezone-aware decision instant; clockless research reports are labelled
# non-actionable and never ticket, and their stress test is not live parity.
#
# Round 7 (Codex HOLD 6033471347, at cbfd8301): a head's direction is the
# aggregate over everything that head consumed, so spreading it to the
# head's configured families signs a family that was unused, or bearish,
# as bullish; and ``model_votes`` keeps the top ten heads, so a dissenting
# eleventh head cannot veto. Anik decided "Abstain now (B)" on 2026-11-24:
# the head vote stays on each row as labelled context (``signed_input``,
# ``basis="aggregate_head_vote_not_a_receipt"``) and never counts, and
# eligibility needs a complete signed per-family receipt instead. The live
# outcomes below are the abstain outcomes; the positive paths are contract
# tests with test-constructed receipts.

_SIGNED_CLOCK = _ZERO_EVIDENCE_CLOCK


def _vote(
    model: str,
    direction: str | None,
    families: list[str] | dict[str, float],
    weight: float = 1.0,
) -> dict[str, Any]:
    """One oracle head vote; ``direction=None`` is an unsigned vote."""
    vote: dict[str, Any] = {
        "model_name": model,
        "vote_weight": weight,
        "signal_sources": dict(families) if isinstance(families, dict) else list(families),
    }
    if direction is not None:
        vote["direction"] = direction
    return vote


def _signed_prediction(
    *,
    call: str,
    votes: list[dict[str, Any]],
    confidence: float = 0.92,
    ticker: str = "NVDA",
) -> SimpleNamespace:
    """A live prediction whose rows and signs both come from ``votes``."""
    pred = _prediction(ticker=ticker, direction=call, confidence=confidence, model_votes=votes)
    pred.as_of = _SIGNED_CLOCK
    return pred


def _strong_jodi() -> dict[str, SignalScorecard]:
    return {"jodi_oil": _fenced_card("jodi_oil", 1.3)}


def _restamp(report: sp.TradeProvenanceReport, stamp: datetime) -> list[sp.SignalEvidence]:
    return [
        replace(ev, scorecard=replace(ev.scorecard, last_updated=stamp))
        if ev.scorecard is not None
        else ev
        for ev in report.signal_evidence
    ]


def _stage_at(report: Any, now: datetime) -> tuple[Any, str | None]:
    """The gateway's ticket stage with the wall clock at ``now``."""
    with ExitStack() as stack:
        stack.enter_context(
            patch("oracle.risk.get_global_circuit_breaker", lambda: CircuitBreaker())
        )
        stack.enter_context(patch.object(sp, "_utc_now", lambda: now))
        return dg._run_trade_ticket(
            report,
            account_size_usd=100_000,
            current_price=120.0,
            vol_30d=0.35,
            instrument="equity",
            unified_verdict=report.verdict,
        )


def _gateway_ticket_stage(report: Any) -> tuple[Any, str | None]:
    """The gateway's ticket stage at the fixtures' wall clock."""
    return _stage_at(report, _LIVE_NOW)


class TestCodexHold6032982233SignedInput:
    """Point (1): eligible evidence is a signed input bound to the decision.

    Round 7 (Codex HOLD 6033471347; "Abstain now (B)"): the head-vote sign
    built here is the aggregate head vote, kept as labelled context. It no
    longer makes a row eligible, so the live outcomes are LOW with no ticket;
    the positive paths pass test-constructed receipts (contract tests).
    """

    @pytest.mark.parametrize("sign", ["bullish", "bearish"])
    def test_paired_same_card_and_input_only_the_matching_call_tickets(self, sign: str) -> None:
        votes = [_vote("head_a", sign, ["jodi_oil"])]
        other = "bearish" if sign == "bullish" else "bullish"
        # Live: the head vote agrees with ``sign``, but it is not a receipt,
        # so neither call tickets (round 6 ticketed the matching call).
        for call in (sign, other):
            live = _run_gateway(_signed_prediction(call=call, votes=votes), cards=_strong_jodi())
            assert _outcome(live) == ("low", "low", False), call
            assert _eligible_count(live.provenance_report) == 0, call
        # Contract: the same card with a receipt signed ``sign``.
        receipt = {"jodi_oil": _receipt("jodi_oil", sign)}
        match = _run_gateway(
            _signed_prediction(call=sign, votes=votes), cards=_strong_jodi(), receipts=receipt
        )
        against = _run_gateway(
            _signed_prediction(call=other, votes=votes), cards=_strong_jodi(), receipts=receipt
        )

        # Only the call the receipt agrees with qualifies...
        assert _outcome(match) == ("medium", "medium", True)
        assert match.trade_ticket.direction == sign
        assert _outcome(against) == ("low", "low", False)
        assert against.provenance_report.verdict_reason.startswith(
            sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE
        )
        # ...from the same card, row weight, head vote and receipt.
        m_row = _evidence_rows(match.provenance_report)["jodi_oil"]
        a_row = _evidence_rows(against.provenance_report)["jodi_oil"]
        assert m_row["scorecard"] == a_row["scorecard"]
        assert m_row["classification"] == a_row["classification"] == "strong"
        assert m_row["shapley_weight"] == a_row["shapley_weight"] == pytest.approx(1.0)
        assert m_row["signed_input"] == a_row["signed_input"]
        assert m_row["signed_input"]["direction"] == sign
        assert m_row["receipt"] == a_row["receipt"]
        assert m_row["receipt"]["direction"] == sign
        assert _eligible_count(match.provenance_report) == 1
        assert _eligible_count(against.provenance_report) == 0

    def test_the_signed_input_is_the_head_vote_bound_to_ticker_and_clock(self) -> None:
        # Round 7: kept as labelled context; it is not a receipt and does
        # not count, so the strong card behind it abstains.
        report = _build_live(
            _signed_prediction(
                call="bullish",
                votes=[_vote("head_a", "bullish", ["jodi_oil"], weight=2.0)],
            ),
            cards=_strong_jodi(),
        )
        assert report.verdict == "low"
        assert _eligible_count(report) == 0
        assert report.signal_evidence[0].receipt is None
        assert _evidence_rows(report)["jodi_oil"]["receipt"] is None
        si = report.signal_evidence[0].signed_input
        assert si == sp.SignedInput(
            signal_source="jodi_oil",
            direction="bullish",
            ticker="NVDA",
            decision_clock=_SIGNED_CLOCK,
            bullish_weight=2.0,
            bearish_weight=0.0,
            models=("head_a",),
        )
        assert _evidence_rows(report)["jodi_oil"]["signed_input"] == {
            "basis": "aggregate_head_vote_not_a_receipt",
            "signal_source": "jodi_oil",
            "direction": "bullish",
            "ticker": "NVDA",
            "decision_clock": _SIGNED_CLOCK.isoformat(),
            "bullish_weight": 2.0,
            "bearish_weight": 0.0,
            "models": ["head_a"],
        }

    @pytest.mark.parametrize(
        ("votes", "signed_direction"),
        [
            pytest.param([_vote("head_a", "bearish", ["jodi_oil"])], "bearish", id="opposite"),
            pytest.param(
                [
                    _vote("head_a", "bullish", ["jodi_oil"]),
                    _vote("head_b", "bearish", ["jodi_oil"], weight=0.5),
                ],
                "mixed",
                id="heads-disagree",
            ),
            pytest.param([_vote("head_a", "neutral", ["jodi_oil"])], "neutral", id="neutral"),
            pytest.param([_vote("head_a", None, ["jodi_oil"])], None, id="unsigned-vote"),
        ],
    )
    def test_an_opposite_mixed_neutral_or_absent_input_is_refused(
        self, votes: list[dict[str, Any]], signed_direction: str | None
    ) -> None:
        resp = _run_gateway(_signed_prediction(call="bullish", votes=votes), cards=_strong_jodi())
        prov = resp.provenance_report
        assert _outcome(resp) == ("low", "low", False)
        assert prov.verdict_reason.startswith(sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE)
        row = _evidence_rows(prov)["jodi_oil"]
        # The card alone would qualify (it is the positive control's card).
        assert row["classification"] == "strong"
        if signed_direction is None:
            assert row["signed_input"] is None
        else:
            assert row["signed_input"]["direction"] == signed_direction
        assert _eligible_count(prov) == 0
        # The ticket backstop refuses the same report forced to MEDIUM.
        forced = replace(prov, verdict="medium")
        assert _ticket(forced) is None
        assert sp.actionable_refusal_reason(forced, now=_LIVE_NOW).startswith(
            sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE
        )
        # Contract: a receipt that says the same as this input is refused too.
        if signed_direction is not None:
            receipt = (
                _receipt("jodi_oil", heads=(("head_a", "bullish"), ("head_b", "bearish")))
                if signed_direction == sp.SIGNED_INPUT_MIXED
                else _receipt("jodi_oil", signed_direction)
            )
            contract = _run_gateway(
                _signed_prediction(call="bullish", votes=votes),
                cards=_strong_jodi(),
                receipts={"jodi_oil": receipt},
            )
            assert _outcome(contract) == ("low", "low", False)
            assert _eligible_count(contract.provenance_report) == 0

    def _positive_control(self, receipts: dict[str, Any] | None) -> dg.DecisionResponse:
        return _run_gateway(
            _signed_prediction(
                call="bullish",
                votes=[
                    # The zero-evidence fixture's shares: the card is on half
                    # the weight, so a perturbed card keeps conviction >= 0.70.
                    _vote("head_a", "bullish", {"jodi_oil": 0.5, "pcr": 0.3, "ghost_signal": 0.2}),
                    # A neutral vote on the family does not unsign it.
                    _vote("head_b", "neutral", ["jodi_oil"], weight=0.1),
                ],
            ),
            cards=_strong_jodi(),
            receipts=receipts,
        )

    def test_positive_control_matching_sign_gives_no_ticket_live(self) -> None:
        # Round 6's positive control: a matching head vote alone. Round 7
        # abstains: the aggregate head vote is not a receipt.
        resp = self._positive_control(None)
        prov = resp.provenance_report
        assert _outcome(resp) == ("low", "low", False)
        assert _evidence_rows(prov)["jodi_oil"]["signed_input"]["direction"] == "bullish"
        assert _eligible_count(prov) == 0
        assert sp.actionable_refusal_reason(prov, now=_LIVE_NOW).startswith(
            sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE
        )

    def test_positive_control_matching_sign_gives_a_robust_ticket(self) -> None:
        # Contract: the same prediction with a complete, agreeing receipt.
        resp = self._positive_control(_receipts("jodi_oil"))
        prov = resp.provenance_report
        assert _outcome(resp) == ("medium", "medium", True)
        assert resp.stage_errors == {}
        assert sp.actionable_refusal_reason(prov, now=_LIVE_NOW) is None
        jodi = _evidence_rows(prov)["jodi_oil"]["signed_input"]
        assert (jodi["direction"], jodi["models"]) == ("bullish", ["head_a"])
        assert _eligible_count(prov) == 1
        # Stress perturbs the card's skill, not what the signal said at the
        # decision: each perturbed row keeps its receipt and head vote, the
        # card stays neutral or better at every sigma, and the call holds.
        (idx,) = [i for i, ev in enumerate(prov.signal_evidence) if ev.signal_source == "jodi_oil"]
        swapped = cs._swap_signal_evidence(prov.signal_evidence, idx, 0.6)
        assert swapped[idx].signed_input == prov.signal_evidence[idx].signed_input
        assert swapped[idx].receipt == prov.signal_evidence[idx].receipt is not None
        assert resp.stress_report.robustness_label == "robust"
        assert resp.stress_report.break_count == 0
        assert resp.stress_report.live_parity is True

    def test_an_input_from_another_ticker_or_decision_is_refused(self) -> None:
        # Contract: each report carries a receipt for its own ticker.
        nvda = _build_live(
            _signed_prediction(call="bullish", votes=[_vote("head_a", "bullish", ["jodi_oil"])]),
            cards=_strong_jodi(),
            receipts=_receipts("jodi_oil"),
        )
        amd = _build_live(
            _signed_prediction(
                call="bullish", votes=[_vote("head_a", "bullish", ["jodi_oil"])], ticker="AMD"
            ),
            cards=_strong_jodi(),
            receipts=_receipts("jodi_oil", ticker="AMD"),
        )
        assert nvda.verdict == amd.verdict == "medium"
        assert _ticket(nvda) is not None

        cases = {
            # AMD's rows, signed for AMD, moved into the NVDA report.
            "rows-from-another-ticker": replace(nvda, signal_evidence=amd.signal_evidence),
            # The NVDA report relabelled as another ticker.
            "report-for-another-ticker": replace(nvda, ticker="AMD"),
            # The same rows under an earlier decision clock: the card is
            # still fenced, but the receipt is from the later decision.
            "another-decision": replace(
                nvda, decision_clock=(_SIGNED_CLOCK - timedelta(minutes=1)).isoformat()
            ),
        }
        for name, forged in cases.items():
            assert _ticket(forged) is None, name
            assert sp.actionable_refusal_reason(forged, now=_LIVE_NOW).startswith(
                sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE
            ), name

    def test_head_votes_sign_families_through_the_oracle_models_map(self) -> None:
        # The live shape: predict() votes carry only model_name, direction
        # and weight; oracle_models maps each head to its families.
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchall.return_value = [
            ("flow_momentum", ["equity", "flows", "breadth", "vol"], None),
            ("regime_contrarian", ["rates", "credit", "vol", "macro"], None),
        ]
        families = ("equity", "flows", "breadth", "vol", "rates", "credit", "macro")
        cards = {name: _fenced_card(name, 1.3) for name in families}
        votes = [
            {"model_name": "flow_momentum", "vote_weight": 0.4, "direction": "bullish"},
            {"model_name": "regime_contrarian", "vote_weight": 0.6, "direction": "bearish"},
        ]
        signs: dict[str, dict[str, Any]] = {}
        for call in ("bullish", "bearish"):
            pred = _signed_prediction(call=call, votes=votes)
            with ExitStack() as stack:
                _patch_all_non_fudge_adapters(stack, value=_UNFENCED_LAYER_VALUE)
                stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW))
                _patch_calibration_tables(stack, regime_cards=cards, flat_cards={})
                report = sp.build_provenance_report(engine, prediction=pred)
            signs[call] = {
                ev.signal_source: ev.signed_input.direction for ev in report.signal_evidence
            }
            eligible = {
                ev.signal_source
                for ev in report.signal_evidence
                if sp.is_eligible_directional_evidence(
                    ev, direction=call, decision_clock=_SIGNED_CLOCK, ticker="NVDA"
                )
            }
            # Round 6 kept, behind each call, the families its heads signed
            # its way (bullish: equity, flows, breadth; bearish: rates,
            # credit, macro). That is the aggregate spread Codex HOLD
            # 6033471347 refused: round 7 counts none of them.
            assert eligible == set(), call
            assert all(ev.receipt is None for ev in report.signal_evidence), call
            assert report.verdict == "low", call
        assert signs["bullish"] == signs["bearish"] == {
            "equity": "bullish",
            "flows": "bullish",
            "breadth": "bullish",
            "vol": sp.SIGNED_INPUT_MIXED,
            "rates": "bearish",
            "credit": "bearish",
            "macro": "bearish",
        }

    def test_eligibility_reads_the_sign_ticker_and_clock(self) -> None:
        # Contract: the sign, ticker and clock read are now the receipt's.
        report = _build_live(
            _signed_prediction(call="bullish", votes=[_vote("head_a", "bullish", ["jodi_oil"])]),
            cards=_strong_jodi(),
            receipts=_receipts("jodi_oil"),
        )
        (ev,) = report.signal_evidence

        def eligible(**kw: Any) -> bool:
            args: dict[str, Any] = {
                "direction": "bullish",
                "decision_clock": _SIGNED_CLOCK,
                "ticker": "NVDA",
            }
            args.update(kw)
            return sp.is_eligible_directional_evidence(ev, **args)

        assert eligible()
        assert eligible(ticker="nvda")
        assert not eligible(direction="bearish")
        assert not eligible(ticker="AMD")
        assert not eligible(ticker="")
        assert not eligible(decision_clock=_SIGNED_CLOCK + timedelta(seconds=1))
        assert not eligible(decision_clock=None)
        # The agreeing head vote alone (no receipt) does not count.
        assert ev.signed_input.direction == "bullish"
        assert not sp.is_eligible_directional_evidence(
            replace(ev, receipt=None),
            direction="bullish",
            decision_clock=_SIGNED_CLOCK,
            ticker="NVDA",
        )
        assert not sp.is_eligible_directional_evidence(
            replace(ev, receipt=replace(ev.receipt, signal_source="pcr")),
            direction="bullish",
            decision_clock=_SIGNED_CLOCK,
            ticker="NVDA",
        )
        # The receipt alone carries it: without the head vote it still counts.
        assert sp.is_eligible_directional_evidence(
            replace(ev, signed_input=None),
            direction="bullish",
            decision_clock=_SIGNED_CLOCK,
            ticker="NVDA",
        )


def _ticketable_report() -> sp.TradeProvenanceReport:
    """CONTRACT FIXTURE: a live-built report that tickets because its one
    row carries a test-constructed receipt (live rows have none)."""
    report = _build_live(
        _signed_prediction(call="bullish", votes=[_vote("head_a", "bullish", ["jodi_oil"])]),
        cards=_strong_jodi(),
        receipts=_receipts("jodi_oil"),
    )
    assert report.verdict == "medium"
    assert _ticket(report) is not None
    return report


class TestCodexHold6032982233DecisionInstant:
    """Point (2): actionable entry points need an explicit, aware instant."""

    def test_refusal_codes_are_stable(self) -> None:
        assert (
            sp.DECISION_CLOCK_MISSING,
            sp.DECISION_CLOCK_BLANK,
            sp.DECISION_CLOCK_DATE_ONLY,
            sp.DECISION_CLOCK_NOT_AWARE,
            sp.DECISION_CLOCK_UNPARSEABLE,
            sp.DECISION_CLOCK_IN_FUTURE,
            sp.NOT_ACTIONABLE,
            sp.SIGNED_INPUT_MIXED,
        ) == (
            "decision_clock_missing",
            "decision_clock_blank",
            "decision_clock_date_only",
            "decision_clock_not_timezone_aware",
            "decision_clock_unparseable",
            "decision_clock_in_future",
            "not_actionable",
            "mixed",
        )

    @pytest.mark.parametrize(
        ("clock", "card_stamp", "code"),
        [
            # Before the fix, no clock skipped the card fence, so a card
            # stamped a day after the decision qualified.
            pytest.param(
                None, _SIGNED_CLOCK + timedelta(days=1), "decision_clock_missing", id="none"
            ),
            pytest.param(
                "", _SIGNED_CLOCK + timedelta(days=1), "decision_clock_blank", id="blank"
            ),
            pytest.param(
                "   ", _SIGNED_CLOCK + timedelta(days=1), "decision_clock_blank", id="whitespace"
            ),
            # Before the fix a bare date read as midnight.
            pytest.param(
                _SIGNED_CLOCK.date().isoformat(),
                _SIGNED_CLOCK - timedelta(days=1),
                "decision_clock_date_only",
                id="date-only",
            ),
            pytest.param(
                _SIGNED_CLOCK.replace(tzinfo=None).isoformat(),
                _SIGNED_CLOCK - timedelta(days=1),
                "decision_clock_not_timezone_aware",
                id="naive",
            ),
            pytest.param(
                "not-a-clock",
                _SIGNED_CLOCK - timedelta(days=1),
                "decision_clock_unparseable",
                id="unparseable",
            ),
        ],
    )
    def test_a_constructed_medium_report_without_a_valid_instant_gets_no_ticket(
        self, clock: Any, card_stamp: datetime, code: str
    ) -> None:
        report = _ticketable_report()
        forged = replace(
            report, decision_clock=clock, signal_evidence=_restamp(report, card_stamp)
        )
        assert forged.verdict == "medium"
        assert _ticket(forged) is None
        reason = sp.actionable_refusal_reason(forged, now=_LIVE_NOW)
        assert reason is not None and reason.startswith(code)
        ticket, err = _gateway_ticket_stage(forged)
        assert ticket is None
        assert err is not None and err.startswith(code)

    def test_a_decision_instant_after_now_is_refused(self) -> None:
        report = _ticketable_report()
        future = datetime.now(timezone.utc) + timedelta(days=1)
        forged = replace(report, decision_clock=future.isoformat())
        assert _ticket(forged) is None
        assert sp.actionable_refusal_reason(forged).startswith(sp.DECISION_CLOCK_IN_FUTURE)
        ticket, err = _gateway_ticket_stage(forged)
        assert ticket is None and err.startswith(sp.DECISION_CLOCK_IN_FUTURE)

    @pytest.mark.parametrize(
        "late",
        [timedelta(microseconds=1), timedelta(minutes=5), timedelta(days=1)],
        ids=["1us", "5min", "1d"],
    )
    def test_a_card_stamped_after_the_decision_is_ineligible_in_the_backstop(
        self, late: timedelta
    ) -> None:
        report = _ticketable_report()
        forged = replace(report, signal_evidence=_restamp(report, _SIGNED_CLOCK + late))
        assert _ticket(forged) is None
        assert sp.actionable_refusal_reason(forged, now=_LIVE_NOW).startswith(
            sp.NO_ELIGIBLE_DIRECTIONAL_EVIDENCE
        )

    @pytest.mark.parametrize(
        "raw",
        [
            pytest.param(None, id="none"),
            pytest.param("", id="blank"),
            pytest.param("   ", id="whitespace"),
            pytest.param(_SIGNED_CLOCK.date().isoformat(), id="date-only-string"),
            pytest.param(_SIGNED_CLOCK.date(), id="date"),
            pytest.param(_SIGNED_CLOCK.replace(tzinfo=None), id="naive-datetime"),
            pytest.param(_SIGNED_CLOCK.replace(tzinfo=None).isoformat(), id="naive-string"),
            pytest.param("not-a-clock", id="unparseable"),
            pytest.param(1764000000, id="number"),
        ],
    )
    def test_parse_refuses_anything_but_an_aware_instant(self, raw: Any) -> None:
        clock, reason = sp.parse_decision_instant(raw)
        assert clock is None
        assert reason is not None
        # Nothing counts as eligible on such a clock.
        report = _ticketable_report()
        assert sp.count_eligible_directional_evidence(
            report.signal_evidence, direction="bullish", decision_clock=raw, ticker="NVDA"
        ) == 0

    def test_parse_accepts_an_aware_instant_and_its_iso_string(self) -> None:
        for raw in (_SIGNED_CLOCK, _SIGNED_CLOCK.isoformat()):
            assert sp.parse_decision_instant(raw) == (_SIGNED_CLOCK, None)
        offset = _SIGNED_CLOCK.astimezone(timezone(timedelta(hours=-5)))
        assert sp.parse_decision_instant(offset.isoformat()) == (_SIGNED_CLOCK, None)

    @pytest.mark.parametrize(
        "as_of",
        [
            pytest.param(None, id="none"),
            pytest.param("", id="blank"),
            pytest.param(_LIVE_NOW.replace(tzinfo=None) - timedelta(minutes=2), id="naive"),
            pytest.param(_LIVE_NOW.date(), id="date-only"),
            pytest.param(_LIVE_NOW + timedelta(minutes=5), id="future"),
        ],
    )
    def test_the_builder_refuses_a_clock_that_is_not_an_aware_instant(self, as_of: Any) -> None:
        pred = _signed_prediction(call="bullish", votes=[_vote("head_a", "bullish", ["jodi_oil"])])
        pred.as_of = as_of
        with ExitStack() as stack:
            reads = _spy_every_layer(stack)
            stack.enter_context(patch.object(sp, "_utc_now", lambda: _LIVE_NOW))
            with pytest.raises(ValueError, match="live-only"):
                sp.build_provenance_report(MagicMock(), prediction=pred)
        assert reads == []


def _walk_forward_report() -> sp.TradeProvenanceReport:
    from scripts import walk_forward_validate as wfv

    created = _SIGNED_CLOCK - timedelta(days=30)
    row = {
        "id": "p1",
        "ticker": "NVDA",
        "created_at": created,
        "expiry": created + timedelta(days=7),
        "confidence": 0.92,
        "verdict": "hit",
        "model_name": None,
        "signals": {"direction": "bullish", "regime": "EXPANSION", "fci_regime": "EASY"},
        "signal_contributions": {"jodi_oil": 1.0},
        "model_weights": None,
    }
    return wfv.build_time_frozen_provenance(
        row, {"jodi_oil": _card_at("jodi_oil", 1.3, created - timedelta(days=1))}
    )


class TestCodexHold6032982233ResearchReports:
    """Clockless research reports are labelled and never ticket."""

    def test_live_report_is_labelled_actionable(self) -> None:
        report = _ticketable_report()
        assert report.actionable is True
        assert report.basis == sp.REPORT_BASIS_LIVE == "live_decision_clock"
        out = report.to_dict()
        assert out["actionable"] is True
        assert out["basis"] == "live_decision_clock"

    def test_walk_forward_report_is_research_and_never_tickets(self) -> None:
        report = _walk_forward_report()
        assert report.actionable is False
        assert report.basis == sp.REPORT_BASIS_RESEARCH == "research_no_clock"
        assert report.decision_clock == ""
        out = report.to_dict()
        assert (out["actionable"], out["basis"]) == (False, "research_no_clock")
        # Forced to a strong MEDIUM call, and even given a valid clock, it is
        # refused: research reports are not actionable.
        forced = replace(report, verdict="medium", aggregate_conviction=1.3)
        with_clock = replace(forced, decision_clock=_SIGNED_CLOCK.isoformat())
        for forged in (forced, with_clock):
            assert _ticket(forged) is None
            assert sp.actionable_refusal_reason(forged).startswith(sp.NOT_ACTIONABLE)
            ticket, err = _gateway_ticket_stage(forged)
            assert ticket is None and err.startswith(sp.NOT_ACTIONABLE)

    def test_a_hand_built_report_defaults_to_research(self) -> None:
        live = _ticketable_report()
        fields = {
            name: getattr(live, name)
            for name in sp.TradeProvenanceReport.__dataclass_fields__
            if name not in ("actionable", "basis")
        }
        bare = sp.TradeProvenanceReport(**fields)
        assert (bare.actionable, bare.basis) == (False, sp.REPORT_BASIS_RESEARCH)
        assert _ticket(bare) is None

    def test_research_stress_is_labelled_and_is_not_live_parity(self) -> None:
        research = cs.run_stress_test(_walk_forward_report())
        assert research.basis == sp.REPORT_BASIS_RESEARCH
        assert research.live_parity is False
        assert research.to_dict()["live_parity"] is False
        assert research.to_dict()["basis"] == "research_no_clock"

        live = cs.run_stress_test(_ticketable_report())
        assert live.basis == sp.REPORT_BASIS_LIVE
        assert live.live_parity is True

        # A research report given a clock string is still not re-decided
        # with the live policy.
        clocked = cs.run_stress_test(
            replace(_walk_forward_report(), decision_clock=_SIGNED_CLOCK.isoformat())
        )
        assert clocked.live_parity is False

    def test_call_a_trade_is_research_and_prints_no_ticket(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from scripts import call_a_trade

        rc = call_a_trade.main()
        out = capsys.readouterr().out
        assert rc == 1
        assert "research_no_clock" in out
        assert "not actionable" in out
        assert "TRADE TICKET" not in out


# ── Codex HOLD 6033471347: abstain on the signed-evidence floor; issuance age ─
#
# Codex review 6033471347 (at cbfd8301) accepted the explicit decision
# instant and the research separation, and held live use on the signed
# evidence floor. ``oracle/engine.py`` exports each head's aggregate
# direction and count, not the family, sign, IDs or decision-time
# eligibility of the inputs that head consumed; spreading that aggregate
# over the head's configured families (``_vote_family_shares``) can sign an
# unused or bearish family bullish; and ``model_votes`` keeps the top ten
# heads, so a dissenting head beyond them cannot veto. "Require actual
# signed per-family input receipts or abstain." Anik decided first-hand on
# 2026-11-24: "Abstain now (B)". Codex also asked for a bounded, documented
# issuance freshness, with delayed issuance tested.


def _ensemble_prediction(
    heads: list[tuple[str, float, list[tuple[str, str, float]]]],
    *,
    ticker: str = "NVDA",
    as_of: datetime = _SIGNED_CLOCK,
) -> Any:
    """The real ``EnsemblePredictor.predict`` over synthetic heads.

    Each head is ``(name, hit_rate, consumed)``; ``consumed`` lists the
    ``(family, sign, size)`` inputs the head read at ``as_of``. The head's
    direction is the sign of their net, as an aggregator nets what a head
    consumed, and its exported vote keeps that direction and nothing about
    which family moved it. Stubbed as ``tests/test_fp_ensemble_risk_fixes``
    stubs them: the registry, the signal reads, the aggregator and the
    SWEEP context. Real: ``predict`` and its top-ten ``model_votes``.
    """
    from oracle.engine import EnsemblePredictor
    from tests.test_fp_ensemble_risk_fixes import _neutral_context_patches

    consumed = {name: list(inputs) for name, _rate, inputs in heads}
    hit_rates = {name: rate for name, rate, _inputs in heads}

    def _aggregate(signals: list[Any], _config: Any, _as_of: Any) -> SimpleNamespace:
        net = sum(size if sign == "bullish" else -size for _family, sign, size in signals)
        direction = "bullish" if net > 0 else "bearish" if net < 0 else "neutral"
        return SimpleNamespace(
            direction=direction,
            strength=0.8,
            confidence=0.95,
            coherence=1.0,
            signal_count=len(signals),
        )

    predictor = EnsemblePredictor.__new__(EnsemblePredictor)
    predictor.engine = MagicMock()
    predictor.factory = MagicMock()
    predictor.factory.list_active_models.return_value = [
        SimpleNamespace(name=name, min_signals=1, weight_config={}) for name, _r, _i in heads
    ]
    predictor.factory.get_signals_for_model.side_effect = lambda name, *_a, **_k: consumed[name]
    predictor.aggregator = MagicMock()
    predictor.aggregator.aggregate.side_effect = _aggregate
    predictor._get_hit_rate = lambda name, *, horizon=None: hit_rates[name]
    predictor._get_bucket_weight = lambda name, *, horizon=None: 1.0
    with ExitStack() as stack:
        for context in _neutral_context_patches():
            stack.enter_context(context)
        stack.enter_context(
            patch(
                "intelligence.market_implied_prob.options_implied_probability",
                return_value=None,
            )
        )
        stack.enter_context(
            patch(
                "oracle.model_factory.ModelFactory.active_model_names",
                lambda conn, names, **kw: set(names),
            )
        )
        stack.enter_context(
            patch("oracle.contrast_distillation.compute_divergence", lambda rollouts: 0.0)
        )
        return predictor.predict(ticker, as_of=as_of)


def _oracle_models_engine(families: dict[str, list[str]]) -> MagicMock:
    """An engine whose ``oracle_models`` read maps each head to its
    configured families, as production's registry does."""
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = [
        (name, list(fams), None) for name, fams in families.items()
    ]
    return engine


_EXACT_CASES = (
    "unconsumed-family",
    "opposite-family",
    "truncated-dissenting-head",
    "aggregate-head-vote-alone",
)


def _exact_case(case: str) -> tuple[Any, dict[str, list[str]], str]:
    """``(prediction, oracle_models map, family)`` for one exact-ensemble
    case; ``family`` holds the strong fenced card and is the one the
    aggregate head vote signs bullish."""
    # Codex's example is a vol+macro head. ``vol`` carries a 0.30 production
    # weight override (intelligence.signal_weight_overrides), which would
    # hold conviction under 0.70 on its own, so the card family is credit.
    credit_macro = {"credit_macro": ["credit", "macro"]}
    if case == "unconsumed-family":
        # A credit+macro head bullish from macro alone: it read no credit input.
        heads = [("credit_macro", 0.6, [("macro", "bullish", 2.0), ("macro", "bullish", 1.0)])]
        return _ensemble_prediction(heads), credit_macro, "credit"
    if case == "opposite-family":
        # The same head read a bearish credit input, outweighed by macro.
        heads = [("credit_macro", 0.6, [("credit", "bearish", 1.0), ("macro", "bullish", 3.0)])]
        return _ensemble_prediction(heads), credit_macro, "credit"
    if case == "truncated-dissenting-head":
        # Ten flows heads bullish; an eleventh, the lightest, read a bearish
        # flows input. model_votes keeps the top ten, so it is not there.
        heads = [(f"flows_{i:02d}", 0.6, [("flows", "bullish", 1.0)]) for i in range(10)]
        heads.append(("flows_dissent", 0.05, [("flows", "bearish", 1.0)]))
        return _ensemble_prediction(heads), {name: ["flows"] for name, _r, _i in heads}, "flows"
    if case == "aggregate-head-vote-alone":
        # One head on one family that it read, bullish: the aggregate is
        # right here, and is still only an aggregate.
        heads = [("credit_only", 0.6, [("credit", "bullish", 1.0)])]
        return _ensemble_prediction(heads), {"credit_only": ["credit"]}, "credit"
    raise AssertionError(case)


def _exact_gateway(
    case: str, receipts: dict[str, Any] | None = None
) -> tuple[Any, str, dg.DecisionResponse]:
    """Run ``should_i_trade`` on an exact-ensemble case's prediction."""
    pred, families, family = _exact_case(case)
    # predict() leaves the regime blank here, so the card is the flat one.
    resp = _run_gateway(
        pred,
        cards={},
        flat_cards={family: _fenced_card(family, 1.3)},
        receipts=receipts,
        engine=_oracle_models_engine(families),
    )
    return pred, family, resp


class TestCodexHold6033471347SignedFamilyReceipts:
    """Abstain until signed per-family receipts exist; bounded issuance age."""

    @pytest.mark.parametrize("case", _EXACT_CASES)
    def test_exact_ensemble_gives_no_ticket_on_the_aggregate_head_vote(self, case: str) -> None:
        from oracle.engine import EnsemblePrediction

        pred, family, resp = _exact_gateway(case)
        assert isinstance(pred, EnsemblePrediction)
        assert (pred.direction, pred.as_of) == ("bullish", _SIGNED_CLOCK)
        assert pred.confidence > 0.85
        prov = resp.provenance_report
        assert prov is not None, resp.stage_errors
        # Round 6 counted the aggregate spread here: MEDIUM and a ticket.
        assert _outcome(resp) == ("low", "low", False)
        row = _evidence_rows(prov)[family]
        assert row["classification"] == "strong"
        assert row["signed_input"]["direction"] == "bullish"
        assert row["signed_input"]["basis"] == "aggregate_head_vote_not_a_receipt"
        assert row["receipt"] is None
        assert _eligible_count(prov) == 0
        assert prov.verdict_reason.startswith("no_eligible_directional_evidence")
        assert "no_signed_family_receipt" in prov.verdict_reason
        # The backstops refuse the same report forced to MEDIUM, and say why.
        forced = replace(prov, verdict="medium")
        reason = sp.actionable_refusal_reason(forced, now=_LIVE_NOW)
        assert reason.startswith("no_eligible_directional_evidence")
        assert "no_signed_family_receipt" in reason
        assert _ticket(forced) is None
        ticket, err = _gateway_ticket_stage(forced)
        assert ticket is None and err.startswith("no_eligible_directional_evidence")
        if case == "truncated-dissenting-head":
            assert pred.model_count == 11
            assert len(pred.model_votes) == 10
            assert "flows_dissent" not in {v["model_name"] for v in pred.model_votes}

    def test_receipt_contract_fields(self) -> None:
        assert [f.name for f in fields(sp.SignedFamilyReceipt)] == [
            "signal_source",
            "direction",
            "ticker",
            "decision_clock",
            "consumed_signals",
            "head_directions",
            "complete_contributing_set",
        ]
        receipt = _receipt(
            "jodi_oil", heads=(("head_a", "bullish"), ("head_b", "bullish"))
        )
        assert receipt.to_dict() == {
            "signal_source": "jodi_oil",
            "direction": "bullish",
            "ticker": "NVDA",
            "decision_clock": _ZERO_EVIDENCE_CLOCK.isoformat(),
            "consumed_signals": ["jodi_oil:obs-1"],
            "head_directions": [["head_a", "bullish"], ["head_b", "bullish"]],
            "complete_contributing_set": True,
        }
        assert (
            sp.NO_SIGNED_FAMILY_RECEIPT,
            sp.AGGREGATE_HEAD_VOTE_BASIS,
            sp.DECISION_EXPIRED,
        ) == (
            "no_signed_family_receipt",
            "aggregate_head_vote_not_a_receipt",
            "decision_expired",
        )

    @pytest.mark.parametrize(
        ("variant", "eligible"),
        [
            pytest.param("as-built", True, id="complete-and-agreeing"),
            pytest.param("incomplete", False, id="incomplete-contributing-set"),
            pytest.param("complete-not-a-bool", False, id="complete-is-not-True"),
            pytest.param("head-dissents", False, id="one-head-dissents"),
            pytest.param("head-neutral", False, id="one-head-neutral"),
            pytest.param("no-heads", False, id="no-heads"),
            pytest.param("blank-head", False, id="blank-head-name"),
            pytest.param("no-consumed", False, id="no-consumed-signals"),
            pytest.param("blank-consumed", False, id="blank-consumed-signal"),
            pytest.param("opposite-sign", False, id="opposite-sign"),
            pytest.param("another-family", False, id="another-family"),
            pytest.param("another-ticker", False, id="another-ticker"),
            pytest.param("another-clock", False, id="another-decision-clock"),
            pytest.param("naive-clock", False, id="naive-clock"),
        ],
    )
    def test_only_a_receipt_meeting_the_contract_is_eligible_contract(
        self, variant: str, eligible: bool
    ) -> None:
        # Contract, not live: test-constructed receipts on a fenced strong card.
        receipt = _receipt("jodi_oil", clock=_SIGNED_CLOCK)
        changes: dict[str, dict[str, Any]] = {
            "as-built": {},
            "incomplete": {"complete_contributing_set": False},
            "complete-not-a-bool": {"complete_contributing_set": 1},
            "head-dissents": {"head_directions": (("head_a", "bullish"), ("head_b", "bearish"))},
            "head-neutral": {"head_directions": (("head_a", "bullish"), ("head_b", "neutral"))},
            "no-heads": {"head_directions": ()},
            "blank-head": {"head_directions": ((" ", "bullish"),)},
            "no-consumed": {"consumed_signals": ()},
            "blank-consumed": {"consumed_signals": ("  ",)},
            "opposite-sign": {"direction": "bearish"},
            "another-family": {"signal_source": "pcr"},
            "another-ticker": {"ticker": "AMD"},
            "another-clock": {"decision_clock": _SIGNED_CLOCK + timedelta(seconds=1)},
            "naive-clock": {"decision_clock": _SIGNED_CLOCK.replace(tzinfo=None)},
        }
        row = sp.SignalEvidence(
            "jodi_oil",
            1.0,
            _fenced_card("jodi_oil", 1.3),
            "strong",
            receipt=replace(receipt, **changes[variant]),
        )
        assert sp.is_eligible_directional_evidence(
            row, direction="bullish", decision_clock=_SIGNED_CLOCK, ticker="NVDA"
        ) is eligible

    def test_a_complete_agreeing_receipt_passes_the_floor_contract(self) -> None:
        # Contract, not live: once an engine change exports receipts, a
        # complete, agreeing receipt behind a fenced strong card at
        # confidence above 0.85 tickets; an incomplete one does not.
        pred, family, _ = _exact_case("aggregate-head-vote-alone")
        receipt = _receipt(
            family,
            clock=_SIGNED_CLOCK,
            heads=(("credit_only", "bullish"),),
            consumed=("credit:obs-1",),
        )
        assert pred.confidence > 0.85
        _, _, resp = _exact_gateway("aggregate-head-vote-alone", {family: receipt})
        assert _outcome(resp) == ("medium", "medium", True)
        assert resp.trade_ticket.direction == "bullish"
        assert resp.stage_errors == {}
        assert _eligible_count(resp.provenance_report) == 1
        assert sp.actionable_refusal_reason(resp.provenance_report, now=_LIVE_NOW) is None

        incomplete = replace(receipt, complete_contributing_set=False)
        _, _, refused = _exact_gateway("aggregate-head-vote-alone", {family: incomplete})
        assert _outcome(refused) == ("low", "low", False)
        assert _eligible_count(refused.provenance_report) == 0

    def test_a_dissenting_head_beyond_the_top_ten_vetoes_contract(self) -> None:
        # Disagreement is judged over the complete contributing set: the
        # eleventh head that model_votes dropped still vetoes.
        pred, _, _ = _exact_case("truncated-dissenting-head")
        top_ten = tuple((v["model_name"], "bullish") for v in pred.model_votes)
        cases = {
            "complete-set-with-the-dissent": _receipt(
                "flows", clock=_SIGNED_CLOCK, heads=top_ten + (("flows_dissent", "bearish"),)
            ),
            "top-ten-only-not-complete": _receipt(
                "flows", clock=_SIGNED_CLOCK, heads=top_ten, complete=False
            ),
        }
        for name, receipt in cases.items():
            _, _, resp = _exact_gateway("truncated-dissenting-head", {"flows": receipt})
            assert _outcome(resp) == ("low", "low", False), name
            assert _eligible_count(resp.provenance_report) == 0, name
        # Control: the complete set, every head agreeing, would ticket.
        agreeing = _receipt(
            "flows", clock=_SIGNED_CLOCK, heads=top_ten + (("flows_dissent", "bullish"),)
        )
        _, _, resp = _exact_gateway("truncated-dissenting-head", {"flows": agreeing})
        assert _outcome(resp) == ("medium", "medium", True)

    def test_production_builds_no_receipt(self) -> None:
        pred, _, _ = _exact_case("aggregate-head-vote-alone")
        assert sp._family_receipts(pred, ticker="NVDA", clock=_SIGNED_CLOCK) == {}
        # Receipt-shaped data riding on a prediction is not read either.
        carrier = _signed_prediction(
            call="bullish", votes=[_vote("head_a", "bullish", ["jodi_oil"])]
        )
        carrier.family_receipts = _receipts("jodi_oil")
        carrier.receipts = _receipts("jodi_oil")
        assert sp._family_receipts(carrier, ticker="NVDA", clock=_SIGNED_CLOCK) == {}
        report = _build_live(carrier, cards=_strong_jodi())
        assert all(ev.receipt is None for ev in report.signal_evidence)
        assert (report.verdict, _ticket(replace(report, verdict="medium"))) == ("low", None)

    def test_an_expired_report_is_refused_as_expired(self) -> None:
        # Live (no receipt): before the bound, the floor refuses it; one
        # microsecond past an hour after its decision, the age does first.
        report = _build_live(
            _signed_prediction(call="bullish", votes=[_vote("head_a", "bullish", ["jodi_oil"])]),
            cards=_strong_jodi(),
        )
        inside = sp.actionable_refusal_reason(report, now=_SIGNED_CLOCK + timedelta(hours=1))
        past = sp.actionable_refusal_reason(
            report, now=_SIGNED_CLOCK + timedelta(hours=1, microseconds=1)
        )
        assert inside.startswith("no_eligible_directional_evidence")
        assert past.startswith("decision_expired")

    def test_the_issuance_bound_is_named_and_documented(self) -> None:
        assert sp.ISSUANCE_MAX_AGE == sp.LIVE_CLOCK_MAX_AGE == timedelta(hours=1)
        assert "ISSUANCE_MAX_AGE" in (sp.actionable_refusal_reason.__doc__ or "")

    @pytest.mark.parametrize(
        ("delay", "issued"),
        [
            pytest.param(timedelta(0), True, id="at-the-decision"),
            pytest.param(timedelta(minutes=59, seconds=59), True, id="1s-inside"),
            pytest.param(timedelta(hours=1), True, id="at-the-bound"),
            pytest.param(timedelta(hours=1, microseconds=1), False, id="1us-past"),
            pytest.param(timedelta(hours=1, seconds=1), False, id="1s-past"),
            pytest.param(timedelta(days=1), False, id="1d-past"),
        ],
    )
    def test_delayed_issuance(self, delay: timedelta, issued: bool) -> None:
        # Contract: eligible evidence exists (a test-constructed receipt), so
        # only the issuance age decides.
        report = _ticketable_report()
        now = _SIGNED_CLOCK + delay
        reason = sp.actionable_refusal_reason(report, now=now)
        ticket = _ticket_at(report, now)
        stage_ticket, err = _stage_at(report, now)
        if issued:
            assert reason is None
            assert ticket is not None
            assert stage_ticket is not None and err is None
        else:
            assert reason is not None and reason.startswith("decision_expired")
            assert ticket is None
            assert stage_ticket is None
            assert err is not None and err.startswith("decision_expired")

    def test_the_bound_runs_from_the_decision_not_the_build(self) -> None:
        # Contract: built 59 minutes after its decision (inside the builder's
        # live window), the report has one minute left to issue.
        clock = _LIVE_NOW - timedelta(minutes=59)
        pred = _signed_prediction(call="bullish", votes=[_vote("head_a", "bullish", ["jodi_oil"])])
        pred.as_of = clock
        report = _build_live(
            pred,
            cards={"jodi_oil": _card_at("jodi_oil", 1.3, clock - timedelta(days=1))},
            receipts=_receipts("jodi_oil", clock=clock),
        )
        assert report.verdict == "medium"
        assert _ticket_at(report, _LIVE_NOW + timedelta(minutes=1)) is not None
        assert _ticket_at(report, _LIVE_NOW + timedelta(minutes=1, seconds=1)) is None
        late = sp.actionable_refusal_reason(report, now=_LIVE_NOW + timedelta(minutes=2))
        assert late is not None and late.startswith("decision_expired")
