"""Coverage semantics added to the conviction stack on 2026-09-10.

The GRID-4 pivot's root cause for "11.9% hit rate at HIGH confidence" was
that every adjuster layer defaulted to a neutral 1.0 when its upstream was
missing, so a report with nothing computed looked identical to a report
where everything was checked and neutral. These tests pin that:

* ``aggregate_conviction_with_coverage`` reports which layers were present,
  and a ``None`` layer changes coverage but not the value;
* evidence coverage is the Shapley-weighted share of calibrated scorecards;
* the verdict demotes HIGH to LOW when either coverage is below its floor,
  and callers that pass no coverage keep the pre-gate behaviour;
* ``build_provenance_report`` surfaces all of it, and a report whose layers
  all fail no longer comes out HIGH.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from features.per_signal_brier import SignalScorecard
from intelligence import signal_provenance as sp
from intelligence.signal_provenance import (
    CONVICTION_LAYERS,
    MIN_EVIDENCE_COVERAGE_FOR_HIGH,
    MIN_LAYER_COVERAGE_FOR_HIGH,
    SignalEvidence,
    _verdict_from_aggregate,
    aggregate_conviction_with_coverage,
    build_provenance_report,
    compute_aggregate_conviction,
    compute_evidence_coverage,
)


def _card(source: str, weight: float, calibrated: bool = True) -> SignalScorecard:
    return SignalScorecard(
        signal_source=source,
        horizon_days=7,
        scored_count=50,
        running_brier=0.1,
        running_ece=0.1,
        hit_rate=0.6,
        last_updated=datetime.now(timezone.utc),
        is_calibrated=calibrated,
        conviction_weight=weight,
    )


def _strong(source: str = "jodi_oil", weight: float = 1.0) -> SignalEvidence:
    return SignalEvidence(source, weight, _card(source, 1.5), "strong")


class TestLayerCoverage:
    def test_all_layers_absent_when_nothing_passed(self):
        agg = aggregate_conviction_with_coverage([_strong()])
        assert set(agg.layer_coverage) == set(CONVICTION_LAYERS)
        assert agg.layers_present == 0
        assert agg.layers_total == len(CONVICTION_LAYERS)
        assert agg.layer_coverage_ratio == 0.0
        assert agg.value == 1.5  # value identical to the old neutral path

    def test_present_layers_are_counted_and_applied(self):
        agg = aggregate_conviction_with_coverage(
            [_strong()],
            fragility_multiplier=0.5,
            disagreement_score=0.0,
            money_flow_multiplier=1.0,
        )
        assert agg.layer_coverage["fragility"] is True
        assert agg.layer_coverage["disagreement"] is True
        assert agg.layer_coverage["money_flow"] is True
        assert agg.layer_coverage["scenario"] is False
        assert agg.layers_present == 3
        assert abs(agg.value - 0.75) < 1e-9  # 1.5 × 0.5

    def test_none_layer_and_neutral_layer_have_equal_value_but_different_coverage(self):
        absent = aggregate_conviction_with_coverage([_strong()], scenario_multiplier=None)
        neutral = aggregate_conviction_with_coverage([_strong()], scenario_multiplier=1.0)
        assert absent.value == neutral.value
        assert absent.layer_coverage["scenario"] is False
        assert neutral.layer_coverage["scenario"] is True

    def test_scalar_wrapper_matches_value(self):
        kwargs = dict(fragility_multiplier=0.8, contra_indicator_multiplier=1.1)
        assert compute_aggregate_conviction([_strong()], **kwargs) == (
            aggregate_conviction_with_coverage([_strong()], **kwargs).value
        )


class TestEvidenceCoverage:
    def test_no_evidence_is_zero(self):
        assert compute_evidence_coverage([]) == 0.0

    def test_weighted_share_of_calibrated_scorecards(self):
        ev = [
            SignalEvidence("a", 0.5, _card("a", 1.2), "strong"),
            SignalEvidence("b", 0.3, None, "no_history"),
            SignalEvidence("c", 0.2, _card("c", 1.0, calibrated=False), "cold_start"),
        ]
        assert abs(compute_evidence_coverage(ev) - 0.5) < 1e-9
        assert abs(aggregate_conviction_with_coverage(ev).evidence_coverage - 0.5) < 1e-9

    def test_all_no_history_is_zero(self):
        ev = [SignalEvidence("a", 1.0, None, "no_history")]
        assert compute_evidence_coverage(ev) == 0.0


class TestVerdictCoverageGate:
    def test_default_kwargs_keep_pre_gate_behaviour(self):
        assert _verdict_from_aggregate(1.0, 0.7) == "high"

    def test_thin_layer_coverage_demotes_high_to_low(self):
        assert _verdict_from_aggregate(
            1.0, 0.7, layer_coverage_ratio=MIN_LAYER_COVERAGE_FOR_HIGH - 0.01, evidence_coverage=1.0
        ) == "low"

    def test_thin_evidence_coverage_demotes_high_to_low(self):
        assert _verdict_from_aggregate(
            1.0, 0.7, layer_coverage_ratio=1.0, evidence_coverage=MIN_EVIDENCE_COVERAGE_FOR_HIGH - 0.01
        ) == "low"

    def test_floors_are_inclusive(self):
        assert _verdict_from_aggregate(
            1.0, 0.7,
            layer_coverage_ratio=MIN_LAYER_COVERAGE_FOR_HIGH,
            evidence_coverage=MIN_EVIDENCE_COVERAGE_FOR_HIGH,
        ) == "high"

    def test_coverage_does_not_rescue_other_verdicts(self):
        assert _verdict_from_aggregate(0.2, 0.7, layer_coverage_ratio=0.0, evidence_coverage=0.0) == "no_trade"
        assert _verdict_from_aggregate(1.0, 0.95, layer_coverage_ratio=0.0, evidence_coverage=0.0) == "medium"


class TestBuildProvenanceReportCoverage:
    def _prediction(self):
        return SimpleNamespace(
            ticker="NVDA",
            direction="bullish",
            confidence=0.7,
            confidence_lower=0.6,
            confidence_upper=0.8,
            horizon=7,
            regime="NEUTRAL",
            fci_regime="EASY",
            model_votes=[{"model_name": "flow_momentum", "weight": 1.0}],
            shapley_top_contributor="flow_momentum",
            shapley_top_share=1.0,
            fragility_multiplier=1.0,
            disagreement_score=0.0,
            signals={},
        )

    def _patch_all_layers(self, stack, *, raise_layers: bool, scorecard):
        """Make every DB-backed layer either succeed neutrally or raise."""
        def _fail(*_a, **_k):
            raise RuntimeError("layer unavailable")

        neutral = (lambda *_a, **_k: 1.0)
        fn = _fail if raise_layers else neutral
        for name in (
            "get_lift_multiplier",
            "conviction_multiplier_for_bucket",
            "scenario_conviction_multiplier",
            "null_hypothesis_penalty",
            "get_aggregate_weight_multiplier",
            "contra_conviction_multiplier",
            "squeeze_conviction_multiplier",
            "arbitrage_conviction_multiplier",
            "convergence_conviction_multiplier",
            "money_flow_conviction_multiplier",
            "memory_lesson_conviction_multiplier",
        ):
            stack.enter_context(patch.object(sp, name, fn))
        stack.enter_context(patch.object(sp, "get_scorecard_with_regime_fallback", lambda *_a, **_k: scorecard))
        stack.enter_context(patch.object(sp, "_recent_fudge_alerts", lambda *_a, **_k: []))
        stack.enter_context(patch.object(sp, "build_condition_tuple", lambda **_k: ("t",)))

    def test_all_layers_failing_is_reported_and_is_not_high(self):
        from contextlib import ExitStack

        with ExitStack() as stack:
            self._patch_all_layers(stack, raise_layers=True, scorecard=_card("flow_momentum", 1.3))
            report = build_provenance_report(MagicMock(), prediction=self._prediction())

        # Only the prediction-level layers (fragility, disagreement, red_team,
        # fudge_alerts) could compute; 11 of 15 DB-backed layers failed.
        assert report.layers_total == len(CONVICTION_LAYERS)
        assert report.layer_coverage["scenario"] is False
        assert report.layer_coverage["fragility"] is True
        assert report.layers_present < report.layers_total * MIN_LAYER_COVERAGE_FOR_HIGH
        assert report.verdict == "low"
        assert "adjuster layers computed" in report.verdict_reason
        # The API-facing multipliers stay numeric; absence is carried by coverage.
        assert report.scenario_multiplier == 1.0
        assert report.to_dict()["layer_coverage"]["scenario"] is False

    def test_layers_computed_but_no_scorecards_is_not_high(self):
        from contextlib import ExitStack

        with ExitStack() as stack:
            self._patch_all_layers(stack, raise_layers=False, scorecard=None)
            report = build_provenance_report(MagicMock(), prediction=self._prediction())

        assert report.layers_present == report.layers_total - 1  # edge_signal is never passed here
        assert report.evidence_coverage == 0.0
        assert report.aggregate_conviction == 1.0  # the "dead dimension" value the 2026-05-17 probe found
        assert report.verdict == "low"
        assert "calibrated" in report.verdict_reason

    def test_full_coverage_with_calibrated_scorecard_is_high(self):
        from contextlib import ExitStack

        with ExitStack() as stack:
            self._patch_all_layers(stack, raise_layers=False, scorecard=_card("flow_momentum", 1.3))
            report = build_provenance_report(MagicMock(), prediction=self._prediction())

        assert report.evidence_coverage == 1.0
        assert report.verdict == "high"
        assert report.verdict_reason.startswith("high")
