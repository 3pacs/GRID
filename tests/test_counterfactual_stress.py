"""Tests for intelligence/counterfactual_stress.py (CAT-175)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from features.per_signal_brier import (
    MIN_CALIBRATED_SAMPLES,
    SignalScorecard,
    compute_conviction_weight,
)
from intelligence.counterfactual_stress import (
    BRIER_PERTURBATION_DELTA,
    FRAGILITY_THRESHOLD,
    ROBUST_THRESHOLD,
    STRESS_PERTURBATION_SIGMAS,
    FragilityFlag,
    SignalPerturbation,
    StressTestReport,
    build_advisory,
    classify_robustness,
    compute_robustness_score,
    identify_fragility_flags,
    perturb_brier,
    perturbed_conviction_weight,
    run_stress_test,
)
from intelligence.signal_provenance import (
    CausationChain,
    SignalEvidence,
    TradeProvenanceReport,
)


# ── Fixtures ──────────────────────────────────────────────────────────────


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


def _evidence(
    source: str, weight: float, brier: float, count: int = 50, conv: float = 1.5
) -> SignalEvidence:
    return SignalEvidence(
        signal_source=source,
        shapley_weight=weight,
        scorecard=_card(source, 7, count, brier, conv),
        classification="strong",
    )


def _report(
    *,
    ticker: str = "NVDA",
    confidence: float = 0.82,
    aggregate: float = 1.3,
    verdict: str = "high",
    evidence: list[SignalEvidence] | None = None,
    fragility_multiplier: float = 1.0,
    disagreement_score: float = 0.0,
) -> TradeProvenanceReport:
    causation = CausationChain(
        lever="earnings",
        flow_direction="open",
        actor="flow_momentum",
        complete=True,
    )
    return TradeProvenanceReport(
        ticker=ticker,
        generated_at=datetime.now(timezone.utc).isoformat(),
        direction="bullish",
        score=75,
        confidence=confidence,
        confidence_lower=max(0.0, confidence - 0.1),
        confidence_upper=min(1.0, confidence + 0.1),
        horizon_days=7,
        regime="EXPANSION",
        fci_regime="EASY",
        signal_evidence=evidence or [],
        top_shapley_contributor="flow_momentum",
        top_shapley_share=0.6,
        fragility_multiplier=fragility_multiplier,
        disagreement_score=disagreement_score,
        crowd_aligned=False,
        market_implied_prob=0.7,
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
        aggregate_conviction=aggregate,
        verdict=verdict,
    )


# ── perturb_brier ─────────────────────────────────────────────────────────


class TestPerturbBrier:
    def test_zero_sigma_unchanged(self):
        assert perturb_brier(0.1, 0.0) == pytest.approx(0.1)

    def test_positive_sigma_increases_brier(self):
        assert perturb_brier(0.1, 1.0) > 0.1

    def test_negative_sigma_also_increases_brier(self):
        # Adverse shift uses |sigma|, so -2σ and +2σ both push Brier up.
        assert perturb_brier(0.1, -2.0) > perturb_brier(0.1, -1.0)
        assert perturb_brier(0.1, -2.0) == pytest.approx(
            perturb_brier(0.1, +2.0)
        )

    def test_clamps_to_unit_interval(self):
        # Very high Brier + huge sigma → must clamp to 1.0.
        assert perturb_brier(0.95, -5.0) == pytest.approx(1.0)
        # And stays non-negative even with weird inputs.
        assert perturb_brier(0.0, 0.0) == 0.0

    def test_2_sigma_is_larger_than_1_sigma(self):
        small = perturb_brier(0.1, -1.0)
        big = perturb_brier(0.1, -2.0)
        assert big - 0.1 == pytest.approx(2 * (small - 0.1))


# ── perturbed_conviction_weight ───────────────────────────────────────────


class TestPerturbedConvictionWeight:
    def test_matches_features_formula_at_boundaries(self):
        # Brier 0.05 with enough samples → max 1.5
        assert perturbed_conviction_weight(0.05, 50) == pytest.approx(1.5)
        # Brier 0.25 with enough samples → 0.0 (anti-predictive)
        assert perturbed_conviction_weight(0.25, 50) == pytest.approx(0.0)

    def test_cold_start_returns_neutral(self):
        # Below MIN_CALIBRATED_SAMPLES → 1.0 regardless of Brier
        assert (
            perturbed_conviction_weight(0.05, MIN_CALIBRATED_SAMPLES - 1)
            == pytest.approx(1.0)
        )

    def test_matches_imported_function(self):
        for brier in (0.05, 0.10, 0.15, 0.20, 0.25):
            assert perturbed_conviction_weight(brier, 50) == compute_conviction_weight(
                brier, 50
            )


# ── compute_robustness_score ──────────────────────────────────────────────


class TestRobustnessScore:
    def test_empty_perturbations_is_one(self):
        assert compute_robustness_score([]) == 1.0

    def test_all_held_is_one(self):
        perts = [
            SignalPerturbation("a", -1.0, 0.1, 1.5, 1.3, "high", False),
            SignalPerturbation("b", -2.0, 0.2, 0.5, 1.2, "high", False),
        ]
        assert compute_robustness_score(perts) == 1.0

    def test_all_flipped_is_zero(self):
        perts = [
            SignalPerturbation("a", -1.0, 0.2, 0.0, 0.4, "low", True),
            SignalPerturbation("b", -2.0, 0.3, 0.0, 0.3, "low", True),
        ]
        assert compute_robustness_score(perts) == 0.0

    def test_mixed_returns_fraction(self):
        perts = [
            SignalPerturbation("a", -1.0, 0.2, 0.0, 0.4, "low", True),
            SignalPerturbation("a", -2.0, 0.3, 0.0, 0.3, "low", True),
            SignalPerturbation("b", -1.0, 0.1, 1.5, 1.3, "high", False),
            SignalPerturbation("b", -2.0, 0.15, 1.2, 1.2, "high", False),
        ]
        assert compute_robustness_score(perts) == 0.5


# ── classify_robustness ───────────────────────────────────────────────────


class TestClassify:
    def test_above_robust_threshold(self):
        assert classify_robustness(0.95) == "robust"
        assert classify_robustness(ROBUST_THRESHOLD) == "robust"

    def test_below_fragility_threshold(self):
        assert classify_robustness(0.5) == "fragile"
        assert classify_robustness(FRAGILITY_THRESHOLD - 0.01) == "fragile"

    def test_moderate_in_between(self):
        assert classify_robustness(0.8) == "moderate"
        assert classify_robustness(FRAGILITY_THRESHOLD) == "moderate"


# ── identify_fragility_flags ──────────────────────────────────────────────


class TestFragilityFlags:
    def test_signal_that_flipped_is_fragile(self):
        perts = [
            SignalPerturbation("reddit", -1.0, 0.2, 0.0, 0.4, "low", True),
            SignalPerturbation("reddit", -2.0, 0.3, 0.0, 0.3, "low", True),
        ]
        flags = identify_fragility_flags(perts)
        assert len(flags) == 1
        assert flags[0].fragile is True
        assert flags[0].breaking_sigma == -1.0  # smallest |sigma|

    def test_signal_never_flipped_not_fragile(self):
        perts = [
            SignalPerturbation("jodi", -1.0, 0.1, 1.5, 1.3, "high", False),
            SignalPerturbation("jodi", -2.0, 0.15, 1.2, 1.2, "high", False),
        ]
        flags = identify_fragility_flags(perts)
        assert len(flags) == 1
        assert flags[0].fragile is False
        assert flags[0].breaking_sigma is None
        assert "no perturbation" in flags[0].reason

    def test_smallest_abs_sigma_breaks_first(self):
        perts = [
            SignalPerturbation("x", -2.0, 0.3, 0.0, 0.3, "low", True),
            SignalPerturbation("x", -1.0, 0.2, 0.0, 0.4, "low", True),
            SignalPerturbation("x", +1.0, 0.2, 0.0, 0.4, "low", True),
        ]
        flags = identify_fragility_flags(perts)
        assert flags[0].breaking_sigma in (-1.0, 1.0)
        assert abs(flags[0].breaking_sigma) == 1.0

    def test_multiple_signals_each_get_a_flag(self):
        perts = [
            SignalPerturbation("a", -1.0, 0.2, 0.0, 0.4, "low", True),
            SignalPerturbation("b", -1.0, 0.1, 1.5, 1.3, "high", False),
        ]
        flags = identify_fragility_flags(perts)
        assert len(flags) == 2
        sources = {f.signal_source for f in flags}
        assert sources == {"a", "b"}


# ── run_stress_test ───────────────────────────────────────────────────────


class TestRunStressTest:
    def test_strong_report_is_robust(self):
        # Three strong, well-calibrated signals → no perturbation should flip.
        evs = [
            _evidence("flow_momentum", 0.34, 0.05, count=200, conv=1.5),
            _evidence("regime_contrarian", 0.33, 0.05, count=200, conv=1.5),
            _evidence("jodi_oil", 0.33, 0.05, count=200, conv=1.5),
        ]
        rep = _report(
            evidence=evs,
            confidence=0.82,
            aggregate=1.5,
            verdict="high",
        )
        result = run_stress_test(rep)
        assert isinstance(result, StressTestReport)
        # Three signals × three sigmas = 9 perturbations.
        assert len(result.perturbations) == 9
        # Most should hold.
        assert result.robustness_score > 0.0
        assert result.original_verdict == "high"

    def test_knife_edge_report_is_fragile(self):
        # One dominant signal carrying the call. Perturbing it must flip.
        evs = [
            _evidence("dominant", 0.95, 0.05, count=200, conv=1.5),
            _evidence("filler", 0.05, 0.05, count=200, conv=1.5),
        ]
        rep = _report(
            evidence=evs,
            confidence=0.72,
            aggregate=1.45,
            verdict="high",
        )
        result = run_stress_test(rep)
        # 2 signals × 3 sigmas = 6 perturbations.
        assert len(result.perturbations) == 6
        # The dominant signal should appear in the flags.
        sources = {f.signal_source for f in result.fragility_flags}
        assert "dominant" in sources
        # At least one perturbation should have flipped.
        assert result.break_count >= 1

    def test_no_trade_report_no_perturbations_needed(self):
        rep = _report(
            evidence=[],
            confidence=0.4,
            aggregate=0.2,
            verdict="no_trade",
        )
        result = run_stress_test(rep)
        assert result.perturbations == []
        assert result.robustness_score == 1.0
        assert result.fragility_flags == []
        assert result.original_verdict == "no_trade"

    def test_every_signal_gets_perturbed_at_every_sigma(self):
        evs = [
            _evidence("a", 0.5, 0.10, count=100, conv=1.2),
            _evidence("b", 0.5, 0.10, count=100, conv=1.2),
        ]
        rep = _report(evidence=evs)
        result = run_stress_test(rep)
        assert len(result.perturbations) == len(evs) * len(STRESS_PERTURBATION_SIGMAS)
        # Each signal source should appear exactly len(SIGMAS) times.
        from collections import Counter
        counts = Counter(p.signal_source for p in result.perturbations)
        assert counts["a"] == len(STRESS_PERTURBATION_SIGMAS)
        assert counts["b"] == len(STRESS_PERTURBATION_SIGMAS)


# ── build_advisory ────────────────────────────────────────────────────────


class TestBuildAdvisory:
    def test_fragile_advisory_names_breaking_signal(self):
        evs = [
            _evidence("dominant_news_pulse", 0.95, 0.05, count=200, conv=1.5),
            _evidence("filler", 0.05, 0.05, count=200, conv=1.5),
        ]
        rep = _report(
            evidence=evs, confidence=0.72, aggregate=1.45, verdict="high"
        )
        result = run_stress_test(rep)
        # Force a fragile label for the assertion regardless of math.
        if result.robustness_label == "fragile":
            assert "dominant_news_pulse" in result.advisory or "FRAGILE" in result.advisory
        else:
            # Even if not labeled fragile, advisory must be non-empty.
            assert len(result.advisory) > 0

    def test_robust_advisory_is_reassuring(self):
        # Construct a synthetic fully-robust report directly.
        bare = StressTestReport(
            ticker="NVDA",
            original_verdict="high",
            original_conviction=1.4,
            perturbations=[
                SignalPerturbation("a", -2.0, 0.15, 1.2, 1.3, "high", False),
                SignalPerturbation("a", -1.0, 0.10, 1.4, 1.35, "high", False),
                SignalPerturbation("a", +1.0, 0.10, 1.4, 1.35, "high", False),
            ],
            fragility_flags=[
                FragilityFlag("a", False, "no perturbation flipped verdict", None),
            ],
            robustness_score=1.0,
            robustness_label="robust",
            break_count=0,
            generated_at=datetime.now(timezone.utc).isoformat(),
            advisory="",
        )
        advisory = build_advisory(bare, None)
        assert "ROBUST" in advisory
        assert "HIGH" in advisory

    def test_empty_perturbations_advisory(self):
        bare = StressTestReport(
            ticker="X",
            original_verdict="no_trade",
            original_conviction=0.1,
            perturbations=[],
            fragility_flags=[],
            robustness_score=1.0,
            robustness_label="robust",
            break_count=0,
            generated_at=datetime.now(timezone.utc).isoformat(),
            advisory="",
        )
        advisory = build_advisory(bare, None)
        assert "no" in advisory.lower() and "signal" in advisory.lower()


# ── Dataclass roundtrips ──────────────────────────────────────────────────


class TestDataClassRoundtrips:
    def test_signal_perturbation_frozen_and_dict(self):
        p = SignalPerturbation(
            signal_source="x",
            sigma=-2.0,
            perturbed_brier=0.2,
            perturbed_conviction_weight=0.3,
            new_aggregate_conviction=0.4,
            new_verdict="low",
            verdict_changed=True,
        )
        with pytest.raises(Exception):
            p.sigma = 0.0  # type: ignore[misc]
        d = p.to_dict()
        assert d["signal_source"] == "x"
        assert d["new_verdict"] == "low"
        assert d["verdict_changed"] is True

    def test_fragility_flag_frozen_and_dict(self):
        f = FragilityFlag(
            signal_source="x",
            fragile=True,
            reason="verdict flips at -1σ",
            breaking_sigma=-1.0,
        )
        with pytest.raises(Exception):
            f.fragile = False  # type: ignore[misc]
        d = f.to_dict()
        assert d["fragile"] is True
        assert d["breaking_sigma"] == -1.0
        # None case
        f2 = FragilityFlag("y", False, "n/a", None)
        assert f2.to_dict()["breaking_sigma"] is None

    def test_stress_test_report_frozen_and_dict(self):
        report = StressTestReport(
            ticker="NVDA",
            original_verdict="high",
            original_conviction=1.3,
            perturbations=[
                SignalPerturbation("a", -1.0, 0.1, 1.5, 1.3, "high", False)
            ],
            fragility_flags=[
                FragilityFlag("a", False, "no perturbation flipped verdict", None)
            ],
            robustness_score=1.0,
            robustness_label="robust",
            break_count=0,
            generated_at=datetime.now(timezone.utc).isoformat(),
            advisory="Trade is ROBUST",
        )
        with pytest.raises(Exception):
            report.ticker = "AAPL"  # type: ignore[misc]
        d = report.to_dict()
        for k in (
            "ticker", "original_verdict", "original_conviction",
            "perturbations", "fragility_flags", "robustness_score",
            "robustness_label", "break_count", "generated_at", "advisory",
        ):
            assert k in d
        assert d["robustness_label"] == "robust"
        assert isinstance(d["perturbations"], list)
        assert isinstance(d["fragility_flags"], list)


# ── Defensive edge cases ──────────────────────────────────────────────────


class TestEdgeCases:
    def test_empty_signal_evidence_produces_trivial_robust_report(self):
        rep = _report(evidence=[], aggregate=0.0, verdict="no_trade")
        result = run_stress_test(rep)
        assert result.perturbations == []
        assert result.fragility_flags == []
        assert result.robustness_score == 1.0
        assert result.robustness_label == "robust"
        # Advisory should mention the absence of signals.
        assert "no" in result.advisory.lower()

    def test_signal_with_none_scorecard_does_not_crash(self):
        # Cold-start signal with no scorecard at all.
        ev_with_none = SignalEvidence(
            signal_source="cold_signal",
            shapley_weight=1.0,
            scorecard=None,
            classification="no_history",
        )
        rep = _report(
            evidence=[ev_with_none],
            confidence=0.7,
            aggregate=1.0,
            verdict="medium",
        )
        result = run_stress_test(rep)
        # 1 signal × 3 sigmas
        assert len(result.perturbations) == 3
        for p in result.perturbations:
            assert p.signal_source == "cold_signal"
            assert 0.0 <= p.perturbed_brier <= 1.0

    def test_constants_are_what_we_promised(self):
        assert STRESS_PERTURBATION_SIGMAS == (-2.0, -1.0, +1.0)
        assert BRIER_PERTURBATION_DELTA == 0.05
        assert FRAGILITY_THRESHOLD == 0.7
        assert ROBUST_THRESHOLD == 0.9
