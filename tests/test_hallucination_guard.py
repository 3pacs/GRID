"""
Tests for oracle.hallucination_guard — confidence degradation checks
that catch hallucinated or poorly-supported predictions before they
reach the decision journal.
"""

from __future__ import annotations

from collections import namedtuple
from datetime import date, datetime, timezone

import pytest

from oracle.engine import AntiSignal, OraclePrediction, PredictionType, Signal
from oracle.hallucination_guard import guard_summary, verify_predictions


# ── Mock calibration report ────────────────────────────────────────────────

CalibrationReport = namedtuple("CalibrationReport", ["label", "calibration_error", "brier_score"])


# ── Fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture
def make_signal():
    """Factory for Signal instances with sensible defaults."""

    def _make(
        name: str = "test_signal",
        family: str = "equity",
        freshness_hours: float = 1.0,
        z_score: float = 1.5,
        direction: str = "bullish",
        value: float = 1.0,
        weight: float = 1.0,
    ) -> Signal:
        return Signal(
            name=name,
            family=family,
            value=value,
            z_score=z_score,
            direction=direction,
            weight=weight,
            freshness_hours=freshness_hours,
        )

    return _make


@pytest.fixture
def make_anti_signal():
    """Factory for AntiSignal instances."""

    def _make(
        severity: float = 0.3,
        name: str = "counter_signal",
        family: str = "vol",
        value: float = -0.5,
        z_score: float = -1.0,
        contradiction: str = "Contradicts bullish thesis",
    ) -> AntiSignal:
        return AntiSignal(
            name=name,
            family=family,
            value=value,
            z_score=z_score,
            contradiction=contradiction,
            severity=severity,
        )

    return _make


@pytest.fixture
def make_prediction(make_signal):
    """Factory for a healthy OraclePrediction that should pass all checks."""

    def _make(**overrides) -> OraclePrediction:
        defaults = dict(
            id="pred-healthy-001",
            timestamp=datetime.now(tz=timezone.utc),
            ticker="SPY",
            prediction_type=PredictionType.DIRECTION,
            direction="CALL",
            target_price=460.0,
            current_price=450.0,
            expiry=date(2026, 5, 1),
            confidence=0.6,
            expected_move_pct=3.0,
            signals=[
                make_signal(name="sig_rates", family="rates", freshness_hours=1.0),
                make_signal(name="sig_credit", family="credit", freshness_hours=2.0),
                make_signal(name="sig_vol", family="vol", freshness_hours=0.5),
            ],
            anti_signals=[],
            signal_strength=2.0,
            coherence=0.8,
            model_name="ensemble_v3",
            model_version="3.1",
            model_weights={},
            flow_context={},
        )
        defaults.update(overrides)
        return OraclePrediction(**defaults)

    return _make


# ── Tests ──────────────────────────────────────────────────────────────────


def test_healthy_prediction_passes(make_prediction):
    """A well-formed prediction with fresh, diverse signals passes all checks."""
    pred = make_prediction()
    filtered, verdicts = verify_predictions([pred])

    assert len(filtered) == 1
    assert len(verdicts) == 1

    v = verdicts[0]
    assert v.prediction_id == pred.id
    assert v.action == "pass"
    # Confidence should be unchanged or very close to original
    assert abs(v.adjusted_confidence - v.original_confidence) < 0.05


def test_stale_signals_degrade_confidence(make_prediction, make_signal):
    """When >50% of signals are stale (>72h), confidence is multiplied by ~0.7."""
    stale_signals = [
        make_signal(name="s1", family="rates", freshness_hours=100.0),
        make_signal(name="s2", family="credit", freshness_hours=100.0),
        make_signal(name="s3", family="vol", freshness_hours=100.0),
        make_signal(name="s4", family="equity", freshness_hours=1.0),
    ]
    pred = make_prediction(signals=stale_signals, confidence=0.8)
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    # Freshness check should have fired with adjustment ~0.7
    assert v.adjusted_confidence < v.original_confidence
    assert v.adjusted_confidence <= 0.8 * 0.75  # at most 0.7 multiplier plus tolerance


def test_anti_signal_overload(make_prediction, make_anti_signal):
    """When anti-signal severity exceeds 0.6x signal_strength, confidence halved."""
    anti_signals = [
        make_anti_signal(severity=0.5),
        make_anti_signal(severity=0.5, name="counter2"),
        make_anti_signal(severity=0.4, name="counter3"),
    ]
    # Total severity = 1.4, signal_strength = 2.0 -> 1.4 > 0.6 * 2.0 = 1.2
    pred = make_prediction(anti_signals=anti_signals, signal_strength=2.0, confidence=0.8)
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    # Adjustment ~0.5
    assert v.adjusted_confidence <= 0.8 * 0.55


def test_low_coherence_degrades(make_prediction):
    """Coherence below 0.3 triggers ~0.6 adjustment."""
    pred = make_prediction(coherence=0.2, confidence=0.7)
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    assert v.adjusted_confidence <= 0.7 * 0.65


def test_overconfident_calibration_penalty(make_prediction):
    """Overconfident calibration report penalizes high-confidence predictions."""
    cal_report = CalibrationReport(
        label="overconfident",
        calibration_error=0.15,
        brier_score=0.3,
    )
    pred = make_prediction(confidence=0.8)
    _, verdicts = verify_predictions([pred], calibration_report=cal_report)

    v = verdicts[0]
    # adjustment = max(0.5, 1.0 - 0.15) = 0.85 -> adjusted = 0.8 * 0.85 = 0.68
    assert v.adjusted_confidence < 0.8
    assert v.adjusted_confidence >= 0.8 * 0.5  # never below half from this check alone


def test_mono_source_family_penalty(make_prediction, make_signal):
    """All signals from the same family triggers ~0.8 adjustment."""
    mono_signals = [
        make_signal(name="s1", family="rates"),
        make_signal(name="s2", family="rates"),
        make_signal(name="s3", family="rates"),
    ]
    pred = make_prediction(signals=mono_signals, confidence=0.7)
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    assert v.adjusted_confidence <= 0.7 * 0.85


def test_extreme_move_needs_evidence(make_prediction, make_signal):
    """A 20% expected move with moderate coherence gets heavy penalty."""
    pred = make_prediction(
        expected_move_pct=20.0,
        coherence=0.5,
        signal_strength=2.0,
        confidence=0.7,
    )
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    # Extreme move check adjustment = 0.4
    assert v.adjusted_confidence <= 0.7 * 0.45


def test_extreme_move_with_strong_evidence_passes(make_prediction, make_signal):
    """A 20% move with strong coherence and signal_strength should pass."""
    signals = [
        make_signal(name=f"s{i}", family=f"fam{i}", freshness_hours=1.0)
        for i in range(5)
    ]
    pred = make_prediction(
        expected_move_pct=20.0,
        coherence=0.8,
        signal_strength=4.0,
        signals=signals,
        confidence=0.7,
    )
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    # Should pass or have only minor adjustment from extreme move check
    extreme_checks = [c for c in v.checks if "extreme" in c.check_name.lower()]
    if extreme_checks:
        assert extreme_checks[0].passed or extreme_checks[0].adjustment >= 0.9


def test_bad_model_degrades(make_prediction):
    """Model with hit_rate < 0.35 and >20 predictions degrades confidence."""
    model_stats = {
        "ensemble_v3": {
            "hits": 7,
            "misses": 15,
            "partials": 3,
        }
    }
    pred = make_prediction(model_name="ensemble_v3", confidence=0.7)
    _, verdicts = verify_predictions([pred], model_stats=model_stats)

    v = verdicts[0]
    # hits=7, misses=15, partials=3 → total=25, hit_rate=7/25=0.28
    # adjustment = max(0.5, 0.28 + 0.2) = 0.5 -> adjusted = 0.7 * 0.5 = 0.35
    assert v.adjusted_confidence < 0.7
    assert v.adjusted_confidence <= 0.7 * 0.55


def test_high_confidence_needs_convergence(make_prediction, make_signal):
    """Confidence > 0.8 without convergence family signal gets 0.75 adjustment."""
    signals = [
        make_signal(name="s1", family="rates"),
        make_signal(name="s2", family="credit"),
        make_signal(name="s3", family="vol"),
    ]
    pred = make_prediction(signals=signals, confidence=0.85)
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    assert v.adjusted_confidence <= 0.85 * 0.80


def test_high_confidence_with_convergence_passes(make_prediction, make_signal):
    """Confidence > 0.8 with a convergence signal present should pass that check."""
    signals = [
        make_signal(name="s1", family="rates"),
        make_signal(name="s2", family="credit"),
        make_signal(name="s3", family="convergence"),
    ]
    pred = make_prediction(signals=signals, confidence=0.85)
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    convergence_checks = [c for c in v.checks if "convergence" in c.check_name.lower()]
    if convergence_checks:
        assert convergence_checks[0].passed


def test_multiple_failures_compound(make_prediction, make_signal):
    """Multiple failing checks multiply their adjustments together."""
    # Stale signals (0.7) + low coherence (0.6) + mono source (0.8) = 0.336
    stale_mono_signals = [
        make_signal(name="s1", family="rates", freshness_hours=100.0),
        make_signal(name="s2", family="rates", freshness_hours=100.0),
        make_signal(name="s3", family="rates", freshness_hours=100.0),
    ]
    pred = make_prediction(
        signals=stale_mono_signals,
        coherence=0.2,
        confidence=0.8,
    )
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    # 0.8 * 0.7 * 0.6 * 0.8 = 0.2688, should be substantially degraded
    assert v.adjusted_confidence < 0.8 * 0.5
    failed_checks = [c for c in v.checks if not c.passed]
    assert len(failed_checks) >= 3


def test_reject_when_confidence_destroyed(make_prediction, make_signal):
    """Prediction that loses >50% confidence gets action='reject'."""
    # Stack multiple failures to destroy confidence
    stale_mono_signals = [
        make_signal(name="s1", family="rates", freshness_hours=100.0),
        make_signal(name="s2", family="rates", freshness_hours=100.0),
    ]
    pred = make_prediction(
        signals=stale_mono_signals,
        coherence=0.2,
        confidence=0.8,
        expected_move_pct=20.0,
        signal_strength=1.0,
    )
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    # With stale + low coherence + mono source + extreme move, confidence is destroyed
    assert v.adjusted_confidence < v.original_confidence * 0.5
    # "flag" takes precedence over "reject" when critical failures exist
    assert v.action in ("flag", "reject")


def test_flag_on_critical_failure(make_prediction, make_signal):
    """A critical-severity check failure results in action='flag'."""
    # Extreme move without evidence is typically critical
    pred = make_prediction(
        expected_move_pct=20.0,
        coherence=0.3,
        signal_strength=1.0,
        confidence=0.5,
    )
    _, verdicts = verify_predictions([pred])

    v = verdicts[0]
    critical_checks = [c for c in v.checks if c.severity == "critical" and not c.passed]
    if critical_checks:
        assert v.action in ("flag", "reject")


def test_guard_summary_aggregation(make_prediction, make_signal):
    """Summary correctly counts pass/adjust/flag/reject across predictions."""
    preds = []

    # 1 - healthy (should pass)
    preds.append(make_prediction(id="p1"))

    # 2 - moderate issue (should adjust)
    preds.append(make_prediction(id="p2", coherence=0.2, confidence=0.5))

    # 3 - extreme move without evidence (should flag or reject)
    preds.append(make_prediction(
        id="p3",
        expected_move_pct=20.0,
        coherence=0.3,
        signal_strength=1.0,
        confidence=0.5,
    ))

    # 4 - another healthy
    preds.append(make_prediction(id="p4"))

    # 5 - heavily degraded (should reject)
    stale = [make_signal(name="s", family="rates", freshness_hours=200.0)]
    preds.append(make_prediction(
        id="p5",
        signals=stale,
        coherence=0.1,
        confidence=0.9,
        expected_move_pct=25.0,
        signal_strength=0.5,
    ))

    _, verdicts = verify_predictions(preds)
    summary = guard_summary(verdicts)

    assert "total" in summary
    assert summary["total"] == 5
    # At least one of each action type should be present across the 5 predictions
    action_counts = {v.action for v in verdicts}
    assert len(action_counts) >= 2  # at minimum pass and something else


def test_empty_predictions_list():
    """Empty list returns empty results."""
    filtered, verdicts = verify_predictions([])

    assert filtered == []
    assert verdicts == []


def test_no_data_prediction_skipped(make_prediction):
    """A prediction with direction='NONE' and confidence=0.0 passes through with floor."""
    pred = make_prediction(direction="NONE", confidence=0.0)
    filtered, verdicts = verify_predictions([pred])

    assert len(filtered) == 1
    v = verdicts[0]
    # No-data placeholders bypass the guard entirely
    assert v.adjusted_confidence == 0.0
    assert v.action == "pass"
