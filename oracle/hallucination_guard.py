"""
GRID Oracle Hallucination Guard — deterministic pre-storage verification layer.

Inspired by Feynman's principle: "The first principle is that you must not
fool yourself — and you are the easiest person to fool."

Every prediction the Oracle generates passes through this guard BEFORE storage.
The guard runs 8 independent checks that look for hallucination signatures:
signals that are stale, contradicted, incoherent, mono-source, uncalibrated,
or making extreme claims without proportional evidence.

No LLM calls. Purely deterministic. Each check returns a confidence multiplier
that compounds to produce an adjusted confidence. If too much confidence is
destroyed, the prediction is flagged or rejected outright.

The guard does NOT suppress predictions — it adjusts confidence downward and
leaves an audit trail. Even rejected predictions are logged with full
provenance so the Oracle can learn from what it almost said.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from functools import reduce
from typing import Any

from loguru import logger as log


# ── Data Structures ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class GuardCheck:
    """Result of a single verification check."""

    check_name: str
    passed: bool
    severity: str  # "info", "warning", "critical"
    message: str
    adjustment: float  # Confidence multiplier (1.0 = no change, 0.5 = halve)


@dataclass(frozen=True)
class GuardVerdict:
    """Complete verification result for a prediction."""

    prediction_id: str
    original_confidence: float
    adjusted_confidence: float
    checks: tuple[GuardCheck, ...]
    action: str  # "pass", "adjust", "flag", "reject"
    reasons: tuple[str, ...]


# ── Thresholds ──────────────────────────────────────────────────────────────

_STALE_HOURS = 72.0
_STALE_RATIO_THRESHOLD = 0.50
_STALE_ADJUSTMENT = 0.7

_ANTI_SIGNAL_RATIO_THRESHOLD = 0.6
_ANTI_SIGNAL_ADJUSTMENT = 0.5

_COHERENCE_FLOOR = 0.3
_COHERENCE_ADJUSTMENT = 0.6

_OVERCONFIDENT_THRESHOLD = 0.7
_MIN_CALIBRATION_ADJUSTMENT = 0.5

_MIN_SIGNAL_FAMILIES = 2
_MONO_SOURCE_ADJUSTMENT = 0.8

_EXTREME_MOVE_PCT = 15.0
_EXTREME_COHERENCE_REQ = 0.7
_EXTREME_STRENGTH_REQ = 3.0
_EXTREME_ADJUSTMENT = 0.4

_MIN_SCORED_PREDICTIONS = 20
_LOW_HIT_RATE = 0.35
_MIN_MODEL_ADJUSTMENT = 0.5

_HIGH_CONFIDENCE_THRESHOLD = 0.8
_CONVERGENCE_ADJUSTMENT = 0.75

_CONFIDENCE_FLOOR = 0.05
_REJECT_RATIO = 0.5  # reject if adjusted < original * this


# ── Individual Checks ───────────────────────────────────────────────────────


def _check_signal_freshness(pred: Any) -> GuardCheck:
    """Stale signals indicate hallucinated relevance."""
    signals = getattr(pred, "signals", [])
    if not signals:
        return GuardCheck(
            check_name="signal_freshness",
            passed=True,
            severity="info",
            message="No signals to check freshness",
            adjustment=1.0,
        )

    stale_count = sum(
        1 for s in signals if s.freshness_hours > _STALE_HOURS
    )
    stale_ratio = stale_count / len(signals)
    passed = stale_ratio <= _STALE_RATIO_THRESHOLD

    return GuardCheck(
        check_name="signal_freshness",
        passed=passed,
        severity="warning" if not passed else "info",
        message=(
            f"{stale_count}/{len(signals)} signals stale "
            f"(>{_STALE_HOURS}h) — ratio {stale_ratio:.2f}"
        ),
        adjustment=_STALE_ADJUSTMENT if not passed else 1.0,
    )


def _check_anti_signal_ratio(pred: Any) -> GuardCheck:
    """Too much contradicting evidence undermines the prediction."""
    anti_signals = getattr(pred, "anti_signals", [])
    signal_strength = getattr(pred, "signal_strength", 0.0)

    if not anti_signals or signal_strength <= 0:
        return GuardCheck(
            check_name="anti_signal_ratio",
            passed=True,
            severity="info",
            message="No anti-signals or zero signal strength",
            adjustment=1.0,
        )

    total_severity = sum(a.severity for a in anti_signals)
    threshold = _ANTI_SIGNAL_RATIO_THRESHOLD * signal_strength
    passed = total_severity <= threshold

    return GuardCheck(
        check_name="anti_signal_ratio",
        passed=passed,
        severity="critical" if not passed else "info",
        message=(
            f"Anti-signal severity {total_severity:.2f} vs "
            f"threshold {threshold:.2f} "
            f"({_ANTI_SIGNAL_RATIO_THRESHOLD}x signal_strength {signal_strength:.2f})"
        ),
        adjustment=_ANTI_SIGNAL_ADJUSTMENT if not passed else 1.0,
    )


def _check_coherence_floor(pred: Any) -> GuardCheck:
    """Low coherence means signals point in all directions — direction is noise."""
    coherence = getattr(pred, "coherence", 0.0)
    passed = coherence >= _COHERENCE_FLOOR

    return GuardCheck(
        check_name="coherence_floor",
        passed=passed,
        severity="warning" if not passed else "info",
        message=f"Coherence {coherence:.3f} vs floor {_COHERENCE_FLOOR}",
        adjustment=_COHERENCE_ADJUSTMENT if not passed else 1.0,
    )


def _check_confidence_calibration(
    pred: Any,
    calibration_report: Any | None,
) -> GuardCheck:
    """Penalize overconfident predictions when calibration data confirms the bias."""
    if calibration_report is None:
        return GuardCheck(
            check_name="confidence_calibration",
            passed=True,
            severity="info",
            message="No calibration report available — skipping",
            adjustment=1.0,
        )

    label = getattr(calibration_report, "label", "")
    confidence = getattr(pred, "confidence", 0.0)
    ece = getattr(calibration_report, "calibration_error", 0.0)

    if label != "overconfident" or confidence <= _OVERCONFIDENT_THRESHOLD:
        return GuardCheck(
            check_name="confidence_calibration",
            passed=True,
            severity="info",
            message=(
                f"Calibration label={label}, confidence={confidence:.2f} — "
                f"no penalty needed"
            ),
            adjustment=1.0,
        )

    adjustment = max(_MIN_CALIBRATION_ADJUSTMENT, 1.0 - ece)

    return GuardCheck(
        check_name="confidence_calibration",
        passed=False,
        severity="warning",
        message=(
            f"Overconfident (label={label}, conf={confidence:.2f}, "
            f"ECE={ece:.3f}) — adjustment {adjustment:.2f}"
        ),
        adjustment=adjustment,
    )


def _check_empty_signal_families(pred: Any) -> GuardCheck:
    """Mono-source predictions lack the diversification that reduces hallucination."""
    signals = getattr(pred, "signals", [])
    families = {s.family for s in signals}
    count = len(families)
    passed = count >= _MIN_SIGNAL_FAMILIES

    return GuardCheck(
        check_name="empty_signal_families",
        passed=passed,
        severity="warning" if not passed else "info",
        message=(
            f"{count} distinct signal families "
            f"({', '.join(sorted(families)) if families else 'none'}) — "
            f"min required {_MIN_SIGNAL_FAMILIES}"
        ),
        adjustment=_MONO_SOURCE_ADJUSTMENT if not passed else 1.0,
    )


def _check_extreme_move(pred: Any) -> GuardCheck:
    """Big claims need big evidence — extraordinary claims, extraordinary evidence."""
    expected_move = abs(getattr(pred, "expected_move_pct", 0.0))

    if expected_move <= _EXTREME_MOVE_PCT:
        return GuardCheck(
            check_name="extreme_move",
            passed=True,
            severity="info",
            message=f"Expected move {expected_move:.1f}% within normal range",
            adjustment=1.0,
        )

    coherence = getattr(pred, "coherence", 0.0)
    strength = getattr(pred, "signal_strength", 0.0)
    meets_coherence = coherence > _EXTREME_COHERENCE_REQ
    meets_strength = strength > _EXTREME_STRENGTH_REQ
    passed = meets_coherence and meets_strength

    return GuardCheck(
        check_name="extreme_move",
        passed=passed,
        severity="critical" if not passed else "info",
        message=(
            f"Extreme move {expected_move:.1f}% — "
            f"coherence {coherence:.2f} ({'ok' if meets_coherence else 'FAIL'} "
            f"vs {_EXTREME_COHERENCE_REQ}), "
            f"strength {strength:.1f} ({'ok' if meets_strength else 'FAIL'} "
            f"vs {_EXTREME_STRENGTH_REQ})"
        ),
        adjustment=1.0 if passed else _EXTREME_ADJUSTMENT,
    )


def _check_model_track_record(
    pred: Any,
    model_stats: dict[str, dict[str, int]] | None,
) -> GuardCheck:
    """Degrade predictions from models with proven poor hit rates."""
    if model_stats is None:
        return GuardCheck(
            check_name="model_track_record",
            passed=True,
            severity="info",
            message="No model stats available — skipping",
            adjustment=1.0,
        )

    model_name = getattr(pred, "model_name", "")
    stats = model_stats.get(model_name)

    if stats is None:
        return GuardCheck(
            check_name="model_track_record",
            passed=True,
            severity="info",
            message=f"No stats for model '{model_name}' — skipping",
            adjustment=1.0,
        )

    hits = stats.get("hits", 0)
    misses = stats.get("misses", 0)
    partials = stats.get("partials", 0)
    total = hits + misses + partials

    if total < _MIN_SCORED_PREDICTIONS:
        return GuardCheck(
            check_name="model_track_record",
            passed=True,
            severity="info",
            message=(
                f"Model '{model_name}' has {total} scored predictions "
                f"(< {_MIN_SCORED_PREDICTIONS}) — insufficient data"
            ),
            adjustment=1.0,
        )

    hit_rate = hits / total
    passed = hit_rate >= _LOW_HIT_RATE
    adjustment = 1.0 if passed else max(_MIN_MODEL_ADJUSTMENT, hit_rate + 0.2)

    return GuardCheck(
        check_name="model_track_record",
        passed=passed,
        severity="warning" if not passed else "info",
        message=(
            f"Model '{model_name}' hit rate {hit_rate:.2f} "
            f"({hits}/{total}) — "
            f"{'ok' if passed else f'below {_LOW_HIT_RATE}, adjustment {adjustment:.2f}'}"
        ),
        adjustment=adjustment,
    )


def _check_convergence_required(pred: Any) -> GuardCheck:
    """High confidence without independent convergence is a hallucination signature."""
    confidence = getattr(pred, "confidence", 0.0)

    if confidence <= _HIGH_CONFIDENCE_THRESHOLD:
        return GuardCheck(
            check_name="convergence_required",
            passed=True,
            severity="info",
            message=f"Confidence {confidence:.2f} below threshold — no convergence required",
            adjustment=1.0,
        )

    signals = getattr(pred, "signals", [])
    has_convergence = any(s.family == "convergence" for s in signals)
    passed = has_convergence

    return GuardCheck(
        check_name="convergence_required",
        passed=passed,
        severity="warning" if not passed else "info",
        message=(
            f"High confidence {confidence:.2f} — "
            f"convergence signal {'present' if has_convergence else 'MISSING'}"
        ),
        adjustment=_CONVERGENCE_ADJUSTMENT if not passed else 1.0,
    )


def _check_reference_validity(pred: Any) -> GuardCheck:
    """Check if any URLs in prediction narrative/rationale are hallucinated.

    Uses cached ref verification results if available. Skipped gracefully
    if the verification module or config is unavailable.
    """
    # Extract text that might contain URLs
    narrative = getattr(pred, "narrative", "") or ""
    rationale = getattr(pred, "rationale", "") or ""
    text = f"{narrative} {rationale}".strip()

    if not text or "http" not in text:
        return GuardCheck(
            check_name="ref_hallucinated", passed=True, severity="info",
            message="No URLs in prediction text", adjustment=1.0,
        )

    try:
        from config import settings
        if not settings.REF_CHECK_ENABLED:
            return GuardCheck(
                check_name="ref_hallucinated", passed=True, severity="info",
                message="Reference check disabled", adjustment=1.0,
            )

        from verification.ref_extractor import extract_refs
        refs = extract_refs(text)
        if not refs:
            return GuardCheck(
                check_name="ref_hallucinated", passed=True, severity="info",
                message="No extractable URLs in prediction text", adjustment=1.0,
            )

        # Synchronous check — use cached results or skip if async not available
        import asyncio
        from verification.url_health import check_urls
        loop = asyncio.new_event_loop()
        try:
            url_results = loop.run_until_complete(check_urls(
                [r.url for r in refs],
                max_concurrent=settings.REF_CHECK_MAX_CONCURRENT,
                timeout_s=settings.REF_CHECK_TIMEOUT_S,
                wayback_enabled=settings.REF_CHECK_WAYBACK_ENABLED,
            ))
        finally:
            loop.close()

        from verification.url_health import URLClassification
        hallucinated = sum(
            1 for r in url_results
            if r.classification == URLClassification.LIKELY_HALLUCINATED
        )
        if hallucinated == 0:
            return GuardCheck(
                check_name="ref_hallucinated", passed=True, severity="info",
                message=f"{len(refs)} refs checked — all valid", adjustment=1.0,
            )

        adjustment = 0.5 ** hallucinated
        return GuardCheck(
            check_name="ref_hallucinated", passed=False, severity="critical",
            message=f"{hallucinated}/{len(refs)} refs likely hallucinated",
            adjustment=adjustment,
        )

    except Exception as exc:
        log.debug("Reference validity check skipped: {e}", e=str(exc))
        return GuardCheck(
            check_name="ref_hallucinated", passed=True, severity="info",
            message=f"Reference check skipped: {exc}", adjustment=1.0,
        )


# ── Main Verification ──────────────────────────────────────────────────────

_ALL_CHECKS = (
    "_check_signal_freshness",
    "_check_anti_signal_ratio",
    "_check_coherence_floor",
    "_check_confidence_calibration",
    "_check_empty_signal_families",
    "_check_extreme_move",
    "_check_model_track_record",
    "_check_convergence_required",
    "_check_reference_validity",
)


def _run_checks(
    pred: Any,
    calibration_report: Any | None,
    model_stats: dict[str, dict[str, int]] | None,
) -> tuple[GuardCheck, ...]:
    """Run all guard checks against a single prediction."""
    checks: list[GuardCheck] = [
        _check_signal_freshness(pred),
        _check_anti_signal_ratio(pred),
        _check_coherence_floor(pred),
        _check_confidence_calibration(pred, calibration_report),
        _check_empty_signal_families(pred),
        _check_extreme_move(pred),
        _check_model_track_record(pred, model_stats),
        _check_convergence_required(pred),
        _check_reference_validity(pred),
    ]
    return tuple(checks)


def _determine_action(
    checks: tuple[GuardCheck, ...],
    original_confidence: float,
    adjusted_confidence: float,
) -> str:
    """Determine the verdict action based on check results."""
    has_critical_fail = any(
        c.severity == "critical" and not c.passed for c in checks
    )
    if has_critical_fail:
        return "flag"

    if adjusted_confidence < original_confidence * _REJECT_RATIO:
        return "reject"

    if adjusted_confidence < original_confidence:
        return "adjust"

    return "pass"


def verify_predictions(
    predictions: list[Any],
    calibration_report: Any | None = None,
    model_stats: dict[str, dict[str, int]] | None = None,
) -> tuple[list[Any], list[GuardVerdict]]:
    """Verify predictions and adjust confidence to reduce hallucinations.

    Runs 8 independent checks per prediction. Each check produces a confidence
    multiplier. Multipliers compound to produce an adjusted confidence. If
    confidence is sufficiently destroyed, the prediction is flagged or rejected.

    Args:
        predictions: List of OraclePrediction objects.
        calibration_report: Optional CalibrationReport for calibration checks.
        model_stats: Optional dict mapping model_name to
            {"hits": int, "misses": int, "partials": int}.

    Returns:
        (adjusted_predictions, verdicts) — predictions with adjusted confidence,
        plus full audit trail of all checks.
    """
    adjusted_predictions: list[Any] = []
    verdicts: list[GuardVerdict] = []

    for pred in predictions:
        original_confidence = getattr(pred, "confidence", 0.0)
        pred_id = getattr(pred, "id", "unknown")
        ticker = getattr(pred, "ticker", "?")

        # Skip guard for no-data placeholders (confidence=0, direction=NONE)
        if original_confidence == 0.0:
            verdict = GuardVerdict(
                prediction_id=pred_id,
                original_confidence=0.0,
                adjusted_confidence=0.0,
                checks=(),
                action="pass",
                reasons=(),
            )
            verdicts.append(verdict)
            adjusted_predictions.append(pred)
            continue

        checks = _run_checks(pred, calibration_report, model_stats)

        # Compound all adjustments multiplicatively
        final_multiplier = reduce(
            lambda acc, c: acc * c.adjustment, checks, 1.0
        )
        adjusted_confidence = max(
            _CONFIDENCE_FLOOR, original_confidence * final_multiplier
        )

        action = _determine_action(checks, original_confidence, adjusted_confidence)

        failed_reasons = tuple(
            c.message for c in checks if not c.passed
        )

        verdict = GuardVerdict(
            prediction_id=pred_id,
            original_confidence=original_confidence,
            adjusted_confidence=adjusted_confidence,
            checks=checks,
            action=action,
            reasons=failed_reasons,
        )
        verdicts.append(verdict)

        # Create adjusted copy
        adjusted_pred = replace(pred, confidence=adjusted_confidence)
        adjusted_predictions.append(adjusted_pred)

        # Log summary per prediction
        if action == "pass":
            log.debug(
                "Guard PASS: {} {} — confidence {:.2f} unchanged",
                ticker, pred_id[:8], original_confidence,
            )
        elif action == "adjust":
            log.info(
                "Guard ADJUST: {} {} — confidence {:.2f} -> {:.2f} (x{:.2f})",
                ticker, pred_id[:8], original_confidence,
                adjusted_confidence, final_multiplier,
            )
        elif action == "flag":
            log.warning(
                "Guard FLAG: {} {} — confidence {:.2f} -> {:.2f} | {}",
                ticker, pred_id[:8], original_confidence,
                adjusted_confidence, "; ".join(failed_reasons),
            )
        elif action == "reject":
            log.warning(
                "Guard REJECT: {} {} — confidence {:.2f} -> {:.2f} "
                "(destroyed >{:.0f}%) | {}",
                ticker, pred_id[:8], original_confidence,
                adjusted_confidence, (1 - _REJECT_RATIO) * 100,
                "; ".join(failed_reasons),
            )

    # Batch summary
    summary = guard_summary(verdicts)
    log.info(
        "Hallucination guard: {} predictions — "
        "{} passed, {} adjusted, {} flagged, {} rejected "
        "(avg confidence change {:.2f})",
        summary["total"],
        summary["passed"],
        summary["adjusted"],
        summary["flagged"],
        summary["rejected"],
        summary["avg_confidence_change"],
    )

    return adjusted_predictions, verdicts


# ── Summary ─────────────────────────────────────────────────────────────────


def guard_summary(verdicts: list[GuardVerdict]) -> dict[str, Any]:
    """Aggregate stats: passed, adjusted, flagged, rejected counts + avg confidence change.

    Args:
        verdicts: List of GuardVerdict objects from verify_predictions.

    Returns:
        Dict with keys: total, passed, adjusted, flagged, rejected,
        avg_confidence_change (negative means confidence reduced on average).
    """
    total = len(verdicts)
    if total == 0:
        return {
            "total": 0,
            "passed": 0,
            "adjusted": 0,
            "flagged": 0,
            "rejected": 0,
            "avg_confidence_change": 0.0,
        }

    counts: dict[str, int] = {"pass": 0, "adjust": 0, "flag": 0, "reject": 0}
    total_change = 0.0

    for v in verdicts:
        counts[v.action] = counts.get(v.action, 0) + 1
        total_change += v.adjusted_confidence - v.original_confidence

    return {
        "total": total,
        "passed": counts["pass"],
        "adjusted": counts["adjust"],
        "flagged": counts["flag"],
        "rejected": counts["reject"],
        "avg_confidence_change": total_change / total,
    }
