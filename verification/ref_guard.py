"""Reference hallucination guard — confidence adjustment based on URL health.

Mirrors the oracle/hallucination_guard.py GuardCheck pattern: each check
produces a confidence multiplier that compounds to an adjusted confidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import Any

from oracle.hallucination_guard import GuardCheck
from verification.url_health import URLCheckResult, URLClassification


# ── Thresholds ──────────────────────────────────────────────────────────────

_HALLUCINATED_ADJUSTMENT = 0.5  # Per hallucinated ref
_DEAD_RATIO_THRESHOLD = 0.50
_DEAD_ADJUSTMENT = 0.8
_UNKNOWN_RATIO_THRESHOLD = 0.30
_UNKNOWN_ADJUSTMENT = 0.9
_OVER_CITATION_REFS = 5
_OVER_CITATION_WORDS = 500
_OVER_CITATION_ADJUSTMENT = 0.9
_REJECT_RATIO = 0.4


@dataclass(frozen=True)
class ReferenceVerdict:
    """Complete reference verification result."""

    original_confidence: float
    adjusted_confidence: float
    checks: tuple[GuardCheck, ...]
    action: str  # "pass", "clean", "flag", "reject"
    url_results: tuple[URLCheckResult, ...]
    reasons: tuple[str, ...]


# ── Individual Checks ───────────────────────────────────────────────────────

def _check_hallucinated_refs(results: list[URLCheckResult]) -> GuardCheck:
    """Flag any LIKELY_HALLUCINATED URLs — 0.5x per instance."""
    hallucinated = [r for r in results if r.classification == URLClassification.LIKELY_HALLUCINATED]
    count = len(hallucinated)
    if count == 0:
        return GuardCheck(
            check_name="ref_hallucinated", passed=True, severity="info",
            message="No hallucinated references detected",
            adjustment=1.0,
        )
    adjustment = _HALLUCINATED_ADJUSTMENT ** count
    urls = ", ".join(r.url[:60] for r in hallucinated[:3])
    return GuardCheck(
        check_name="ref_hallucinated", passed=False, severity="critical",
        message=f"{count} likely hallucinated URL(s): {urls}",
        adjustment=adjustment,
    )


def _check_dead_refs(results: list[URLCheckResult]) -> GuardCheck:
    """Check if majority of refs are dead (real but moved/deleted)."""
    if not results:
        return GuardCheck(
            check_name="ref_dead", passed=True, severity="info",
            message="No references to check", adjustment=1.0,
        )
    dead = sum(1 for r in results if r.classification == URLClassification.DEAD)
    ratio = dead / len(results)
    passed = ratio < _DEAD_RATIO_THRESHOLD
    return GuardCheck(
        check_name="ref_dead", passed=passed, severity="warning" if not passed else "info",
        message=f"{dead}/{len(results)} refs are dead ({ratio:.0%})",
        adjustment=1.0 if passed else _DEAD_ADJUSTMENT,
    )


def _check_unreachable_refs(results: list[URLCheckResult]) -> GuardCheck:
    """Check if too many refs are unreachable (network issues).

    BOT_BLOCKED refs are excluded — they're real sites with bot protection.
    """
    if not results:
        return GuardCheck(
            check_name="ref_unreachable", passed=True, severity="info",
            message="No references to check", adjustment=1.0,
        )
    # Only count truly unknown refs, not bot-blocked ones
    unknown = sum(1 for r in results if r.classification == URLClassification.UNKNOWN)
    ratio = unknown / len(results)
    passed = ratio < _UNKNOWN_RATIO_THRESHOLD
    return GuardCheck(
        check_name="ref_unreachable", passed=passed,
        severity="warning" if not passed else "info",
        message=f"{unknown}/{len(results)} refs unreachable ({ratio:.0%})",
        adjustment=1.0 if passed else _UNKNOWN_ADJUSTMENT,
    )


def _check_over_citation(results: list[URLCheckResult], word_count: int) -> GuardCheck:
    """Excessive citations in short text may indicate compensation for weak claims."""
    if word_count >= _OVER_CITATION_WORDS or len(results) <= _OVER_CITATION_REFS:
        return GuardCheck(
            check_name="ref_density", passed=True, severity="info",
            message=f"{len(results)} refs in {word_count} words — normal density",
            adjustment=1.0,
        )
    return GuardCheck(
        check_name="ref_density", passed=False, severity="warning",
        message=f"{len(results)} refs in only {word_count} words — over-citation",
        adjustment=_OVER_CITATION_ADJUSTMENT,
    )


# ── Main Verification ──────────────────────────────────────────────────────

def verify_references(
    text: str,
    original_confidence: float,
    url_results: list[URLCheckResult],
) -> ReferenceVerdict:
    """Run reference checks and produce confidence-adjusted verdict.

    Args:
        text: The LLM-generated text containing references.
        original_confidence: Confidence before reference verification.
        url_results: Pre-fetched URL health check results.

    Returns:
        ReferenceVerdict with adjusted confidence and audit trail.
    """
    if not url_results:
        return ReferenceVerdict(
            original_confidence=original_confidence,
            adjusted_confidence=original_confidence,
            checks=(),
            action="pass",
            url_results=(),
            reasons=(),
        )

    word_count = len(text.split())

    checks = (
        _check_hallucinated_refs(url_results),
        _check_dead_refs(url_results),
        _check_unreachable_refs(url_results),
        _check_over_citation(url_results, word_count),
    )

    final_multiplier = reduce(lambda acc, c: acc * c.adjustment, checks, 1.0)
    adjusted = max(0.01, original_confidence * final_multiplier)

    # Determine action
    has_hallucinated = any(
        r.classification == URLClassification.LIKELY_HALLUCINATED
        for r in url_results
    )
    has_dead = any(r.classification == URLClassification.DEAD for r in url_results)

    if adjusted < original_confidence * _REJECT_RATIO:
        action = "reject"
    elif has_hallucinated:
        action = "flag"
    elif has_dead:
        action = "clean"
    elif adjusted < original_confidence:
        action = "adjust"
    else:
        action = "pass"

    reasons = tuple(c.message for c in checks if not c.passed)

    return ReferenceVerdict(
        original_confidence=original_confidence,
        adjusted_confidence=adjusted,
        checks=checks,
        action=action,
        url_results=tuple(url_results),
        reasons=reasons,
    )
