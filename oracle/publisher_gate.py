"""
GRID — Publisher gate: decide publish / review / reject.

Deterministic rules based on claim verification and sanity check results.
No LLM dependency.

Re-exports the canonical AstroGrid `publish_astrogrid_prediction` from
``oracle.publish`` so that every importer — whether reaching it through
``oracle.publish`` directly or through ``oracle.publisher_gate`` — gets the
enriched implementation (regime / fci_regime / vix_level /
signal_contributions). Previously this module carried its own un-enriched
copy, which silently stripped conviction-stack context from astrogrid
predictions written through ``api.routers.astrogrid_helpers``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from oracle.publish import publish_astrogrid_prediction
from oracle.sanity_checker import CheckedClaim

Decision = Literal["publish", "review", "reject"]
VerificationStatus = Literal[
    "verified",
    "no_claims",
    "unverified",
    "flagged",
    "low_confidence",
    "rejected",
]

__all__ = [
    "Decision",
    "PublishDecision",
    "VerificationStatus",
    "gate_decision",
    "publish_astrogrid_prediction",
]


@dataclass(frozen=True)
class PublishDecision:
    """Final gate decision for an LLM output."""

    decision: Decision
    score: float | None
    claims: tuple[CheckedClaim, ...]
    reasons: tuple[str, ...]
    verification: VerificationStatus = "verified"


# ── Thresholds ───────────────────────────────────────────────────────────

_AUTO_PUBLISH_CONFIDENCE = 0.85
_REVIEW_REWRITE_RATIO = 0.30  # >30% claims flagged → review


def gate_decision(claims: list[CheckedClaim]) -> PublishDecision:
    """Evaluate checked claims and return a publish/review/reject decision.

    Hard fail rules (reject immediately):
      - Any claim contradicted by evidence
      - Any sanity check failure on a critical claim (price/percentage)
      - Price/number that fails range check

    No verifiable claims extracted:
      - Publish pass-through with ``verification="no_claims"`` and ``score=None``

    Clean auto-publish (unmarked):
      - All claims supported, zero sanity flags, avg confidence >= 0.85

    Marked review (publishes WITH ``[UNVERIFIED]`` markers — never blocks chat):
      - Any flagged claim (sanity warn or non-critical fail) — checked before
        all-insufficient so warned claims are never masked
      - All claims insufficient (no DB evidence)
      - Avg confidence < 0.85
      - Mixed verdicts containing any unverified (insufficient/ambiguous) claim
    """
    if not claims:
        return PublishDecision(
            decision="publish",
            score=None,
            claims=tuple(claims),
            reasons=("No verifiable claims found — pass-through",),
            verification="no_claims",
        )

    reasons: list[str] = []
    total = len(claims)
    supported = 0
    contradicted = 0
    insufficient = 0
    flagged = 0
    critical_fails = 0

    for cc in claims:
        v = cc.verified
        if v.verdict == "supported":
            supported += 1
        elif v.verdict == "contradicted":
            contradicted += 1
            reasons.append(f"CONTRADICTED: {v.reason}")
        elif v.verdict == "insufficient":
            insufficient += 1

        if cc.critical_fail:
            critical_fails += 1
            fail_msgs = [r.message for r in cc.checks if r.flag == "fail"]
            reasons.append(f"SANITY FAIL: {'; '.join(fail_msgs)}")

        has_any_flag = any(r.flag in ("fail", "warn") for r in cc.checks)
        if has_any_flag:
            flagged += 1

    # --- Hard reject ---
    if contradicted > 0:
        return PublishDecision(
            decision="reject",
            score=0.0,
            claims=tuple(claims),
            reasons=tuple(reasons or ["Contradicted claims detected"]),
            verification="rejected",
        )

    if critical_fails > 0:
        return PublishDecision(
            decision="reject",
            score=0.0,
            claims=tuple(claims),
            reasons=tuple(reasons or ["Critical sanity check failures"]),
            verification="rejected",
        )

    avg_confidence = (
        sum(cc.verified.confidence for cc in claims) / total
    )

    # --- Flagged claims (checked BEFORE all-insufficient) ---
    rewrite_ratio = flagged / total if total > 0 else 0.0
    if rewrite_ratio > _REVIEW_REWRITE_RATIO or flagged > 0:
        reasons.append(f"{flagged}/{total} claims flagged ({rewrite_ratio:.0%})")
        return PublishDecision(
            decision="review",
            score=avg_confidence,
            claims=tuple(claims),
            reasons=tuple(reasons),
            verification="flagged",
        )

    # --- All claims insufficient (no DB evidence) → publish with [UNVERIFIED] ---
    if insufficient == total:
        return PublishDecision(
            decision="review",
            score=None,
            claims=tuple(claims),
            reasons=("No verifiable claims — insufficient data for verification",),
            verification="unverified",
        )

    # --- Clean auto-publish (all claims supported, unflagged, >= 0.85) ---
    if supported == total and avg_confidence >= _AUTO_PUBLISH_CONFIDENCE:
        return PublishDecision(
            decision="publish",
            score=avg_confidence,
            claims=tuple(claims),
            reasons=("All verifiable claims supported, checks pass",),
            verification="verified",
        )

    # --- Below confidence threshold → publish with [UNVERIFIED] markers ---
    if avg_confidence < _AUTO_PUBLISH_CONFIDENCE:
        reasons.append(
            f"Average confidence {avg_confidence:.2f} below auto-publish "
            f"threshold {_AUTO_PUBLISH_CONFIDENCE:.2f}"
        )
        return PublishDecision(
            decision="review",
            score=avg_confidence,
            claims=tuple(claims),
            reasons=tuple(reasons),
            verification="low_confidence",
        )

    # --- Mixed verdicts containing unverified (insufficient/ambiguous) claims ---
    reasons.append("Mixed verdicts include unverified claims")
    return PublishDecision(
        decision="review",
        score=avg_confidence,
        claims=tuple(claims),
        reasons=tuple(reasons),
        verification="unverified",
    )
