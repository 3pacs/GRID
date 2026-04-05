"""
GRID — Publisher gate: decide publish / review / reject.

Deterministic rules based on claim verification and sanity check results.
No LLM dependency.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from oracle.sanity_checker import CheckedClaim

Decision = Literal["publish", "review", "reject"]


@dataclass(frozen=True)
class PublishDecision:
    """Final gate decision for an LLM output."""

    decision: Decision
    score: float
    claims: tuple[CheckedClaim, ...]
    reasons: tuple[str, ...]


# ── Thresholds ───────────────────────────────────────────────────────────

_AUTO_PUBLISH_CONFIDENCE = 0.85
_REVIEW_REWRITE_RATIO = 0.30  # >30% claims flagged → review


def gate_decision(claims: list[CheckedClaim]) -> PublishDecision:
    """Evaluate checked claims and return a publish/review/reject decision.

    Hard fail rules (reject immediately):
      - Any claim contradicted by evidence
      - Any sanity check failure on a critical claim (price/percentage)
      - Price/number that fails range check

    Auto-publish rules:
      - All claims supported, all checks pass, confidence > 0.85

    Human review:
      - Mixed verdicts, source disagreement, >30% claims flagged
    """
    if not claims:
        return PublishDecision(
            decision="publish",
            score=1.0,
            claims=tuple(claims),
            reasons=("No verifiable claims found — pass-through",),
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
        )

    if critical_fails > 0:
        return PublishDecision(
            decision="reject",
            score=0.0,
            claims=tuple(claims),
            reasons=tuple(reasons or ["Critical sanity check failures"]),
        )

    # --- Auto-publish ---
    verifiable = supported + contradicted
    avg_confidence = (
        sum(cc.verified.confidence for cc in claims) / total
    )

    if (
        supported == verifiable
        and verifiable > 0
        and flagged == 0
        and avg_confidence >= _AUTO_PUBLISH_CONFIDENCE
    ):
        return PublishDecision(
            decision="publish",
            score=avg_confidence,
            claims=tuple(claims),
            reasons=("All verifiable claims supported, checks pass",),
        )

    # All claims insufficient (no verifiable data) — still publish with lower score
    if insufficient == total:
        return PublishDecision(
            decision="publish",
            score=0.5,
            claims=tuple(claims),
            reasons=("No verifiable claims — insufficient data for verification",),
        )

    # --- Review ---
    rewrite_ratio = flagged / total if total > 0 else 0
    if rewrite_ratio > _REVIEW_REWRITE_RATIO or flagged > 0:
        reasons.append(f"{flagged}/{total} claims flagged ({rewrite_ratio:.0%})")
        return PublishDecision(
            decision="review",
            score=avg_confidence,
            claims=tuple(claims),
            reasons=tuple(reasons),
        )

    # Default: publish with moderate confidence
    return PublishDecision(
        decision="publish",
        score=avg_confidence,
        claims=tuple(claims),
        reasons=("Mixed verdicts but no contradictions or failures",),
    )
