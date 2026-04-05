"""
GRID — Publishing firewall: single entry point for claim-level verification.

Chains: extract → verify → sanity check → gate.
Returns the full pipeline result with the original text, verified claims,
and decision.  If rejected, returns a safe fallback.  If review,
inserts [UNVERIFIED] markers on flagged claims.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from loguru import logger as log
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from oracle.claim_extractor import extract_claims
from oracle.claim_verifier import verify_claims
from oracle.publisher_gate import PublishDecision, gate_decision
from oracle.sanity_checker import CheckedClaim, run_sanity_checks


@dataclass(frozen=True)
class FirewallResult:
    """Full pipeline result."""

    original_text: str
    output_text: str
    decision: PublishDecision
    claim_count: int
    flagged_count: int


_SAFE_FALLBACK = (
    "I was unable to verify some claims in this response against our data. "
    "Please check the latest data directly or ask me to regenerate."
)


def verify_output(text: str, engine: Engine) -> FirewallResult:
    """Run the full publishing firewall pipeline.

    1. Extract atomic claims from LLM text
    2. Verify each claim against the database
    3. Run deterministic sanity checks
    4. Make a publish/review/reject gate decision
    5. Audit to claim_audit table
    6. Return annotated text or safe fallback
    """
    # 1. Extract
    claims = extract_claims(text)
    if not claims:
        decision = gate_decision([])
        return FirewallResult(
            original_text=text,
            output_text=text,
            decision=decision,
            claim_count=0,
            flagged_count=0,
        )

    # 2. Verify
    verified = verify_claims(claims, engine)

    # 3. Sanity check
    checked = run_sanity_checks(verified)

    # 4. Gate decision
    decision = gate_decision(checked)

    # 5. Audit (best-effort, non-blocking)
    _audit_claims(engine, checked, decision)

    # 6. Build output text
    flagged_count = sum(1 for cc in checked if cc.critical_fail or cc.verified.verdict == "contradicted")

    if decision.decision == "reject":
        output_text = _SAFE_FALLBACK
    elif decision.decision == "review":
        output_text = _mark_unverified(text, checked)
    else:
        output_text = text

    return FirewallResult(
        original_text=text,
        output_text=output_text,
        decision=decision,
        claim_count=len(claims),
        flagged_count=flagged_count,
    )


def _mark_unverified(text: str, checked: list[CheckedClaim]) -> str:
    """Insert [UNVERIFIED] markers before flagged claim sentences."""
    result = text
    # Process in reverse order of source_span to preserve positions
    flagged = [
        cc for cc in checked
        if cc.critical_fail
        or cc.verified.verdict in ("contradicted", "ambiguous")
        or any(r.flag == "warn" for r in cc.checks)
    ]
    # Sort by source span start, descending, so insertions don't shift later spans
    flagged.sort(key=lambda cc: cc.verified.claim.source_span[0], reverse=True)

    for cc in flagged:
        span_start = cc.verified.claim.source_span[0]
        if 0 < span_start <= len(result):
            result = result[:span_start] + "[UNVERIFIED] " + result[span_start:]
        elif span_start == 0:
            result = "[UNVERIFIED] " + result

    return result


def _audit_claims(
    engine: Engine,
    checked: list[CheckedClaim],
    decision: PublishDecision,
) -> None:
    """Write claim audit records to claim_audit table (best-effort)."""
    try:
        with engine.connect() as conn:
            for cc in checked:
                v = cc.verified
                c = v.claim
                evidence = {
                    "value": v.evidence_value,
                    "date": v.evidence_date,
                    "source": v.evidence_source,
                    "reason": v.reason,
                }
                sanity = [
                    {"check": r.check_name, "flag": r.flag, "msg": r.message}
                    for r in cc.checks
                ]
                # Determine materiality from claim type
                materiality = "high" if c.claim_type in ("price", "percentage") else "medium"

                conn.execute(sql_text(
                    "INSERT INTO claim_audit "
                    "(source, claim_text, claim_type, materiality, verdict, "
                    " confidence, evidence, sanity_checks, published) "
                    "VALUES (:source, :claim_text, :claim_type, :materiality, "
                    " :verdict, :confidence, :evidence, :sanity_checks, :published)"
                ), {
                    "source": "chat_firewall",
                    "claim_text": c.text[:500],
                    "claim_type": c.claim_type,
                    "materiality": materiality,
                    "verdict": v.verdict,
                    "confidence": v.confidence,
                    "evidence": json.dumps(evidence),
                    "sanity_checks": json.dumps(sanity),
                    "published": decision.decision == "publish",
                })
            conn.commit()
    except Exception as exc:
        log.warning("Firewall audit write failed (non-fatal): {e}", e=str(exc))
