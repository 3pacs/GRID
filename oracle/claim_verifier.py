"""
GRID — Claim verification against database evidence.

Looks up each extracted claim against resolved_series / feature_registry
and returns a verdict: supported, contradicted, insufficient, or ambiguous.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from loguru import logger as log
from sqlalchemy import text as sql_text
from sqlalchemy.engine import Engine

from oracle.claim_extractor import Claim

Verdict = Literal["supported", "contradicted", "insufficient", "ambiguous"]


@dataclass(frozen=True)
class VerifiedClaim:
    """A claim paired with evidence and a verdict."""

    claim: Claim
    verdict: Verdict
    confidence: float = 0.5
    evidence_value: float | None = None
    evidence_date: str | None = None
    evidence_source: str | None = None
    reason: str = ""


# ── Tolerance thresholds ─────────────────────────────────────────────────

_PRICE_TOLERANCE_PCT = 5.0   # within 5% = supported
_PCT_TOLERANCE_ABS = 3.0     # within 3 percentage-points = supported


# ── DB helpers ───────────────────────────────────────────────────────────

def _lookup_latest_value(
    engine: Engine,
    ticker: str,
) -> tuple[float | None, str | None, str | None]:
    """Return (value, obs_date, feature_name) for a ticker's latest price."""
    t_lower = ticker.lower()
    candidates = [t_lower, f"{t_lower}_full", f"{t_lower}_usd_full"]
    try:
        with engine.connect() as conn:
            for name in candidates:
                row = conn.execute(sql_text(
                    "SELECT rs.value, rs.obs_date, fr.name "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE fr.name = :name "
                    "ORDER BY rs.obs_date DESC LIMIT 1"
                ), {"name": name}).fetchone()
                if row and row[0] is not None:
                    return (float(row[0]), str(row[1]), row[2])
    except Exception as exc:
        log.debug("claim_verifier: DB lookup failed for {t}: {e}", t=ticker, e=str(exc))
    return (None, None, None)


def _lookup_feature_value(
    engine: Engine,
    feature_name: str,
) -> tuple[float | None, str | None]:
    """Return (value, obs_date) for a named feature."""
    try:
        with engine.connect() as conn:
            row = conn.execute(sql_text(
                "SELECT rs.value, rs.obs_date "
                "FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE fr.name = :name "
                "ORDER BY rs.obs_date DESC LIMIT 1"
            ), {"name": feature_name}).fetchone()
            if row and row[0] is not None:
                return (float(row[0]), str(row[1]))
    except Exception as exc:
        log.debug("claim_verifier: feature lookup failed for {f}: {e}", f=feature_name, e=str(exc))
    return (None, None)


def _lookup_price_change(
    engine: Engine,
    ticker: str,
    periods: int = 2,
) -> tuple[float | None, float | None]:
    """Return (latest_value, previous_value) for computing direction / pct change."""
    t_lower = ticker.lower()
    candidates = [t_lower, f"{t_lower}_full", f"{t_lower}_usd_full"]
    try:
        with engine.connect() as conn:
            for name in candidates:
                rows = conn.execute(sql_text(
                    "SELECT rs.value "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE fr.name = :name "
                    "ORDER BY rs.obs_date DESC LIMIT :n"
                ), {"name": name, "n": periods}).fetchall()
                if rows and len(rows) >= 2:
                    return (float(rows[0][0]), float(rows[-1][0]))
    except Exception as exc:
        log.debug("claim_verifier: price change lookup failed for {t}: {e}", t=ticker, e=str(exc))
    return (None, None)


# ── Verification logic per claim type ────────────────────────────────────

def _verify_price(claim: Claim, engine: Engine) -> VerifiedClaim:
    """Verify a price claim against the latest DB value."""
    if not claim.ticker or claim.value is None:
        return VerifiedClaim(claim=claim, verdict="insufficient", reason="No ticker or value to verify")

    actual, obs_date, source = _lookup_latest_value(engine, claim.ticker)
    if actual is None:
        return VerifiedClaim(claim=claim, verdict="insufficient", reason=f"No DB data for {claim.ticker}")

    pct_diff = abs(claim.value - actual) / actual * 100 if actual != 0 else 0
    if pct_diff <= _PRICE_TOLERANCE_PCT:
        return VerifiedClaim(
            claim=claim, verdict="supported", confidence=0.9,
            evidence_value=actual, evidence_date=obs_date, evidence_source=source,
            reason=f"Claimed ${claim.value:,.2f}, actual ${actual:,.2f} ({pct_diff:.1f}% diff)",
        )
    return VerifiedClaim(
        claim=claim, verdict="contradicted", confidence=0.9,
        evidence_value=actual, evidence_date=obs_date, evidence_source=source,
        reason=f"Claimed ${claim.value:,.2f}, actual ${actual:,.2f} ({pct_diff:.1f}% diff)",
    )


def _verify_percentage(claim: Claim, engine: Engine) -> VerifiedClaim:
    """Verify a percentage claim by computing actual % change."""
    if not claim.ticker or claim.value is None:
        return VerifiedClaim(claim=claim, verdict="insufficient", reason="No ticker or value to verify")

    latest, previous = _lookup_price_change(engine, claim.ticker)
    if latest is None or previous is None or previous == 0:
        return VerifiedClaim(claim=claim, verdict="insufficient", reason=f"Insufficient data for {claim.ticker}")

    actual_pct = (latest - previous) / previous * 100
    diff = abs(claim.value - actual_pct)
    if diff <= _PCT_TOLERANCE_ABS:
        return VerifiedClaim(
            claim=claim, verdict="supported", confidence=0.85,
            evidence_value=actual_pct, evidence_source=f"{claim.ticker} price change",
            reason=f"Claimed {claim.value:+.1f}%, actual {actual_pct:+.1f}%",
        )
    return VerifiedClaim(
        claim=claim, verdict="contradicted", confidence=0.85,
        evidence_value=actual_pct, evidence_source=f"{claim.ticker} price change",
        reason=f"Claimed {claim.value:+.1f}%, actual {actual_pct:+.1f}%",
    )


def _verify_direction(claim: Claim, engine: Engine) -> VerifiedClaim:
    """Verify a directional claim (surged/dropped) against actual movement."""
    if not claim.ticker:
        return VerifiedClaim(claim=claim, verdict="insufficient", reason="No ticker for direction check")

    latest, previous = _lookup_price_change(engine, claim.ticker)
    if latest is None or previous is None:
        return VerifiedClaim(claim=claim, verdict="insufficient", reason=f"No price data for {claim.ticker}")

    actual_direction = 1.0 if latest >= previous else -1.0
    claimed_direction = claim.value or 0.0

    if (claimed_direction > 0 and actual_direction > 0) or (claimed_direction < 0 and actual_direction < 0):
        return VerifiedClaim(
            claim=claim, verdict="supported", confidence=0.8,
            evidence_value=latest, evidence_source=f"{claim.ticker} direction",
            reason=f"Direction matches: {'up' if actual_direction > 0 else 'down'}",
        )
    return VerifiedClaim(
        claim=claim, verdict="contradicted", confidence=0.8,
        evidence_value=latest, evidence_source=f"{claim.ticker} direction",
        reason=f"Claimed {'up' if claimed_direction > 0 else 'down'}, actual {'up' if actual_direction > 0 else 'down'}",
    )


def _verify_generic(claim: Claim) -> VerifiedClaim:
    """Fallback for claim types without specific DB evidence."""
    return VerifiedClaim(
        claim=claim, verdict="ambiguous", confidence=0.3,
        reason=f"No automated verification for {claim.claim_type} claims",
    )


# ── Main entry point ────────────────────────────────────────────────────

_VERIFIERS = {
    "price": _verify_price,
    "percentage": _verify_percentage,
    "direction": _verify_direction,
}


def verify_claims(claims: list[Claim], engine: Engine) -> list[VerifiedClaim]:
    """Verify a list of claims against database evidence.

    Returns a VerifiedClaim for each input claim.
    """
    results: list[VerifiedClaim] = []
    for claim in claims:
        verifier = _VERIFIERS.get(claim.claim_type)
        if verifier is not None:
            results.append(verifier(claim, engine))
        else:
            results.append(_verify_generic(claim))
    return results
