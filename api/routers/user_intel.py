"""
User-contributed intelligence router.

Users act as tentacles — they can submit biographical facts, connections,
loyalties, stances, rumors, or tips about any actor directly in the app.
Other users upvote/downvote/flag. Admins verify. Verified intel boosts trust.

All DB access uses parameterized SQL. Submissions are rate-limited to 20/hour
per user. Vote dedup is enforced via a UNIQUE (intel_id, user_id) constraint.

Prefix: mounted at /api/v1 — paths are actor- or intel-scoped.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query, status
from loguru import logger as log
from pydantic import BaseModel, Field
from sqlalchemy import text

from api.auth import decode_token, require_auth, require_role
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1", tags=["user-intel"])


# ── Config ────────────────────────────────────────────────────────────────

VALID_TYPES = {"biography", "connection", "loyalty", "stance", "rumor", "tip", "fact"}
VALID_CONFIDENCE = {"high", "medium", "low"}
VALID_STATUS = {"pending", "verified", "rejected"}
RATE_LIMIT_PER_HOUR = int(os.getenv("GRID_USER_INTEL_RATE_LIMIT", "20"))
NOTE_MAX = 4000
URL_MAX = 1000


# ── Pydantic schemas ──────────────────────────────────────────────────────


class IntelSubmission(BaseModel):
    intel_type: str = Field(..., description="biography|connection|loyalty|stance|rumor|tip|fact")
    note: str = Field(..., min_length=1, max_length=NOTE_MAX)
    source_url: Optional[str] = Field(default=None, max_length=URL_MAX)
    confidence: Optional[str] = Field(default=None, description="high|medium|low")


class VotePayload(BaseModel):
    vote: int = Field(..., description="1 for upvote, -1 for downvote")


class VerifyPayload(BaseModel):
    action: str = Field(..., description="verified|rejected")


# ── Helpers ───────────────────────────────────────────────────────────────


def _submitter_from_token(token: str | None) -> str:
    """Extract the username (sub) from a JWT. Defaults to 'anonymous'."""
    if not token:
        return "anonymous"
    payload = decode_token(token) or {}
    return str(payload.get("sub") or "anonymous")


def _role_from_token(token: str | None) -> str:
    if not token:
        return "contributor"
    payload = decode_token(token) or {}
    return str(payload.get("role") or "contributor")


def _validate_submission(body: dict[str, Any] | IntelSubmission) -> IntelSubmission:
    """Coerce dict → IntelSubmission and validate enums."""
    if isinstance(body, IntelSubmission):
        sub = body
    else:
        sub = IntelSubmission(**body)
    if sub.intel_type not in VALID_TYPES:
        raise HTTPException(400, f"intel_type must be one of {sorted(VALID_TYPES)}")
    if sub.confidence is not None and sub.confidence not in VALID_CONFIDENCE:
        raise HTTPException(400, f"confidence must be one of {sorted(VALID_CONFIDENCE)}")
    note = sub.note.strip()
    if not note:
        raise HTTPException(400, "note is required")
    if len(note) > NOTE_MAX:
        raise HTTPException(400, f"note too long (max {NOTE_MAX})")
    return sub


def _check_rate_limit(engine, user_id: str) -> None:
    """Raise 429 if user has submitted > RATE_LIMIT_PER_HOUR in last hour."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT COUNT(*) FROM user_intel "
                "WHERE submitted_by = :u "
                "AND submitted_at >= NOW() - INTERVAL '1 hour'"
            ).bindparams(u=user_id)
        )
        count = int(result.scalar() or 0)
    if count >= RATE_LIMIT_PER_HOUR:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Rate limit exceeded ({RATE_LIMIT_PER_HOUR}/hour). Try again later.",
        )


def _row_to_dict(row: Any) -> dict[str, Any]:
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    for k, v in list(d.items()):
        if isinstance(v, datetime):
            d[k] = v.isoformat()
    d["score"] = int(d.get("upvotes") or 0) - int(d.get("downvotes") or 0)
    return d


# ── Core functions (callable directly for scripts/tests) ──────────────────


async def submit_intel(
    actor_id: str,
    body: dict[str, Any] | IntelSubmission,
    submitted_by: str,
) -> dict[str, Any]:
    """Insert a new intel submission. Rate-limited per submitter."""
    sub = _validate_submission(body)
    if not actor_id or not actor_id.strip():
        raise HTTPException(400, "actor_id is required")
    if not submitted_by or not submitted_by.strip():
        raise HTTPException(401, "submitted_by required")

    engine = get_db_engine()
    _check_rate_limit(engine, submitted_by)

    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO user_intel
                    (actor_id, intel_type, note, source_url, confidence,
                     submitted_by, verification_status)
                VALUES
                    (:actor_id, :intel_type, :note, :source_url, :confidence,
                     :submitted_by, 'pending')
                RETURNING id, actor_id, intel_type, note, source_url, confidence,
                          submitted_by, submitted_at, verification_status,
                          upvotes, downvotes, flags
                """
            ).bindparams(
                actor_id=actor_id.strip(),
                intel_type=sub.intel_type,
                note=sub.note.strip(),
                source_url=(sub.source_url or None),
                confidence=sub.confidence,
                submitted_by=submitted_by,
            )
        ).first()
    if row is None:
        raise HTTPException(500, "Insert failed")
    log.info(
        "user_intel submitted: actor={a} type={t} by={u}",
        a=actor_id,
        t=sub.intel_type,
        u=submitted_by,
    )
    return _row_to_dict(row)


async def get_actor_intel(
    actor_id: str,
    limit: int = 50,
    viewer_id: str = "",
) -> list[dict[str, Any]]:
    """Return intel for an actor, ordered by score (upvotes - downvotes) DESC."""
    if not actor_id or not actor_id.strip():
        raise HTTPException(400, "actor_id is required")
    limit = max(1, min(int(limit or 50), 200))
    engine = get_db_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, actor_id, intel_type, note, source_url, confidence,
                       submitted_by, submitted_at, verified_by, verified_at,
                       verification_status, upvotes, downvotes, flags
                FROM user_intel
                WHERE actor_id = :a
                  AND verification_status <> 'rejected'
                ORDER BY (upvotes - downvotes) DESC, submitted_at DESC
                LIMIT :lim
                """
            ).bindparams(a=actor_id.strip(), lim=limit)
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


async def vote_intel(intel_id: int, vote: int, user_id: str) -> dict[str, Any]:
    """Cast or update a vote. UNIQUE constraint dedups one vote per user."""
    if vote not in (-1, 1):
        raise HTTPException(400, "vote must be -1 or 1")
    if not user_id:
        raise HTTPException(401, "user_id required")
    engine = get_db_engine()
    with engine.begin() as conn:
        # Upsert vote; ON CONFLICT handles re-vote from same user.
        existing = conn.execute(
            text(
                "SELECT vote FROM user_intel_votes WHERE intel_id = :i AND user_id = :u"
            ).bindparams(i=intel_id, u=user_id)
        ).scalar()

        conn.execute(
            text(
                """
                INSERT INTO user_intel_votes (intel_id, user_id, vote)
                VALUES (:i, :u, :v)
                ON CONFLICT (intel_id, user_id)
                DO UPDATE SET vote = EXCLUDED.vote, voted_at = NOW()
                """
            ).bindparams(i=intel_id, u=user_id, v=vote)
        )

        # Re-aggregate from ledger (source of truth) to keep counts consistent.
        agg = conn.execute(
            text(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN vote = 1 THEN 1 ELSE 0 END), 0) AS up,
                    COALESCE(SUM(CASE WHEN vote = -1 THEN 1 ELSE 0 END), 0) AS dn
                FROM user_intel_votes
                WHERE intel_id = :i
                """
            ).bindparams(i=intel_id)
        ).first()
        up = int(agg.up) if agg else 0
        dn = int(agg.dn) if agg else 0

        conn.execute(
            text(
                "UPDATE user_intel SET upvotes = :up, downvotes = :dn WHERE id = :i"
            ).bindparams(up=up, dn=dn, i=intel_id)
        )
    return {
        "intel_id": intel_id,
        "upvotes": up,
        "downvotes": dn,
        "score": up - dn,
        "previous_vote": existing,
        "current_vote": vote,
    }


async def flag_intel(intel_id: int, user_id: str) -> dict[str, Any]:
    """Increment flag counter. (No ledger — flagging is lightweight.)"""
    if not user_id:
        raise HTTPException(401, "user_id required")
    engine = get_db_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                "UPDATE user_intel SET flags = flags + 1 "
                "WHERE id = :i RETURNING id, flags"
            ).bindparams(i=intel_id)
        ).first()
    if row is None:
        raise HTTPException(404, f"intel {intel_id} not found")
    return {"intel_id": row.id, "flags": int(row.flags), "status": "flagged"}


async def verify_intel(
    intel_id: int,
    action: str,
    verifier: str,
) -> dict[str, Any]:
    """Admin: mark intel as verified or rejected."""
    if action not in ("verified", "rejected"):
        raise HTTPException(400, "action must be 'verified' or 'rejected'")
    if not verifier:
        raise HTTPException(401, "verifier required")
    engine = get_db_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                UPDATE user_intel
                SET verification_status = :status,
                    verified_by = :verifier,
                    verified_at = NOW()
                WHERE id = :i
                RETURNING id, verification_status, verified_by, verified_at
                """
            ).bindparams(status=action, verifier=verifier, i=intel_id)
        ).first()
    if row is None:
        raise HTTPException(404, f"intel {intel_id} not found")
    return {
        "intel_id": row.id,
        "verification_status": row.verification_status,
        "verified_by": row.verified_by,
        "verified_at": (
            row.verified_at.isoformat() if row.verified_at else None
        ),
    }


async def list_pending_intel(limit: int = 100) -> list[dict[str, Any]]:
    """Admin: list pending intel for moderation queue."""
    limit = max(1, min(int(limit or 100), 500))
    engine = get_db_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, actor_id, intel_type, note, source_url, confidence,
                       submitted_by, submitted_at, verified_by, verified_at,
                       verification_status, upvotes, downvotes, flags
                FROM user_intel
                WHERE verification_status = 'pending'
                ORDER BY submitted_at DESC
                LIMIT :lim
                """
            ).bindparams(lim=limit)
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


# ── HTTP endpoints ────────────────────────────────────────────────────────


@router.post("/actors/{actor_id}/intel")
async def http_submit_intel(
    actor_id: str = Path(...),
    body: IntelSubmission = Body(...),
    token: str = Depends(require_auth),
) -> dict[str, Any]:
    submitter = _submitter_from_token(token)
    return await submit_intel(actor_id, body, submitter)


@router.get("/actors/{actor_id}/intel")
async def http_get_actor_intel(
    actor_id: str = Path(...),
    limit: int = Query(50, ge=1, le=200),
    token: str = Depends(require_auth),
) -> list[dict[str, Any]]:
    viewer = _submitter_from_token(token)
    return await get_actor_intel(actor_id, limit=limit, viewer_id=viewer)


@router.post("/intel/{intel_id}/vote")
async def http_vote_intel(
    intel_id: int = Path(..., ge=1),
    body: VotePayload = Body(...),
    token: str = Depends(require_auth),
) -> dict[str, Any]:
    user_id = _submitter_from_token(token)
    return await vote_intel(intel_id, body.vote, user_id)


@router.post("/intel/{intel_id}/flag")
async def http_flag_intel(
    intel_id: int = Path(..., ge=1),
    token: str = Depends(require_auth),
) -> dict[str, Any]:
    user_id = _submitter_from_token(token)
    return await flag_intel(intel_id, user_id)


@router.post("/intel/{intel_id}/verify")
async def http_verify_intel(
    intel_id: int = Path(..., ge=1),
    body: VerifyPayload = Body(...),
    _token: str = Depends(require_role("admin")),
) -> dict[str, Any]:
    verifier = _submitter_from_token(_token)
    return await verify_intel(intel_id, body.action, verifier)


@router.get("/intel/pending")
async def http_list_pending(
    limit: int = Query(100, ge=1, le=500),
    _token: str = Depends(require_role("admin")),
) -> list[dict[str, Any]]:
    return await list_pending_intel(limit=limit)
