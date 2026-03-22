"""Decision journal endpoints."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_journal
from api.schemas.journal import (
    JournalEntryCreate,
    JournalEntryResponse,
    JournalOutcomeRecord,
    JournalStatsResponse,
)

router = APIRouter(prefix="/api/v1/journal", tags=["journal"])


def _row_to_response(row: Any) -> dict:
    """Convert a DB row to a journal entry dict."""
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    # Serialize datetime fields
    for key in ("decision_timestamp", "outcome_recorded_at"):
        if d.get(key) is not None:
            d[key] = str(d[key])
        else:
            d[key] = None
    return d


@router.get("")
async def get_all(
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    verdict: str | None = Query(default=None),
    _token: str = Depends(require_auth),
) -> dict:
    """Return paginated journal entries."""
    engine = get_db_engine()

    query = "SELECT * FROM decision_journal"
    params: dict[str, Any] = {}

    if verdict:
        if verdict == "PENDING":
            query += " WHERE outcome_recorded_at IS NULL"
        else:
            query += " WHERE verdict = :verdict"
            params["verdict"] = verdict

    query += " ORDER BY decision_timestamp DESC LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset

    # Build matching COUNT query with same WHERE clause
    count_q = "SELECT COUNT(*) FROM decision_journal"
    count_params: dict[str, Any] = {}
    if verdict:
        if verdict == "PENDING":
            count_q += " WHERE outcome_recorded_at IS NULL"
        else:
            count_q += " WHERE verdict = :verdict"
            count_params["verdict"] = verdict

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()
        total = conn.execute(text(count_q), count_params).fetchone()[0]

    entries = [_row_to_response(row) for row in rows]
    return {"entries": entries, "total": total, "limit": limit, "offset": offset}


@router.get("/stats", response_model=JournalStatsResponse)
async def get_stats(
    _token: str = Depends(require_auth),
) -> JournalStatsResponse:
    """Return journal performance summary."""
    journal = get_journal()
    summary = journal.get_performance_summary()
    return JournalStatsResponse(**summary)


@router.get("/{entry_id}")
async def get_one(
    entry_id: int,
    _token: str = Depends(require_auth),
) -> dict:
    """Return a single journal entry."""
    engine = get_db_engine()

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM decision_journal WHERE id = :id"),
            {"id": entry_id},
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Journal entry not found")

    return _row_to_response(row)


@router.post("", status_code=201)
async def create(
    body: JournalEntryCreate,
    _token: str = Depends(require_auth),
) -> dict:
    """Create a new journal entry."""
    journal = get_journal()
    try:
        entry_id = journal.log_decision(
            model_version_id=body.model_version_id,
            inferred_state=body.inferred_state,
            state_confidence=body.state_confidence,
            transition_probability=body.transition_probability,
            contradiction_flags=body.contradiction_flags,
            grid_recommendation=body.grid_recommendation,
            baseline_recommendation=body.baseline_recommendation,
            action_taken=body.action_taken,
            counterfactual=body.counterfactual,
            operator_confidence=body.operator_confidence,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return {"id": entry_id, "status": "created"}


@router.put("/{entry_id}/outcome")
async def record_outcome(
    entry_id: int,
    body: JournalOutcomeRecord,
    _token: str = Depends(require_auth),
) -> dict:
    """Record outcome for an existing journal entry."""
    journal = get_journal()
    try:
        journal.record_outcome(
            decision_id=entry_id,
            outcome_value=body.outcome_value,
            verdict=body.verdict,
            annotation=body.annotation,
        )
    except ValueError as exc:
        error_msg = str(exc)
        if "already recorded" in error_msg.lower() or "immutable" in error_msg.lower():
            raise HTTPException(status_code=409, detail=error_msg)
        raise HTTPException(status_code=422, detail=error_msg)

    return {"status": "recorded", "entry_id": entry_id}
