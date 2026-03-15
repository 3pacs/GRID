"""Regime state endpoints."""

from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store
from api.schemas.regime import (
    RegimeCurrentResponse,
    RegimeDriver,
    RegimeHistoryEntry,
    RegimeHistoryResponse,
    RegimeTransition,
    RegimeTransitionsResponse,
)

router = APIRouter(prefix="/api/v1/regime", tags=["regime"])


@router.get("/current", response_model=RegimeCurrentResponse)
async def get_current(_token: str = Depends(require_auth)) -> RegimeCurrentResponse:
    """Return current inferred regime state."""
    engine = get_db_engine()

    # Check for production model
    with engine.connect() as conn:
        prod = conn.execute(
            text(
                "SELECT id, name, version FROM model_registry "
                "WHERE state = 'PRODUCTION' AND layer = 'REGIME' LIMIT 1"
            )
        ).fetchone()

    if prod is None:
        return RegimeCurrentResponse(state="UNCALIBRATED")

    # Get latest journal entry for regime state
    with engine.connect() as conn:
        latest = conn.execute(
            text(
                "SELECT inferred_state, state_confidence, transition_probability, "
                "contradiction_flags, grid_recommendation, baseline_recommendation, "
                "decision_timestamp "
                "FROM decision_journal "
                "WHERE model_version_id = :mid "
                "ORDER BY decision_timestamp DESC LIMIT 1"
            ),
            {"mid": prod[0]},
        ).fetchone()

    if latest is None:
        return RegimeCurrentResponse(
            state="UNCALIBRATED",
            model_version=f"{prod[1]} v{prod[2]}",
        )

    flags = latest[3] if isinstance(latest[3], dict) else {}
    contradiction_list = [f"{k}: {v}" for k, v in flags.items()] if flags else []

    return RegimeCurrentResponse(
        state=latest[0],
        confidence=float(latest[1]),
        transition_probability=float(latest[2]),
        contradiction_flags=contradiction_list,
        model_version=f"{prod[1]} v{prod[2]}",
        as_of=latest[6].isoformat() if latest[6] else "",
        baseline_comparison=latest[5] or "",
    )


@router.get("/history", response_model=RegimeHistoryResponse)
async def get_history(
    days: int = Query(default=90, ge=1, le=365),
    _token: str = Depends(require_auth),
) -> RegimeHistoryResponse:
    """Return regime history."""
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT DATE(decision_timestamp) AS dt, "
                "inferred_state, state_confidence "
                "FROM decision_journal "
                "WHERE decision_timestamp >= NOW() - INTERVAL '{days} days' "
                "ORDER BY decision_timestamp".format(days=days)
            )
        ).fetchall()

    history = [
        RegimeHistoryEntry(
            date=str(row[0]),
            state=row[1],
            confidence=float(row[2]),
        )
        for row in rows
    ]

    return RegimeHistoryResponse(history=history)


@router.get("/transitions", response_model=RegimeTransitionsResponse)
async def get_transitions(
    _token: str = Depends(require_auth),
) -> RegimeTransitionsResponse:
    """Return all detected regime transitions."""
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT decision_timestamp, inferred_state, state_confidence "
                "FROM decision_journal "
                "ORDER BY decision_timestamp"
            )
        ).fetchall()

    transitions: list[RegimeTransition] = []
    for i in range(1, len(rows)):
        if rows[i][1] != rows[i - 1][1]:
            transitions.append(
                RegimeTransition(
                    date=str(rows[i][0]),
                    from_state=rows[i - 1][1],
                    to_state=rows[i][1],
                    confidence=float(rows[i][2]),
                )
            )

    return RegimeTransitionsResponse(transitions=transitions)
