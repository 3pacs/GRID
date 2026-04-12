"""FastAPI router for contracts infrastructure endpoints.

Endpoints:
    GET  /api/v1/contracts/metrics
    GET  /api/v1/contracts/lineage/{correlation_id}
    POST /api/v1/contracts/dead-letter/{entry_id}/replay
"""
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from contracts import observability as obs
from contracts.replay import _load_filtered, replay_entry


router = APIRouter(prefix="/api/v1/contracts", tags=["contracts"])


@router.get("/metrics")
def contracts_metrics(user=Depends(require_auth)) -> Response:
    """Prometheus text-format metrics for the contracts layer."""
    return Response(
        content=obs.render_prometheus(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@router.get("/lineage/{correlation_id}")
def contracts_lineage(correlation_id: UUID, user=Depends(require_auth)) -> dict:
    """Full emission history for a given correlation id."""
    engine = get_db_engine()
    sql = text(
        """
        SELECT event_id, contract_type, producer_module,
               emitted_at, dispatched_to, payload_hash
        FROM contracts_audit
        WHERE correlation_id = :cid
        ORDER BY emitted_at
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql.bindparams(cid=str(correlation_id))).fetchall()
    return {
        "events": [
            {
                "event_id": str(r[0]),
                "contract_type": r[1],
                "producer_module": r[2],
                "emitted_at": r[3].isoformat() if r[3] else None,
                "dispatched_to": list(r[4]) if r[4] else [],
                "payload_hash": r[5],
            }
            for r in rows
        ]
    }


@router.post("/dead-letter/{entry_id}/replay")
def contracts_dead_letter_replay(
    entry_id: int, user=Depends(require_auth)
) -> dict:
    """Manually replay a single dead-letter entry."""
    engine = get_db_engine()
    entries = _load_filtered(engine, contract_type=None, limit=1000)
    match = [e for e in entries if e.id == entry_id]
    if not match:
        raise HTTPException(
            status_code=404, detail=f"dead-letter entry {entry_id} not found"
        )
    ok = replay_entry(engine, match[0])
    return {"success": 1 if ok else 0, "failed": 0 if ok else 1}
