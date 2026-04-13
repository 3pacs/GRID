"""Trade tickets derived from contagion predictions.

Exposes the output of ``trading.contagion_to_ticket`` so the frontend can
render LEVER / CONDITION / THESIS / INVALIDATION trade cards underneath the
contagion sankey.  Every ticket combines:

  - a specific contagion prediction (``prediction_id``)
  - the downstream victim (ticker)
  - dealer gamma magnets (walls / max_pain / flip)
  - Kelly-sized position from historical contagion backtest accuracy
  - an immutable journal row id (when available)

Routes
------
- ``GET /api/v1/trade-tickets/recent?since_hours=24``
- ``GET /api/v1/contagion/{prediction_id}/tickets``
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine
from trading.contagion_to_ticket import (
    generate_tickets_for_prediction,
    generate_tickets_for_recent_predictions,
)

router = APIRouter(tags=["trade-tickets"])


@router.get("/api/v1/trade-tickets/recent")
async def recent_tickets(
    since_hours: int = Query(24, ge=1, le=720),
    write_journal: bool = Query(False),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return every ticket generated from contagion predictions in the window.

    Journaling defaults to ``False`` on the HTTP path — we don't want every
    dashboard refresh to flood ``decision_journal``.  The background worker
    that polls this endpoint should pass ``write_journal=true`` explicitly.
    """
    engine = get_db_engine()
    try:
        tickets = generate_tickets_for_recent_predictions(
            engine, since_hours=since_hours, journal=write_journal
        )
    except Exception as exc:
        log.warning("trade_tickets recent failed: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail="trade ticket generation failed")

    return {
        "tickets": tickets,
        "count": len(tickets),
        "since_hours": since_hours,
        "journaled": bool(write_journal),
    }


@router.get("/api/v1/contagion/{prediction_id}/tickets")
async def tickets_for_prediction(
    prediction_id: int = Path(..., ge=1),
    write_journal: bool = Query(False),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return tickets for one specific contagion prediction."""
    engine = get_db_engine()
    try:
        tickets = generate_tickets_for_prediction(
            engine, prediction_id=prediction_id, journal=write_journal
        )
    except Exception as exc:
        log.warning(
            "trade_tickets for prediction {p} failed: {e}",
            p=prediction_id, e=str(exc),
        )
        raise HTTPException(status_code=500, detail="trade ticket generation failed")

    return {
        "prediction_id": prediction_id,
        "tickets": tickets,
        "count": len(tickets),
        "journaled": bool(write_journal),
    }
