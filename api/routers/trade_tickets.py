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

import math
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine
from trading.contagion_to_ticket import (
    generate_tickets_for_prediction,
    generate_tickets_for_recent_predictions,
)
from trading.options_recommender import MAX_KELLY_PER_TICKET

router = APIRouter(tags=["trade-tickets"])


def _finite_in_range(value: Any, low: float, high: float) -> float | None:
    """``value`` as a float when it is a finite number inside [low, high], else None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    # Compare before converting: float(10**400) raises OverflowError, but an int compares exactly with
    # the float bounds, so an oversized integer is simply out of range.
    return float(value) if low <= value <= high else None


def _validated_ticket(ticket: dict[str, Any]) -> dict[str, Any]:
    """A copy of ``ticket`` whose confidence and kelly size are real values or absent (#959).

    The route returns plain dicts, so nothing bounded these fields: a NaN/Infinity accuracy (not valid
    JSON, so the response would 500) or an out-of-range number reached the page as a number. A
    confidence outside [0, 1] or a kelly size outside [0, MAX_KELLY_PER_TICKET] (or not a finite
    number) becomes None, which the page shows as not reported. A measured 0 is kept.
    """
    cleaned = dict(ticket)
    # Only fields the ticket carries are checked; an absent key stays absent (the page reads it as
    # not reported), so the response shape does not change for tickets without them.
    if "confidence" in cleaned:
        cleaned["confidence"] = _finite_in_range(cleaned["confidence"], 0.0, 1.0)
    if "kelly_size" in cleaned:
        cleaned["kelly_size"] = _finite_in_range(cleaned["kelly_size"], 0.0, float(MAX_KELLY_PER_TICKET))
    return cleaned


@router.get("/api/v1/trade-tickets/recent")
def recent_tickets(
    since_hours: int = Query(24, ge=1, le=720),
    limit: int | None = Query(None, ge=1, le=500),
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
            engine,
            since_hours=since_hours,
            journal=write_journal,
            limit=limit,
        )
    except Exception as exc:
        log.warning("trade_tickets recent failed: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail="trade ticket generation failed")

    tickets = [_validated_ticket(t) for t in tickets]
    return {
        "tickets": tickets,
        "count": len(tickets),
        "since_hours": since_hours,
        "limit": limit,
        "journaled": bool(write_journal),
    }


@router.get("/api/v1/contagion/{prediction_id}/tickets")
def tickets_for_prediction(
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

    tickets = [_validated_ticket(t) for t in tickets]
    return {
        "prediction_id": prediction_id,
        "tickets": tickets,
        "count": len(tickets),
        "journaled": bool(write_journal),
    }
