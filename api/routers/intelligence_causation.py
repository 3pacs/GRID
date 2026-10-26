"""Intelligence sub-router: persisted causal links for the Timeline view.

GET /intelligence/causal-links?ticker=<ticker>&days=90 returns the edges the
scheduled job (``scripts/run_causal_links.py`` -> ``intelligence.causal_links``)
persisted: a public event on the ticker (the arrow's start, ``cause_date`` =
event date) that was knowable before an actor traded it (the arrow's end,
``effect_date`` = trade date). Every link carries ``known_at``, the run that
wrote it and the code sha; the payload carries the finished run's ``as_of``.

SELECT-only. Nothing is computed or invented here: the old route derived an
"effect" two days after the trade and a "Price reaction following ..."
description that no data supported.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["intelligence"])


@router.get("/causal-links")
def get_causal_links(
    ticker: str = Query(..., min_length=1, max_length=10, description="Ticker symbol"),
    days: int = Query(90, ge=1, le=730, description="Lookback window in days (trade date)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return persisted preceding-event links with as-of labels."""
    from intelligence.causal_links import read_links_payload

    ticker_upper = ticker.strip().upper()
    try:
        payload = read_links_payload(get_db_engine(), ticker=ticker_upper, days=days, limit=200)
    except Exception as exc:
        log.warning("Causal links read failed for {t}: {e}", t=ticker_upper, e=str(exc))
        return {
            "links": [], "ticker": ticker_upper, "days": days, "generated": False,
            "as_of": None, "last_run": None, "error": "causal links unavailable",
        }
    return {"ticker": ticker_upper, "days": days, **payload}
