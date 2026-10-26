"""God view API (slice G8): read-only, point-in-time pillar summary and history.

Deliberately named ``godview.py``, never ``god_view.py``: untracked incident
files named ``api/routers/god_view.py`` still sit in both deployed trees on
grid-svr, and a tracked file at that path would collide with them on
``git checkout`` (plan finding 6).

Routes (all behind ``require_auth``, all SELECT-only):

* ``GET /api/v1/godview/latest?as_of=`` — one payload per pillar
  (``fed_liquidity``, ``cftc``, ``dealer_gex``) with ``status``
  (available / stale / partial / unavailable), ``reason``, ``as_of`` (the
  data's own observation date), ``release_at``, ``available_at``,
  ``availability_basis``, ``provenance``, the latest finished writer run,
  and the values. An unavailable pillar has ``data: null``.
* ``GET /api/v1/godview/history?pillar=&from=&to=&market=&as_of=&limit=&offset=``
  — provenance rows of one pillar, paginated (``total``/``entries``/
  ``limit``/``offset``/``has_more``).

The business logic lives in ``godview/read_model.py``. This module only
validates parameters, opens a connection and maps a database failure to 503.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy.exc import SQLAlchemyError

from api.auth import require_auth
from api.dependencies import get_db_engine
from godview import read_model

router = APIRouter(
    prefix="/api/v1/godview",
    tags=["godview"],
    dependencies=[Depends(require_auth)],
)

PillarName = Literal["fed_liquidity", "cftc", "dealer_gex"]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _as_of_or_422(raw: str | None) -> tuple[datetime, str]:
    try:
        return read_model.resolve_as_of(raw, now=_now())
    except read_model.AsOfError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def _store_unavailable(exc: Exception) -> HTTPException:
    log.warning("godview read failed: {e}", e=type(exc).__name__)
    return HTTPException(status_code=503, detail="godview_store_unavailable")


@router.get("/latest")
def get_godview_latest(
    as_of: Annotated[
        str | None,
        Query(description="ISO date (end of that ET day) or ISO datetime with offset; default now"),
    ] = None,
) -> dict[str, Any]:
    """Latest provenance row per pillar that was available at ``as_of``."""
    cutoff, source = _as_of_or_422(as_of)
    try:
        with get_db_engine().connect() as conn:
            return read_model.read_latest(conn, as_of=cutoff, as_of_source=source)
    except SQLAlchemyError as exc:
        raise _store_unavailable(exc) from exc


@router.get("/history")
def get_godview_history(
    pillar: Annotated[PillarName, Query(description="fed_liquidity | cftc | dealer_gex")],
    date_from: Annotated[date | None, Query(alias="from", description="first observation date (default to - 365 d)")] = None,
    date_to: Annotated[date | None, Query(alias="to", description="last observation date (default the as_of ET date)")] = None,
    market: Annotated[
        str | None,
        Query(pattern=r"^[A-Z]{1,3}$", description="CFTC root symbol (e.g. ES); cftc pillar only"),
    ] = None,
    as_of: Annotated[str | None, Query(description="as for /latest")] = None,
    limit: Annotated[int, Query(ge=1, le=read_model.HISTORY_MAX_LIMIT)] = read_model.HISTORY_DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, le=1_000_000)] = 0,
) -> dict[str, Any]:
    """Provenance rows of one pillar with an observation date in [from, to], PIT at ``as_of``."""
    cutoff, _source = _as_of_or_422(as_of)
    if market is not None:
        if pillar != read_model.PILLAR_CFTC:
            raise HTTPException(status_code=422, detail="market applies to the cftc pillar only")
        if market not in read_model.CFTC_TRACKED_ROOTS:
            raise HTTPException(status_code=422, detail=f"market must be one of {sorted(read_model.CFTC_TRACKED_ROOTS)}")
    to_day = date_to or read_model.et_date(cutoff)
    from_day = date_from or (to_day - timedelta(days=read_model.HISTORY_DEFAULT_DAYS))
    if from_day > to_day:
        raise HTTPException(status_code=422, detail="from must be on or before to")
    try:
        with get_db_engine().connect() as conn:
            return read_model.read_history(
                conn,
                pillar=pillar,
                date_from=from_day,
                date_to=to_day,
                as_of=cutoff,
                market=market,
                limit=limit,
                offset=offset,
            )
    except SQLAlchemyError as exc:
        raise _store_unavailable(exc) from exc
