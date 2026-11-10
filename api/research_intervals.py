"""Private read-only interval service; not registered in api.main.

A deployment must explicitly supply a retained-minute loader and trusted exchange
calendar lookup. No network fallback, credentials, schedules or public projection.
"""

from __future__ import annotations
from dataclasses import asdict
from datetime import datetime, timezone
from ingestion.realtime.native_minutes import select_as_of
from ingestion.realtime.research_intervals import INTERVALS, rollup, _utc


def interval_view(
    rows, session, *, symbol, source, interval, as_of, price_basis="unadjusted"
):
    selected = select_as_of(
        rows,
        symbol=symbol,
        source=source,
        price_basis=price_basis,
        as_of=as_of,
        opens=session.opens,
        closes=session.closes,
    )
    out = rollup(selected, session, interval, as_of=as_of)
    complete = sum(b.complete for b in out)
    return {
        "schema": "grid.research.intervals.v1",
        "symbol": symbol,
        "source": source,
        "interval": interval,
        "price_basis": price_basis,
        "as_of": _utc(as_of),
        "session": asdict(session),
        "status": (
            "no_coverage"
            if not selected
            else "awaiting_closed_window"
            if not out
            else "complete"
            if complete == len(out)
            else "gapped"
        ),
        "closed_windows": len(out),
        "complete_windows": complete,
        "source_minutes_as_of": len(selected),
        "latest_event_at": max((b.ts for b in selected), default=None),
        "latest_receipt_at": max((b.available_at for b in selected), default=None),
        "bars": [asdict(b) for b in out],
        "live": False,  # historical/requested as-of coverage is not feed-health evidence
    }


def build_router(*, loader=None, calendar=None, authorize=None, clock=None):
    """Construct a read-only admin router; absent dependencies fail closed.

    Loader is a bounded local retained-data reader: (symbol, source, session,
    as_of) -> immutable revision rows. Calendar: (session_key) -> Session.
    Production must use the default existing GRID admin authorization.
    """
    from fastapi import APIRouter, Depends, HTTPException, Query
    from fastapi.encoders import jsonable_encoder
    from fastapi.responses import JSONResponse

    if authorize is None:
        from api.auth import require_role

        authorize = require_role("admin")
    clock = clock or (lambda: datetime.now(timezone.utc))
    router = APIRouter(
        prefix="/api/v1/research/intervals", dependencies=[Depends(authorize)]
    )
    headers = {"Cache-Control": "no-store, private", "X-GRID-Audience": "private"}

    @router.get("")
    def read(
        symbol: str = Query(pattern=r"^[A-Z0-9.$^/-]{1,12}$"),
        source: str = Query(pattern=r"^[a-z0-9_]{1,48}$"),
        session_key: str = Query(min_length=1, max_length=80),
        interval: str = Query(default="3m"),
        as_of: datetime = Query(),
        price_basis: str = Query(
            default="provider_unspecified",
            pattern=r"^(unadjusted|split_adjusted|provider_unspecified)$",
        ),
    ):
        if interval not in INTERVALS:
            raise HTTPException(422, "unsupported interval")
        try:
            cutoff = _utc(as_of)
        except ValueError:
            raise HTTPException(422, "timezone required") from None
        if cutoff > _utc(clock()):
            raise HTTPException(422, "future as_of is unavailable")
        if loader is None or calendar is None:
            return JSONResponse(
                {
                    "status": "unavailable",
                    "reason": "retained_store_or_calendar_not_configured",
                },
                503,
                headers=headers,
            )
        try:
            session = calendar(session_key)
            if session is None:
                raise ValueError("session unavailable")
            rows = loader(symbol, source, session, cutoff)
            result = interval_view(
                rows,
                session,
                symbol=symbol,
                source=source,
                interval=interval,
                as_of=cutoff,
                price_basis=price_basis,
            )
        except ValueError:
            return JSONResponse(
                {"status": "unavailable", "reason": "invalid_provenance_or_session"},
                422,
                headers=headers,
            )
        except Exception:
            return JSONResponse(
                {"status": "unavailable", "reason": "retained_read_failed"},
                503,
                headers=headers,
            )
        return JSONResponse(jsonable_encoder(result), headers=headers)

    return router
