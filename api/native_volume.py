"""Private read-only native volume API factory. Explicit wiring only, no fetches."""
from datetime import datetime, timezone
from ingestion.realtime.research_intervals import _utc
from ingestion.realtime.native_volume import retained_volume_view


def build_router(*, loader=None, calendar=None, authorize=None, clock=None):
    from fastapi import APIRouter, Depends, HTTPException, Query
    from fastapi.encoders import jsonable_encoder
    from fastapi.responses import JSONResponse
    if authorize is None:
        from api.auth import require_role
        authorize = require_role('admin')
    clock = clock or (lambda: datetime.now(timezone.utc))
    router = APIRouter(prefix='/api/v1/research/native-volume', dependencies=[Depends(authorize)])
    headers = {'Cache-Control':'no-store, private', 'X-GRID-Audience':'private'}

    @router.get('')
    def read(symbol: str = Query(pattern=r'^[A-Z0-9.$^/-]{1,12}$'), session_key: str = Query(min_length=1,max_length=80), as_of: datetime | None = None):
        now = _utc(clock())
        try:
            cutoff = _utc(as_of) if as_of is not None else now
        except ValueError:
            raise HTTPException(422,'timezone required') from None
        if cutoff > now:
            raise HTTPException(422,'future cutoff')
        if loader is None or calendar is None:
            return JSONResponse({'status':'unavailable','reason':'retained_store_or_calendar_not_configured'},503,headers=headers)
        try:
            result = retained_volume_view(loader,calendar,session_key=session_key,symbol=symbol,source='schwab_pricehistory',price_basis='provider_unspecified',as_of=cutoff)
            result['served_at'] = now
            result['historical_request'] = (now-cutoff).total_seconds()>120
        except ValueError:
            return JSONResponse({'status':'unavailable','reason':'invalid_provenance_or_session'},422,headers=headers)
        except Exception:
            return JSONResponse({'status':'unavailable','reason':'retained_read_failed'},503,headers=headers)
        return JSONResponse(jsonable_encoder(result),headers=headers)
    return router
