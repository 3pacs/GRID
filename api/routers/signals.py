"""Live signals endpoints."""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


@router.get("")
async def get_signals(_token: str = Depends(require_auth)) -> dict:
    """Return live signals from the inference engine."""
    try:
        from inference.live import LiveInference

        engine = get_db_engine()
        pit = get_pit_store()
        li = LiveInference(engine, pit)
        result = li.run_inference()
        return {"signals": result}
    except Exception as exc:
        log.warning("Signal generation failed: {e}", e=str(exc))
        return {"signals": {"error": str(exc), "layers": {}}}


@router.get("/snapshot")
async def get_snapshot(_token: str = Depends(require_auth)) -> dict:
    """Return current feature snapshot."""
    try:
        from inference.live import LiveInference

        engine = get_db_engine()
        pit = get_pit_store()
        li = LiveInference(engine, pit)
        df = li.get_feature_snapshot()
        records = df.to_dict("records") if not df.empty else []
        return {"features": records, "count": len(records)}
    except Exception as exc:
        log.warning("Feature snapshot failed: {e}", e=str(exc))
        return {"features": [], "count": 0, "error": str(exc)}
