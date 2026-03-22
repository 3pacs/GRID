"""Model registry endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_model_registry
from api.schemas.models import ModelTransitionRequest

router = APIRouter(prefix="/api/v1/models", tags=["models"])


def _model_row_to_dict(row: Any) -> dict:
    """Convert a model registry row to a serializable dict."""
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    for key in ("created_at", "promoted_at", "retired_at", "updated_at"):
        if d.get(key) is not None:
            d[key] = str(d[key])
    return d


@router.get("")
async def get_all(
    layer: str | None = Query(default=None),
    state: str | None = Query(default=None),
    _token: str = Depends(require_auth),
) -> dict:
    """Return all models with optional filters."""
    engine = get_db_engine()

    query = "SELECT * FROM model_registry WHERE 1=1"
    params: dict[str, Any] = {}

    if layer:
        query += " AND layer = :layer"
        params["layer"] = layer
    if state:
        query += " AND state = :state"
        params["state"] = state

    query += " ORDER BY created_at DESC"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    return {"models": [_model_row_to_dict(r) for r in rows]}


@router.get("/production")
async def get_production(
    _token: str = Depends(require_auth),
) -> dict:
    """Return current production model for each layer."""
    registry = get_model_registry()
    result: dict[str, Any] = {}
    for layer in ("REGIME", "TACTICAL", "EXECUTION"):
        model = registry.get_production_model(layer)
        if model:
            for key in ("created_at", "promoted_at", "retired_at", "updated_at"):
                if model.get(key) is not None:
                    model[key] = str(model[key])
        result[layer] = model
    return {"models": result}


@router.get("/{model_id}")
async def get_one(
    model_id: int,
    _token: str = Depends(require_auth),
) -> dict:
    """Return a single model with full details."""
    engine = get_db_engine()

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM model_registry WHERE id = :id"),
            {"id": model_id},
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="Model not found")

    model = _model_row_to_dict(row)

    # Attach validation results (using same connection to avoid N+1)
    with engine.connect() as conn:
        val_rows = conn.execute(
            text(
                "SELECT * FROM validation_results "
                "WHERE model_version_id = :mid ORDER BY run_timestamp DESC"
            ),
            {"mid": model_id},
        ).fetchall()

    validations = []
    for vr in val_rows:
        vd = dict(vr._mapping) if hasattr(vr, "_mapping") else dict(vr)
        for key in ("created_at",):
            if vd.get(key) is not None:
                vd[key] = str(vd[key])
        validations.append(vd)

    model["validation_results"] = validations
    return model


@router.post("/{model_id}/transition")
async def transition_model(
    model_id: int,
    body: ModelTransitionRequest,
    _token: str = Depends(require_auth),
) -> dict:
    """Transition a model to a new state."""
    registry = get_model_registry()
    try:
        registry.transition(
            model_id=model_id,
            new_state=body.new_state,
            operator_id="api-operator",
            reason=body.reason or None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return {"status": "transitioned", "model_id": model_id, "new_state": body.new_state}


@router.post("/{model_id}/rollback")
async def rollback_model(
    model_id: int,
    _token: str = Depends(require_auth),
) -> dict:
    """Rollback a model."""
    registry = get_model_registry()
    try:
        registry.rollback(model_id=model_id, operator_id="api-operator")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return {"status": "rolled_back", "model_id": model_id}
