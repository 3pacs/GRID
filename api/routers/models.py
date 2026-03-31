"""Model registry endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_model_registry
from api.schemas.models import ModelFromHypothesisRequest, ModelTransitionRequest

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
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
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

    query += " ORDER BY created_at DESC LIMIT :lim OFFSET :off"
    params["lim"] = limit
    params["off"] = offset

    count_query = "SELECT COUNT(*) FROM model_registry WHERE 1=1"
    count_params: dict[str, Any] = {}
    if layer:
        count_query += " AND layer = :layer"
        count_params["layer"] = layer
    if state:
        count_query += " AND state = :state"
        count_params["state"] = state

    with engine.connect() as conn:
        total: int = conn.execute(text(count_query), count_params).scalar_one()
        rows = conn.execute(text(query), params).fetchall()

    return {
        "models": [_model_row_to_dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


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
        # Single JOIN — fetches model and all validation rows in one round-trip.
        # model_registry columns are prefixed with "m_", validation_results with "v_".
        join_rows = conn.execute(
            text(
                "SELECT "
                "  mr.*, "
                "  vr.id            AS v_id, "
                "  vr.created_at    AS v_created_at, "
                "  vr.overall_verdict AS v_overall_verdict, "
                "  vr.metrics       AS v_metrics, "
                "  vr.run_timestamp AS v_run_timestamp, "
                "  vr.hypothesis_id AS v_hypothesis_id "
                "FROM model_registry mr "
                "LEFT JOIN validation_results vr "
                "  ON vr.model_version_id = mr.id "
                "WHERE mr.id = :id "
                "ORDER BY vr.created_at DESC"
            ),
            {"id": model_id},
        ).fetchall()

    if not join_rows:
        raise HTTPException(status_code=404, detail="Model not found")

    # First row carries the model columns; subsequent rows repeat them.
    model = _model_row_to_dict(join_rows[0])

    # Strip the v_ prefixed keys from the model dict — they belong to validations.
    v_keys = [k for k in model if k.startswith("v_")]
    for k in v_keys:
        model.pop(k)

    # Collect validation rows (LEFT JOIN produces one NULL row when there are none).
    validations = []
    for row in join_rows:
        rd = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
        if rd.get("v_id") is None:
            continue
        vd = {
            "id": rd["v_id"],
            "created_at": str(rd["v_created_at"]) if rd.get("v_created_at") else None,
            "overall_verdict": rd["v_overall_verdict"],
            "metrics": rd["v_metrics"],
            "run_timestamp": rd["v_run_timestamp"],
            "hypothesis_id": rd["v_hypothesis_id"],
        }
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


@router.post("/from-hypothesis/{hypothesis_id}")
async def create_from_hypothesis(
    hypothesis_id: int,
    body: ModelFromHypothesisRequest | None = None,
    _token: str = Depends(require_auth),
) -> dict:
    """Create a CANDIDATE model from a PASSED hypothesis.

    Reads the hypothesis and its most recent PASS validation result,
    then inserts a new model_registry row in CANDIDATE state.
    """
    engine = get_db_engine()

    with engine.connect() as conn:
        hyp = conn.execute(
            text(
                "SELECT id, statement, layer, feature_ids, lag_structure, "
                "proposed_metric, proposed_threshold, state "
                "FROM hypothesis_registry WHERE id = :hid"
            ),
            {"hid": hypothesis_id},
        ).fetchone()

        if hyp is None:
            raise HTTPException(status_code=404, detail="Hypothesis not found")

        hyp_map = dict(hyp._mapping)
        if hyp_map["state"] != "PASSED":
            raise HTTPException(
                status_code=422,
                detail=f"Hypothesis is in state '{hyp_map['state']}', not PASSED",
            )

        # Find the most recent PASS validation result for this hypothesis
        val_row = conn.execute(
            text(
                "SELECT id FROM validation_results "
                "WHERE hypothesis_id = :hid AND overall_verdict = 'PASS' "
                "ORDER BY run_timestamp DESC LIMIT 1"
            ),
            {"hid": hypothesis_id},
        ).fetchone()

    validation_run_id = val_row[0] if val_row else None

    # Build model name and version
    name = body.name if body and body.name else f"hyp-{hypothesis_id}-{hyp_map['layer'].lower()}"
    version = body.version if body and body.version else datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    # Build parameter snapshot from hypothesis
    import json

    lag_structure = hyp_map["lag_structure"]
    if isinstance(lag_structure, str):
        lag_structure = json.loads(lag_structure)
    parameter_snapshot = {
        "proposed_metric": hyp_map["proposed_metric"],
        "proposed_threshold": hyp_map["proposed_threshold"],
        "lag_structure": lag_structure,
    }

    with engine.begin() as conn:
        row = conn.execute(
            text(
                "INSERT INTO model_registry "
                "(name, layer, version, state, hypothesis_id, validation_run_id, "
                " feature_set, parameter_snapshot) "
                "VALUES (:name, :layer, :version, 'CANDIDATE', :hid, :vid, "
                " :fset, :params) "
                "RETURNING id"
            ),
            {
                "name": name,
                "layer": hyp_map["layer"],
                "version": version,
                "hid": hypothesis_id,
                "vid": validation_run_id,
                "fset": hyp_map["feature_ids"],
                "params": json.dumps(parameter_snapshot),
            },
        ).fetchone()

    model_id = row[0]
    log.info(
        "Model created from hypothesis — model_id={m}, hypothesis_id={h}, layer={l}",
        m=model_id, h=hypothesis_id, l=hyp_map["layer"],
    )

    return {
        "status": "created",
        "model_id": model_id,
        "hypothesis_id": hypothesis_id,
        "name": name,
        "version": version,
        "layer": hyp_map["layer"],
        "state": "CANDIDATE",
        "validation_run_id": validation_run_id,
    }


@router.get("/{model_id}/feature-importance")
async def get_feature_importance(
    model_id: int,
    _token: str = Depends(require_auth),
) -> dict:
    """Compute and return feature importance for a model.

    Returns a complete report combining permutation importance,
    regime correlation, and rolling stability metrics.
    """
    from features.importance import FeatureImportanceTracker
    from api.dependencies import get_pit_store

    engine = get_db_engine()
    pit_store = get_pit_store()
    tracker = FeatureImportanceTracker(db_engine=engine, pit_store=pit_store)

    report = tracker.get_importance_report(model_id=model_id)

    if "error" in report:
        raise HTTPException(status_code=404, detail=report["error"])

    return report
