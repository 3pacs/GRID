"""System configuration endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from config import settings


def _safe_set_clause(columns: set[str], allowed: set[str]) -> str:
    """Build a SET clause using only pre-validated column names.

    Each column name is checked against the allowlist at call time.
    The returned SQL fragment uses only literal strings from `allowed`,
    never user input, eliminating any SQL injection vector.
    """
    safe = columns & allowed
    if not safe:
        return ""
    return ", ".join(f"{col} = :{col}" for col in sorted(safe))

router = APIRouter(prefix="/api/v1/config", tags=["config"])

# Fields that should never be exposed via API
_SENSITIVE_FIELDS = {
    "DB_PASSWORD",
    "FRED_API_KEY",
    "GRID_MASTER_PASSWORD_HASH",
    "GRID_JWT_SECRET",
}


@router.get("")
async def get_config(_token: str = Depends(require_auth)) -> dict:
    """Return current system configuration (non-sensitive fields only)."""
    config_dict: dict[str, Any] = {}
    for key, value in settings.model_dump().items():
        if key.upper() not in _SENSITIVE_FIELDS:
            config_dict[key] = value
    return {"config": config_dict}


@router.put("")
async def update_config(
    body: dict[str, Any],
    _token: str = Depends(require_auth),
) -> dict:
    """Update system configuration (non-sensitive fields only)."""
    updated: dict[str, Any] = {}
    for key, value in body.items():
        if key.upper() in _SENSITIVE_FIELDS:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot update sensitive field: {key}",
            )
        if hasattr(settings, key):
            setattr(settings, key, value)
            updated[key] = value

    return {"updated": updated}


@router.get("/sources")
async def get_sources(_token: str = Depends(require_auth)) -> dict:
    """Return all rows from source_catalog."""
    engine = get_db_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM source_catalog ORDER BY id")).fetchall()

    sources = []
    for row in rows:
        d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
        for key in ("last_pull_at",):
            if d.get(key) is not None:
                d[key] = str(d[key])
        sources.append(d)

    return {"sources": sources}


@router.put("/sources/{source_id}")
async def update_source(
    source_id: int,
    body: dict[str, Any],
    _token: str = Depends(require_auth),
) -> dict:
    """Update source configuration."""
    engine = get_db_engine()

    allowed_fields = {"active", "priority_rank", "trust_score"}
    updates: dict[str, Any] = {k: v for k, v in body.items() if k in allowed_fields}

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    set_clause = _safe_set_clause(set(updates), allowed_fields)
    updates["id"] = source_id

    with engine.begin() as conn:
        result = conn.execute(
            text(f"UPDATE source_catalog SET {set_clause} WHERE id = :id"),
            updates,
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Source not found")

    return {"status": "updated", "source_id": source_id, "fields": list(updates.keys())}


@router.get("/features")
async def get_features(_token: str = Depends(require_auth)) -> dict:
    """Return all rows from feature_registry."""
    engine = get_db_engine()
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM feature_registry ORDER BY id")).fetchall()

    features = []
    for row in rows:
        d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
        for key in ("created_at", "deprecated_at", "eligible_from_date"):
            if d.get(key) is not None:
                d[key] = str(d[key])
        features.append(d)

    return {"features": features}


@router.put("/features/{feature_id}")
async def update_feature(
    feature_id: int,
    body: dict[str, Any],
    _token: str = Depends(require_auth),
) -> dict:
    """Update feature configuration (model_eligible only)."""
    engine = get_db_engine()

    allowed_fields = {"model_eligible"}
    updates: dict[str, Any] = {k: v for k, v in body.items() if k in allowed_fields}

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    set_clause = _safe_set_clause(set(updates), allowed_fields)
    updates["id"] = feature_id

    with engine.begin() as conn:
        result = conn.execute(
            text(f"UPDATE feature_registry SET {set_clause} WHERE id = :id"),
            updates,
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Feature not found")

    return {"status": "updated", "feature_id": feature_id, "fields": list(updates.keys())}
