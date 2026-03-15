"""System configuration endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from config import settings

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
    updates: dict[str, Any] = {}
    for key, value in body.items():
        if key in allowed_fields:
            updates[key] = value

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
    updates["id"] = source_id

    with engine.begin() as conn:
        result = conn.execute(
            text(f"UPDATE source_catalog SET {set_clauses} WHERE id = :id"),
            updates,
        )
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="Source not found")

    return {"status": "updated", "source_id": source_id, "fields": list(updates.keys())}
