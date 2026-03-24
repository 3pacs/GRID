"""
GRID API — Knowledge tree endpoints.

Provides search, summary, and CRUD access to the institutional
knowledge base built from LLM Q&A interactions.

  GET    /api/v1/knowledge          — Search knowledge tree
  GET    /api/v1/knowledge/summary  — Stats and recent topics
  GET    /api/v1/knowledge/{id}     — Get specific entry + related
  DELETE /api/v1/knowledge/{id}     — Admin: remove bad entries
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log

from api.auth import require_auth

router = APIRouter(
    prefix="/api/v1/knowledge",
    tags=["knowledge"],
    dependencies=[Depends(require_auth)],
)


@router.get("")
async def search_knowledge_endpoint(
    q: str = Query(default="", description="Search query"),
    category: str = Query(default="", description="Filter by category"),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Search the knowledge tree with optional category filter."""
    from knowledge.tree import search_knowledge

    cat = category if category else None
    query = q if q else None

    result = search_knowledge(query=query or "", category=cat, limit=limit, offset=offset)
    return result


@router.get("/summary")
async def knowledge_summary() -> dict[str, Any]:
    """Get knowledge tree statistics and recent topics."""
    from knowledge.tree import get_knowledge_summary

    return get_knowledge_summary()


@router.get("/{entry_id}")
async def get_knowledge_entry(entry_id: int) -> dict[str, Any]:
    """Get a specific knowledge tree entry with related entries."""
    from knowledge.tree import get_entry_by_id, get_related

    entry = get_entry_by_id(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="Knowledge entry not found")

    # Find related entries
    related = []
    try:
        related_raw = get_related(entry["question"], limit=5)
        related = [
            {"id": r["id"], "question": r["question"], "category": r.get("category")}
            for r in related_raw
            if r.get("id") != entry_id
        ]
    except Exception as exc:
        log.debug("Related knowledge lookup failed: {e}", e=str(exc))

    return {"entry": entry, "related": related}


@router.delete("/{entry_id}")
async def delete_knowledge_entry(entry_id: int) -> dict[str, Any]:
    """Delete a knowledge tree entry (admin only)."""
    from knowledge.tree import delete_entry

    deleted = delete_entry(entry_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Knowledge entry not found")

    return {"deleted": True, "id": entry_id}
