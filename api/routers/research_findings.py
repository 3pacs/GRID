"""GRID API — Research Desk findings (analysis/research_findings.py).

  GET /api/v1/research/findings — the latest catalogue's findings, filtered and paginated
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine
from analysis.research_findings import AREAS, VERDICTS, current_findings

router = APIRouter(prefix="/api/v1/research", tags=["research"], dependencies=[Depends(require_auth)])


@router.get("/findings")
def list_findings(
    family: str | None = Query(default=None, max_length=80),
    area: str | None = Query(default=None, max_length=40),
    verdict: str | None = Query(default=None, max_length=20),
    q: str | None = Query(default=None, max_length=200, description="words in the question, rule or family"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0, le=100_000),
) -> dict[str, Any]:
    """What has been tested, with verdicts, effects and intervals as recorded."""
    if area and area not in AREAS:
        raise HTTPException(status_code=422, detail=f"area must be one of {list(AREAS)}")
    if verdict and verdict not in VERDICTS:
        raise HTTPException(status_code=422, detail=f"verdict must be one of {list(VERDICTS)}")
    try:
        entries, total = current_findings(get_db_engine(), family=family, area=area, verdict=verdict, q=q,
                                          limit=limit, offset=offset)
    except Exception as exc:  # noqa: BLE001 - the table may not exist before the first load
        log.warning("research findings read failed: {e}", e=str(exc))
        raise HTTPException(status_code=503, detail="research findings not readable") from exc
    return {"entries": entries, "total": total, "limit": limit, "offset": offset, "has_more": offset + len(entries) < total}
