"""GRID API — agent report reads (intelligence/report_reader.py).

  GET /api/v1/agent-reports/reads     — read reports, newest first, filtered and paginated
  GET /api/v1/agent-reports/coverage  — per day: received, read, unread, unreadable
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from intelligence.report_reader import KINDS, coverage

router = APIRouter(prefix="/api/v1/agent-reports", tags=["agent-reports"], dependencies=[Depends(require_auth)])

_FILTERS = {"agent": "r.agent = :agent", "kind": "x.kind = :kind", "needs_anik": "x.needs_anik = :needs_anik"}


@router.get("/reads")
def list_reads(
    agent: str | None = Query(default=None, max_length=80),
    kind: str | None = Query(default=None, max_length=20),
    needs_anik: bool | None = Query(default=None),
    days: int = Query(default=7, ge=1, le=400),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0, le=100_000),
) -> dict[str, Any]:
    """What each report said, as read: summary, kind, open items, results."""
    if kind and kind not in KINDS:
        raise HTTPException(status_code=422, detail=f"kind must be one of {list(KINDS)}")
    where, params = ["r.date >= :since"], {"since": date.today() - timedelta(days=days)}
    for col, val in (("agent", agent), ("kind", kind), ("needs_anik", needs_anik)):
        if val is not None:
            where.append(_FILTERS[col])
            params[col] = val
    base = " FROM agent_reports r JOIN agent_report_reads x ON x.report_id = r.id WHERE " + " AND ".join(where)
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*)" + base), params).scalar() or 0
            rows = conn.execute(text("SELECT r.id::text AS id, r.date, r.agent, r.host, r.title, x.how, x.kind, x.needs_anik, "
                                     "x.summary, x.open_items, x.results, x.refs" + base
                                     + " ORDER BY r.created_at DESC LIMIT :limit OFFSET :offset"),
                                dict(params, limit=limit, offset=offset)).fetchall()
    except Exception as exc:  # noqa: BLE001 - the tables may not exist before the first run
        log.warning("agent report reads failed: {e}", e=str(exc))
        raise HTTPException(status_code=503, detail="agent report reads not readable") from exc
    entries = [dict(r._mapping, date=r.date.isoformat()) for r in rows]
    return {"entries": entries, "total": int(total), "limit": limit, "offset": offset,
            "has_more": offset + len(entries) < total}


@router.get("/coverage")
def report_coverage(days: int = Query(default=7, ge=1, le=400)) -> dict[str, Any]:
    """Whether every report was read: received, read, unread and unreadable per day."""
    try:
        rows = coverage(get_db_engine(), date.today() - timedelta(days=days))
    except Exception as exc:  # noqa: BLE001
        log.warning("agent report coverage failed: {e}", e=str(exc))
        raise HTTPException(status_code=503, detail="coverage not readable") from exc
    return {"days": rows, "unread": sum(r["unread"] for r in rows)}
