"""Discovery engine endpoints."""

from __future__ import annotations

import asyncio
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store

router = APIRouter(prefix="/api/v1/discovery", tags=["discovery"])

# In-memory job tracking (guarded by lock for thread safety)
_jobs: dict[str, dict[str, Any]] = {}
_jobs_lock = threading.Lock()


def _run_orthogonality(job_id: str) -> None:
    """Run orthogonality audit in background."""
    with _jobs_lock:
        _jobs[job_id]["status"] = "running"
    try:
        from discovery.orthogonality import OrthogonalityAudit

        engine = get_db_engine()
        pit = get_pit_store()
        audit = OrthogonalityAudit(engine, pit)
        result = audit.run_full_audit()
        with _jobs_lock:
            _jobs[job_id]["status"] = "complete"
            _jobs[job_id]["result"] = result
    except Exception as exc:
        log.error("Orthogonality job failed: {e}", e=str(exc))
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["result"] = {"error": str(exc)}
    with _jobs_lock:
        _jobs[job_id]["finished"] = datetime.now(timezone.utc).isoformat()


def _run_clustering(job_id: str, n_components: int) -> None:
    """Run cluster discovery in background."""
    with _jobs_lock:
        _jobs[job_id]["status"] = "running"
    try:
        from discovery.clustering import ClusterDiscovery

        engine = get_db_engine()
        pit = get_pit_store()
        cd = ClusterDiscovery(engine, pit)
        result = cd.run_cluster_discovery(n_components=n_components)
        with _jobs_lock:
            _jobs[job_id]["status"] = "complete"
            _jobs[job_id]["result"] = result
    except Exception as exc:
        log.error("Clustering job failed: {e}", e=str(exc))
        with _jobs_lock:
            _jobs[job_id]["status"] = "failed"
            _jobs[job_id]["result"] = {"error": str(exc)}
    with _jobs_lock:
        _jobs[job_id]["finished"] = datetime.now(timezone.utc).isoformat()


@router.post("/orthogonality")
async def trigger_orthogonality(
    _token: str = Depends(require_auth),
) -> dict:
    """Trigger orthogonality audit as background task."""
    job_id = str(uuid.uuid4())[:8]
    with _jobs_lock:
        _jobs[job_id] = {
            "id": job_id,
            "type": "orthogonality",
            "status": "queued",
            "started": datetime.now(timezone.utc).isoformat(),
            "finished": None,
            "result": None,
        }
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _run_orthogonality, job_id)
    return {"job_id": job_id, "status": "queued"}


@router.post("/clustering")
async def trigger_clustering(
    n_components: int = Query(default=3, ge=1, le=20),
    _token: str = Depends(require_auth),
) -> dict:
    """Trigger cluster discovery as background task."""
    job_id = str(uuid.uuid4())[:8]
    with _jobs_lock:
        _jobs[job_id] = {
            "id": job_id,
            "type": "clustering",
            "status": "queued",
            "started": datetime.now(timezone.utc).isoformat(),
            "finished": None,
            "result": None,
        }
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _run_clustering, job_id, n_components)
    return {"job_id": job_id, "status": "queued"}


@router.get("/jobs")
async def get_jobs(
    _token: str = Depends(require_auth),
) -> dict:
    """Return list of recent jobs."""
    with _jobs_lock:
        jobs_list = sorted(
            _jobs.values(), key=lambda j: j["started"], reverse=True
        )[:20]
    return {"jobs": jobs_list}


@router.get("/results/orthogonality")
async def get_orthogonality_results(
    _token: str = Depends(require_auth),
) -> dict:
    """Return most recent orthogonality results."""
    with _jobs_lock:
        sorted_jobs = sorted(_jobs.values(), key=lambda j: j["started"], reverse=True)
    for job in sorted_jobs:
        if job["type"] == "orthogonality" and job["status"] == "complete":
            return {"result": job["result"]}
    return {"result": None, "message": "No completed orthogonality audit found"}


@router.get("/results/clustering")
async def get_clustering_results(
    _token: str = Depends(require_auth),
) -> dict:
    """Return most recent clustering results."""
    with _jobs_lock:
        sorted_jobs = sorted(_jobs.values(), key=lambda j: j["started"], reverse=True)
    for job in sorted_jobs:
        if job["type"] == "clustering" and job["status"] == "complete":
            return {"result": job["result"]}
    return {"result": None, "message": "No completed clustering run found"}


@router.get("/hypotheses")
async def get_hypotheses(
    state: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict:
    """Return all hypotheses from hypothesis_registry."""
    engine = get_db_engine()

    query = "SELECT * FROM hypothesis_registry"
    params: dict[str, Any] = {"lim": limit, "off": offset}
    if state:
        query += " WHERE state = :state"
        params["state"] = state
    query += " ORDER BY created_at DESC LIMIT :lim OFFSET :off"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    hypotheses = []
    for row in rows:
        d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
        for key in ("created_at", "updated_at"):
            if d.get(key) is not None:
                d[key] = str(d[key])
        hypotheses.append(d)

    return {"hypotheses": hypotheses}
