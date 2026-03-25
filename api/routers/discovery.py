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


@router.get("/smart-heatmap")
async def smart_heatmap(
    family: str | None = Query(default=None, description="Filter by feature family (rates, macro, credit, etc.)"),
    orthogonal_only: bool = Query(default=True, description="Filter to orthogonal features only"),
    corr_threshold: float = Query(default=0.8, ge=0.5, le=1.0),
    _token: str = Depends(require_auth),
) -> dict:
    """Return feature heatmap data filtered by orthogonality and optionally by family.

    Returns the correlation submatrix and z-scores for the selected feature set.
    This is the "smart" version that first removes redundant features via
    orthogonality analysis, then optionally filters by taxonomy family.
    """
    import json
    import numpy as np

    engine = get_db_engine()
    pit_store = get_pit_store()

    # Load all model-eligible features
    with engine.connect() as conn:
        feat_rows = conn.execute(text(
            "SELECT id, name, family FROM feature_registry "
            "WHERE model_eligible = TRUE ORDER BY id"
        )).fetchall()

    if not feat_rows:
        return {"features": [], "matrix": [], "z_scores": [], "families": []}

    all_ids = [r[0] for r in feat_rows]
    id_to_name = {r[0]: r[1] for r in feat_rows}
    id_to_family = {r[0]: r[2] for r in feat_rows}

    # Filter by orthogonality if requested
    selected_ids = all_ids
    redundant_pairs = []
    if orthogonal_only:
        try:
            from discovery.orthogonality import OrthogonalityAudit
            audit = OrthogonalityAudit(db_engine=engine, pit_store=pit_store)
            ortho = audit.get_orthogonal_features(corr_threshold=corr_threshold)
            selected_ids = ortho["orthogonal_ids"]
            redundant_pairs = ortho["redundant_pairs"]
        except Exception as exc:
            log.warning("Orthogonal filter failed, using all features: {e}", e=str(exc))

    # Filter by family if specified
    if family:
        selected_ids = [fid for fid in selected_ids if id_to_family.get(fid, "") == family]

    if not selected_ids:
        return {"features": [], "matrix": [], "z_scores": [], "families": [],
                "filtered_count": 0, "total_count": len(all_ids)}

    # Get feature names for selected IDs
    features = [id_to_name[fid] for fid in selected_ids if fid in id_to_name]

    # Build correlation matrix for selected features
    try:
        df = pit_store.get_feature_matrix(feature_ids=selected_ids, as_of_date=None)
        if df is not None and not df.empty:
            corr = df.corr()
            matrix = corr.values.tolist()

            # Get latest z-scores
            z_scores = []
            if len(df) > 0:
                last_row = df.iloc[-1]
                mean = df.mean()
                std = df.std().replace(0, 1)
                z = ((last_row - mean) / std).fillna(0)
                z_scores = [{"feature": features[i], "zscore": float(z.iloc[i]),
                             "family": id_to_family.get(selected_ids[i], "other")}
                            for i in range(min(len(features), len(z)))]
        else:
            matrix = []
            z_scores = []
    except Exception as exc:
        log.warning("Smart heatmap matrix failed: {e}", e=str(exc))
        matrix = []
        z_scores = []

    # Available families
    all_families = sorted(set(id_to_family.values()))

    return {
        "features": features,
        "matrix": matrix,
        "z_scores": z_scores,
        "families": all_families,
        "filtered_count": len(selected_ids),
        "total_count": len(all_ids),
        "redundant_pairs": redundant_pairs[:10],
        "orthogonal_only": orthogonal_only,
        "family_filter": family,
    }
