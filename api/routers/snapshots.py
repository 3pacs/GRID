"""Analytical snapshot query endpoints.

Provides API access to historical analytical outputs stored in
``analytical_snapshots``.  Enables comparing clustering, orthogonality,
regime, and feature importance results across time.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/snapshots", tags=["snapshots"])


@router.get("/latest/{category}")
def get_latest_snapshots(
    category: str,
    n: int = Query(default=1, ge=1, le=50),
    _user: dict = Depends(require_auth),
) -> list[dict[str, Any]]:
    """Return the N most recent snapshots for a category."""
    from store.snapshots import AnalyticalSnapshotStore

    engine = get_db_engine()
    store = AnalyticalSnapshotStore(db_engine=engine)

    if category not in store.CATEGORIES:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown category '{category}'. Valid: {store.CATEGORIES}",
        )

    results = store.get_latest(category, n=n)
    return results


@router.get("/history/{category}")
def get_snapshot_history(
    category: str,
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    _user: dict = Depends(require_auth),
) -> list[dict[str, Any]]:
    """Return metrics history for a category (for trending/charting)."""
    from store.snapshots import AnalyticalSnapshotStore

    engine = get_db_engine()
    store = AnalyticalSnapshotStore(db_engine=engine)

    df = store.get_history(category, start_date=start_date, end_date=end_date)
    if df.empty:
        return []
    return df.to_dict("records")


@router.get("/compare/{category}")
def compare_snapshots(
    category: str,
    date_a: date = Query(...),
    date_b: date = Query(...),
    _user: dict = Depends(require_auth),
) -> dict[str, Any]:
    """Compare two snapshots from different dates."""
    from store.snapshots import AnalyticalSnapshotStore

    engine = get_db_engine()
    store = AnalyticalSnapshotStore(db_engine=engine)

    result = store.compare_snapshots(category, date_a, date_b)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.get("/categories")
def list_categories(
    _user: dict = Depends(require_auth),
) -> list[str]:
    """Return all available snapshot categories."""
    from store.snapshots import AnalyticalSnapshotStore
    return list(AnalyticalSnapshotStore.CATEGORIES)
