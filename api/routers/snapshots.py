"""Analytical snapshot query endpoints.

Provides API access to historical analytical outputs stored in
``analytical_snapshots``.  Enables comparing clustering, orthogonality,
regime, and feature importance results across time.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/snapshots", tags=["snapshots"])

# ``analytical_snapshots.category`` is an unconstrained TEXT column, so the
# boundary check here is on the shape of the value, not on membership of a
# list. Which categories exist is data (see store/snapshots.py's "Category
# discovery" note) and these handlers must not second-guess it: every category
# parameter is a bound query parameter, and an unrecognised one simply matches
# no rows.
_MAX_CATEGORY_LEN = 128


# ------------------------------------------------------------------
# Research run status (GRID W4c) — read-only surface over the
# scripts/autoresearch.py run-state trail (category="research_run",
# subcategory="autoresearch"; see scripts/research_status.py and
# docs/handoffs/2026-09-18/fable-w4b-runstate.md for the record schema).
# ------------------------------------------------------------------

@router.get("/research/latest")
def get_latest_research_run(
    _user: dict = Depends(require_auth),
) -> dict[str, Any]:
    """Return the latest autoresearch research_run event, if any.

    Response shapes (never a 500, including on a missing table):
      - A run/event exists: the record flattened to the top level —
        ``run_id``, ``status`` (the record's OWN lifecycle status:
        started/running/ok/failed/timeout/abandoned — not an envelope
        sentinel), ``phase``, ``error``, ``error_category``, ``iteration``,
        ``iterations``, ``skip_reasons``, ``failure_reasons``,
        ``duration_s``, ``generation``, ``code_sha``, ``inputs`` (which
        carries ``evaluation_version``) — plus ``latest_hypothesis`` (the
        most recent ``hypothesis_registry`` outcome: id/statement/layer/
        state/kill_reason/updated_at) when one exists. NOTE: the
        research_run record itself does not currently link to a specific
        hypothesis_id, so ``latest_hypothesis`` is the latest hypothesis
        outcome independent of which run produced it, not necessarily the
        one this run tested — see scripts/research_status.py::
        latest_hypothesis_outcome's docstring.
      - No research_run rows exist yet (table reachable, empty):
        ``{"status": "no_runs"}``.
      - The query itself failed (table missing, database unreachable):
        ``{"status": "unavailable", "reason": "<short diagnostic>"}``.
    """
    from scripts.research_status import latest_hypothesis_outcome, latest_research_run_result

    engine = get_db_engine()
    result = latest_research_run_result(engine)

    if result.get("status") in ("no_runs", "unavailable"):
        return result

    hypothesis = latest_hypothesis_outcome(engine)
    if hypothesis is not None:
        result["latest_hypothesis"] = hypothesis

    return result


@router.get("/latest/{category}")
def get_latest_snapshots(
    category: str = Path(..., min_length=1, max_length=_MAX_CATEGORY_LEN),
    n: int = Query(default=1, ge=1, le=50),
    _user: dict = Depends(require_auth),
) -> list[dict[str, Any]]:
    """Return the N most recent snapshots for a category.

    A category with no rows is not an error — it returns ``[]``, matching
    ``/history``. This endpoint used to reject anything outside a hardcoded
    eight-value tuple with HTTP 400, which made most of the table
    unreadable; see store/snapshots.py's PIPELINE_CATEGORIES note.
    Call ``/categories`` to discover what exists.
    """
    from store.snapshots import AnalyticalSnapshotStore

    engine = get_db_engine()
    store = AnalyticalSnapshotStore(db_engine=engine)

    return store.get_latest(category, n=n)


@router.get("/history/{category}")
def get_snapshot_history(
    category: str = Path(..., min_length=1, max_length=_MAX_CATEGORY_LEN),
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
    category: str = Path(..., min_length=1, max_length=_MAX_CATEGORY_LEN),
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
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    _user: dict = Depends(require_auth),
) -> dict[str, Any]:
    """Return every snapshot category present in ``analytical_snapshots``.

    Read from the table, so it reports what the writers actually produced
    rather than a maintained literal. Each entry carries ``category``,
    ``snapshot_count`` and ``latest_snapshot_date`` so a dashboard can show
    what is available and how stale it is without a probe request per
    category.
    """
    from store.snapshots import AnalyticalSnapshotStore

    engine = get_db_engine()
    store = AnalyticalSnapshotStore(db_engine=engine)

    categories = store.list_categories()
    total = len(categories)
    return {
        "entries": categories[offset : offset + limit],
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < total,
    }


# ------------------------------------------------------------------
# Operator issues (bug/fix tracking for external model analysis)
# ------------------------------------------------------------------

@router.get("/issues")
def get_operator_issues(
    days_back: int = Query(default=30, ge=1, le=365),
    category: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    _user: dict = Depends(require_auth),
) -> list[dict[str, Any]]:
    """Export operator issues for analysis.

    Feed this to a smarter model to find root causes across failures.
    """
    from sqlalchemy import text

    engine = get_db_engine()

    # Build query with optional filters
    conditions = ["created_at > NOW() - :days * INTERVAL '1 day'"]
    params: dict[str, Any] = {"days": days_back}

    if category:
        conditions.append("category = :cat")
        params["cat"] = category
    if severity:
        conditions.append("severity = :sev")
        params["sev"] = severity

    where = " AND ".join(conditions) if conditions else "1=1"

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, created_at, category, severity, source, title, "
                    "       detail, stack_trace, hermes_diagnosis, fix_applied, "
                    "       fix_result, resolved_at, cycle_number "
                    "FROM operator_issues "
                    "WHERE " + where + " "
                    "ORDER BY created_at DESC "
                    "LIMIT 500"
                ),
                params,
            ).fetchall()
    except Exception:
        # Table may not exist yet
        return []

    return [
        {
            "id": r[0],
            "created_at": r[1].isoformat() if r[1] else None,
            "category": r[2], "severity": r[3], "source": r[4],
            "title": r[5], "detail": r[6], "stack_trace": r[7],
            "hermes_diagnosis": r[8], "fix_applied": r[9],
            "fix_result": r[10],
            "resolved_at": r[11].isoformat() if r[11] else None,
            "cycle_number": r[12],
        }
        for r in rows
    ]
