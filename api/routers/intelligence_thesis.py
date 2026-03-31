"""Intelligence sub-router: Thesis, sleuth/leads, and market diary endpoints."""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["intelligence"])


# ── Unified Thesis Endpoint ───────────────────────────────────────────────

_thesis_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_THESIS_CACHE_TTL = 600  # 10 minutes


@router.get("/thesis")
async def get_unified_thesis(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the unified market thesis combining all models and signals.

    Aggregates Fed liquidity, dealer gamma, vanna/charm, institutional rotation,
    congressional signals, insider clusters, cross-reference divergences,
    supply chain, prediction markets, and trust convergence into a single
    directional view with conviction, key drivers, risk factors, and narrative.

    Cached for 10 minutes.
    """
    now = time.time()
    if _thesis_cache["data"] and (now - _thesis_cache["ts"]) < _THESIS_CACHE_TTL:
        return _thesis_cache["data"]

    try:
        from analysis.flow_thesis import generate_unified_thesis

        engine = get_db_engine()
        thesis = generate_unified_thesis(engine)
        _thesis_cache["data"] = thesis
        _thesis_cache["ts"] = now
        return thesis
    except Exception as exc:
        log.error("Unified thesis generation failed: {e}", e=str(exc))
        return {
            "overall_direction": "NEUTRAL",
            "conviction": 0,
            "bullish_score": 0,
            "bearish_score": 0,
            "active_theses": 0,
            "key_drivers": [],
            "risk_factors": [],
            "agreements": [],
            "contradictions": [],
            "theses": [],
            "narrative": f"Thesis generation failed: {exc}",
            "generated_at": None,
            "error": str(exc),
        }


# ── Thesis Tracker Endpoints ─────────────────────────────────────────────


@router.get("/thesis/history")
async def get_thesis_history_endpoint(
    days: int = Query(90, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return archived thesis snapshots with scoring outcomes.

    Shows the evolution of thesis direction and conviction over time,
    along with whether each thesis was correct, wrong, or partial.
    """
    try:
        from intelligence.thesis_tracker import get_thesis_history

        engine = get_db_engine()
        snapshots = get_thesis_history(engine, days=days)
        return {
            "snapshots": [s.to_dict() for s in snapshots],
            "count": len(snapshots),
            "days": days,
        }
    except Exception as exc:
        log.warning("Thesis history failed: {e}", e=str(exc))
        return {"snapshots": [], "count": 0, "error": str(exc)}


@router.get("/thesis/accuracy")
async def get_thesis_accuracy_endpoint(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return thesis accuracy statistics.

    Includes overall accuracy, per-model accuracy, monthly trend, and
    accuracy by conviction level.
    """
    try:
        from intelligence.thesis_tracker import get_thesis_accuracy

        engine = get_db_engine()
        return get_thesis_accuracy(engine)
    except Exception as exc:
        log.warning("Thesis accuracy failed: {e}", e=str(exc))
        return {
            "overall": {"accuracy_pct": 0, "total_scored": 0},
            "per_model": [],
            "trend": [],
            "best_conditions": {},
            "error": str(exc),
        }


@router.get("/thesis/postmortems")
async def get_thesis_postmortems_endpoint(
    days: int = Query(90, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return thesis post-mortems for wrong or partially-correct theses.

    Each post-mortem explains which models were right vs wrong, what was
    missed, the root cause classification, and an actionable lesson.
    """
    try:
        from intelligence.thesis_tracker import load_thesis_postmortems

        engine = get_db_engine()
        postmortems = load_thesis_postmortems(engine, days=days)

        root_cause_counts: dict[str, int] = {}
        for pm in postmortems:
            rc = pm.get("root_cause", "unknown")
            root_cause_counts[rc] = root_cause_counts.get(rc, 0) + 1

        return {
            "postmortems": postmortems,
            "count": len(postmortems),
            "root_cause_counts": root_cause_counts,
            "days": days,
        }
    except Exception as exc:
        log.warning("Thesis postmortems failed: {e}", e=str(exc))
        return {"postmortems": [], "count": 0, "error": str(exc)}


# ── Sleuth / Investigation Endpoints ─────────────────────────────────────


@router.get("/leads")
async def get_investigation_leads(
    status: str | None = Query(None, description="Filter by status: new, investigating, resolved, dead_end"),
    category: str | None = Query(None, description="Filter by category"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return investigation leads with optional filters."""
    try:
        from intelligence.sleuth import Sleuth
        from dataclasses import asdict

        engine = get_db_engine()
        sleuth = Sleuth(engine)
        leads = sleuth.get_leads(status=status, category=category, limit=limit, offset=offset)
        total = sleuth.count_leads(status=status)

        return {
            "leads": [asdict(l) for l in leads],
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    except Exception as exc:
        log.warning("Sleuth leads query failed: {e}", e=str(exc))
        return {"leads": [], "total": 0, "error": str(exc)}


@router.get("/leads/{lead_id}")
async def get_investigation_lead(
    lead_id: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return a single investigation lead with full detail."""
    try:
        from intelligence.sleuth import Sleuth
        from dataclasses import asdict

        engine = get_db_engine()
        sleuth = Sleuth(engine)
        lead = sleuth._load_lead(lead_id)

        if not lead:
            return {"error": "Lead not found", "lead_id": lead_id}

        return {"lead": asdict(lead)}
    except Exception as exc:
        log.warning("Sleuth lead detail failed: {e}", e=str(exc))
        return {"error": str(exc), "lead_id": lead_id}


@router.post("/leads/investigate")
async def investigate_lead(
    lead_id: str = Query(..., description="ID of the lead to investigate"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger an LLM investigation on a specific lead."""
    try:
        from intelligence.sleuth import Sleuth
        from dataclasses import asdict

        engine = get_db_engine()
        sleuth = Sleuth(engine)
        lead = sleuth._load_lead(lead_id)

        if not lead:
            return {"error": "Lead not found", "lead_id": lead_id}

        result = sleuth.investigate_lead(lead)
        return {
            "status": "investigated",
            "lead": asdict(result),
        }
    except Exception as exc:
        log.warning("Sleuth investigation failed: {e}", e=str(exc))
        return {"error": str(exc), "lead_id": lead_id}


@router.post("/leads/generate")
async def generate_leads(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger lead generation across all intelligence sources."""
    try:
        from intelligence.sleuth import Sleuth
        from dataclasses import asdict

        engine = get_db_engine()
        sleuth = Sleuth(engine)
        leads = sleuth.generate_leads()

        return {
            "status": "generated",
            "leads_created": len(leads),
            "leads": [asdict(l) for l in leads[:20]],
        }
    except Exception as exc:
        log.warning("Sleuth lead generation failed: {e}", e=str(exc))
        return {"error": str(exc), "leads_created": 0}


@router.post("/leads/daily-investigation")
async def run_daily_investigation(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run a full daily investigation cycle (generate + investigate + rabbit holes)."""
    try:
        from intelligence.sleuth import Sleuth

        engine = get_db_engine()
        sleuth = Sleuth(engine)
        report = sleuth.daily_investigation()

        return {"status": "complete", "report": report}
    except Exception as exc:
        log.warning("Sleuth daily investigation failed: {e}", e=str(exc))
        return {"error": str(exc), "status": "failed"}


# ── Market Diary Endpoints ─────────────────────────────────────────────


@router.get("/diary")
async def get_diary(
    date: str | None = Query(None, description="Date in YYYY-MM-DD format"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Retrieve a market diary entry by date.

    If no date is provided, returns the most recent entry.
    """
    from datetime import date as date_type

    try:
        from intelligence.market_diary import get_diary_entry, list_diary_entries

        engine = get_db_engine()

        if date:
            target = date_type.fromisoformat(date)
            entry = get_diary_entry(engine, target)
            if entry is None:
                return {"error": "No diary entry for this date", "date": date}
            return entry
        else:
            result = list_diary_entries(engine, limit=1)
            if result["entries"]:
                entry_date = date_type.fromisoformat(result["entries"][0]["date"])
                entry = get_diary_entry(engine, entry_date)
                return entry or {"error": "Entry not found"}
            return {"error": "No diary entries yet"}
    except Exception as exc:
        log.warning("Diary fetch failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.get("/diary/list")
async def list_diaries(
    limit: int = Query(30, ge=1, le=365),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """List diary entries with summary metadata (date, verdict, return)."""
    try:
        from intelligence.market_diary import list_diary_entries

        engine = get_db_engine()
        return list_diary_entries(engine, limit=limit, offset=offset)
    except Exception as exc:
        log.warning("Diary list failed: {e}", e=str(exc))
        return {"entries": [], "total": 0, "error": str(exc)}


@router.get("/diary/search")
async def search_diaries(
    q: str = Query(..., min_length=2, description="Search term"),
    limit: int = Query(20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Search diary entries by keyword."""
    try:
        from intelligence.market_diary import search_diary

        engine = get_db_engine()
        results = search_diary(engine, q, limit=limit)
        return {"results": results, "query": q}
    except Exception as exc:
        log.warning("Diary search failed: {e}", e=str(exc))
        return {"results": [], "query": q, "error": str(exc)}


@router.post("/diary/generate")
async def generate_diary(
    date: str | None = Query(None, description="Date in YYYY-MM-DD format"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Manually trigger diary generation for a given date (defaults to today)."""
    from datetime import date as date_type

    try:
        from intelligence.market_diary import write_diary_entry

        engine = get_db_engine()
        target = date_type.fromisoformat(date) if date else date_type.today()
        result = write_diary_entry(engine, target_date=target)
        return result
    except Exception as exc:
        log.warning("Diary generation failed: {e}", e=str(exc))
        return {"error": str(exc)}
