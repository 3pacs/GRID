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
        from analysis.thesis_scorer import score_thesis, snapshot_thesis

        engine = get_db_engine()
        thesis = score_thesis(engine)

        # Snapshot for accuracy tracking (non-blocking)
        try:
            snapshot_thesis(engine, thesis)
        except Exception as e:
            log.warning("Thesis: snapshot failed: {e}", e=str(e))

        # Map new scorer output to frontend-compatible fields
        thesis["overall_direction"] = thesis["direction"]
        thesis["bullish_score"] = thesis["bull_pct"]
        thesis["bearish_score"] = thesis["bear_pct"]
        thesis["active_theses"] = thesis["active_models"]

        # Build key_drivers / risk_factors from top models
        active = [m for m in thesis.get("models", []) if m["status"] == "active"]
        thesis["key_drivers"] = [
            {"key": m["key"], "name": m["name"], "direction": m["direction"],
             "detail": m["reasoning"], "weight": m["weight_in_final"]}
            for m in sorted(active, key=lambda x: -abs(x["score"]))[:3]
        ]
        thesis["risk_factors"] = [
            {"key": m["key"], "name": m["name"], "direction": m["direction"],
             "detail": m["reasoning"], "weight": m["weight_in_final"]}
            for m in active
            if (thesis["direction"] == "BULLISH" and m["direction"] == "bearish")
            or (thesis["direction"] == "BEARISH" and m["direction"] == "bullish")
        ][:3]

        # Build narrative from the scorer
        from analysis.thesis_scorer import _build_narrative
        thesis["narrative"] = _build_narrative(thesis)
        thesis["theses"] = thesis.get("models", [])

        _thesis_cache["data"] = thesis
        _thesis_cache["ts"] = now
        return thesis
    except Exception as exc:
        log.error("Thesis scoring failed: {e}", e=str(exc))
        return {
            "overall_direction": "NEUTRAL",
            "conviction": 0,
            "score": 0,
            "bullish_score": 0,
            "bearish_score": 0,
            "active_theses": 0,
            "key_drivers": [],
            "risk_factors": [],
            "models": [],
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


# ── Deep Dive Endpoints ──────────────────────────────────────────────────


@router.get("/deep-dives")
async def get_deep_dives_endpoint(
    days: int = Query(90, ge=1, le=365, description="Lookback days"),
    limit: int = Query(50, ge=1, le=200),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return archived deep dive analyses. Deep dives are NEVER deleted.

    Each deep dive is a thorough LLM analysis triggered when a new thesis
    is generated. Contains key insights, contrarian signals, blind spots,
    and follow-up research questions.
    """
    try:
        from intelligence.deep_dive import get_deep_dives

        engine = get_db_engine()
        dives = get_deep_dives(engine, days=days, limit=limit)
        return {"deep_dives": dives, "count": len(dives), "days": days}
    except Exception as exc:
        log.warning("Deep dives query failed: {e}", e=str(exc))
        return {"deep_dives": [], "count": 0, "error": str(exc)}


@router.get("/deep-dives/{dive_id}")
async def get_deep_dive_endpoint(
    dive_id: int,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return a single deep dive analysis by ID."""
    try:
        from intelligence.deep_dive import get_deep_dive

        engine = get_db_engine()
        dive = get_deep_dive(engine, dive_id)
        if dive is None:
            return {"error": f"Deep dive {dive_id} not found"}
        return {"deep_dive": dive}
    except Exception as exc:
        log.warning("Deep dive detail failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.post("/deep-dives/generate")
async def trigger_deep_dive(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Manually trigger a deep dive on the current thesis.

    Runs in the background — returns immediately with status.
    """
    try:
        from analysis.flow_thesis import generate_unified_thesis
        from intelligence.deep_dive import deep_dive_async
        from intelligence.thesis_tracker import snapshot_thesis

        engine = get_db_engine()
        thesis_data = generate_unified_thesis(engine)
        if not thesis_data:
            return {"error": "No thesis data available", "status": "FAILED"}

        snapshot_id = snapshot_thesis(engine, thesis_data)
        deep_dive_async(engine, thesis_data, snapshot_id)

        return {
            "status": "LAUNCHED",
            "snapshot_id": snapshot_id,
            "message": "Deep dive running in background. Check /deep-dives for results.",
        }
    except Exception as exc:
        log.error("Manual deep dive trigger failed: {e}", e=str(exc))
        return {"error": str(exc), "status": "FAILED"}


# ── Research Archive Endpoint ────────────────────────────────────────────


@router.get("/archive")
async def get_research_archive(
    days: int = Query(365, ge=1, le=3650, description="Lookback days"),
    limit: int = Query(100, ge=1, le=500),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the full research archive: deep dives, postmortems, diary entries.

    This is the permanent record of every analysis GRID has produced.
    Nothing is ever deleted.
    """
    engine = get_db_engine()
    archive: dict[str, Any] = {"days": days}

    # Deep dives
    try:
        from intelligence.deep_dive import get_deep_dives
        dives = get_deep_dives(engine, days=days, limit=limit)
        archive["deep_dives"] = dives
        archive["deep_dive_count"] = len(dives)
    except Exception as exc:
        archive["deep_dives"] = []
        archive["deep_dive_count"] = 0
        archive["deep_dive_error"] = str(exc)

    # Postmortems
    try:
        from intelligence.thesis_tracker import load_thesis_postmortems
        pms = load_thesis_postmortems(engine, days=days)
        archive["postmortems"] = pms
        archive["postmortem_count"] = len(pms)
    except Exception as exc:
        archive["postmortems"] = []
        archive["postmortem_count"] = 0
        archive["postmortem_error"] = str(exc)

    # Thesis snapshots
    try:
        from intelligence.thesis_tracker import get_thesis_history
        snapshots = get_thesis_history(engine, days=days)
        archive["thesis_snapshots"] = [s.to_dict() for s in snapshots]
        archive["thesis_count"] = len(snapshots)
    except Exception as exc:
        archive["thesis_snapshots"] = []
        archive["thesis_count"] = 0
        archive["thesis_error"] = str(exc)

    # Audio briefings
    try:
        from intelligence.audio_briefing import list_all_briefings
        archive["audio_briefings"] = list_all_briefings()
        archive["audio_count"] = len(archive["audio_briefings"])
    except Exception as exc:
        archive["audio_briefings"] = []
        archive["audio_count"] = 0
        archive["audio_error"] = str(exc)

    # Diary entries
    try:
        from intelligence.market_diary import list_diary_entries
        diary = list_diary_entries(engine, limit=limit)
        archive["diary_entries"] = diary.get("entries", [])
        archive["diary_count"] = diary.get("total", 0)
    except Exception as exc:
        archive["diary_entries"] = []
        archive["diary_count"] = 0
        archive["diary_error"] = str(exc)

    return archive


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
