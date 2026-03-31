"""Intelligence sub-router: Global lever map, deep-dive, and expectations endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["intelligence"])


# ── Global Lever Map Endpoints ──────────────────────────────────────────

_lever_cache: dict[str, Any] = {"data": None, "ts": None}
_LEVER_CACHE_TTL = 600  # 10 minutes


@router.get("/levers")
async def get_levers(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the full global lever hierarchy — all 8 domains.

    Cached for 10 minutes.  Includes hierarchy, summaries, and cross-domain
    actor index.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    if (
        _lever_cache["data"]
        and _lever_cache["ts"]
        and (now - _lever_cache["ts"]).total_seconds() < _LEVER_CACHE_TTL
    ):
        return _lever_cache["data"]

    try:
        from intelligence.global_levers import (
            get_lever_hierarchy,
            find_cross_domain_actors,
        )

        engine = get_db_engine()
        hierarchy = get_lever_hierarchy()
        cross_domain = find_cross_domain_actors(engine)

        result = {
            **hierarchy,
            "cross_domain_actors": cross_domain[:20],
        }

        _lever_cache["data"] = result
        _lever_cache["ts"] = now
        return result

    except Exception as exc:
        log.warning("Global lever map failed: {e}", e=str(exc))
        return {"error": str(exc), "hierarchy": {}}


@router.get("/levers/{domain}")
async def get_lever_domain_endpoint(
    domain: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return a single lever domain with full actor details."""
    try:
        from intelligence.global_levers import get_lever_domain

        return get_lever_domain(domain)

    except Exception as exc:
        log.warning("Lever domain lookup failed: {e}", e=str(exc))
        return {"error": str(exc)}


@router.get("/levers/chain/{event}")
async def trace_lever_chain_endpoint(
    event: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trace the chain of effects from a named event.

    Example: /api/v1/intelligence/levers/chain/interest_rate_hike
    """
    try:
        from intelligence.global_levers import trace_lever_chain

        chain = trace_lever_chain(event)
        return {"event": event, "chain": chain, "steps": len(chain)}

    except Exception as exc:
        log.warning("Lever chain trace failed: {e}", e=str(exc))
        return {"error": str(exc), "chain": []}


@router.get("/levers/cross-domain")
async def get_cross_domain_actors_endpoint(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Find actors appearing in 2+ lever domains — the most powerful players."""
    try:
        from intelligence.global_levers import find_cross_domain_actors

        engine = get_db_engine()
        actors = find_cross_domain_actors(engine)
        return {"actors": actors, "count": len(actors)}

    except Exception as exc:
        log.warning("Cross-domain actor lookup failed: {e}", e=str(exc))
        return {"error": str(exc), "actors": []}


@router.get("/levers/report")
async def get_lever_report_endpoint(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate a narrative report: who's pulling what lever right now."""
    try:
        from intelligence.global_levers import generate_lever_report

        engine = get_db_engine()
        report = generate_lever_report(engine)
        return {"report": report}

    except Exception as exc:
        log.warning("Lever report generation failed: {e}", e=str(exc))
        return {"error": str(exc), "report": ""}


# ── News Impact / Deep Dive Endpoints ────────────────────────────────────


@router.get("/deep-dive/{ticker}")
async def get_deep_dive(
    ticker: str,
    days: int = Query(90, ge=7, le=365),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Full forensic deep dive: news impact attribution, expectations, catalysts.

    Decomposes every significant price move into its catalysts (news, signals,
    macro, sector), tracks what's baked into the price vs still expected,
    and generates an LLM narrative.
    """
    try:
        from intelligence.news_impact import DeepDiveEngine, ensure_tables

        engine = get_db_engine()
        ensure_tables(engine)
        dive = DeepDiveEngine(engine)
        report = dive.generate_deep_dive(ticker.upper(), days)

        return {
            "ticker": report.ticker,
            "name": report.name,
            "generated_at": report.generated_at.isoformat(),
            "total_moves_analyzed": report.total_moves_analyzed,
            "avg_explained_pct": report.avg_explained_pct,
            "total_baked_in_bps": report.total_baked_in_bps,
            "total_pending_bps": report.total_pending_bps,
            "historical_hit_rate": report.historical_hit_rate,
            "catalyst_breakdown": report.catalyst_breakdown,
            "top_catalysts": [
                {
                    "title": c.title,
                    "type": c.catalyst_type,
                    "horizon": c.horizon,
                    "direction": c.direction,
                    "estimated_bps": c.estimated_bps,
                    "confidence": c.confidence,
                    "date": c.event_date.isoformat() if c.event_date else None,
                }
                for c in report.top_catalysts
            ],
            "significant_moves": [
                {
                    "date": str(a.move_date),
                    "pct": round(a.move_pct * 100, 2),
                    "direction": a.move_direction,
                    "explained_bps": a.total_explained_bps,
                    "unexplained_bps": a.unexplained_bps,
                    "macro_bps": a.macro_contribution_bps,
                    "sector_bps": a.sector_contribution_bps,
                    "catalysts": [
                        {
                            "title": c.title[:80],
                            "type": c.catalyst_type,
                            "bps": c.estimated_bps,
                            "direction": c.direction,
                        }
                        for c in a.catalysts[:5]
                    ],
                }
                for a in report.significant_moves
            ],
            "active_expectations": [
                {
                    "description": e.description,
                    "catalyst_type": e.catalyst_type,
                    "horizon": e.horizon,
                    "direction": e.expected_direction,
                    "magnitude_bps": e.expected_magnitude_bps,
                    "baked_in_pct": e.baked_in_pct,
                    "deadline": e.deadline.isoformat() if e.deadline else None,
                }
                for e in report.active_expectations
            ],
            "narrative": report.narrative,
            "confidence": report.confidence,
        }
    except Exception as exc:
        log.warning("Deep dive failed for {t}: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker.upper(), "error": str(exc)}


@router.post("/deep-dive/mag7")
async def run_mag7_deep_dives(
    days: int = Query(90, ge=7, le=365),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run full deep dives for all Mag 7 tickers."""
    try:
        from intelligence.news_impact import DeepDiveEngine, MAG7_TICKERS, ensure_tables

        engine = get_db_engine()
        ensure_tables(engine)
        dive = DeepDiveEngine(engine)
        results = []
        for ticker in MAG7_TICKERS:
            try:
                report = dive.generate_deep_dive(ticker, days)
                results.append({
                    "ticker": report.ticker,
                    "moves_analyzed": report.total_moves_analyzed,
                    "avg_explained_pct": report.avg_explained_pct,
                    "pending_bps": report.total_pending_bps,
                    "narrative": report.narrative[:200],
                })
            except Exception as exc:
                results.append({"ticker": ticker, "error": str(exc)})

        return {"status": "SUCCESS", "results": results}
    except Exception as exc:
        return {"error": str(exc)}


@router.get("/expectations/{ticker}")
async def get_expectations(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Get active market expectations for a ticker — what's baked in vs pending."""
    try:
        from intelligence.news_impact import ExpectationTracker, ensure_tables

        engine = get_db_engine()
        ensure_tables(engine)
        tracker = ExpectationTracker(engine)

        expectations = tracker.get_active_expectations(ticker.upper())
        net = tracker.compute_net_expectations(ticker.upper())

        return {
            "ticker": ticker.upper(),
            "summary": net,
            "expectations": [
                {
                    "id": e.id,
                    "description": e.description,
                    "catalyst_type": e.catalyst_type,
                    "horizon": e.horizon,
                    "direction": e.expected_direction,
                    "magnitude_bps": e.expected_magnitude_bps,
                    "baked_in_pct": e.baked_in_pct,
                    "deadline": e.deadline.isoformat() if e.deadline else None,
                    "status": e.status,
                }
                for e in expectations
            ],
        }
    except Exception as exc:
        return {"ticker": ticker.upper(), "error": str(exc)}
