"""Intelligence sub-router: Company analyzer, deep graph, and institutional map."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["intelligence"])


# ── Company Analyzer Endpoints ──────────────────────────────────────────
# NOTE: Specific path routes (/companies/patterns, /companies/sector-report,
# /companies/analyze) MUST be registered before the parameterized
# /companies/{ticker} route to avoid FastAPI matching "patterns" as a ticker.


@router.get("/companies")
async def get_all_company_profiles(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return all analyzed company influence profiles, sorted by suspicion score.

    Each profile contains government contracts, congressional holdings,
    insider activity, lobbying, influence loops, and LLM narrative.
    """
    try:
        from intelligence.company_analyzer import get_all_profiles

        engine = get_db_engine()
        profiles = get_all_profiles(engine)
        return {
            "count": len(profiles),
            "profiles": [p.to_dict() for p in profiles],
        }

    except Exception as exc:
        log.warning("Company profiles endpoint failed: {e}", e=str(exc))
        return {"count": 0, "profiles": [], "error": str(exc)}


@router.get("/companies/patterns")
async def get_cross_company_patterns(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect cross-company influence patterns.

    Looks for sector-wide lobbying surges, coordinated insider selling,
    committee members with concentrated holdings, suspicion clusters,
    and government contract concentration.
    """
    try:
        from intelligence.company_analyzer import find_cross_company_patterns

        engine = get_db_engine()
        patterns = find_cross_company_patterns(engine)
        return {
            "count": len(patterns),
            "patterns": patterns,
        }

    except Exception as exc:
        log.warning("Cross-company patterns endpoint failed: {e}", e=str(exc))
        return {"count": 0, "patterns": [], "error": str(exc)}


@router.get("/companies/sector-report")
async def get_sector_influence_report(
    sector: str = Query(..., description="Sector name (e.g. Technology, Semiconductors)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate an LLM narrative summarizing influence across a sector.

    Aggregates all company profiles in the sector and produces a
    multi-paragraph analysis of lobbying, contracts, insider activity,
    and suspicion patterns.
    """
    try:
        from intelligence.company_analyzer import generate_sector_influence_report

        engine = get_db_engine()
        report = generate_sector_influence_report(engine, sector)
        return {
            "sector": sector,
            "report": report,
        }

    except Exception as exc:
        log.warning("Sector report for {s} failed: {e}", s=sector, e=str(exc))
        return {"sector": sector, "report": "", "error": str(exc)}


@router.post("/companies/analyze")
async def trigger_company_analysis(
    ticker: str = Query(..., description="Stock ticker to analyze (e.g. AAPL, NVDA)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger full influence analysis for a single company.

    Queries all intelligence modules (gov contracts, lobbying, insider,
    congressional, export controls, actor network) and generates an
    LLM narrative. Results are stored in the company_profiles table.
    """
    try:
        from intelligence.company_analyzer import analyze_company

        engine = get_db_engine()
        profile = analyze_company(engine, ticker)
        return {
            "status": "analyzed",
            "profile": profile.to_dict(),
        }

    except Exception as exc:
        log.warning("Company analysis for {t} failed: {e}", t=ticker, e=str(exc))
        return {"status": "error", "ticker": ticker, "error": str(exc)}


@router.get("/companies/{ticker}")
async def get_company_profile(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the influence profile for a single company.

    If the company has not been analyzed yet, returns a 404-style response.
    Use POST /companies/analyze?ticker=AAPL to trigger analysis.
    """
    try:
        from intelligence.company_analyzer import get_all_profiles

        engine = get_db_engine()
        profiles = get_all_profiles(engine)
        ticker_upper = ticker.strip().upper()

        for p in profiles:
            if p.ticker == ticker_upper:
                return {"profile": p.to_dict()}

        return {"profile": None, "error": f"No analysis found for {ticker_upper}"}

    except Exception as exc:
        log.warning("Company profile for {t} failed: {e}", t=ticker, e=str(exc))
        return {"profile": None, "error": str(exc)}


# ── Deep Graph Endpoints ──────────────────────────────────────────────────

_deep_graph_cache: dict[str, Any] = {}
_DEEP_GRAPH_TTL = 900  # 15 minutes


@router.get("/deep-graph/{ticker}")
async def get_deep_graph(
    ticker: str,
    depth: int = Query(default=10, ge=1, le=10, description="Traversal depth (1-10)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Drill 10 layers deep from a ticker to map the full actor network.

    Layer 1: Company -> Layer 2: Board/C-Suite -> Layer 3: Other affiliations ->
    Layer 4: Lobbyists -> Layer 5: Politicians -> Layer 6: Committees ->
    Layer 7: Affected companies -> Layer 8: Insiders -> Layer 9: Cross-holding funds ->
    Layer 10: Beneficial owners.

    At each layer: WHO, HOW MUCH money, WHEN, and CONNECTION TYPE.
    Capped at 1000 actors to prevent explosion.
    """
    import time
    from datetime import datetime, timezone

    cache_key = f"{ticker.upper()}:{depth}"
    now = datetime.now(timezone.utc)
    cached = _deep_graph_cache.get(cache_key)
    if cached and cached.get("ts") and (now - cached["ts"]).total_seconds() < _DEEP_GRAPH_TTL:
        return cached["data"]

    try:
        from intelligence.deep_graph import deep_drill

        engine = get_db_engine()
        t0 = time.time()
        result = deep_drill(engine, ticker, max_depth=depth)
        elapsed = time.time() - t0

        response = {
            "drill": result,
            "elapsed_seconds": round(elapsed, 2),
        }

        _deep_graph_cache[cache_key] = {"data": response, "ts": now}
        return response

    except Exception as exc:
        log.warning("Deep graph drill for {t} failed: {e}", t=ticker, e=str(exc))
        return {"drill": None, "error": str(exc)}


@router.get("/overlaps")
async def get_overlaps(
    ticker_a: str = Query(..., description="First ticker"),
    ticker_b: str = Query(..., description="Second ticker"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Find hidden connections between two seemingly unrelated tickers.

    Drills from both tickers independently and finds where the two graphs
    intersect — shared actors, committees, funds, and dollar flows.
    """
    import time

    try:
        from intelligence.deep_graph import find_overlaps

        engine = get_db_engine()
        t0 = time.time()
        overlaps = find_overlaps(engine, ticker_a, ticker_b)
        elapsed = time.time() - t0

        return {
            "ticker_a": ticker_a.upper(),
            "ticker_b": ticker_b.upper(),
            "overlaps": [o.to_dict() for o in overlaps],
            "count": len(overlaps),
            "elapsed_seconds": round(elapsed, 2),
        }

    except Exception as exc:
        log.warning(
            "Overlap detection {a} <-> {b} failed: {e}",
            a=ticker_a, b=ticker_b, e=str(exc),
        )
        return {"overlaps": [], "count": 0, "error": str(exc)}


@router.get("/overlaps/all")
async def get_all_overlaps(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Find all hidden connections across the entire watchlist.

    Runs pairwise overlap detection on all active watchlist tickers.
    "Your watchlist has 15 hidden connections you didn't know about."
    """
    import time

    try:
        from intelligence.deep_graph import find_all_overlaps

        engine = get_db_engine()
        t0 = time.time()
        overlaps = find_all_overlaps(engine)
        elapsed = time.time() - t0

        return {
            "overlaps": [o.to_dict() for o in overlaps],
            "count": len(overlaps),
            "elapsed_seconds": round(elapsed, 2),
        }

    except Exception as exc:
        log.warning("All-overlaps scan failed: {e}", e=str(exc))
        return {"overlaps": [], "count": 0, "error": str(exc)}


@router.get("/deep-graph/{ticker}/map")
async def get_connection_map(
    ticker: str,
    depth: int = Query(default=5, ge=1, le=10, description="Map depth (1-10)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate a D3-ready connection map for a ticker.

    Returns nodes colored by layer depth with overlap nodes highlighted.
    Suitable for force-directed graph visualization.
    """
    try:
        from intelligence.deep_graph import generate_connection_map

        engine = get_db_engine()
        result = generate_connection_map(engine, ticker, depth=depth)
        return result

    except Exception as exc:
        log.warning("Connection map for {t} failed: {e}", t=ticker, e=str(exc))
        return {"nodes": [], "links": [], "metadata": {}, "error": str(exc)}


@router.get("/institutional-map")
async def get_institutional_map(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the full institutional map: private credit funds, hedge funds,
    pension systems, allocation links, revolving door, and conflicts of interest.

    This is the shadow banking layer -- where pension dollars flow through
    opaque fee structures into private credit and leveraged buyouts.
    """
    import time

    try:
        from intelligence.institutional_map import (
            build_institutional_graph,
            find_conflicts_of_interest,
            get_institutional_summary,
        )

        engine = get_db_engine()
        t0 = time.time()
        graph = build_institutional_graph(engine)
        summary = get_institutional_summary()
        elapsed = time.time() - t0

        return {
            **graph,
            "summary": summary,
            "elapsed_seconds": round(elapsed, 2),
        }

    except Exception as exc:
        log.warning("Institutional map failed: {e}", e=str(exc))
        return {"nodes": [], "links": [], "metadata": {}, "error": str(exc)}


@router.get("/institutional-map/trace/{pension_name}")
async def trace_pension(
    pension_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trace where a specific pension fund's money ends up.

    Follow the dollars from beneficiary contributions through to fund managers,
    with fee extraction estimates at each step.
    """
    try:
        from intelligence.institutional_map import trace_pension_dollars

        result = trace_pension_dollars(pension_name)
        return result

    except Exception as exc:
        log.warning("Pension trace for {p} failed: {e}", p=pension_name, e=str(exc))
        return {"error": str(exc)}


@router.get("/institutional-map/fees/{fund_name}")
async def get_fund_fees(
    fund_name: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Estimate fee extraction for a specific fund from pension capital.

    Shows management fees, performance fees, passthrough fees, and
    10-year extraction projections.
    """
    try:
        from intelligence.institutional_map import get_fee_extraction_estimate

        result = get_fee_extraction_estimate(fund_name)
        return result

    except Exception as exc:
        log.warning("Fee estimate for {f} failed: {e}", f=fund_name, e=str(exc))
        return {"error": str(exc)}


@router.get("/institutional-map/conflicts")
async def get_institutional_conflicts(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return all detected conflicts of interest in the institutional map.

    Includes revolving door, pay-to-play, consultant conflicts,
    underfunded pension risk mismatches, and liquidity crises.
    """
    try:
        from intelligence.institutional_map import find_conflicts_of_interest

        conflicts = find_conflicts_of_interest()
        return {
            "conflicts": conflicts,
            "count": len(conflicts),
            "severity_breakdown": {
                "critical": len([c for c in conflicts if c.get("severity") == "critical"]),
                "high": len([c for c in conflicts if c.get("severity") == "high"]),
                "medium": len([c for c in conflicts if c.get("severity") == "medium"]),
                "low": len([c for c in conflicts if c.get("severity") == "low"]),
            },
        }

    except Exception as exc:
        log.warning("Conflict detection failed: {e}", e=str(exc))
        return {"conflicts": [], "count": 0, "error": str(exc)}


@router.get("/hidden-influence")
async def get_hidden_influence(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Discover hidden influence patterns across the watchlist.

    Cross-references deep graph overlaps with causal chains to find:
    - Actors connecting seemingly unrelated events
    - Committee influence over multiple positions
    - Fund concentration risk
    """
    import time

    try:
        from intelligence.deep_graph import discover_hidden_influence

        engine = get_db_engine()
        t0 = time.time()
        discoveries = discover_hidden_influence(engine)
        elapsed = time.time() - t0

        return {
            "discoveries": discoveries,
            "count": len(discoveries),
            "elapsed_seconds": round(elapsed, 2),
        }

    except Exception as exc:
        log.warning("Hidden influence discovery failed: {e}", e=str(exc))
        return {"discoveries": [], "count": 0, "error": str(exc)}
