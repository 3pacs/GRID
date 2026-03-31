"""Intelligence sub-router: Forensics, causation, influence network, and export controls."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["intelligence"])


# ── Forensic Analysis Endpoints ──────────────────────────────────────────


@router.get("/forensics/{ticker}")
async def get_forensic_reports(
    ticker: str,
    days: int = Query(90, ge=1, le=365, description="Lookback window in days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return all stored forensic reports for a ticker.

    Forensic reports reconstruct what events preceded significant
    price moves, identifying who was active and what signals fired.
    """
    try:
        from intelligence.forensics import load_forensic_reports, generate_forensic_summary

        engine = get_db_engine()
        reports = load_forensic_reports(engine, ticker, days=days)

        summary: str | None = None
        if reports:
            try:
                summary = generate_forensic_summary(engine, ticker, days=days)
            except Exception as exc:
                log.debug("Forensic summary generation failed: {e}", e=str(exc))

        return {
            "ticker": ticker.upper(),
            "reports": reports,
            "count": len(reports),
            "days": days,
            "summary": summary,
        }
    except Exception as exc:
        log.warning("Forensic reports endpoint failed for {t}: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker.upper(), "reports": [], "count": 0, "error": str(exc)}


@router.post("/forensics/{ticker}/analyze")
async def analyze_forensic_move(
    ticker: str,
    date: str = Query(..., description="Move date in YYYY-MM-DD format"),
    lookback: int = Query(14, ge=1, le=60, description="Lookback days before the move"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Analyze a specific price move forensically.

    Reconstructs the event timeline preceding the move, identifies
    key actors and aligned signals, and generates a narrative.
    """
    try:
        from intelligence.forensics import analyze_move

        engine = get_db_engine()
        report = analyze_move(engine, ticker, date, lookback_days=lookback)

        if report is None:
            return {
                "ticker": ticker.upper(),
                "date": date,
                "error": "No price data found for the specified date.",
            }

        return {
            "ticker": ticker.upper(),
            "date": date,
            "report": report.to_dict(),
        }
    except Exception as exc:
        log.warning(
            "Forensic analysis endpoint failed for {t} on {d}: {e}",
            t=ticker, d=date, e=str(exc),
        )
        return {"ticker": ticker.upper(), "date": date, "error": str(exc)}


# ── Causation Endpoints ─────────────────────────────────────────────────


@router.get("/causation")
async def get_causation(
    ticker: str | None = Query(None, description="Filter by ticker"),
    days: int = Query(30, ge=1, le=365, description="Look-back window in days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return causal links for recent trading activity.

    If ticker is provided, generates a causal narrative for that ticker
    and returns causes for its recent signals.  Otherwise returns batch
    results across all recent signals.
    """
    try:
        from intelligence.causation import (
            find_causes as _find_causes,
            batch_find_causes as _batch,
            generate_causal_narrative as _narrative,
        )

        engine = get_db_engine()

        if ticker:
            ticker_upper = ticker.strip().upper()
            narrative = _narrative(engine, ticker_upper)

            from datetime import date as _date, timedelta as _td
            from sqlalchemy import text as _text

            cutoff = _date.today() - _td(days=days)
            with engine.connect() as conn:
                rows = conn.execute(
                    _text(
                        "SELECT id, source_id, signal_type, signal_date "
                        "FROM signal_sources "
                        "WHERE ticker = :t AND signal_date >= :c "
                        "AND source_type IN ('congressional', 'insider') "
                        "ORDER BY signal_date DESC "
                        "LIMIT 20"
                    ),
                    {"t": ticker_upper, "c": cutoff},
                ).fetchall()

            causes = []
            for row in rows:
                found = _find_causes(
                    engine, row[1], row[2], ticker_upper, str(row[3]),
                    signal_id=row[0],
                )
                causes.extend([c.to_dict() for c in found])

            return {
                "ticker": ticker_upper,
                "days": days,
                "narrative": narrative,
                "causes": causes[:100],
                "total_causes": len(causes),
            }

        all_causes = _batch(engine, days=days)
        return {
            "days": days,
            "causes": [c.to_dict() for c in all_causes[:200]],
            "total_causes": len(all_causes),
        }

    except Exception as exc:
        log.warning("Causation endpoint failed: {e}", e=str(exc))
        return {"error": str(exc), "causes": [], "total_causes": 0}


@router.get("/causation/suspicious")
async def get_suspicious_trades_endpoint(
    days: int = Query(90, ge=1, le=365, description="Look-back window in days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return trades flagged as potentially informed by non-public information.

    Detects:
      - Congressional trades with committee jurisdiction overlap + active legislation
      - Insider buys preceding government contract awards
      - Insider sells preceding earnings misses
    """
    try:
        from intelligence.causation import get_suspicious_trades as _suspicious

        engine = get_db_engine()
        trades = _suspicious(engine, days=days)

        return {
            "days": days,
            "suspicious_trades": trades[:200],
            "total": len(trades),
        }

    except Exception as exc:
        log.warning("Suspicious trades endpoint failed: {e}", e=str(exc))
        return {"error": str(exc), "suspicious_trades": [], "total": 0}


@router.get("/causation/narrative/{ticker}")
async def get_causal_narrative_endpoint(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate a narrative explaining why people are trading a specific ticker."""
    try:
        from intelligence.causation import generate_causal_narrative as _narrative

        engine = get_db_engine()
        narrative = _narrative(engine, ticker.strip().upper())

        return {
            "ticker": ticker.strip().upper(),
            "narrative": narrative,
        }

    except Exception as exc:
        log.warning(
            "Causal narrative for {t} failed: {e}", t=ticker, e=str(exc),
        )
        return {"ticker": ticker.strip().upper(), "narrative": "", "error": str(exc)}


# ── Causal Chain Endpoints ─────────────────────────────────────────────


@router.get("/causal-chains")
async def get_causal_chains(
    ticker: str | None = Query(None, description="Filter by ticker"),
    hops: int = Query(5, ge=2, le=10, description="Max hops for chain tracing"),
    days: int = Query(180, ge=1, le=730, description="Look-back window for longest chains"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trace multi-hop causal chains for a ticker or find longest chains globally.

    Chains trace paths like: lobbying -> legislation -> contract award ->
    stock price move -> insider sale.

    If ticker is provided, traces chains for that ticker (up to `hops` deep).
    Otherwise, finds the longest chains across all tickers in the system.
    """
    try:
        from intelligence.causation import (
            trace_causal_chain,
            find_longest_chains,
        )

        engine = get_db_engine()

        if ticker:
            ticker_upper = ticker.strip().upper()
            chains = trace_causal_chain(engine, ticker_upper, max_hops=hops)
            return {
                "ticker": ticker_upper,
                "max_hops": hops,
                "chains": [c.to_dict() for c in chains[:50]],
                "total_chains": len(chains),
                "longest_chain": chains[0].total_hops if chains else 0,
            }

        chains = find_longest_chains(engine, days=days)
        return {
            "days": days,
            "chains": [c.to_dict() for c in chains[:100]],
            "total_chains": len(chains),
            "longest_chain": chains[0].total_hops if chains else 0,
            "tickers_covered": list({c.ticker for c in chains}),
        }

    except Exception as exc:
        log.warning("Causal chains endpoint failed: {e}", e=str(exc))
        return {"error": str(exc), "chains": [], "total_chains": 0}


@router.get("/causal-chains/active")
async def get_active_causal_chains(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect causal chains currently in progress.

    Identifies tickers where the early stages of a known causal pattern
    are unfolding — e.g., lobbying spend increase + legislative hearings
    scheduled + insider buying = something is coming.
    """
    try:
        from intelligence.causation import detect_chain_in_progress

        engine = get_db_engine()
        active = detect_chain_in_progress(engine)

        return {
            "active_patterns": active[:50],
            "total": len(active),
            "tickers_with_active_chains": list({p["ticker"] for p in active}),
        }

    except Exception as exc:
        log.warning("Active causal chains endpoint failed: {e}", e=str(exc))
        return {"error": str(exc), "active_patterns": [], "total": 0}


# ── Influence Network Endpoints ─────────────────────────────────────────

_influence_graph_cache: dict[str, Any] = {"data": None, "ts": None}
_INFLUENCE_GRAPH_TTL = 1800  # 30 minutes


@router.get("/influence")
async def get_influence_network(
    ticker: str | None = Query(None, description="Filter by ticker symbol"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return influence network data — the money-in-politics graph.

    Without ticker parameter: returns the full influence graph (cached 30 min).
    With ticker parameter: returns influence data for that specific company.
    """
    from datetime import datetime, timezone

    try:
        engine = get_db_engine()

        if ticker:
            from intelligence.influence_network import get_influence_for_ticker
            return get_influence_for_ticker(engine, ticker.strip().upper())

        now = datetime.now(timezone.utc)
        if (
            _influence_graph_cache["data"]
            and _influence_graph_cache["ts"]
            and (now - _influence_graph_cache["ts"]).total_seconds() < _INFLUENCE_GRAPH_TTL
        ):
            return _influence_graph_cache["data"]

        from intelligence.influence_network import build_influence_graph
        result = build_influence_graph(engine)
        _influence_graph_cache["data"] = result
        _influence_graph_cache["ts"] = now
        return result

    except Exception as exc:
        log.warning("Influence network endpoint failed: {e}", e=str(exc))
        return {"nodes": [], "links": [], "metadata": {}, "error": str(exc)}


@router.get("/influence/circular-flows")
async def get_circular_flows(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect circular flows: Company -> lobbies -> Member -> votes -> Bill -> funds -> Company."""
    try:
        from intelligence.influence_network import detect_circular_flows

        engine = get_db_engine()
        loops = detect_circular_flows(engine)
        return {
            "loops": [l.to_dict() for l in loops],
            "total": len(loops),
            "circular_count": sum(1 for l in loops if l.circular_flow_detected),
        }

    except Exception as exc:
        log.warning("Circular flows endpoint failed: {e}", e=str(exc))
        return {"loops": [], "total": 0, "circular_count": 0, "error": str(exc)}


@router.get("/influence/hypocrisy")
async def get_vote_trade_hypocrisy(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect vote/trade hypocrisy — members who vote one way but trade another."""
    try:
        from intelligence.influence_network import vote_trade_hypocrisy

        engine = get_db_engine()
        flags = vote_trade_hypocrisy(engine)
        return {
            "flags": flags,
            "total": len(flags),
            "members_flagged": len({f["member"] for f in flags}),
        }

    except Exception as exc:
        log.warning("Vote-trade hypocrisy endpoint failed: {e}", e=str(exc))
        return {"flags": [], "total": 0, "members_flagged": 0, "error": str(exc)}


# ── Export Controls Endpoints ──────────────────────────────────────────────


@router.get("/export-controls")
async def get_export_controls(
    ticker: str | None = Query(None, description="Filter by stock ticker (e.g. NVDA, ASML)"),
    days: int = Query(90, ge=1, le=730, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return export control actions affecting semiconductor/tech companies.

    If ticker is provided, returns actions for that company only plus
    a revenue impact assessment. Otherwise returns all recent actions.

    For companies like NVIDIA, export controls to China have been more
    material than the CHIPS Act (~25% of revenue at risk).
    """
    try:
        from intelligence.export_intel import (
            get_recent_controls,
            get_controls_for_ticker,
            assess_revenue_impact,
        )

        engine = get_db_engine()

        if ticker:
            controls = get_controls_for_ticker(engine, ticker)
        else:
            controls = get_recent_controls(engine, days=days)

        result: dict[str, Any] = {
            "controls": [c.to_dict() for c in controls],
            "total": len(controls),
            "ticker": ticker,
            "days": days,
        }

        if ticker:
            try:
                impact = assess_revenue_impact(engine, ticker)
                result["revenue_impact"] = impact
            except Exception as exc:
                log.debug("Revenue impact assessment failed: {e}", e=str(exc))
                result["revenue_impact"] = None

        return result

    except Exception as exc:
        log.warning("Export controls endpoint failed: {e}", e=str(exc))
        return {
            "controls": [],
            "total": 0,
            "ticker": ticker,
            "days": days,
            "error": str(exc),
        }


@router.get("/export-controls/impact")
async def get_export_control_impact(
    ticker: str = Query(..., description="Stock ticker (e.g. NVDA, ASML, LRCX)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Assess revenue impact of export controls for a specific company.

    Returns estimated % of revenue at risk, active restriction count,
    severity assessment, and China revenue baseline data.
    """
    try:
        from intelligence.export_intel import assess_revenue_impact

        engine = get_db_engine()
        impact = assess_revenue_impact(engine, ticker)
        return impact

    except Exception as exc:
        log.warning("Export control impact endpoint failed: {e}", e=str(exc))
        return {
            "ticker": ticker,
            "risk_level": "UNKNOWN",
            "error": str(exc),
        }
