"""Intelligence sub-router: Government contracts, dollar flows, and legislative intelligence."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["intelligence"])


# ── Government Contract Endpoints ────────────────────────────────────────


@router.get("/gov-contracts")
async def get_gov_contracts(
    ticker: str | None = Query(None, description="Filter by stock ticker"),
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return government contract awards, optionally filtered by ticker.

    If ticker is provided, returns contracts for that company only.
    Otherwise returns all contracts in the lookback window.
    Includes insider/congressional overlap detection when ticker is specified.
    """
    try:
        from intelligence.gov_intel import (
            get_recent_contracts,
            get_contracts_for_ticker,
            detect_contract_insider_overlap,
        )

        engine = get_db_engine()

        if ticker:
            contracts = get_contracts_for_ticker(engine, ticker)
        else:
            contracts = get_recent_contracts(engine, days=days)

        result: dict[str, Any] = {
            "contracts": [c.to_dict() for c in contracts],
            "total": len(contracts),
            "ticker": ticker,
            "days": days,
        }

        if ticker:
            try:
                overlaps = detect_contract_insider_overlap(engine, lookback_days=days)
                ticker_overlaps = [
                    o.to_dict() for o in overlaps
                    if o.ticker == ticker.strip().upper()
                ]
                result["insider_overlaps"] = ticker_overlaps
            except Exception as exc:
                log.debug("Overlap detection failed: {e}", e=str(exc))
                result["insider_overlaps"] = []

        return result

    except Exception as exc:
        log.warning("Gov contracts endpoint failed: {e}", e=str(exc))
        return {
            "contracts": [],
            "total": 0,
            "ticker": ticker,
            "days": days,
            "error": str(exc),
        }


@router.get("/gov-contracts/overlaps")
async def get_contract_insider_overlaps(
    days: int = Query(90, ge=1, le=365, description="Lookback days for contracts"),
    window: int = Query(30, ge=1, le=90, description="Pre-contract trade window in days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect insider/congressional trades that preceded government contract awards.

    Returns cases where a BUY signal from an insider or congressional member
    occurred within the specified window before a contract award for the same company.
    Sorted by suspicion score (higher = more suspicious).
    """
    try:
        from intelligence.gov_intel import detect_contract_insider_overlap

        engine = get_db_engine()
        overlaps = detect_contract_insider_overlap(
            engine,
            lookback_days=days,
            pre_contract_window_days=window,
        )

        return {
            "overlaps": [o.to_dict() for o in overlaps],
            "total": len(overlaps),
            "lookback_days": days,
            "pre_contract_window_days": window,
        }

    except Exception as exc:
        log.warning("Contract overlap endpoint failed: {e}", e=str(exc))
        return {
            "overlaps": [],
            "total": 0,
            "error": str(exc),
        }


# ── Dollar Flow Endpoints ────────────────────────────────────────────────


@router.get("/dollar-flows")
async def get_dollar_flows(
    ticker: str | None = Query(None, description="Filter by ticker"),
    sector: str | None = Query(None, description="Filter by sector"),
    days: int = Query(30, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return normalized dollar flows across all signal sources.

    Converts congressional trades, insider filings, dark pool activity,
    13F position changes, ETF flows, whale options, and prediction market
    signals into estimated USD amounts for apples-to-apples comparison.
    """
    try:
        from intelligence.dollar_flows import (
            get_flows_by_ticker,
            get_flows_by_sector,
            get_aggregate_flows,
            get_biggest_movers,
        )

        engine = get_db_engine()

        if ticker:
            flows = get_flows_by_ticker(engine, ticker, days=days)
            return {
                "flows": flows,
                "count": len(flows),
                "ticker": ticker,
                "days": days,
            }

        if sector:
            flows = get_flows_by_sector(engine, sector, days=days)
            return {
                "flows": flows,
                "count": len(flows),
                "sector": sector,
                "days": days,
            }

        aggregates = get_aggregate_flows(engine, days=days)
        movers = get_biggest_movers(engine, days=min(days, 7))

        return {
            "aggregates": aggregates,
            "biggest_movers": movers,
            "days": days,
        }

    except Exception as exc:
        log.warning("Dollar flows endpoint failed: {e}", e=str(exc))
        return {
            "flows": [],
            "aggregates": {},
            "biggest_movers": [],
            "error": str(exc),
        }


@router.post("/dollar-flows/normalize")
async def trigger_dollar_flow_normalization(
    days: int = Query(90, ge=1, le=365, description="Lookback days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Trigger a full dollar flow normalization cycle.

    Scans all signal sources and raw_series within the lookback window,
    converts every signal to estimated USD, and persists to the
    dollar_flows table.
    """
    try:
        from intelligence.dollar_flows import normalize_all_flows

        engine = get_db_engine()
        flows = normalize_all_flows(engine, days=days)

        return {
            "normalized": len(flows),
            "days": days,
            "status": "ok",
        }

    except Exception as exc:
        log.warning("Dollar flow normalization failed: {e}", e=str(exc))
        return {"normalized": 0, "error": str(exc)}


# ── Legislative Intelligence Endpoints ──────────────────────────────────


@router.get("/legislation")
async def get_legislation_overview(
    ticker: str | None = Query(None, description="Filter by affected ticker"),
    committee: str | None = Query(None, description="Filter by committee name"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Legislative intelligence: bills, hearings, and trading alerts.

    Returns upcoming hearings, bills affecting a ticker, and the key
    insight -- committee members trading in sectors their committee is
    actively legislating.

    Parameters:
        ticker: Optional ticker to filter results.
        committee: Optional committee name to filter results.
    """
    try:
        from intelligence.legislative_intel import (
            get_upcoming_hearings,
            get_bills_affecting_ticker,
            detect_legislative_trading,
            get_legislation_summary,
        )

        engine = get_db_engine()

        if ticker:
            bills = get_bills_affecting_ticker(engine, ticker)
            trade_alerts = detect_legislative_trading(engine, days_back=30)
            ticker_alerts = [a for a in trade_alerts if a["ticker"] == ticker.upper()]
            return {
                "ticker": ticker.upper(),
                "bills": bills[:50],
                "trade_alerts": ticker_alerts,
                "hearings": get_upcoming_hearings(engine, days=14),
            }

        if committee:
            hearings = get_upcoming_hearings(engine, days=14)
            committee_lower = committee.lower()
            filtered_hearings = [
                h for h in hearings
                if any(committee_lower in c.lower() for c in h.get("committees", []))
            ]
            trade_alerts = detect_legislative_trading(engine, days_back=30)
            committee_alerts = [
                a for a in trade_alerts
                if committee_lower in a.get("committee", "").lower()
            ]
            return {
                "committee": committee,
                "hearings": filtered_hearings,
                "trade_alerts": committee_alerts,
            }

        return get_legislation_summary(engine)

    except Exception as exc:
        log.warning("Legislation endpoint failed: {e}", e=str(exc))
        return {
            "upcoming_hearings_count": 0,
            "upcoming_hearings": [],
            "trade_alerts_count": 0,
            "high_severity_alerts": [],
            "medium_severity_alerts": [],
            "most_legislated_tickers": [],
            "error": str(exc),
        }


@router.get("/legislation/hearings")
async def get_legislation_hearings(
    days: int = Query(14, ge=1, le=60, description="Days ahead to search"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return upcoming committee hearings with sector/ticker impact."""
    try:
        from intelligence.legislative_intel import get_upcoming_hearings

        engine = get_db_engine()
        hearings = get_upcoming_hearings(engine, days=days)
        return {"hearings": hearings, "count": len(hearings)}
    except Exception as exc:
        log.warning("Legislation hearings endpoint failed: {e}", e=str(exc))
        return {"hearings": [], "count": 0, "error": str(exc)}


@router.get("/legislation/trading-alerts")
async def get_legislation_trading_alerts(
    days: int = Query(30, ge=1, le=90, description="Days back to search"),
    severity: str | None = Query(None, description="Filter by severity: HIGH, MEDIUM"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Detect committee members trading in sectors they oversee.

    This is the key intelligence output -- flags potentially informed
    trading by members of Congress.
    """
    try:
        from intelligence.legislative_intel import detect_legislative_trading

        engine = get_db_engine()
        alerts = detect_legislative_trading(engine, days_back=days)

        if severity:
            alerts = [a for a in alerts if a["severity"] == severity.upper()]

        return {
            "alerts": alerts,
            "count": len(alerts),
            "high_count": sum(1 for a in alerts if a["severity"] == "HIGH"),
            "medium_count": sum(1 for a in alerts if a["severity"] == "MEDIUM"),
        }
    except Exception as exc:
        log.warning("Legislation trading alerts endpoint failed: {e}", e=str(exc))
        return {"alerts": [], "count": 0, "error": str(exc)}
