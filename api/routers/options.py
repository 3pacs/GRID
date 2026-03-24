"""Options scanner API endpoints."""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/options", tags=["options"])


@router.get("/signals")
async def get_options_signals(
    ticker: str | None = Query(None, description="Filter by ticker"),
    limit: int = Query(50, ge=1, le=500),
    _token: str = Depends(require_auth),
) -> dict:
    """Return latest options daily signals."""
    from sqlalchemy import text

    engine = get_db_engine()
    with engine.connect() as conn:
        if ticker:
            rows = conn.execute(
                text(
                    "SELECT ticker, signal_date, put_call_ratio, max_pain, "
                    "iv_skew, total_oi, total_volume, near_expiry, spot_price, "
                    "iv_atm, term_structure_slope, oi_concentration "
                    "FROM options_daily_signals "
                    "WHERE ticker = :ticker "
                    "ORDER BY signal_date DESC LIMIT :lim"
                ),
                {"ticker": ticker, "lim": limit},
            ).fetchall()
        else:
            rows = conn.execute(
                text(
                    "SELECT ticker, signal_date, put_call_ratio, max_pain, "
                    "iv_skew, total_oi, total_volume, near_expiry, spot_price, "
                    "iv_atm, term_structure_slope, oi_concentration "
                    "FROM options_daily_signals "
                    "ORDER BY signal_date DESC, ticker LIMIT :lim"
                ),
                {"lim": limit},
            ).fetchall()

    signals = [
        {
            "ticker": r[0],
            "signal_date": str(r[1]),
            "put_call_ratio": r[2],
            "max_pain": r[3],
            "iv_skew": r[4],
            "total_oi": r[5],
            "total_volume": r[6],
            "near_expiry": str(r[7]) if r[7] else None,
            "spot_price": r[8],
            "iv_atm": r[9],
            "term_structure_slope": r[10],
            "oi_concentration": r[11],
        }
        for r in rows
    ]

    return {"signals": signals, "count": len(signals)}


@router.get("/scan")
async def scan_mispricing(
    min_score: float = Query(5.0, ge=0, le=10, description="Minimum score"),
    _token: str = Depends(require_auth),
) -> dict:
    """Run the mispricing scanner and return flagged opportunities."""
    try:
        from discovery.options_scanner import OptionsScanner

        engine = get_db_engine()
        scanner = OptionsScanner(engine)
        opps = scanner.scan_all(min_score=min_score)

        results = [
            {
                "ticker": o.ticker,
                "scan_date": str(o.scan_date),
                "score": o.score,
                "estimated_payoff_multiple": o.estimated_payoff_multiple,
                "direction": o.direction,
                "thesis": o.thesis,
                "strikes": o.strikes,
                "expiry": o.expiry,
                "spot_price": o.spot_price,
                "iv_atm": o.iv_atm,
                "confidence": o.confidence,
                "is_100x": o.is_100x,
            }
            for o in opps
        ]

        return {
            "opportunities": results,
            "count": len(results),
            "count_100x": sum(1 for o in opps if o.is_100x),
        }
    except Exception as exc:
        log.warning("Options scan failed: {e}", e=str(exc))
        return {"opportunities": [], "count": 0, "error": str(exc)}


@router.get("/100x")
async def get_100x_opportunities(
    _token: str = Depends(require_auth),
) -> dict:
    """Return only 100x+ flagged mispricing opportunities."""
    try:
        from discovery.options_scanner import OptionsScanner

        engine = get_db_engine()
        scanner = OptionsScanner(engine)
        opps = scanner.get_100x_opportunities()

        results = [
            {
                "ticker": o.ticker,
                "scan_date": str(o.scan_date),
                "score": o.score,
                "estimated_payoff_multiple": o.estimated_payoff_multiple,
                "direction": o.direction,
                "thesis": o.thesis,
                "strikes": o.strikes,
                "expiry": o.expiry,
                "spot_price": o.spot_price,
                "iv_atm": o.iv_atm,
                "confidence": o.confidence,
            }
            for o in opps
        ]

        return {"opportunities": results, "count": len(results)}
    except Exception as exc:
        log.warning("100x scan failed: {e}", e=str(exc))
        return {"opportunities": [], "count": 0, "error": str(exc)}


@router.get("/history")
async def get_scan_history(
    ticker: str | None = Query(None),
    days: int = Query(30, ge=1, le=365),
    only_100x: bool = Query(False),
    limit: int = Query(100, ge=1, le=500),
    _token: str = Depends(require_auth),
) -> dict:
    """Return historical mispricing scan results."""
    from sqlalchemy import text

    engine = get_db_engine()

    # Build query safely — all conditions use parameterized placeholders
    base_query = (
        "SELECT ticker, scan_date, score, payoff_multiple, direction, "
        "thesis, confidence, is_100x, spot_price, iv_atm "
        "FROM options_mispricing_scans "
        "WHERE scan_date >= CURRENT_DATE - make_interval(days => :days)"
    )
    params: dict[str, Any] = {"days": days, "lim": limit}

    if ticker:
        base_query += " AND ticker = :ticker"
        params["ticker"] = ticker
    if only_100x:
        base_query += " AND is_100x = TRUE"

    base_query += " ORDER BY score DESC LIMIT :lim"

    with engine.connect() as conn:
        rows = conn.execute(
            text(base_query), params,
        ).fetchall()

    results = [
        {
            "ticker": r[0],
            "scan_date": str(r[1]),
            "score": r[2],
            "payoff_multiple": r[3],
            "direction": r[4],
            "thesis": r[5],
            "confidence": r[6],
            "is_100x": r[7],
            "spot_price": r[8],
            "iv_atm": r[9],
        }
        for r in rows
    ]

    return {"history": results, "count": len(results)}
