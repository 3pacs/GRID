"""Options scanner API endpoints."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/options", tags=["options"])


# ── Recommendation helpers ───────────────────────────────────


def _persist_recommendations(engine, recommendations: list[dict]) -> int:
    """Insert new recommendations into the log table, skipping duplicates.

    Duplicates are identified by (ticker, strike, expiry).
    Returns the number of newly inserted rows.
    """
    from sqlalchemy import text

    if not recommendations:
        return 0

    inserted = 0
    with engine.begin() as conn:
        for rec in recommendations:
            # Check for existing duplicate
            exists = conn.execute(
                text(
                    "SELECT 1 FROM options_recommendations "
                    "WHERE ticker = :ticker AND strike = :strike AND expiry = :expiry "
                    "LIMIT 1"
                ),
                {
                    "ticker": rec.get("ticker"),
                    "strike": rec.get("strike"),
                    "expiry": rec.get("expiry"),
                },
            ).fetchone()
            if exists:
                continue

            conn.execute(
                text(
                    "INSERT INTO options_recommendations "
                    "(ticker, direction, strike, expiry, entry_price, target_price, "
                    "stop_loss, expected_return, kelly_fraction, confidence, thesis, "
                    "dealer_context, sanity_status, generated_at) "
                    "VALUES (:ticker, :direction, :strike, :expiry, :entry_price, "
                    ":target_price, :stop_loss, :expected_return, :kelly_fraction, "
                    ":confidence, :thesis, :dealer_context, :sanity_status, :generated_at)"
                ),
                {
                    "ticker": rec.get("ticker"),
                    "direction": rec.get("direction"),
                    "strike": rec.get("strike"),
                    "expiry": rec.get("expiry"),
                    "entry_price": rec.get("entry_price"),
                    "target_price": rec.get("target_price"),
                    "stop_loss": rec.get("stop_loss"),
                    "expected_return": rec.get("expected_return"),
                    "kelly_fraction": rec.get("kelly_fraction"),
                    "confidence": rec.get("confidence"),
                    "thesis": rec.get("thesis"),
                    "dealer_context": rec.get("dealer_context"),
                    "sanity_status": rec.get("sanity_status"),
                    "generated_at": rec.get("generated_at", datetime.now(timezone.utc).isoformat()),
                },
            )
            inserted += 1

    return inserted


def _format_recommendation_response(
    recommendations: list[dict],
    scan_summary: dict | None = None,
    generated_at: str | None = None,
) -> dict:
    """Build the standard response envelope for recommendations."""
    now = generated_at or datetime.now(timezone.utc).isoformat()
    summary = scan_summary or {
        "total_scanned": len(recommendations),
        "passed_sanity": len(recommendations),
        "rejected": 0,
    }
    return {
        "recommendations": recommendations,
        "generated_at": now,
        "scan_summary": summary,
    }


# ── Recommendation endpoints ────────────────────────────────


@router.get("/recommendations")
async def get_recommendations(
    ticker: str | None = Query(None, description="Filter to a single ticker"),
    _token: str = Depends(require_auth),
) -> dict:
    """Generate options trade recommendations and persist new ones."""
    try:
        from trading.options_recommender import generate_recommendations

        engine = get_db_engine()
        result = generate_recommendations(engine)

        # result is expected to be a dict with at least 'recommendations' list
        recommendations = result.get("recommendations", [])
        scan_summary = result.get("scan_summary")
        generated_at = result.get("generated_at", datetime.now(timezone.utc).isoformat())

        # Persist new recommendations (skip duplicates)
        _persist_recommendations(engine, recommendations)

        # Filter by ticker if requested
        if ticker:
            recommendations = [
                r for r in recommendations if r.get("ticker", "").upper() == ticker.upper()
            ]

        return _format_recommendation_response(recommendations, scan_summary, generated_at)

    except ImportError:
        log.warning("trading.options_recommender module not available")
        raise HTTPException(
            status_code=501,
            detail="Options recommender module is not installed",
        )
    except Exception as exc:
        log.error("Recommendation generation failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=500,
            detail=f"Recommendation generation failed: {exc}",
        )


@router.post("/recommendations/refresh")
async def refresh_recommendations(
    _token: str = Depends(require_auth),
) -> dict:
    """Force a fresh recommendation scan, bypassing any cache."""
    try:
        from trading.options_recommender import generate_recommendations

        engine = get_db_engine()
        result = generate_recommendations(engine, force_refresh=True)

        recommendations = result.get("recommendations", [])
        scan_summary = result.get("scan_summary")
        generated_at = result.get("generated_at", datetime.now(timezone.utc).isoformat())

        # Persist new recommendations (skip duplicates)
        _persist_recommendations(engine, recommendations)

        return _format_recommendation_response(recommendations, scan_summary, generated_at)

    except ImportError:
        log.warning("trading.options_recommender module not available")
        raise HTTPException(
            status_code=501,
            detail="Options recommender module is not installed",
        )
    except Exception as exc:
        log.error("Recommendation refresh failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=500,
            detail=f"Recommendation refresh failed: {exc}",
        )


@router.get("/recommendations/history")
async def get_recommendation_history(
    ticker: str | None = Query(None, description="Filter by ticker"),
    outcome: str | None = Query(None, description="Filter by outcome (WIN/LOSS/EXPIRED/OPEN)"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict:
    """Return past recommendations with outcome data from the log table."""
    from sqlalchemy import text

    engine = get_db_engine()

    base_query = (
        "SELECT id, ticker, direction, strike, expiry, entry_price, target_price, "
        "stop_loss, expected_return, kelly_fraction, confidence, thesis, "
        "dealer_context, sanity_status, generated_at, outcome, actual_return, closed_at "
        "FROM options_recommendations WHERE 1=1"
    )
    count_query = "SELECT COUNT(*) FROM options_recommendations WHERE 1=1"
    params: dict[str, Any] = {"lim": limit, "off": offset}

    if ticker:
        base_query += " AND ticker = :ticker"
        count_query += " AND ticker = :ticker"
        params["ticker"] = ticker.upper()
    if outcome:
        base_query += " AND outcome = :outcome"
        count_query += " AND outcome = :outcome"
        params["outcome"] = outcome.upper()

    base_query += " ORDER BY generated_at DESC LIMIT :lim OFFSET :off"

    with engine.connect() as conn:
        total_row = conn.execute(text(count_query), params).fetchone()
        total = total_row[0] if total_row else 0

        rows = conn.execute(text(base_query), params).fetchall()

    history = [
        {
            "id": r[0],
            "ticker": r[1],
            "direction": r[2],
            "strike": float(r[3]) if r[3] is not None else None,
            "expiry": str(r[4]) if r[4] else None,
            "entry_price": float(r[5]) if r[5] is not None else None,
            "target_price": float(r[6]) if r[6] is not None else None,
            "stop_loss": float(r[7]) if r[7] is not None else None,
            "expected_return": float(r[8]) if r[8] is not None else None,
            "kelly_fraction": float(r[9]) if r[9] is not None else None,
            "confidence": float(r[10]) if r[10] is not None else None,
            "thesis": r[11],
            "dealer_context": r[12],
            "sanity_status": r[13],
            "generated_at": r[14].isoformat() if r[14] else None,
            "outcome": r[15],
            "actual_return": float(r[16]) if r[16] is not None else None,
            "closed_at": r[17].isoformat() if r[17] else None,
        }
        for r in rows
    ]

    return {
        "history": history,
        "count": len(history),
        "total": total,
        "limit": limit,
        "offset": offset,
    }


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
