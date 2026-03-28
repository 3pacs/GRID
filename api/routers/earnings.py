"""Earnings calendar & prediction endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/earnings", tags=["earnings"])


# ── GET /calendar ────────────────────────────────────────────────────────

@router.get("/calendar")
async def get_earnings_calendar(
    days_ahead: int = Query(30, ge=1, le=90),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Upcoming earnings calendar with IV enrichment and predictions."""
    engine = get_db_engine()
    try:
        from intelligence.earnings_intel import get_earnings_calendar as _get_cal
        entries = _get_cal(engine, days_ahead=days_ahead)
        return {"entries": entries, "count": len(entries), "days_ahead": days_ahead}
    except Exception as exc:
        log.error("Earnings calendar failed: {e}", e=str(exc))
        return {"entries": [], "count": 0, "error": str(exc)}


# ── GET /recent ──────────────────────────────────────────────────────────

@router.get("/recent")
async def get_recent_earnings(
    days_back: int = Query(30, ge=1, le=180),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Recently reported earnings with surprise data."""
    engine = get_db_engine()
    try:
        from ingestion.altdata.earnings_calendar import get_recent_earnings as _get_recent
        entries = _get_recent(engine, days_back=days_back)
        return {"entries": entries, "count": len(entries), "days_back": days_back}
    except Exception as exc:
        log.error("Recent earnings failed: {e}", e=str(exc))
        return {"entries": [], "count": 0, "error": str(exc)}


# ── GET /surprise/{ticker} ──────────────────────────────────────────────

@router.get("/surprise/{ticker}")
async def get_earnings_surprise(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Post-earnings surprise analysis for a ticker."""
    engine = get_db_engine()
    try:
        from intelligence.earnings_intel import analyze_earnings_surprise
        return analyze_earnings_surprise(engine, ticker.upper())
    except Exception as exc:
        log.error("Earnings surprise analysis failed for {t}: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker, "error": str(exc)}


# ── POST /predict/{ticker} ──────────────────────────────────────────────

@router.post("/predict/{ticker}")
async def predict_earnings(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Generate or retrieve pre-earnings reaction prediction."""
    engine = get_db_engine()
    try:
        from intelligence.earnings_intel import predict_earnings_reaction
        return predict_earnings_reaction(engine, ticker.upper())
    except Exception as exc:
        log.error("Earnings prediction failed for {t}: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker, "error": str(exc)}


# ── GET /scorecard ───────────────────────────────────────────────────────

@router.get("/scorecard")
async def get_earnings_scorecard(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Prediction scorecard: accuracy, calibration, recent outcomes."""
    engine = get_db_engine()
    try:
        from intelligence.earnings_intel import get_prediction_scorecard
        return get_prediction_scorecard(engine)
    except Exception as exc:
        log.error("Earnings scorecard failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ── GET /history/{ticker} ───────────────────────────────────────────────

@router.get("/history/{ticker}")
async def get_earnings_history(
    ticker: str,
    limit: int = Query(20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Full earnings history for a ticker."""
    engine = get_db_engine()
    try:
        from ingestion.altdata.earnings_calendar import get_earnings_history as _get_hist
        entries = _get_hist(engine, ticker.upper(), limit=limit)
        return {"ticker": ticker.upper(), "entries": entries, "count": len(entries)}
    except Exception as exc:
        log.error("Earnings history failed for {t}: {e}", t=ticker, e=str(exc))
        return {"ticker": ticker, "entries": [], "error": str(exc)}


# ── POST /cycle ──────────────────────────────────────────────────────────

@router.post("/cycle")
async def run_earnings_cycle(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run a full earnings intelligence cycle: score + predict."""
    engine = get_db_engine()
    try:
        from intelligence.earnings_intel import run_earnings_cycle as _cycle
        return _cycle(engine)
    except Exception as exc:
        log.error("Earnings cycle failed: {e}", e=str(exc))
        return {"error": str(exc)}
