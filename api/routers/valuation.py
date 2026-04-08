"""
GRID API — Valuation & Derivatives Support endpoints.

Provides:
  - GET /valuation/analyze/{ticker} — Full composite valuation
  - GET /valuation/prompt/{ticker} — Generate Claude Max prompt
  - POST /valuation/response — Log Claude Max response + predictions
  - GET /valuation/history/{ticker} — Valuation history timeline
  - GET /valuation/predictions/{ticker} — Prediction accuracy tracking
  - GET /valuation/milestones/{ticker} — Company milestones
  - POST /valuation/milestones — Add a milestone
  - PUT /valuation/milestones/{id}/status — Update milestone status
  - GET /valuation/derivatives/{ticker} — Derivatives support analysis
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from pydantic import BaseModel, Field
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(
    prefix="/api/v1/valuation",
    tags=["valuation"],
    dependencies=[Depends(require_auth)],
)


# ── Request/Response models ─────────────────────────────────────────

class MilestoneCreate(BaseModel):
    ticker: str
    milestone_type: str
    announced_date: str
    description: str
    target_date: str | None = None
    target_value: float | None = None
    target_unit: str | None = None
    probability: float = Field(default=0.5, ge=0, le=1)
    confidence_source: str = "ANALYST"
    value_impact_ps: float | None = None
    value_impact_pct: float | None = None
    status: str = "PENDING"
    source_url: str | None = None
    notes: str | None = None


class MilestoneStatusUpdate(BaseModel):
    status: str
    actual_value: float | None = None
    actual_date: str | None = None
    notes: str | None = None


class AnalysisResponse(BaseModel):
    analysis_id: str
    ticker: str
    response_text: str
    predictions: list[dict[str, Any]] = Field(default_factory=list)


# ── Lazy imports ────────────────────────────────────────────────────

def _get_composite_engine():
    from valuation.composite import CompositeValuationEngine
    return CompositeValuationEngine(get_db_engine())


def _get_milestone_tracker():
    from valuation.milestones import MilestoneTracker
    return MilestoneTracker(get_db_engine())


def _get_derivatives_engine():
    from valuation.derivatives_support import DerivativesSupportEngine
    return DerivativesSupportEngine(get_db_engine())


# ── GET /analyze/{ticker} ──────────────────────────────────────────

@router.get("/analyze/{ticker}")
async def analyze_ticker(ticker: str) -> dict[str, Any]:
    """Run full composite valuation analysis for a ticker.

    Returns intrinsic values, milestone scorecard, derivatives support,
    and the overall assessment.
    """
    try:
        engine = _get_composite_engine()
        composite = engine.analyze(ticker)
        return {"status": "ok", **composite.to_dict()}
    except Exception as exc:
        log.warning("Valuation analysis failed for {t}: {e}", t=ticker, e=str(exc))
        return {"status": "error", "error": str(exc), "ticker": ticker.upper()}


# ── GET /prompt/{ticker} ───────────────────────────────────────────

@router.get("/prompt/{ticker}")
async def generate_prompt(ticker: str) -> dict[str, Any]:
    """Generate a Claude Max prompt with all quantified valuation data.

    Returns the prompt text, raw data, and an analysis_id for tracking.
    Paste the prompt into Claude Max, then submit the response back
    via POST /valuation/response.
    """
    try:
        engine = _get_composite_engine()
        result = engine.generate_prompt(ticker)
        return {"status": "ok", **result}
    except Exception as exc:
        log.warning("Prompt generation failed for {t}: {e}", t=ticker, e=str(exc))
        return {"status": "error", "error": str(exc), "ticker": ticker.upper()}


# ── POST /response ─────────────────────────────────────────────────

@router.post("/response")
async def log_analysis_response(body: AnalysisResponse) -> dict[str, Any]:
    """Log a Claude Max analysis response with date-stamped predictions.

    Send the analysis_id from the prompt, the full response text, and
    any extracted predictions. All predictions are date-stamped and
    tracked for future accuracy scoring.
    """
    try:
        engine = _get_composite_engine()
        log_id = engine.log_response(
            analysis_id=body.analysis_id,
            ticker=body.ticker,
            response_text=body.response_text,
            predictions=body.predictions,
        )
        return {
            "status": "ok",
            "log_id": log_id,
            "analysis_id": body.analysis_id,
            "predictions_logged": len(body.predictions),
        }
    except Exception as exc:
        log.warning("Response logging failed: {e}", e=str(exc))
        return {"status": "error", "error": str(exc)}


# ── GET /history/{ticker} ──────────────────────────────────────────

@router.get("/history/{ticker}")
async def valuation_history(
    ticker: str,
    days: int = Query(default=365, ge=7, le=3650),
) -> dict[str, Any]:
    """Get valuation history timeline for a ticker."""
    try:
        from valuation.intrinsic import IntrinsicValueEngine
        engine = IntrinsicValueEngine(get_db_engine())
        history = engine.get_history(ticker, days)
        return {"status": "ok", "ticker": ticker.upper(), "history": history}
    except Exception as exc:
        log.warning("Valuation history failed for {t}: {e}", t=ticker, e=str(exc))
        return {"status": "error", "error": str(exc)}


# ── GET /predictions/{ticker} ──────────────────────────────────────

@router.get("/predictions/{ticker}")
async def prediction_history(
    ticker: str,
    limit: int = Query(default=50, ge=1, le=200),
) -> dict[str, Any]:
    """Get all past analysis predictions for accuracy tracking."""
    try:
        engine = _get_composite_engine()
        predictions = engine.get_prediction_history(ticker, limit)
        return {"status": "ok", "ticker": ticker.upper(), "predictions": predictions}
    except Exception as exc:
        log.warning("Prediction history failed for {t}: {e}", t=ticker, e=str(exc))
        return {"status": "error", "error": str(exc)}


# ── GET /milestones/{ticker} ───────────────────────────────────────

@router.get("/milestones/{ticker}")
async def get_milestones(
    ticker: str,
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    """Get milestones and execution scorecard for a ticker."""
    try:
        tracker = _get_milestone_tracker()
        status_filter = [status] if status else None
        milestones = tracker.get_for_ticker(ticker, status_filter=status_filter)
        scorecard = tracker.get_scorecard(ticker)
        return {
            "status": "ok",
            "ticker": ticker.upper(),
            "milestones": milestones,
            "scorecard": scorecard,
        }
    except Exception as exc:
        log.warning("Milestones fetch failed for {t}: {e}", t=ticker, e=str(exc))
        return {"status": "error", "error": str(exc)}


# ── POST /milestones ───────────────────────────────────────────────

@router.post("/milestones")
async def add_milestone(body: MilestoneCreate) -> dict[str, Any]:
    """Add a new company milestone / goal / guidance item."""
    try:
        from valuation.milestones import Milestone
        tracker = _get_milestone_tracker()

        milestone = Milestone(
            ticker=body.ticker.upper(),
            milestone_type=body.milestone_type,
            announced_date=date.fromisoformat(body.announced_date),
            description=body.description,
            target_date=date.fromisoformat(body.target_date) if body.target_date else None,
            target_value=body.target_value,
            target_unit=body.target_unit,
            probability=body.probability,
            confidence_source=body.confidence_source,
            value_impact_ps=body.value_impact_ps,
            value_impact_pct=body.value_impact_pct,
            status=body.status,
            source_url=body.source_url,
            notes=body.notes,
        )
        new_id = tracker.add(milestone)
        return {"status": "ok", "milestone_id": new_id}
    except ValueError as exc:
        return {"status": "error", "error": str(exc)}
    except Exception as exc:
        log.warning("Milestone add failed: {e}", e=str(exc))
        return {"status": "error", "error": str(exc)}


# ── PUT /milestones/{id}/status ────────────────────────────────────

@router.put("/milestones/{milestone_id}/status")
async def update_milestone_status(
    milestone_id: int, body: MilestoneStatusUpdate,
) -> dict[str, Any]:
    """Update a milestone's status and optionally record actual values."""
    try:
        tracker = _get_milestone_tracker()
        tracker.update_status(
            milestone_id=milestone_id,
            status=body.status,
            actual_value=body.actual_value,
            actual_date=date.fromisoformat(body.actual_date) if body.actual_date else None,
            notes=body.notes,
        )
        return {"status": "ok", "milestone_id": milestone_id, "new_status": body.status}
    except ValueError as exc:
        return {"status": "error", "error": str(exc)}
    except Exception as exc:
        log.warning("Milestone status update failed: {e}", e=str(exc))
        return {"status": "error", "error": str(exc)}


# ── GET /derivatives/{ticker} ──────────────────────────────────────

@router.get("/derivatives/{ticker}")
async def derivatives_support(ticker: str) -> dict[str, Any]:
    """Get derivatives support analysis for a ticker.

    Shows how short interest, dealer gamma, and options flow
    are supporting or pressuring the current price.
    """
    try:
        engine = _get_derivatives_engine()
        result = engine.analyze(ticker)
        return {"status": "ok", **result.to_dict()}
    except Exception as exc:
        log.warning("Derivatives support failed for {t}: {e}", t=ticker, e=str(exc))
        return {"status": "error", "error": str(exc), "ticker": ticker.upper()}
