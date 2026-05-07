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


# ── GET /catalyst-timeline/{ticker} ──────────────────────────────────

@router.get("/catalyst-timeline/{ticker}")
async def catalyst_timeline(
    ticker: str,
    months_forward: int = Query(default=12, ge=1, le=36),
    months_back: int = Query(default=6, ge=0, le=24),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return a unified catalyst timeline for a ticker.

    Aggregates:
    - company_milestones (earnings guidance, product launches, etc.)
    - catalyst_calendar (trial readouts, FDA decisions)
    - oracle_predictions (model predictions with expiry)
    - discovered_hypotheses (active theses with invalidation conditions)
    - Intrinsic + extrinsic value snapshot

    Each event has an invalidation condition and value impact.
    """
    from datetime import timedelta

    engine = get_db_engine()
    ticker_upper = ticker.upper()
    today = date.today()
    start = today - timedelta(days=months_back * 30)
    end = today + timedelta(days=months_forward * 30)

    events: list[dict] = []

    with engine.connect() as conn:

        # ── 1. Company Milestones ──
        try:
            ms_rows = conn.execute(text("""
                SELECT id, milestone_type, description, target_date, actual_date,
                       target_value, target_unit, actual_value, achievement_pct,
                       probability, confidence_source, value_impact_ps, value_impact_pct,
                       status, notes, announced_date
                FROM company_milestones
                WHERE UPPER(ticker) = :ticker
                AND (target_date BETWEEN :start AND :end
                     OR actual_date BETWEEN :start AND :end
                     OR (target_date IS NULL AND announced_date BETWEEN :start AND :end))
                ORDER BY COALESCE(target_date, announced_date)
            """), {"ticker": ticker_upper, "start": start, "end": end}).fetchall()

            for r in ms_rows:
                event_date = str(r[3] or r[4] or r[15])
                status = r[13] or "PENDING"
                events.append({
                    "id": f"ms-{r[0]}",
                    "type": "milestone",
                    "subtype": r[1],
                    "date": event_date,
                    "label": r[2] or r[1],
                    "status": status,
                    "target_value": r[5],
                    "target_unit": r[6],
                    "actual_value": r[7],
                    "achievement_pct": r[8],
                    "probability": r[9],
                    "confidence_source": r[10],
                    "value_impact_ps": float(r[11]) if r[11] else None,
                    "value_impact_pct": float(r[12]) if r[12] else None,
                    "notes": r[14],
                    "invalidation": _milestone_invalidation(r[1], r[5], r[6], status),
                })
        except Exception as exc:
            log.debug("Catalyst timeline milestones: {e}", e=str(exc))

        # ── 2. Catalyst Calendar (trials) ──
        try:
            cat_rows = conn.execute(text("""
                SELECT ticker, nct_id, event_type, expected_date,
                       confidence_window_days, source, notes
                FROM catalyst_calendar
                WHERE UPPER(ticker) = :ticker AND is_active = TRUE
                AND expected_date BETWEEN :start AND :end
                ORDER BY expected_date
            """), {"ticker": ticker_upper, "start": start, "end": end}).fetchall()

            for r in cat_rows:
                window = r[4] or 30
                events.append({
                    "id": f"cat-{r[1]}-{r[2]}",
                    "type": "catalyst",
                    "subtype": r[2],
                    "date": str(r[3]),
                    "label": f"{r[2].replace('_', ' ').title()}: {r[1]}",
                    "confidence_window_days": window,
                    "source": r[5],
                    "notes": r[6],
                    "invalidation": f"Trial {r[1]} fails to report within {window}d of {r[3]}",
                })
        except Exception as exc:
            log.debug("Catalyst timeline catalysts: {e}", e=str(exc))

        # ── 3. Oracle Predictions ──
        try:
            pred_rows = conn.execute(text("""
                SELECT id, prediction_type, direction, target_price, entry_price,
                       expiry, confidence, expected_move_pct, model_name,
                       verdict, actual_price, actual_move_pct, score_notes,
                       created_at
                FROM oracle_predictions
                WHERE UPPER(ticker) = :ticker
                AND (expiry BETWEEN :start AND :end
                     OR created_at::date BETWEEN :start AND :end)
                ORDER BY expiry NULLS LAST
            """), {"ticker": ticker_upper, "start": start, "end": end}).fetchall()

            for r in pred_rows:
                direction = r[2] or "neutral"
                target = r[3]
                entry = r[4]
                events.append({
                    "id": f"pred-{r[0]}",
                    "type": "prediction",
                    "subtype": r[1],
                    "date": str(r[5] or r[13])[:10],
                    "label": f"{direction.upper()} \u2192 ${float(target):.2f}" if target else f"{direction.upper()} {r[1]}",
                    "direction": direction,
                    "target_price": float(target) if target else None,
                    "entry_price": float(entry) if entry else None,
                    "confidence": float(r[6]) if r[6] else None,
                    "expected_move_pct": float(r[7]) if r[7] else None,
                    "model": r[8],
                    "verdict": r[9],
                    "actual_price": float(r[10]) if r[10] else None,
                    "actual_move_pct": float(r[11]) if r[11] else None,
                    "invalidation": _prediction_invalidation(direction, target, entry),
                })
        except Exception as exc:
            log.debug("Catalyst timeline predictions: {e}", e=str(exc))

        # ── 4. Active Hypotheses ──
        try:
            hyp_rows = conn.execute(text("""
                SELECT id, thesis, role, confidence, invalidation, status,
                       kill_reason, pattern_type, times_tested, times_correct,
                       created_at
                FROM discovered_hypotheses
                WHERE (thesis ILIKE :pat OR thesis ILIKE :tpat)
                AND status NOT IN ('killed', 'expired')
                AND confidence > 0.2
                ORDER BY confidence DESC
                LIMIT 10
            """), {"pat": f"%{ticker_upper}%", "tpat": f"%{ticker.lower()}%"}).fetchall()

            for r in hyp_rows:
                role = r[2] or "hypothesis"
                events.append({
                    "id": f"hyp-{r[0]}",
                    "type": "hypothesis",
                    "subtype": role,
                    "date": str(r[10])[:10] if r[10] else str(today),
                    "label": (r[1] or "")[:120],
                    "role": role,
                    "confidence": float(r[3]) if r[3] else None,
                    "invalidation": r[4] or "No explicit invalidation defined",
                    "status": r[5],
                    "pattern_type": r[7],
                    "tested": r[8],
                    "correct": r[9],
                    "accuracy": round(r[9] / r[8], 2) if r[8] and r[8] > 0 else None,
                })
        except Exception as exc:
            log.debug("Catalyst timeline hypotheses: {e}", e=str(exc))

        # ── 5. Intrinsic + Extrinsic Value Snapshot ──
        valuation: dict[str, Any] = {}
        try:
            val_row = conn.execute(text("""
                SELECT ticker, analysis_date, fair_value_low, fair_value_mid,
                       fair_value_high, current_price, margin_of_safety,
                       conviction
                FROM company_valuations
                WHERE UPPER(ticker) = :ticker
                ORDER BY analysis_date DESC LIMIT 1
            """), {"ticker": ticker_upper}).fetchone()

            if val_row:
                valuation = {
                    "analysis_date": str(val_row[1]),
                    "fair_value_low": float(val_row[2]) if val_row[2] else None,
                    "fair_value_mid": float(val_row[3]) if val_row[3] else None,
                    "fair_value_high": float(val_row[4]) if val_row[4] else None,
                    "current_price": float(val_row[5]) if val_row[5] else None,
                    "margin_of_safety": float(val_row[6]) if val_row[6] else None,
                    "conviction": val_row[7],
                }
        except Exception as exc:
            log.debug("Catalyst timeline valuation: {e}", e=str(exc))

        # ── 6. Current price for context ──
        try:
            price_row = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            ), {"sid": f"YF:{ticker_upper}:close"}).fetchone()
            if price_row:
                valuation["live_price"] = float(price_row[0])
        except Exception:
            pass

    events.sort(key=lambda e: e.get("date", "9999"))

    return {
        "status": "ok",
        "ticker": ticker_upper,
        "today": str(today),
        "range": {"start": str(start), "end": str(end)},
        "events": events,
        "event_count": len(events),
        "valuation": valuation,
    }


def _milestone_invalidation(ms_type: str, target_val, target_unit, status: str) -> str:
    """Generate a human-readable invalidation condition for a milestone."""
    if status in ("ACHIEVED", "MISSED", "CANCELLED"):
        return f"Already resolved: {status}"

    type_map = {
        "EARNINGS_GUIDANCE": "Misses guidance by >10% or withdraws forecast",
        "REVENUE_GUIDANCE": "Revenue comes in >15% below target",
        "PRODUCT_LAUNCH": "Launch delayed >2 quarters or cancelled",
        "EXPANSION": "Market entry postponed or regulatory block",
        "M_AND_A": "Deal terminated, regulatory block, or renegotiated >20% lower",
        "REGULATORY": "Regulatory rejection, CRL, or >6 month delay",
        "COST_TARGET": "Costs exceed target by >20% or restructuring charge",
        "BUYBACK": "Buyback suspended or reduced >50%",
        "DIVIDEND": "Dividend cut or suspended",
        "DEBT_TARGET": "Debt reduction misses by >25% or new debt issued",
        "STRATEGIC": "Strategic pivot reversed or key executive departure",
        "RUMOR": "Rumor denied or contradicted by filing",
    }

    base = type_map.get(ms_type, "Milestone not achieved by target date")
    if target_val and target_unit:
        return f"{base}. Target: {target_val} {target_unit}"
    return base


def _prediction_invalidation(direction: str, target, entry) -> str:
    """Generate invalidation for an oracle prediction."""
    if not target or not entry:
        return f"Price moves opposite to {direction} thesis"

    float(target)
    entry_f = float(entry)

    if direction in ("bullish", "long", "buy"):
        stop = entry_f * 0.95
        return f"Invalidated below ${stop:.2f} (5% below entry ${entry_f:.2f})"
    elif direction in ("bearish", "short", "sell"):
        stop = entry_f * 1.05
        return f"Invalidated above ${stop:.2f} (5% above entry ${entry_f:.2f})"
    return f"Price moves >10% opposite to {direction} thesis"
