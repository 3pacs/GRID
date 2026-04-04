"""
GRID API — Market Briefing & Sentiment Endpoints.

Serves pre-computed sentiment scores, daily briefings, and historical
sentiment data. This is a revenue-generating product endpoint.

Endpoints:
    GET /api/v1/briefing/sentiment       — Current computed sentiment score + breakdown
    GET /api/v1/briefing/latest           — Latest briefing content + sentiment
    GET /api/v1/briefing/history          — Historical briefings with pagination
    GET /api/v1/briefing/sentiment/history — Sentiment score time series
    POST /api/v1/briefing/generate        — Trigger briefing generation (admin only)
"""

from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth, require_role
from db import get_engine

router = APIRouter(prefix="/api/v1/briefing", tags=["briefing"])


@router.get("/sentiment")
async def get_current_sentiment(_token: str = Depends(require_auth)) -> dict:
    """Compute and return current market sentiment score with full breakdown."""
    try:
        from intelligence.sentiment_scorer import compute_sentiment
        engine = get_engine()
        result = compute_sentiment(engine)
        return result.to_dict()
    except Exception as e:
        log.warning("Sentiment computation failed: {e}", e=e)
        return {"error": str(e)}


@router.get("/latest")
async def get_latest_briefing(
    briefing_type: str = Query("daily", regex="^(hourly|daily|weekly)$"),
    _token: str = Depends(require_auth),
) -> dict:
    """Get the most recent briefing of the specified type."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT id, briefing_type, briefing_date, content, "
                "sentiment_score, sentiment_label, sentiment_data, created_at "
                "FROM market_briefings "
                "WHERE briefing_type = :btype "
                "ORDER BY created_at DESC LIMIT 1"
            ), {"btype": briefing_type}).fetchone()

            if not row:
                return {"error": "No briefing found", "briefing_type": briefing_type}

            return {
                "id": row[0],
                "briefing_type": row[1],
                "briefing_date": str(row[2]),
                "content": row[3],
                "sentiment_score": row[4],
                "sentiment_label": row[5],
                "sentiment": row[6],
                "created_at": str(row[7]),
            }
    except Exception as e:
        log.warning("Failed to fetch latest briefing: {e}", e=e)
        return {"error": str(e)}


@router.get("/history")
async def get_briefing_history(
    briefing_type: str = Query("daily", regex="^(hourly|daily|weekly)$"),
    days: int = Query(30, ge=1, le=365),
    _token: str = Depends(require_auth),
) -> dict:
    """Get historical briefings with pagination."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT id, briefing_date, sentiment_score, sentiment_label, "
                "LEFT(content, 300) as preview, created_at "
                "FROM market_briefings "
                "WHERE briefing_type = :btype "
                "AND briefing_date >= CURRENT_DATE - :days "
                "ORDER BY briefing_date DESC"
            ), {"btype": briefing_type, "days": days}).fetchall()

            return {
                "briefing_type": briefing_type,
                "count": len(rows),
                "briefings": [
                    {
                        "id": r[0],
                        "date": str(r[1]),
                        "sentiment_score": r[2],
                        "sentiment_label": r[3],
                        "preview": r[4],
                        "created_at": str(r[5]),
                    }
                    for r in rows
                ],
            }
    except Exception as e:
        log.warning("Failed to fetch briefing history: {e}", e=e)
        return {"error": str(e)}


@router.get("/sentiment/history")
async def get_sentiment_history(
    days: int = Query(30, ge=1, le=365),
    _token: str = Depends(require_auth),
) -> dict:
    """Sentiment score time series for charting."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT prediction_date, score, label, components, "
                "realized_return, outcome "
                "FROM sentiment_predictions "
                "WHERE prediction_date >= CURRENT_DATE - :days "
                "ORDER BY prediction_date"
            ), {"days": days}).fetchall()

            return {
                "count": len(rows),
                "series": [
                    {
                        "date": str(r[0]),
                        "score": r[1],
                        "label": r[2],
                        "components": r[3],
                        "realized_return": r[4],
                        "outcome": r[5],
                    }
                    for r in rows
                ],
            }
    except Exception as e:
        log.warning("Failed to fetch sentiment history: {e}", e=e)
        return {"error": str(e)}


@router.get("/sentiment/accuracy")
async def get_sentiment_accuracy(_token: str = Depends(require_auth)) -> dict:
    """Sentiment model accuracy and weight evolution."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            # Overall accuracy
            scored = conn.execute(text(
                "SELECT outcome, COUNT(*) FROM sentiment_predictions "
                "WHERE outcome != 'PENDING' GROUP BY outcome"
            )).fetchall()

            # Weight history
            weights = conn.execute(text(
                "SELECT version, weights, accuracy, updated_at "
                "FROM sentiment_weights ORDER BY version DESC LIMIT 10"
            )).fetchall()

            total = sum(r[1] for r in scored)
            correct = sum(r[1] for r in scored if r[0] in ("CORRECT", "NEUTRAL_CORRECT"))

            return {
                "total_predictions": total,
                "correct": correct,
                "accuracy": round(correct / total * 100, 1) if total > 0 else None,
                "outcomes": {r[0]: r[1] for r in scored},
                "weight_versions": [
                    {
                        "version": r[0],
                        "weights": r[1],
                        "accuracy": r[2],
                        "updated_at": str(r[3]),
                    }
                    for r in weights
                ],
            }
    except Exception as e:
        log.warning("Failed to fetch sentiment accuracy: {e}", e=e)
        return {"error": str(e)}


@router.post("/generate")
async def trigger_briefing(
    briefing_type: str = Query("daily", regex="^(hourly|daily|weekly)$"),
    _token: str = Depends(require_role("admin")),
) -> dict:
    """Manually trigger a briefing generation (admin only)."""
    try:
        from ollama.market_briefing import MarketBriefingEngine
        engine = get_engine()
        mbe = MarketBriefingEngine(db_engine=engine)
        result = mbe.generate_briefing(briefing_type=briefing_type, save=True)
        return {
            "status": "ok",
            "briefing_type": briefing_type,
            "sentiment": result.get("sentiment"),
            "content_length": len(result.get("content", "")),
        }
    except Exception as e:
        log.warning("Briefing generation failed: {e}", e=e)
        return {"error": str(e)}
