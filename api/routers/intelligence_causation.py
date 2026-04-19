"""Intelligence sub-router: Causal links for the Timeline forensic visualization.

Provides GET /intelligence/causal-links?ticker=<ticker>&days=90
which returns causal arrows data for the Timeline D3 overlay.

Joins causal_links with signal_data to fill in descriptions and dates
that are not stored directly in causal_links.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["intelligence"])


@router.get("/causal-links")
async def get_causal_links(
    ticker: str = Query(..., min_length=1, max_length=10, description="Ticker symbol"),
    days: int = Query(90, ge=1, le=730, description="Lookback window in days"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return causal links for Timeline arrow overlay.

    Joins causal_links with signal_data to enrich cause/effect descriptions.
    Each link represents a probable causal connection between an event
    (congressional trade, insider filing, dark pool spike, etc.) and a
    subsequent price move or market effect.
    """
    ticker_upper = ticker.strip().upper()
    cutoff = date.today() - timedelta(days=days)

    try:
        engine = get_db_engine()
    except Exception as exc:
        log.error("Failed to get DB engine for causal-links: {e}", e=str(exc))
        return {"links": [], "ticker": ticker_upper, "days": days, "error": "Database unavailable"}

    # Query causal_links joined with signal_data for richer descriptions.
    # causal_links columns: id, signal_id, actor, ticker, action_date, cause_type,
    #                       probable_cause, evidence, probability, created_at
    # We join signal_data on signal_id to get the effect description and dates.
    sql = text("""
        SELECT
            cl.id,
            cl.signal_id AS cause_signal_id,
            cl.cause_type,
            cl.action_date AS cause_date,
            cl.probable_cause AS cause_description,
            cl.ticker AS effect_ticker,
            cl.actor AS lever_actor,
            cl.probability,
            cl.evidence,
            -- Compute effect date: action_date + lead_time from evidence, or fallback
            CASE
                WHEN cl.evidence IS NOT NULL AND cl.evidence->>'lead_time_days' IS NOT NULL
                THEN cl.action_date + (cl.evidence->>'lead_time_days')::int
                ELSE cl.action_date + 2
            END AS effect_date,
            -- Pull effect description from signal_data if joined
            COALESCE(sd.description, 'Price reaction following ' || cl.cause_type || ' signal') AS effect_description,
            -- Lead time: from evidence JSON or default
            COALESCE(
                (cl.evidence->>'lead_time_days')::numeric,
                2
            ) AS lead_time_days
        FROM causal_links cl
        LEFT JOIN signal_data sd ON sd.id = cl.signal_id
        WHERE cl.ticker = :ticker
          AND cl.action_date >= :cutoff
        ORDER BY cl.action_date DESC
        LIMIT 200
    """).bindparams(ticker=ticker_upper, cutoff=cutoff)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
    except Exception as exc:
        log.warning(
            "Causal links query failed for {t}: {e}",
            t=ticker_upper,
            e=str(exc),
        )
        return {"links": [], "ticker": ticker_upper, "days": days, "error": str(exc)}

    links = []
    for row in rows:
        link = {
            "id": str(row[0]),
            "cause_signal_id": str(row[1]) if row[1] is not None else None,
            "cause_type": row[2] or "unknown",
            "cause_date": str(row[3]) if row[3] is not None else None,
            "cause_description": row[4] or "",
            "effect_ticker": row[5] or ticker_upper,
            "lever_actor": row[6] or "Unknown",
            "probability": float(row[7]) if row[7] is not None else 0.5,
            "evidence": _parse_evidence(row[8]),
            "effect_date": str(row[9]) if row[9] is not None else None,
            "effect_description": row[10] or "",
            "lead_time_days": float(row[11]) if row[11] is not None else 2.0,
        }
        links.append(link)

    log.debug(
        "Causal links for {t}: {n} links in {d}d window",
        t=ticker_upper,
        n=len(links),
        d=days,
    )

    return {
        "links": links,
        "ticker": ticker_upper,
        "days": days,
    }


def _parse_evidence(raw: Any) -> dict[str, Any]:
    """Parse evidence JSONB field into a dict."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        import json
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}
