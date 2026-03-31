"""MCP export endpoints — lightweight JSON wrappers for GRID intelligence.

These endpoints are designed for consumption by the GRID MCP server
(mcp_server.py) and return structured JSON without HTML rendering.
All endpoints require authentication.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/mcp", tags=["mcp-export"])


# ── Helpers ───────────────────────────────────────────────────────────────


def _safe_float(v: Any) -> float | None:
    """Safely convert a value to float, returning None for NaN/None."""
    if v is None:
        return None
    try:
        import math
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else round(f, 6)
    except (ValueError, TypeError):
        return None


def _safe_iso(v: Any) -> str | None:
    """Safely format a datetime to ISO string."""
    if v is None:
        return None
    try:
        return v.isoformat()
    except AttributeError:
        return str(v)


# ── 1. Trust Score ────────────────────────────────────────────────────────


@router.get("/trust-score")
async def mcp_trust_score(
    actor: str = Query(..., description="Actor name or ID"),
    window_days: int = Query(default=90, ge=1, le=365),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return trust score + provenance for a named actor."""
    engine = get_db_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, name, tier, trust_score, credibility, "
                "influence_score, updated_at "
                "FROM actors "
                "WHERE UPPER(name) = UPPER(:n) OR UPPER(id) = UPPER(:n) "
                "LIMIT 1"
            ),
            {"n": actor},
        ).fetchone()

    if not row:
        return {"ok": False, "error": f"Actor '{actor}' not found"}

    return {
        "ok": True,
        "actor_id": row[0],
        "name": row[1],
        "tier": row[2],
        "trust_score": _safe_float(row[3]),
        "credibility": row[4],
        "influence_score": _safe_float(row[5]),
        "updated_at": _safe_iso(row[6]),
        "window_days": window_days,
    }


# ── 2. Actor Profile ─────────────────────────────────────────────────────


@router.get("/actor-profile")
async def mcp_actor_profile(
    name: str = Query(..., description="Actor name or ID"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Full actor dossier: identity, connections, positions, trust."""
    engine = get_db_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id, name, tier, category, aum, trust_score, "
                "motivation_model, connections, known_positions, "
                "board_seats, political_affiliations, credibility, "
                "influence_score, metadata, updated_at "
                "FROM actors "
                "WHERE UPPER(name) = UPPER(:n) OR UPPER(id) = UPPER(:n) "
                "LIMIT 1"
            ),
            {"n": name},
        ).fetchone()

    if not row:
        return {"ok": False, "error": f"Actor '{name}' not found"}

    return {
        "ok": True,
        "profile": {
            "id": row[0],
            "name": row[1],
            "tier": row[2],
            "category": row[3],
            "aum": _safe_float(row[4]),
            "trust_score": _safe_float(row[5]),
            "motivation_model": row[6],
            "connections": row[7] if row[7] else [],
            "known_positions": row[8] if row[8] else [],
            "board_seats": row[9] if row[9] else [],
            "political_affiliations": row[10] if row[10] else [],
            "credibility": row[11],
            "influence_score": _safe_float(row[12]),
            "metadata": row[13] if row[13] else {},
            "updated_at": _safe_iso(row[14]),
        },
    }


# ── 3. Predictions ───────────────────────────────────────────────────────


@router.get("/predictions")
async def mcp_predictions(
    symbol: str = Query(default="", description="Ticker symbol (empty = all)"),
    lookback_days: int = Query(default=14, ge=1, le=90),
    limit: int = Query(default=20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Recent predictions with verdict and PnL."""
    engine = get_db_engine()
    params: dict[str, Any] = {
        "cutoff": date.today() - timedelta(days=lookback_days),
        "lim": limit,
    }

    if symbol:
        params["ticker"] = symbol.upper()
        query = text(
            "SELECT id, ticker, model_name, direction, confidence, "
            "entry_price, target_price, stop_price, verdict, "
            "actual_price, pnl, created_at, scored_at "
            "FROM oracle_predictions "
            "WHERE ticker = :ticker AND created_at >= :cutoff "
            "ORDER BY created_at DESC LIMIT :lim"
        )
    else:
        query = text(
            "SELECT id, ticker, model_name, direction, confidence, "
            "entry_price, target_price, stop_price, verdict, "
            "actual_price, pnl, created_at, scored_at "
            "FROM oracle_predictions "
            "WHERE created_at >= :cutoff "
            "ORDER BY created_at DESC LIMIT :lim"
        )

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    predictions = []
    for r in rows:
        predictions.append({
            "id": r[0],
            "ticker": r[1],
            "model": r[2],
            "direction": r[3],
            "confidence": _safe_float(r[4]),
            "entry_price": _safe_float(r[5]),
            "target_price": _safe_float(r[6]),
            "stop_price": _safe_float(r[7]),
            "verdict": r[8],
            "actual_price": _safe_float(r[9]),
            "pnl": _safe_float(r[10]),
            "created_at": _safe_iso(r[11]),
            "scored_at": _safe_iso(r[12]),
        })

    return {"ok": True, "predictions": predictions, "count": len(predictions)}


# ── 4. Prediction Accuracy ───────────────────────────────────────────────


@router.get("/prediction-accuracy")
async def mcp_prediction_accuracy(
    group_by: str = Query(default="model", description="model | ticker | direction"),
    lookback_days: int = Query(default=30, ge=1, le=365),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Prediction accuracy breakdown by model, ticker, or direction."""
    allowed_groups = {"model": "model_name", "ticker": "ticker", "direction": "direction"}
    col = allowed_groups.get(group_by)
    if not col:
        return {"ok": False, "error": f"group_by must be one of: {list(allowed_groups.keys())}"}

    engine = get_db_engine()
    # col is from hardcoded whitelist — safe for identifier use
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT {col} AS grp, "
                "COUNT(*) AS total, "
                "SUM(CASE WHEN verdict = 'hit' THEN 1 ELSE 0 END) AS hits, "
                "SUM(CASE WHEN verdict = 'partial' THEN 1 ELSE 0 END) AS partials, "
                "SUM(CASE WHEN verdict = 'miss' THEN 1 ELSE 0 END) AS misses, "
                "AVG(pnl) AS avg_pnl "
                "FROM oracle_predictions "
                "WHERE verdict IS NOT NULL "
                "  AND scored_at >= :cutoff "
                f"GROUP BY {col} "
                f"ORDER BY COUNT(*) DESC"
            ),
            {"cutoff": date.today() - timedelta(days=lookback_days)},
        ).fetchall()

    groups = []
    for r in rows:
        total = int(r[1]) if r[1] else 0
        hits = int(r[2]) if r[2] else 0
        groups.append({
            "group": r[0],
            "total": total,
            "hits": hits,
            "partials": int(r[3]) if r[3] else 0,
            "misses": int(r[4]) if r[4] else 0,
            "hit_rate": round(hits / total, 4) if total > 0 else 0,
            "avg_pnl": _safe_float(r[5]),
        })

    return {"ok": True, "group_by": group_by, "accuracy": groups}


# ── 5. Data Freshness ────────────────────────────────────────────────────


@router.get("/data-freshness")
async def mcp_data_freshness(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Source staleness report — last pull time for each data source."""
    engine = get_db_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT sc.name, sc.source_type, "
                "MAX(rs.release_date) AS last_release, "
                "MAX(rs.obs_date) AS last_obs, "
                "COUNT(*) AS row_count "
                "FROM source_catalog sc "
                "LEFT JOIN raw_series rs ON rs.source_id = sc.id "
                "GROUP BY sc.id, sc.name, sc.source_type "
                "ORDER BY last_release DESC NULLS LAST"
            )
        ).fetchall()

    sources = []
    today = date.today()
    for r in rows:
        last_release = r[2]
        staleness_days = (today - last_release).days if last_release else None
        sources.append({
            "name": r[0],
            "source_type": r[1],
            "last_release": _safe_iso(last_release),
            "last_obs": _safe_iso(r[3]),
            "row_count": r[4],
            "staleness_days": staleness_days,
            "status": "fresh" if staleness_days and staleness_days <= 7
                      else "stale" if staleness_days and staleness_days <= 30
                      else "dead" if staleness_days
                      else "empty",
        })

    stale_count = sum(1 for s in sources if s["status"] in ("stale", "dead"))
    return {
        "ok": True,
        "sources": sources,
        "total": len(sources),
        "stale_count": stale_count,
    }


# ── 6. Signal Sources ────────────────────────────────────────────────────


@router.get("/signal-sources")
async def mcp_signal_sources(
    symbol: str = Query(default="", description="Ticker (empty = all recent)"),
    lookback_days: int = Query(default=14, ge=1, le=90),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Active signal sources for a symbol with trust scores."""
    engine = get_db_engine()
    params: dict[str, Any] = {
        "cutoff": date.today() - timedelta(days=lookback_days),
    }

    if symbol:
        params["ticker"] = symbol.upper()
        query = text(
            "SELECT ticker, source_type, source_id, signal_type, "
            "signal_date, trust_score, outcome "
            "FROM signal_sources "
            "WHERE ticker = :ticker AND signal_date >= :cutoff "
            "ORDER BY signal_date DESC LIMIT 50"
        )
    else:
        query = text(
            "SELECT ticker, source_type, source_id, signal_type, "
            "signal_date, trust_score, outcome "
            "FROM signal_sources "
            "WHERE signal_date >= :cutoff "
            "ORDER BY signal_date DESC LIMIT 50"
        )

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    signals = []
    for r in rows:
        signals.append({
            "ticker": r[0],
            "source_type": r[1],
            "source_id": r[2],
            "signal_type": r[3],
            "signal_date": _safe_iso(r[4]),
            "trust_score": _safe_float(r[5]),
            "outcome": r[6],
        })

    return {"ok": True, "signals": signals, "count": len(signals)}


# ── 7. Wealth Flows ──────────────────────────────────────────────────────


@router.get("/wealth-flows")
async def mcp_wealth_flows(
    actor: str = Query(default="", description="Actor name (empty = all recent)"),
    lookback_days: int = Query(default=30, ge=1, le=365),
    limit: int = Query(default=20, ge=1, le=100),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Recent wealth flows between actors/entities."""
    engine = get_db_engine()
    params: dict[str, Any] = {
        "cutoff": date.today() - timedelta(days=lookback_days),
        "lim": limit,
    }

    if actor:
        params["actor"] = actor
        query = text(
            "SELECT flow_date, from_actor, to_entity, amount_estimate, "
            "confidence, implication "
            "FROM wealth_flows "
            "WHERE (UPPER(from_actor) = UPPER(:actor) "
            "   OR UPPER(to_entity) = UPPER(:actor)) "
            "  AND flow_date >= :cutoff "
            "ORDER BY flow_date DESC LIMIT :lim"
        )
    else:
        query = text(
            "SELECT flow_date, from_actor, to_entity, amount_estimate, "
            "confidence, implication "
            "FROM wealth_flows "
            "WHERE flow_date >= :cutoff "
            "ORDER BY amount_estimate DESC NULLS LAST LIMIT :lim"
        )

    with engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()

    flows = []
    for r in rows:
        flows.append({
            "date": _safe_iso(r[0]),
            "from": r[1],
            "to": r[2],
            "amount_usd": _safe_float(r[3]),
            "confidence": r[4],
            "implication": r[5],
        })

    return {"ok": True, "flows": flows, "count": len(flows)}


# ── 8. Regime State ──────────────────────────────────────────────────────


@router.get("/regime")
async def mcp_regime(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Current regime classification and recent history."""
    engine = get_db_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT inferred_state, state_confidence, "
                "decision_timestamp "
                "FROM decision_journal "
                "ORDER BY decision_timestamp DESC LIMIT 1"
            )
        ).fetchone()

        history = conn.execute(
            text(
                "SELECT DATE(decision_timestamp) AS dt, "
                "inferred_state, state_confidence "
                "FROM decision_journal "
                "WHERE decision_timestamp >= NOW() - INTERVAL '30 days' "
                "ORDER BY decision_timestamp DESC LIMIT 30"
            )
        ).fetchall()

    current = None
    if row:
        current = {
            "state": row[0],
            "confidence": _safe_float(row[1]),
            "as_of": _safe_iso(row[2]),
        }

    regime_history = [
        {"date": _safe_iso(r[0]), "state": r[1], "confidence": _safe_float(r[2])}
        for r in history
    ]

    return {
        "ok": True,
        "current": current,
        "history": regime_history,
    }
