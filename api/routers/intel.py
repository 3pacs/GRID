"""
GRID Intelligence API Product — the core paid API.

Clean, unified endpoints that expose GRID's intelligence as a product.
Every response includes: data, meta (count, query_time), tier_required,
and confidence labels on all data points.

Prefix: /api/v1/intel
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/intel", tags=["intel-product"])


# ── Tier Definitions ──────────────────────────────────────────────────────

class Tier(str, Enum):
    FREE = "free"
    BASIC = "basic"
    PRO = "pro"
    ENTERPRISE = "enterprise"


# ── Response Helpers ──────────────────────────────────────────────────────

def _ok(
    data: Any,
    *,
    meta: dict[str, Any] | None = None,
    tier_required: str = Tier.BASIC,
    query_start: float | None = None,
) -> dict[str, Any]:
    """Standardized success envelope."""
    m: dict[str, Any] = meta or {}
    if query_start is not None:
        m["query_time_ms"] = round((time.time() - query_start) * 1000, 1)
    if isinstance(data, list):
        m.setdefault("count", len(data))
    return {"data": data, "meta": m, "tier_required": tier_required}


def _empty(
    message: str = "No results",
    *,
    tier_required: str = Tier.BASIC,
    query_start: float | None = None,
) -> dict[str, Any]:
    """Standardized empty response."""
    m: dict[str, Any] = {"count": 0, "message": message}
    if query_start is not None:
        m["query_time_ms"] = round((time.time() - query_start) * 1000, 1)
    return {"data": [], "meta": m, "tier_required": tier_required}


def _safe_json(val: Any) -> Any:
    """Parse JSON string or return as-is."""
    if isinstance(val, str):
        import json
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            return val
    return val


def _row_to_dict(row: Any, columns: list[str]) -> dict[str, Any]:
    """Convert a DB row tuple to a dict using column names."""
    return {col: row[i] for i, col in enumerate(columns)}


def _safe_isoformat(val: Any) -> str | None:
    """Convert date/datetime to ISO string or None."""
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return str(val)


# ── Valve Identification (Prediction Causation Standard) ─────────────────

# Maps actor category + action to the liquidity valve being opened/closed.
_VALVE_MAP: dict[str, dict[str, str]] = {
    "congress": {
        "BUY": "legislative-insider flow valve opens — capital entering ahead of policy action",
        "SELL": "legislative-insider flow valve closes — capital exiting ahead of policy action",
    },
    "insider": {
        "BUY": "corporate-insider confidence valve opens — management accumulating",
        "SELL": "corporate-insider confidence valve closes — management distributing",
    },
    "fed": {
        "SPEECH_HAWKISH": "credit valve tightening — risk assets reprice downward",
        "SPEECH_DOVISH": "credit valve loosening — risk assets bid",
        "BUY": "central bank liquidity valve opens — QE/reserve injection",
        "SELL": "central bank liquidity valve closes — QT/reserve drain",
    },
    "institutional": {
        "BUY": "institutional allocation valve opens — smart money entering",
        "SELL": "institutional allocation valve closes — smart money exiting",
        "POSITION_INCREASE": "institutional allocation valve opens — position scaling",
    },
    "darkpool": {
        "BUY": "dark pool accumulation valve — hidden institutional demand",
        "SELL": "dark pool distribution valve — hidden institutional supply",
    },
    "dealer": {
        "BUY": "dealer hedging flow valve — market maker positioning long",
        "SELL": "dealer hedging flow valve — market maker positioning short",
    },
}


def _identify_valve(category: str, action: str, tickers: list[str]) -> str:
    """Identify the liquidity valve being pulled per the Prediction Causation Standard.

    Every lever event must name: the valve, the flow direction, and why it matters.
    """
    cat_map = _VALVE_MAP.get(category, {})
    if action in cat_map:
        return cat_map[action]

    # Generic fallback for unknown category/action combos
    direction = "opens" if action in ("BUY", "POSITION_INCREASE", "SPEECH_DOVISH") else "closes"
    ticker_str = ", ".join(tickers[:3]) if tickers else "broad market"
    return f"{category} flow valve {direction} — {action.lower()} signal on {ticker_str}"


# ── 1. Search ─────────────────────────────────────────────────────────────

@router.get("/search")
async def intel_search(
    q: str = Query(..., min_length=1, max_length=200, description="Search query"),
    type: str = Query("all", description="entity | actor | ticker | all"),
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Search across actors, entities, tickers. Returns matches with confidence labels.

    Searches actor_network (actors), icij_relationships (entities),
    oracle_predictions + options data (tickers).
    """
    t0 = time.time()
    engine = get_db_engine()
    results: list[dict[str, Any]] = []
    query_upper = q.upper()
    query_like = f"%{q}%"

    with engine.connect() as conn:
        # -- Actors --
        if type in ("all", "actor"):
            try:
                rows = conn.execute(
                    text(
                        "SELECT id, name, tier, category, aum, trust_score, "
                        "credibility "
                        "FROM actors "
                        "WHERE UPPER(name) LIKE UPPER(:q) "
                        "   OR UPPER(id) LIKE UPPER(:q) "
                        "ORDER BY aum DESC NULLS LAST "
                        "LIMIT :lim OFFSET :off"
                    ),
                    {"q": query_like, "lim": limit, "off": offset},
                ).fetchall()
                for r in rows:
                    results.append({
                        "match_type": "actor",
                        "id": r[0],
                        "name": r[1],
                        "tier": r[2],
                        "sector": r[3],
                        "aum_usd": r[4],
                        "trust_score": r[5],
                        "confidence": r[6] or "derived",
                    })
            except Exception as exc:
                log.debug("Actor search skipped: {e}", e=str(exc))

        # -- Entities (ICIJ offshore) --
        if type in ("all", "entity"):
            try:
                rows = conn.execute(
                    text(
                        "SELECT DISTINCT entity_name, jurisdiction, source_dataset, "
                        "linked_to, relationship_type "
                        "FROM icij_relationships "
                        "WHERE UPPER(entity_name) LIKE UPPER(:q) "
                        "   OR UPPER(linked_to) LIKE UPPER(:q) "
                        "ORDER BY entity_name "
                        "LIMIT :lim OFFSET :off"
                    ),
                    {"q": query_like, "lim": limit, "off": offset},
                ).fetchall()
                for r in rows:
                    results.append({
                        "match_type": "entity",
                        "name": r[0],
                        "jurisdiction": r[1],
                        "source_dataset": r[2],
                        "linked_to": r[3],
                        "relationship_type": r[4],
                        "confidence": "confirmed",  # ICIJ is primary source
                    })
            except Exception as exc:
                log.debug("Entity search skipped: {e}", e=str(exc))

        # -- Tickers --
        if type in ("all", "ticker"):
            try:
                rows = conn.execute(
                    text(
                        "SELECT DISTINCT ticker, model_name, direction, confidence, "
                        "verdict, created_at "
                        "FROM oracle_predictions "
                        "WHERE UPPER(ticker) = :q "
                        "ORDER BY created_at DESC "
                        "LIMIT :lim OFFSET :off"
                    ),
                    {"q": query_upper, "lim": limit, "off": offset},
                ).fetchall()
                for r in rows:
                    results.append({
                        "match_type": "ticker",
                        "ticker": r[0],
                        "model": r[1],
                        "direction": r[2],
                        "confidence": float(r[3]) if r[3] else None,
                        "verdict": r[4],
                        "created_at": _safe_isoformat(r[5]),
                        "confidence_label": "derived",
                    })
            except Exception as exc:
                log.debug("Ticker search skipped: {e}", e=str(exc))

    if not results:
        return _empty("No matches found", query_start=t0)

    return _ok(
        results,
        meta={"query": q, "type": type},
        tier_required=Tier.BASIC,
        query_start=t0,
    )


# ── 2. Entity Profile ────────────────────────────────────────────────────

@router.get("/entity/{name:path}")
async def intel_entity_profile(
    name: str = Path(..., description="Entity name"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Full entity profile: offshore connections, board interlocks, connected
    actors, trust score, wealth estimates, red flags.
    """
    t0 = time.time()
    engine = get_db_engine()
    entity: dict[str, Any] = {
        "name": name,
        "offshore_connections": [],
        "connected_actors": [],
        "board_interlocks": [],
        "trust_score": None,
        "wealth_estimate": None,
        "red_flags": [],
        "confidence": "derived",
    }

    with engine.connect() as conn:
        # ICIJ offshore connections
        try:
            rows = conn.execute(
                text(
                    "SELECT entity_name, linked_to, relationship_type, jurisdiction, "
                    "source_dataset, intermediary, status "
                    "FROM icij_relationships "
                    "WHERE UPPER(entity_name) = UPPER(:n) "
                    "   OR UPPER(linked_to) = UPPER(:n) "
                    "ORDER BY entity_name"
                ),
                {"n": name},
            ).fetchall()
            for r in rows:
                entity["offshore_connections"].append({
                    "entity_name": r[0],
                    "linked_to": r[1],
                    "relationship_type": r[2],
                    "jurisdiction": r[3],
                    "source_dataset": r[4],
                    "intermediary": r[5],
                    "status": r[6],
                    "confidence": "confirmed",
                })
        except Exception as exc:
            log.debug("ICIJ lookup skipped for {n}: {e}", n=name, e=str(exc))

        # Connected actors
        try:
            rows = conn.execute(
                text(
                    "SELECT id, name, tier, category, aum, trust_score "
                    "FROM actors "
                    "WHERE UPPER(name) LIKE UPPER(:q) "
                    "   OR connections::text ILIKE :q2"
                ),
                {"q": f"%{name}%", "q2": f"%{name}%"},
            ).fetchall()
            for r in rows:
                entity["connected_actors"].append({
                    "actor_id": r[0],
                    "name": r[1],
                    "tier": r[2],
                    "sector": r[3],
                    "aum_usd": r[4],
                    "trust_score": r[5],
                    "confidence": "derived",
                })
        except Exception as exc:
            log.debug("Actor lookup skipped for {n}: {e}", n=name, e=str(exc))

        # Red flags: check for multiple jurisdictions, intermediary chains
        offshore = entity["offshore_connections"]
        jurisdictions = {c["jurisdiction"] for c in offshore if c.get("jurisdiction")}
        if len(jurisdictions) >= 3:
            entity["red_flags"].append({
                "flag": "multi_jurisdiction",
                "detail": f"Entity appears in {len(jurisdictions)} jurisdictions: "
                          f"{', '.join(sorted(jurisdictions))}",
                "severity": "high",
                "confidence": "confirmed",
            })
        intermediaries = [c for c in offshore if c.get("intermediary")]
        if len(intermediaries) >= 2:
            entity["red_flags"].append({
                "flag": "intermediary_chain",
                "detail": f"{len(intermediaries)} intermediary relationships detected",
                "severity": "medium",
                "confidence": "confirmed",
            })

    if not entity["offshore_connections"] and not entity["connected_actors"]:
        return _empty(f"No data found for entity: {name}", query_start=t0)

    return _ok(
        entity,
        meta={"entity": name},
        tier_required=Tier.PRO,
        query_start=t0,
    )


# ── 3. Actor Dossier ─────────────────────────────────────────────────────

@router.get("/actor/{name:path}")
async def intel_actor_dossier(
    name: str = Path(..., description="Actor name or ID"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Actor dossier: identity, what they control, wealth flows, trust score
    history, connected entities, sector influence.
    """
    t0 = time.time()
    engine = get_db_engine()
    dossier: dict[str, Any] = {
        "name": name,
        "identity": None,
        "controls": [],
        "wealth_flows": [],
        "trust_history": [],
        "connected_entities": [],
        "sector_influence": [],
        "confidence": "derived",
    }

    with engine.connect() as conn:
        # Actor identity
        try:
            row = conn.execute(
                text(
                    "SELECT id, name, tier, category, aum, trust_score, "
                    "motivation_model, connections, credibility, known_positions "
                    "FROM actors "
                    "WHERE UPPER(name) = UPPER(:n) "
                    "   OR UPPER(id) = UPPER(:n) "
                    "LIMIT 1"
                ),
                {"n": name},
            ).fetchone()
            if row:
                dossier["identity"] = {
                    "actor_id": row[0],
                    "name": row[1],
                    "tier": row[2],
                    "sector": row[3],
                    "aum_usd": row[4],
                    "trust_score": row[5],
                    "motivation": row[6],
                    "connections": _safe_json(row[7]),
                    "confidence": row[8] or "derived",
                    "known_positions": _safe_json(row[9]),
                }
        except Exception as exc:
            log.debug("Actor identity lookup failed: {e}", e=str(exc))

        # Wealth flows
        try:
            rows = conn.execute(
                text(
                    "SELECT flow_date, from_actor, to_entity, amount_estimate, "
                    "implication, confidence "
                    "FROM wealth_flows "
                    "WHERE UPPER(from_actor) = UPPER(:n) "
                    "   OR UPPER(to_entity) = UPPER(:n) "
                    "ORDER BY flow_date DESC "
                    "LIMIT 50"
                ),
                {"n": name},
            ).fetchall()
            for r in rows:
                dossier["wealth_flows"].append({
                    "date": _safe_isoformat(r[0]),
                    "from": r[1],
                    "to": r[2],
                    "amount_usd": r[3],
                    "implication": r[4],
                    "confidence": r[5] or "estimated",
                })
        except Exception as exc:
            log.debug("Wealth flows lookup failed: {e}", e=str(exc))

        # Trust score history
        try:
            rows = conn.execute(
                text(
                    "SELECT scored_at, source_type, trust_score, hits, misses, "
                    "total_signals, recency_weighted_score "
                    "FROM trust_scores "
                    "WHERE UPPER(source_name) LIKE UPPER(:q) "
                    "ORDER BY scored_at DESC "
                    "LIMIT 30"
                ),
                {"q": f"%{name}%"},
            ).fetchall()
            for r in rows:
                dossier["trust_history"].append({
                    "scored_at": _safe_isoformat(r[0]),
                    "source_type": r[1],
                    "trust_score": r[2],
                    "hits": r[3],
                    "misses": r[4],
                    "total_signals": r[5],
                    "recency_weighted": r[6],
                    "confidence": "confirmed",
                })
        except Exception as exc:
            log.debug("Trust score history lookup failed: {e}", e=str(exc))

        # Connected entities (ICIJ)
        try:
            rows = conn.execute(
                text(
                    "SELECT entity_name, linked_to, relationship_type, jurisdiction, "
                    "source_dataset "
                    "FROM icij_relationships "
                    "WHERE UPPER(entity_name) LIKE UPPER(:q) "
                    "   OR UPPER(linked_to) LIKE UPPER(:q) "
                    "ORDER BY entity_name "
                    "LIMIT 50"
                ),
                {"q": f"%{name}%"},
            ).fetchall()
            for r in rows:
                dossier["connected_entities"].append({
                    "entity": r[0],
                    "linked_to": r[1],
                    "relationship": r[2],
                    "jurisdiction": r[3],
                    "source": r[4],
                    "confidence": "confirmed",
                })
        except Exception as exc:
            log.debug("ICIJ entity lookup failed: {e}", e=str(exc))

    if dossier["identity"] is None and not dossier["wealth_flows"]:
        return _empty(f"No actor data found for: {name}", query_start=t0)

    return _ok(
        dossier,
        meta={"actor": name},
        tier_required=Tier.PRO,
        query_start=t0,
    )


# ── 4. Ticker Intelligence ───────────────────────────────────────────────

@router.get("/ticker/{symbol}")
async def intel_ticker(
    symbol: str = Path(..., description="Ticker symbol"),
    days: int = Query(30, ge=1, le=365, description="Lookback window"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Everything GRID knows about a ticker: active signals, actor exposure,
    insider activity, dark pool data, options flow, predictions, dealer gamma,
    regime classification.
    """
    t0 = time.time()
    engine = get_db_engine()
    ticker = symbol.upper()
    cutoff = date.today() - timedelta(days=days)
    today = date.today()

    result: dict[str, Any] = {
        "ticker": ticker,
        "predictions": [],
        "signals": [],
        "actor_exposure": [],
        "insider_activity": [],
        "dark_pool": [],
        "options_flow": [],
        "dealer_gamma": None,
        "regime": None,
    }

    with engine.connect() as conn:
        # Active predictions
        try:
            rows = conn.execute(
                text(
                    "SELECT id, model_name, direction, target_price, entry_price, "
                    "confidence, expected_move_pct, expiry, verdict, created_at, "
                    "signals, anti_signals "
                    "FROM oracle_predictions "
                    "WHERE ticker = :t AND created_at >= :cutoff "
                    "ORDER BY created_at DESC "
                    "LIMIT 20"
                ),
                {"t": ticker, "cutoff": cutoff},
            ).fetchall()
            for r in rows:
                result["predictions"].append({
                    "id": r[0],
                    "model": r[1],
                    "direction": r[2],
                    "target_price": float(r[3]) if r[3] else None,
                    "entry_price": float(r[4]) if r[4] else None,
                    "confidence": float(r[5]) if r[5] else None,
                    "expected_move_pct": float(r[6]) if r[6] else None,
                    "expiry": _safe_isoformat(r[7]),
                    "verdict": r[8],
                    "created_at": _safe_isoformat(r[9]),
                    "signals": _safe_json(r[10]),
                    "anti_signals": _safe_json(r[11]),
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("Predictions lookup failed for {t}: {e}", t=ticker, e=str(exc))

        # Signals from signal_data
        try:
            rows = conn.execute(
                text(
                    "SELECT signal_date, source_type, signal_type, direction, "
                    "magnitude, raw_value, confidence, source_name "
                    "FROM signal_data "
                    "WHERE ticker = :t AND signal_date >= :cutoff "
                    "ORDER BY signal_date DESC "
                    "LIMIT 50"
                ),
                {"t": ticker, "cutoff": cutoff},
            ).fetchall()
            for r in rows:
                result["signals"].append({
                    "date": _safe_isoformat(r[0]),
                    "source_type": r[1],
                    "signal_type": r[2],
                    "direction": r[3],
                    "magnitude": float(r[4]) if r[4] else None,
                    "raw_value": r[5],
                    "confidence": float(r[6]) if r[6] else None,
                    "source_name": r[7],
                    "confidence_label": "confirmed",
                })
        except Exception as exc:
            log.debug("Signal lookup failed for {t}: {e}", t=ticker, e=str(exc))

        # Actor exposure — who cares about this ticker
        try:
            rows = conn.execute(
                text(
                    "SELECT actor_id, name, tier, sector, aum_usd, known_positions "
                    "FROM actors "
                    "WHERE known_positions::text ILIKE :q "
                    "   OR connections::text ILIKE :q"
                ),
                {"q": f"%{ticker}%"},
            ).fetchall()
            for r in rows:
                positions = _safe_json(r[5])
                # Extract the specific position for this ticker
                ticker_position = None
                if isinstance(positions, dict):
                    ticker_position = positions.get(ticker)
                elif isinstance(positions, list):
                    ticker_position = [
                        p for p in positions
                        if isinstance(p, dict) and p.get("ticker") == ticker
                    ]

                result["actor_exposure"].append({
                    "actor_id": r[0],
                    "name": r[1],
                    "tier": r[2],
                    "sector": r[3],
                    "aum_usd": r[4],
                    "position": ticker_position,
                    "confidence": "derived",
                })
        except Exception as exc:
            log.debug("Actor exposure lookup failed for {t}: {e}", t=ticker, e=str(exc))

        # Insider activity (Form 4)
        try:
            rows = conn.execute(
                text(
                    "SELECT signal_date, source_name, direction, magnitude, "
                    "raw_value, confidence "
                    "FROM signal_data "
                    "WHERE ticker = :t AND source_type = 'insider' "
                    "  AND signal_date >= :cutoff "
                    "ORDER BY signal_date DESC "
                    "LIMIT 20"
                ),
                {"t": ticker, "cutoff": cutoff},
            ).fetchall()
            for r in rows:
                result["insider_activity"].append({
                    "date": _safe_isoformat(r[0]),
                    "insider": r[1],
                    "direction": r[2],
                    "magnitude": float(r[3]) if r[3] else None,
                    "value": r[4],
                    "confidence": float(r[5]) if r[5] else None,
                    "confidence_label": "confirmed",
                })
        except Exception as exc:
            log.debug("Insider lookup failed for {t}: {e}", t=ticker, e=str(exc))

        # Dark pool activity
        try:
            rows = conn.execute(
                text(
                    "SELECT signal_date, magnitude, raw_value, confidence "
                    "FROM signal_data "
                    "WHERE ticker = :t AND source_type = 'darkpool' "
                    "  AND signal_date >= :cutoff "
                    "ORDER BY signal_date DESC "
                    "LIMIT 20"
                ),
                {"t": ticker, "cutoff": cutoff},
            ).fetchall()
            for r in rows:
                result["dark_pool"].append({
                    "date": _safe_isoformat(r[0]),
                    "magnitude": float(r[1]) if r[1] else None,
                    "value": r[2],
                    "confidence": float(r[3]) if r[3] else None,
                    "confidence_label": "confirmed",
                })
        except Exception as exc:
            log.debug("Dark pool lookup failed for {t}: {e}", t=ticker, e=str(exc))

        # Options flow
        try:
            rows = conn.execute(
                text(
                    "SELECT signal_date, signal_type, direction, magnitude, "
                    "raw_value, confidence "
                    "FROM signal_data "
                    "WHERE ticker = :t "
                    "  AND source_type IN ('options', 'scanner', 'unusual_whales') "
                    "  AND signal_date >= :cutoff "
                    "ORDER BY signal_date DESC "
                    "LIMIT 20"
                ),
                {"t": ticker, "cutoff": cutoff},
            ).fetchall()
            for r in rows:
                result["options_flow"].append({
                    "date": _safe_isoformat(r[0]),
                    "signal_type": r[1],
                    "direction": r[2],
                    "magnitude": float(r[3]) if r[3] else None,
                    "value": r[4],
                    "confidence": float(r[5]) if r[5] else None,
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("Options flow lookup failed for {t}: {e}", t=ticker, e=str(exc))

        # Dealer gamma
        try:
            row = conn.execute(
                text(
                    "SELECT signal_date, raw_value, confidence "
                    "FROM signal_data "
                    "WHERE ticker = :t AND signal_type = 'dealer_gamma' "
                    "ORDER BY signal_date DESC "
                    "LIMIT 1"
                ),
                {"t": ticker},
            ).fetchone()
            if row:
                result["dealer_gamma"] = {
                    "date": _safe_isoformat(row[0]),
                    "value": _safe_json(row[1]),
                    "confidence": float(row[2]) if row[2] else None,
                    "confidence_label": "derived",
                }
        except Exception as exc:
            log.debug("Dealer gamma lookup failed for {t}: {e}", t=ticker, e=str(exc))

        # Regime classification from analytical_snapshots
        try:
            row = conn.execute(
                text(
                    "SELECT payload, created_at "
                    "FROM analytical_snapshots "
                    "WHERE payload::text ILIKE :q "
                    "ORDER BY created_at DESC "
                    "LIMIT 1"
                ),
                {"q": f"%regime%"},
            ).fetchone()
            if row:
                payload = _safe_json(row[0])
                if isinstance(payload, dict):
                    result["regime"] = {
                        "label": payload.get("regime_label")
                              or payload.get("regime")
                              or payload.get("current_regime"),
                        "snapshot_at": _safe_isoformat(row[1]),
                        "confidence_label": "derived",
                    }
        except Exception as exc:
            log.debug("Regime lookup failed: {e}", e=str(exc))

    return _ok(
        result,
        meta={"ticker": ticker, "lookback_days": days},
        tier_required=Tier.BASIC,
        query_start=t0,
    )


# ── 5. Cross-Reference Lie Detector ──────────────────────────────────────

@router.get("/cross-reference/{indicator}")
async def intel_cross_reference(
    indicator: str = Path(..., description="Indicator name (e.g. 'cpi', 'jobs', 'gdp')"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Government stats vs physical reality lie detector for a specific indicator.

    Compares official statistics against physical-world proxies (satellite,
    shipping, energy, employment) to detect divergences.
    """
    t0 = time.time()

    try:
        from intelligence.cross_reference import (
            CrossReferenceEngine,
        )

        engine = get_db_engine()
        xref = CrossReferenceEngine(engine)
        report = xref.generate_report()

        # Filter to the requested indicator
        matches = []
        for item in report.get("comparisons", []):
            item_name = (
                item.get("indicator", "")
                or item.get("name", "")
                or item.get("official_name", "")
            )
            if indicator.lower() in item_name.lower():
                matches.append({
                    **item,
                    "confidence": item.get("confidence_label", "derived"),
                })

        if not matches:
            # Return full report if no specific indicator match
            return _ok(
                {
                    "indicator": indicator,
                    "message": "No exact match. Returning full cross-reference report.",
                    "report": report,
                    "confidence": "derived",
                },
                meta={"indicator": indicator},
                tier_required=Tier.PRO,
                query_start=t0,
            )

        return _ok(
            {
                "indicator": indicator,
                "comparisons": matches,
                "divergence_count": len([
                    m for m in matches if m.get("divergence", False)
                ]),
            },
            meta={"indicator": indicator, "match_count": len(matches)},
            tier_required=Tier.PRO,
            query_start=t0,
        )

    except ImportError as exc:
        raise HTTPException(status_code=503, detail="Cross-reference engine not available") from exc
    except Exception as exc:
        log.warning("Cross-reference failed for {i}: {e}", i=indicator, e=str(exc))
        raise HTTPException(status_code=500, detail=f"Cross-reference failed: {exc}") from exc


# ── 6. Deep Dive — Forensic Price Decomposition ─────────────────────────

@router.get("/deep-dive/{ticker}")
async def intel_deep_dive(
    ticker: str = Path(..., description="Ticker symbol"),
    days: int = Query(90, ge=7, le=365, description="Lookback window"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Full forensic price decomposition: what moved the price, who did it,
    why, what's priced in vs not.
    """
    t0 = time.time()
    ticker = ticker.upper()

    try:
        from intelligence.news_impact import DeepDiveEngine, ensure_tables

        engine = get_db_engine()
        ensure_tables(engine)
        dive = DeepDiveEngine(engine)
        report = dive.generate_deep_dive(ticker, days)

        return _ok(
            {
                "ticker": report.ticker,
                "name": report.name,
                "generated_at": _safe_isoformat(report.generated_at),
                "total_moves_analyzed": report.total_moves_analyzed,
                "avg_explained_pct": report.avg_explained_pct,
                "total_baked_in_bps": report.total_baked_in_bps,
                "total_pending_bps": report.total_pending_bps,
                "historical_hit_rate": report.historical_hit_rate,
                "catalyst_breakdown": report.catalyst_breakdown,
                "top_catalysts": [
                    {
                        "title": c.title,
                        "type": c.catalyst_type,
                        "horizon": c.horizon,
                        "direction": c.direction,
                        "estimated_bps": c.estimated_bps,
                        "confidence": c.confidence,
                        "date": _safe_isoformat(c.event_date),
                        "confidence_label": "derived",
                    }
                    for c in report.top_catalysts
                ],
                "significant_moves": [
                    {
                        "date": str(a.move_date),
                        "pct": round(a.move_pct * 100, 2),
                        "direction": a.move_direction,
                        "explained_bps": a.total_explained_bps,
                        "unexplained_bps": a.unexplained_bps,
                        "macro_bps": a.macro_contribution_bps,
                        "sector_bps": a.sector_contribution_bps,
                        "catalysts": [
                            {
                                "title": c.title[:80],
                                "type": c.catalyst_type,
                                "bps": c.estimated_bps,
                                "direction": c.direction,
                                "confidence_label": "derived",
                            }
                            for c in a.catalysts[:5]
                        ],
                    }
                    for a in report.significant_moves
                ],
                "active_expectations": [
                    {
                        "description": e.description,
                        "catalyst_type": e.catalyst_type,
                        "horizon": e.horizon,
                        "direction": e.expected_direction,
                        "magnitude_bps": e.expected_magnitude_bps,
                        "baked_in_pct": e.baked_in_pct,
                        "deadline": _safe_isoformat(e.deadline),
                        "confidence_label": "estimated",
                    }
                    for e in report.active_expectations
                ],
                "narrative": report.narrative,
                "confidence": report.confidence,
                "confidence_label": "derived",
            },
            meta={"ticker": ticker, "lookback_days": days},
            tier_required=Tier.ENTERPRISE,
            query_start=t0,
        )

    except ImportError as exc:
        raise HTTPException(status_code=503, detail="Deep dive engine not available") from exc
    except Exception as exc:
        log.warning("Deep dive failed for {t}: {e}", t=ticker, e=str(exc))
        raise HTTPException(status_code=500, detail=f"Deep dive failed: {exc}") from exc


# ── 7. Network Graph Traversal ───────────────────────────────────────────

@router.get("/network/{entity:path}")
async def intel_network(
    entity: str = Path(..., description="Entity or actor name"),
    depth: int = Query(2, ge=1, le=5, description="Hop depth (1-5)"),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Multi-hop graph traversal from an entity through ICIJ/actor data.
    Returns nodes and edges for visualization.
    """
    t0 = time.time()
    engine = get_db_engine()

    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    frontier: set[str] = {entity.upper()}
    visited: set[str] = set()

    with engine.connect() as conn:
        for hop in range(depth):
            if not frontier:
                break

            next_frontier: set[str] = set()
            for name in frontier:
                if name in visited:
                    continue
                visited.add(name)

                # Add this node
                if name not in nodes:
                    nodes[name] = {
                        "id": name,
                        "type": "unknown",
                        "hop": hop,
                        "confidence": "derived",
                    }

                # ICIJ relationships
                try:
                    rows = conn.execute(
                        text(
                            "SELECT entity_name, linked_to, relationship_type, "
                            "jurisdiction, source_dataset "
                            "FROM icij_relationships "
                            "WHERE UPPER(entity_name) = :n "
                            "   OR UPPER(linked_to) = :n"
                        ),
                        {"n": name},
                    ).fetchall()
                    for r in rows:
                        src = r[0].upper() if r[0] else name
                        tgt = r[1].upper() if r[1] else name
                        edges.append({
                            "source": src,
                            "target": tgt,
                            "relationship": r[2],
                            "jurisdiction": r[3],
                            "dataset": r[4],
                            "hop": hop,
                            "confidence": "confirmed",
                        })
                        for n in (src, tgt):
                            if n not in visited:
                                next_frontier.add(n)
                            if n not in nodes:
                                nodes[n] = {
                                    "id": n,
                                    "type": "entity",
                                    "hop": hop + 1,
                                    "confidence": "confirmed",
                                }
                except Exception as exc:
                    log.debug(
                        "ICIJ traversal failed at hop {h} for {n}: {e}",
                        h=hop, n=name, e=str(exc),
                    )

                # Actor connections
                try:
                    rows = conn.execute(
                        text(
                            "SELECT actor_id, name, tier, sector, connections "
                            "FROM actors "
                            "WHERE UPPER(name) = :n "
                            "   OR UPPER(actor_id) = :n"
                        ),
                        {"n": name},
                    ).fetchall()
                    for r in rows:
                        actor_name = r[1].upper() if r[1] else name
                        if actor_name not in nodes:
                            nodes[actor_name] = {
                                "id": actor_name,
                                "type": "actor",
                                "tier": r[2],
                                "sector": r[3],
                                "hop": hop,
                                "confidence": "derived",
                            }
                        # Parse connections to find adjacent nodes
                        connections = _safe_json(r[4])
                        if isinstance(connections, list):
                            for conn_name in connections[:20]:
                                if isinstance(conn_name, str):
                                    cn = conn_name.upper()
                                    edges.append({
                                        "source": actor_name,
                                        "target": cn,
                                        "relationship": "connected",
                                        "hop": hop,
                                        "confidence": "derived",
                                    })
                                    if cn not in visited:
                                        next_frontier.add(cn)
                                elif isinstance(conn_name, dict):
                                    cn = (
                                        conn_name.get("name", "")
                                        or conn_name.get("actor", "")
                                    ).upper()
                                    if cn:
                                        edges.append({
                                            "source": actor_name,
                                            "target": cn,
                                            "relationship": conn_name.get(
                                                "relationship", "connected"
                                            ),
                                            "hop": hop,
                                            "confidence": "derived",
                                        })
                                        if cn not in visited:
                                            next_frontier.add(cn)
                except Exception as exc:
                    log.debug(
                        "Actor traversal failed at hop {h} for {n}: {e}",
                        h=hop, n=name, e=str(exc),
                    )

            frontier = next_frontier

    if not nodes:
        return _empty(
            f"No network data found for: {entity}",
            tier_required=Tier.PRO,
            query_start=t0,
        )

    # De-duplicate edges
    seen_edges: set[str] = set()
    unique_edges: list[dict[str, Any]] = []
    for e in edges:
        key = f"{e['source']}|{e['target']}|{e.get('relationship', '')}"
        rev_key = f"{e['target']}|{e['source']}|{e.get('relationship', '')}"
        if key not in seen_edges and rev_key not in seen_edges:
            seen_edges.add(key)
            unique_edges.append(e)

    return _ok(
        {
            "root": entity,
            "depth": depth,
            "nodes": list(nodes.values()),
            "edges": unique_edges,
        },
        meta={
            "entity": entity,
            "depth": depth,
            "node_count": len(nodes),
            "edge_count": len(unique_edges),
            "hops_traversed": min(depth, len(visited)),
        },
        tier_required=Tier.PRO,
        query_start=t0,
    )


# ── 8. Market Brief ──────────────────────────────────────────────────────

@router.get("/market-brief")
async def intel_market_brief(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Current market summary: regime, top signals, active predictions,
    notable flows. The daily intelligence briefing.
    """
    t0 = time.time()
    engine = get_db_engine()
    today = date.today()
    week_ago = today - timedelta(days=7)

    brief: dict[str, Any] = {
        "date": today.isoformat(),
        "regime": None,
        "top_signals": [],
        "active_predictions": [],
        "notable_flows": [],
        "summary_stats": {},
    }

    with engine.connect() as conn:
        # Current regime
        try:
            row = conn.execute(
                text(
                    "SELECT payload, created_at "
                    "FROM analytical_snapshots "
                    "WHERE payload::text ILIKE :q "
                    "ORDER BY created_at DESC "
                    "LIMIT 1"
                ),
                {"q": "%regime%"},
            ).fetchone()
            if row:
                payload = _safe_json(row[0])
                if isinstance(payload, dict):
                    brief["regime"] = {
                        "label": payload.get("regime_label")
                              or payload.get("regime")
                              or payload.get("current_regime"),
                        "updated_at": _safe_isoformat(row[1]),
                        "confidence_label": "derived",
                    }
        except Exception as exc:
            log.debug("Regime lookup failed: {e}", e=str(exc))

        # Top signals (last 7 days, highest confidence)
        try:
            rows = conn.execute(
                text(
                    "SELECT ticker, signal_date, source_type, signal_type, "
                    "direction, magnitude, confidence, source_name "
                    "FROM signal_data "
                    "WHERE signal_date >= :cutoff "
                    "ORDER BY confidence DESC NULLS LAST, magnitude DESC NULLS LAST "
                    "LIMIT 15"
                ),
                {"cutoff": week_ago},
            ).fetchall()
            for r in rows:
                brief["top_signals"].append({
                    "ticker": r[0],
                    "date": _safe_isoformat(r[1]),
                    "source_type": r[2],
                    "signal_type": r[3],
                    "direction": r[4],
                    "magnitude": float(r[5]) if r[5] else None,
                    "confidence": float(r[6]) if r[6] else None,
                    "source": r[7],
                    "confidence_label": "confirmed",
                })
        except Exception as exc:
            log.debug("Top signals lookup failed: {e}", e=str(exc))

        # Active predictions
        try:
            rows = conn.execute(
                text(
                    "SELECT id, ticker, model_name, direction, confidence, "
                    "target_price, entry_price, expiry, created_at "
                    "FROM oracle_predictions "
                    "WHERE verdict = 'pending' AND expiry > :today "
                    "ORDER BY confidence DESC NULLS LAST "
                    "LIMIT 10"
                ),
                {"today": today},
            ).fetchall()
            for r in rows:
                brief["active_predictions"].append({
                    "id": r[0],
                    "ticker": r[1],
                    "model": r[2],
                    "direction": r[3],
                    "confidence": float(r[4]) if r[4] else None,
                    "target_price": float(r[5]) if r[5] else None,
                    "entry_price": float(r[6]) if r[6] else None,
                    "expiry": _safe_isoformat(r[7]),
                    "created_at": _safe_isoformat(r[8]),
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("Active predictions lookup failed: {e}", e=str(exc))

        # Notable wealth flows (last 7 days)
        try:
            rows = conn.execute(
                text(
                    "SELECT flow_date, from_actor, to_entity, amount_estimate, "
                    "implication, confidence "
                    "FROM wealth_flows "
                    "WHERE flow_date >= :cutoff "
                    "ORDER BY amount_estimate DESC NULLS LAST "
                    "LIMIT 10"
                ),
                {"cutoff": week_ago},
            ).fetchall()
            for r in rows:
                brief["notable_flows"].append({
                    "date": _safe_isoformat(r[0]),
                    "from": r[1],
                    "to": r[2],
                    "amount_usd": r[3],
                    "implication": r[4],
                    "confidence": r[5] or "estimated",
                })
        except Exception as exc:
            log.debug("Wealth flows lookup failed: {e}", e=str(exc))

        # Summary stats
        try:
            stats_row = conn.execute(
                text(
                    "SELECT "
                    "  (SELECT COUNT(*) FROM oracle_predictions "
                    "   WHERE verdict = 'pending' AND expiry > :today) AS active_preds, "
                    "  (SELECT COUNT(*) FROM signal_data "
                    "   WHERE signal_date >= :cutoff) AS signals_7d, "
                    "  (SELECT COUNT(DISTINCT ticker) FROM signal_data "
                    "   WHERE signal_date >= :cutoff) AS tickers_active"
                ),
                {"today": today, "cutoff": week_ago},
            ).fetchone()
            if stats_row:
                brief["summary_stats"] = {
                    "active_predictions": stats_row[0],
                    "signals_7d": stats_row[1],
                    "tickers_active": stats_row[2],
                    "confidence_label": "confirmed",
                }
        except Exception as exc:
            log.debug("Summary stats failed: {e}", e=str(exc))

    return _ok(
        brief,
        meta={"date": today.isoformat()},
        tier_required=Tier.FREE,
        query_start=t0,
    )


# ── 9. Active Predictions ────────────────────────────────────────────────

@router.get("/predictions/active")
async def intel_predictions_active(
    ticker: str | None = Query(None, description="Filter by ticker"),
    model: str | None = Query(None, description="Filter by model"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Active predictions with scores, confidence, lever/condition breakdown."""
    t0 = time.time()
    engine = get_db_engine()
    today = date.today()

    # Security: where_clauses contains only static string literals — no user
    # input is ever interpolated into the SQL fragment strings themselves.
    # All user-supplied values (ticker, model) flow exclusively through named
    # :param bind parameters in the params dict, never into where_sql directly.
    where_clauses = ["verdict = 'pending'", "expiry > :today"]
    params: dict[str, Any] = {"today": today, "lim": limit, "off": offset}

    if ticker:
        where_clauses.append("ticker = :ticker")
        params["ticker"] = ticker.upper()
    if model:
        where_clauses.append("model_name = :model")
        params["model"] = model

    where_sql = " AND ".join(where_clauses)

    predictions: list[dict[str, Any]] = []

    with engine.connect() as conn:
        try:
            count_row = conn.execute(
                text(f"SELECT COUNT(*) FROM oracle_predictions WHERE {where_sql}"),
                params,
            ).fetchone()
            total = count_row[0] if count_row else 0

            rows = conn.execute(
                text(
                    f"SELECT id, created_at, ticker, prediction_type, direction, "
                    f"target_price, entry_price, expiry, confidence, "
                    f"expected_move_pct, signal_strength, coherence, "
                    f"model_name, model_version, signals, anti_signals, "
                    f"flow_context "
                    f"FROM oracle_predictions WHERE {where_sql} "
                    f"ORDER BY confidence DESC NULLS LAST, created_at DESC "
                    f"LIMIT :lim OFFSET :off"
                ),
                params,
            ).fetchall()

            for r in rows:
                signals = _safe_json(r[14])
                anti_signals = _safe_json(r[15])
                flow_context = _safe_json(r[16])

                # Separate levers from conditions
                levers = []
                conditions = []
                if isinstance(signals, list):
                    for s in signals:
                        if isinstance(s, dict):
                            if s.get("is_lever") or s.get("type") == "lever":
                                levers.append(s)
                            else:
                                conditions.append(s)
                        elif isinstance(s, str):
                            conditions.append({"description": s})

                predictions.append({
                    "id": r[0],
                    "created_at": _safe_isoformat(r[1]),
                    "ticker": r[2],
                    "prediction_type": r[3],
                    "direction": r[4],
                    "target_price": float(r[5]) if r[5] else None,
                    "entry_price": float(r[6]) if r[6] else None,
                    "expiry": _safe_isoformat(r[7]),
                    "confidence": float(r[8]) if r[8] else None,
                    "expected_move_pct": float(r[9]) if r[9] else None,
                    "signal_strength": float(r[10]) if r[10] else None,
                    "coherence": float(r[11]) if r[11] else None,
                    "model": r[12],
                    "model_version": r[13],
                    "levers": levers,
                    "conditions": conditions,
                    "anti_signals": anti_signals,
                    "flow_context": flow_context,
                    "days_remaining": (r[7] - today).days if r[7] else None,
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.warning("Active predictions query failed: {e}", e=str(exc))
            return _empty(
                f"Predictions query failed: {exc}",
                tier_required=Tier.BASIC,
                query_start=t0,
            )

    return _ok(
        predictions,
        meta={"total": total, "limit": limit, "offset": offset},
        tier_required=Tier.BASIC,
        query_start=t0,
    )


# ── 10. Track Record — Proof We're Worth Paying For ──────────────────────

@router.get("/predictions/track-record")
async def intel_predictions_track_record(
    model: str | None = Query(None, description="Filter by model"),
    ticker: str | None = Query(None, description="Filter by ticker"),
    timeframe: str | None = Query(
        None, description="day | week | month | quarter | all"
    ),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Historical prediction accuracy by model, ticker, timeframe.
    This is the 'proof we're worth paying for' endpoint.
    """
    t0 = time.time()
    engine = get_db_engine()
    today = date.today()

    # Timeframe filter
    cutoff = None
    if timeframe == "day":
        cutoff = today - timedelta(days=1)
    elif timeframe == "week":
        cutoff = today - timedelta(days=7)
    elif timeframe == "month":
        cutoff = today - timedelta(days=30)
    elif timeframe == "quarter":
        cutoff = today - timedelta(days=90)
    # "all" or None = no cutoff

    where_clauses = ["verdict IN ('hit', 'miss', 'partial')"]
    params: dict[str, Any] = {}

    if cutoff:
        where_clauses.append("scored_at >= :cutoff")
        params["cutoff"] = cutoff
    if model:
        where_clauses.append("model_name = :model")
        params["model"] = model
    if ticker:
        where_clauses.append("ticker = :ticker")
        params["ticker"] = ticker.upper()

    where_sql = " AND ".join(where_clauses)

    track_record: dict[str, Any] = {
        "overall": {},
        "by_model": [],
        "by_ticker": [],
        "by_direction": [],
        "recent_results": [],
        "calibration": [],
    }

    with engine.connect() as conn:
        try:
            # Overall stats
            row = conn.execute(
                text(
                    f"SELECT "
                    f"  COUNT(*) AS total, "
                    f"  COUNT(*) FILTER (WHERE verdict = 'hit') AS hits, "
                    f"  COUNT(*) FILTER (WHERE verdict = 'miss') AS misses, "
                    f"  COUNT(*) FILTER (WHERE verdict = 'partial') AS partials, "
                    f"  AVG(pnl_pct) AS avg_pnl, "
                    f"  AVG(confidence) AS avg_confidence, "
                    f"  MIN(scored_at) AS first_scored, "
                    f"  MAX(scored_at) AS last_scored "
                    f"FROM oracle_predictions WHERE {where_sql}"
                ),
                params,
            ).fetchone()

            if row and row[0] > 0:
                total = row[0]
                hits = row[1]
                track_record["overall"] = {
                    "total_scored": total,
                    "hits": hits,
                    "misses": row[2],
                    "partials": row[3],
                    "hit_rate": round(hits / total, 4) if total else 0,
                    "avg_pnl_pct": round(float(row[4]), 2) if row[4] else None,
                    "avg_confidence": round(float(row[5]), 4) if row[5] else None,
                    "first_scored": _safe_isoformat(row[6]),
                    "last_scored": _safe_isoformat(row[7]),
                    "confidence_label": "confirmed",
                }

            # By model
            rows = conn.execute(
                text(
                    f"SELECT model_name, "
                    f"  COUNT(*) AS total, "
                    f"  COUNT(*) FILTER (WHERE verdict = 'hit') AS hits, "
                    f"  COUNT(*) FILTER (WHERE verdict = 'miss') AS misses, "
                    f"  AVG(pnl_pct) AS avg_pnl, "
                    f"  AVG(confidence) AS avg_conf "
                    f"FROM oracle_predictions WHERE {where_sql} "
                    f"GROUP BY model_name "
                    f"ORDER BY COUNT(*) FILTER (WHERE verdict = 'hit')::float "
                    f"  / NULLIF(COUNT(*), 0) DESC"
                ),
                params,
            ).fetchall()
            for r in rows:
                t_count = r[1]
                h_count = r[2]
                track_record["by_model"].append({
                    "model": r[0],
                    "total": t_count,
                    "hits": h_count,
                    "misses": r[3],
                    "hit_rate": round(h_count / t_count, 4) if t_count else 0,
                    "avg_pnl_pct": round(float(r[4]), 2) if r[4] else None,
                    "avg_confidence": round(float(r[5]), 4) if r[5] else None,
                    "confidence_label": "confirmed",
                })

            # By ticker (top 20)
            rows = conn.execute(
                text(
                    f"SELECT ticker, "
                    f"  COUNT(*) AS total, "
                    f"  COUNT(*) FILTER (WHERE verdict = 'hit') AS hits, "
                    f"  COUNT(*) FILTER (WHERE verdict = 'miss') AS misses, "
                    f"  AVG(pnl_pct) AS avg_pnl "
                    f"FROM oracle_predictions WHERE {where_sql} "
                    f"GROUP BY ticker "
                    f"HAVING COUNT(*) >= 3 "
                    f"ORDER BY AVG(pnl_pct) DESC NULLS LAST "
                    f"LIMIT 20"
                ),
                params,
            ).fetchall()
            for r in rows:
                t_count = r[1]
                h_count = r[2]
                track_record["by_ticker"].append({
                    "ticker": r[0],
                    "total": t_count,
                    "hits": h_count,
                    "misses": r[3],
                    "hit_rate": round(h_count / t_count, 4) if t_count else 0,
                    "avg_pnl_pct": round(float(r[4]), 2) if r[4] else None,
                    "confidence_label": "confirmed",
                })

            # By direction
            rows = conn.execute(
                text(
                    f"SELECT direction, "
                    f"  COUNT(*) AS total, "
                    f"  COUNT(*) FILTER (WHERE verdict = 'hit') AS hits, "
                    f"  AVG(pnl_pct) AS avg_pnl "
                    f"FROM oracle_predictions WHERE {where_sql} "
                    f"GROUP BY direction "
                    f"ORDER BY direction"
                ),
                params,
            ).fetchall()
            for r in rows:
                t_count = r[1]
                h_count = r[2]
                track_record["by_direction"].append({
                    "direction": r[0],
                    "total": t_count,
                    "hits": h_count,
                    "hit_rate": round(h_count / t_count, 4) if t_count else 0,
                    "avg_pnl_pct": round(float(r[3]), 2) if r[3] else None,
                    "confidence_label": "confirmed",
                })

            # Recent results (last 20 scored)
            rows = conn.execute(
                text(
                    f"SELECT id, ticker, model_name, direction, confidence, "
                    f"verdict, pnl_pct, actual_move_pct, scored_at, score_notes "
                    f"FROM oracle_predictions WHERE {where_sql} "
                    f"ORDER BY scored_at DESC "
                    f"LIMIT 20"
                ),
                params,
            ).fetchall()
            for r in rows:
                track_record["recent_results"].append({
                    "id": r[0],
                    "ticker": r[1],
                    "model": r[2],
                    "direction": r[3],
                    "confidence": float(r[4]) if r[4] else None,
                    "verdict": r[5],
                    "pnl_pct": round(float(r[6]), 2) if r[6] else None,
                    "actual_move_pct": round(float(r[7]), 2) if r[7] else None,
                    "scored_at": _safe_isoformat(r[8]),
                    "notes": r[9],
                    "confidence_label": "confirmed",
                })

            # Calibration: confidence bucket vs actual hit rate
            rows = conn.execute(
                text(
                    f"SELECT "
                    f"  CASE "
                    f"    WHEN confidence < 0.3 THEN 'low (0-30%)' "
                    f"    WHEN confidence < 0.5 THEN 'moderate (30-50%)' "
                    f"    WHEN confidence < 0.7 THEN 'good (50-70%)' "
                    f"    WHEN confidence < 0.85 THEN 'high (70-85%)' "
                    f"    ELSE 'very_high (85-100%)' "
                    f"  END AS bucket, "
                    f"  COUNT(*) AS total, "
                    f"  COUNT(*) FILTER (WHERE verdict = 'hit') AS hits, "
                    f"  AVG(confidence) AS avg_stated_confidence "
                    f"FROM oracle_predictions WHERE {where_sql} "
                    f"  AND confidence IS NOT NULL "
                    f"GROUP BY bucket "
                    f"ORDER BY AVG(confidence)"
                ),
                params,
            ).fetchall()
            for r in rows:
                t_count = r[1]
                h_count = r[2]
                stated = float(r[3]) if r[3] else 0
                actual = h_count / t_count if t_count else 0
                track_record["calibration"].append({
                    "bucket": r[0],
                    "total": t_count,
                    "hits": h_count,
                    "stated_confidence": round(stated, 4),
                    "actual_hit_rate": round(actual, 4),
                    "calibration_error": round(abs(stated - actual), 4),
                    "confidence_label": "confirmed",
                })

        except Exception as exc:
            log.warning("Track record query failed: {e}", e=str(exc))
            return _empty(
                f"Track record query failed: {exc}",
                tier_required=Tier.FREE,
                query_start=t0,
            )

    if not track_record["overall"]:
        return _empty(
            "No scored predictions yet",
            tier_required=Tier.FREE,
            query_start=t0,
        )

    return _ok(
        track_record,
        meta={
            "model_filter": model,
            "ticker_filter": ticker,
            "timeframe": timeframe or "all",
        },
        tier_required=Tier.FREE,
        query_start=t0,
    )


# ── 11. BRIEFING — the front door ───────────────────────────────────────

@router.get("/briefing")
async def intel_briefing(
    since: str | None = Query(
        None,
        description="ISO timestamp — only return signals newer than this (delta mode)",
    ),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Single-call situation report. This is the product.

    Aggregates: regime, active causal chains, lever events (24h),
    convergence signals, cross-reference red flags, top options mispricing,
    active predictions with inline track record, and a thesis summary.

    Every signal includes an invalidation condition where available.
    Every signal includes inline historical accuracy where data exists.
    Every lever event identifies the valve, the flow direction, the actor,
    and the estimated dollar magnitude per the Prediction Causation Standard.

    Pass ?since=<ISO timestamp> to get only what changed since your last call
    (delta mode). Omit for the full picture.

    One call. Full picture. What an LLM agent or trader checking their
    phone at 6am actually needs.
    """
    t0 = time.time()
    engine = get_db_engine()
    now = datetime.now(timezone.utc)
    today = now.date()
    day_ago = now - timedelta(hours=24)
    week_ago = today - timedelta(days=7)

    # Delta mode: if caller passes ?since=<ISO>, narrow the window
    delta_cutoff: datetime | None = None
    if since:
        try:
            delta_cutoff = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass  # fall back to full briefing

    # Use delta cutoff for time-filtered sections if provided
    signal_cutoff = delta_cutoff or day_ago
    flow_cutoff = delta_cutoff.date() if delta_cutoff else week_ago

    briefing: dict[str, Any] = {
        "as_of": now.isoformat(),
        "delta_since": delta_cutoff.isoformat() if delta_cutoff else None,
        "regime": None,
        "thesis": None,
        "lever_events_24h": [],
        "causal_chains_active": [],
        "convergence_signals": [],
        "cross_reference_flags": [],
        "options_mispricing": [],
        "predictions_active": [],
        "track_record": {},
        "notable_flows": [],
    }

    with engine.connect() as conn:
        # ── REGIME ────────────────────────────────────────────────
        try:
            row = conn.execute(text(
                "SELECT regime, confidence, obs_date, source "
                "FROM regime_history ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()
            if row:
                briefing["regime"] = {
                    "label": row[0],
                    "confidence": float(row[1]) if row[1] else None,
                    "as_of": _safe_isoformat(row[2]),
                    "source": row[3],
                    "confidence_label": "derived",
                }
        except Exception as exc:
            log.debug("Intel briefing: regime query failed: {e}", e=str(exc))

        # ── THESIS ────────────────────────────────────────────────
        try:
            row = conn.execute(text(
                "SELECT overall_direction, conviction, narrative, "
                "key_drivers, risk_factors, timestamp "
                "FROM thesis_snapshots ORDER BY timestamp DESC LIMIT 1"
            )).fetchone()
            if row:
                briefing["thesis"] = {
                    "direction": row[0],
                    "conviction": float(row[1]) if row[1] else None,
                    "narrative": row[2],
                    "key_drivers": _safe_json(row[3]),
                    "risk_factors": _safe_json(row[4]),
                    "generated_at": _safe_isoformat(row[5]),
                    "confidence_label": "derived",
                }
        except Exception as exc:
            log.debug("Intel briefing: thesis query failed: {e}", e=str(exc))

        # ── LEVER EVENTS (last 24h) — enriched with dollar flows + valve ID ──
        # Per Prediction Causation Standard: every lever event must identify
        # WHO did WHAT affecting WHICH liquidity valve, with dollar magnitude.
        try:
            from intelligence.lever_pullers import get_active_lever_events
            from dataclasses import asdict
            lever_events = get_active_lever_events(engine, days=1)
            # Build dollar flow lookup for enrichment
            flow_lookup: dict[str, dict[str, Any]] = {}
            try:
                flow_rows = conn.execute(text(
                    "SELECT source_id, ticker, amount_usd, direction "
                    "FROM wealth_flows "
                    "WHERE flow_date >= :cutoff "
                    "ORDER BY amount_usd DESC NULLS LAST"
                ), {"cutoff": signal_cutoff.date() if isinstance(signal_cutoff, datetime) else signal_cutoff}).fetchall()
                for fr in flow_rows:
                    key = f"{fr[0]}:{fr[1]}".lower()
                    flow_lookup[key] = {
                        "amount_usd": float(fr[2]) if fr[2] else None,
                        "flow_direction": fr[3],
                    }
            except Exception as exc:
                log.debug("Intel briefing: dollar flow enrichment failed: {e}", e=str(exc))

            for evt in lever_events[:15]:
                evt_dict = asdict(evt) if hasattr(evt, "__dataclass_fields__") else evt
                puller = evt_dict.get("puller", {})
                if hasattr(puller, "__dataclass_fields__"):
                    puller = asdict(puller)
                tickers = evt_dict.get("tickers", [])
                action = evt_dict.get("action", "")

                # Identify the valve per SOP
                valve = _identify_valve(
                    puller.get("category", ""),
                    action,
                    tickers,
                )

                # Look up dollar magnitude from flows
                amount_usd = None
                for tk in tickers:
                    flow_key = f"{puller.get('id', '')}:{tk}".lower()
                    if flow_key in flow_lookup:
                        amount_usd = flow_lookup[flow_key].get("amount_usd")
                        break
                    # Fallback: try source_id without prefix
                    src_id = puller.get("id", "").split(":", 1)[-1] if ":" in puller.get("id", "") else puller.get("id", "")
                    flow_key2 = f"{src_id}:{tk}".lower()
                    if flow_key2 in flow_lookup:
                        amount_usd = flow_lookup[flow_key2].get("amount_usd")
                        break

                briefing["lever_events_24h"].append({
                    "actor": puller.get("name", puller.get("id", "unknown")),
                    "actor_category": puller.get("category", "unknown"),
                    "action": action,
                    "tickers": tickers,
                    "valve": valve,
                    "flow_direction": "inflow" if action in ("BUY", "POSITION_INCREASE", "SPEECH_DOVISH") else "outflow",
                    "amount_usd": amount_usd,
                    "motivation": evt_dict.get("motivation_assessment", "unknown"),
                    "trust_score": puller.get("trust_score"),
                    "influence_rank": puller.get("influence_rank"),
                    "confidence": evt_dict.get("confidence"),
                    "invalidation": f"Reversed if {puller.get('name', 'actor')} closes position within 5 days",
                    "confidence_label": "confirmed" if evt_dict.get("confidence", 0) > 0.7 else "derived",
                })
        except ImportError:
            # Fallback: raw signal_data query if intelligence modules not available
            try:
                recent_actors = conn.execute(text(
                    "SELECT DISTINCT source_name, ticker, direction, confidence "
                    "FROM signal_data "
                    "WHERE signal_date >= :cutoff AND confidence > 0.6 "
                    "ORDER BY confidence DESC LIMIT 15"
                ), {"cutoff": signal_cutoff.date() if isinstance(signal_cutoff, datetime) else signal_cutoff}).fetchall()
                for r in recent_actors:
                    briefing["lever_events_24h"].append({
                        "actor": r[0],
                        "tickers": [r[1]] if r[1] else [],
                        "action": r[2],
                        "confidence": float(r[3]) if r[3] else None,
                        "confidence_label": "confirmed",
                    })
            except Exception as exc:
                log.debug("Intel briefing: signal_data actor fallback failed: {e}", e=str(exc))
        except Exception as exc:
            log.debug("Lever events enrichment failed: {e}", e=str(exc))

        # ── CAUSAL CHAINS (active) ───────────────────────────────
        try:
            rows = conn.execute(text(
                "SELECT id, ticker, chain, total_hops, timespan_days, "
                "total_dollar_flow, key_actors, narrative, confidence, created_at "
                "FROM causal_chains "
                "ORDER BY confidence DESC NULLS LAST, created_at DESC "
                "LIMIT 10"
            )).fetchall()
            for r in rows:
                briefing["causal_chains_active"].append({
                    "id": r[0],
                    "ticker": r[1],
                    "chain": _safe_json(r[2]),
                    "hops": r[3],
                    "timespan_days": r[4],
                    "dollar_flow": float(r[5]) if r[5] else None,
                    "key_actors": _safe_json(r[6]),
                    "narrative": r[7],
                    "confidence": float(r[8]) if r[8] else None,
                    "created_at": _safe_isoformat(r[9]),
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("Intel briefing: causal chains query failed: {e}", e=str(exc))

        # ── CONVERGENCE SIGNALS (trust-weighted) ─────────────────
        # Multiple signal types firing on the same ticker = convergence.
        # Weighted by each source's Bayesian trust score so 2 high-trust
        # sources outrank 3 low-trust sources.
        try:
            # Join signal_data with trust scores from signal_sources
            rows = conn.execute(text(
                "SELECT sd.ticker, "
                "  COUNT(DISTINCT sd.source_type) AS source_count, "
                "  array_agg(DISTINCT sd.source_type) AS sources, "
                "  MODE() WITHIN GROUP (ORDER BY sd.direction) AS consensus_dir, "
                "  AVG(sd.confidence) AS avg_conf, "
                "  COUNT(*) AS total_signals, "
                "  AVG(COALESCE(ss.trust_score, 0.5)) AS avg_trust, "
                "  SUM(sd.confidence * COALESCE(ss.trust_score, 0.5)) "
                "    / NULLIF(SUM(COALESCE(ss.trust_score, 0.5)), 0) AS weighted_conf "
                "FROM signal_data sd "
                "LEFT JOIN signal_sources ss "
                "  ON sd.source_type = ss.source_type "
                "  AND sd.source_name = ss.source_id "
                "WHERE sd.signal_date >= :cutoff "
                "GROUP BY sd.ticker "
                "HAVING COUNT(DISTINCT sd.source_type) >= 2 "
                "ORDER BY "
                "  SUM(sd.confidence * COALESCE(ss.trust_score, 0.5)) "
                "    / NULLIF(SUM(COALESCE(ss.trust_score, 0.5)), 0) DESC NULLS LAST, "
                "  COUNT(DISTINCT sd.source_type) DESC "
                "LIMIT 10"
            ), {"cutoff": flow_cutoff}).fetchall()
            for r in rows:
                weighted = round(float(r[7]), 3) if r[7] else None
                raw_avg = round(float(r[4]), 3) if r[4] else None
                avg_trust = round(float(r[6]), 3) if r[6] else None
                briefing["convergence_signals"].append({
                    "ticker": r[0],
                    "source_count": int(r[1]),
                    "sources": r[2] if isinstance(r[2], list) else [],
                    "consensus_direction": r[3],
                    "confidence_raw": raw_avg,
                    "confidence_trust_weighted": weighted,
                    "avg_source_trust": avg_trust,
                    "total_signals": int(r[5]),
                    "invalidation": f"Drops below {r[1]-1} confirming sources or avg trust < 0.5",
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("Intel briefing: trust-weighted convergence failed, trying unweighted: {e}", e=str(exc))
            # Fallback to unweighted if join fails (signal_sources may not exist)
            try:
                rows = conn.execute(text(
                    "SELECT ticker, "
                    "  COUNT(DISTINCT source_type) as source_count, "
                    "  array_agg(DISTINCT source_type) as sources, "
                    "  MODE() WITHIN GROUP (ORDER BY direction) as consensus_dir, "
                    "  AVG(confidence) as avg_conf, "
                    "  COUNT(*) as total_signals "
                    "FROM signal_data "
                    "WHERE signal_date >= :cutoff "
                    "GROUP BY ticker "
                    "HAVING COUNT(DISTINCT source_type) >= 2 "
                    "ORDER BY COUNT(DISTINCT source_type) DESC, AVG(confidence) DESC "
                    "LIMIT 10"
                ), {"cutoff": flow_cutoff}).fetchall()
                for r in rows:
                    briefing["convergence_signals"].append({
                        "ticker": r[0],
                        "source_count": int(r[1]),
                        "sources": r[2] if isinstance(r[2], list) else [],
                        "consensus_direction": r[3],
                        "confidence_raw": round(float(r[4]), 3) if r[4] else None,
                        "confidence_trust_weighted": None,
                        "avg_source_trust": None,
                        "total_signals": int(r[5]),
                        "invalidation": f"Drops below {r[1]-1} confirming sources",
                        "confidence_label": "derived",
                    })
            except Exception as exc:
                log.debug("Intel briefing: unweighted convergence fallback also failed: {e}", e=str(exc))

        # ── CROSS-REFERENCE RED FLAGS ────────────────────────────
        try:
            rows = conn.execute(text(
                "SELECT name, category, official_source, official_value, "
                "physical_source, physical_value, divergence_zscore, "
                "assessment, implication, confidence, checked_at "
                "FROM cross_reference_checks "
                "WHERE assessment IN ('suspicious', 'divergent', 'lying', 'WARNING', 'CRITICAL') "
                "ORDER BY ABS(divergence_zscore) DESC NULLS LAST "
                "LIMIT 10"
            )).fetchall()
            for r in rows:
                briefing["cross_reference_flags"].append({
                    "indicator": r[0],
                    "category": r[1],
                    "official_source": r[2],
                    "official_value": float(r[3]) if r[3] else None,
                    "physical_source": r[4],
                    "physical_value": float(r[5]) if r[5] else None,
                    "divergence_zscore": float(r[6]) if r[6] else None,
                    "assessment": r[7],
                    "implication": r[8],
                    "invalidation": "Flag clears when z-score drops below 2.0",
                    "confidence_label": "confirmed",
                })
        except Exception as exc:
            log.debug("Intel briefing: cross-reference red flags query failed: {e}", e=str(exc))

        # ── OPTIONS MISPRICING ───────────────────────────────────
        try:
            rows = conn.execute(text(
                "SELECT ticker, score, payoff_multiple, direction, "
                "thesis, expiry, spot_price, iv_atm, confidence, "
                "is_100x, created_at "
                "FROM options_mispricing_scans "
                "ORDER BY score DESC NULLS LAST "
                "LIMIT 10"
            )).fetchall()
            for r in rows:
                expiry_str = _safe_isoformat(r[5])
                spot = float(r[6]) if r[6] else None
                briefing["options_mispricing"].append({
                    "ticker": r[0],
                    "score": float(r[1]) if r[1] else None,
                    "payoff_multiple": float(r[2]) if r[2] else None,
                    "direction": r[3],
                    "thesis": r[4],
                    "expiry": expiry_str,
                    "spot_price": spot,
                    "iv_atm": float(r[7]) if r[7] else None,
                    "confidence": float(r[8]) if r[8] else None,
                    "is_100x": r[9],
                    "scanned_at": _safe_isoformat(r[10]),
                    "invalidation": (
                        f"Score drops below 4.0 or IV normalizes above 50th pct"
                        + (f" or {r[0]} moves >5% against {r[3]} before {expiry_str}" if expiry_str else "")
                    ),
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("Intel briefing: options mispricing query failed: {e}", e=str(exc))

        # ── ACTIVE PREDICTIONS (with inline track record) ────────
        # Fixed N+1: was firing one track-record SELECT per prediction row.
        # Now: one query for active predictions + one aggregated query for all
        # track records keyed by (model_name, ticker), then merge in Python.
        try:
            rows = conn.execute(text(
                "SELECT id, ticker, model_name, direction, confidence, "
                "target_price, entry_price, expiry, created_at "
                "FROM oracle_predictions "
                "WHERE verdict = 'pending' AND expiry > :today "
                "ORDER BY confidence DESC NULLS LAST "
                "LIMIT 10"
            ), {"today": today}).fetchall()

            if rows:
                # Collect unique (model_name, ticker) pairs from the active preds
                model_ticker_pairs = list({(r[2], r[1]) for r in rows})
                models_list = [p[0] for p in model_ticker_pairs]
                tickers_list = [p[1] for p in model_ticker_pairs]

                # Single aggregated track-record query — replaces N individual queries
                try:
                    track_rows = conn.execute(text(
                        "SELECT model_name, ticker, "
                        "COUNT(*) AS total, "
                        "COUNT(*) FILTER (WHERE verdict = 'hit') AS hits, "
                        "AVG(pnl_pct) AS avg_pnl "
                        "FROM oracle_predictions "
                        "WHERE verdict IN ('hit','miss','partial') "
                        "AND (model_name, ticker) IN "
                        "  (SELECT unnest(CAST(:models AS text[])), "
                        "          unnest(CAST(:tickers AS text[]))) "
                        "GROUP BY model_name, ticker"
                    ), {"models": models_list, "tickers": tickers_list}).fetchall()
                    track_map: dict[tuple[str, str], dict] = {}
                    for tr in track_rows:
                        if tr[2] > 0:
                            track_map[(tr[0], tr[1])] = {
                                "total_scored": int(tr[2]),
                                "hit_rate": round(int(tr[3]) / int(tr[2]), 3),
                                "avg_pnl_pct": round(float(tr[4]), 2) if tr[4] else None,
                            }
                except Exception:
                    track_map = {}

                for r in rows:
                    pred = {
                        "id": r[0],
                        "ticker": r[1],
                        "model": r[2],
                        "direction": r[3],
                        "confidence": float(r[4]) if r[4] else None,
                        "target_price": float(r[5]) if r[5] else None,
                        "entry_price": float(r[6]) if r[6] else None,
                        "expiry": _safe_isoformat(r[7]),
                        "created_at": _safe_isoformat(r[8]),
                        "confidence_label": "derived",
                    }
                    tr = track_map.get((r[2], r[1]))
                    if tr:
                        pred["track_record"] = tr
                    briefing["predictions_active"].append(pred)
        except Exception as exc:
            log.debug("Intel briefing: active predictions query failed: {e}", e=str(exc))

        # ── OVERALL TRACK RECORD ─────────────────────────────────
        try:
            row = conn.execute(text(
                "SELECT COUNT(*) as total, "
                "COUNT(*) FILTER (WHERE verdict = 'hit') as hits, "
                "COUNT(*) FILTER (WHERE verdict = 'miss') as misses, "
                "COUNT(*) FILTER (WHERE verdict = 'partial') as partials, "
                "AVG(pnl_pct) as avg_pnl "
                "FROM oracle_predictions "
                "WHERE verdict IN ('hit','miss','partial')"
            )).fetchone()
            if row and row[0] > 0:
                briefing["track_record"] = {
                    "total_scored": int(row[0]),
                    "hits": int(row[1]),
                    "misses": int(row[2]),
                    "partials": int(row[3]),
                    "hit_rate": round(int(row[1]) / int(row[0]), 3),
                    "avg_pnl_pct": round(float(row[4]), 2) if row[4] else None,
                    "confidence_label": "confirmed",
                }
        except Exception as exc:
            log.debug("Intel briefing: overall track record query failed: {e}", e=str(exc))

        # ── NOTABLE FLOWS (enriched with actor trust scores) ─────
        try:
            rows = conn.execute(text(
                "SELECT wf.flow_date, wf.from_actor, wf.to_entity, "
                "  wf.amount_estimate, wf.confidence, wf.evidence, "
                "  wf.implication, ss.trust_score "
                "FROM wealth_flows wf "
                "LEFT JOIN signal_sources ss "
                "  ON wf.from_actor = ss.source_id "
                "WHERE wf.flow_date >= :cutoff "
                "ORDER BY wf.amount_estimate DESC NULLS LAST "
                "LIMIT 10"
            ), {"cutoff": flow_cutoff}).fetchall()
            for r in rows:
                briefing["notable_flows"].append({
                    "date": _safe_isoformat(r[0]),
                    "from": r[1],
                    "to": r[2],
                    "amount_usd": float(r[3]) if r[3] else None,
                    "evidence": _safe_json(r[5]),
                    "implication": r[6],
                    "actor_trust_score": round(float(r[7]), 3) if r[7] else None,
                    "confidence_label": r[4] or "estimated",
                })
        except Exception:
            # Fallback without trust join
            try:
                rows = conn.execute(text(
                    "SELECT flow_date, from_actor, to_entity, amount_estimate, "
                    "confidence, evidence, implication "
                    "FROM wealth_flows "
                    "WHERE flow_date >= :cutoff "
                    "ORDER BY amount_estimate DESC NULLS LAST "
                    "LIMIT 10"
                ), {"cutoff": flow_cutoff}).fetchall()
                for r in rows:
                    briefing["notable_flows"].append({
                        "date": _safe_isoformat(r[0]),
                        "from": r[1],
                        "to": r[2],
                        "amount_usd": float(r[3]) if r[3] else None,
                        "evidence": _safe_json(r[5]),
                        "implication": r[6],
                        "actor_trust_score": None,
                        "confidence_label": r[4] or "estimated",
                    })
            except Exception as exc:
                log.debug("Intel briefing: notable flows unjoined fallback failed: {e}", e=str(exc))

    # Count non-empty sections for meta
    skip_keys = ("as_of", "delta_since")
    sections_populated = sum(
        1 for k, v in briefing.items()
        if k not in skip_keys and v  # has data
    )

    return _ok(
        briefing,
        meta={
            "sections_populated": sections_populated,
            "sections_total": 10,
            "mode": "delta" if delta_cutoff else "full",
        },
        tier_required=Tier.BASIC,
        query_start=t0,
    )
