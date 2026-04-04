"""
GRID Intelligence MCP Server — Model Context Protocol interface.

Lets any MCP-compatible LLM (Claude, GPT, Gemini) query GRID's intelligence
data directly: briefings, actor dossiers, entity profiles, ticker analysis,
predictions, and distributed compute task management.

Installation:
    pip install fastmcp

Run standalone:
    python mcp_server.py

Claude Code MCP config (~/.claude/mcp.json):
    {
      "mcpServers": {
        "grid-intelligence": {
          "command": "python",
          "args": ["/path/to/grid/mcp_server.py"],
          "env": {
            "DB_HOST": "your_db_host",
            "DB_PORT": "5432",
            "DB_NAME": "grid",
            "DB_USER": "grid_user",
            "DB_PASSWORD": "your_password"
          }
        }
      }
    }

Claude Desktop config:
    Add the same block under "mcpServers" in claude_desktop_config.json.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastmcp import FastMCP
from loguru import logger as log

# Ensure GRID root is on the path so we can import db/config
_GRID_DIR = os.path.dirname(os.path.abspath(__file__))
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

mcp = FastMCP(
    "GRID Intelligence",
    description=(
        "Query GRID's trading intelligence platform: market briefings, "
        "actor dossiers, entity profiles, ticker analysis, predictions, "
        "and distributed compute task management."
    ),
)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def _get_engine():
    """Return the SQLAlchemy engine from GRID's db module."""
    from db import get_engine
    return get_engine()


def _safe_json(val: Any) -> Any:
    """Parse a JSON string or return as-is."""
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, ValueError):
            return val
    return val


def _iso(val: Any) -> str | None:
    """Convert date/datetime to ISO string or None."""
    if val is None:
        return None
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    return str(val)


def _safe_float(val: Any) -> float | None:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 1. grid_briefing — current intelligence briefing
# ---------------------------------------------------------------------------

@mcp.tool()
def grid_briefing() -> dict[str, Any]:
    """Return the current GRID intelligence briefing.

    Includes: market regime, top signals (last 7 days), active predictions,
    notable wealth flows, and summary statistics. All data points carry
    confidence labels (confirmed/derived/estimated/rumored/inferred).
    """
    from sqlalchemy import text

    engine = _get_engine()
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
                        "label": (
                            payload.get("regime_label")
                            or payload.get("regime")
                            or payload.get("current_regime")
                        ),
                        "updated_at": _iso(row[1]),
                        "confidence_label": "derived",
                    }
        except Exception as exc:
            log.debug("MCP briefing: regime query failed: {e}", e=str(exc))

        # Top signals (last 7 days, highest confidence)
        try:
            rows = conn.execute(
                text(
                    "SELECT ticker, signal_date, source_type, signal_type, "
                    "direction, magnitude, confidence, source_name "
                    "FROM signal_data "
                    "WHERE signal_date >= :cutoff "
                    "ORDER BY confidence DESC NULLS LAST, "
                    "magnitude DESC NULLS LAST "
                    "LIMIT 15"
                ),
                {"cutoff": week_ago},
            ).fetchall()
            for r in rows:
                brief["top_signals"].append({
                    "ticker": r[0],
                    "date": _iso(r[1]),
                    "source_type": r[2],
                    "signal_type": r[3],
                    "direction": r[4],
                    "magnitude": _safe_float(r[5]),
                    "confidence": _safe_float(r[6]),
                    "source": r[7],
                    "confidence_label": "confirmed",
                })
        except Exception as exc:
            log.debug("MCP briefing: top signals query failed: {e}", e=str(exc))

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
                    "confidence": _safe_float(r[4]),
                    "target_price": _safe_float(r[5]),
                    "entry_price": _safe_float(r[6]),
                    "expiry": _iso(r[7]),
                    "created_at": _iso(r[8]),
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("MCP briefing: active predictions query failed: {e}", e=str(exc))

        # Notable wealth flows (last 7 days)
        try:
            rows = conn.execute(
                text(
                    "SELECT flow_date, from_actor, to_entity, amount_estimate, "
                    "confidence, implication "
                    "FROM wealth_flows "
                    "WHERE flow_date >= :cutoff "
                    "ORDER BY amount_estimate DESC NULLS LAST "
                    "LIMIT 10"
                ),
                {"cutoff": week_ago},
            ).fetchall()
            for r in rows:
                brief["notable_flows"].append({
                    "date": _iso(r[0]),
                    "from": r[1],
                    "to": r[2],
                    "amount_estimate": _safe_float(r[3]),
                    "confidence": r[4] or "estimated",
                    "implication": r[5],
                })
        except Exception as exc:
            log.debug("MCP briefing: notable flows query failed: {e}", e=str(exc))

        # Summary stats
        try:
            row = conn.execute(
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
            if row:
                brief["summary_stats"] = {
                    "active_predictions": row[0],
                    "signals_7d": row[1],
                    "tickers_active": row[2],
                    "confidence_label": "confirmed",
                }
        except Exception as exc:
            log.debug("MCP briefing: summary stats query failed: {e}", e=str(exc))

    return brief


# ---------------------------------------------------------------------------
# 2. grid_search — cross-domain search
# ---------------------------------------------------------------------------

@mcp.tool()
def grid_search(query: str) -> dict[str, Any]:
    """Search across GRID's actors, entities (offshore/ICIJ), and tickers.

    Returns matched results with type labels and confidence ratings.
    Use this for broad lookups when you're not sure which domain to query.
    """
    from sqlalchemy import text

    engine = _get_engine()
    results: list[dict[str, Any]] = []
    query_upper = query.upper()
    query_like = f"%{query}%"

    with engine.connect() as conn:
        # Actors
        try:
            rows = conn.execute(
                text(
                    "SELECT id, name, tier, category, aum, "
                    "trust_score, credibility "
                    "FROM actors "
                    "WHERE UPPER(name) LIKE UPPER(:q) "
                    "   OR UPPER(id) LIKE UPPER(:q) "
                    "ORDER BY aum DESC NULLS LAST "
                    "LIMIT 25"
                ),
                {"q": query_like},
            ).fetchall()
            for r in rows:
                results.append({
                    "match_type": "actor",
                    "id": r[0],
                    "name": r[1],
                    "tier": r[2],
                    "category": r[3],
                    "aum": r[4],
                    "trust_score": r[5],
                    "confidence": r[6] or "derived",
                })
        except Exception as exc:
            log.debug("MCP search: actors query failed: {e}", e=str(exc))

        # Entities (ICIJ offshore)
        try:
            rows = conn.execute(
                text(
                    "SELECT DISTINCT entity_name, jurisdiction, source_dataset, "
                    "linked_to, relationship_type "
                    "FROM icij_relationships "
                    "WHERE UPPER(entity_name) LIKE UPPER(:q) "
                    "   OR UPPER(linked_to) LIKE UPPER(:q) "
                    "ORDER BY entity_name "
                    "LIMIT 25"
                ),
                {"q": query_like},
            ).fetchall()
            for r in rows:
                results.append({
                    "match_type": "entity",
                    "name": r[0],
                    "jurisdiction": r[1],
                    "source_dataset": r[2],
                    "linked_to": r[3],
                    "relationship_type": r[4],
                    "confidence": "confirmed",
                })
        except Exception as exc:
            log.debug("MCP search: entities query failed: {e}", e=str(exc))

        # Tickers
        try:
            rows = conn.execute(
                text(
                    "SELECT DISTINCT ticker, model_name, direction, confidence, "
                    "verdict, created_at "
                    "FROM oracle_predictions "
                    "WHERE UPPER(ticker) = :q "
                    "ORDER BY created_at DESC "
                    "LIMIT 25"
                ),
                {"q": query_upper},
            ).fetchall()
            for r in rows:
                results.append({
                    "match_type": "ticker",
                    "ticker": r[0],
                    "model": r[1],
                    "direction": r[2],
                    "confidence": _safe_float(r[3]),
                    "verdict": r[4],
                    "created_at": _iso(r[5]),
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("MCP search: tickers query failed: {e}", e=str(exc))

    return {
        "query": query,
        "count": len(results),
        "results": results,
    }


# ---------------------------------------------------------------------------
# 3. grid_ticker — everything about a ticker
# ---------------------------------------------------------------------------

@mcp.tool()
def grid_ticker(symbol: str) -> dict[str, Any]:
    """Everything GRID knows about a ticker symbol.

    Returns: active predictions, signals, actor exposure, insider activity,
    dark pool data, options flow, dealer gamma positioning, and regime
    classification. All data points carry confidence labels.
    """
    from sqlalchemy import text

    engine = _get_engine()
    ticker = symbol.upper()
    cutoff = date.today() - timedelta(days=30)

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
        # Predictions
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
                    "target_price": _safe_float(r[3]),
                    "entry_price": _safe_float(r[4]),
                    "confidence": _safe_float(r[5]),
                    "expected_move_pct": _safe_float(r[6]),
                    "expiry": _iso(r[7]),
                    "verdict": r[8],
                    "created_at": _iso(r[9]),
                    "signals": _safe_json(r[10]),
                    "anti_signals": _safe_json(r[11]),
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("MCP ticker: predictions query failed for {t}: {e}", t=ticker, e=str(exc))

        # Signals
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
                    "date": _iso(r[0]),
                    "source_type": r[1],
                    "signal_type": r[2],
                    "direction": r[3],
                    "magnitude": _safe_float(r[4]),
                    "raw_value": r[5],
                    "confidence": _safe_float(r[6]),
                    "source_name": r[7],
                    "confidence_label": "confirmed",
                })
        except Exception as exc:
            log.debug("MCP ticker: signals query failed for {t}: {e}", t=ticker, e=str(exc))

        # Actor exposure
        try:
            rows = conn.execute(
                text(
                    "SELECT id, name, tier, category, aum, known_positions "
                    "FROM actors "
                    "WHERE known_positions::text ILIKE :q "
                    "   OR connections::text ILIKE :q"
                ),
                {"q": f"%{ticker}%"},
            ).fetchall()
            for r in rows:
                positions = _safe_json(r[5])
                ticker_position = None
                if isinstance(positions, dict):
                    ticker_position = positions.get(ticker)
                elif isinstance(positions, list):
                    ticker_position = [
                        p for p in positions
                        if isinstance(p, dict) and p.get("ticker") == ticker
                    ]
                result["actor_exposure"].append({
                    "id": r[0],
                    "name": r[1],
                    "tier": r[2],
                    "category": r[3],
                    "aum": r[4],
                    "position": ticker_position,
                    "confidence": "derived",
                })
        except Exception as exc:
            log.debug("MCP ticker: actor exposure query failed for {t}: {e}", t=ticker, e=str(exc))

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
                    "date": _iso(r[0]),
                    "insider": r[1],
                    "direction": r[2],
                    "magnitude": _safe_float(r[3]),
                    "value": r[4],
                    "confidence": _safe_float(r[5]),
                    "confidence_label": "confirmed",
                })
        except Exception as exc:
            log.debug("MCP ticker: insider activity query failed for {t}: {e}", t=ticker, e=str(exc))

        # Dark pool
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
                    "date": _iso(r[0]),
                    "magnitude": _safe_float(r[1]),
                    "value": r[2],
                    "confidence": _safe_float(r[3]),
                    "confidence_label": "confirmed",
                })
        except Exception as exc:
            log.debug("MCP ticker: dark pool query failed for {t}: {e}", t=ticker, e=str(exc))

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
                    "date": _iso(r[0]),
                    "signal_type": r[1],
                    "direction": r[2],
                    "magnitude": _safe_float(r[3]),
                    "value": r[4],
                    "confidence": _safe_float(r[5]),
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("MCP ticker: options flow query failed for {t}: {e}", t=ticker, e=str(exc))

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
                    "date": _iso(row[0]),
                    "value": _safe_json(row[1]),
                    "confidence": _safe_float(row[2]),
                    "confidence_label": "derived",
                }
        except Exception as exc:
            log.debug("MCP ticker: dealer gamma query failed for {t}: {e}", t=ticker, e=str(exc))

        # Regime
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
                    result["regime"] = {
                        "label": (
                            payload.get("regime_label")
                            or payload.get("regime")
                            or payload.get("current_regime")
                        ),
                        "snapshot_at": _iso(row[1]),
                        "confidence_label": "derived",
                    }
        except Exception as exc:
            log.debug("MCP ticker: regime query failed for {t}: {e}", t=ticker, e=str(exc))

    return result


# ---------------------------------------------------------------------------
# 4. grid_actor — actor dossier
# ---------------------------------------------------------------------------

@mcp.tool()
def grid_actor(name: str) -> dict[str, Any]:
    """Full actor dossier: identity, what they control, wealth flows,
    trust score history, connected entities, and sector influence.

    Actors are the lever-pullers who move markets: fund managers, politicians,
    central bankers, corporate insiders, etc. All data carries confidence labels.
    """
    from sqlalchemy import text

    engine = _get_engine()
    dossier: dict[str, Any] = {
        "name": name,
        "identity": None,
        "wealth_flows": [],
        "trust_history": [],
        "connected_entities": [],
        "confidence": "derived",
    }

    with engine.connect() as conn:
        # Identity
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
                    "id": row[0],
                    "name": row[1],
                    "tier": row[2],
                    "category": row[3],
                    "aum": row[4],
                    "trust_score": row[5],
                    "motivation_model": row[6],
                    "connections": _safe_json(row[7]),
                    "credibility": row[8] or "derived",
                    "known_positions": _safe_json(row[9]),
                }
        except Exception as exc:
            log.debug("MCP actor: identity query failed for {n}: {e}", n=name, e=str(exc))

        # Wealth flows
        try:
            rows = conn.execute(
                text(
                    "SELECT flow_date, from_actor, to_entity, amount_estimate, "
                    "confidence, implication "
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
                    "date": _iso(r[0]),
                    "from": r[1],
                    "to": r[2],
                    "amount_estimate": _safe_float(r[3]),
                    "confidence": r[4] or "estimated",
                    "implication": r[5],
                })
        except Exception as exc:
            log.debug("MCP actor: wealth flows query failed for {n}: {e}", n=name, e=str(exc))

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
                    "scored_at": _iso(r[0]),
                    "source_type": r[1],
                    "trust_score": r[2],
                    "hits": r[3],
                    "misses": r[4],
                    "total_signals": r[5],
                    "recency_weighted": r[6],
                    "confidence": "confirmed",
                })
        except Exception as exc:
            log.debug("MCP actor: trust history query failed for {n}: {e}", n=name, e=str(exc))

        # Connected entities (ICIJ)
        try:
            rows = conn.execute(
                text(
                    "SELECT entity_name, linked_to, relationship_type, "
                    "jurisdiction, source_dataset "
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
            log.debug("MCP actor: connected entities query failed for {n}: {e}", n=name, e=str(exc))

    if dossier["identity"] is None and not dossier["wealth_flows"]:
        return {"name": name, "error": "No actor data found", "confidence": "n/a"}

    return dossier


# ---------------------------------------------------------------------------
# 5. grid_entity — entity profile (offshore connections)
# ---------------------------------------------------------------------------

@mcp.tool()
def grid_entity(name: str) -> dict[str, Any]:
    """Entity profile: offshore connections, jurisdictions, red flags,
    and connected actors.

    Entities come from ICIJ Panama/Pandora Papers and GRID's actor network.
    Red flags are auto-detected (multi-jurisdiction, intermediary chains).
    All data carries confidence labels.
    """
    from sqlalchemy import text

    engine = _get_engine()
    entity: dict[str, Any] = {
        "name": name,
        "offshore_connections": [],
        "connected_actors": [],
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
            log.debug("MCP entity: offshore connections query failed for {n}: {e}", n=name, e=str(exc))

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
                    "id": r[0],
                    "name": r[1],
                    "tier": r[2],
                    "category": r[3],
                    "aum": r[4],
                    "trust_score": r[5],
                    "confidence": "derived",
                })
        except Exception as exc:
            log.debug("MCP entity: connected actors query failed for {n}: {e}", n=name, e=str(exc))

        # Red flags
        offshore = entity["offshore_connections"]
        jurisdictions = {c["jurisdiction"] for c in offshore if c.get("jurisdiction")}
        if len(jurisdictions) >= 3:
            entity["red_flags"].append({
                "flag": "multi_jurisdiction",
                "detail": (
                    f"Entity appears in {len(jurisdictions)} jurisdictions: "
                    f"{', '.join(sorted(jurisdictions))}"
                ),
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
        return {"name": name, "error": "No entity data found", "confidence": "n/a"}

    return entity


# ---------------------------------------------------------------------------
# 6. grid_predictions — active predictions with track record
# ---------------------------------------------------------------------------

@mcp.tool()
def grid_predictions() -> dict[str, Any]:
    """Active predictions with track record.

    Returns all pending predictions (not yet expired) plus historical
    accuracy statistics. Each prediction includes ticker, direction,
    confidence, target/entry prices, and expiry. Confidence labels on all data.
    """
    from sqlalchemy import text

    engine = _get_engine()
    today = date.today()

    output: dict[str, Any] = {
        "active": [],
        "track_record": {},
    }

    with engine.connect() as conn:
        # Active predictions
        try:
            rows = conn.execute(
                text(
                    "SELECT id, ticker, model_name, direction, confidence, "
                    "target_price, entry_price, expected_move_pct, expiry, "
                    "created_at, signals, anti_signals "
                    "FROM oracle_predictions "
                    "WHERE verdict = 'pending' AND expiry > :today "
                    "ORDER BY confidence DESC NULLS LAST "
                    "LIMIT 50"
                ),
                {"today": today},
            ).fetchall()
            for r in rows:
                output["active"].append({
                    "id": r[0],
                    "ticker": r[1],
                    "model": r[2],
                    "direction": r[3],
                    "confidence": _safe_float(r[4]),
                    "target_price": _safe_float(r[5]),
                    "entry_price": _safe_float(r[6]),
                    "expected_move_pct": _safe_float(r[7]),
                    "expiry": _iso(r[8]),
                    "created_at": _iso(r[9]),
                    "signals": _safe_json(r[10]),
                    "anti_signals": _safe_json(r[11]),
                    "confidence_label": "derived",
                })
        except Exception as exc:
            log.debug("MCP predictions: active predictions query failed: {e}", e=str(exc))

        # Track record
        try:
            row = conn.execute(
                text(
                    "SELECT "
                    "  COUNT(*) FILTER (WHERE verdict = 'hit') AS hits, "
                    "  COUNT(*) FILTER (WHERE verdict = 'miss') AS misses, "
                    "  COUNT(*) FILTER (WHERE verdict = 'partial') AS partials, "
                    "  COUNT(*) FILTER (WHERE verdict = 'pending') AS pending, "
                    "  COUNT(*) AS total, "
                    "  ROUND(AVG(confidence)::numeric, 3) AS avg_confidence "
                    "FROM oracle_predictions"
                ),
            ).fetchone()
            if row:
                hits = row[0] or 0
                misses = row[1] or 0
                partials = row[2] or 0
                total_scored = hits + misses + partials
                output["track_record"] = {
                    "hits": hits,
                    "misses": misses,
                    "partials": partials,
                    "pending": row[3] or 0,
                    "total": row[4] or 0,
                    "hit_rate": (
                        round(hits / total_scored, 3)
                        if total_scored > 0
                        else None
                    ),
                    "avg_confidence": float(row[5]) if row[5] else None,
                    "confidence_label": "confirmed",
                }
        except Exception as exc:
            log.debug("MCP predictions: track record query failed: {e}", e=str(exc))

    return output


# ---------------------------------------------------------------------------
# 7. grid_submit_research — submit research back (BOINC miners)
# ---------------------------------------------------------------------------

@mcp.tool()
def grid_submit_research(task_type: str, response: str) -> dict[str, Any]:
    """Submit a completed research response back to GRID.

    Used by BOINC-style distributed compute miners to return results.
    The task_type should match what was received from grid_pull_task().
    The response should be the completed research text.

    Returns confirmation with task acceptance status.
    """
    from sqlalchemy import text

    engine = _get_engine()

    try:
        with engine.begin() as conn:
            # Store the research result in analytical_snapshots
            conn.execute(
                text(
                    "INSERT INTO analytical_snapshots (category, payload, created_at) "
                    "VALUES (:cat, :payload, NOW())"
                ),
                {
                    "cat": f"mcp_research_{task_type}",
                    "payload": json.dumps({
                        "task_type": task_type,
                        "response": response,
                        "source": "mcp_submission",
                        "confidence_label": "estimated",
                    }),
                },
            )

        return {
            "status": "accepted",
            "task_type": task_type,
            "response_length": len(response),
            "confidence_label": "confirmed",
        }
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc),
            "confidence_label": "n/a",
        }


# ---------------------------------------------------------------------------
# 8. grid_pull_task — pull a research task from the backlog
# ---------------------------------------------------------------------------

@mcp.tool()
def grid_pull_task() -> dict[str, Any]:
    """Pull the next available research task from GRID's backlog.

    Used by BOINC-style distributed compute miners. Returns a task with
    task_type, prompt, and context. Returns empty if no tasks available.

    Task types include: actor_research, company_analysis,
    offshore_leak_investigation, panama_papers_research, sp500_profile,
    feature_interpretation, hypothesis_generation, and more.
    """
    from sqlalchemy import text

    engine = _get_engine()

    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "UPDATE llm_task_backlog SET status = 'processing' "
                    "WHERE id = ("
                    "  SELECT id FROM llm_task_backlog "
                    "  WHERE status = 'pending' "
                    "  ORDER BY priority ASC, created_at ASC "
                    "  LIMIT 1 "
                    "  FOR UPDATE SKIP LOCKED"
                    ") "
                    "RETURNING id, task_type, prompt, context"
                ),
            ).fetchone()

            if not row:
                return {
                    "status": "empty",
                    "message": "No pending tasks in backlog",
                }

            return {
                "status": "assigned",
                "task_id": row[0],
                "task_type": row[1],
                "prompt": row[2],
                "context": _safe_json(row[3]),
                "confidence_label": "confirmed",
            }
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc),
            "confidence_label": "n/a",
        }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
