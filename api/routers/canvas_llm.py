"""Canvas sub-router: LLM-powered intelligence features."""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["canvas"])


# ── Request / Response schemas ─────────────────────────────────────────


class ExplainRequest(BaseModel):
    source_node_id: str
    target_node_id: str
    board_id: str


class ExplainResponse(BaseModel):
    explanation: str
    confidence: str  # confirmed / derived / estimated
    key_facts: list[str]
    lever: str | None = None
    source_label: str
    target_label: str


# ── Helpers ────────────────────────────────────────────────────────────


def _get_node(conn: Connection, board_id: str, node_id: str) -> dict | None:
    """Fetch a canvas node by board_id and node_id."""
    row = conn.execute(
        text(
            "SELECT id, node_type, label, data "
            "FROM canvas_nodes "
            "WHERE board_id = :board_id AND node_id = :node_id"
        ),
        {"board_id": board_id, "node_id": node_id},
    ).mappings().first()

    if not row:
        return None

    result = dict(row)
    if isinstance(result.get("data"), str):
        try:
            result["data"] = json.loads(result["data"])
        except (json.JSONDecodeError, TypeError):
            result["data"] = {}
    elif result.get("data") is None:
        result["data"] = {}

    return result


def _gather_context(conn: Connection, source: dict, target: dict) -> dict:
    """Gather intelligence context for the two nodes."""
    context: dict[str, Any] = {
        "signals": [],
        "connections": [],
        "wealth_flows": [],
        "shared_tickers": [],
    }

    s_data = source.get("data") or {}
    t_data = target.get("data") or {}
    s_entity = s_data.get("entityId") or s_data.get("entity_id") or source.get("id")
    t_entity = t_data.get("entityId") or t_data.get("entity_id") or target.get("id")

    # Direct actor connections
    if source.get("node_type") == "actor" and target.get("node_type") == "actor":
        rows = conn.execute(
            text(
                "SELECT relationship, strength, metadata "
                "FROM actor_connections "
                "WHERE (from_actor_id = :s AND to_actor_id = :t) "
                "   OR (from_actor_id = :t AND to_actor_id = :s) "
                "LIMIT 10"
            ),
            {"s": s_entity, "t": t_entity},
        ).mappings().all()
        context["connections"] = [dict(r) for r in rows]

    # Shared signals
    s_name = source.get("label", "")
    t_name = target.get("label", "")

    if s_name and t_name:
        rows = conn.execute(
            text(
                "SELECT signal_type, ticker, description, signal_date, direction, confidence "
                "FROM signal_data "
                "WHERE (actor ILIKE :s_name OR description ILIKE :s_pattern) "
                "  AND (actor ILIKE :t_name OR description ILIKE :t_pattern) "
                "ORDER BY signal_date DESC LIMIT 10"
            ),
            {
                "s_name": s_name,
                "s_pattern": f"%{s_name}%",
                "t_name": t_name,
                "t_pattern": f"%{t_name}%",
            },
        ).mappings().all()
        context["signals"] = [_serialize_row(r) for r in rows]

    # Wealth flows
    if s_name and t_name:
        rows = conn.execute(
            text(
                "SELECT from_actor, to_entity, amount_estimate, confidence, flow_date "
                "FROM wealth_flows "
                "WHERE (from_actor ILIKE :s AND to_entity ILIKE :t) "
                "   OR (from_actor ILIKE :t AND to_entity ILIKE :s) "
                "ORDER BY flow_date DESC LIMIT 5"
            ),
            {"s": f"%{s_name}%", "t": f"%{t_name}%"},
        ).mappings().all()
        context["wealth_flows"] = [_serialize_row(r) for r in rows]

    return context


def _serialize_row(row: Any) -> dict:
    """Convert a mapping-proxy row to a JSON-safe dict."""
    d = dict(row)
    for key, val in d.items():
        if isinstance(val, datetime):
            d[key] = val.isoformat()
    return d


def _build_prompt(source: dict, target: dict, context: dict) -> str:
    """Build the LLM prompt from source/target nodes and gathered context."""
    connections_str = json.dumps(
        context.get("connections", []), indent=2, default=str
    )[:2000]
    signals_str = json.dumps(
        context.get("signals", []), indent=2, default=str
    )[:2000]
    flows_str = json.dumps(
        context.get("wealth_flows", []), indent=2, default=str
    )[:1000]

    return f"""Analyze the connection between two entities in a financial intelligence investigation.

SOURCE: {source.get('label', 'Unknown')} (type: {source.get('node_type', 'unknown')})
TARGET: {target.get('label', 'Unknown')} (type: {target.get('node_type', 'unknown')})

KNOWN CONNECTIONS:
{connections_str}

SHARED SIGNALS:
{signals_str}

WEALTH FLOWS:
{flows_str}

Provide:
1. A 2-3 sentence explanation of how these entities are connected and why it matters for market analysis
2. Confidence level: "confirmed" if direct evidence exists, "derived" if inferred from patterns, "estimated" if speculative
3. 2-4 key facts supporting the connection
4. If applicable: identify the LEVER (who is pulling what financial lever affecting whom)

Respond in JSON format:
{{"explanation": "...", "confidence": "confirmed|derived|estimated", "key_facts": ["...", "..."], "lever": "..." or null}}"""


SYSTEM_PROMPT = (
    "You are a financial intelligence analyst. Be precise and factual. "
    "Always cite specific evidence. Respond only in valid JSON."
)


def _call_llm(source: dict, target: dict, context: dict) -> ExplainResponse:
    """Call REASON-tier LLM to explain the connection."""
    source_label = source.get("label", "")
    target_label = target.get("label", "")
    n_connections = len(context.get("connections", []))
    n_signals = len(context.get("signals", []))

    try:
        from llm.router import Tier, get_llm

        client = get_llm(Tier.REASON)
    except Exception:
        return ExplainResponse(
            explanation=(
                f"LLM unavailable. Based on available data, these entities share "
                f"{n_connections} direct connections and {n_signals} shared signals."
            ),
            confidence="estimated",
            key_facts=[f"{n_connections} direct connections found"],
            source_label=source_label,
            target_label=target_label,
        )

    prompt = _build_prompt(source, target, context)

    try:
        response = client.generate(prompt, system=SYSTEM_PROMPT)

        if response:
            json_match = re.search(r"\{.*\}", response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                return ExplainResponse(
                    explanation=data.get("explanation", "No explanation generated"),
                    confidence=data.get("confidence", "estimated"),
                    key_facts=data.get("key_facts", []),
                    lever=data.get("lever"),
                    source_label=source_label,
                    target_label=target_label,
                )
    except Exception as e:
        log.warning("LLM explain failed: {err}", err=str(e))

    # Fallback when LLM produces no parseable output
    return ExplainResponse(
        explanation=(
            f"Connection between {source_label} and {target_label} — "
            f"{n_connections} direct links, {n_signals} shared signals."
        ),
        confidence="estimated",
        key_facts=[],
        source_label=source_label,
        target_label=target_label,
    )


# ── Endpoint ───────────────────────────────────────────────────────────


@router.post("/explain", response_model=ExplainResponse)
async def explain_connection(
    req: ExplainRequest,
    engine=Depends(get_db_engine),
    _=Depends(require_auth),
):
    """Use local LLM to explain the connection between two canvas nodes."""
    with engine.connect() as conn:
        source = _get_node(conn, req.board_id, req.source_node_id)
        target = _get_node(conn, req.board_id, req.target_node_id)

        if not source or not target:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="One or both nodes not found on this board",
            )

        context = _gather_context(conn, source, target)

    explanation = _call_llm(source, target, context)

    log.info(
        "Canvas explain: {src} <-> {tgt} confidence={c}",
        src=source.get("label", ""),
        tgt=target.get("label", ""),
        c=explanation.confidence,
    )

    return explanation
