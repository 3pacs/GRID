"""Canvas sub-router: convert canvas investigation to scored prediction."""

from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, Depends, HTTPException
from loguru import logger as log
from pydantic import BaseModel
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(tags=["canvas"])


# ── Request / Response schemas ────────────────────────────────────────────


class PredictionRequest(BaseModel):
    board_id: str
    thesis_text: str
    ticker: str
    direction: str  # bullish / bearish
    timeframe_days: int = 30
    lever_node_id: str | None = None  # actor/signal node that is the lever
    condition_node_ids: list[str] = []  # nodes that are conditions
    confidence: float = 0.5  # 0-1


class PredictionResponse(BaseModel):
    hypothesis_id: str
    thesis: str
    pattern_type: str
    confidence: float
    status: str
    canvas_node_id: str  # the new HypothesisNode added to canvas


# ── Helpers ───────────────────────────────────────────────────────────────


def _parse_node_data(raw_data) -> dict:
    """Safely parse node data from DB which may be str, dict, or None."""
    if raw_data is None:
        return {}
    if isinstance(raw_data, dict):
        return raw_data
    if isinstance(raw_data, str):
        try:
            return json.loads(raw_data)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _clamp_confidence(val: float) -> float:
    """Clamp confidence to [0.0, 1.0]."""
    return max(0.0, min(1.0, val))


# ── Endpoint ──────────────────────────────────────────────────────────────


@router.post("/predict")
async def create_prediction(
    req: PredictionRequest,
    _token: str = Depends(require_auth),
) -> PredictionResponse:
    """Convert a canvas investigation into a scored hypothesis.

    Builds a LEVER -> CONDITION -> THESIS structure from the selected canvas
    nodes, inserts the hypothesis *and* its automatic antithesis into
    ``discovered_hypotheses``, and adds a HypothesisNode back to the board.
    """
    # Validate direction
    if req.direction not in ("bullish", "bearish"):
        raise HTTPException(400, "direction must be 'bullish' or 'bearish'")

    if not req.thesis_text.strip():
        raise HTTPException(400, "thesis_text cannot be empty")

    if not req.ticker.strip():
        raise HTTPException(400, "ticker cannot be empty")

    confidence = _clamp_confidence(req.confidence)
    engine = get_db_engine()

    with engine.begin() as conn:
        # 1. Gather evidence from board nodes
        nodes = conn.execute(
            text(
                "SELECT id, node_type, label, position_x, position_y, data"
                " FROM canvas_nodes WHERE board_id = :bid"
            ),
            {"bid": req.board_id},
        ).mappings().all()

        if not nodes:
            raise HTTPException(404, "Board has no nodes")

        # 2. Build structured thesis from canvas
        lever_text = ""
        conditions: list[str] = []
        evidence_nodes: list[dict] = []
        condition_ids_set = set(req.condition_node_ids)

        for n in nodes:
            node_data = _parse_node_data(n["data"])
            nid = n["id"]
            label = n["label"] or ""
            ntype = n["node_type"]

            if nid == req.lever_node_id:
                lever_text = f"LEVER: {label} ({ntype})"
            elif nid in condition_ids_set:
                conditions.append(f"CONDITION: {label}")
            else:
                evidence_nodes.append({"type": ntype, "label": label})

        # 3. Build full thesis with LEVER -> CONDITION -> THESIS structure
        structured_thesis = req.thesis_text
        if lever_text or conditions:
            parts: list[str] = []
            if lever_text:
                parts.append(lever_text)
            for c in conditions:
                parts.append(c)
            parts.append(f"THESIS: {req.thesis_text}")
            structured_thesis = "\n".join(parts)

        # 4. Build evidence JSON
        evidence = {
            "canvas_board_id": req.board_id,
            "canvas_node_count": len(nodes),
            "lever_node_id": req.lever_node_id,
            "condition_node_ids": req.condition_node_ids,
            "evidence_nodes": evidence_nodes[:20],  # cap at 20
            "created_from": "canvas",
            "ticker": req.ticker,
            "direction": req.direction,
        }

        # 5. Build test criteria
        threshold = 2.0 if req.direction == "bullish" else -2.0
        test_criteria = {
            "ticker": req.ticker,
            "direction": req.direction,
            "check_type": "price_move",
            "threshold_pct": threshold,
            "window_days": req.timeframe_days,
        }

        # 6. Build invalidation string
        anti_direction = "bearish" if req.direction == "bullish" else "bullish"
        invalidation = (
            f"Invalidated if {req.ticker} moves "
            f"{'below' if req.direction == 'bullish' else 'above'} "
            f"entry within {req.timeframe_days}d, or lever reverses."
        )

        # 7. Insert discovered_hypothesis (thesis)
        hyp_id = str(uuid.uuid4())

        conn.execute(
            text("""
                INSERT INTO discovered_hypotheses
                    (id, thesis, pattern_type, evidence, test_criteria,
                     invalidation, confidence, status, role)
                VALUES
                    (:id, :thesis, :ptype, :evidence, :criteria,
                     :inv, :conf, 'active', 'thesis')
            """),
            {
                "id": hyp_id,
                "thesis": structured_thesis,
                "ptype": "canvas_investigation",
                "evidence": json.dumps(evidence),
                "criteria": json.dumps(test_criteria),
                "inv": invalidation,
                "conf": confidence,
            },
        )

        # 8. Insert the automatic antithesis (every thesis gets an inverse)
        anti_id = str(uuid.uuid4())
        anti_thesis = f"ANTITHESIS of canvas investigation: {req.thesis_text}"
        anti_criteria = {
            "ticker": req.ticker,
            "direction": anti_direction,
            "check_type": "price_move",
            "threshold_pct": -threshold,
            "window_days": req.timeframe_days,
        }
        anti_evidence = {
            "canvas_board_id": req.board_id,
            "created_from": "canvas_antithesis",
            "ticker": req.ticker,
            "direction": anti_direction,
        }

        conn.execute(
            text("""
                INSERT INTO discovered_hypotheses
                    (id, thesis, pattern_type, evidence, test_criteria,
                     invalidation, confidence, status, role, pair_id)
                VALUES
                    (:id, :thesis, :ptype, :evidence, :criteria,
                     :inv, :conf, 'active', 'antithesis', :pair_id)
            """),
            {
                "id": anti_id,
                "thesis": anti_thesis,
                "ptype": "canvas_investigation",
                "evidence": json.dumps(anti_evidence),
                "criteria": json.dumps(anti_criteria),
                "inv": f"Invalidated if original thesis is confirmed.",
                "conf": _clamp_confidence(1.0 - confidence),
                "pair_id": hyp_id,
            },
        )

        # 9. Link thesis back to its antithesis
        conn.execute(
            text(
                "UPDATE discovered_hypotheses SET pair_id = :pair_id WHERE id = :id"
            ),
            {"pair_id": anti_id, "id": hyp_id},
        )

        # 10. Add a HypothesisNode to the canvas board
        canvas_node_id = f"hyp-{hyp_id[:8]}"

        # Compute average position of existing nodes for placement
        xs = [n["position_x"] for n in nodes if n["position_x"] is not None]
        ys = [n["position_y"] for n in nodes if n["position_y"] is not None]
        avg_x = sum(xs) / max(len(xs), 1) if xs else 400.0
        avg_y = sum(ys) / max(len(ys), 1) if ys else 300.0

        conn.execute(
            text("""
                INSERT INTO canvas_nodes
                    (id, board_id, node_type, label, position_x, position_y, data)
                VALUES
                    (:id, :bid, 'hypothesis', :label, :px, :py, :data)
                ON CONFLICT (id) DO NOTHING
            """),
            {
                "id": canvas_node_id,
                "bid": req.board_id,
                "label": (
                    f"[{req.direction.upper()}] {req.ticker}: "
                    f"{req.thesis_text[:60]}"
                ),
                "px": avg_x + 200,
                "py": avg_y,
                "data": json.dumps({
                    "entityId": hyp_id,
                    "hypothesis_id": hyp_id,
                    "direction": req.direction,
                    "ticker": req.ticker,
                    "confidence": confidence,
                    "status": "active",
                }),
            },
        )

        # Touch board updated_at
        conn.execute(
            text("UPDATE canvas_boards SET updated_at = NOW() WHERE id = :bid"),
            {"bid": req.board_id},
        )

    log.info(
        "Canvas prediction created: {hyp} ({ticker} {dir}) from board {board}",
        hyp=hyp_id,
        ticker=req.ticker,
        dir=req.direction,
        board=req.board_id,
    )

    return PredictionResponse(
        hypothesis_id=hyp_id,
        thesis=structured_thesis,
        pattern_type="canvas_investigation",
        confidence=confidence,
        status="active",
        canvas_node_id=canvas_node_id,
    )
