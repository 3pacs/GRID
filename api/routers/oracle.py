"""Oracle prediction endpoints — predictions, scoreboard, latest cycle."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from oracle.publish import publish_astrogrid_prediction
from oracle.scoreboard import build_oracle_scoreboard

router = APIRouter(prefix="/api/v1/oracle", tags=["oracle"])


class OraclePublishRequest(BaseModel):
    prediction_id: str
    question: str
    target_universe: str = "hybrid"
    target_symbols: list[str] = []
    horizon_label: str = "swing"
    call: str
    timing: str
    invalidation: str
    confidence: float = 0.5
    weight_version: str = "astrogrid-v1"
    model_version: str = "astrogrid-oracle-v1"
    grid_summary: str | None = None
    mystical_summary: str | None = None
    oracle_prediction_id: str | None = None


# ── GET /predictions ───────────────────────────────────────────────────────

@router.get("/predictions")
async def get_predictions(
    ticker: str | None = Query(None, description="Filter by ticker"),
    model: str | None = Query(None, description="Filter by model name"),
    status: str | None = Query(None, description="active / expired / scored"),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _token: str = Depends(require_auth),
) -> dict:
    """Active predictions with confidence, direction, signal/anti-signal."""
    engine = get_db_engine()
    today = date.today()

    where_clauses = []
    params: dict[str, Any] = {"lim": limit, "off": offset, "today": today}

    if ticker:
        where_clauses.append("ticker = :ticker")
        params["ticker"] = ticker
    if model:
        where_clauses.append("model_name = :model")
        params["model"] = model
    if status == "active":
        where_clauses.append("verdict = 'pending' AND expiry > :today")
    elif status == "expired":
        where_clauses.append("verdict = 'pending' AND expiry <= :today")
    elif status == "scored":
        where_clauses.append("verdict IN ('hit', 'miss', 'partial')")

    where_sql = " AND ".join(["1=1"] + where_clauses)

    with engine.connect() as conn:
        count_row = conn.execute(
            text("SELECT COUNT(*) FROM oracle_predictions WHERE " + where_sql),
            params,
        ).fetchone()
        total = count_row[0] if count_row else 0

        rows = conn.execute(
            text(
                "SELECT id, created_at, ticker, prediction_type, direction, "
                "target_price, entry_price, expiry, confidence, "
                "expected_move_pct, signal_strength, coherence, "
                "model_name, model_version, signals, anti_signals, "
                "flow_context, verdict, actual_price, actual_move_pct, "
                "pnl_pct, scored_at, score_notes "
                "FROM oracle_predictions WHERE " + where_sql + " "
                "ORDER BY created_at DESC LIMIT :lim OFFSET :off"
            ),
            params,
        ).fetchall()

    predictions = []
    for r in rows:
        expiry_date = r[7]
        if isinstance(expiry_date, str):
            expiry_date = date.fromisoformat(expiry_date)
        days_left = (expiry_date - today).days if expiry_date else 0

        # Compute tracking P&L for active predictions
        tracking_pnl = None
        if r[17] == "pending" and r[6]:
            try:
                with engine.connect() as conn2:
                    spot = conn2.execute(text("""
                        SELECT spot_price FROM options_daily_signals
                        WHERE ticker = :t AND spot_price > 0
                        ORDER BY signal_date DESC LIMIT 1
                    """), {"t": r[2]}).fetchone()
                    if spot:
                        current = float(spot[0])
                        entry = float(r[6])
                        move_pct = (current - entry) / entry * 100
                        tracking_pnl = move_pct if r[4] == "CALL" else -move_pct
            except Exception:
                pass

        predictions.append({
            "id": r[0],
            "created_at": r[1].isoformat() if r[1] else None,
            "ticker": r[2],
            "prediction_type": r[3],
            "direction": r[4],
            "target_price": r[5],
            "entry_price": r[6],
            "expiry": expiry_date.isoformat() if expiry_date else None,
            "confidence": r[8],
            "expected_move_pct": r[9],
            "signal_strength": r[10],
            "coherence": r[11],
            "model_name": r[12],
            "model_version": r[13],
            "signals": r[14] if isinstance(r[14], list) else [],
            "anti_signals": r[15] if isinstance(r[15], list) else [],
            "flow_context": r[16] if isinstance(r[16], dict) else {},
            "verdict": r[17],
            "actual_price": r[18],
            "actual_move_pct": r[19],
            "pnl_pct": r[20],
            "scored_at": r[21].isoformat() if r[21] else None,
            "score_notes": r[22],
            "days_left": days_left,
            "tracking_pnl": round(tracking_pnl, 2) if tracking_pnl is not None else None,
        })

    return {"predictions": predictions, "total": total, "limit": limit, "offset": offset}


# ── GET /scoreboard ────────────────────────────────────────────────────────

@router.get("/scoreboard")
async def get_scoreboard(
    _token: str = Depends(require_auth),
) -> dict:
    """Model tournament: overall accuracy, by-model, by-ticker, calibration."""
    engine = get_db_engine()
    return build_oracle_scoreboard(engine)


# ── GET /latest ────────────────────────────────────────────────────────────

@router.get("/latest")
async def get_latest(
    _token: str = Depends(require_auth),
) -> dict:
    """Most recent prediction cycle results — headline predictions."""
    engine = get_db_engine()

    with engine.connect() as conn:
        # Get the most recent cycle timestamp
        latest_ts = conn.execute(text("""
            SELECT created_at FROM oracle_predictions
            ORDER BY created_at DESC LIMIT 1
        """)).fetchone()

        if not latest_ts:
            return {"cycle_time": None, "predictions": [], "streak": None, "recent_scored": []}

        cycle_time = latest_ts[0]

        # Headline predictions from latest cycle (same created_at within 5 min window)
        rows = conn.execute(text("""
            SELECT id, ticker, direction, target_price, entry_price,
                   expiry, confidence, expected_move_pct, model_name,
                   signal_strength, coherence, signals, anti_signals,
                   flow_context, created_at
            FROM oracle_predictions
            WHERE created_at >= :ct - INTERVAL '5 minutes'
            ORDER BY confidence DESC
            LIMIT 20
        """), {"ct": cycle_time}).fetchall()

        predictions = []
        for r in rows:
            predictions.append({
                "id": r[0], "ticker": r[1], "direction": r[2],
                "target_price": r[3], "entry_price": r[4],
                "expiry": r[5].isoformat() if r[5] else None,
                "confidence": r[6], "expected_move_pct": r[7],
                "model_name": r[8], "signal_strength": r[9],
                "coherence": r[10],
                "signals": r[11] if isinstance(r[11], list) else [],
                "anti_signals": r[12] if isinstance(r[12], list) else [],
                "flow_context": r[13] if isinstance(r[13], dict) else {},
                "created_at": r[14].isoformat() if r[14] else None,
            })

        # Recent scored predictions for track record
        scored_rows = conn.execute(text("""
            SELECT id, ticker, direction, entry_price, actual_price,
                   confidence, verdict, pnl_pct, scored_at, score_notes,
                   model_name, expiry
            FROM oracle_predictions
            WHERE verdict IN ('hit', 'miss', 'partial')
            ORDER BY scored_at DESC
            LIMIT 20
        """)).fetchall()

        recent_scored = []
        for s in scored_rows:
            recent_scored.append({
                "id": s[0], "ticker": s[1], "direction": s[2],
                "entry_price": s[3], "actual_price": s[4],
                "confidence": s[5], "verdict": s[6],
                "pnl_pct": s[7],
                "scored_at": s[8].isoformat() if s[8] else None,
                "score_notes": s[9], "model_name": s[10],
                "expiry": s[11].isoformat() if s[11] else None,
            })

        # Compute streak
        streak = _compute_streak(engine)

    return {
        "cycle_time": cycle_time.isoformat() if cycle_time else None,
        "predictions": predictions,
        "recent_scored": recent_scored,
        "streak": streak,
    }


def _compute_streak(engine) -> dict:
    """Compute current win/loss streak."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT verdict FROM oracle_predictions
            WHERE verdict IN ('hit', 'miss', 'partial')
            ORDER BY scored_at DESC
            LIMIT 50
        """)).fetchall()

    if not rows:
        return {"type": "none", "count": 0, "label": "No scored predictions"}

    verdicts = [r[0] for r in rows]

    # Count streak from most recent
    streak_type = "win" if verdicts[0] in ("hit", "partial") else "loss"
    count = 0
    for v in verdicts:
        is_win = v in ("hit", "partial")
        if (streak_type == "win" and is_win) or (streak_type == "loss" and not is_win):
            count += 1
        else:
            break

    if streak_type == "win":
        label = f"{count} win{'s' if count != 1 else ''} in a row"
    else:
        label = f"cold streak: {count} miss{'es' if count != 1 else ''}"

    return {"type": streak_type, "count": count, "label": label}


@router.post("/publish")
async def publish_prediction(
    req: OraclePublishRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Explicit write contract for reduced comparable prediction records."""
    try:
        return publish_astrogrid_prediction(get_db_engine(), req.model_dump())
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Oracle publish failed: {exc}") from exc
