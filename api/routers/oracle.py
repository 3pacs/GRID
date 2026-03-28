"""Oracle prediction endpoints — predictions, scoreboard, latest cycle."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/oracle", tags=["oracle"])


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

    where_sql = (" AND " + " AND ".join(where_clauses)) if where_clauses else ""

    with engine.connect() as conn:
        # Get total count
        count_row = conn.execute(text(
            f"SELECT COUNT(*) FROM oracle_predictions WHERE 1=1 {where_sql}"
        ), params).fetchone()
        total = count_row[0] if count_row else 0

        rows = conn.execute(text(f"""
            SELECT id, created_at, ticker, prediction_type, direction,
                   target_price, entry_price, expiry, confidence,
                   expected_move_pct, signal_strength, coherence,
                   model_name, model_version, signals, anti_signals,
                   flow_context, verdict, actual_price, actual_move_pct,
                   pnl_pct, scored_at, score_notes
            FROM oracle_predictions
            WHERE 1=1 {where_sql}
            ORDER BY created_at DESC
            LIMIT :lim OFFSET :off
        """), params).fetchall()

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

    with engine.connect() as conn:
        # Overall stats
        overall = conn.execute(text("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN verdict = 'hit' THEN 1 ELSE 0 END) AS hits,
                SUM(CASE WHEN verdict = 'miss' THEN 1 ELSE 0 END) AS misses,
                SUM(CASE WHEN verdict = 'partial' THEN 1 ELSE 0 END) AS partials,
                SUM(CASE WHEN verdict = 'pending' THEN 1 ELSE 0 END) AS pending,
                AVG(CASE WHEN verdict IN ('hit','miss','partial') THEN pnl_pct END) AS avg_pnl,
                SUM(CASE WHEN verdict IN ('hit','miss','partial') THEN pnl_pct ELSE 0 END) AS total_pnl
            FROM oracle_predictions
        """)).fetchone()

        total = overall[0] or 0
        hits = overall[1] or 0
        misses = overall[2] or 0
        partials = overall[3] or 0
        pending = overall[4] or 0
        avg_pnl = float(overall[5]) if overall[5] else 0.0
        total_pnl = float(overall[6]) if overall[6] else 0.0
        scored = hits + misses + partials
        accuracy = (hits + partials * 0.5) / scored if scored > 0 else 0.0

        # By-model stats
        model_rows = conn.execute(text("""
            SELECT om.name, om.weight, om.predictions_made,
                   om.hits, om.misses, om.partials,
                   om.cumulative_pnl, om.sharpe, om.description
            FROM oracle_models om
            ORDER BY om.weight DESC
        """)).fetchall()

        models = []
        for m in model_rows:
            m_total = (m[3] or 0) + (m[4] or 0) + (m[5] or 0)
            m_accuracy = ((m[3] or 0) + (m[5] or 0) * 0.5) / m_total if m_total > 0 else 0.0
            models.append({
                "name": m[0],
                "weight": float(m[1] or 1.0),
                "predictions_made": m[2] or 0,
                "hits": m[3] or 0,
                "misses": m[4] or 0,
                "partials": m[5] or 0,
                "accuracy": round(m_accuracy, 4),
                "cumulative_pnl": round(float(m[6] or 0), 2),
                "sharpe": round(float(m[7] or 0), 2),
                "description": m[8] or "",
                "total_scored": m_total,
            })

        # By-ticker stats
        ticker_rows = conn.execute(text("""
            SELECT ticker,
                   COUNT(*) AS total,
                   SUM(CASE WHEN verdict = 'hit' THEN 1 ELSE 0 END) AS hits,
                   SUM(CASE WHEN verdict = 'miss' THEN 1 ELSE 0 END) AS misses,
                   SUM(CASE WHEN verdict = 'partial' THEN 1 ELSE 0 END) AS partials,
                   SUM(CASE WHEN verdict IN ('hit','miss','partial') THEN pnl_pct ELSE 0 END) AS pnl
            FROM oracle_predictions
            WHERE verdict IN ('hit', 'miss', 'partial')
            GROUP BY ticker
            ORDER BY COUNT(*) DESC
            LIMIT 30
        """)).fetchall()

        by_ticker = []
        for t in ticker_rows:
            t_scored = (t[2] or 0) + (t[3] or 0) + (t[4] or 0)
            t_acc = ((t[2] or 0) + (t[4] or 0) * 0.5) / t_scored if t_scored > 0 else 0.0
            by_ticker.append({
                "ticker": t[0],
                "total": t[1] or 0,
                "hits": t[2] or 0,
                "misses": t[3] or 0,
                "partials": t[4] or 0,
                "accuracy": round(t_acc, 4),
                "pnl": round(float(t[5] or 0), 2),
            })

        # Calibration data
        calibration_data = None
        try:
            from oracle.calibration import compute_calibration
            cal = compute_calibration(engine)
            calibration_data = cal.to_dict()
        except Exception as exc:
            log.warning("Calibration computation failed: {e}", e=str(exc))

    return {
        "overall": {
            "total_predictions": total,
            "scored": scored,
            "pending": pending,
            "hits": hits,
            "misses": misses,
            "partials": partials,
            "accuracy": round(accuracy, 4),
            "avg_pnl": round(avg_pnl, 2),
            "total_pnl": round(total_pnl, 2),
        },
        "models": models,
        "by_ticker": by_ticker,
        "calibration": calibration_data,
    }


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
