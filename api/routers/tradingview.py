"""TradingView webhook integration.

Receives Pine Script alert webhooks from TradingView, validates them,
stores the signal in raw_series, and optionally resolves to features.

Endpoints:
  POST /api/v1/tradingview/webhook  — Receive alert (API key auth)
  GET  /api/v1/tradingview/signals  — List recent TV signals (JWT auth)
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/tradingview", tags=["tradingview"])

# ── Source catalog auto-registration ────────────────────────────

_SOURCE_ID_CACHE: int | None = None


def _get_source_id(engine: Engine) -> int:
    global _SOURCE_ID_CACHE
    if _SOURCE_ID_CACHE is not None:
        return _SOURCE_ID_CACHE

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT id FROM source_catalog WHERE name = 'TradingView'")
        ).fetchone()
        if row:
            _SOURCE_ID_CACHE = row[0]
            return _SOURCE_ID_CACHE

    with engine.begin() as conn:
        result = conn.execute(
            text(
                "INSERT INTO source_catalog "
                "(name, base_url, cost_tier, latency_class, "
                "pit_available, revision_behavior, trust_score, priority_rank, active) "
                "VALUES ('TradingView', 'https://tradingview.com', 'FREE', "
                "'REALTIME', FALSE, 'NEVER', 'MED', 25, TRUE) "
                "ON CONFLICT (name) DO UPDATE SET name = 'TradingView' "
                "RETURNING id"
            )
        )
        _SOURCE_ID_CACHE = result.fetchone()[0]
        return _SOURCE_ID_CACHE


# ── Webhook key validation ──────────────────────────────────────

def _validate_webhook_key(
    request: Request,
    x_webhook_key: str | None = Header(default=None),
) -> str:
    """Validate the webhook API key from header or query param."""
    from config import settings

    secret = getattr(settings, "TRADINGVIEW_WEBHOOK_SECRET", "")
    if not secret:
        raise HTTPException(503, "TradingView webhook not configured")

    # Check header first, then query param (TradingView can't send custom headers)
    key = x_webhook_key or request.query_params.get("key")
    if key != secret:
        raise HTTPException(401, "Invalid webhook key")
    return key


# ── Webhook endpoint ────────────────────────────────────────────

@router.post("/webhook")
async def receive_webhook(
    request: Request,
    _key: str = Depends(_validate_webhook_key),
) -> dict[str, Any]:
    """Receive a TradingView Pine Script alert webhook.

    Expected JSON payload (customizable in TradingView alert message):
    {
        "ticker": "AAPL",
        "action": "buy" | "sell" | "alert",
        "price": 185.50,
        "interval": "1h",
        "strategy": "my_strategy",
        "message": "optional free text"
    }

    All fields are optional except ticker. Extra fields are stored in raw_payload.
    """
    try:
        body = await request.json()
    except Exception:
        raw = await request.body()
        # TradingView sometimes sends plain text
        body = {"message": raw.decode("utf-8", errors="replace")}

    ticker = body.get("ticker", body.get("symbol", "UNKNOWN"))
    action = body.get("action", body.get("order_action", "alert")).lower()
    price = body.get("price", body.get("close"))
    interval = body.get("interval", body.get("timeframe", ""))
    strategy = body.get("strategy", body.get("strategy_name", ""))
    message = body.get("message", "")

    # Normalize price to float
    if price is not None:
        try:
            price = float(price)
        except (ValueError, TypeError):
            price = None

    # Map action to numeric signal: buy=1, sell=-1, alert=0
    signal_value = {"buy": 1.0, "sell": -1.0, "strong_buy": 2.0, "strong_sell": -2.0}.get(
        action, 0.0
    )

    now = datetime.now(timezone.utc)
    today = date.today()

    engine = get_db_engine()
    source_id = _get_source_id(engine)

    # Build series_id for this signal
    series_id = f"tv_{ticker}_{strategy}".lower().replace(" ", "_") if strategy else f"tv_{ticker}".lower()

    # Store in raw_series
    raw_payload = {
        "ticker": ticker,
        "action": action,
        "price": price,
        "interval": interval,
        "strategy": strategy,
        "message": message,
        "signal_value": signal_value,
        "received_at": now.isoformat(),
        **{k: v for k, v in body.items() if k not in ("ticker", "action", "price", "interval", "strategy", "message", "symbol", "close", "order_action", "timeframe", "strategy_name")},
    }

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, "
                "pull_timestamp, value, raw_payload, pull_status) "
                "VALUES (:sid, :src, :obs, :ts, :val, :payload, 'SUCCESS')"
            ),
            {
                "sid": series_id,
                "src": source_id,
                "obs": today,
                "ts": now,
                "val": signal_value,
                "payload": json.dumps(raw_payload),
            },
        )

    log.info(
        "TradingView webhook — {t} {a} @ {p} ({s})",
        t=ticker, a=action, p=price, s=strategy or "no strategy",
    )

    return {
        "status": "received",
        "ticker": ticker,
        "action": action,
        "signal_value": signal_value,
        "series_id": series_id,
        "timestamp": now.isoformat(),
    }


# ── Query recent signals ────────────────────────────────────────

@router.get("/signals")
async def get_signals(
    limit: int = 50,
    ticker: str | None = None,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return recent TradingView webhook signals."""
    engine = get_db_engine()

    query = (
        "SELECT rs.series_id, rs.obs_date, rs.pull_timestamp, "
        "rs.value, rs.raw_payload "
        "FROM raw_series rs "
        "JOIN source_catalog sc ON sc.id = rs.source_id "
        "WHERE sc.name = 'TradingView' "
    )
    params: dict[str, Any] = {"limit": min(limit, 200)}

    if ticker:
        query += "AND rs.series_id LIKE :ticker_pat "
        params["ticker_pat"] = f"tv_{ticker.lower()}%"

    query += "ORDER BY rs.pull_timestamp DESC LIMIT :limit"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).fetchall()

    signals = []
    for row in rows:
        payload = row[4] if isinstance(row[4], dict) else json.loads(row[4]) if row[4] else {}
        signals.append({
            "series_id": row[0],
            "date": str(row[1]),
            "received_at": row[2].isoformat() if row[2] else None,
            "signal_value": float(row[3]) if row[3] is not None else None,
            "ticker": payload.get("ticker"),
            "action": payload.get("action"),
            "price": payload.get("price"),
            "strategy": payload.get("strategy"),
            "message": payload.get("message"),
        })

    return {"signals": signals, "count": len(signals)}
