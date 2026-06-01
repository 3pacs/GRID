"""Price alerts for stepdad.finance.

Dad (or any user) can ask to be told when a stock crosses a price. We store the
alert, a background checker (scripts/check_price_alerts.py) polls prices, and on
a cross we fire a real iMessage to the alert's owner via the Mac mini.

This module owns the table + CRUD + the price read. ``create_alert_record`` is
imported by the chat composer so a natural-language request ("tell me when Apple
hits $250") creates an alert directly instead of being logged as a capability
gap.

All writes are immutable in spirit: we INSERT new rows and flip ``active`` flags
rather than mutating request history.
"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends
from loguru import logger as log
from pydantic import BaseModel, Field
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.chat import _user_id_from_token
from api.routers.watchlist_helpers import (
    _cache_price_to_db,
    _fetch_live_price,
    _resolve_feature_names,
)

router = APIRouter(prefix="/api/v1/alerts", tags=["alerts"])

VALID_DIRECTIONS = {"above", "below"}

_ALERTS_DDL = """
CREATE TABLE IF NOT EXISTS sd_price_alerts (
    id              BIGSERIAL PRIMARY KEY,
    owner           TEXT NOT NULL DEFAULT 'dad',
    ticker          TEXT NOT NULL,
    direction       TEXT NOT NULL,
    threshold       DOUBLE PRECISION NOT NULL,
    note            TEXT,
    active          BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    triggered_at    TIMESTAMPTZ,
    last_price      DOUBLE PRECISION,
    price_at_create DOUBLE PRECISION
)
"""


def ensure_alerts_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_ALERTS_DDL))


# ── Price read (shared with the checker) ─────────────────────────────────

def current_price(ticker: str, *, prefer_live: bool = False) -> tuple[float | None, str]:
    """Latest known price for a ticker as (price, source).

    ``prefer_live=True`` (used by the checker) fetches a fresh quote first so a
    cross is detected intraday; the REST create path uses the fast cached read.
    """
    engine = get_db_engine()
    tk = (ticker or "").strip().upper()
    if not tk:
        return None, "none"

    def _from_grid() -> float | None:
        names = _resolve_feature_names(tk)
        try:
            with engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT rs.value FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE fr.name = ANY(:names) "
                    "ORDER BY rs.obs_date DESC LIMIT 1"
                ), {"names": names}).fetchone()
            return float(row[0]) if row and row[0] is not None else None
        except Exception as exc:
            log.debug("Alert price (grid) failed for {t}: {e}", t=tk, e=str(exc))
            return None

    def _from_live() -> float | None:
        try:
            live = _fetch_live_price(tk)
            if live and live.get("price") is not None:
                px = float(live["price"])
                try:
                    _cache_price_to_db(engine, tk, px, date.today())
                except Exception:
                    pass
                return px
        except Exception as exc:
            log.debug("Alert price (live) failed for {t}: {e}", t=tk, e=str(exc))
        return None

    if prefer_live:
        px = _from_live()
        if px is not None:
            return px, "live"
        px = _from_grid()
        return (px, "grid") if px is not None else (None, "none")

    px = _from_grid()
    if px is not None:
        return px, "grid"
    px = _from_live()
    return (px, "live") if px is not None else (None, "none")


# ── Core create (importable by the composer) ─────────────────────────────

def create_alert_record(
    owner: str,
    ticker: str,
    direction: str,
    threshold: float,
    note: str | None = None,
) -> dict:
    """Validate + insert an alert. Returns a result dict (never raises)."""
    tk = (ticker or "").strip().upper()
    direction = (direction or "").strip().lower()
    if not tk:
        return {"ok": False, "error": "Which stock? I didn't catch a ticker."}
    if direction not in VALID_DIRECTIONS:
        return {"ok": False, "error": "Tell me 'above' or 'below' a price."}
    try:
        threshold = float(threshold)
    except (TypeError, ValueError):
        return {"ok": False, "error": "I need a dollar amount to watch for."}
    if threshold <= 0:
        return {"ok": False, "error": "The price to watch must be above $0."}

    px, _src = current_price(tk, prefer_live=False)
    already_met = px is not None and (
        (direction == "above" and px >= threshold)
        or (direction == "below" and px <= threshold)
    )

    engine = get_db_engine()
    ensure_alerts_table(engine)
    try:
        with engine.begin() as conn:
            row = conn.execute(text(
                "INSERT INTO sd_price_alerts "
                "(owner, ticker, direction, threshold, note, price_at_create) "
                "VALUES (:o, :t, :d, :th, :n, :p) RETURNING id"
            ), {"o": owner or "dad", "t": tk, "d": direction,
                "th": threshold, "n": note, "p": px}).fetchone()
        alert_id = int(row[0])
    except Exception as exc:
        log.warning("Create alert failed: {e}", e=str(exc))
        return {"ok": False, "error": "Couldn't save that alert — try again."}

    return {
        "ok": True,
        "id": alert_id,
        "ticker": tk,
        "direction": direction,
        "threshold": threshold,
        "current_price": px,
        "already_met": bool(already_met),
    }


# ── REST models + endpoints ──────────────────────────────────────────────

class AlertCreate(BaseModel):
    ticker: str = Field(..., min_length=1, max_length=12)
    direction: str = Field(..., pattern="^(above|below)$")
    threshold: float = Field(..., gt=0)
    note: str | None = Field(None, max_length=280)


@router.post("")
async def create_alert(req: AlertCreate, token: str = Depends(require_auth)) -> dict:
    owner = _user_id_from_token(token) or "dad"
    return create_alert_record(owner, req.ticker, req.direction, req.threshold, req.note)


@router.get("")
async def list_alerts(token: str = Depends(require_auth)) -> dict:
    """Active alerts + recently triggered ones for the current user."""
    owner = _user_id_from_token(token) or "dad"
    cols = ["id", "ticker", "direction", "threshold", "note", "active",
            "created_at", "triggered_at", "last_price", "price_at_create"]
    try:
        engine = get_db_engine()
        ensure_alerts_table(engine)
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT {', '.join(cols)} FROM sd_price_alerts "
                "WHERE owner = :o AND (active = true OR triggered_at > now() - interval '7 days') "
                "ORDER BY active DESC, COALESCE(triggered_at, created_at) DESC LIMIT 100"
            ), {"o": owner}).fetchall()
        return {"alerts": [dict(zip(cols, r)) for r in rows]}
    except Exception as exc:
        return {"alerts": [], "error": str(exc)}


@router.delete("/{alert_id}")
async def cancel_alert(alert_id: int, token: str = Depends(require_auth)) -> dict:
    owner = _user_id_from_token(token) or "dad"
    try:
        engine = get_db_engine()
        ensure_alerts_table(engine)
        with engine.begin() as conn:
            res = conn.execute(text(
                "UPDATE sd_price_alerts SET active = false "
                "WHERE id = :id AND owner = :o AND active = true"
            ), {"id": alert_id, "o": owner})
        return {"ok": True, "cancelled": res.rowcount}
    except Exception as exc:
        log.warning("Cancel alert failed: {e}", e=str(exc))
        return {"ok": False, "error": str(exc)}
