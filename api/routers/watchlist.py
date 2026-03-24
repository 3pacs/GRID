"""Watchlist endpoints."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.schemas.watchlist import WatchlistItemCreate, WatchlistItemResponse

router = APIRouter(prefix="/api/v1/watchlist", tags=["watchlist"])


def _ensure_watchlist_table() -> None:
    """Create the watchlist table if it does not exist."""
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS watchlist (
                id           SERIAL PRIMARY KEY,
                ticker       TEXT NOT NULL UNIQUE,
                display_name TEXT,
                asset_type   TEXT NOT NULL DEFAULT 'stock'
                                 CHECK (asset_type IN (
                                     'stock', 'crypto', 'commodity',
                                     'etf', 'index', 'forex'
                                 )),
                added_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                notes        TEXT
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_watchlist_ticker
            ON watchlist (ticker)
        """))
    log.debug("Watchlist table ensured")


# Ensure table exists on module import (startup)
_table_ready = False


def _init_table() -> None:
    """Lazy-init the table on first request."""
    global _table_ready
    if not _table_ready:
        try:
            _ensure_watchlist_table()
            _table_ready = True
        except Exception as exc:
            log.warning("Watchlist table init failed: {e}", e=str(exc))


def _row_to_dict(row: Any) -> dict:
    """Convert a DB row to a watchlist item dict."""
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    if d.get("added_at") is not None:
        d["added_at"] = str(d["added_at"])
    return d


@router.get("")
async def list_watchlist(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    _token: str = Depends(require_auth),
) -> dict:
    """Return all watchlist items with pagination."""
    _init_table()
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT * FROM watchlist"
                " ORDER BY added_at DESC"
                " LIMIT :limit OFFSET :offset"
            ),
            {"limit": limit, "offset": offset},
        ).fetchall()

        total = conn.execute(
            text("SELECT COUNT(*) FROM watchlist")
        ).fetchone()[0]

    items = [_row_to_dict(row) for row in rows]
    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < total,
    }


@router.post("", status_code=201)
async def add_to_watchlist(
    body: WatchlistItemCreate,
    _token: str = Depends(require_auth),
) -> dict:
    """Add a ticker to the watchlist."""
    _init_table()
    engine = get_db_engine()

    with engine.begin() as conn:
        # Check if already exists
        existing = conn.execute(
            text("SELECT id FROM watchlist WHERE ticker = :ticker"),
            {"ticker": body.ticker},
        ).fetchone()

        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Ticker {body.ticker} is already on the watchlist",
            )

        result = conn.execute(
            text(
                "INSERT INTO watchlist (ticker, display_name, asset_type, notes)"
                " VALUES (:ticker, :display_name, :asset_type, :notes)"
                " RETURNING id, added_at"
            ),
            {
                "ticker": body.ticker,
                "display_name": body.display_name or body.ticker,
                "asset_type": body.asset_type,
                "notes": body.notes,
            },
        ).fetchone()

    return {
        "id": result[0],
        "ticker": body.ticker,
        "added_at": str(result[1]),
        "status": "added",
    }


@router.delete("/{ticker}", status_code=200)
async def remove_from_watchlist(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Remove a ticker from the watchlist."""
    _init_table()
    engine = get_db_engine()
    ticker = ticker.strip().upper()

    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM watchlist WHERE ticker = :ticker RETURNING id"),
            {"ticker": ticker},
        ).fetchone()

    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ticker {ticker} not found on watchlist",
        )

    return {"status": "removed", "ticker": ticker}


@router.get("/{ticker}/analysis")
async def get_ticker_analysis(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict:
    """Get analysis for a watchlist ticker.

    Queries latest signals, regime context, and any journal entries
    mentioning this ticker.
    """
    _init_table()
    engine = get_db_engine()
    ticker = ticker.strip().upper()

    # Verify ticker is on watchlist
    with engine.connect() as conn:
        item = conn.execute(
            text("SELECT * FROM watchlist WHERE ticker = :ticker"),
            {"ticker": ticker},
        ).fetchone()

    if item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ticker {ticker} not on watchlist",
        )

    analysis: dict[str, Any] = {
        "ticker": ticker,
        "watchlist_item": _row_to_dict(item),
    }

    # Try to get latest signals for this ticker
    try:
        from inference.live import LiveInference
        from api.dependencies import get_pit_store

        pit = get_pit_store()
        li = LiveInference(engine, pit)
        result = li.run_inference()
        # Extract signal info relevant to this ticker if available
        analysis["signals"] = result if result else {}
    except Exception as exc:
        log.debug("Signal fetch for {t} failed: {e}", t=ticker, e=str(exc))
        analysis["signals"] = {"error": str(exc)}

    # Get current regime context
    try:
        with engine.connect() as conn:
            regime_row = conn.execute(
                text(
                    "SELECT * FROM decision_journal"
                    " ORDER BY decision_timestamp DESC LIMIT 1"
                )
            ).fetchone()
            if regime_row:
                d = dict(regime_row._mapping) if hasattr(regime_row, "_mapping") else dict(regime_row)
                analysis["latest_journal"] = {
                    "inferred_state": d.get("inferred_state"),
                    "action_taken": d.get("action_taken"),
                    "decision_timestamp": str(d.get("decision_timestamp")),
                }
            else:
                analysis["latest_journal"] = None
    except Exception as exc:
        log.debug("Journal fetch for analysis failed: {e}", e=str(exc))
        analysis["latest_journal"] = None

    # Get options signals if available
    try:
        with engine.connect() as conn:
            opts = conn.execute(
                text(
                    "SELECT * FROM options_daily_signals"
                    " WHERE ticker = :ticker"
                    " ORDER BY signal_date DESC LIMIT 1"
                ),
                {"ticker": ticker},
            ).fetchone()
            if opts:
                d = dict(opts._mapping) if hasattr(opts, "_mapping") else dict(opts)
                for k, v in d.items():
                    if hasattr(v, "isoformat"):
                        d[k] = str(v)
                analysis["options_signals"] = d
            else:
                analysis["options_signals"] = None
    except Exception as exc:
        log.debug("Options signals fetch for {t} failed: {e}", t=ticker, e=str(exc))
        analysis["options_signals"] = None

    return analysis
