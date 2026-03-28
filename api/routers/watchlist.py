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


def _resolve_feature_names(ticker: str) -> list[str]:
    """Resolve a watchlist ticker to all possible feature_registry names.

    Checks the entity map for the canonical mapping (YF:{ticker}:close),
    then falls back to common naming conventions ({ticker}_close, {ticker}_full,
    bare {ticker}).

    Parameters:
        ticker: Uppercase ticker symbol (e.g. 'SMH', '^GSPC').

    Returns:
        list[str]: Candidate feature names, ordered by likelihood.
    """
    from normalization.entity_map import SEED_MAPPINGS, NEW_MAPPINGS_V2

    tk_lower = ticker.lower().replace("-", "_")
    candidates: list[str] = []

    # 1. Check entity map for the canonical YF close mapping
    yf_key = f"YF:{ticker}:close"
    mapped = SEED_MAPPINGS.get(yf_key) or NEW_MAPPINGS_V2.get(yf_key)
    if mapped:
        candidates.append(mapped)

    # 2. Also check adj_close mapping
    yf_adj_key = f"YF:{ticker}:adj_close"
    mapped_adj = SEED_MAPPINGS.get(yf_adj_key) or NEW_MAPPINGS_V2.get(yf_adj_key)
    if mapped_adj and mapped_adj not in candidates:
        candidates.append(mapped_adj)

    # 3. Common naming conventions as fallback
    for suffix in ("_close", "_full", ""):
        name = f"{tk_lower}{suffix}"
        if name not in candidates:
            candidates.append(name)

    # 4. Also try without special chars (^gspc -> gspc)
    tk_clean = tk_lower.lstrip("^").replace("=", "")
    for suffix in ("_close", "_full", ""):
        name = f"{tk_clean}{suffix}"
        if name not in candidates:
            candidates.append(name)

    return candidates


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


@router.get("/enriched")
async def list_watchlist_enriched(
    limit: int = Query(default=20, ge=1, le=50),
    _token: str = Depends(require_auth),
) -> dict:
    """Return watchlist items enriched with sector context, options, z-scores, and insight.

    Each item includes: price, 1d/1w/1m changes, sector/subsector from sector_map,
    influence weight, options positioning, connected feature z-scores, and a
    human-readable 'insight' line explaining why this ticker matters right now.
    """
    from datetime import date, timedelta

    _init_table()
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM watchlist ORDER BY added_at DESC LIMIT :limit"),
            {"limit": limit},
        ).fetchall()

    if not rows:
        return {"items": [], "suggestions": []}

    items = [_row_to_dict(row) for row in rows]
    tickers = [it["ticker"] for it in items]

    # ── Sector map context ─────────────────────────────────────
    sector_ctx: dict[str, dict] = {}
    try:
        from analysis.sector_map import SECTOR_MAP
        for sector_name, sector in SECTOR_MAP.items():
            for sub_name, sub in sector.get("subsectors", {}).items():
                for actor in sub.get("actors", []):
                    tk = actor.get("ticker")
                    if tk and tk in tickers:
                        sector_ctx[tk] = {
                            "sector": sector_name,
                            "subsector": sub_name,
                            "influence": round(sub.get("weight", 0) * actor.get("weight", 0), 4),
                            "description": actor.get("description", ""),
                        }
    except Exception:
        pass

    # ── Options data ──────────────────────────────────────────
    opts_map: dict[str, dict] = {}
    try:
        with engine.connect() as conn:
            opts = conn.execute(text(
                "SELECT ticker, put_call_ratio, iv_atm, max_pain, spot_price "
                "FROM options_daily_signals "
                "WHERE signal_date = (SELECT MAX(signal_date) FROM options_daily_signals) "
                "AND ticker = ANY(:tickers)"
            ), {"tickers": tickers}).fetchall()
            for o in opts:
                opts_map[o[0]] = {"pcr": o[1], "iv": o[2], "max_pain": o[3], "spot": o[4]}
    except Exception:
        pass

    # ── Price changes + z-scores ──────────────────────────────
    price_data: dict[str, dict] = {}
    try:
        today = date.today()
        with engine.connect() as conn:
            for tk in tickers:
                feature_names = _resolve_feature_names(tk)
                if not feature_names:
                    continue

                # Get latest price using resolved feature names
                price_row = conn.execute(text(
                    "SELECT rs.value, rs.obs_date FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE fr.name = ANY(:names) "
                    "ORDER BY rs.obs_date DESC LIMIT 1"
                ), {"names": feature_names}).fetchone()

                if price_row:
                    latest = float(price_row[0])
                    # Get 30d ago price
                    prev_row = conn.execute(text(
                        "SELECT rs.value FROM resolved_series rs "
                        "JOIN feature_registry fr ON fr.id = rs.feature_id "
                        "WHERE fr.name = ANY(:names) "
                        "AND rs.obs_date <= :d30 "
                        "ORDER BY rs.obs_date DESC LIMIT 1"
                    ), {"names": feature_names,
                        "d30": today - timedelta(days=30)}).fetchone()

                    pct_1m = None
                    if prev_row and float(prev_row[0]) != 0:
                        pct_1m = round((latest - float(prev_row[0])) / float(prev_row[0]), 5)

                    price_data[tk] = {"price": latest, "pct_1m": pct_1m}
    except Exception:
        pass

    # ── Regime context ────────────────────────────────────────
    regime_state = None
    try:
        with engine.connect() as conn:
            r = conn.execute(text(
                "SELECT inferred_state, state_confidence FROM decision_journal "
                "ORDER BY decision_timestamp DESC LIMIT 1"
            )).fetchone()
            if r:
                regime_state = {"state": r[0], "confidence": float(r[1]) if r[1] else None}
    except Exception:
        pass

    # ── Build enriched items ──────────────────────────────────
    enriched = []
    for item in items:
        tk = item["ticker"]
        sc = sector_ctx.get(tk, {})
        opts = opts_map.get(tk)
        pd_ = price_data.get(tk, {})

        # Generate insight line
        parts = []
        if sc.get("sector"):
            parts.append(f"{sc['sector']} / {sc.get('subsector', '?')}")
            if sc.get("influence"):
                parts.append(f"influence {sc['influence']:.0%}")
        if pd_.get("pct_1m") is not None:
            pct = pd_["pct_1m"]
            parts.append(f"{'up' if pct > 0 else 'down'} {abs(pct)*100:.1f}% in 30d")
        if opts and opts.get("pcr"):
            pcr = opts["pcr"]
            sent = "bearish" if pcr > 1.2 else "bullish" if pcr < 0.7 else "neutral"
            parts.append(f"options {sent} (P/C {pcr:.2f})")
        if regime_state:
            parts.append(f"regime {regime_state['state']}")

        insight = " · ".join(parts) if parts else "No context available"

        enriched.append({
            **item,
            "price": pd_.get("price"),
            "pct_1m": pd_.get("pct_1m"),
            "sector": sc.get("sector"),
            "subsector": sc.get("subsector"),
            "influence": sc.get("influence"),
            "actor_description": sc.get("description"),
            "options": opts,
            "regime": regime_state,
            "insight": insight,
        })

    # ── Auto-suggest tickers from sector map not on watchlist ─
    suggestions = []
    try:
        from analysis.sector_map import SECTOR_MAP
        existing = set(tickers)
        for sector_name, sector in SECTOR_MAP.items():
            for sub_name, sub in sector.get("subsectors", {}).items():
                for actor in sub.get("actors", []):
                    tk = actor.get("ticker")
                    if tk and tk not in existing and actor.get("weight", 0) >= 0.15:
                        suggestions.append({
                            "ticker": tk, "name": actor["name"],
                            "sector": sector_name, "subsector": sub_name,
                            "weight": actor["weight"],
                        })
        suggestions.sort(key=lambda x: x["weight"], reverse=True)
        suggestions = suggestions[:5]
    except Exception:
        pass

    return {"items": enriched, "suggestions": suggestions}


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
    """Comprehensive analysis page for a watchlist ticker.

    Returns price history, related features with z-scores, options signals,
    regime context, and TradingView webhook signals — all in one call.
    """
    _init_table()
    engine = get_db_engine()
    ticker_upper = ticker.strip().upper()
    ticker_lower = ticker_upper.lower()

    # Verify ticker is on watchlist
    with engine.connect() as conn:
        item = conn.execute(
            text("SELECT * FROM watchlist WHERE ticker = :ticker"),
            {"ticker": ticker_upper},
        ).fetchone()

    if item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ticker {ticker_upper} not on watchlist",
        )

    analysis: dict[str, Any] = {
        "ticker": ticker_upper,
        "watchlist_item": _row_to_dict(item),
    }

    feature_names = _resolve_feature_names(ticker_upper)

    with engine.connect() as conn:
        # ── Price history (last 90 days from resolved_series) ──
        try:
            price_rows = conn.execute(
                text(
                    "SELECT rs.obs_date, rs.value "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE fr.name = ANY(:names) "
                    "AND rs.obs_date >= CURRENT_DATE - 90 "
                    "ORDER BY rs.obs_date"
                ),
                {"names": feature_names},
            ).fetchall()
            analysis["price_history"] = [
                {"date": str(r[0]), "value": float(r[1])} for r in price_rows
            ]
        except Exception as exc:
            log.debug("Price history for {t}: {e}", t=ticker_upper, e=str(exc))
            analysis["price_history"] = []

        # ── Related features with z-scores ──
        try:
            # Build LIKE patterns from both the raw ticker and canonical feature base
            like_patterns = [f"{ticker_lower}%"]
            tk_clean = ticker_lower.lstrip("^").replace("=", "")
            if tk_clean != ticker_lower:
                like_patterns.append(f"{tk_clean}%")
            # Add the canonical feature base (e.g. "sp500" from "sp500_close")
            if feature_names:
                canonical_base = feature_names[0].rsplit("_", 1)[0]
                pattern = f"{canonical_base}%"
                if pattern not in like_patterns:
                    like_patterns.append(pattern)

            feat_rows = conn.execute(
                text(
                    "SELECT fr.name, fr.family, rs.value, rs.obs_date "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE (" + " OR ".join(
                        f"fr.name LIKE :p{i}" for i in range(len(like_patterns))
                    ) + ") "
                    "AND rs.obs_date = ("
                    "  SELECT MAX(rs2.obs_date) FROM resolved_series rs2 "
                    "  WHERE rs2.feature_id = rs.feature_id"
                    ") "
                    "ORDER BY fr.name"
                ),
                {f"p{i}": p for i, p in enumerate(like_patterns)},
            ).fetchall()
            analysis["related_features"] = [
                {
                    "name": r[0], "family": r[1],
                    "value": float(r[2]) if r[2] is not None else None,
                    "obs_date": str(r[3]),
                }
                for r in feat_rows
            ]
        except Exception as exc:
            log.debug("Related features for {t}: {e}", t=ticker_upper, e=str(exc))
            analysis["related_features"] = []

        # ── Options signals ──
        try:
            opts = conn.execute(
                text(
                    "SELECT signal_date, put_call_ratio, max_pain, iv_skew, "
                    "total_oi, total_volume, spot_price, iv_atm, "
                    "iv_25d_put, iv_25d_call, term_structure_slope, oi_concentration "
                    "FROM options_daily_signals "
                    "WHERE ticker = :ticker "
                    "ORDER BY signal_date DESC LIMIT 5"
                ),
                {"ticker": ticker_upper},
            ).fetchall()
            analysis["options"] = [
                {
                    "date": str(r[0]),
                    "put_call_ratio": r[1], "max_pain": r[2], "iv_skew": r[3],
                    "total_oi": r[4], "total_volume": r[5], "spot_price": r[6],
                    "iv_atm": r[7], "iv_25d_put": r[8], "iv_25d_call": r[9],
                    "term_slope": r[10], "oi_concentration": r[11],
                }
                for r in opts
            ]
        except Exception as exc:
            log.debug("Options for {t}: {e}", t=ticker_upper, e=str(exc))
            analysis["options"] = []

        # ── Current regime context ──
        try:
            regime = conn.execute(
                text(
                    "SELECT inferred_state, state_confidence, "
                    "grid_recommendation, decision_timestamp "
                    "FROM decision_journal "
                    "ORDER BY decision_timestamp DESC LIMIT 1"
                )
            ).fetchone()
            if regime:
                analysis["regime"] = {
                    "state": regime[0], "confidence": float(regime[1]) if regime[1] else None,
                    "posture": regime[2], "as_of": str(regime[3]),
                }
        except Exception:
            analysis["regime"] = None

        # ── TradingView webhook signals for this ticker ──
        try:
            tv_rows = conn.execute(
                text(
                    "SELECT rs.pull_timestamp, rs.value, rs.raw_payload "
                    "FROM raw_series rs "
                    "JOIN source_catalog sc ON sc.id = rs.source_id "
                    "WHERE sc.name = 'TradingView' "
                    "AND rs.series_id LIKE :pattern "
                    "ORDER BY rs.pull_timestamp DESC LIMIT 10"
                ),
                {"pattern": f"tv_{ticker_lower}%"},
            ).fetchall()
            import json as _json
            analysis["tradingview_signals"] = [
                {
                    "timestamp": str(r[0]),
                    "signal_value": float(r[1]) if r[1] is not None else None,
                    **(r[2] if isinstance(r[2], dict) else _json.loads(r[2]) if r[2] else {}),
                }
                for r in tv_rows
            ]
        except Exception as exc:
            log.debug("TV signals for {t}: {e}", t=ticker_upper, e=str(exc))
            analysis["tradingview_signals"] = []

    return analysis
