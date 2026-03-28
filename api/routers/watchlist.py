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

# ── Simple in-memory cache for ticker search ──────────────────
_search_cache: dict[str, tuple[float, list[dict]]] = {}
_SEARCH_CACHE_TTL = 600  # 10 minutes

# ── Batch price cache ─────────────────────────────────────────
_price_cache: dict[str, dict] = {}
_price_cache_ts: float = 0.0
_PRICE_CACHE_TTL = 300  # 5 minutes


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


def _fetch_live_price(ticker: str) -> dict | None:
    """Fetch a live/recent price from yfinance as fallback.

    Returns {"price": float, "prev_close": float, "pct_1d": float, "source": "live"}
    or None on failure.
    """
    try:
        import yfinance as yf

        # Crypto tickers need -USD suffix for yfinance
        yf_ticker = ticker
        _CRYPTO_TICKERS = {"BTC", "ETH", "SOL", "DOGE", "TAO", "ADA", "XRP", "DOT", "AVAX", "MATIC", "LINK", "UNI"}
        if ticker.upper() in _CRYPTO_TICKERS and "-" not in ticker and "=" not in ticker:
            yf_ticker = f"{ticker}-USD"

        tk = yf.Ticker(yf_ticker)
        info = tk.fast_info
        price = getattr(info, "last_price", None)
        prev = getattr(info, "previous_close", None)
        if price is None:
            hist = tk.history(period="5d")
            if not hist.empty:
                price = float(hist["Close"].iloc[-1])
                if len(hist) >= 2:
                    prev = float(hist["Close"].iloc[-2])
        if price is None:
            return None
        pct_1d = round((price - prev) / prev, 5) if prev and prev != 0 else None
        return {"price": round(price, 4), "prev_close": round(prev, 4) if prev else None,
                "pct_1d": pct_1d, "source": "live"}
    except Exception as exc:
        log.debug("Live price fetch failed for {t}: {e}", t=ticker, e=str(exc))
        return None


def _batch_fetch_prices(tickers: list[str]) -> dict[str, dict]:
    """Batch-fetch live prices for multiple tickers via yf.download.

    Returns {TICKER: {price, prev_close, pct_1d, pct_1w, updated_at}, ...}.
    """
    import time
    from datetime import datetime, timezone

    if not tickers:
        return {}

    try:
        import yfinance as yf
        import pandas as pd

        joined = " ".join(tickers)
        df = yf.download(joined, period="5d", group_by="ticker", progress=False)

        results: dict[str, dict] = {}
        now_iso = datetime.now(timezone.utc).isoformat()

        for tk in tickers:
            try:
                if len(tickers) == 1:
                    close = df["Close"].dropna()
                else:
                    close = df[tk]["Close"].dropna()

                if close.empty:
                    continue

                last_price = float(close.iloc[-1])
                prev_close = float(close.iloc[-2]) if len(close) >= 2 else None
                first_price = float(close.iloc[0]) if len(close) >= 2 else None

                pct_1d = None
                if prev_close and prev_close != 0:
                    pct_1d = round((last_price - prev_close) / prev_close, 5)

                pct_1w = None
                if first_price and first_price != 0:
                    pct_1w = round((last_price - first_price) / first_price, 5)

                results[tk] = {
                    "price": round(last_price, 4),
                    "prev_close": round(prev_close, 4) if prev_close else None,
                    "pct_1d": pct_1d,
                    "pct_1w": pct_1w,
                    "updated_at": now_iso,
                }
            except Exception as exc:
                log.debug("Batch price parse failed for {t}: {e}", t=tk, e=str(exc))

        return results
    except Exception as exc:
        log.warning("Batch price download failed: {e}", e=str(exc))
        return {}


def _get_cached_prices() -> dict[str, dict] | None:
    """Return cached prices if within TTL, else None."""
    import time

    if _price_cache and (time.time() - _price_cache_ts) < _PRICE_CACHE_TTL:
        return _price_cache
    return None


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


@router.post("/refresh-prices")
async def refresh_watchlist_prices(
    _token: str = Depends(require_auth),
) -> dict:
    """Batch-fetch live prices for all watchlist tickers.

    Uses yf.download for a single API call. Results are cached for 5 minutes.
    If called within TTL, returns cached data instantly.
    """
    import time

    global _price_cache, _price_cache_ts

    # Return cached if fresh
    cached = _get_cached_prices()
    if cached is not None:
        return {"prices": cached, "cached": True}

    _init_table()
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT ticker FROM watchlist")).fetchall()

    tickers = [row[0] for row in rows]
    if not tickers:
        return {"prices": {}, "cached": False}

    prices = _batch_fetch_prices(tickers)

    # Update module-level cache
    _price_cache = prices
    _price_cache_ts = time.time()

    return {"prices": prices, "cached": False}


@router.get("/prices")
async def get_watchlist_prices(
    _token: str = Depends(require_auth),
) -> dict:
    """Return cached batch prices without triggering a refresh."""
    cached = _get_cached_prices()
    if cached is not None:
        return {"prices": cached, "fresh": True}
    return {"prices": _price_cache, "fresh": False}


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
    cached_prices = _get_cached_prices()
    try:
        today = date.today()
        with engine.connect() as conn:
            for tk in tickers:
                # Use batch cache if fresh
                if cached_prices and tk in cached_prices:
                    cp = cached_prices[tk]
                    price_data[tk] = {
                        "price": cp["price"],
                        "pct_1d": cp.get("pct_1d"),
                        "pct_1w": cp.get("pct_1w"),
                        "pct_1m": None,
                        "source": "batch",
                    }
                    continue

                feature_names = _resolve_feature_names(tk)

                # Try resolved_series first
                price_row = None
                if feature_names:
                    price_row = conn.execute(text(
                        "SELECT rs.value, rs.obs_date FROM resolved_series rs "
                        "JOIN feature_registry fr ON fr.id = rs.feature_id "
                        "WHERE fr.name = ANY(:names) "
                        "ORDER BY rs.obs_date DESC LIMIT 1"
                    ), {"names": feature_names}).fetchone()

                db_stale = (
                    price_row is None
                    or (today - price_row[1]).days > 3
                )

                if not db_stale and price_row:
                    latest = float(price_row[0])
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

                    price_data[tk] = {"price": latest, "pct_1m": pct_1m, "source": "grid"}
                else:
                    # Fallback: live price from yfinance
                    live = _fetch_live_price(tk)
                    if live:
                        price_data[tk] = {
                            "price": live["price"],
                            "pct_1d": live["pct_1d"],
                            "pct_1m": None,
                            "source": "live",
                        }
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
            "pct_1d": pd_.get("pct_1d"),
            "pct_1w": pd_.get("pct_1w"),
            "pct_1m": pd_.get("pct_1m"),
            "price_source": pd_.get("source"),
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


@router.get("/search")
async def search_tickers(
    q: str = Query(default="", min_length=1, max_length=20),
    _token: str = Depends(require_auth),
) -> dict:
    """Search for tickers via yfinance and the local feature_registry.

    Returns up to 8 results: [{ticker, name, asset_type, source}].
    Results are cached for 10 minutes.
    """
    import time

    q = q.strip().upper()
    if not q:
        return {"results": []}

    # Check cache
    now = time.time()
    if q in _search_cache:
        cached_at, cached_results = _search_cache[q]
        if now - cached_at < _SEARCH_CACHE_TTL:
            return {"results": cached_results}

    # Evict stale cache entries periodically (keep cache bounded)
    if len(_search_cache) > 500:
        stale_keys = [k for k, (t, _) in _search_cache.items() if now - t > _SEARCH_CACHE_TTL]
        for k in stale_keys:
            del _search_cache[k]

    results: list[dict] = []
    seen_tickers: set[str] = set()

    # ── 1. Search local feature_registry for matching tickers ────
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT DISTINCT name, family FROM feature_registry "
                    "WHERE UPPER(name) LIKE :pattern "
                    "ORDER BY name LIMIT 20"
                ),
                {"pattern": f"{q.lower()}%"},
            ).fetchall()

            for row in rows:
                name = row[0]
                family = row[1] or ""
                tk = name.split("_")[0].upper()
                if tk in seen_tickers:
                    continue
                seen_tickers.add(tk)
                results.append({
                    "ticker": tk,
                    "name": name,
                    "asset_type": _guess_asset_type(tk),
                    "source": "grid",
                })
    except Exception as exc:
        log.debug("Feature registry search failed: {e}", e=str(exc))

    # ── 2. Search yfinance for the query ─────────────────────────
    try:
        import yfinance as yf
        import concurrent.futures

        def _yf_lookup(symbol: str) -> dict | None:
            try:
                tk = yf.Ticker(symbol)
                info = tk.info or {}
                name = info.get("longName") or info.get("shortName") or symbol
                qtype = info.get("quoteType", "").lower()
                asset_type = (
                    "etf" if qtype == "etf" else
                    "crypto" if qtype == "cryptocurrency" else
                    "index" if qtype == "index" else
                    "forex" if qtype == "currency" else
                    "commodity" if qtype == "future" else
                    "stock"
                )
                return {
                    "ticker": symbol, "name": name,
                    "asset_type": asset_type, "source": "yfinance",
                }
            except Exception:
                return None

        candidates = [q]
        if not q.startswith("^"):
            candidates.append(f"^{q}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_map = {executor.submit(_yf_lookup, c): c for c in candidates}
            done, _ = concurrent.futures.wait(future_map, timeout=4.0)
            for fut in done:
                try:
                    result = fut.result(timeout=0.1)
                    if result and result["ticker"] not in seen_tickers:
                        seen_tickers.add(result["ticker"])
                        results.append(result)
                except Exception:
                    pass
    except Exception as exc:
        log.debug("yfinance search failed: {e}", e=str(exc))

    # ── 3. Also check sector_map for matching actors ─────────────
    try:
        from analysis.sector_map import SECTOR_MAP
        q_lower = q.lower()
        for sector_name, sector in SECTOR_MAP.items():
            for sub_name, sub in sector.get("subsectors", {}).items():
                for actor in sub.get("actors", []):
                    tk = actor.get("ticker", "")
                    name = actor.get("name", "")
                    if (
                        (tk.upper().startswith(q) or q_lower in name.lower())
                        and tk not in seen_tickers
                    ):
                        seen_tickers.add(tk)
                        results.append({
                            "ticker": tk,
                            "name": name,
                            "asset_type": _guess_asset_type(tk),
                            "source": "sector_map",
                        })
    except Exception:
        pass

    results = results[:8]
    _search_cache[q] = (now, results)
    return {"results": results}


def _guess_asset_type(ticker: str) -> str:
    """Guess asset type from ticker conventions."""
    tk = ticker.upper()
    if tk.startswith("^"):
        return "index"
    if tk.endswith("-USD") or (tk.endswith("USD") and len(tk) <= 7):
        return "crypto"
    if tk in ("GLD", "SLV", "USO", "UNG", "DBA", "DBC"):
        return "commodity"
    if tk in (
        "SPY", "QQQ", "IWM", "DIA", "TLT", "HYG", "LQD", "XLF",
        "XLE", "XLK", "XLV", "XLI", "XLP", "XLU", "XLY", "XLB",
        "SMH", "ARKK", "EEM", "VTI", "VOO", "VEA", "VWO", "BND",
    ):
        return "etf"
    return "stock"


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
    period: str = Query(default="3M", regex=r"^(1W|1M|3M|6M|1Y)$"),
    _token: str = Depends(require_auth),
) -> dict:
    """Comprehensive analysis page for a watchlist ticker.

    Returns price history, related features with z-scores, options signals,
    regime context, and TradingView webhook signals — all in one call.

    Query params:
        period: 1W | 1M | 3M | 6M | 1Y (default 3M) — controls price_history window.
    """
    _init_table()
    engine = get_db_engine()
    ticker_upper = ticker.strip().upper()
    ticker_lower = ticker_upper.lower()

    # Map period string to number of calendar days
    _period_days = {"1W": 7, "1M": 30, "3M": 90, "6M": 180, "1Y": 365}
    lookback_days = _period_days.get(period, 90)

    # Map to yfinance period strings (used for fallback)
    _yf_period_map = {"1W": "1mo", "1M": "1mo", "3M": "3mo", "6M": "6mo", "1Y": "1y"}

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
        "period": period,
    }

    feature_names = _resolve_feature_names(ticker_upper)

    with engine.connect() as conn:
        # ── Price history (from resolved_series, period-aware) ──
        try:
            price_rows = conn.execute(
                text(
                    "SELECT rs.obs_date, rs.value "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE fr.name = ANY(:names) "
                    "AND rs.obs_date >= CURRENT_DATE - :days "
                    "ORDER BY rs.obs_date"
                ),
                {"names": feature_names, "days": lookback_days},
            ).fetchall()
            analysis["price_history"] = [
                {"date": str(r[0]), "value": float(r[1])} for r in price_rows
            ]
            analysis["price_source"] = "grid"
        except Exception as exc:
            log.debug("Price history for {t}: {e}", t=ticker_upper, e=str(exc))
            analysis["price_history"] = []

        # Fallback to yfinance if no DB history
        if not analysis["price_history"]:
            try:
                import yfinance as yf

                yf_period = _yf_period_map.get(period, "3mo")
                hist = yf.Ticker(ticker_upper).history(period=yf_period)
                if not hist.empty:
                    rows = []
                    for idx, row in hist.iterrows():
                        entry: dict[str, Any] = {
                            "date": idx.strftime("%Y-%m-%d"),
                            "value": round(float(row["Close"]), 4),
                        }
                        if "Volume" in row and row["Volume"] is not None:
                            entry["volume"] = int(row["Volume"])
                        rows.append(entry)
                    analysis["price_history"] = rows
                    analysis["price_source"] = "yfinance"
            except Exception as exc:
                log.debug("yfinance fallback for {t}: {e}", t=ticker_upper, e=str(exc))

            # Final fallback: single live price point
            if not analysis["price_history"]:
                live = _fetch_live_price(ticker_upper)
                if live:
                    analysis["live_price"] = live
                    analysis["price_source"] = "live"

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


@router.get("/{ticker}/overview")
async def get_ticker_overview(
    ticker: str,
    _token: str = Depends(require_auth),
) -> dict:
    """AI-generated market overview for a watchlist ticker.

    Gathers price, options, regime, and sector context, then asks the LLM
    to produce a concise 3-5 sentence narrative.  Falls back to a rule-based
    overview when the LLM is unavailable.

    Returns:
        dict with keys: overview, key_levels, sentiment, generated_at,
        sector_path (for the capital-flow mini-chart).
    """
    from datetime import datetime, date, timedelta

    _init_table()
    engine = get_db_engine()
    ticker_upper = ticker.strip().upper()

    # ── Gather data ──────────────────────────────────────────────
    price_info: dict = {}
    options_info: dict | None = None
    regime_info: dict | None = None
    sector_info: dict = {}
    related_features: list[dict] = []
    feature_names = _resolve_feature_names(ticker_upper)

    with engine.connect() as conn:
        # Price
        try:
            price_row = conn.execute(text(
                "SELECT rs.value, rs.obs_date FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE fr.name = ANY(:names) "
                "ORDER BY rs.obs_date DESC LIMIT 1"
            ), {"names": feature_names}).fetchone()
            if price_row:
                price_info = {"price": float(price_row[0]), "date": str(price_row[1]), "source": "grid"}
        except Exception:
            pass

        if not price_info:
            live = _fetch_live_price(ticker_upper)
            if live:
                price_info = {"price": live["price"], "pct_1d": live.get("pct_1d"), "source": "live"}

        # Options (latest)
        try:
            opt_row = conn.execute(text(
                "SELECT signal_date, put_call_ratio, max_pain, iv_atm, iv_skew, "
                "spot_price, total_oi "
                "FROM options_daily_signals "
                "WHERE ticker = :ticker "
                "ORDER BY signal_date DESC LIMIT 1"
            ), {"ticker": ticker_upper}).fetchone()
            if opt_row:
                options_info = {
                    "date": str(opt_row[0]),
                    "put_call_ratio": opt_row[1],
                    "max_pain": opt_row[2],
                    "iv_atm": opt_row[3],
                    "iv_skew": opt_row[4],
                    "spot_price": opt_row[5],
                    "total_oi": opt_row[6],
                }
        except Exception:
            pass

        # Regime
        try:
            regime_row = conn.execute(text(
                "SELECT inferred_state, state_confidence, grid_recommendation "
                "FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1"
            )).fetchone()
            if regime_row:
                regime_info = {
                    "state": regime_row[0],
                    "confidence": float(regime_row[1]) if regime_row[1] else None,
                    "posture": regime_row[2],
                }
        except Exception:
            pass

        # Related features (recent values for context)
        try:
            tk_lower = ticker_upper.lower().replace("-", "_")
            like_patterns = [f"{tk_lower}%"]
            tk_clean = tk_lower.lstrip("^").replace("=", "")
            if tk_clean != tk_lower:
                like_patterns.append(f"{tk_clean}%")
            if feature_names:
                canonical_base = feature_names[0].rsplit("_", 1)[0]
                pattern = f"{canonical_base}%"
                if pattern not in like_patterns:
                    like_patterns.append(pattern)

            feat_rows = conn.execute(
                text(
                    "SELECT fr.name, rs.value, rs.obs_date "
                    "FROM resolved_series rs "
                    "JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "WHERE (" + " OR ".join(
                        f"fr.name LIKE :p{i}" for i in range(len(like_patterns))
                    ) + ") "
                    "AND rs.obs_date = ("
                    "  SELECT MAX(rs2.obs_date) FROM resolved_series rs2 "
                    "  WHERE rs2.feature_id = rs.feature_id"
                    ") "
                    "ORDER BY fr.name LIMIT 10"
                ),
                {f"p{i}": p for i, p in enumerate(like_patterns)},
            ).fetchall()
            related_features = [
                {"name": r[0], "value": float(r[1]) if r[1] is not None else None, "date": str(r[2])}
                for r in feat_rows
            ]
        except Exception:
            pass

    # ── Sector path (for capital-flow mini-chart) ────────────────
    try:
        from analysis.sector_map import SECTOR_MAP
        for sector_name, sector in SECTOR_MAP.items():
            for sub_name, sub in sector.get("subsectors", {}).items():
                for actor in sub.get("actors", []):
                    if actor.get("ticker") == ticker_upper:
                        # Collect peers: other actors in the same subsector with a ticker,
                        # sorted by weight descending, up to 5.
                        peers = sorted(
                            [
                                {"ticker": a["ticker"], "name": a.get("name", a["ticker"]),
                                 "weight": a.get("weight", 0)}
                                for a in sub.get("actors", [])
                                if a.get("ticker") and a["ticker"] != ticker_upper
                            ],
                            key=lambda p: p["weight"],
                            reverse=True,
                        )[:5]
                        sector_info = {
                            "sector": sector_name,
                            "sector_etf": sector.get("etf"),
                            "subsector": sub_name,
                            "subsector_weight": sub.get("weight", 0),
                            "actor_name": actor.get("name", ticker_upper),
                            "actor_weight": actor.get("weight", 0),
                            "influence": round(sub.get("weight", 0) * actor.get("weight", 0), 4),
                            "description": actor.get("description", ""),
                            "peers": peers,
                        }
                        break
                if sector_info:
                    break
            if sector_info:
                break
    except Exception:
        pass

    # ── Derive sentiment (rule-based) ────────────────────────────
    sentiment_score = 0
    if options_info and options_info.get("put_call_ratio") is not None:
        pcr = options_info["put_call_ratio"]
        if pcr < 0.7:
            sentiment_score += 1
        elif pcr > 1.3:
            sentiment_score -= 1

    if options_info and options_info.get("iv_atm") is not None:
        if options_info["iv_atm"] > 0.4:
            sentiment_score -= 1

    if regime_info and regime_info.get("state"):
        state = regime_info["state"].upper()
        if state == "GROWTH":
            sentiment_score += 1
        elif state in ("CRISIS", "FRAGILE"):
            sentiment_score -= 1

    sentiment = "bullish" if sentiment_score > 0 else "bearish" if sentiment_score < 0 else "neutral"

    # ── Key levels ───────────────────────────────────────────────
    key_levels: list[dict] = []
    if options_info and options_info.get("max_pain") is not None:
        key_levels.append({"label": "Max Pain", "value": options_info["max_pain"]})
    if options_info and options_info.get("spot_price") is not None:
        key_levels.append({"label": "Spot", "value": options_info["spot_price"]})
    if price_info.get("price") is not None:
        key_levels.append({"label": "Last", "value": price_info["price"]})

    # ── Build LLM prompt ─────────────────────────────────────────
    context_parts: list[str] = []
    if price_info.get("price"):
        context_parts.append(f"Current price: ${price_info['price']:.2f}")
    if options_info:
        pcr_val = options_info.get("put_call_ratio")
        iv_val = options_info.get("iv_atm")
        mp_val = options_info.get("max_pain")
        skew_val = options_info.get("iv_skew")
        context_parts.append(
            f"Options: P/C ratio {pcr_val:.2f}, IV ATM {iv_val*100:.1f}%, "
            f"max pain ${mp_val:.0f}, IV skew {skew_val:.2f}"
            if pcr_val is not None and iv_val is not None and mp_val is not None and skew_val is not None
            else "Options data available (partial)"
        )
    if sector_info:
        context_parts.append(
            f"Sector: {sector_info['sector']} / {sector_info['subsector']} — "
            f"{sector_info.get('description', '')}"
        )
    if regime_info:
        context_parts.append(
            f"Macro regime: {regime_info['state']} "
            f"(confidence {regime_info['confidence']*100:.0f}%)"
            if regime_info.get("confidence") else
            f"Macro regime: {regime_info['state']}"
        )
    if related_features:
        feat_summary = ", ".join(
            f"{f['name']}={f['value']:.4f}" for f in related_features[:5] if f.get("value") is not None
        )
        if feat_summary:
            context_parts.append(f"Related features: {feat_summary}")

    prompt_text = (
        f"Write a structured market overview for {ticker_upper}. "
        f"Return ONLY valid JSON (no markdown, no code fences) with this exact schema:\n"
        f'{{"sections": ['
        f'{{"title": "Price Action", "body": "1-2 sentences on current price and recent moves"}},'
        f'{{"title": "Options Flow", "body": "1-2 sentences on options positioning"}},'
        f'{{"title": "Sector Context", "body": "1-2 sentences on sector dynamics"}},'
        f'{{"title": "Risk & Levels", "body": "1-2 sentences on key risk levels to watch"}}'
        f'], "bottom_line": "One sentence: what to do right now"}}\n\n'
        f"Context:\n" + "\n".join(f"- {p}" for p in context_parts)
    )

    llm_system_prompt = (
        "You are a senior market analyst. Respond ONLY with valid JSON matching "
        "the requested schema. Be specific about numbers. No disclaimers."
    )

    # ── Call LLM (llama.cpp first, ollama fallback) ──────────────
    raw_llm_text: str | None = None
    try:
        from llamacpp.client import get_client as get_llamacpp
        llm = get_llamacpp()
        if llm.is_available:
            raw_llm_text = llm.chat(
                messages=[
                    {"role": "system", "content": llm_system_prompt},
                    {"role": "user", "content": prompt_text},
                ],
                temperature=0.3,
                num_predict=800,
            )
    except Exception as exc:
        log.debug("llama.cpp overview failed: {e}", e=str(exc))

    if raw_llm_text is None:
        try:
            from ollama.client import get_client as get_ollama
            llm_ollama = get_ollama()
            if llm_ollama.is_available:
                raw_llm_text = llm_ollama.chat(
                    messages=[
                        {"role": "system", "content": llm_system_prompt},
                        {"role": "user", "content": prompt_text},
                    ],
                    temperature=0.3,
                    num_predict=800,
                )
        except Exception as exc:
            log.debug("Ollama overview failed: {e}", e=str(exc))

    # ── Parse LLM JSON response ─────────────────────────────────
    import json as _json

    sections: list[dict] | None = None
    bottom_line: str | None = None

    if raw_llm_text is not None:
        # Strip markdown code fences if present
        cleaned = raw_llm_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1]  # drop first ```json line
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

        try:
            parsed = _json.loads(cleaned)
            if isinstance(parsed, dict) and "sections" in parsed:
                sections = parsed["sections"]
                bottom_line = parsed.get("bottom_line")
        except (_json.JSONDecodeError, TypeError):
            log.debug("LLM returned non-JSON overview, falling back to single section")
            sections = [{"title": "Overview", "body": raw_llm_text}]

    # ── Rule-based fallback (structured sections) ────────────────
    if sections is None:
        sections = []

        # Price Action section
        if price_info.get("price"):
            price_body = f"{ticker_upper} is trading at ${price_info['price']:.2f}."
            if price_info.get("pct_1d") is not None:
                pct = price_info["pct_1d"]
                direction = "up" if pct >= 0 else "down"
                price_body += f" The stock is {direction} {abs(pct):.1f}% on the day."
            sections.append({"title": "Price Action", "body": price_body})

        # Options Flow section
        if options_info and options_info.get("put_call_ratio") is not None:
            pcr = options_info["put_call_ratio"]
            opts_sent = "bearish" if pcr > 1.3 else "bullish" if pcr < 0.7 else "neutral"
            opts_body = f"Options positioning is {opts_sent} with a put/call ratio of {pcr:.2f}."
            if options_info.get("iv_atm"):
                opts_body += f" IV ATM sits at {options_info['iv_atm']*100:.1f}%."
            if options_info.get("iv_skew"):
                skew = options_info["iv_skew"]
                skew_desc = "elevated put demand" if skew > 1.3 else "complacent skew" if skew < 0.9 else "normal skew"
                opts_body += f" IV skew at {skew:.2f} indicates {skew_desc}."
            sections.append({"title": "Options Flow", "body": opts_body})

        # Sector Context section
        if sector_info:
            sect_body = (
                f"Within {sector_info['sector']}/{sector_info['subsector']}, "
                f"this name carries {sector_info['influence']:.0%} influence weight."
            )
            if sector_info.get("description"):
                sect_body += f" {sector_info['description']}"
            sections.append({"title": "Sector Context", "body": sect_body})

        # Risk & Levels section
        risk_parts: list[str] = []
        if regime_info:
            risk_parts.append(f"The macro regime is currently {regime_info['state']}.")
        if options_info and options_info.get("max_pain") is not None and options_info.get("spot_price") is not None:
            mp = options_info["max_pain"]
            spot = options_info["spot_price"]
            gap_pct = ((mp / spot) - 1) * 100 if spot else 0
            risk_parts.append(f"Max pain at ${mp:.0f} ({gap_pct:+.1f}% from spot).")
        if risk_parts:
            sections.append({"title": "Risk & Levels", "body": " ".join(risk_parts)})

        if not sections:
            sections.append({"title": "Overview", "body": f"No detailed data available for {ticker_upper} at this time."})

        # Rule-based bottom line
        bottom_line = f"Monitor {ticker_upper} — sentiment is {sentiment}."

    # ── Backwards-compatible overview text ───────────────────────
    overview_text = " ".join(s["body"] for s in sections)

    return {
        "overview": overview_text,
        "sections": sections,
        "bottom_line": bottom_line,
        "key_levels": key_levels,
        "sentiment": sentiment,
        "sector_path": sector_info or None,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }
