"""Watchlist sub-router: core CRUD and utility endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.schemas.watchlist import WatchlistItemCreate
from api.routers.watchlist_helpers import (
    _ANALYSIS_CACHE_TTL,
    _batch_fetch_prices,
    _get_cached_prices,
    _guess_asset_type,
    _init_table,
    _fetch_live_price,
    _cache_price_to_db,
    _resolve_feature_names,
    _row_to_dict,
    _SEARCH_CACHE_TTL,
    _preload_one,
)

# The module-level caches live in watchlist_helpers — import them as module
# references so mutations (via global) are visible here too.
import api.routers.watchlist_helpers as _wh

router = APIRouter(tags=["watchlist"])


@router.get("/")
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
    _wh._price_cache = prices
    _wh._price_cache_ts = time.time()

    # Push live price update to all WebSocket clients
    try:
        from api.main import broadcast_event
        broadcast_event("prices", prices)
    except Exception:
        pass  # graceful degradation

    return {"prices": prices, "cached": False}


@router.get("/prices")
async def get_watchlist_prices(
    _token: str = Depends(require_auth),
) -> dict:
    """Return cached batch prices without triggering a refresh."""
    cached = _get_cached_prices()
    if cached is not None:
        return {"prices": cached, "fresh": True}
    return {"prices": _wh._price_cache, "fresh": False}


@router.get("/portfolio")
async def get_portfolio(
    _token: str = Depends(require_auth),
) -> dict:
    """Portfolio analytics view — watchlist as a portfolio with P&L, allocation, risk.

    Since we don't have actual position sizes, each ticker gets equal weight
    unless a custom weight column is set. Computes allocation by sector and
    asset type, risk metrics (concentration, beta, diversification), and
    options P&L from the recommendation tracker.
    """
    import time
    from datetime import date, timedelta

    _init_table()
    engine = get_db_engine()

    # ── Ensure weight column exists ──────────────────────────────
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE watchlist ADD COLUMN IF NOT EXISTS weight NUMERIC DEFAULT NULL"
            ))
    except Exception:
        pass  # column already exists or DB doesn't support IF NOT EXISTS

    # ── Load watchlist with weights ──────────────────────────────
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT ticker, display_name, asset_type, weight FROM watchlist ORDER BY added_at"
        )).fetchall()

    if not rows:
        return {
            "total_value": 0, "total_pnl_1d": 0, "total_pnl_1d_pct": 0,
            "total_pnl_1m": 0, "positions": [], "allocation": {
                "by_sector": {}, "by_asset_type": {},
            }, "risk_metrics": {
                "concentration_top3": 0, "beta_weighted": 0,
                "sector_diversification_score": 0,
            }, "options_pnl": {
                "total_recommendations": 0, "wins": 0, "losses": 0,
                "open": 0, "total_return": 0,
            },
        }

    items = []
    for r in rows:
        items.append({
            "ticker": r[0],
            "display_name": r[1],
            "asset_type": r[2] or "stock",
            "custom_weight": float(r[3]) if r[3] is not None else None,
        })

    tickers = [it["ticker"] for it in items]
    n = len(tickers)

    # ── Assign weights (custom or equal) ─────────────────────────
    has_custom = any(it["custom_weight"] is not None for it in items)
    if has_custom:
        total_custom = sum(it["custom_weight"] or 0 for it in items)
        for it in items:
            it["weight"] = (it["custom_weight"] / total_custom) if (
                it["custom_weight"] and total_custom > 0
            ) else (1.0 / n)
    else:
        for it in items:
            it["weight"] = 1.0 / n

    # ── Fetch prices ─────────────────────────────────────────────
    cached_prices = _get_cached_prices()
    if not cached_prices:
        cached_prices = _batch_fetch_prices(tickers)
        if cached_prices:
            _wh._price_cache = cached_prices
            _wh._price_cache_ts = time.time()

    # ── Sector map ───────────────────────────────────────────────
    sector_ctx: dict[str, str] = {}
    try:
        from analysis.sector_map import SECTOR_MAP
        for sector_name, sector in SECTOR_MAP.items():
            for sub_name, sub in sector.get("subsectors", {}).items():
                for actor in sub.get("actors", []):
                    tk = actor.get("ticker")
                    if tk and tk in tickers:
                        sector_ctx[tk] = sector_name
    except Exception:
        pass

    # Default sector guesses by asset_type
    for it in items:
        if it["ticker"] not in sector_ctx:
            if it["asset_type"] == "crypto":
                sector_ctx[it["ticker"]] = "Crypto"
            elif it["asset_type"] == "etf":
                sector_ctx[it["ticker"]] = "ETF"
            elif it["asset_type"] == "commodity":
                sector_ctx[it["ticker"]] = "Commodities"
            else:
                sector_ctx[it["ticker"]] = "Other"

    # ── Build positions list ─────────────────────────────────────
    ESTIMATED_PORTFOLIO = 125_000  # estimated portfolio value
    positions = []
    total_pnl_1d = 0.0
    total_pnl_1m = 0.0

    for it in items:
        tk = it["ticker"]
        pd_ = cached_prices.get(tk, {}) if cached_prices else {}
        price = pd_.get("price")
        pct_1d = pd_.get("pct_1d")
        pct_1w = pd_.get("pct_1w")

        # Estimate 1m from 1w if not available
        pct_1m = None
        if pct_1w is not None:
            pct_1m = pct_1w * 4.0 / 1.0  # rough extrapolation from 1w

        alloc_value = ESTIMATED_PORTFOLIO * it["weight"]
        pnl_1d = round(alloc_value * pct_1d, 2) if pct_1d is not None else 0
        pnl_1m = round(alloc_value * (pct_1m or 0), 2)

        total_pnl_1d += pnl_1d
        total_pnl_1m += pnl_1m

        positions.append({
            "ticker": tk,
            "display_name": it["display_name"],
            "price": price,
            "change_1d": pct_1d,
            "change_1w": pct_1w,
            "weight": round(it["weight"], 4),
            "sector": sector_ctx.get(tk, "Other"),
            "asset_type": it["asset_type"],
            "pnl_1d": pnl_1d,
        })

    # ── Allocation ───────────────────────────────────────────────
    by_sector: dict[str, float] = {}
    by_asset_type: dict[str, float] = {}
    for pos in positions:
        sec = pos["sector"]
        by_sector[sec] = round(by_sector.get(sec, 0) + pos["weight"], 4)
        at = pos["asset_type"]
        by_asset_type[at] = round(by_asset_type.get(at, 0) + pos["weight"], 4)

    # ── Risk metrics ─────────────────────────────────────────────
    sorted_weights = sorted([p["weight"] for p in positions], reverse=True)
    concentration_top3 = round(sum(sorted_weights[:3]), 4) if len(sorted_weights) >= 3 else 1.0

    # Simple beta estimate: weight stocks ~1.1, crypto ~1.8, etf ~1.0
    beta_map = {"stock": 1.1, "crypto": 1.8, "etf": 1.0, "commodity": 0.6,
                "index": 1.0, "forex": 0.3}
    beta_weighted = round(sum(
        p["weight"] * beta_map.get(p["asset_type"], 1.0) for p in positions
    ), 2)

    # Sector diversification: 1 - HHI (Herfindahl) of sector weights
    hhi = sum(w ** 2 for w in by_sector.values())
    sector_diversification = round(1.0 - hhi, 4)

    # ── Options P&L from recommendation tracker ──────────────────
    options_pnl = {
        "total_recommendations": 0, "wins": 0, "losses": 0,
        "open": 0, "total_return": 0,
    }
    try:
        with engine.connect() as conn:
            stats = conn.execute(text("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE outcome = 'WIN') AS wins,
                    COUNT(*) FILTER (WHERE outcome = 'LOSS') AS losses,
                    COUNT(*) FILTER (WHERE outcome = 'EXPIRED') AS expired,
                    COUNT(*) FILTER (WHERE outcome IS NULL AND expiry > CURRENT_DATE) AS open,
                    COALESCE(SUM(actual_return) FILTER (WHERE outcome IS NOT NULL), 0) AS total_return
                FROM options_recommendations
            """)).fetchone()
            if stats:
                options_pnl = {
                    "total_recommendations": stats[0] or 0,
                    "wins": stats[1] or 0,
                    "losses": (stats[2] or 0) + (stats[3] or 0),
                    "open": stats[4] or 0,
                    "total_return": round(float(stats[5] or 0), 2),
                }
    except Exception as exc:
        log.debug("Options P&L query failed: {e}", e=str(exc))

    return {
        "total_value": ESTIMATED_PORTFOLIO,
        "total_pnl_1d": round(total_pnl_1d, 2),
        "total_pnl_1d_pct": round(total_pnl_1d / ESTIMATED_PORTFOLIO, 4) if ESTIMATED_PORTFOLIO else 0,
        "total_pnl_1m": round(total_pnl_1m, 2),
        "positions": positions,
        "allocation": {
            "by_sector": by_sector,
            "by_asset_type": by_asset_type,
        },
        "risk_metrics": {
            "concentration_top3": concentration_top3,
            "beta_weighted": beta_weighted,
            "sector_diversification_score": sector_diversification,
        },
        "options_pnl": options_pnl,
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
                        # Write back to DB so next lookup is fast
                        _cache_price_to_db(engine, tk, live["price"], today)
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
    if q in _wh._search_cache:
        cached_at, cached_results = _wh._search_cache[q]
        if now - cached_at < _SEARCH_CACHE_TTL:
            return {"results": cached_results}

    # Evict stale cache entries periodically (keep cache bounded)
    if len(_wh._search_cache) > 500:
        stale_keys = [k for k, (t, _) in _wh._search_cache.items() if now - t > _SEARCH_CACHE_TTL]
        for k in stale_keys:
            del _wh._search_cache[k]

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
    _wh._search_cache[q] = (now, results)
    return {"results": results}


@router.post("/", status_code=201)
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


@router.get("/preload")
async def preload_watchlist(
    _token: str = Depends(require_auth),
) -> dict:
    """Trigger background loading of analysis data for ALL watchlist tickers.

    Call this on dashboard load so that by the time the user clicks a ticker,
    the analysis data is already cached and ready to serve instantly.

    Returns immediately with the list of tickers being preloaded.
    """
    import time
    import concurrent.futures

    _init_table()
    engine = get_db_engine()

    with engine.connect() as conn:
        rows = conn.execute(text("SELECT ticker FROM watchlist")).fetchall()

    tickers = [row[0] for row in rows]
    if not tickers:
        return {"preloading": [], "status": "empty"}

    # Filter to only tickers that are NOT already cached
    now = time.time()
    need_loading = []
    for tk in tickers:
        cache_key = f"{tk}:3M"
        if cache_key in _wh._analysis_cache:
            cached_at, _ = _wh._analysis_cache[cache_key]
            if (now - cached_at) < _ANALYSIS_CACHE_TTL:
                continue
        need_loading.append(tk)

    if not need_loading:
        return {"preloading": [], "already_cached": len(tickers), "status": "all_cached"}

    # Run preloads in a thread pool
    loaded = []
    failed = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_map = {executor.submit(_preload_one, tk): tk for tk in need_loading}
        done, _ = concurrent.futures.wait(future_map, timeout=30.0)
        for fut in done:
            try:
                result = fut.result(timeout=0.1)
                if result:
                    loaded.append(result)
                else:
                    failed.append(future_map[fut])
            except Exception:
                failed.append(future_map[fut])

    return {
        "preloading": loaded,
        "failed": failed,
        "already_cached": len(tickers) - len(need_loading),
        "status": "ok",
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
