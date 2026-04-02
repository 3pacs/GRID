"""Watchlist shared helpers — utilities imported by sub-routers and external callers.

Exported symbols used outside this package:
    _batch_fetch_prices     — api.routers.astrogrid_core, scripts/seed_astrogrid_prediction_corpus
    _cache_price_to_db      — api.routers.astrogrid_core
    _resolve_feature_names  — api.routers.astrogrid_helpers
"""

from __future__ import annotations

import re
import time
from typing import Any

from loguru import logger as log
from sqlalchemy import text

from api.dependencies import get_db_engine

# ── Human-readable display names for feature keys ─────────────────────────
# Pattern-based: regex -> format string (group 1 is title-cased and
# inserted via .format()).  Ordered so the first match wins.

_FEATURE_DISPLAY: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^(.+)_fifty_day_avg$"), "{} 50-Day Average"),
    (re.compile(r"^(.+)_two_hundred_avg$"), "{} 200-Day Average"),
    (re.compile(r"^(.+)_fifty_two_high$"), "{} 52-Week High"),
    (re.compile(r"^(.+)_fifty_two_low$"), "{} 52-Week Low"),
    (re.compile(r"^(.+)_market_cap$"), "{} Market Cap"),
    (re.compile(r"^(.+)_total_volume$"), "{} Volume"),
    (re.compile(r"^(.+)_avg_volume$"), "{} Avg Volume"),
    (re.compile(r"^(.+)_close$"), "{} Price"),
    (re.compile(r"^(.+)_full$"), "{} Price"),
    (re.compile(r"^vix_(.+)$"), "VIX {}"),
    (re.compile(r"^dxy_(.+)$"), "Dollar Index {}"),
    (re.compile(r"^dex_(.+)_volume$"), "{} DEX Volume"),
    (re.compile(r"^dex_(.+)_liquidity$"), "{} DEX Liquidity"),
    (re.compile(r"^tvl_(.+)$"), "{} Total Value Locked"),
    (re.compile(r"^wiki_(.+)$"), "{} Wikipedia Views"),
    (re.compile(r"^defi_(.+)$"), "DeFi {}"),
]

# ── In-memory caches ───────────────────────────────────────────────────────
_search_cache: dict[str, tuple[float, list[dict]]] = {}
_SEARCH_CACHE_TTL = 600  # 10 minutes

_price_cache: dict[str, dict] = {}
_price_cache_ts: float = 0.0
_PRICE_CACHE_TTL = 300  # 5 minutes

_analysis_cache: dict[str, tuple[float, dict]] = {}
_ANALYSIS_CACHE_TTL = 300  # 5 minutes

# ── Table init ────────────────────────────────────────────────────────────
_table_ready = False


def _get_display_name(feature_name: str) -> str:
    """Return a human-readable display name for a feature key."""
    for pattern, fmt in _FEATURE_DISPLAY:
        m = pattern.match(feature_name)
        if m:
            token = m.group(1).replace("_", " ").title()
            return fmt.format(token)
    return feature_name.replace("_", " ").title()


def _fmt_large_number(v: float) -> str:
    """Format a large number as $X.XB / $X.XM / $X.XK."""
    abs_v = abs(v)
    if abs_v >= 1e12:
        return f"${v / 1e12:,.1f}T"
    if abs_v >= 1e9:
        return f"${v / 1e9:,.1f}B"
    if abs_v >= 1e6:
        return f"${v / 1e6:,.1f}M"
    if abs_v >= 1e3:
        return f"${v / 1e3:,.1f}K"
    return f"${v:,.2f}"


def _interpret_feature(
    name: str,
    value: float | None,
    ticker_price: float | None,
) -> tuple[str | None, str]:
    """Generate a one-line interpretation and signal for a feature.

    Returns (interpretation_text, signal) where signal is one of
    "bullish", "bearish", or "neutral".
    """
    if value is None:
        return None, "neutral"

    if "_fifty_day_avg" in name or "_two_hundred_avg" in name:
        if ticker_price is not None and value > 0:
            pct = (ticker_price - value) / value * 100
            direction = "above" if pct >= 0 else "below"
            signal = "bullish" if pct > 0 else "bearish" if pct < -0 else "neutral"
            label = "50-day" if "fifty_day" in name else "200-day"
            return (
                f"Trading {abs(pct):.1f}% {direction} its {label} average"
                f" — {'bullish momentum' if signal == 'bullish' else 'bearish pressure'}",
                signal,
            )

    if "_fifty_two_high" in name:
        if ticker_price is not None and value > 0:
            pct = (ticker_price - value) / value * 100
            signal = "bullish" if abs(pct) < 5 else "neutral"
            return (f"{abs(pct):.1f}% from 52-week high ({_fmt_large_number(value)})", signal)

    if "_fifty_two_low" in name:
        if ticker_price is not None and value > 0:
            pct = (ticker_price - value) / value * 100
            signal = "bearish" if pct < 10 else "neutral"
            return (f"{pct:.1f}% above 52-week low ({_fmt_large_number(value)})", signal)

    if "_market_cap" in name:
        return f"Market cap {_fmt_large_number(value)}", "neutral"

    if "_total_volume" in name or "_avg_volume" in name:
        return f"Volume {_fmt_large_number(value)}", "neutral"

    if name.startswith("vix"):
        if value > 25:
            return f"VIX at {value:.1f} — elevated volatility, risk-off", "bearish"
        elif value < 15:
            return f"VIX at {value:.1f} — low volatility, complacent market", "bullish"
        else:
            return f"VIX at {value:.1f} — normal range", "neutral"

    if name.startswith("dxy"):
        if value > 105:
            return f"Dollar index at {value:.1f} — strong dollar", "bearish"
        elif value < 95:
            return f"Dollar index at {value:.1f} — weak dollar", "bullish"
        else:
            return f"Dollar index at {value:.1f} — stable", "neutral"

    if name.startswith("tvl_") or name.startswith("defi_"):
        return f"Value {_fmt_large_number(value)}", "neutral"

    if name.startswith("dex_"):
        return f"{_fmt_large_number(value)}", "neutral"

    if name.startswith("wiki_"):
        return (
            f"{value:,.0f} views — "
            f"{'high public attention' if value > 100_000 else 'normal interest'}",
            "bullish" if value > 100_000 else "neutral",
        )

    if name.endswith("_close") or name.endswith("_full"):
        return f"{_fmt_large_number(value)}", "neutral"

    if abs(value) >= 1000:
        return f"{_fmt_large_number(value)}", "neutral"
    elif abs(value) < 0.01:
        return f"{value:.6f}", "neutral"
    else:
        return f"{value:,.2f}", "neutral"


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

        _CRYPTO_TICKERS = {
            "BTC", "ETH", "SOL", "DOGE", "TAO", "ADA", "XRP",
            "DOT", "AVAX", "MATIC", "LINK", "UNI",
        }
        yf_ticker = ticker
        if (
            ticker.upper() in _CRYPTO_TICKERS
            and "-" not in ticker
            and "=" not in ticker
        ):
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
        return {
            "price": round(price, 4),
            "prev_close": round(prev, 4) if prev else None,
            "pct_1d": pct_1d,
            "source": "live",
        }
    except Exception as exc:
        log.debug("Live price fetch failed for {t}: {e}", t=ticker, e=str(exc))
        return None


def _cache_price_to_db(engine: Any, ticker: str, price: float, date: Any) -> None:
    """Write yfinance price back to raw_series for future lookups."""
    try:
        from datetime import datetime, timezone

        with engine.begin() as conn:
            src_row = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = 'yfinance' LIMIT 1")
            ).fetchone()
            if src_row is None:
                src_row = conn.execute(
                    text(
                        "INSERT INTO source_catalog (name, source_type) "
                        "VALUES ('yfinance', 'market') RETURNING id"
                    )
                ).fetchone()
            source_id = src_row[0]

            series_id = f"yf_{ticker.lower()}_close"
            obs_date = date if date else datetime.now(timezone.utc).date()

            conn.execute(
                text(
                    "INSERT INTO raw_series "
                    "(source_id, series_id, obs_date, value, pull_timestamp) "
                    "VALUES (:source_id, :series_id, :obs_date, :value, NOW()) "
                    "ON CONFLICT (source_id, series_id, obs_date) "
                    "DO UPDATE SET value = EXCLUDED.value, pull_timestamp = NOW()"
                ),
                {
                    "source_id": source_id,
                    "series_id": series_id,
                    "obs_date": str(obs_date),
                    "value": price,
                },
            )
        log.debug("Cached price to DB: {t} = {p} on {d}", t=ticker, p=price, d=date)
    except Exception as exc:
        log.debug("Failed to cache price to DB for {t}: {e}", t=ticker, e=str(exc))


def _batch_fetch_prices(tickers: list[str]) -> dict[str, dict]:
    """Batch-fetch live prices for multiple tickers via yf.download.

    Returns {TICKER: {price, prev_close, pct_1d, pct_1w, updated_at}, ...}.
    """
    from datetime import datetime, timezone

    if not tickers:
        return {}

    try:
        import yfinance as yf

        _CRYPTO_TICKERS = {
            "BTC", "ETH", "SOL", "DOGE", "TAO", "ADA", "XRP",
            "DOT", "AVAX", "MATIC", "LINK", "UNI",
        }

        # Map original ticker -> yfinance ticker (add -USD for crypto)
        yf_map: dict[str, str] = {}
        for tk in tickers:
            if (
                tk.upper() in _CRYPTO_TICKERS
                and "-" not in tk
                and "=" not in tk
            ):
                yf_map[tk] = f"{tk}-USD"
            else:
                yf_map[tk] = tk

        yf_tickers = list(yf_map.values())
        joined = " ".join(yf_tickers)
        df = yf.download(joined, period="5d", group_by="ticker", progress=False)

        # Reverse map: yfinance ticker -> original ticker
        reverse_map = {v: k for k, v in yf_map.items()}

        results: dict[str, dict] = {}
        now_iso = datetime.now(timezone.utc).isoformat()

        for tk in tickers:
            try:
                yf_tk = yf_map.get(tk, tk)
                if len(yf_tickers) == 1:
                    close = df["Close"].dropna()
                else:
                    close = df[yf_tk]["Close"].dropna()

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
    if _price_cache and (time.time() - _price_cache_ts) < _PRICE_CACHE_TTL:
        return _price_cache
    return None


def _resolve_feature_names(ticker: str) -> list[str]:
    """Resolve a watchlist ticker to all possible feature_registry names.

    Checks the entity map for the canonical mapping (YF:{ticker}:close),
    then falls back to common naming conventions ({ticker}_close, {ticker}_full,
    bare {ticker}).
    """
    from normalization.entity_map import SEED_MAPPINGS, NEW_MAPPINGS_V2

    tk_lower = ticker.lower().replace("-", "_")
    candidates: list[str] = []

    yf_key = f"YF:{ticker}:close"
    mapped = SEED_MAPPINGS.get(yf_key) or NEW_MAPPINGS_V2.get(yf_key)
    if mapped:
        candidates.append(mapped)

    yf_adj_key = f"YF:{ticker}:adj_close"
    mapped_adj = SEED_MAPPINGS.get(yf_adj_key) or NEW_MAPPINGS_V2.get(yf_adj_key)
    if mapped_adj and mapped_adj not in candidates:
        candidates.append(mapped_adj)

    for suffix in ("_close", "_full", ""):
        name = f"{tk_lower}{suffix}"
        if name not in candidates:
            candidates.append(name)

    tk_clean = tk_lower.lstrip("^").replace("=", "")
    for suffix in ("_close", "_full", ""):
        name = f"{tk_clean}{suffix}"
        if name not in candidates:
            candidates.append(name)

    return candidates


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


def _get_analysis_cached(ticker: str, period: str) -> dict | None:
    """Return cached analysis if within TTL, else None."""
    cache_key = f"{ticker.upper()}:{period}"
    if cache_key in _analysis_cache:
        cached_at, cached_data = _analysis_cache[cache_key]
        if (time.time() - cached_at) < _ANALYSIS_CACHE_TTL:
            return cached_data
    return None


def _set_analysis_cache(ticker: str, period: str, data: dict) -> None:
    """Store analysis data in cache."""
    cache_key = f"{ticker.upper()}:{period}"
    _analysis_cache[cache_key] = (time.time(), data)

    if len(_analysis_cache) > 200:
        now = time.time()
        stale = [
            k for k, (t, _) in _analysis_cache.items()
            if now - t > _ANALYSIS_CACHE_TTL
        ]
        for k in stale:
            del _analysis_cache[k]


def _preload_one(tk: str) -> str | None:
    """Preload analysis for a single ticker (runs in a thread pool worker).

    Called by the /preload endpoint in watchlist_core.py.
    Returns the uppercased ticker on success, None on failure.
    """
    from typing import Any

    try:
        ticker_upper = tk.strip().upper()
        ticker_lower = ticker_upper.lower()
        period = "3M"
        lookback_days = 90

        _init_table()
        engine = get_db_engine()
        feature_names = _resolve_feature_names(ticker_upper)

        analysis: dict[str, Any] = {
            "ticker": ticker_upper,
            "period": period,
        }

        with engine.connect() as conn:
            # Watchlist item
            item = conn.execute(
                text("SELECT * FROM watchlist WHERE ticker = :ticker"),
                {"ticker": ticker_upper},
            ).fetchone()
            if item:
                analysis["watchlist_item"] = _row_to_dict(item)

            # Price history
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
            except Exception:
                analysis["price_history"] = []

            # Options signals
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
            except Exception:
                analysis["options"] = []

            # Regime
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
                        "state": regime[0],
                        "confidence": float(regime[1]) if regime[1] else None,
                        "posture": regime[2],
                        "as_of": str(regime[3]),
                    }
            except Exception:
                analysis["regime"] = None

            # Related features
            try:
                like_patterns = [f"{ticker_lower}%"]
                tk_clean = ticker_lower.lstrip("^").replace("=", "")
                if tk_clean != ticker_lower:
                    like_patterns.append(f"{tk_clean}%")
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

                _ticker_price: float | None = None
                if analysis.get("price_history"):
                    _ticker_price = analysis["price_history"][-1]["value"]

                enriched_feats: list[dict[str, Any]] = []
                for r in feat_rows:
                    fname = r[0]
                    fval = float(r[2]) if r[2] is not None else None
                    interpretation, signal = _interpret_feature(fname, fval, _ticker_price)
                    enriched_feats.append({
                        "name": fname,
                        "display_name": _get_display_name(fname),
                        "family": r[1],
                        "value": fval,
                        "obs_date": str(r[3]),
                        "interpretation": interpretation,
                        "signal": signal,
                    })
                analysis["related_features"] = enriched_feats
            except Exception:
                analysis["related_features"] = []

            # TradingView signals
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
            except Exception:
                analysis["tradingview_signals"] = []

        _set_analysis_cache(ticker_upper, period, analysis)
        return ticker_upper
    except Exception as exc:
        log.debug("Preload failed for {t}: {e}", t=tk, e=str(exc))
        return None
