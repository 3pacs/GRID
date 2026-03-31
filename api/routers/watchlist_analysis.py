"""Watchlist sub-router: per-ticker technical analysis endpoint."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.routers.watchlist_helpers import (
    _cache_price_to_db,
    _fetch_live_price,
    _get_analysis_cached,
    _get_display_name,
    _init_table,
    _interpret_feature,
    _resolve_feature_names,
    _row_to_dict,
    _set_analysis_cache,
)

router = APIRouter(tags=["watchlist"])


@router.get("/{ticker}/analysis")
async def get_ticker_analysis(
    ticker: str,
    period: str = Query(default="3M", pattern=r"^(1W|1M|3M|6M|1Y)$"),
    _token: str = Depends(require_auth),
) -> dict:
    """Comprehensive analysis page for a watchlist ticker.

    Returns price history, related features with z-scores, options signals,
    regime context, and TradingView webhook signals — all in one call.

    Results are cached for 5 minutes. Cached data is returned instantly;
    a background refresh is triggered when the cache is stale.

    Query params:
        period: 1W | 1M | 3M | 6M | 1Y (default 3M) — controls price_history window.
    """
    ticker_upper = ticker.strip().upper()

    # Return cached data instantly if fresh
    cached = _get_analysis_cached(ticker_upper, period)
    if cached is not None:
        return {**cached, "_cached": True}

    _init_table()
    engine = get_db_engine()
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
                    # Cache latest price to DB for future fast lookups
                    if rows:
                        from datetime import date as _date
                        _cache_price_to_db(
                            engine, ticker_upper,
                            rows[-1]["value"],
                            rows[-1]["date"],
                        )
            except Exception as exc:
                log.debug("yfinance fallback for {t}: {e}", t=ticker_upper, e=str(exc))

            # Final fallback: single live price point
            if not analysis["price_history"]:
                live = _fetch_live_price(ticker_upper)
                if live:
                    analysis["live_price"] = live
                    analysis["price_source"] = "live"
                    from datetime import date as _date
                    _cache_price_to_db(engine, ticker_upper, live["price"], _date.today())

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
            # Derive the current ticker price for interpretation context
            _ticker_price: float | None = None
            if analysis.get("price_history"):
                _ticker_price = analysis["price_history"][-1]["value"]
            elif analysis.get("live_price"):
                _ticker_price = analysis["live_price"].get("price")

            enriched: list[dict[str, Any]] = []
            for r in feat_rows:
                fname = r[0]
                fval = float(r[2]) if r[2] is not None else None
                interpretation, signal = _interpret_feature(fname, fval, _ticker_price)
                enriched.append({
                    "name": fname,
                    "display_name": _get_display_name(fname),
                    "family": r[1],
                    "value": fval,
                    "obs_date": str(r[3]),
                    "interpretation": interpretation,
                    "signal": signal,
                })
            analysis["related_features"] = enriched
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

    # Cache for subsequent requests
    _set_analysis_cache(ticker_upper, period, analysis)

    return analysis
