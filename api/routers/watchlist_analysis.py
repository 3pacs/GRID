"""Watchlist sub-router: per-ticker technical analysis endpoint."""

from __future__ import annotations

from datetime import date, datetime
import json

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine, get_pit_store
from store.pit import PITStore, validate_source_snapshot_request
from api.routers.watchlist_helpers import (
    _fetch_live_price,
    _get_analysis_cached,
    _get_display_name,
    _guess_asset_type,
    _interpret_feature,
    _resolve_feature_names,
    _row_to_dict,
    _set_analysis_cache,
)

router = APIRouter(tags=["watchlist"])


def _build_watchlist_item(ticker_upper: str, item: Any) -> tuple[dict[str, Any], bool]:
    """Return a watchlist-style item payload for saved or ad hoc ticker views."""
    if item is not None:
        return _row_to_dict(item), True

    display_name = ticker_upper
    try:
        from analysis.market_universe import search_company

        matches = search_company(ticker_upper)
        exact = next(
            (match for match in matches if match.get("ticker", "").upper() == ticker_upper),
            None,
        )
        if exact and exact.get("name"):
            display_name = exact["name"]
    except Exception as exc:
        log.debug("Watchlist item lookup fallback for {t}: {e}", t=ticker_upper, e=str(exc))

    return {
        "ticker": ticker_upper,
        "display_name": display_name,
        "asset_type": _guess_asset_type(ticker_upper),
        "notes": None,
        "added_at": None,
    }, False


def _attach_market_evidence(analysis: dict[str, Any]) -> dict[str, Any]:
    """Display existing response evidence without new reads or qualification."""
    history = analysis.get("price_history")
    rows = (
        [
            {"as_of": row.get("date"), "value": row.get("value")}
            for row in history
            if isinstance(row, dict)
        ]
        if isinstance(history, list)
        else []
    )
    live = analysis.get("live_price")
    if not rows and isinstance(live, dict) and "price" in live:
        rows = [{"as_of": live.get("as_of"), "value": live.get("price")}]
    availability = analysis.get("availability")
    declared = availability.get("price") if isinstance(availability, dict) else None
    negative = declared in ("unavailable", "failed", "stale", "unknown")
    status = declared if negative else "observed" if rows else "unknown"
    reason = (
        "price_section_" + declared
        if negative
        else "existing_scalar_evidence"
        if rows
        else "no_returned_price_rows"
    )
    narrative = analysis.get("briefing")
    # Only a supplied receipt; never fetch or invent a latest briefing.
    receipt = (
        {
            "status": narrative.get("status"),
            "reason": narrative.get("reason"),
            "as_of": narrative.get("as_of"),
        }
        if isinstance(narrative, dict)
        else None
    )
    return {
        **analysis,
        "market_features": {
            "schema": "watchlist-existing-evidence-v1",
            "ticker": analysis.get("ticker"),
            "daily": {
                "status": status,
                "reason": reason,
                "source": analysis.get("price_source"),
                "section_availability": declared,
                "rows": rows,
                "returned_row_count": len(rows),
                "as_of": rows[-1]["as_of"] if rows else None,
                "freshness": {
                    "status": "stale" if declared == "stale" else "unknown",
                    "reason": "declared_price_section_stale"
                    if declared == "stale"
                    else "freshness_policy_not_supplied",
                },
                "served_from_cache": analysis.get("_cached") is True,
                "representation": "existing_scalar_not_qualified_intraday",
            },
            "intraday": {
                "status": "unavailable",
                "reason": "qualified_intraday_result_absent",
                "opening_range": None,
                "trade_vwap": None,
                "hlc3_volume_proxy": None,
            },
            "briefing_receipt": receipt,
        },
    }


def _stored_source_analysis(ticker: str, period: str, request: str | None) -> dict:
    """Stored-only read: never enter legacy cache, price or provider paths."""
    result = _attach_market_evidence({"ticker": ticker, "period": period})
    if request is None:
        snapshot = {
            "schema": "stored-source-snapshot-v1", "as_of": None,
            "start_date": None, "vintage_policy": None, "selections": [],
            "status": "unavailable", "reason": "stored_request_missing",
        }
    else:
        def unique_object(pairs):
            obj = {}
            for key, value in pairs:
                if key in obj:
                    raise ValueError("Duplicate request key")
                obj[key] = value
            return obj

        try:
            if type(request) is not str or len(request.encode("utf-8")) > 16384:
                raise ValueError("Stored request size/type")
            parsed = json.loads(request, object_pairs_hook=unique_object)
            allowed = {"selections", "as_of", "start_date", "vintage_policy"}
            if type(parsed) is not dict or set(parsed) - allowed:
                raise ValueError("Stored request keys")
            if type(parsed.get("as_of")) is not str or type(parsed.get("start_date")) is not str:
                raise ValueError("Explicit cutoff and range required")
            normalized = validate_source_snapshot_request(
                parsed.get("selections"),
                as_of=datetime.fromisoformat(parsed["as_of"]),
                start_date=date.fromisoformat(parsed["start_date"]),
                vintage_policy=parsed.get("vintage_policy", "LATEST_AS_OF"),
            )
            if any(
                item.get("scope") == "ticker" and item.get("ticker_or_cusip") != ticker
                for item in normalized["selections"]
            ):
                raise ValueError("Ticker selection does not match route")
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise HTTPException(status_code=400, detail="invalid_stored_request") from None

        # Pure validation precedes store/engine access. Missing bindings and the
        # unsupported event source can be represented without opening a DB path.
        bound = any(
            item["source_key"] != "quiverquant" and all(
                item.get(key) is not None
                for key in ("series_id", "expected_source_name", "feature_name", "feature_id")
            )
            for item in normalized["selections"]
        )
        try:
            store = get_pit_store() if bound else PITStore(None)
            snapshot = store.get_source_snapshot(**normalized)
            if (snapshot.get("schema") != "stored-source-snapshot-v1"
                or snapshot.get("as_of") != normalized["as_of"].isoformat()
                or snapshot.get("start_date") != normalized["start_date"].isoformat()
                or snapshot.get("vintage_policy") != normalized["vintage_policy"]):
                raise ValueError("Mismatched snapshot request context")
        except Exception:
            # Do not substitute cache/provider/raw values on a failed read.
            snapshot = {
                "schema": "stored-source-snapshot-v1",
                "as_of": normalized["as_of"].isoformat(),
                "start_date": normalized["start_date"].isoformat(),
                "vintage_policy": normalized["vintage_policy"],
                "selections": [], "status": "unavailable", "reason": "read_failed",
            }
    # Keep this context in the existing guarded response envelope: initial and
    # period callbacks already mask/replace market_features as one unit.
    binding = {
        "ticker": ticker, "as_of": None, "start_date": None,
        "vintage_policy": None, "selections": [],
    } if request is None else {
        "ticker": ticker, "as_of": normalized["as_of"].isoformat(),
        "start_date": normalized["start_date"].isoformat(),
        "vintage_policy": normalized["vintage_policy"],
        "selections": normalized["selections"],
    }
    result["market_features"]["stored_source_evidence"] = {
        **snapshot, "ticker": ticker, "request_binding": binding,
    }
    return result


@router.get("/{ticker}/analysis")
def get_ticker_analysis(
    ticker: str,
    period: str = Query(default="3M", pattern=r"^(1W|1M|3M|6M|1Y)$"),
    _token: str = Depends(require_auth),
    stored_only: bool = False,
    stored_request: str | None = Query(default=None),
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
    if stored_only:
        return _stored_source_analysis(ticker_upper, period, stored_request)

    # Return cached data instantly if fresh
    cached = _get_analysis_cached(ticker_upper, period)
    if cached is not None:
        return _attach_market_evidence({**cached, "_cached": True})

    engine = get_db_engine()
    ticker_lower = ticker_upper.lower()

    # Map period string to number of calendar days
    _period_days = {"1W": 7, "1M": 30, "3M": 90, "6M": 180, "1Y": 365}
    lookback_days = _period_days.get(period, 90)

    # Map to yfinance period strings (used for fallback)
    _yf_period_map = {"1W": "1mo", "1M": "1mo", "3M": "3mo", "6M": "6mo", "1Y": "1y"}

    try:
        with engine.connect() as conn:
            item = conn.execute(
                text("SELECT * FROM watchlist WHERE ticker = :ticker"),
                {"ticker": ticker_upper},
            ).fetchone()
        watchlist_status = "available"
    except Exception as exc:
        # An ad hoc ticker does not require the optional saved-watchlist table.
        log.debug("Watchlist item read for {t}: {e}", t=ticker_upper, e=str(exc))
        item = None
        watchlist_status = "unavailable"

    watchlist_item, watchlist_saved = _build_watchlist_item(ticker_upper, item)

    analysis: dict[str, Any] = {
        "ticker": ticker_upper,
        "watchlist_item": watchlist_item,
        "watchlist_saved": watchlist_saved,
        "availability": {"watchlist": watchlist_status},
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
            conn.rollback()
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

        analysis["price_source"] = analysis.get("price_source", "unavailable")
        analysis["availability"]["price"] = (
            "available" if analysis["price_source"] != "unavailable" else "unavailable"
        )

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
            analysis["availability"]["related_features"] = "available"
        except Exception as exc:
            conn.rollback()
            log.debug("Related features for {t}: {e}", t=ticker_upper, e=str(exc))
            analysis["related_features"] = []
            analysis["availability"]["related_features"] = "unavailable"

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
            analysis["availability"]["options"] = "available"
        except Exception as exc:
            conn.rollback()
            log.debug("Options for {t}: {e}", t=ticker_upper, e=str(exc))
            analysis["options"] = []
            analysis["availability"]["options"] = "unavailable"

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
            analysis["availability"]["regime"] = "available"
        except Exception:
            conn.rollback()
            analysis["regime"] = None
            analysis["availability"]["regime"] = "unavailable"

        # ── TradingView webhook signals for this ticker ──
        try:
            tv_rows = conn.execute(
                text(
                    "SELECT rs.pull_timestamp, rs.value, rs.raw_payload "
                    "FROM raw_series rs "
                    "JOIN source_catalog sc ON sc.id = rs.source_id "
                    "WHERE sc.name = 'TradingView' "
                    "AND rs.pull_status = 'SUCCESS' "
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
            analysis["availability"]["tradingview_signals"] = "available"
        except Exception as exc:
            conn.rollback()
            log.debug("TV signals for {t}: {e}", t=ticker_upper, e=str(exc))
            analysis["tradingview_signals"] = []
            analysis["availability"]["tradingview_signals"] = "unavailable"

    # Cache for subsequent requests
    # A partial failure should be retried on the next request, not cached as
    # checked-empty for five minutes.
    if all(status == "available" for status in analysis["availability"].values()):
        _set_analysis_cache(ticker_upper, period, analysis)

    return _attach_market_evidence(analysis)
