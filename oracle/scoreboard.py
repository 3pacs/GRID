"""Shared Oracle scoreboard helpers used by GRID and AstroGrid."""

from __future__ import annotations

from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


def _coerce_int(value: Any) -> int:
    return int(value or 0)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _prediction_accuracy(hits: int, misses: int, partials: int) -> float:
    scored = hits + misses + partials
    if scored <= 0:
        return 0.0
    return round((hits + (partials * 0.5)) / scored, 4)


def _ticker_filter_sql(tickers: list[str] | None) -> tuple[str, dict[str, Any]]:
    if not tickers:
        return "WHERE dedup_keep = TRUE", {}
    return "WHERE dedup_keep = TRUE AND ticker = ANY(:tickers)", {"tickers": tickers}


def _fetch_overall_stats(engine: Engine, tickers: list[str] | None = None) -> dict[str, Any]:
    where_sql, params = _ticker_filter_sql(tickers)
    today = date.today()
    params["today"] = today
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN verdict = 'hit' THEN 1 ELSE 0 END) AS hits,
                    SUM(CASE WHEN verdict = 'miss' THEN 1 ELSE 0 END) AS misses,
                    SUM(CASE WHEN verdict = 'partial' THEN 1 ELSE 0 END) AS partials,
                    SUM(CASE WHEN verdict = 'pending' THEN 1 ELSE 0 END) AS pending,
                    AVG(CASE WHEN verdict IN ('hit','miss','partial') THEN pnl_pct END) AS avg_pnl,
                    SUM(CASE WHEN verdict IN ('hit','miss','partial') THEN pnl_pct ELSE 0 END) AS total_pnl,
                    SUM(CASE WHEN verdict = 'pending' AND expiry > :today THEN 1 ELSE 0 END) AS active,
                    SUM(CASE WHEN verdict = 'pending' AND expiry <= :today THEN 1 ELSE 0 END) AS awaiting_score,
                    SUM(CASE WHEN verdict = 'no_data' THEN 1 ELSE 0 END) AS unscorable
                FROM oracle_predictions
                {where_sql}
                """
            ),
            params,
        ).fetchone()

    total = _coerce_int(row[0] if row else 0)
    hits = _coerce_int(row[1] if row else 0)
    misses = _coerce_int(row[2] if row else 0)
    partials = _coerce_int(row[3] if row else 0)
    pending = _coerce_int(row[4] if row else 0)
    avg_pnl = _coerce_float(row[5] if row else None)
    total_pnl = _coerce_float(row[6] if row else None) or 0.0
    scored = hits + misses + partials
    active = _coerce_int(row[7] if row else 0)
    awaiting_score = _coerce_int(row[8] if row else 0)
    unscorable = _coerce_int(row[9] if row else 0)

    return {
        "total_predictions": total,
        "scored": scored,
        "pending": pending,
        "hits": hits,
        "misses": misses,
        "partials": partials,
        "active": active,
        "awaiting_score": awaiting_score,
        "unscorable": unscorable,
        # Include unknown verdicts and pending rows without an expiry instead
        # of silently dropping them from the operator's lifecycle totals.
        "other": total - scored - active - awaiting_score - unscorable,
        "lifecycle_as_of": today.isoformat(),
        "mean_signed_return_pct": round(avg_pnl, 2) if avg_pnl is not None else None,
        "accuracy": _prediction_accuracy(hits, misses, partials),
        "avg_pnl": round(avg_pnl, 2) if avg_pnl is not None else 0.0,
        "total_pnl": round(total_pnl, 2),
    }


def _fetch_model_rows(engine: Engine) -> list[dict[str, Any]]:
    """Per-model rows on the same basis as ``overall`` (dedup_keep predictions).

    ``oracle_models.predictions_made/hits/misses/partials/cumulative_pnl`` are
    running counters the scorer increments over every raw prediction,
    duplicates and pre-rescore labels included. On 2026-11-07 they showed
    options_flow 55,964 predictions / +14,776 P&L beside an overall of 24,387
    predictions / -558, so the table contradicted its own headline. Counts and
    P&L now come from ``oracle_predictions`` with the ``overall`` filter;
    ``weight``, ``sharpe`` and ``description`` still come from ``oracle_models``.
    """
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                WITH stats AS (
                    SELECT model_name,
                           COUNT(*) AS made,
                           SUM(CASE WHEN verdict = 'hit' THEN 1 ELSE 0 END) AS hits,
                           SUM(CASE WHEN verdict = 'miss' THEN 1 ELSE 0 END) AS misses,
                           SUM(CASE WHEN verdict = 'partial' THEN 1 ELSE 0 END) AS partials,
                           SUM(CASE WHEN verdict = 'pending' THEN 1 ELSE 0 END) AS pending,
                           SUM(CASE WHEN verdict IN ('hit','miss','partial') THEN pnl_pct ELSE 0 END) AS pnl
                    FROM oracle_predictions
                    WHERE dedup_keep = TRUE
                    GROUP BY model_name
                )
                SELECT om.name, om.weight, COALESCE(s.made, 0),
                       COALESCE(s.hits, 0), COALESCE(s.misses, 0), COALESCE(s.partials, 0),
                       COALESCE(s.pnl, 0), om.sharpe, om.description, COALESCE(s.pending, 0)
                FROM oracle_models om
                LEFT JOIN stats s ON s.model_name = om.name
                ORDER BY om.weight DESC
                """
            )
        ).fetchall()

    models: list[dict[str, Any]] = []
    for row in rows:
        hits = _coerce_int(row[3])
        misses = _coerce_int(row[4])
        partials = _coerce_int(row[5])
        total_scored = hits + misses + partials
        models.append(
            {
                "name": row[0],
                "weight": float(row[1] or 1.0),
                "predictions_made": _coerce_int(row[2]),
                "hits": hits,
                "misses": misses,
                "partials": partials,
                "pending": _coerce_int(row[9]),
                "accuracy": _prediction_accuracy(hits, misses, partials),
                "cumulative_pnl": round(float(row[6] or 0), 2),
                "sharpe": round(float(row[7] or 0), 2),
                "description": row[8] or "",
                "total_scored": total_scored,
                "basis": "oracle_predictions dedup_keep",
            }
        )
    return models


def _fetch_ticker_rows(
    engine: Engine,
    tickers: list[str] | None = None,
    limit: int | None = None,
    ticker_aliases: dict[str, str] | None = None,
    include_calibration: bool = False,
) -> list[dict[str, Any]]:
    where_sql, params = _ticker_filter_sql(tickers)
    order_sql = "ORDER BY ticker" if tickers else "ORDER BY COUNT(*) DESC, ticker"
    limit_sql = ""
    if not tickers and limit:
        limit_sql = " LIMIT :limit"
        params["limit"] = limit

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"""
                SELECT ticker,
                       COUNT(*) AS total,
                       SUM(CASE WHEN verdict = 'hit' THEN 1 ELSE 0 END) AS hits,
                       SUM(CASE WHEN verdict = 'miss' THEN 1 ELSE 0 END) AS misses,
                       SUM(CASE WHEN verdict = 'partial' THEN 1 ELSE 0 END) AS partials,
                       SUM(CASE WHEN verdict = 'pending' THEN 1 ELSE 0 END) AS pending,
                       AVG(CASE WHEN verdict IN ('hit','miss','partial') THEN pnl_pct END) AS avg_pnl,
                       SUM(CASE WHEN verdict IN ('hit','miss','partial') THEN pnl_pct ELSE 0 END) AS total_pnl
                FROM oracle_predictions
                {where_sql}
                GROUP BY ticker
                {order_sql}
                {limit_sql}
                """
            ),
            params,
        ).fetchall()

    calibration_by_ticker: dict[str, dict[str, Any]] = {}
    if include_calibration:
        try:
            from oracle.calibration import compute_calibration

            calibration_targets = [str(row[0]) for row in rows]
            for ticker in calibration_targets:
                report = compute_calibration(engine, ticker=ticker).to_dict()
                if report.get("total_predictions"):
                    calibration_by_ticker[ticker] = report
        except Exception as exc:
            log.debug("Oracle ticker calibration unavailable: {e}", e=str(exc))

    alias_map = ticker_aliases or {}
    items: list[dict[str, Any]] = []
    for row in rows:
        raw_ticker = str(row[0])
        hits = _coerce_int(row[2])
        misses = _coerce_int(row[3])
        partials = _coerce_int(row[4])
        scored = hits + misses + partials
        avg_pnl = _coerce_float(row[6])
        item = {
            "ticker": alias_map.get(raw_ticker, raw_ticker),
            "lookup_ticker": raw_ticker,
            "total": _coerce_int(row[1]),
            "scored": scored,
            "hits": hits,
            "misses": misses,
            "partials": partials,
            "pending": _coerce_int(row[5]),
            "accuracy": _prediction_accuracy(hits, misses, partials),
            "avg_pnl": round(avg_pnl, 2) if avg_pnl is not None else None,
            "total_pnl": round(_coerce_float(row[7]) or 0.0, 2),
        }
        if include_calibration:
            item["calibration"] = calibration_by_ticker.get(raw_ticker)
        items.append(item)
    return items


def build_oracle_ticker_rollup(
    engine: Engine,
    tickers: list[str] | None = None,
    ticker_aliases: dict[str, str] | None = None,
    include_calibration: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    return {
        "overall": _fetch_overall_stats(engine, tickers=tickers),
        "by_ticker": _fetch_ticker_rows(
            engine,
            tickers=tickers,
            limit=limit,
            ticker_aliases=ticker_aliases,
            include_calibration=include_calibration,
        ),
    }


def _fetch_recording_cohorts(engine: Engine) -> dict[str, dict[str, Any]]:
    """Keep newly recorded reference forecasts apart from unqualified history."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT recording_policy, COUNT(*),
                   SUM(CASE WHEN verdict = 'pending' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN verdict = 'no_data' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN verdict IN ('hit', 'partial', 'miss') THEN 1 ELSE 0 END),
                   AVG(CASE WHEN verdict IN ('hit', 'partial', 'miss') THEN pnl_pct END)
            FROM oracle_predictions WHERE dedup_keep = TRUE GROUP BY recording_policy
        """)).fetchall()
    cohorts = {key: {"total": 0, "pending": 0, "unscorable": 0, "scored": 0,
                     "other": 0, "mean_signed_return_pct": None}
               for key in ("oracle_forward_v2", "oracle_external_v2", "legacy")}
    for row in rows:
        total, pending, unavailable, scored = (_coerce_int(row[i]) for i in range(1, 5))
        cohorts[row[0] if row[0] is not None else "legacy"] = {
            "total": total, "pending": pending, "unscorable": unavailable, "scored": scored,
            "other": total - pending - unavailable - scored,
            "mean_signed_return_pct": _coerce_float(row[5]),
        }
    return cohorts


def build_oracle_scoreboard(engine: Engine, ticker_limit: int = 30) -> dict[str, Any]:
    calibration_data = None
    try:
        from oracle.calibration import compute_calibration

        calibration_data = compute_calibration(engine).to_dict()
    except Exception as exc:
        log.warning("Calibration computation failed: {e}", e=str(exc))

    ticker_rollup = build_oracle_ticker_rollup(engine, limit=ticker_limit)
    return {
        "overall": ticker_rollup["overall"],
        "models": _fetch_model_rows(engine),
        "by_ticker": ticker_rollup["by_ticker"],
        "calibration": calibration_data,
        "recording_cohorts": _fetch_recording_cohorts(engine),
    }
