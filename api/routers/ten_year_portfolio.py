"""Ten-year portfolio query endpoints."""

from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Any

import psycopg2
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from loguru import logger as log
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from api.auth import require_auth
from api.dependencies import get_db_engine
from strategy.ten_year_portfolio import (
    DEFAULT_CHART_UNIVERSE,
    FRONTIER_RAW_HISTORY_TICKERS,
    FRONTIER_THEMATIC_UNIVERSE,
    PROFILES,
    build_weekly_recommendation,
    parse_yf_series_id,
)
from strategy.portfolio_workbook_plan import (
    MAX_UPLOAD_BYTES,
    build_plan_export_workbook,
    build_sanitized_master_plan,
    scan_workbook_bytes,
)


router = APIRouter(
    prefix="/api/v1/ten-year-portfolio",
    tags=["ten-year-portfolio"],
    dependencies=[Depends(require_auth)],
)

EXCEL_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

# Tickers from DEFAULT_CHART_UNIVERSE + QQQ that have a deduped `<ticker>_full`
# entry in feature_registry / resolved_series.  These bypass the expensive
# DISTINCT-ON scan on raw_series (1.9B rows, ~100 re-pulls per date) and hit
# the cheap resolved_series index instead (cost ~20K vs ~3.6M).
# The 11 FRONTIER_RAW_HISTORY_TICKERS are NOT in this set and still use raw_series.
_RESOLVED_TICKER_TO_FEATURE: dict[str, str] = {
    "AAPL":  "aapl_full",
    "AMD":   "amd_full",
    "AMZN":  "amzn_full",
    "AVGO":  "avgo_full",
    "BRK-B": "brk-b_full",
    "CAT":   "cat_full",
    "COST":  "cost_full",
    "GE":    "ge_full",
    "GOOGL": "googl_full",
    "HD":    "hd_full",
    "JPM":   "jpm_full",
    "LLY":   "lly_full",
    "MA":    "ma_full",
    "META":  "meta_full",
    "MSFT":  "msft_full",
    "NFLX":  "nflx_full",
    "NVDA":  "nvda_full",
    "QQQ":   "qqq_full",
    "TSLA":  "tsla_full",
    "V":     "v_full",
}


def _fetch_resolved_rows(
    dsn: str,
    feature_names: list[str],
    lookback_days: int,
) -> list[tuple[Any, ...]]:
    """Query resolved_series for a list of feature names.

    Uses its own psycopg2 connection so it can run concurrently with
    _fetch_raw_rows without sharing a SQLAlchemy connection.
    """
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout='30000'")
        cur.execute(
            """
            SELECT DISTINCT ON (fr.name, rs.obs_date)
                   fr.name, rs.obs_date, rs.value, rs.vintage_date
            FROM feature_registry fr
            JOIN resolved_series rs ON rs.feature_id = fr.id
            WHERE fr.name = ANY(%s)
              AND rs.obs_date >= CURRENT_DATE - make_interval(days => %s)
              AND rs.value > 0
            ORDER BY fr.name, rs.obs_date ASC, rs.vintage_date DESC
            """,
            (feature_names, lookback_days),
        )
        return cur.fetchall()
    finally:
        conn.rollback()
        conn.close()


def _fetch_raw_rows(
    dsn: str,
    series_ids: list[str],
    lookback_days: int,
) -> list[tuple[Any, ...]]:
    """Query raw_series for a list of series_ids.

    Restricted to the 11 FRONTIER_RAW_HISTORY_TICKERS that have no resolved
    equivalent, keeping the scan to ~2M rows instead of ~7M.
    """
    if not series_ids:
        return []
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout='30000'")
        cur.execute(
            """
            SELECT DISTINCT ON (series_id, obs_date)
                   series_id, obs_date, value, pull_timestamp
            FROM raw_series
            WHERE pull_status = 'SUCCESS'
              AND series_id = ANY(%s)
              AND obs_date >= CURRENT_DATE - make_interval(days => %s)
              AND value > 0
            ORDER BY series_id, obs_date ASC, pull_timestamp DESC
            """,
            (series_ids, lookback_days),
        )
        return cur.fetchall()
    finally:
        conn.rollback()
        conn.close()


def _load_price_history(engine: Engine, *, years: int) -> dict[str, list[tuple[date, float]]]:
    """Load deduped Yahoo adjusted-close history for chart candidate universes.

    Performance design
    ------------------
    The original query ran a single DISTINCT ON across 31 series_ids in
    raw_series (~7M rows, each date re-pulled ~30-50 times), producing a sort
    cost of ~3.6M planner units and timing out under the 120s statement_timeout.

    Fix: split into three concurrent queries.

    1. resolved_series (20 tickers) — hits idx_resolved_series_feature_obs with
       a nested-loop index scan; planner cost ~20K, measured ~0.17s.
    2. resolved_series (6 frontier thematic tickers) — same path, already existed.
    3. raw_series (11 FRONTIER_RAW_HISTORY_TICKERS only) — no _full feature
       exists for these; still uses DISTINCT ON but on ~2M rows instead of ~7M.

    Queries 1+2 are merged into a single resolved_series call.  All three run
    concurrently via ThreadPoolExecutor.  Wall time measured ~3.5s vs >120s.
    """
    lookback_days = max(365, int(years * 365.25) + 45)

    # Tickers that have no _full resolved equivalent — must use raw_series.
    raw_only_tickers: list[str] = sorted(FRONTIER_RAW_HISTORY_TICKERS)
    raw_series_ids = [f"YF:{t}:adj_close" for t in raw_only_tickers]

    # All feature names for resolved_series:
    # (a) the 20 main tickers with _full equivalents
    # (b) the 6 frontier thematic tickers that were already using resolved_series
    frontier_thematic_tickers = sorted(set(FRONTIER_THEMATIC_UNIVERSE) - set(raw_only_tickers))
    frontier_feature_to_ticker = {f"{t.lower()}_full": t for t in frontier_thematic_tickers}
    resolved_feature_names: list[str] = (
        list(_RESOLVED_TICKER_TO_FEATURE.values())
        + list(frontier_feature_to_ticker.keys())
    )

    dsn = engine.url.render_as_string(hide_password=False)

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_resolved = pool.submit(
            _fetch_resolved_rows, dsn, resolved_feature_names, lookback_days
        )
        fut_raw = pool.submit(
            _fetch_raw_rows, dsn, raw_series_ids, lookback_days
        )
        resolved_rows = fut_resolved.result()
        raw_rows = fut_raw.result()

    # Build a reversed lookup: feature_name -> ticker for all resolved rows.
    feature_to_ticker: dict[str, str] = {
        **{v: k for k, v in _RESOLVED_TICKER_TO_FEATURE.items()},
        **frontier_feature_to_ticker,
    }

    by_ticker_date: dict[tuple[str, date], tuple[Any, float]] = {}

    # Process raw_series rows (frontier tickers only).
    for row in raw_rows:
        parsed = parse_yf_series_id(row[0])
        if parsed is None:
            continue
        ticker, _ = parsed
        key = (ticker, row[1])
        current = by_ticker_date.get(key)
        candidate = (row[3], float(row[2]))
        if current is None or candidate[0] >= current[0]:
            by_ticker_date[key] = candidate

    # Process resolved_series rows (main 20 + frontier thematic 6).
    for row in resolved_rows:
        ticker = feature_to_ticker.get(row[0])
        if ticker is None:
            continue
        key = (ticker, row[1])
        current = by_ticker_date.get(key)
        candidate = (row[3], float(row[2]))
        if current is None or candidate[0] >= current[0]:
            by_ticker_date[key] = candidate

    history: dict[str, list[tuple[date, float]]] = defaultdict(list)
    for (ticker, obs_date), (_, value) in sorted(by_ticker_date.items()):
        history[ticker].append((obs_date, value))
    return dict(history)


async def _read_private_upload(file: UploadFile) -> bytes:
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Upload is empty.")
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Upload too large. Max is 15MB.")
    return content


def _excel_response(content: bytes, filename: str) -> Response:
    return Response(
        content=content,
        media_type=EXCEL_MEDIA_TYPE,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/profiles")
async def list_profiles() -> dict[str, Any]:
    """Return configured investor profiles."""
    return {
        "status": "ok",
        "profiles": [
            {
                "id": profile.id,
                "label": profile.label,
                "description": profile.description,
                "top_n": profile.top_n,
                "max_position": profile.max_position,
                "cash_pct": profile.cash_pct,
                "min_years": profile.min_years,
                "hold_buffer": profile.hold_buffer,
            }
            for profile in PROFILES.values()
        ],
    }


@router.get("/weekly")
async def weekly_ten_year_portfolio(
    capital: float = Query(default=1_000_000.0, ge=10_000.0, le=100_000_000.0),
    years: int = Query(default=10, ge=3, le=20),
    profile: str | None = Query(default=None),
    engine: Engine = Depends(get_db_engine),
) -> dict[str, Any]:
    """Build weekly buy/hold candidates from 10-year chart quality."""
    try:
        history = _load_price_history(engine, years=years)
        result = build_weekly_recommendation(
            history,
            capital=capital,
            years=years,
            profile_id=profile,
        )
        result["universe"] = {
            **result["universe"],
            "source": "raw_series:yfinance_adjusted_close",
            "frontier_source": "resolved_series:ticker_full",
            "input_universe_size": len(set(DEFAULT_CHART_UNIVERSE)),
            "input_universe": "dad_chart_core_universe",
            "frontier_input_universe_size": len(set(FRONTIER_THEMATIC_UNIVERSE)),
        }
        if result["universe"]["ranked_candidates"] == 0:
            return {
                **result,
                "status": "empty",
                "message": "No eligible Yahoo adjusted-close price history found.",
            }
        return result
    except Exception as exc:
        log.warning("Ten-year portfolio query failed: {e}", e=str(exc))
        return {"status": "error", "error": str(exc)}


@router.post("/workbook/analyze")
async def analyze_private_workbook(
    file: UploadFile = File(...),
    capital: float = Query(default=1_000_000.0, ge=10_000.0, le=100_000_000.0),
    years: int = Query(default=10, ge=3, le=20),
    engine: Engine = Depends(get_db_engine),
) -> dict[str, Any]:
    """Analyze a workbook in memory and return only sanitized planning metadata."""
    try:
        content = await _read_private_upload(file)
        scan = scan_workbook_bytes(file.filename or "uploaded-workbook", content)
        history = _load_price_history(engine, years=years)
        recommendation = build_weekly_recommendation(history, capital=capital, years=years)
        return build_sanitized_master_plan(scan, recommendation)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        log.warning("Private workbook analysis failed: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail="Workbook analysis failed.") from exc


@router.get("/export.xlsx")
async def export_current_model_workbook(
    capital: float = Query(default=1_000_000.0, ge=10_000.0, le=100_000_000.0),
    years: int = Query(default=10, ge=3, le=20),
    engine: Engine = Depends(get_db_engine),
) -> Response:
    """Export the current sanitized model portfolio and candidate boards as Excel."""
    history = _load_price_history(engine, years=years)
    recommendation = build_weekly_recommendation(history, capital=capital, years=years)
    plan = {
        "status": "ok",
        "generated_at": recommendation.get("as_of"),
        "file_type": "grid-model",
        "privacy": {
            "status": "sanitized",
            "raw_holdings_returned": False,
            "raw_account_values_returned": False,
            "raw_sheet_names_returned": False,
            "policy": "Export contains only GRID model allocations and candidate boards.",
        },
        "workbook_summary": {
            "method_signals": [
                {"label": "Long Term Chart", "strength": 1},
                {"label": "Relative Strength", "strength": 1},
                {"label": "Risk", "strength": 1},
                {"label": "Portfolio Rules", "strength": 1},
            ],
            "formula_functions": [],
        },
        "master_plan": {
            "objective": "Run the preloaded $1M 10-year GRID plan and review candidate boards weekly.",
            "steps": [
                {"step": "Run weekly query", "action": "Refresh Dad Chartist and Frontier Infrastructure boards."},
                {"step": "Review Monte Carlo", "action": "Use p10/p50/p90 ranges as risk context, not a guarantee."},
                {"step": "Export packet", "action": "Save this workbook for weekly review."},
            ],
        },
        "candidate_boards": [
            {
                "id": board.get("id"),
                "label": board.get("label"),
                "top_candidates": [
                    {
                        "ticker": row.get("ticker"),
                        "score": row.get("score"),
                        "themes": row.get("themes", []),
                        "years": row.get("years"),
                    }
                    for row in board.get("ranked", [])[:25]
                ],
            }
            for board in recommendation.get("candidate_boards", [])
        ],
    }
    content = build_plan_export_workbook(plan, recommendation)
    return _excel_response(content, "grid-10-year-master-plan.xlsx")


@router.post("/workbook/export.xlsx")
async def export_private_workbook_plan(
    file: UploadFile = File(...),
    capital: float = Query(default=1_000_000.0, ge=10_000.0, le=100_000_000.0),
    years: int = Query(default=10, ge=3, le=20),
    engine: Engine = Depends(get_db_engine),
) -> Response:
    """Analyze a private workbook in memory and export a sanitized Excel plan."""
    try:
        content = await _read_private_upload(file)
        scan = scan_workbook_bytes(file.filename or "uploaded-workbook", content)
        history = _load_price_history(engine, years=years)
        recommendation = build_weekly_recommendation(history, capital=capital, years=years)
        plan = build_sanitized_master_plan(scan, recommendation)
        export = build_plan_export_workbook(plan, recommendation)
        return _excel_response(export, "sanitized-dad-method-master-plan.xlsx")
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        log.warning("Private workbook export failed: {e}", e=str(exc))
        raise HTTPException(status_code=500, detail="Workbook export failed.") from exc
