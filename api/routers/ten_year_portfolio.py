"""Ten-year portfolio query endpoints."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any

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


def _load_price_history(engine: Engine, *, years: int) -> dict[str, list[tuple[date, float]]]:
    """Load deduped Yahoo adjusted-close history for chart candidate universes."""
    lookback_days = max(365, int(years * 365.25) + 45)
    raw_tickers = sorted({*DEFAULT_CHART_UNIVERSE, *FRONTIER_RAW_HISTORY_TICKERS, "QQQ"})
    series_ids = [f"YF:{ticker}:adj_close" for ticker in raw_tickers]
    resolved_tickers = sorted(set(FRONTIER_THEMATIC_UNIVERSE) - set(raw_tickers))
    feature_to_ticker = {f"{ticker.lower()}_full": ticker for ticker in resolved_tickers}
    with engine.connect() as conn:
        raw_rows = conn.execute(
            text(
                """
                SELECT DISTINCT ON (series_id, obs_date)
                       series_id, obs_date, value, pull_timestamp
                FROM raw_series
                WHERE pull_status = 'SUCCESS'
                  AND series_id IN :series_ids
                  AND obs_date >= CURRENT_DATE - make_interval(days => :lookback_days)
                  AND value > 0
                ORDER BY series_id, obs_date ASC, pull_timestamp DESC
                """
            ).bindparams(bindparam("series_ids", expanding=True)),
            {
                "lookback_days": lookback_days,
                "series_ids": series_ids,
            },
        ).fetchall()
        resolved_rows = conn.execute(
            text(
                """
                SELECT DISTINCT ON (fr.name, rs.obs_date)
                       fr.name, rs.obs_date, rs.value, rs.vintage_date
                FROM feature_registry fr
                JOIN resolved_series rs ON rs.feature_id = fr.id
                WHERE fr.name IN :feature_names
                  AND rs.obs_date >= CURRENT_DATE - make_interval(days => :lookback_days)
                  AND rs.value > 0
                ORDER BY fr.name, rs.obs_date ASC, rs.vintage_date DESC
                """
            ).bindparams(bindparam("feature_names", expanding=True)),
            {
                "lookback_days": lookback_days,
                "feature_names": list(feature_to_ticker),
            },
        ).fetchall()

    by_ticker_date: dict[tuple[str, date], tuple[Any, float]] = {}
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
