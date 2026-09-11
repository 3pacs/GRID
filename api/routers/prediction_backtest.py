"""
GRID API — Prediction Market Backtesting endpoints.

  GET  /api/v1/pm-backtest/strategies     — List available strategies
  GET  /api/v1/pm-backtest/markets        — Search available markets
  POST /api/v1/pm-backtest/run            — Run a hypothesis backtest
  GET  /api/v1/pm-backtest/stats          — Dataset statistics
  POST /api/v1/pm-backtest/export         — Export trades to Parquet
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from loguru import logger as log
from pydantic import BaseModel

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(
    prefix="/api/v1/pm-backtest",
    tags=["prediction-market-backtest"],
    dependencies=[Depends(require_auth)],
)


class HypothesisRequest(BaseModel):
    name: str
    market_filter: str
    strategy: str
    params: dict[str, Any] | None = None
    start_date: str | None = None
    end_date: str | None = None
    position_size: float = 100.0
    description: str = ""


class ExportRequest(BaseModel):
    platform: str = "kalshi"
    market_filter: str | None = None


@router.get("/strategies")
def list_strategies():
    """List available prediction market backtest strategies."""
    from trading.prediction_backtest import list_strategies
    return {"strategies": list_strategies()}


@router.get("/markets")
def search_markets(
    platform: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    engine=Depends(get_db_engine),
):
    """Search available prediction markets for backtesting."""
    from trading.prediction_backtest import list_available_markets
    df = list_available_markets(engine, platform=platform, search=search, limit=limit)
    return {"markets": df.to_dict(orient="records"), "total": len(df)}


@router.post("/run")
def run_hypothesis(req: HypothesisRequest, engine=Depends(get_db_engine)):
    """Run a prediction market hypothesis backtest."""
    from trading.prediction_backtest import run_hypothesis as _run

    try:
        result = _run(
            engine=engine,
            name=req.name,
            market_filter=req.market_filter,
            strategy=req.strategy,
            params=req.params,
            start_date=req.start_date,
            end_date=req.end_date,
            position_size=req.position_size,
            description=req.description,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        log.error("Hypothesis backtest failed: {e}", e=exc)
        raise HTTPException(status_code=500, detail="Backtest failed")

    return {
        "name": result.name,
        "strategy": result.strategy,
        "market_filter": result.market_filter,
        "params": result.params,
        "period": {"start": result.start_date, "end": result.end_date},
        "metrics": {
            "total_trades": result.total_trades,
            "winning_trades": result.winning_trades,
            "losing_trades": result.losing_trades,
            "win_rate": result.win_rate,
            "total_pnl": result.total_pnl,
            "sharpe_ratio": result.sharpe_ratio,
            "max_drawdown": result.max_drawdown,
        },
        "trade_log": result.trade_log[:100],  # Cap response size
    }


@router.get("/stats")
def dataset_stats(engine=Depends(get_db_engine)):
    """Get prediction market dataset statistics."""
    from sqlalchemy import text

    # Static, allowlisted per-table count queries. Never build SQL by
    # interpolating a table name into an f-string — see
    # .claude/rules/security.md.
    _COUNT_QUERIES = {
        "prediction_market_markets": text(
            "SELECT COUNT(*) FROM prediction_market_markets"
        ),
        "prediction_market_trades": text(
            "SELECT COUNT(*) FROM prediction_market_trades"
        ),
    }

    stats = {}
    with engine.connect() as conn:
        for table, query in _COUNT_QUERIES.items():
            try:
                row = conn.execute(query).scalar()
                stats[table] = row
            except Exception:
                stats[table] = 0

        # Platform breakdown
        try:
            rows = conn.execute(text(
                "SELECT platform, COUNT(*) FROM prediction_market_markets "
                "GROUP BY platform"
            )).fetchall()
            stats["markets_by_platform"] = {r[0]: r[1] for r in rows}
        except Exception:
            stats["markets_by_platform"] = {}

        try:
            rows = conn.execute(text(
                "SELECT platform, COUNT(*) FROM prediction_market_trades "
                "GROUP BY platform"
            )).fetchall()
            stats["trades_by_platform"] = {r[0]: r[1] for r in rows}
        except Exception:
            stats["trades_by_platform"] = {}

        # Date range
        try:
            row = conn.execute(text(
                "SELECT MIN(trade_timestamp), MAX(trade_timestamp) "
                "FROM prediction_market_trades"
            )).fetchone()
            if row:
                stats["date_range"] = {
                    "earliest": str(row[0]) if row[0] else None,
                    "latest": str(row[1]) if row[1] else None,
                }
        except Exception:
            stats["date_range"] = {}

    return stats


@router.post("/export")
def export_trades(req: ExportRequest, engine=Depends(get_db_engine)):
    """Export prediction market trades to Parquet for external backtester."""
    from trading.prediction_backtest import (
        export_kalshi_trades,
        export_markets,
        export_polymarket_trades,
    )

    try:
        if req.platform == "kalshi":
            trades_path = export_kalshi_trades(engine, market_filter=req.market_filter)
        elif req.platform == "polymarket":
            trades_path = export_polymarket_trades(engine, market_filter=req.market_filter)
        else:
            raise HTTPException(status_code=400, detail="Platform must be 'kalshi' or 'polymarket'")

        markets_path = export_markets(engine, platform=req.platform)

        return {
            "status": "SUCCESS",
            "trades_file": str(trades_path),
            "markets_file": str(markets_path),
        }
    except Exception as exc:
        log.error("Export failed: {e}", e=exc)
        raise HTTPException(status_code=500, detail="Export failed")
