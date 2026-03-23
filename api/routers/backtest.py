"""
GRID API — Backtest & paper trade endpoints.

  POST /api/v1/backtest/run          — Run pitch backtest
  GET  /api/v1/backtest/results      — Get latest results
  GET  /api/v1/backtest/summary      — Get pitch summary
  POST /api/v1/backtest/charts       — Generate charts
  GET  /api/v1/backtest/charts/{name} — Serve a chart image
  POST /api/v1/backtest/paper-trade  — Create paper trade snapshot
  GET  /api/v1/backtest/paper-trades — List paper trade snapshots
  GET  /api/v1/backtest/paper-trades/{filename} — Get specific snapshot
  POST /api/v1/backtest/paper-trade/score — Score expired predictions
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from loguru import logger as log
from pydantic import BaseModel

from api.auth import require_auth

router = APIRouter(
    prefix="/api/v1/backtest",
    tags=["backtest"],
    dependencies=[Depends(require_auth)],
)

_CHART_DIR = Path(__file__).parent.parent.parent / "outputs" / "backtest" / "charts"


class BacktestRequest(BaseModel):
    start_date: str = "2015-01-01"
    initial_capital: float = 100_000
    cost_bps: float = 10.0


@router.post("/run")
async def run_backtest(req: BacktestRequest) -> dict[str, Any]:
    """Run the full pitch backtest."""
    from backtest.engine import PitchBacktester

    bt = PitchBacktester()
    try:
        start = date.fromisoformat(req.start_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid start_date format")

    result = bt.run_full_backtest(
        start_date=start,
        initial_capital=req.initial_capital,
        cost_bps=req.cost_bps,
    )

    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])

    # Return summary instead of full result (equity curve data is huge)
    return {
        "status": "complete",
        "period": result.get("period"),
        "final_value": result.get("final_value"),
        "grid_metrics": result.get("grid_metrics"),
        "benchmark_metrics": result.get("benchmark_metrics"),
        "regime_stats": result.get("regime_stats"),
        "position_sizing": result.get("position_sizing"),
        "total_transitions": result.get("total_transitions"),
    }


@router.get("/results")
async def get_results() -> dict[str, Any]:
    """Get latest full backtest results (includes equity curve data)."""
    from backtest.engine import PitchBacktester

    bt = PitchBacktester()
    result = bt.get_latest_results()
    if not result:
        raise HTTPException(status_code=404, detail="No backtest results. Run /run first.")
    return result


@router.get("/summary")
async def get_summary() -> dict[str, Any]:
    """Get pitch-ready summary of latest backtest."""
    from backtest.engine import PitchBacktester

    bt = PitchBacktester()
    summary = bt.get_summary()
    if not summary:
        raise HTTPException(status_code=404, detail="No backtest results. Run /run first.")
    return summary


@router.post("/charts")
async def generate_charts() -> dict[str, Any]:
    """Generate all pitch charts from latest backtest results."""
    from backtest.charts import generate_all_charts

    charts = generate_all_charts()
    if "error" in charts:
        raise HTTPException(status_code=500, detail=charts["error"])
    return {"charts": {k: v for k, v in charts.items() if v}}


@router.get("/charts/{name}")
async def get_chart(name: str) -> FileResponse:
    """Serve a generated chart image."""
    # Sanitize: allow only alphanumeric, hyphens, underscores, and dots
    safe_name = name.replace("/", "").replace("..", "").replace("\\", "")
    if not safe_name.endswith(".png"):
        safe_name += ".png"

    filepath = (_CHART_DIR / safe_name).resolve()
    # Ensure resolved path is still inside _CHART_DIR
    if not str(filepath).startswith(str(_CHART_DIR.resolve())):
        raise HTTPException(status_code=400, detail="Invalid chart name")
    if not filepath.exists():
        raise HTTPException(status_code=404, detail=f"Chart '{name}' not found")

    return FileResponse(filepath, media_type="image/png")


@router.post("/paper-trade")
async def create_paper_trade() -> dict[str, Any]:
    """Create a timestamped paper trade snapshot."""
    from backtest.paper_trade import PaperTradeTracker

    tracker = PaperTradeTracker()
    snapshot = tracker.create_snapshot()
    if "error" in snapshot:
        raise HTTPException(status_code=500, detail=snapshot["error"])
    return snapshot


@router.get("/paper-trades")
async def list_paper_trades() -> dict[str, Any]:
    """List all paper trade snapshots."""
    from backtest.paper_trade import PaperTradeTracker

    tracker = PaperTradeTracker()
    snapshots = tracker.list_snapshots()
    return {"snapshots": snapshots, "total": len(snapshots)}


@router.get("/paper-trades/{filename}")
async def get_paper_trade(filename: str) -> dict[str, Any]:
    """Get a specific paper trade snapshot."""
    from backtest.paper_trade import PaperTradeTracker

    tracker = PaperTradeTracker()
    snapshot = tracker.get_snapshot(filename)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return snapshot


@router.post("/paper-trade/score")
async def score_predictions() -> dict[str, Any]:
    """Score all expired paper trade predictions."""
    from backtest.paper_trade import PaperTradeTracker

    tracker = PaperTradeTracker()
    scored = tracker.score_predictions()
    return {"scored": scored, "total": len(scored)}
