"""
GRID API — TradingAgents router.

Endpoints for triggering agent runs, viewing deliberation results,
backtesting, schedule management, and checking agent system status.
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from loguru import logger as log
from pydantic import BaseModel

from api.auth import require_auth
from config import settings

router = APIRouter(
    prefix="/api/v1/agents",
    tags=["agents"],
    dependencies=[Depends(require_auth)],
)


class RunRequest(BaseModel):
    ticker: str | None = None
    as_of_date: date | None = None


class BacktestRequest(BaseModel):
    ticker: str | None = None
    days_back: int = 90


def _get_runner():
    """Lazy-init the AgentRunner (avoids import at module level)."""
    from db import get_engine
    from agents.runner import AgentRunner
    return AgentRunner(get_engine())


def _get_backtester():
    from db import get_engine
    from agents.backtest import AgentBacktester
    return AgentBacktester(get_engine())


def _run_in_background(ticker: str | None, as_of_date: date | None) -> None:
    """Execute an agent run synchronously (called from BackgroundTasks)."""
    runner = _get_runner()
    runner.run(ticker=ticker, as_of_date=as_of_date)


@router.get("/status")
async def agent_status() -> dict[str, Any]:
    """Check whether agents are enabled and which LLM is configured."""
    tradingagents_installed = False
    try:
        import tradingagents  # noqa: F401
        tradingagents_installed = True
    except ImportError:
        pass

    schedule_info = {}
    try:
        from agents.scheduler import get_schedule_status
        schedule_info = get_schedule_status()
    except Exception as e:
        log.warning("Agents: schedule status unavailable: {e}", e=str(e))

    return {
        "enabled": settings.AGENTS_ENABLED,
        "llm_provider": settings.AGENTS_LLM_PROVIDER,
        "llm_model": settings.AGENTS_LLM_MODEL,
        "default_ticker": settings.AGENTS_DEFAULT_TICKER,
        "debate_rounds": settings.AGENTS_DEBATE_ROUNDS,
        "tradingagents_installed": tradingagents_installed,
        "schedule": schedule_info,
    }


@router.post("/run")
async def trigger_run(req: RunRequest, background_tasks: BackgroundTasks) -> dict[str, Any]:
    """Trigger a new TradingAgents deliberation run.

    The run executes in a background thread so the API responds immediately.
    Progress updates are pushed via WebSocket (type: agent_progress).
    The completed result is broadcast as agent_run_complete.
    """
    if not settings.AGENTS_ENABLED:
        raise HTTPException(
            status_code=503,
            detail="TradingAgents integration is disabled. Set AGENTS_ENABLED=true.",
        )

    background_tasks.add_task(_run_in_background, req.ticker, req.as_of_date)

    return {
        "status": "started",
        "ticker": req.ticker or settings.AGENTS_DEFAULT_TICKER,
        "as_of_date": (req.as_of_date or date.today()).isoformat(),
        "message": "Agent run started. Watch WebSocket for progress updates.",
    }


@router.post("/run/sync")
async def trigger_run_sync(req: RunRequest) -> dict[str, Any]:
    """Trigger and wait for a TradingAgents run (blocking)."""
    if not settings.AGENTS_ENABLED:
        raise HTTPException(
            status_code=503,
            detail="TradingAgents integration is disabled. Set AGENTS_ENABLED=true.",
        )

    runner = _get_runner()
    result = runner.run(ticker=req.ticker, as_of_date=req.as_of_date)

    if result.get("error"):
        log.warning("Agent run completed with error: {e}", e=result["error"])

    return result


@router.get("/runs")
async def list_runs(
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """List recent agent runs."""
    runner = _get_runner()
    return runner.get_runs(limit=limit)


@router.get("/runs/{run_id}")
async def get_run(run_id: int) -> dict[str, Any]:
    """Get full details of a specific agent run."""
    runner = _get_runner()
    result = runner.get_run(run_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Agent run not found")
    return result


# --- Backtesting ---

@router.post("/backtest")
async def run_backtest(req: BacktestRequest) -> dict[str, Any]:
    """Backtest agent decisions against journal outcomes."""
    bt = _get_backtester()
    return bt.run_backtest(ticker=req.ticker, days_back=req.days_back)


@router.get("/backtest/summary")
async def backtest_summary(
    days_back: int = Query(default=90, ge=1, le=365),
) -> dict[str, Any]:
    """Quick summary comparing agent vs GRID performance."""
    bt = _get_backtester()
    return bt.get_comparison_summary(days_back=days_back)


# --- Schedule ---

@router.get("/schedule")
async def get_schedule() -> dict[str, Any]:
    """Get agent schedule status."""
    try:
        from agents.scheduler import get_schedule_status
        return get_schedule_status()
    except Exception as exc:
        return {"error": str(exc)}


@router.post("/schedule/start")
async def start_schedule() -> dict[str, Any]:
    """Start the agent scheduler."""
    if not settings.AGENTS_ENABLED:
        raise HTTPException(status_code=503, detail="Agents disabled")

    from agents.scheduler import start_agent_scheduler, get_schedule_status
    start_agent_scheduler()
    return get_schedule_status()


@router.post("/schedule/stop")
async def stop_schedule() -> dict[str, Any]:
    """Stop the agent scheduler."""
    from agents.scheduler import stop_agent_scheduler, get_schedule_status
    stop_agent_scheduler()
    return get_schedule_status()
