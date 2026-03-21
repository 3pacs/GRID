"""
GRID API — TradingAgents router.

Endpoints for triggering agent runs, viewing deliberation results,
and checking agent system status.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
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


def _get_runner():
    """Lazy-init the AgentRunner (avoids import at module level)."""
    from db import get_engine
    from agents.runner import AgentRunner
    return AgentRunner(get_engine())


@router.get("/status")
async def agent_status() -> dict[str, Any]:
    """Check whether agents are enabled and which LLM is configured."""
    tradingagents_installed = False
    try:
        import tradingagents  # noqa: F401
        tradingagents_installed = True
    except ImportError:
        pass

    return {
        "enabled": settings.AGENTS_ENABLED,
        "llm_provider": settings.AGENTS_LLM_PROVIDER,
        "llm_model": settings.AGENTS_LLM_MODEL,
        "default_ticker": settings.AGENTS_DEFAULT_TICKER,
        "debate_rounds": settings.AGENTS_DEBATE_ROUNDS,
        "tradingagents_installed": tradingagents_installed,
    }


@router.post("/run")
async def trigger_run(req: RunRequest) -> dict[str, Any]:
    """Trigger a new TradingAgents deliberation run."""
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
