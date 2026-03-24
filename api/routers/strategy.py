"""Strategy overlay endpoints — regime-independent strategy assignments."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status
from loguru import logger as log
from pydantic import BaseModel, Field

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(prefix="/api/v1/strategy", tags=["strategy"])


# -- Schemas --

class StrategyResponse(BaseModel):
    id: int | None = None
    regime_state: str
    name: str
    posture: str
    allocation: str = ""
    risk_level: str = "Medium"
    action: str = ""
    rationale: str = ""
    assigned_at: str = ""
    active: bool = True
    source: str = "default"


class StrategyAssignRequest(BaseModel):
    regime_state: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=200)
    posture: str = Field(..., min_length=1, max_length=100)
    allocation: str = Field(default="", max_length=500)
    risk_level: str = Field(default="Medium", max_length=50)
    action: str = Field(default="", max_length=500)
    rationale: str = Field(default="", max_length=1000)


def _get_strategy_engine() -> Any:
    """Lazy-load the strategy engine singleton."""
    from strategy.engine import StrategyEngine

    engine = get_db_engine()
    # Module-level cache to avoid recreating on every request
    if not hasattr(_get_strategy_engine, "_instance"):
        _get_strategy_engine._instance = StrategyEngine(engine)
    return _get_strategy_engine._instance


@router.get("/active", response_model=list[StrategyResponse])
async def get_active_strategies(
    _token: str = Depends(require_auth),
) -> list[dict[str, Any]]:
    """Return all active strategies (DB overrides + defaults for missing regimes)."""
    se = _get_strategy_engine()
    return se.get_active_strategies()


@router.get("/for-regime/{regime_state}", response_model=StrategyResponse | None)
async def get_strategy_for_regime(
    regime_state: str,
    _token: str = Depends(require_auth),
) -> dict[str, Any] | None:
    """Return the active strategy for a given regime state."""
    regime_state = regime_state.upper().strip()
    se = _get_strategy_engine()
    result = se.get_strategy_for_regime(regime_state)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No strategy found for regime '{regime_state}'",
        )
    return result


@router.post("/assign", response_model=StrategyResponse, status_code=status.HTTP_201_CREATED)
async def assign_strategy(
    body: StrategyAssignRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Assign or update a strategy for a regime state. Requires authentication."""
    se = _get_strategy_engine()
    log.info(
        "Strategy assignment requested: {name} for {regime}",
        name=body.name,
        regime=body.regime_state,
    )
    try:
        result = se.assign_strategy(
            regime_state=body.regime_state.upper().strip(),
            name=body.name,
            posture=body.posture,
            allocation=body.allocation,
            risk_level=body.risk_level,
            action=body.action,
            rationale=body.rationale,
        )
        return result
    except Exception as exc:
        log.error("Strategy assignment failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to assign strategy: {exc}",
        ) from exc
