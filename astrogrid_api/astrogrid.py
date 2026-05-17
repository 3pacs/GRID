"""AstroGrid facade router and compatibility export surface.

The standalone AstroGrid app owns the canonical route and helper
implementation. Legacy import sites can keep importing from
``astrogrid_api.astrogrid`` while the runtime stays centralized in the
focused submodules.
"""

from __future__ import annotations

from fastapi import APIRouter

from astrogrid_api import astrogrid_helpers as _astrogrid_helpers
from astrogrid_api.astrogrid_core import router as _core_router
from astrogrid_api.astrogrid_core import (
    get_overview,
    get_scoreable_universe,
    get_scorecard,
    get_snapshot,
    interpret_snapshot,
)
from astrogrid_api.astrogrid_predictions import router as _predictions_router
from astrogrid_api.astrogrid_predictions import (
    ask_guru,
    approve_weight_proposal,
    create_prediction,
    generate_review_run,
    get_backtest_results,
    get_backtest_summary,
    get_current_weights,
    get_latest_predictions,
    get_latest_review,
    get_postmortems,
    get_prediction_detail,
    get_prediction_scoreboard,
    get_weight_proposals,
    reject_weight_proposal,
    run_backtest,
    run_learning_loop,
    score_predictions,
)
from astrogrid_api.astrogrid_celestial import router as _celestial_router

# Re-export all public names from helpers so existing callers (tests, scripts) still work
from astrogrid_api.astrogrid_helpers import *  # noqa: F401,F403

# Re-export names that tests mock on this module
from astrogrid_api.dependencies import get_astrogrid_store  # noqa: F401
from astrogrid_api.astrogrid_helpers import (  # noqa: F401
    _classify_prediction_scoreability,
    publish_astrogrid_prediction,
)

__all__ = [
    *_astrogrid_helpers.__all__,
    "router",
    "astrogrid_health",
    "astrogrid_ready",
    "get_overview",
    "get_snapshot",
    "get_scorecard",
    "get_scoreable_universe",
    "interpret_snapshot",
    "create_prediction",
    "ask_guru",
    "get_latest_predictions",
    "get_postmortems",
    "score_predictions",
    "get_prediction_scoreboard",
    "run_backtest",
    "get_backtest_summary",
    "get_backtest_results",
    "get_current_weights",
    "generate_review_run",
    "run_learning_loop",
    "get_latest_review",
    "get_weight_proposals",
    "approve_weight_proposal",
    "reject_weight_proposal",
    "get_prediction_detail",
    "get_astrogrid_store",
    "_classify_prediction_scoreability",
    "publish_astrogrid_prediction",
]

router = APIRouter(prefix="/api/v1/astrogrid", tags=["astrogrid"])


@router.get("/health")
async def astrogrid_health() -> dict[str, str]:
    return {"status": "ok", "service": "astrogrid-api"}


@router.get("/ready")
async def astrogrid_ready() -> dict[str, object]:
    store = get_astrogrid_store()
    latest_review = store.get_latest_review() or {}
    backtest_summary = store.get_backtest_summary(limit=3)
    latest_by_variant = backtest_summary.get("latest_by_variant") or {}
    latest_backtest = next(iter(latest_by_variant.values()), {}) if isinstance(latest_by_variant, dict) else {}
    latest_backtest_summary = latest_backtest.get("summary") if isinstance(latest_backtest, dict) else {}
    latest_scoreboard = store.build_prediction_scoreboard()
    return {
        "status": "ready",
        "service": "astrogrid-api",
        "latest_successful_review_at": latest_review.get("created_at"),
        "latest_scoring_summary": latest_scoreboard.get("overall") if isinstance(latest_scoreboard, dict) else {},
        "latest_backtest_summary": latest_backtest_summary or {},
        "latest_review_summary": latest_review.get("review") if isinstance(latest_review.get("review"), dict) else {},
        "latest_weight_proposal": latest_review.get("proposal") if isinstance(latest_review.get("proposal"), dict) else {},
    }

router.include_router(_core_router)
router.include_router(_predictions_router)
router.include_router(_celestial_router)
