"""AstroGrid API — expanded celestial intelligence endpoints.

This is the facade router. All endpoints are implemented in focused sub-routers
and included here to preserve the /api/v1/astrogrid/* URL prefix.

Sub-routers:
  astrogrid_helpers.py     — Pydantic models and computation utilities (no endpoints)
  astrogrid_core.py        — /overview, /snapshot, /scorecard, /universe, /interpret
  astrogrid_predictions.py — /predictions, /backtest, /weights, /review, /learning-loop
  astrogrid_celestial.py   — /ephemeris, /correlations, /timeline, /briefing, /compare,
                              /retrograde, /eclipses, /nakshatra, /lunar/calendar, /solar/activity
"""

from __future__ import annotations

from fastapi import APIRouter

from api.routers.astrogrid_core import router as _core_router
from api.routers.astrogrid_predictions import router as _predictions_router
from api.routers.astrogrid_celestial import router as _celestial_router

# Re-export all public names from helpers so existing callers (tests, scripts) still work
from api.routers.astrogrid_helpers import *  # noqa: F401,F403

# Re-export names that tests mock on this module
from api.dependencies import get_astrogrid_store  # noqa: F401
from api.routers.astrogrid_helpers import (  # noqa: F401
    _classify_prediction_scoreability,
    publish_astrogrid_prediction,
)

router = APIRouter(prefix="/api/v1/astrogrid", tags=["astrogrid"])

router.include_router(_core_router)
router.include_router(_predictions_router)
router.include_router(_celestial_router)
