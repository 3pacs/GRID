"""Watchlist API — facade router.

All endpoints are implemented in focused sub-routers and included here
to preserve the /api/v1/watchlist/* URL prefix.

Sub-routers:
  watchlist_helpers.py   — shared utilities and in-memory caches (no endpoints)
  watchlist_core.py      — list, refresh-prices, prices, portfolio, enriched,
                           search, add, preload, delete
  watchlist_analysis.py  — /{ticker}/analysis
  watchlist_overview.py  — /{ticker}/overview, /{ticker}/edge

Re-exports for external callers that import from this module directly:
  _batch_fetch_prices    — api.routers.astrogrid_core
  _cache_price_to_db     — api.routers.astrogrid_core
  _resolve_feature_names — api.routers.astrogrid_helpers
"""

from __future__ import annotations

from fastapi import APIRouter

from api.routers.watchlist_core import router as _core_router
from api.routers.watchlist_analysis import router as _analysis_router
from api.routers.watchlist_overview import router as _overview_router

# Re-export shared helpers so existing callers importing from this module
# continue to work without changes.
from api.routers.watchlist_helpers import (  # noqa: F401
    _batch_fetch_prices,
    _cache_price_to_db,
    _resolve_feature_names,
)

router = APIRouter(prefix="/api/v1/watchlist", tags=["watchlist"])

router.include_router(_core_router)
router.include_router(_analysis_router)
router.include_router(_overview_router)
