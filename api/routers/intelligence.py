"""Cross-reference intelligence endpoints — lie detector for government statistics.

This is the facade router. All endpoints are implemented in focused sub-routers
and included here to preserve the /api/v1/intelligence/* URL prefix.

Sub-routers:
  intelligence_actors.py   — Actor network, post-mortems, trends
  intelligence_risk.py     — Risk map, dashboard, globe
  intelligence_thesis.py   — Thesis, sleuth/leads, market diary
  intelligence_news.py     — News, events, patterns
  intelligence_govflow.py  — Gov contracts, dollar flows, legislation
  intelligence_forensics.py — Forensics, causation, influence, export controls
  intelligence_companies.py — Company analyzer, deep graph, institutional map
  intelligence_deepdive.py  — Levers, deep dive, expectations
"""

from __future__ import annotations

from fastapi import APIRouter

from api.routers.intelligence_actors import router as _actors_router
from api.routers.intelligence_risk import router as _risk_router
from api.routers.intelligence_thesis import router as _thesis_router
from api.routers.intelligence_news import router as _news_router
from api.routers.intelligence_govflow import router as _govflow_router
from api.routers.intelligence_forensics import router as _forensics_router
from api.routers.intelligence_companies import router as _companies_router
from api.routers.intelligence_deepdive import router as _deepdive_router

router = APIRouter(prefix="/api/v1/intelligence", tags=["intelligence"])

router.include_router(_actors_router)
router.include_router(_risk_router)
router.include_router(_thesis_router)
router.include_router(_news_router)
router.include_router(_govflow_router)
router.include_router(_forensics_router)
router.include_router(_companies_router)
router.include_router(_deepdive_router)
