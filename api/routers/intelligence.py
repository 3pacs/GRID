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
  intelligence_causation.py — Causation chains
  intelligence_edges.py     — Structural market-edge scanner

Sub-routers are included DEFENSIVELY: a single missing or broken sub-module must
not take down the entire /api/v1/intelligence surface. Previously these were
hard top-level imports, so one missing module (api.lf_helpers, then
intelligence_edges/market_edge_scanner) raised at import time, _load_router in
api/main.py swallowed it (required=False), and EVERY facade route — /news,
/events, /patterns, actors, risk, thesis, … — silently 404'd. Per-sub-router
try/except keeps the healthy routes serving and logs the gap. (2026-05-24)
"""

from __future__ import annotations

from importlib import import_module

from fastapi import APIRouter
from loguru import logger as log

router = APIRouter(prefix="/api/v1/intelligence", tags=["intelligence"])

# Order preserved from the original facade; first match wins on path collisions.
_SUB_ROUTERS = (
    "intelligence_actors",
    "intelligence_risk",
    "intelligence_thesis",
    "intelligence_news",
    "intelligence_govflow",
    "intelligence_forensics",
    "intelligence_companies",
    "intelligence_deepdive",
    "intelligence_causation",
    "intelligence_edges",
)

for _modname in _SUB_ROUTERS:
    try:
        _mod = import_module(f"api.routers.{_modname}")
        router.include_router(_mod.router)
    except Exception as _exc:  # pragma: no cover - degrade gracefully
        log.warning(
            "intelligence facade: sub-router '{m}' unavailable, skipping: {e}",
            m=_modname,
            e=str(_exc),
        )
