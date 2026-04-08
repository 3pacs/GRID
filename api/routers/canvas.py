"""Canvas API — facade router.

All endpoints are implemented in focused sub-routers and included here
to preserve the /api/v1/canvas/* URL prefix.

Sub-routers:
  canvas_core.py    — board CRUD (list, create, get, update, delete)
  canvas_graph.py   — node + edge CRUD, bulk graph save
  canvas_expand.py  — graph expansion (expand network, path, suggest connections)
  canvas_llm.py     — LLM-powered intelligence (explain connection)
  canvas_predict.py — convert canvas investigation to scored prediction
"""

from __future__ import annotations

from fastapi import APIRouter

from api.routers.canvas_core import router as _core_router
from api.routers.canvas_expand import router as _expand_router
from api.routers.canvas_graph import router as _graph_router
from api.routers.canvas_llm import router as _llm_router
from api.routers.canvas_predict import router as _predict_router

router = APIRouter(prefix="/api/v1/canvas", tags=["canvas"])

router.include_router(_core_router)
router.include_router(_graph_router)
router.include_router(_expand_router)
router.include_router(_llm_router)
router.include_router(_predict_router)
