"""AstroGrid standalone FastAPI app.

This app serves AstroGrid routes and static assets without mounting the rest of
GRID's frontend or router surface. Shared data still comes from the same DB and
published contracts until AstroGrid is fully extracted.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from astrogrid_api.auth import router as astrogrid_auth_router
from astrogrid_api.astrogrid import router as astrogrid_router
from astrogrid_api.dependencies import get_astrogrid_store
from astrogrid_api.observability import flush_langfuse, get_tracer

app = FastAPI(title="AstroGrid API", version="0.1.0")
app.include_router(astrogrid_auth_router)
app.include_router(astrogrid_router)

# Initialize Langfuse tracer at import time so the first request doesn't pay
# the connect cost. No-op when LANGFUSE_* keys are absent.
get_tracer()
app.router.on_shutdown.append(flush_langfuse)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "service": "astrogrid-api"}


@app.get("/readyz")
async def readyz() -> dict[str, object]:
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

_astrogrid_web = Path(__file__).resolve().parents[1] / "astrogrid_web"
if _astrogrid_web.exists():
    app.mount("/astrogrid", StaticFiles(directory=str(_astrogrid_web), html=True), name="astrogrid")
