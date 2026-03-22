"""
GRID Intelligence API — FastAPI application entry point.

Serves the API at /api/v1/* and the PWA at /.
WebSocket endpoint at /ws for real-time updates.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger as log
from starlette.middleware.base import BaseHTTPMiddleware

from api.auth import router as auth_router, verify_token
from api.routers.config import router as config_router
from api.routers.physics import router as physics_router
from api.routers.workflows import router as workflows_router
from api.routers.discovery import router as discovery_router
from api.routers.journal import router as journal_router
from api.routers.models import router as models_router
from api.routers.regime import router as regime_router
from api.routers.signals import router as signals_router
from api.routers.agents import router as agents_router
from api.routers.backtest import router as backtest_router
from api.routers.ollama import router as ollama_router
from api.routers.system import router as system_router

_environment = os.getenv("ENVIRONMENT", "development")
_start_time = time.time()

app = FastAPI(
    title="GRID Intelligence API",
    version="1.0.0",
    docs_url="/api/docs" if _environment == "development" else None,
    redoc_url=None,
)

# Security headers middleware
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Add security headers to all responses."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        if _environment != "development":
            response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
        return response


app.add_middleware(SecurityHeadersMiddleware)

# CORS — never allow credentials with wildcard origins
allowed_origins = os.getenv("GRID_ALLOWED_ORIGINS", "").split(",")
allowed_origins = [o.strip() for o in allowed_origins if o.strip()]
if _environment == "development":
    allowed_origins = ["http://localhost:5173", "http://localhost:8000", "http://127.0.0.1:5173"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# Include routers
app.include_router(auth_router)
app.include_router(system_router)
app.include_router(regime_router)
app.include_router(signals_router)
app.include_router(journal_router)
app.include_router(models_router)
app.include_router(discovery_router)
app.include_router(config_router)
app.include_router(physics_router)
app.include_router(workflows_router)
app.include_router(agents_router)
app.include_router(ollama_router)
app.include_router(backtest_router)

# WebSocket connections
_ws_clients: set[WebSocket] = set()


async def _broadcast(message: dict) -> None:
    """Send a message to all connected WebSocket clients."""
    data = json.dumps(message)
    disconnected: set[WebSocket] = set()
    for ws in _ws_clients:
        try:
            await ws.send_text(data)
        except Exception:
            disconnected.add(ws)
    _ws_clients -= disconnected


async def _ws_broadcast_loop() -> None:
    """Background loop that pushes updates every 10 seconds."""
    while True:
        await asyncio.sleep(10)
        if not _ws_clients:
            continue

        now = datetime.now(timezone.utc).isoformat()
        try:
            await _broadcast({
                "type": "ping",
                "timestamp": now,
                "data": {"uptime_seconds": round(time.time() - _start_time, 1)},
            })
        except Exception as exc:
            log.debug("WS broadcast error: {e}", e=str(exc))


@app.on_event("startup")
async def startup() -> None:
    """Verify database and start background tasks."""
    log.info("GRID API starting — environment={e}", e=_environment)
    try:
        from db import health_check

        if health_check():
            log.info("Database connection verified")
        else:
            log.warning("Database not available at startup")
    except Exception as exc:
        log.warning("Database check failed: {e}", e=str(exc))

    asyncio.create_task(_ws_broadcast_loop())

    # Register agent progress broadcast and start scheduler
    try:
        from agents.progress import register_broadcast
        register_broadcast(_broadcast, asyncio.get_event_loop())
        log.info("Agent WebSocket progress broadcast registered")
    except Exception as exc:
        log.debug("Agent progress registration skipped: {e}", e=str(exc))

    try:
        from agents.scheduler import start_agent_scheduler
        start_agent_scheduler()
    except Exception as exc:
        log.debug("Agent scheduler start skipped: {e}", e=str(exc))

    log.info("GRID API ready")


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(default=""),
) -> None:
    """WebSocket endpoint for real-time updates."""
    if not token or not verify_token(token):
        await websocket.close(code=4001, reason="Invalid token")
        return

    await websocket.accept()
    _ws_clients.add(websocket)
    log.info("WebSocket client connected (total={n})", n=len(_ws_clients))

    # Send initial state
    try:
        await websocket.send_json({
            "type": "connected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {
                "message": "Connected to GRID Intelligence",
                "uptime_seconds": round(time.time() - _start_time, 1),
            },
        })
    except Exception:
        _ws_clients.discard(websocket)
        return

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(websocket)
        log.info("WebSocket client disconnected (total={n})", n=len(_ws_clients))


# Serve PWA static files — mount AFTER API routes
_pwa_dist = Path(__file__).parent.parent / "pwa_dist"
_pwa_src = Path(__file__).parent.parent / "pwa"

if _pwa_dist.exists():
    app.mount("/assets", StaticFiles(directory=str(_pwa_dist / "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_pwa(full_path: str) -> FileResponse:
        """Serve PWA — return index.html for all non-API paths (SPA routing)."""
        file_path = _pwa_dist / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_pwa_dist / "index.html"))

elif _pwa_src.exists():
    @app.get("/{full_path:path}")
    async def serve_pwa_dev(full_path: str) -> FileResponse:
        """Serve PWA source in development."""
        file_path = _pwa_src / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_pwa_src / "index.html"))
