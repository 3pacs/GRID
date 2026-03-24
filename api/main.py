"""
GRID Intelligence API — FastAPI application entry point.

Serves the API at /api/v1/* and the PWA at /.
WebSocket endpoint at /ws for real-time updates.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
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
from api.routers.options import router as options_router
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
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "connect-src 'self' ws: wss:; "
            "font-src 'self'; "
            "object-src 'none'; "
            "frame-ancestors 'none'"
        )
        if _environment != "development":
            response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
        return response


app.add_middleware(SecurityHeadersMiddleware)

# Request body size limit — prevent OOM from oversized POST bodies
_MAX_BODY_BYTES = int(os.getenv("GRID_MAX_BODY_BYTES", str(10 * 1024 * 1024)))  # 10 MB


class RequestSizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject requests with bodies exceeding the configured limit."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > _MAX_BODY_BYTES:
            return JSONResponse(
                {"detail": f"Request body too large (max {_MAX_BODY_BYTES} bytes)"},
                status_code=413,
            )
        return await call_next(request)


app.add_middleware(RequestSizeLimitMiddleware)

# CORS — never allow credentials with wildcard origins
#
# CSRF Note: GRID uses JWT via Authorization header (not cookies) for all
# API requests.  Browser-based CSRF attacks require cookie-based auth to
# be effective, so CSRF tokens are not needed for this API.  If cookie-based
# auth is ever added, CSRF middleware must be added simultaneously.
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
app.include_router(options_router)

# WebSocket connections — track last activity to evict idle clients
_ws_clients: dict[WebSocket, float] = {}  # ws → last_active_timestamp
_WS_IDLE_TIMEOUT = 300  # 5 minutes — evict clients silent for this long


async def _broadcast(message: dict) -> None:
    """Send a message to all connected WebSocket clients."""
    data = json.dumps(message)
    disconnected: list[WebSocket] = []
    for ws in list(_ws_clients):
        try:
            await ws.send_text(data)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        _ws_clients.pop(ws, None)


async def _ws_broadcast_loop() -> None:
    """Background loop that pushes pings every 10 seconds and evicts idle clients."""
    while True:
        await asyncio.sleep(10)
        if not _ws_clients:
            continue

        now_ts = time.time()
        now_iso = datetime.now(timezone.utc).isoformat()

        # Evict idle clients (no pong/activity for _WS_IDLE_TIMEOUT seconds)
        stale = [ws for ws, last in _ws_clients.items() if now_ts - last > _WS_IDLE_TIMEOUT]
        for ws in stale:
            _ws_clients.pop(ws, None)
            try:
                await ws.close(code=4002, reason="Idle timeout")
            except Exception:
                pass
        if stale:
            log.info("Evicted {n} idle WebSocket client(s) (total={t})", n=len(stale), t=len(_ws_clients))

        try:
            await _broadcast({
                "type": "ping",
                "timestamp": now_iso,
                "data": {"uptime_seconds": round(now_ts - _start_time, 1)},
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

    # Audit configured API keys
    try:
        from config import settings
        key_audit = settings.audit_api_keys()
        configured = [k for k, v in key_audit.items() if v]
        missing = [k for k, v in key_audit.items() if not v]
        log.info(
            "API key audit — {ok}/{total} configured: {keys}",
            ok=len(configured),
            total=len(key_audit),
            keys=", ".join(configured) if configured else "(none)",
        )
        if missing:
            log.warning(
                "Missing API keys (sources will degrade gracefully): {keys}",
                keys=", ".join(missing),
            )
    except Exception as exc:
        log.debug("API key audit skipped: {e}", e=str(exc))

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

    # Start unified ingestion scheduler (domestic + international)
    try:
        import threading
        from ingestion.scheduler import start_scheduler as _start_scheduler

        t = threading.Thread(target=_start_scheduler, daemon=True, name="ingestion")
        t.start()
        log.info("Unified ingestion scheduler started (domestic + international)")
    except Exception as exc:
        log.warning("Ingestion scheduler failed to start: {e}", e=str(exc))

    # Start server-log git sink (pushes sanitized errors to git)
    try:
        from server_log.git_sink import GitSink
        _git_sink = GitSink()
        # Add loguru sink for ERROR and above
        log.add(_git_sink.write, level="ERROR", format="{message}")
        _git_sink.start()
        app.state.git_sink = _git_sink  # keep reference for shutdown
        log.info("Server-log git sink started (ERROR+ → .server-logs/errors.jsonl)")
    except Exception as exc:
        log.warning("Server-log git sink failed to start: {e}", e=str(exc))

    # Start operator inbox (two-way communication via git)
    try:
        from server_log.inbox import Inbox
        from server_log.git_sink import _repo_root
        _inbox = Inbox(repo_root=_repo_root())
        _inbox.start()
        app.state.inbox = _inbox
        log.info("Operator inbox started (polling .server-logs/inbox.jsonl)")
    except Exception as exc:
        log.warning("Operator inbox failed to start: {e}", e=str(exc))

    # Start insight scanner (daily + weekly reviews of LLM outputs)
    try:
        from outputs.insight_scanner import schedule_reviews
        schedule_reviews()
    except Exception as exc:
        log.debug("Insight scanner start skipped: {e}", e=str(exc))

    log.info("GRID API ready — all subsystems initialised")


@app.on_event("shutdown")
async def shutdown() -> None:
    """Gracefully stop all background services on SIGTERM/shutdown."""
    log.info("GRID API shutting down — stopping subsystems")

    # Stop agent scheduler
    try:
        from agents.scheduler import stop_agent_scheduler
        stop_agent_scheduler()
        log.info("Agent scheduler stopped")
    except Exception as exc:
        log.debug("Agent scheduler stop skipped: {e}", e=str(exc))

    # Stop git sink (flush pending writes)
    try:
        git_sink = getattr(app.state, "git_sink", None)
        if git_sink is not None:
            git_sink.stop()
            log.info("Git sink stopped")
    except Exception as exc:
        log.debug("Git sink stop failed: {e}", e=str(exc))

    # Stop operator inbox
    try:
        inbox = getattr(app.state, "inbox", None)
        if inbox is not None:
            inbox.stop()
            log.info("Operator inbox stopped")
    except Exception as exc:
        log.debug("Inbox stop failed: {e}", e=str(exc))

    # Close all WebSocket connections
    for ws in list(_ws_clients):
        try:
            await ws.close(code=1001, reason="Server shutting down")
        except Exception:
            pass
    _ws_clients.clear()

    # Dispose database engine
    try:
        from api.dependencies import clear_singletons
        clear_singletons()
        log.info("Database connections disposed")
    except Exception as exc:
        log.debug("Singleton cleanup failed: {e}", e=str(exc))

    log.info("GRID API shutdown complete")


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(default=""),
) -> None:
    """WebSocket endpoint for real-time updates.

    Supports two auth modes:
    1. First-message auth (preferred) — connect without token, send
       ``{"type": "auth", "token": "..."}`` as first message.
    2. Query-param auth (legacy) — connect with ``?token=...``.

    First-message auth avoids leaking tokens in URLs, server logs,
    and proxy access logs.
    """
    # Accept the connection first — auth happens via first message or query param
    await websocket.accept()

    # --- Authenticate -------------------------------------------------
    authenticated = False

    # Legacy: query param token
    if token and verify_token(token):
        authenticated = True
    else:
        # First-message auth: wait up to 5 seconds for auth message
        try:
            raw = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
            msg = json.loads(raw)
            if msg.get("type") == "auth" and verify_token(msg.get("token", "")):
                authenticated = True
        except (asyncio.TimeoutError, json.JSONDecodeError, WebSocketDisconnect):
            pass

    if not authenticated:
        await websocket.close(code=4001, reason="Invalid token")
        return

    _ws_clients[websocket] = time.time()
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
        _ws_clients.pop(websocket, None)
        return

    try:
        while True:
            await websocket.receive_text()
            _ws_clients[websocket] = time.time()  # Update activity on any message
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.pop(websocket, None)
        log.info("WebSocket client disconnected (total={n})", n=len(_ws_clients))


# Serve PWA static files — mount AFTER API routes
_pwa_dist = Path(__file__).parent.parent / "pwa_dist"
_pwa_src = Path(__file__).parent.parent / "pwa"

if _pwa_dist.exists():
    app.mount("/assets", StaticFiles(directory=str(_pwa_dist / "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_pwa(request: Request, full_path: str) -> FileResponse:
        """Serve PWA — return index.html for all non-API paths (SPA routing)."""
        # Don't intercept API or docs routes
        if full_path.startswith(("api/", "ws", "docs", "redoc", "openapi.json")):
            from fastapi.responses import JSONResponse
            return JSONResponse({"detail": "Not Found"}, status_code=404)
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
