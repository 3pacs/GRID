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
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger as log
from starlette.middleware.base import BaseHTTPMiddleware

from api.auth import router as auth_router, verify_token

_environment = os.getenv("ENVIRONMENT", "development")
_start_time = time.time()


def _load_router(module_path: str, *, label: str, required: bool = False):
    """Import a router lazily so optional modules don't block server boot."""
    try:
        module = import_module(module_path)
        return getattr(module, "router")
    except Exception as exc:
        if required:
            raise
        log.warning("Skipping router {label}: {error}", label=label, error=str(exc))
        return None


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: startup and shutdown logic.

    CRITICAL: yield as fast as possible so uvicorn can serve requests.
    All slow/blocking work is deferred to background tasks or threads.
    """
    log.info("GRID API starting — environment={e}", e=_environment)

    loop = asyncio.get_event_loop()

    # ── Fast, non-blocking setup (must complete before yield) ─────────

    asyncio.create_task(_ws_broadcast_loop())

    # Register agent progress broadcast (fast — just stores references)
    try:
        from agents.progress import register_broadcast
        register_broadcast(_broadcast, loop)
        log.info("Agent WebSocket progress broadcast registered")
    except Exception as exc:
        log.debug("Agent progress registration skipped: {e}", e=str(exc))

    # Integrate push notifications (fast — just monkey-patches functions)
    try:
        from alerts.push_notify import integrate_with_email_alerts
        integrate_with_email_alerts()
    except Exception as exc:
        log.debug("Push notification integration skipped: {e}", e=str(exc))

    # ── Schedule slow startup work in a separate thread so it NEVER blocks requests ──
    import threading

    def _run_deferred_sync():
        """Run all deferred startup synchronously in a daemon thread."""
        _sync_deferred_startup(app)

    _startup_thread = threading.Thread(target=_run_deferred_sync, daemon=True, name="deferred-startup")
    _startup_thread.start()

    # ── Contracts dispatcher + retry scheduler ──
    # Lightweight: dispatcher just subscribes to the bus, retry scheduler
    # spawns a daemon polling thread. Safe to run synchronously here.
    try:
        from contracts.dispatcher import Dispatcher as _ContractsDispatcher
        from contracts.retry_scheduler import RetryScheduler as _ContractsRetry
        from contracts.dead_letter import record_failure as _record_failure
        from events.bus import bus as _contracts_bus
        from api.dependencies import get_db_engine as _get_db_engine

        _contracts_engine = _get_db_engine()

        def _contracts_dead_letter_writer(**kwargs):
            _record_failure(_contracts_engine, **kwargs)

        app.state.contracts_dispatcher = _ContractsDispatcher(
            bus=_contracts_bus,
            engine=_contracts_engine,
            dead_letter_writer=_contracts_dead_letter_writer,
        )
        app.state.contracts_dispatcher.start()

        app.state.contracts_retry = _ContractsRetry(engine=_contracts_engine)
        app.state.contracts_retry.start()
        log.info("Contracts dispatcher + retry scheduler started")
    except Exception as _c_exc:
        log.warning("Contracts subsystem startup skipped: {e}", e=str(_c_exc))

    log.info("GRID API accepting requests — background subsystems launching in thread")
    yield

    log.info("GRID API shutting down")
    if hasattr(app.state, "contracts_retry"):
        try:
            app.state.contracts_retry.stop()
        except Exception:
            pass


# Separate the slow startup work into a background coroutine that runs
# after uvicorn is already serving requests.

def _sync_deferred_startup(app: FastAPI) -> None:
    """Deferred startup — DB check + pre-warm dashboard cache."""
    try:
        from db import health_check
        ok = health_check()
        log.info("Database: " + ("connected" if ok else "unavailable"))
    except Exception as exc:
        log.warning("Database check failed: {e}", e=str(exc))

    # ALPHA-14: sync the adapter → oracle_models signal_sources merge on
    # startup so newly-wired adapters (flow_thesis, sector_network,
    # trust_scorer, etc.) are actually consumed by predict() without
    # requiring a manual migration run on deploy.
    try:
        from oracle.model_factory import migrate_default_models
        from db import get_engine as _ge
        migrate_default_models(_ge())
        log.info("oracle_models signal_sources migrated (union merge)")
    except Exception as exc:  # noqa: BLE001
        log.warning("oracle_models migration skipped: {e}", e=str(exc))

    # Pre-warm the intelligence dashboard cache so first user request is instant
    try:
        from api.routers.intelligence_risk import _build_dashboard_snapshot, _dashboard_cache
        log.info("Pre-warming intelligence dashboard cache...")
        snapshot = _build_dashboard_snapshot()
        _dashboard_cache.set("intel_dashboard", snapshot)
        log.info("Dashboard cache warmed — confidence={c}", c=snapshot.get("overall_confidence"))

        # Periodic re-warm: rebuild cache every 9 min so it never goes cold (TTL is 10 min)
        while True:
            time.sleep(540)  # 9 minutes
            try:
                snapshot = _build_dashboard_snapshot()
                _dashboard_cache.set("intel_dashboard", snapshot)
                log.debug("Dashboard cache re-warmed — confidence={c}", c=snapshot.get("overall_confidence"))
            except Exception as rewarm_exc:
                log.warning("Dashboard re-warm failed: {e}", e=str(rewarm_exc))
    except Exception as exc:
        log.warning("Dashboard pre-warm failed (will build on first request): {e}", e=str(exc))

    log.info("GRID API ready — serving requests")


app = FastAPI(
    title="GRID Intelligence API",
    version="1.0.0",
    docs_url="/api/docs" if _environment == "development" else None,
    redoc_url=None,
    lifespan=lifespan,
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
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://static.cloudflareinsights.com; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "img-src 'self' data: blob: https://*.cartocdn.com https://*.basemaps.cartocdn.com; "
            "connect-src 'self' ws: wss: https://cloudflareinsights.com "
            "https://*.cartocdn.com https://*.basemaps.cartocdn.com; "
            "worker-src 'self' blob:; "
            "font-src 'self' https://fonts.gstatic.com; "
            "object-src 'none'; "
            "frame-ancestors 'none'"
        )
        if _environment != "development":
            response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
        return response


app.add_middleware(SecurityHeadersMiddleware)

# Rate limiting middleware for expensive endpoints
_EXPENSIVE_PATHS = {
    "/api/v1/intel/deep-dive", "/api/v1/intel/network",
    "/api/v1/intel/ask", "/api/v1/intel/briefing",
    "/api/v1/intelligence/risk-map", "/api/v1/intelligence/globe",
    "/api/v1/intelligence/dashboard",
}


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limit expensive API endpoints per IP."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        path = request.url.path
        if any(path.startswith(p) for p in _EXPENSIVE_PATHS):
            client_ip = request.client.host if request.client else "unknown"
            if not _check_api_rate(client_ip):
                return Response(
                    content='{"error":"Rate limit exceeded. Max 30 requests/min for this endpoint."}',
                    status_code=429,
                    media_type="application/json",
                )
        return await call_next(request)


app.add_middleware(RateLimitMiddleware)


# x402 Agent Micropayment middleware (gated endpoints return 402 without payment)
class X402PaymentMiddleware(BaseHTTPMiddleware):
    """x402 payment middleware for agent micropayments.

    Checks paid endpoints for valid X-PAYMENT header.
    Returns HTTP 402 with payment requirements if missing/invalid.
    Disabled by default — enable via X402_ENABLED=true in .env.
    """

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        try:
            from config import settings as _s
            if not _s.X402_ENABLED:
                pass  # Skip payment check — fall through to call_next below
            else:
                from payments.x402 import X402Middleware, PaymentVerifier

                if not hasattr(app.state, "_x402_middleware"):
                    verifier = PaymentVerifier(
                        receiver_address=_s.X402_RECEIVER_ADDRESS,
                        network=_s.X402_NETWORK,
                        token=_s.X402_TOKEN,
                    )
                    app.state._x402_middleware = X402Middleware(verifier=verifier)

                mw = app.state._x402_middleware
                path = request.url.path
                payment_header = request.headers.get("X-PAYMENT")

                allowed, data = mw.check_payment(path, payment_header)
                if not allowed:
                    return JSONResponse(
                        status_code=402,
                        content=data,
                        headers={"X-Payment-Required": "true"},
                    )

        except Exception as exc:
            log.error(
                "X402 payment middleware error — returning 500 to prevent silent bypass: {e}",
                e=str(exc),
            )
            return JSONResponse(
                status_code=500,
                content={"detail": "Payment verification unavailable. Please try again later."},
            )

        return await call_next(request)


app.add_middleware(X402PaymentMiddleware)

# CORS — never allow credentials with wildcard origins.
# Priority: explicit GRID_ALLOWED_ORIGINS env var > environment default.
_raw_origins = os.getenv("GRID_ALLOWED_ORIGINS", "")
allowed_origins = [o.strip() for o in _raw_origins.split(",") if o.strip()]
if not allowed_origins:
    if _environment == "development":
        allowed_origins = [
            "http://localhost:5173",
            "http://localhost:8000",
            "http://127.0.0.1:5173",
        ]
    else:
        # Production default — explicit allowlist required for credentials.
        allowed_origins = ["https://grid.stepdad.finance"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# Include routers
app.include_router(auth_router)
for _label, _module_path, _required in [
    ("system", "api.routers.system", False),
    ("regime", "api.routers.regime", False),
    ("signals", "api.routers.signals", False),
    ("journal", "api.routers.journal", False),
    ("models", "api.routers.models", False),
    ("discovery", "api.routers.discovery", False),
    ("config", "api.routers.config", False),
    ("physics", "api.routers.physics", False),
    ("workflows", "api.routers.workflows", False),
    ("agents", "api.routers.agents", False),
    ("ollama", "api.routers.ollama", False),
    ("knowledge", "api.routers.knowledge", False),
    ("backtest", "api.routers.backtest", False),
    ("options", "api.routers.options", False),
    ("celestial", "api.routers.celestial", False),
    ("derivatives", "api.routers.derivatives", False),
    ("watchlist", "api.routers.watchlist", False),
    ("associations", "api.routers.associations", False),
    ("strategy", "api.routers.strategy", False),
    ("model_comparison", "api.routers.model_comparison", False),
    ("tradingview", "api.routers.tradingview", False),
    ("flows", "api.routers.flows", False),
    ("trading", "api.routers.trading", False),
    ("astrogrid", "api.routers.astrogrid", True),
    ("viz", "api.routers.viz", False),
    ("oracle", "api.routers.oracle", False),
    ("conviction", "api.routers.conviction", False),
    ("intelligence", "api.routers.intelligence", False),
    ("intel_source_audit", "api.routers.intel_source_audit", False),
    ("intel_cross_reference", "api.routers.intel_cross_reference", False),
    ("intel", "api.routers.intel", False),
    ("trials", "api.routers.trials", False),
    ("earnings", "api.routers.earnings", False),
    ("notifications", "api.routers.notifications", False),
    ("chat", "api.routers.chat", False),
    ("search", "api.routers.search", False),
    ("mcp_export", "api.routers.mcp_export", False),
    ("signal_registry", "api.routers.signal_registry", False),
    ("briefing", "api.routers.briefing", False),
    ("forecasts", "api.routers.forecasts", False),
    ("a2a", "api.routers.a2a", False),
    ("regime", "api.routers.intelligence_regime", False),
    ("feed", "api.routers.feed", False),
    ("vault", "api.routers.vault", False),
    ("spider", "api.routers.intelligence_spider", False),
    ("valuation", "api.routers.valuation", False),
    ("prediction_backtest", "api.routers.prediction_backtest", False),
    ("sse", "api.routers.sse", False),
    ("canvas", "api.routers.canvas", False),
    ("surfacer", "api.routers.surfacer", False),
    ("intelligence_search", "api.routers.intelligence_search", False),
    ("geo", "api.routers.geo", False),
    ("blob", "api.routers.blob", False),
    ("actor_detail", "api.routers.actor_detail", False),
    ("actor_news_api", "api.routers.actor_news_api", False),
    ("supply_chain", "api.routers.supply_chain", False),
    ("capital_flow", "api.routers.capital_flow", False),
    ("divergence", "api.routers.divergence", False),
    ("contagion", "api.routers.contagion", False),
    ("trade_tickets", "api.routers.trade_tickets", False),
    ("contracts", "api.routers.contracts", False),
    ("attributions", "api.routers.attributions", False),
    ("explain", "api.routers.explain", False),
    ("sector_health", "api.routers.sector_health", False),
    ("user_intel", "api.routers.user_intel", False),
    ("snapshots", "api.routers.snapshots", False),
]:
    _router = _load_router(_module_path, label=_label, required=_required)
    if _router is not None:
        app.include_router(_router)

# Contagion sector-matrix router (lives under /api/v1/sectors)
try:
    from api.routers.contagion import sector_router as _contagion_sector_router
    app.include_router(_contagion_sector_router)
    log.info("Contagion sector matrix router loaded")
except Exception as _cs_exc:
    log.debug("Contagion sector matrix router not loaded: {e}", e=str(_cs_exc))

# LLM Task Queue endpoints (GET /api/v1/system/llm-status, POST /api/v1/system/llm-task)
try:
    from orchestration.llm_taskqueue import build_router as _build_tq_router
    app.include_router(_build_tq_router())
    log.info("LLM task queue router loaded")
except Exception as _tq_exc:
    log.debug("LLM task queue router not loaded: {e}", e=str(_tq_exc))

# Distributed compute endpoints (GET /api/v1/compute/task, POST /api/v1/compute/submit)
try:
    from subnet.distributed_compute import compute_router
    app.include_router(compute_router)
    log.info("Distributed compute router loaded")
except Exception as _dc_exc:
    log.debug("Distributed compute router not loaded: {e}", e=str(_dc_exc))

# Mobile mining endpoints (OAuth connect + task processing via ChatGPT/Copilot/Claude/Gemini)
try:
    from subnet.oauth_miner import mine_router
    if mine_router:
        app.include_router(mine_router)
        log.info("Mobile mining router loaded")
except Exception as _mm_exc:
    log.debug("Mobile mining router not loaded: {e}", e=str(_mm_exc))

# WebSocket connections
_ws_clients: set[WebSocket] = set()
_MAX_WS_CONNECTIONS = 200  # prevent memory exhaustion from connection flooding

# Per-IP rate limiting for WebSocket + expensive endpoints
_ws_connect_attempts: dict[str, list[float]] = {}  # ip -> timestamps
_WS_MAX_CONNECTS_PER_MIN = 10
_api_rate_limits: dict[str, list[float]] = {}  # ip -> timestamps
_API_EXPENSIVE_RPM = 30  # expensive endpoints per minute per IP


def _check_ws_rate(ip: str) -> bool:
    """Return True if IP is within WebSocket connection rate limit."""
    now = time.time()
    attempts = _ws_connect_attempts.get(ip, [])
    attempts = [t for t in attempts if t > now - 60]
    _ws_connect_attempts[ip] = attempts
    if len(attempts) >= _WS_MAX_CONNECTS_PER_MIN:
        return False
    attempts.append(now)
    return True


def _check_api_rate(ip: str) -> bool:
    """Return True if IP is within expensive API rate limit."""
    now = time.time()
    attempts = _api_rate_limits.get(ip, [])
    attempts = [t for t in attempts if t > now - 60]
    _api_rate_limits[ip] = attempts
    if len(attempts) >= _API_EXPENSIVE_RPM:
        return False
    attempts.append(now)
    return True


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


# ── Public broadcast helper (importable by other modules) ─────────────

_event_loop: asyncio.AbstractEventLoop | None = None


def broadcast_event(event_type: str, data: dict) -> None:
    """Send a typed event to all connected WebSocket clients.

    Thread-safe: can be called from any thread (ingestion, scheduler, etc.).
    The message is submitted to the event loop as a coroutine.

    Event types: prices, recommendation, alert, regime_change, ping,
                 agent_progress, agent_run_complete, signal_update.

    Example:
        broadcast_event("prices", {"SPY": {"price": 520.5, "pct_1d": 0.012}})
        broadcast_event("alert", {"severity": "high", "message": "Convergence detected"})
    """
    message = {
        "type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": data,
    }
    loop = _event_loop
    if loop is None or loop.is_closed():
        return
    try:
        asyncio.run_coroutine_threadsafe(_broadcast(message), loop)
    except RuntimeError:
        pass  # loop already closed at shutdown


async def _ws_broadcast_loop() -> None:
    """Background loop that pushes ping + live data every 10 seconds."""
    global _event_loop
    _event_loop = asyncio.get_event_loop()

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


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
) -> None:
    """WebSocket endpoint for real-time updates."""
    client_ip = websocket.client.host if websocket.client else "unknown"

    if not _check_ws_rate(client_ip):
        await websocket.close(code=1008, reason="Rate limit exceeded")
        return

    if len(_ws_clients) >= _MAX_WS_CONNECTIONS:
        await websocket.close(code=1008, reason="Server capacity exceeded")
        return

    await websocket.accept()

    # First-message authentication: client must send {"token": "<jwt>"} within 5 seconds.
    try:
        raw = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
        auth_msg = json.loads(raw)
        token = auth_msg.get("token", "") if isinstance(auth_msg, dict) else ""
    except (asyncio.TimeoutError, json.JSONDecodeError, Exception):
        await websocket.close(code=4001, reason="Authentication timeout")
        return

    if not token or not verify_token(token):
        await websocket.close(code=4001, reason="Invalid token")
        return
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


# Serve DerivativesGrid static files
_derivatives_dist = Path(__file__).parent.parent / "derivatives_dist"
if _derivatives_dist.exists():
    app.mount("/derivatives", StaticFiles(directory=str(_derivatives_dist), html=True), name="derivatives")

# Serve AstroGrid directly from source when available
_astrogrid_web = Path(__file__).parent.parent / "astrogrid_web"
_astrogrid_lib = Path(__file__).parent.parent / "astrogrid" / "src" / "lib"
if _astrogrid_lib.exists():
    app.mount("/astrogrid-lib", StaticFiles(directory=str(_astrogrid_lib), html=False), name="astrogrid-lib")
if _astrogrid_web.exists():
    app.mount("/astrogrid", StaticFiles(directory=str(_astrogrid_web), html=True), name="astrogrid")

# Serve AstroGrid built static files as fallback
_astrogrid_dist = Path(__file__).parent.parent / "astrogrid_dist"
if not _astrogrid_web.exists() and _astrogrid_dist.exists():
    app.mount("/astrogrid", StaticFiles(directory=str(_astrogrid_dist), html=True), name="astrogrid")

# Serve PWA static files — mount AFTER API routes
_pwa_dist = Path(__file__).parent.parent / "pwa_dist"
_pwa_src = Path(__file__).parent.parent / "pwa"

if _pwa_dist.exists():
    app.mount("/assets", StaticFiles(directory=str(_pwa_dist / "assets")), name="assets")

    def _pwa_file_response(path: Path) -> FileResponse:
        headers = {}
        if path.name == "service-worker.js":
            headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        elif path.name == "index.html" or path.suffix == ".html":
            headers["Cache-Control"] = "no-cache, must-revalidate"
        return FileResponse(str(path), headers=headers)

    @app.get("/visualizer")
    async def serve_visualizer() -> FileResponse:
        """Serve the standalone data visualizer."""
        viz_path = _pwa_dist / "visualizer.html"
        if viz_path.exists():
            return _pwa_file_response(viz_path)
        return _pwa_file_response(_pwa_dist / "index.html")

    @app.get("/{full_path:path}")
    async def serve_pwa(full_path: str) -> Response:
        """Serve PWA — return index.html for all non-API paths (SPA routing)."""
        if full_path.startswith("api/"):
            return JSONResponse({"detail": "Not found"}, status_code=404)
        file_path = _pwa_dist / full_path
        if file_path.exists() and file_path.is_file():
            return _pwa_file_response(file_path)
        return _pwa_file_response(_pwa_dist / "index.html")

elif _pwa_src.exists():
    @app.get("/{full_path:path}")
    async def serve_pwa_dev(full_path: str) -> Response:
        """Serve PWA source in development."""
        if full_path.startswith("api/"):
            return JSONResponse({"detail": "Not found"}, status_code=404)
        file_path = _pwa_src / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(str(file_path))
        return FileResponse(str(_pwa_src / "index.html"))
