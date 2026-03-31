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
from fastapi.responses import FileResponse
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
    """Application lifespan: startup and shutdown logic."""
    log.info("GRID API starting — environment={e}", e=_environment)

    # Verify database
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
        from config import settings as _settings
        key_audit = _settings.audit_api_keys()
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
        log.add(_git_sink.write, level="ERROR", format="{message}")
        _git_sink.start()
        app.state.git_sink = _git_sink
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

    # Pre-compute capital flow analysis on startup (cached, non-blocking)
    try:
        import threading

        def _preload_capital_flows():
            try:
                from analysis.capital_flows import CapitalFlowResearchEngine
                from db import get_engine as _get_eng
                eng = _get_eng()
                cfe = CapitalFlowResearchEngine(db_engine=eng)
                result = cfe.run_research(force=False)  # uses cache if fresh
                sources = len(result.get("metadata", {}).get("sources_pulled", []))
                log.info("Capital flow pre-load complete: {s} sources", s=sources)
            except Exception as exc:
                log.warning("Capital flow pre-load failed: {e}", e=str(exc))

        threading.Thread(target=_preload_capital_flows, daemon=True, name="capflow-preload").start()
    except Exception:
        pass

    # Start 24/7 intelligence loop (briefings, wiki history, crypto prices)
    try:
        import threading
        import schedule as _sched
        import time as _time

        def _intelligence_loop():
            """Background loop: hourly briefings, 4h capital flows, daily wiki + crypto."""
            from config import Settings
            _s = Settings()

            # Schedule intelligence tasks
            def _hourly_briefing():
                try:
                    from ollama.market_briefing import MarketBriefingEngine
                    from db import get_engine as _ge
                    mbe = MarketBriefingEngine(db_engine=_ge())
                    mbe.generate_briefing("hourly", save=True)
                    log.info("Hourly briefing generated (intelligence loop)")
                except Exception as exc:
                    log.debug("Hourly briefing failed: {e}", e=str(exc))

            def _capital_flow_refresh():
                try:
                    from analysis.capital_flows import CapitalFlowResearchEngine
                    from db import get_engine as _ge
                    cfe = CapitalFlowResearchEngine(db_engine=_ge())
                    cfe.run_research(force=True)
                    log.info("Capital flow refresh complete (intelligence loop)")
                except Exception as exc:
                    log.debug("Capital flow refresh failed: {e}", e=str(exc))

            def _daily_context():
                try:
                    from ingestion.wiki_history import WikiHistoryPuller
                    from db import get_engine as _ge
                    wp = WikiHistoryPuller(db_engine=_ge())
                    data = wp.pull_today()
                    wp.save_to_db(data)
                    log.info("Wiki history ingested: {n} events", n=len(data.get("wiki_events", [])))
                except Exception as exc:
                    log.debug("Wiki history failed: {e}", e=str(exc))

                try:
                    from ingestion.coingecko import CoinGeckoPuller
                    from db import get_engine as _ge
                    cg = CoinGeckoPuller(_ge())
                    cg.pull_all()
                    log.info("CoinGecko crypto prices refreshed (intelligence loop)")
                except Exception as exc:
                    log.debug("CoinGecko pull failed: {e}", e=str(exc))

                try:
                    from ingestion.social_sentiment import SocialSentimentPuller
                    from db import get_engine as _ge
                    sp = SocialSentimentPuller(db_engine=_ge())
                    result = sp.pull_all()
                    sp.save_to_db(result)
                    log.info("Social sentiment: {s}", s=result.get("summary", ""))
                except Exception as exc:
                    log.debug("Social sentiment failed: {e}", e=str(exc))

            def _nightly_research():
                try:
                    from analysis.research_agent import run_full_research
                    from db import get_engine as _ge
                    result = run_full_research(_ge())
                    log.info("Nightly research complete: {r}", r=str(result)[:200])
                except Exception as exc:
                    log.debug("Nightly research failed: {e}", e=str(exc))

            def _taxonomy_audit():
                try:
                    from analysis.taxonomy_audit import run_taxonomy_audit
                    from db import get_engine as _ge
                    report = run_taxonomy_audit(_ge())
                    fixes = len(report.get("auto_fixes", []))
                    recs = len(report.get("recommendations", []))
                    log.info("Taxonomy audit: {f} auto-fixes, {r} recommendations, {c}% coverage",
                             f=fixes, r=recs, c=report.get("stats", {}).get("coverage_pct", 0))
                except Exception as exc:
                    log.debug("Taxonomy audit failed: {e}", e=str(exc))

            def _price_fallback():
                """Pull stale equity/crypto prices via fallback sources."""
                try:
                    from ingestion.price_fallback import PriceFallbackPuller
                    from db import get_engine as _ge
                    eng = _ge()
                    pfp = PriceFallbackPuller(db_engine=eng)
                    # Find stale _full features
                    from sqlalchemy import text as _t
                    with eng.connect() as conn:
                        stale = conn.execute(_t(
                            "SELECT fr.name FROM feature_registry fr "
                            "LEFT JOIN LATERAL ("
                            "  SELECT obs_date FROM resolved_series WHERE feature_id = fr.id "
                            "  ORDER BY obs_date DESC LIMIT 1"
                            ") rs ON TRUE "
                            "WHERE fr.model_eligible = TRUE AND fr.family IN ('equity','crypto','commodity') "
                            "AND (rs.obs_date IS NULL OR rs.obs_date < CURRENT_DATE - 1) "
                            "AND fr.name LIKE '%\_full' ESCAPE '\\'"
                        )).fetchall()
                    tickers = [r[0].replace('_full', '').upper().replace('_', '-') for r in stale]
                    if tickers:
                        results = pfp.pull_many(tickers[:20])
                        pfp.save_to_db(results)
                        log.info("Price fallback: {n}/{t} stale tickers refreshed", n=len(results), t=len(tickers))
                except Exception as exc:
                    log.debug("Price fallback failed: {e}", e=str(exc))

            def _paper_trading_signals():
                try:
                    from trading.signal_executor import execute_signals
                    from db import get_engine as _ge
                    result = execute_signals(_ge())
                    log.info("Paper trading: {o} opened, {c} closed",
                             o=result.get("trades_opened", 0), c=result.get("trades_closed", 0))
                except Exception as exc:
                    log.debug("Paper trading signals failed: {e}", e=str(exc))

            _sched.every(1).hours.do(_paper_trading_signals)
            _sched.every(1).hours.do(_hourly_briefing)
            _sched.every(4).hours.do(_capital_flow_refresh)
            _sched.every(6).hours.do(_price_fallback)
            _sched.every().day.at("02:00").do(_nightly_research)
            _sched.every().day.at("02:30").do(_taxonomy_audit)
            def _celestial_briefing():
                try:
                    from ollama.celestial_briefing import generate_celestial_briefing
                    from db import get_engine as _ge
                    result = generate_celestial_briefing(_ge())
                    log.info("Celestial briefing generated: {n} chars", n=len(result.get("content", "")))
                except Exception as exc:
                    log.debug("Celestial briefing failed: {e}", e=str(exc))

            def _weekly_astro_correlations():
                try:
                    from analysis.astro_correlations import AstroCorrelationEngine
                    from db import get_engine as _ge
                    ace = AstroCorrelationEngine(_ge())
                    results = ace.get_cached_or_compute(force_refresh=True)
                    log.info("Weekly astro correlations: {n} significant pairs", n=len(results))
                except Exception as exc:
                    log.debug("Astro correlations failed: {e}", e=str(exc))

            def _dealer_flow_briefing():
                try:
                    from ollama.dealer_flow_briefing import generate_dealer_flow_briefing
                    from db import get_engine as _ge
                    result = generate_dealer_flow_briefing(_ge())
                    log.info("Dealer flow briefing generated: {n} chars", n=len(result.get("content", "")))
                except Exception as exc:
                    log.debug("Dealer flow briefing failed: {e}", e=str(exc))

            def _options_recommendations():
                try:
                    from trading.options_recommender import OptionsRecommender
                    from db import get_engine as _ge
                    rec = OptionsRecommender(db_engine=_ge())
                    recs = rec.generate_recommendations()
                    log.info("Options recommendations generated: {n} recommendations", n=len(recs))
                except Exception as exc:
                    log.debug("Options recommendations failed: {e}", e=str(exc))

            def _options_tracker():
                try:
                    from trading.options_tracker import run_improvement_cycle
                    from db import get_engine as _ge
                    result = run_improvement_cycle(_ge())
                    log.info(
                        "Options tracker cycle complete — scored={s}",
                        s=result.get("scoring_summary", {}).get("scored", 0),
                    )
                except Exception as exc:
                    log.debug("Options tracker failed: {e}", e=str(exc))

            _sched.every().day.at("06:00").do(_daily_context)
            _sched.every().day.at("07:00").do(_options_recommendations)
            _sched.every().day.at("10:00").do(_celestial_briefing)
            _sched.every().day.at("15:00").do(_dealer_flow_briefing)
            _sched.every().day.at("18:00").do(_daily_context)
            _sched.every().sunday.at("03:00").do(_weekly_astro_correlations)
            _sched.every(7).days.do(_options_tracker)

            log.info("Intelligence loop started — hourly briefings, 4h capital flows, 6h price fallback, nightly research, daily context, weekly astro correlations, dealer flow briefing, daily options recommendations, weekly options tracker")
            while True:
                _sched.run_pending()
                _time.sleep(30)

        threading.Thread(target=_intelligence_loop, daemon=True, name="intel-loop").start()
    except Exception as exc:
        log.warning("Intelligence loop failed to start: {e}", e=str(exc))

    # Integrate push notifications with existing email alerts
    try:
        from alerts.push_notify import integrate_with_email_alerts
        integrate_with_email_alerts()
    except Exception as exc:
        log.debug("Push notification integration skipped: {e}", e=str(exc))

    log.info("GRID API ready — all subsystems initialised")
    yield
    log.info("GRID API shutting down")


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
    ("intelligence", "api.routers.intelligence", False),
    ("intel_source_audit", "api.routers.intel_source_audit", False),
    ("intel_cross_reference", "api.routers.intel_cross_reference", False),
    ("intel", "api.routers.intel", False),
    ("earnings", "api.routers.earnings", False),
    ("notifications", "api.routers.notifications", False),
    ("chat", "api.routers.chat", False),
    ("search", "api.routers.search", False),
    ("mcp_export", "api.routers.mcp_export", False),
    ("signal_registry", "api.routers.signal_registry", False),
]:
    _router = _load_router(_module_path, label=_label, required=_required)
    if _router is not None:
        app.include_router(_router)

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

    @app.get("/visualizer")
    async def serve_visualizer() -> FileResponse:
        """Serve the standalone data visualizer."""
        viz_path = _pwa_dist / "visualizer.html"
        if viz_path.exists():
            return FileResponse(str(viz_path))
        return FileResponse(str(_pwa_dist / "index.html"))

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
