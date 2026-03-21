"""
Agent run progress tracking via WebSocket broadcast.

Provides a thread-safe progress emitter that the AgentRunner calls
at each stage. The API layer registers a broadcast callback so events
reach all connected WebSocket clients.
"""

from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

from loguru import logger as log

# Registered broadcast callback — set by the API startup
_broadcast_fn: Callable[[dict], Coroutine] | None = None
_event_loop: asyncio.AbstractEventLoop | None = None
_lock = threading.Lock()


def register_broadcast(fn: Callable[[dict], Coroutine], loop: asyncio.AbstractEventLoop) -> None:
    """Register the async broadcast function and event loop from the API layer."""
    global _broadcast_fn, _event_loop
    with _lock:
        _broadcast_fn = fn
        _event_loop = loop
    log.info("Agent progress broadcast registered")


def emit_progress(
    run_id: int | None,
    stage: str,
    ticker: str,
    detail: str = "",
    progress_pct: float = 0.0,
    data: dict[str, Any] | None = None,
) -> None:
    """Emit a progress event to all WebSocket clients.

    Called from the AgentRunner (which runs in a sync thread). Schedules
    the async broadcast onto the API event loop.

    Parameters:
        run_id: Agent run ID (None if not yet created).
        stage: Current stage name (e.g. 'context', 'analysts', 'debate').
        ticker: Ticker being analysed.
        detail: Human-readable progress description.
        progress_pct: 0.0 to 1.0 progress estimate.
        data: Optional extra data for the client.
    """
    with _lock:
        fn = _broadcast_fn
        loop = _event_loop

    if fn is None or loop is None:
        return

    message = {
        "type": "agent_progress",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {
            "run_id": run_id,
            "stage": stage,
            "ticker": ticker,
            "detail": detail,
            "progress_pct": round(progress_pct, 2),
            **(data or {}),
        },
    }

    try:
        asyncio.run_coroutine_threadsafe(fn(message), loop)
    except Exception as exc:
        log.debug("Failed to emit agent progress: {e}", e=str(exc))


def emit_run_complete(
    run_id: int,
    ticker: str,
    final_decision: str,
    duration: float,
    error: str | None = None,
) -> None:
    """Emit a run-complete event."""
    with _lock:
        fn = _broadcast_fn
        loop = _event_loop

    if fn is None or loop is None:
        return

    message = {
        "type": "agent_run_complete",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {
            "run_id": run_id,
            "ticker": ticker,
            "final_decision": final_decision,
            "duration_seconds": round(duration, 2),
            "error": error,
        },
    }

    try:
        asyncio.run_coroutine_threadsafe(fn(message), loop)
    except Exception as exc:
        log.debug("Failed to emit run complete: {e}", e=str(exc))
