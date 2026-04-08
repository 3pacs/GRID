"""Server-Sent Events endpoint for real-time event streaming.

Clients connect to ``GET /api/v1/events/stream`` and receive events
from all GRID channels. Optional ``channels`` query param to filter.

Example:
    curl -H "Authorization: Bearer <token>" \\
         "https://grid.stepdad.finance/api/v1/events/stream?channels=grid_signal_fire,grid_regime_change"
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse
from loguru import logger as log

from api.auth import require_auth
from events.bus import bus
from events.channels import ALL_CHANNELS, Event

router = APIRouter(prefix="/api/v1/events", tags=["events"])


@router.get("/stream")
async def event_stream(
    request: Request,
    channels: str | None = Query(None, description="Comma-separated channel names to subscribe to"),
    _token: str = Depends(require_auth),
):
    """SSE endpoint — streams real-time GRID events to the client."""
    # Parse channel filter
    if channels:
        requested = set(ch.strip() for ch in channels.split(","))
        listen_channels = tuple(ch for ch in requested if ch in ALL_CHANNELS)
    else:
        listen_channels = ALL_CHANNELS

    # Queue for this client's events
    queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=100)

    def on_event(event: Event) -> None:
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            pass  # Drop oldest-style: client is too slow

    # Subscribe to requested channels
    for ch in listen_channels:
        bus.subscribe(ch, on_event)

    async def generate():
        try:
            # Send initial connection event
            yield f"event: connected\ndata: {json.dumps({'channels': list(listen_channels)})}\n\n"

            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    break

                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield event.to_sse()
                except asyncio.TimeoutError:
                    # Send keepalive comment every 30s
                    yield ": keepalive\n\n"
        finally:
            # Unsubscribe on disconnect
            for ch in listen_channels:
                try:
                    bus._subscribers[ch].remove(on_event)
                except ValueError:
                    pass
            log.debug("SSE client disconnected")

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/channels")
async def list_channels(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """List all available event channels."""
    return {
        "channels": list(ALL_CHANNELS),
        "count": len(ALL_CHANNELS),
    }
