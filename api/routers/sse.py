"""Server-Sent Events endpoint for real-time event streaming.

Clients connect to ``GET /api/v1/events/stream`` and receive events from
every stream channel: the legacy ``grid_*`` set in ``events/channels.py``
plus every typed contract channel (``grid_contracts_*``) from
``contracts/channels.py``. Optional ``channels`` query param to filter.

Until 2026-09-10 the stream subscribed only to the legacy set, which has no
producers, so it carried the ``connected`` frame and keepalives and nothing
else. The contracts layer is the backbone with real emitters (fifteen typed
events, nine producers); ``contracts/emit.py`` now issues ``pg_notify`` in
the same transaction as its audit write, and ``events/bus.py`` fans
listener-delivered events out to subscribers registered with ``remote=True``.

Example:
    curl -H "Authorization: Bearer <token>" \\
         "https://grid.stepdad.finance/api/v1/events/stream?channels=grid_contracts_signal_fired"
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import StreamingResponse
from loguru import logger as log

from api.auth import require_auth
from events.bus import RecentEventIds, bus
from events.channels import ALL_CHANNELS as LEGACY_CHANNELS, Event

try:
    from contracts.channels import ALL_CHANNELS as CONTRACT_CHANNELS
except Exception as _contracts_exc:  # pragma: no cover — contracts layer is optional at import
    log.warning("SSE: contract channels unavailable: {e}", e=str(_contracts_exc))
    CONTRACT_CHANNELS: tuple[str, ...] = ()

# Every channel the stream can carry, legacy first, de-duplicated, ordered.
STREAM_CHANNELS: tuple[str, ...] = tuple(dict.fromkeys((*LEGACY_CHANNELS, *CONTRACT_CHANNELS)))

router = APIRouter(prefix="/api/v1/events", tags=["events"])


def _event_id_of(event: Event) -> Any:
    payload = event.payload
    return payload.get("event_id") if isinstance(payload, dict) else None


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
        listen_channels = tuple(ch for ch in STREAM_CHANNELS if ch in requested)
    else:
        listen_channels = STREAM_CHANNELS

    # Queue for this client's events
    queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=100)
    # In-process emits arrive twice when this process is also the PG
    # listener (direct fan-out + its own notify echo); drop the second.
    recent = RecentEventIds()

    def on_event(event: Event) -> None:
        if recent.seen_before(_event_id_of(event)):
            return
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            pass  # Drop oldest-style: client is too slow

    # Subscribe to requested channels — remote=True so contracts emitted in
    # other processes (grid-hermes, grid-scheduler) reach the browser.
    for ch in listen_channels:
        bus.subscribe(ch, on_event, remote=True)

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
                bus.unsubscribe(ch, on_event)
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
        "channels": list(STREAM_CHANNELS),
        "count": len(STREAM_CHANNELS),
        "legacy": list(LEGACY_CHANNELS),
        "contracts": list(CONTRACT_CHANNELS),
        "listening": bus.listening,
    }


@router.get("/topics")
async def list_topics(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """List all Redpanda topics with availability status."""
    try:
        from events.producer import TOPICS
        from events.consumer import get_topic_info

        topic_status = {}
        for key in TOPICS:
            topic_status[key] = get_topic_info(key)

        any_available = any(t.get("available", False) for t in topic_status.values())

        return {
            "topics": topic_status,
            "count": len(TOPICS),
            "redpanda_available": any_available,
            "fallback": "pg_notify" if not any_available else None,
        }
    except Exception as e:
        log.warning(f"Topic info retrieval failed: {e}")
        return {
            "topics": {},
            "count": 0,
            "redpanda_available": False,
            "fallback": "pg_notify",
            "error": str(e),
        }
