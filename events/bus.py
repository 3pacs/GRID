"""GRID Event Bus — PG LISTEN/NOTIFY wrapper with in-process fan-out.

Dual-mode operation:
  1. **In-process**: ``emit_sync()`` fans out to local subscribers immediately.
     Used by intelligence modules running in the API process.
  2. **Cross-process** (async): ``emit()`` sends a PG NOTIFY so other processes
     (and the SSE listener) receive the event. Requires an asyncpg connection.

The SSE router subscribes to all channels and streams events to the frontend.
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from loguru import logger as log

from events.channels import ALL_CHANNELS, Event


class EventBus:
    """Lightweight event bus with PG NOTIFY support."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable[[Event], None]]] = defaultdict(list)
        self._pg_conn = None  # asyncpg connection, set via start()

    # ── In-process pub/sub ──

    def subscribe(self, channel: str, callback: Callable[[Event], None]) -> None:
        """Register a callback for events on *channel*."""
        self._subscribers[channel].append(callback)

    def emit_sync(self, channel: str, payload: dict[str, Any]) -> Event:
        """Emit an event synchronously to in-process subscribers.

        Returns the created Event for chaining / logging.
        """
        event = Event(
            channel=channel,
            payload=payload,
            timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        for cb in self._subscribers.get(channel, []):
            try:
                cb(event)
            except Exception as exc:
                log.warning(
                    "Event subscriber error on {ch}: {e}",
                    ch=channel, e=str(exc),
                )
        return event

    # ── PG NOTIFY (cross-process) ──

    async def start(self, dsn: str) -> None:
        """Connect to PostgreSQL and start listening on all channels.

        Parameters:
            dsn: PostgreSQL connection string (e.g. ``postgresql://grid:pw@localhost/grid``)
        """
        try:
            import asyncpg
        except ImportError:
            log.warning("asyncpg not installed — event bus running in local-only mode")
            return

        try:
            self._pg_conn = await asyncpg.connect(dsn)
            for channel in ALL_CHANNELS:
                await self._pg_conn.add_listener(channel, self._on_pg_notify)
            log.info(
                "Event bus connected — listening on {n} PG channels",
                n=len(ALL_CHANNELS),
            )
        except Exception as exc:
            log.warning("Event bus PG connection failed: {e}", e=str(exc))
            self._pg_conn = None

    async def stop(self) -> None:
        """Disconnect from PostgreSQL."""
        if self._pg_conn:
            try:
                for channel in ALL_CHANNELS:
                    await self._pg_conn.remove_listener(channel, self._on_pg_notify)
                await self._pg_conn.close()
            except Exception:
                pass
            self._pg_conn = None

    def _on_pg_notify(
        self, connection: Any, pid: int, channel: str, payload: str
    ) -> None:
        """Handle incoming PG NOTIFY — deserialize and fan out."""
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            data = {"raw": payload}

        event = Event(
            channel=channel,
            payload=data,
            timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        for cb in self._subscribers.get(channel, []):
            try:
                cb(event)
            except Exception as exc:
                log.warning(
                    "Event subscriber error on {ch}: {e}",
                    ch=channel, e=str(exc),
                )

    async def emit(self, channel: str, payload: dict[str, Any]) -> Event:
        """Emit via PG NOTIFY (cross-process) + local fan-out.

        Falls back to local-only if PG is not connected.
        """
        event = self.emit_sync(channel, payload)

        if self._pg_conn:
            try:
                await self._pg_conn.execute(
                    f"SELECT pg_notify($1, $2)",
                    channel,
                    json.dumps(payload),
                )
            except Exception as exc:
                log.warning("PG NOTIFY failed for {ch}: {e}", ch=channel, e=str(exc))

        return event


# Module-level singleton
bus = EventBus()
