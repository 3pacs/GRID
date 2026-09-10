"""GRID Event Bus — PG LISTEN/NOTIFY wrapper with in-process fan-out.

Dual-mode operation:
  1. **In-process**: ``emit_sync()`` fans out to local subscribers immediately.
     Used by intelligence modules running in the API process.
  2. **Cross-process**: another process (``grid-hermes``, ``grid-scheduler``)
     issues ``pg_notify`` — ``contracts/emit.py`` does so inside the same
     transaction that writes ``contracts_audit`` — and this process's
     listener, started with :meth:`EventBus.start`, fans the payload out to
     subscribers that opted in with ``remote=True``.

Why the ``remote`` flag: the contracts dispatcher subscribes in-process and
runs handlers with side effects (trust updates, journal mirrors). It must
see each contract exactly once, so it never receives listener-delivered
copies — including the echo of this process's own ``pg_notify``. The SSE
router, which only forwards to browsers, subscribes with ``remote=True``
and de-duplicates echoes by ``event_id`` (:class:`RecentEventIds`).

The SSE router subscribes to all stream channels and streams events to the
frontend.
"""

from __future__ import annotations

import json
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, NamedTuple

from loguru import logger as log

from events.channels import ALL_CHANNELS, Event

Callback = Callable[[Event], None]


class _Subscriber(NamedTuple):
    callback: Callback
    remote: bool


class RecentEventIds:
    """Bounded, insertion-ordered set of recently seen event ids.

    Used by listener-side subscribers to drop the duplicate that an
    in-process ``emit_sync`` plus its own ``pg_notify`` echo would otherwise
    produce. Ids without a value are never treated as duplicates.
    """

    def __init__(self, capacity: int = 512) -> None:
        self._seen: "OrderedDict[str, None]" = OrderedDict()
        self._capacity = max(1, int(capacity))

    def seen_before(self, event_id: Any) -> bool:
        """Record ``event_id``; return True if it had already been recorded."""
        if not event_id:
            return False
        key = str(event_id)
        if key in self._seen:
            return True
        self._seen[key] = None
        while len(self._seen) > self._capacity:
            self._seen.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._seen)


def normalize_listen_dsn(dsn: str) -> str:
    """Turn a SQLAlchemy-style URL into one asyncpg accepts.

    ``config.Settings.DB_URL`` is a plain ``postgresql://`` URL, but callers
    sometimes hold ``postgresql+psycopg2://``; asyncpg rejects the driver
    suffix.
    """
    for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://", "postgresql+asyncpg://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix):]
    return dsn


class EventBus:
    """Lightweight event bus with PG NOTIFY support."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[_Subscriber]] = defaultdict(list)
        self._pg_conn = None  # asyncpg connection, set via start()
        self._channels: tuple[str, ...] = tuple(ALL_CHANNELS)

    # ── In-process pub/sub ──

    def subscribe(self, channel: str, callback: Callback, *, remote: bool = False) -> None:
        """Register a callback for events on *channel*.

        ``remote=True`` opts the callback in to listener-delivered events
        (other processes' ``pg_notify``, and this process's own echo — see
        :class:`RecentEventIds`). The default only receives in-process emits.
        """
        self._subscribers[channel].append(_Subscriber(callback, bool(remote)))

    def unsubscribe(self, channel: str, callback: Callback) -> None:
        """Remove every registration of *callback* on *channel* (no-op if absent)."""
        subs = self._subscribers.get(channel)
        if not subs:
            return
        # Equality, not identity: bound methods are re-created on every
        # attribute access, so ``obj.method is obj.method`` is False.
        self._subscribers[channel] = [s for s in subs if s.callback != callback]

    def emit_sync(self, channel: str, payload: dict[str, Any]) -> Event:
        """Emit an event synchronously to in-process subscribers.

        Returns the created Event for chaining / logging.
        """
        event = Event(
            channel=channel,
            payload=payload,
            timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        self._fan_out(event, remote_only=False)
        return event

    def _fan_out(self, event: Event, *, remote_only: bool) -> None:
        for sub in list(self._subscribers.get(event.channel, [])):
            if remote_only and not sub.remote:
                continue
            try:
                sub.callback(event)
            except Exception as exc:
                log.warning(
                    "Event subscriber error on {ch}: {e}",
                    ch=event.channel, e=str(exc),
                )

    # ── PG NOTIFY (cross-process) ──

    async def start(self, dsn: str, channels: Iterable[str] | None = None) -> None:
        """Connect to PostgreSQL and start listening.

        Parameters:
            dsn: PostgreSQL connection string (e.g. ``postgresql://grid:pw@localhost/grid``).
            channels: Channels to LISTEN on. Defaults to the legacy
                ``events.channels.ALL_CHANNELS``; the API passes the union
                with the typed contract channels (``api/routers/sse.py``).
        """
        if channels is not None:
            self._channels = tuple(dict.fromkeys(channels))

        try:
            import asyncpg
        except ImportError:
            log.warning("asyncpg not installed — event bus running in local-only mode")
            return

        try:
            self._pg_conn = await asyncpg.connect(normalize_listen_dsn(dsn))
            for channel in self._channels:
                await self._pg_conn.add_listener(channel, self._on_pg_notify)
            log.info(
                "Event bus connected — listening on {n} PG channels",
                n=len(self._channels),
            )
        except Exception as exc:
            log.warning("Event bus PG connection failed: {e}", e=str(exc))
            self._pg_conn = None

    async def stop(self) -> None:
        """Disconnect from PostgreSQL."""
        if self._pg_conn:
            try:
                for channel in self._channels:
                    await self._pg_conn.remove_listener(channel, self._on_pg_notify)
                await self._pg_conn.close()
            except Exception:
                pass
            self._pg_conn = None

    @property
    def listening(self) -> bool:
        return self._pg_conn is not None

    def _on_pg_notify(
        self, connection: Any, pid: int, channel: str, payload: str
    ) -> None:
        """Handle incoming PG NOTIFY — deserialize and fan out to remote subscribers."""
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            data = {"raw": payload}

        event = Event(
            channel=channel,
            payload=data,
            timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        self._fan_out(event, remote_only=True)

    async def emit(self, channel: str, payload: dict[str, Any]) -> Event:
        """Emit via PG NOTIFY (cross-process) + local fan-out.

        Falls back to local-only if PG is not connected.
        """
        event = self.emit_sync(channel, payload)

        if self._pg_conn:
            try:
                await self._pg_conn.execute(
                    "SELECT pg_notify($1, $2)",
                    channel,
                    json.dumps(payload),
                )
            except Exception as exc:
                log.warning("PG NOTIFY failed for {ch}: {e}", ch=channel, e=str(exc))

        return event


# Module-level singleton
bus = EventBus()
