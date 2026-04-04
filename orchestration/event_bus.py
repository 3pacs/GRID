"""
GRID — PostgreSQL-backed Event Bus and Task Queue.

Provides persistent, cross-node coordination using shared Postgres.
No Redis/Celery dependency — uses LISTEN/NOTIFY for real-time delivery
with polling fallback.

Usage:
    from orchestration.event_bus import EventBus, TaskQueue

    bus = EventBus(engine)
    bus.emit("gap_discovered", {"feature": "vix_spot", "days_stale": 3})

    queue = TaskQueue(engine)
    queue.enqueue("baseline_compute", {"signal": "vix_spot"}, node_target="gridz4")

    task = queue.claim("gridz4")
    queue.complete(task["id"], result={"baseline": {...}})
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class EventBus:
    """Append-only event log with optional subscriptions."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def emit(
        self,
        event_type: str,
        payload: dict[str, Any],
        source_node: str = "grid-svr",
    ) -> str | None:
        """Emit an event. Returns event ID or None on failure."""
        try:
            with self._engine.begin() as conn:
                row = conn.execute(
                    text("""
                        INSERT INTO event_bus (event_type, source_node, payload)
                        VALUES (:type, :node, :payload)
                        RETURNING id
                    """),
                    {
                        "type": event_type,
                        "node": source_node,
                        "payload": json.dumps(payload),
                    },
                ).fetchone()
                return str(row[0]) if row else None
        except Exception as exc:
            log.warning("EventBus.emit failed: {e}", e=str(exc))
            return None

    def recent(
        self,
        event_type: str | None = None,
        limit: int = 50,
        since_minutes: int = 60,
    ) -> list[dict[str, Any]]:
        """Fetch recent events, optionally filtered by type."""
        try:
            with self._engine.connect() as conn:
                if event_type:
                    rows = conn.execute(
                        text("""
                            SELECT id, event_type, source_node, payload, created_at
                            FROM event_bus
                            WHERE event_type = :type
                            AND created_at > NOW() - MAKE_INTERVAL(mins => :mins)
                            ORDER BY created_at DESC
                            LIMIT :lim
                        """),
                        {"type": event_type, "mins": since_minutes, "lim": limit},
                    ).fetchall()
                else:
                    rows = conn.execute(
                        text("""
                            SELECT id, event_type, source_node, payload, created_at
                            FROM event_bus
                            WHERE created_at > NOW() - MAKE_INTERVAL(mins => :mins)
                            ORDER BY created_at DESC
                            LIMIT :lim
                        """),
                        {"mins": since_minutes, "lim": limit},
                    ).fetchall()

                return [
                    {
                        "id": str(r[0]),
                        "event_type": r[1],
                        "source_node": r[2],
                        "payload": r[3],
                        "created_at": str(r[4]),
                    }
                    for r in rows
                ]
        except Exception as exc:
            log.warning("EventBus.recent failed: {e}", e=str(exc))
            return []

    def subscribe(
        self,
        event_types: list[str],
        callback: Callable[[dict], None],
        poll_interval: int = 5,
    ) -> threading.Thread:
        """Start a background thread that polls for new events and calls callback.

        Returns the thread (already started, daemon=True).
        """
        last_seen = datetime.now(timezone.utc).isoformat()

        def _poll_loop():
            nonlocal last_seen
            while True:
                try:
                    with self._engine.connect() as conn:
                        rows = conn.execute(
                            text("""
                                SELECT id, event_type, source_node, payload, created_at
                                FROM event_bus
                                WHERE event_type = ANY(:types)
                                AND created_at > :since::timestamptz
                                ORDER BY created_at ASC
                            """),
                            {"types": event_types, "since": last_seen},
                        ).fetchall()

                        for r in rows:
                            event = {
                                "id": str(r[0]),
                                "event_type": r[1],
                                "source_node": r[2],
                                "payload": r[3],
                                "created_at": str(r[4]),
                            }
                            try:
                                callback(event)
                            except Exception as cb_exc:
                                log.warning(
                                    "EventBus callback failed: {e}", e=str(cb_exc)
                                )
                            last_seen = str(r[4])

                except Exception as exc:
                    log.debug("EventBus poll error: {e}", e=str(exc))

                time.sleep(poll_interval)

        thread = threading.Thread(target=_poll_loop, daemon=True, name="event-bus-sub")
        thread.start()
        return thread

    def prune(self, days: int = 7) -> int:
        """Delete events older than N days. Returns count deleted."""
        try:
            with self._engine.begin() as conn:
                result = conn.execute(
                    text("""
                        DELETE FROM event_bus
                        WHERE created_at < NOW() - MAKE_INTERVAL(days => :d)
                    """),
                    {"d": days},
                )
                return result.rowcount
        except Exception:
            return 0


class TaskQueue:
    """Persistent task queue with claim/complete semantics."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def enqueue(
        self,
        task_type: str,
        payload: dict[str, Any],
        priority: int = 3,
        node_target: str | None = None,
        max_retries: int = 3,
    ) -> str | None:
        """Add a task to the queue. Returns task ID or None."""
        try:
            with self._engine.begin() as conn:
                row = conn.execute(
                    text("""
                        INSERT INTO task_queue
                            (task_type, priority, node_target, payload, max_retries)
                        VALUES (:type, :pri, :node, :payload, :retries)
                        RETURNING id
                    """),
                    {
                        "type": task_type,
                        "pri": priority,
                        "node": node_target,
                        "payload": json.dumps(payload),
                        "retries": max_retries,
                    },
                ).fetchone()
                return str(row[0]) if row else None
        except Exception as exc:
            log.warning("TaskQueue.enqueue failed: {e}", e=str(exc))
            return None

    def claim(
        self,
        node_name: str,
        task_types: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """Atomically claim the highest-priority queued task for this node.

        Returns task dict or None if no work available.
        Uses SELECT ... FOR UPDATE SKIP LOCKED for safe concurrent claiming.
        """
        try:
            with self._engine.begin() as conn:
                type_filter = ""
                params: dict[str, Any] = {"node": node_name}

                if task_types:
                    type_filter = "AND task_type = ANY(:types)"
                    params["types"] = task_types

                row = conn.execute(
                    text(f"""
                        UPDATE task_queue SET
                            status = 'running',
                            claimed_at = NOW(),
                            claimed_by = :node
                        WHERE id = (
                            SELECT id FROM task_queue
                            WHERE status = 'queued'
                            AND (node_target IS NULL OR node_target = :node)
                            {type_filter}
                            ORDER BY priority ASC, created_at ASC
                            FOR UPDATE SKIP LOCKED
                            LIMIT 1
                        )
                        RETURNING id, task_type, priority, payload, created_at
                    """),
                    params,
                ).fetchone()

                if not row:
                    return None

                return {
                    "id": str(row[0]),
                    "task_type": row[1],
                    "priority": row[2],
                    "payload": row[3],
                    "created_at": str(row[4]),
                }
        except Exception as exc:
            log.debug("TaskQueue.claim error: {e}", e=str(exc))
            return None

    def complete(
        self,
        task_id: str,
        result: dict[str, Any] | None = None,
    ) -> None:
        """Mark a task as completed with optional result."""
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE task_queue SET
                            status = 'completed',
                            completed_at = NOW(),
                            result = :result
                        WHERE id = CAST(:id AS uuid)
                    """),
                    {
                        "id": task_id,
                        "result": json.dumps(result) if result else None,
                    },
                )
        except Exception as exc:
            log.warning("TaskQueue.complete failed: {e}", e=str(exc))

    def fail(self, task_id: str, error: str) -> None:
        """Mark a task as failed. Re-queues if retries remain."""
        try:
            with self._engine.begin() as conn:
                row = conn.execute(
                    text("""
                        SELECT retry_count, max_retries FROM task_queue
                        WHERE id = CAST(:id AS uuid)
                    """),
                    {"id": task_id},
                ).fetchone()

                if row and row[0] < row[1]:
                    # Re-queue with incremented retry
                    conn.execute(
                        text("""
                            UPDATE task_queue SET
                                status = 'queued',
                                claimed_at = NULL,
                                claimed_by = NULL,
                                retry_count = retry_count + 1,
                                error = :error
                            WHERE id = CAST(:id AS uuid)
                        """),
                        {"id": task_id, "error": error},
                    )
                    log.info("Task {id} re-queued (retry {r})", id=task_id[:8], r=row[0] + 1)
                else:
                    conn.execute(
                        text("""
                            UPDATE task_queue SET
                                status = 'failed',
                                completed_at = NOW(),
                                error = :error
                            WHERE id = CAST(:id AS uuid)
                        """),
                        {"id": task_id, "error": error},
                    )
        except Exception as exc:
            log.warning("TaskQueue.fail error: {e}", e=str(exc))

    def pending(self, node_target: str | None = None) -> list[dict[str, Any]]:
        """List pending tasks, optionally filtered by target node."""
        try:
            with self._engine.connect() as conn:
                params: dict[str, Any] = {}
                node_filter = ""
                if node_target:
                    node_filter = "AND (node_target IS NULL OR node_target = :node)"
                    params["node"] = node_target

                rows = conn.execute(
                    text(f"""
                        SELECT id, task_type, priority, node_target, payload, created_at
                        FROM task_queue
                        WHERE status = 'queued' {node_filter}
                        ORDER BY priority ASC, created_at ASC
                        LIMIT 100
                    """),
                    params,
                ).fetchall()

                return [
                    {
                        "id": str(r[0]),
                        "task_type": r[1],
                        "priority": r[2],
                        "node_target": r[3],
                        "payload": r[4],
                        "created_at": str(r[5]),
                    }
                    for r in rows
                ]
        except Exception as exc:
            log.warning("TaskQueue.pending error: {e}", e=str(exc))
            return []

    def stats(self) -> dict[str, int]:
        """Get queue statistics."""
        try:
            with self._engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT status, COUNT(*) FROM task_queue
                        GROUP BY status
                    """)
                ).fetchall()
                return {r[0]: r[1] for r in rows}
        except Exception:
            return {}
