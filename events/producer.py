"""
Durable event producer -- sends structured events to Redpanda topics.

Falls back to PG NOTIFY if Redpanda is unavailable (graceful degradation).

Topics:
  - grid.signals     -- new signals ingested
  - grid.predictions -- new predictions or score updates
  - grid.alerts      -- triggered alerts
  - grid.canvas      -- canvas board changes
  - grid.ingestion   -- data puller lifecycle events

Usage:
    from events.producer import emit

    emit("signals", {
        "event_type": "signal_ingested",
        "signal_type": "congressional",
        "ticker": "AAPL",
        "actor": "Nancy Pelosi",
        "direction": "bullish",
        "timestamp": "2026-04-08T12:00:00Z",
    })
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from loguru import logger as log


# Topic registry -- all valid topics
TOPICS = {
    "signals": "grid.signals",
    "predictions": "grid.predictions",
    "alerts": "grid.alerts",
    "canvas": "grid.canvas",
    "ingestion": "grid.ingestion",
}

_producer = None
_available = None


def _get_producer():
    """Lazy-init Kafka producer for Redpanda."""
    global _producer, _available

    if _producer is not None:
        return _producer

    try:
        from config import settings

        if not settings.REDPANDA_ENABLED:
            log.info("Redpanda disabled in config")
            _available = False
            return None

        from kafka import KafkaProducer

        _producer = KafkaProducer(
            bootstrap_servers=[settings.REDPANDA_BROKER],
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_block_ms=5000,
            request_timeout_ms=10000,
        )
        _available = True
        log.info(f"Redpanda producer connected: {settings.REDPANDA_BROKER}")
    except Exception as e:
        log.warning(f"Redpanda unavailable, falling back to PG NOTIFY: {e}")
        _producer = None
        _available = False

    return _producer


def emit(topic_key: str, payload: dict, key: str | None = None) -> bool:
    """
    Emit an event to a Redpanda topic.

    Args:
        topic_key: Short topic name (signals, predictions, alerts, canvas, ingestion)
        payload: Event payload dict (will be JSON-serialized)
        key: Optional partition key (e.g., ticker symbol for ordering)

    Returns:
        True if sent successfully, False if fell back to PG NOTIFY or failed.
    """
    topic = TOPICS.get(topic_key)
    if not topic:
        log.warning(f"Unknown topic key: {topic_key}. Valid: {list(TOPICS.keys())}")
        return False

    # Enrich payload with metadata
    event = {
        "event_id": f"{topic_key}-{int(time.time() * 1000)}",
        "topic": topic,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **payload,
    }

    producer = _get_producer()

    if producer:
        try:
            future = producer.send(topic, value=event, key=key)
            future.get(timeout=5)  # Block until sent
            return True
        except Exception as e:
            log.warning(f"Redpanda send failed, falling back: {e}")

    # Fallback to PG NOTIFY
    return _pg_notify_fallback(topic_key, event)


def emit_async(topic_key: str, payload: dict, key: str | None = None) -> bool:
    """Fire-and-forget event emission (no blocking wait)."""
    topic = TOPICS.get(topic_key)
    if not topic:
        return False

    event = {
        "event_id": f"{topic_key}-{int(time.time() * 1000)}",
        "topic": topic,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **payload,
    }

    producer = _get_producer()
    if producer:
        try:
            producer.send(topic, value=event, key=key)
            return True
        except Exception:
            pass

    return _pg_notify_fallback(topic_key, event)


def _pg_notify_fallback(channel: str, event: dict) -> bool:
    """Fall back to PostgreSQL LISTEN/NOTIFY."""
    try:
        from db import get_connection

        payload_str = json.dumps(event, default=str)
        # PG NOTIFY has 8000 byte limit -- truncate if needed
        if len(payload_str) > 7500:
            event_slim = {
                "event_id": event.get("event_id"),
                "topic": event.get("topic"),
                "timestamp": event.get("timestamp"),
                "truncated": True,
            }
            payload_str = json.dumps(event_slim, default=str)

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_notify(%s, %s)",
                    (f"grid_{channel}", payload_str),
                )
            conn.commit()
        return True
    except Exception as e:
        log.error(f"PG NOTIFY fallback also failed: {e}")
        return False


def flush():
    """Flush pending messages (call before shutdown)."""
    if _producer:
        try:
            _producer.flush(timeout=10)
        except Exception:
            pass


def close():
    """Close the producer connection."""
    global _producer, _available
    if _producer:
        try:
            _producer.flush(timeout=5)
            _producer.close(timeout=5)
        except Exception:
            pass
        _producer = None
        _available = None
