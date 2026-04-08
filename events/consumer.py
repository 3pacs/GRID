"""
Durable event consumer -- reads events from Redpanda topics.

Supports multiple consumer groups for fan-out:
  - grid-alerts: alert service reads signals + predictions
  - grid-scoring: hypothesis scoring reads signals
  - grid-canvas: canvas live updates reads canvas events

Usage:
    from events.consumer import consume

    # Blocking consumer loop
    for event in consume("signals", group="grid-alerts"):
        process_signal(event)

    # Or with callback
    consume("signals", group="grid-scoring", callback=process_signal)
"""

from __future__ import annotations

import json
from typing import Any, Callable, Iterator

from loguru import logger as log


def consume(
    topic_key: str,
    group: str = "grid-default",
    callback: Callable[[dict], None] | None = None,
    timeout_ms: int = 1000,
    max_messages: int | None = None,
) -> Iterator[dict]:
    """
    Consume events from a Redpanda topic.

    Args:
        topic_key: Short topic name
        group: Consumer group ID
        callback: Optional callback -- if provided, blocks and calls for each message
        timeout_ms: Poll timeout in milliseconds
        max_messages: Max messages to consume (None = infinite)

    Yields:
        Event dicts (if no callback provided)
    """
    from events.producer import TOPICS

    topic = TOPICS.get(topic_key)
    if not topic:
        log.warning(f"Unknown topic: {topic_key}")
        return

    try:
        from config import settings
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[settings.REDPANDA_BROKER],
            group_id=group,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=timeout_ms if max_messages else -1,
            max_poll_records=100,
        )
    except Exception as e:
        log.warning(f"Redpanda consumer unavailable: {e}")
        return

    count = 0
    try:
        for message in consumer:
            event = message.value
            if callback:
                try:
                    callback(event)
                except Exception as e:
                    log.error(f"Consumer callback error: {e}")
            else:
                yield event

            count += 1
            if max_messages and count >= max_messages:
                break
    finally:
        consumer.close()
        log.info(f"Consumer [{group}] closed after {count} messages from {topic}")


def get_topic_info(topic_key: str) -> dict:
    """Get topic metadata (partitions, offsets, lag)."""
    from events.producer import TOPICS

    topic = TOPICS.get(topic_key)
    if not topic:
        return {"error": f"Unknown topic: {topic_key}"}

    try:
        from config import settings
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=[settings.REDPANDA_BROKER],
            group_id="grid-info",
        )
        partitions = consumer.partitions_for_topic(topic) or set()
        consumer.close()

        return {
            "topic": topic,
            "partitions": len(partitions),
            "available": True,
        }
    except Exception as e:
        return {"topic": topic, "available": False, "error": str(e)}
