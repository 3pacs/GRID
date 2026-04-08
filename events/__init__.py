"""
GRID event system -- durable event streaming via Redpanda with PG NOTIFY fallback.

Topics:
  - signals: new signal ingested
  - predictions: prediction created/scored
  - alerts: alert triggered
  - canvas: canvas board updated
  - ingestion: data puller lifecycle
"""

from events.producer import emit, emit_async, flush, close, TOPICS
from events.consumer import consume, get_topic_info

__all__ = ["emit", "emit_async", "flush", "close", "consume", "get_topic_info", "TOPICS"]
