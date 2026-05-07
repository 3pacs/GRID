"""Tests for the GRID Redpanda durable event stream with PG NOTIFY fallback."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch



# ---------------------------------------------------------------------------
# Topic validation
# ---------------------------------------------------------------------------


class TestTopicValidation:
    """Verify topic registry behaviour."""

    def test_all_topics_present(self):
        from events.producer import TOPICS

        assert "signals" in TOPICS
        assert "predictions" in TOPICS
        assert "alerts" in TOPICS
        assert "canvas" in TOPICS
        assert "ingestion" in TOPICS

    def test_topic_prefix(self):
        from events.producer import TOPICS

        for key, full_name in TOPICS.items():
            assert full_name.startswith("grid."), f"Topic {key} missing grid. prefix"

    def test_emit_unknown_topic_returns_false(self):
        from events.producer import emit

        result = emit("nonexistent_topic", {"data": 1})
        assert result is False


# ---------------------------------------------------------------------------
# Event enrichment
# ---------------------------------------------------------------------------


class TestEventEnrichment:
    """Ensure every emitted event receives required metadata fields."""

    @patch("events.producer._get_producer", return_value=None)
    @patch("events.producer._pg_notify_fallback", return_value=True)
    def test_emit_enriches_event(self, mock_fallback, mock_get_producer):
        from events.producer import emit

        emit("signals", {"ticker": "AAPL", "direction": "bullish"})

        # The fallback should have been called with an enriched event
        assert mock_fallback.called
        args = mock_fallback.call_args
        channel = args[0][0]
        event = args[0][1]

        assert channel == "signals"
        assert "event_id" in event
        assert event["event_id"].startswith("signals-")
        assert "topic" in event
        assert event["topic"] == "grid.signals"
        assert "timestamp" in event
        assert "ticker" in event
        assert event["ticker"] == "AAPL"

    @patch("events.producer._get_producer", return_value=None)
    @patch("events.producer._pg_notify_fallback", return_value=True)
    def test_emit_async_enriches_event(self, mock_fallback, mock_get_producer):
        from events.producer import emit_async

        emit_async("predictions", {"model": "oracle", "score": 0.85})

        assert mock_fallback.called
        event = mock_fallback.call_args[0][1]
        assert event["event_id"].startswith("predictions-")
        assert event["topic"] == "grid.predictions"
        assert event["model"] == "oracle"


# ---------------------------------------------------------------------------
# Redpanda producer (mocked KafkaProducer)
# ---------------------------------------------------------------------------


class TestRedpandaProducer:
    """Test emit via mocked KafkaProducer."""

    def test_emit_sends_to_kafka(self):
        """emit() should call KafkaProducer.send() when Redpanda is available."""
        import events.producer as mod

        mock_future = MagicMock()
        mock_future.get.return_value = None

        mock_producer = MagicMock()
        mock_producer.send.return_value = mock_future

        # Temporarily set the module-level producer
        original_producer = mod._producer
        original_available = mod._available
        try:
            mod._producer = mock_producer
            mod._available = True

            result = mod.emit("alerts", {"severity": "high", "ticker": "TSLA"}, key="TSLA")

            assert result is True
            mock_producer.send.assert_called_once()
            call_kwargs = mock_producer.send.call_args
            assert call_kwargs[0][0] == "grid.alerts"  # topic
            assert call_kwargs[1]["key"] == "TSLA"
        finally:
            mod._producer = original_producer
            mod._available = original_available

    def test_emit_falls_back_on_send_error(self):
        """If KafkaProducer.send() raises, fall back to PG NOTIFY."""
        import events.producer as mod

        mock_future = MagicMock()
        mock_future.get.side_effect = Exception("broker down")

        mock_producer = MagicMock()
        mock_producer.send.return_value = mock_future

        original_producer = mod._producer
        original_available = mod._available
        try:
            mod._producer = mock_producer
            mod._available = True

            with patch.object(mod, "_pg_notify_fallback", return_value=True) as mock_fb:
                result = mod.emit("signals", {"ticker": "SPY"})
                assert result is True
                assert mock_fb.called
        finally:
            mod._producer = original_producer
            mod._available = original_available

    def test_emit_async_fire_and_forget(self):
        """emit_async should not block (no .get() call)."""
        import events.producer as mod

        mock_producer = MagicMock()

        original_producer = mod._producer
        original_available = mod._available
        try:
            mod._producer = mock_producer
            mod._available = True

            result = mod.emit_async("canvas", {"board_id": 1, "action": "add_node"})

            assert result is True
            mock_producer.send.assert_called_once()
            # send().get() should NOT have been called
            mock_producer.send.return_value.get.assert_not_called()
        finally:
            mod._producer = original_producer
            mod._available = original_available


# ---------------------------------------------------------------------------
# PG NOTIFY fallback
# ---------------------------------------------------------------------------


class TestPgNotifyFallback:
    """Test PG NOTIFY fallback when Redpanda is down."""

    @patch("events.producer._get_producer", return_value=None)
    def test_fallback_to_pg_notify(self, mock_get_producer):
        """When Redpanda unavailable, emit should use PG NOTIFY."""
        with patch("events.producer._pg_notify_fallback", return_value=True) as mock_fb:
            from events.producer import emit

            result = emit("ingestion", {"puller": "fred", "status": "complete"})
            assert result is True
            assert mock_fb.called

    def test_pg_notify_truncates_large_payload(self):
        """Payloads > 7500 bytes should be truncated for PG NOTIFY."""
        from events.producer import _pg_notify_fallback

        # Build a large event
        large_event = {
            "event_id": "test-123",
            "topic": "grid.signals",
            "timestamp": "2026-04-08T12:00:00Z",
            "data": "x" * 8000,  # exceeds 7500 byte limit
        }

        mock_cursor = MagicMock()
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.commit = MagicMock()

        # get_connection is a context manager imported via `from db import get_connection`
        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_conn)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        with patch("db.get_connection", return_value=mock_ctx):
            result = _pg_notify_fallback("signals", large_event)

        assert result is True
        # Verify the payload was truncated
        call_args = mock_cursor.execute.call_args[0]
        payload_sent = call_args[1][1]
        parsed = json.loads(payload_sent)
        assert parsed.get("truncated") is True
        assert "data" not in parsed  # large data field stripped

    def test_pg_notify_fallback_handles_db_error(self):
        """If PG NOTIFY also fails, return False gracefully."""
        from events.producer import _pg_notify_fallback

        with patch("db.get_connection", side_effect=Exception("db down")):
            result = _pg_notify_fallback("signals", {"event_id": "test", "topic": "grid.signals"})
            assert result is False


# ---------------------------------------------------------------------------
# Consumer
# ---------------------------------------------------------------------------


class TestConsumer:
    """Test Redpanda consumer with mocked KafkaConsumer."""

    def test_consume_unknown_topic(self):
        """Consuming an unknown topic should yield nothing."""
        from events.consumer import consume

        results = list(consume("bad_topic", max_messages=1))
        assert results == []

    def test_consume_yields_events(self):
        """Consumer should yield deserialized event dicts."""
        import sys

        mock_message_1 = MagicMock()
        mock_message_1.value = {"event_id": "s-1", "ticker": "AAPL"}
        mock_message_2 = MagicMock()
        mock_message_2.value = {"event_id": "s-2", "ticker": "MSFT"}

        mock_consumer_instance = MagicMock()
        mock_consumer_instance.__iter__ = MagicMock(
            return_value=iter([mock_message_1, mock_message_2])
        )
        mock_consumer_instance.close = MagicMock()

        mock_kafka_module = MagicMock()
        mock_kafka_module.KafkaConsumer = MagicMock(return_value=mock_consumer_instance)

        with patch.dict(sys.modules, {"kafka": mock_kafka_module}):
            from events.consumer import consume

            results = list(consume("signals", max_messages=2))

        assert len(results) == 2
        assert results[0]["ticker"] == "AAPL"
        assert results[1]["ticker"] == "MSFT"

    def test_consume_with_callback(self):
        """When callback is provided, it should be called for each message."""
        import sys

        mock_message = MagicMock()
        mock_message.value = {"event_id": "a-1", "severity": "high"}

        mock_consumer_instance = MagicMock()
        mock_consumer_instance.__iter__ = MagicMock(return_value=iter([mock_message]))
        mock_consumer_instance.close = MagicMock()

        mock_kafka_module = MagicMock()
        mock_kafka_module.KafkaConsumer = MagicMock(return_value=mock_consumer_instance)

        callback_results = []

        def my_callback(event):
            callback_results.append(event)

        with patch.dict(sys.modules, {"kafka": mock_kafka_module}):
            from events.consumer import consume

            # consume with callback: the generator still needs to be iterated
            for _ in consume("alerts", callback=my_callback, max_messages=1):
                pass

        assert len(callback_results) == 1
        assert callback_results[0]["severity"] == "high"

    def test_consume_handles_unavailable_broker(self):
        """If Redpanda is down (import fails), consume should return empty iterator."""
        from events.consumer import consume

        # kafka-python-ng is not installed in the test environment,
        # so the import will fail naturally and return an empty iterator
        results = list(consume("signals", max_messages=1))
        assert results == []


# ---------------------------------------------------------------------------
# Topic info
# ---------------------------------------------------------------------------


class TestTopicInfo:
    """Test get_topic_info utility."""

    def test_unknown_topic_returns_error(self):
        from events.consumer import get_topic_info

        result = get_topic_info("nonexistent")
        assert "error" in result

    def test_topic_info_returns_structure(self):
        """get_topic_info should return topic name and availability."""
        import sys

        mock_consumer_instance = MagicMock()
        mock_consumer_instance.partitions_for_topic.return_value = {0, 1, 2}
        mock_consumer_instance.close = MagicMock()

        mock_kafka_module = MagicMock()
        mock_kafka_module.KafkaConsumer = MagicMock(return_value=mock_consumer_instance)

        with patch.dict(sys.modules, {"kafka": mock_kafka_module}):
            from events.consumer import get_topic_info

            result = get_topic_info("signals")

        assert result["topic"] == "grid.signals"
        assert result["partitions"] == 3
        assert result["available"] is True

    def test_topic_info_handles_broker_down(self):
        """If broker is down, should return available=False."""
        with patch("events.consumer.KafkaConsumer", side_effect=Exception("connection refused"), create=True):
            from events.consumer import get_topic_info

            result = get_topic_info("signals")

        assert result["available"] is False
        assert "error" in result


# ---------------------------------------------------------------------------
# flush / close
# ---------------------------------------------------------------------------


class TestLifecycle:
    """Test producer flush and close."""

    def test_flush_calls_producer(self):
        import events.producer as mod

        mock_producer = MagicMock()
        original = mod._producer
        try:
            mod._producer = mock_producer
            mod.flush()
            mock_producer.flush.assert_called_once_with(timeout=10)
        finally:
            mod._producer = original

    def test_close_flushes_and_closes(self):
        import events.producer as mod

        mock_producer = MagicMock()
        original_producer = mod._producer
        original_available = mod._available
        try:
            mod._producer = mock_producer
            mod._available = True
            mod.close()

            mock_producer.flush.assert_called_once_with(timeout=5)
            mock_producer.close.assert_called_once_with(timeout=5)
            assert mod._producer is None
            assert mod._available is None
        finally:
            mod._producer = original_producer
            mod._available = original_available

    def test_flush_noop_when_no_producer(self):
        import events.producer as mod

        original = mod._producer
        try:
            mod._producer = None
            mod.flush()  # Should not raise
        finally:
            mod._producer = original

    def test_close_noop_when_no_producer(self):
        import events.producer as mod

        original_producer = mod._producer
        original_available = mod._available
        try:
            mod._producer = None
            mod._available = None
            mod.close()  # Should not raise
        finally:
            mod._producer = original_producer
            mod._available = original_available


# ---------------------------------------------------------------------------
# __init__ re-exports
# ---------------------------------------------------------------------------


class TestInitExports:
    """Verify events package exports the expected names."""

    def test_exports_available(self):
        from events import emit, emit_async, flush, close, consume, get_topic_info, TOPICS

        assert callable(emit)
        assert callable(emit_async)
        assert callable(flush)
        assert callable(close)
        assert callable(consume)
        assert callable(get_topic_info)
        assert isinstance(TOPICS, dict)
