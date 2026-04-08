"""Tests for the GRID event bus."""

import json
from datetime import datetime

import pytest

from events.channels import (
    ALL_CHANNELS, SIGNAL_FIRE, REGIME_CHANGE, Event,
)
from events.bus import EventBus


# ── Event dataclass tests ──

def test_event_creation():
    e = Event(channel=SIGNAL_FIRE, payload={"ticker": "AAPL"}, timestamp="2026-04-07T12:00:00Z")
    assert e.channel == SIGNAL_FIRE
    assert e.payload["ticker"] == "AAPL"


def test_event_immutability():
    e = Event(channel=SIGNAL_FIRE, payload={}, timestamp="2026-04-07T12:00:00Z")
    with pytest.raises(AttributeError):
        e.channel = "other"


def test_event_to_sse():
    e = Event(channel=REGIME_CHANGE, payload={"from": "GROWTH", "to": "FRAGILE"}, timestamp="2026-04-07T12:00:00Z")
    sse = e.to_sse()
    assert sse.startswith(f"event: {REGIME_CHANGE}\n")
    assert "data: " in sse
    assert sse.endswith("\n\n")
    data_line = sse.split("data: ")[1].strip()
    parsed = json.loads(data_line)
    assert parsed["channel"] == REGIME_CHANGE
    assert parsed["payload"]["to"] == "FRAGILE"


def test_all_channels_has_8_entries():
    assert len(ALL_CHANNELS) == 8
    for ch in ALL_CHANNELS:
        assert ch.startswith("grid_")


# ── EventBus tests ──

def test_bus_subscribe_and_receive():
    """In-process subscribers receive emitted events."""
    bus = EventBus()
    received = []
    bus.subscribe(SIGNAL_FIRE, lambda e: received.append(e))
    bus.emit_sync(SIGNAL_FIRE, {"ticker": "NVDA", "direction": "buy"})
    assert len(received) == 1
    assert received[0].payload["ticker"] == "NVDA"


def test_bus_subscribe_filters_channels():
    """Subscriber only receives events for their channel."""
    bus = EventBus()
    received = []
    bus.subscribe(SIGNAL_FIRE, lambda e: received.append(e))
    bus.emit_sync(REGIME_CHANGE, {"from": "GROWTH", "to": "FRAGILE"})
    assert len(received) == 0


def test_bus_multiple_subscribers():
    """Multiple subscribers on same channel all receive the event."""
    bus = EventBus()
    a, b = [], []
    bus.subscribe(SIGNAL_FIRE, lambda e: a.append(e))
    bus.subscribe(SIGNAL_FIRE, lambda e: b.append(e))
    bus.emit_sync(SIGNAL_FIRE, {"ticker": "SPY"})
    assert len(a) == 1
    assert len(b) == 1


def test_bus_emit_sync_creates_timestamp():
    """emit_sync auto-generates ISO 8601 UTC timestamp."""
    bus = EventBus()
    received = []
    bus.subscribe(SIGNAL_FIRE, lambda e: received.append(e))
    bus.emit_sync(SIGNAL_FIRE, {"ticker": "QQQ"})
    ts = received[0].timestamp
    # Should parse as valid ISO datetime
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    assert dt.tzinfo is not None
