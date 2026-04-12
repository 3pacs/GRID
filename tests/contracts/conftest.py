"""Shared fixtures for contracts tests."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

import pytest


class FakeBus:
    """Minimal drop-in for events.bus.EventBus for tests.

    Captures emitted payloads per channel and fans out to registered
    in-process subscribers exactly like the real bus.
    """

    def __init__(self) -> None:
        self._subs: dict[str, list[Callable[[dict], None]]] = defaultdict(list)
        self.emitted: list[tuple[str, dict[str, Any]]] = []

    def subscribe(self, channel: str, cb: Callable[[dict], None]) -> None:
        self._subs[channel].append(cb)

    def emit_sync(self, channel: str, payload: dict[str, Any]):
        self.emitted.append((channel, payload))
        for cb in self._subs.get(channel, []):
            cb({"channel": channel, "payload": payload})
        return payload


@pytest.fixture
def fake_bus() -> FakeBus:
    return FakeBus()
