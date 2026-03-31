"""
GRID Intelligence — Signal Adapter Protocol and Registry.

Defines the contract every adapter must satisfy, plus AdapterRegistry
which orchestrates bulk refresh across all registered adapters.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

from loguru import logger as log
from sqlalchemy.engine import Engine

from intelligence.signal_registry import RegisteredSignal, SignalRegistry


@runtime_checkable
class SignalAdapter(Protocol):

    @property
    def source_module(self) -> str: ...

    @property
    def refresh_interval_hours(self) -> float: ...

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]: ...


class AdapterRegistry:

    def __init__(self, adapters: list[SignalAdapter]) -> None:
        self._adapters = list(adapters)

    @property
    def adapters(self) -> list[SignalAdapter]:
        return list(self._adapters)

    def refresh_all(self, engine: Engine) -> dict[str, int]:
        started_at = datetime.now(timezone.utc)
        results: dict[str, int] = {}
        for adapter in self._adapters:
            module = adapter.source_module
            try:
                signals = adapter.extract_signals(engine)
                inserted = SignalRegistry.register(signals, engine)
                results[module] = inserted
                log.info("AdapterRegistry: {mod} -> {n} signals", mod=module, n=len(signals))
            except Exception as exc:
                log.error("AdapterRegistry: {mod} failed - {e}", mod=module, e=exc)
                results[module] = 0
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        log.info("AdapterRegistry: completed {n} adapters in {t:.1f}s", n=len(self._adapters), t=elapsed)
        return results
