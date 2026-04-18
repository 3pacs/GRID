"""
GRID Intelligence — Signal Adapter Protocol, BaseAdapter, and Registry.

SignalAdapter: structural Protocol every adapter satisfies.
BaseAdapter: template-method base handling try/except + logging boilerplate.
Subclasses set SOURCE_MODULE / REFRESH_HOURS / LOG_NAME and implement
_build_signals(engine, now) -> list[RegisteredSignal].
AdapterRegistry: orchestrates bulk refresh.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
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


# ── Shared helpers ───────────────────────────────────────────────────────

def sid(*parts: str) -> str:
    """Deterministic 16-char signal ID from colon-joined parts."""
    return hashlib.sha1(
        ":".join(parts).encode(),
        usedforsecurity=False,
    ).hexdigest()[:16]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


# ── BaseAdapter ──────────────────────────────────────────────────────────

class BaseAdapter:
    """Template base for DB-backed signal adapters.

    Subclasses override _build_signals and set SOURCE_MODULE, REFRESH_HOURS,
    and optionally LOG_NAME (defaults to class name).
    """

    SOURCE_MODULE: str = ""
    REFRESH_HOURS: float = 0.0
    LOG_NAME: str | None = None

    @property
    def source_module(self) -> str:
        return self.SOURCE_MODULE

    @property
    def refresh_interval_hours(self) -> float:
        return self.REFRESH_HOURS

    @property
    def _log_name(self) -> str:
        return self.LOG_NAME or self.__class__.__name__

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = now_utc()
        try:
            signals = self._build_signals(engine, now)
        except Exception as exc:
            log.error("{name}: {e}", name=self._log_name, e=exc)
            return []
        log.info("{name}: {n} signals", name=self._log_name, n=len(signals))
        return signals

    def _build_signals(self, engine: Engine, now: datetime) -> list[RegisteredSignal]:
        raise NotImplementedError


# ── Registry ─────────────────────────────────────────────────────────────

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
