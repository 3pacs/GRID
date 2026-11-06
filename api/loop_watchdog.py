"""Event-loop lag watchdog for grid-api.

grid-api is one uvicorn process with one asyncio event loop. Anything
synchronous that runs on that loop -- an ``async def`` route doing DB work, a
graph build, an LLM call -- freezes *every* request until it returns. On
2026-11-06 an actor-graph rebuild did exactly that for minutes, and the only
trace was ``DB connection held 114.0s — acquired_by=MainThread``.

This watchdog makes the next blocker name itself: a daemon thread posts a
no-op heartbeat onto the loop every ``interval`` seconds. If the loop has not
run it within ``threshold`` seconds, the watchdog logs (WARNING,
rate-limited) the loop thread's current Python stack from
``sys._current_frames()``, and once the loop recovers it logs how long the
stall lasted.
"""

from __future__ import annotations

import asyncio
import os
import sys
import threading
import time
import traceback

from loguru import logger as log

_STACK_FRAMES = 25


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, "") or default)
    except ValueError:
        return default
    return value if value > 0 else default


class EventLoopWatchdog:
    """Detect and attribute event-loop stalls from a separate thread."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        loop_thread_ident: int,
        *,
        threshold: float = 2.0,
        interval: float = 0.5,
        cooldown: float = 60.0,
    ) -> None:
        self._loop = loop
        self._loop_ident = loop_thread_ident
        self.threshold = threshold
        self.interval = interval
        self.cooldown = cooldown
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_stack_log = float("-inf")
        self.stalls_detected = 0
        self.last_stack: str | None = None

    @classmethod
    def for_running_loop(cls) -> "EventLoopWatchdog":
        """Build a watchdog for the loop running in the calling thread."""
        return cls(
            asyncio.get_running_loop(),
            threading.get_ident(),
            threshold=_env_float("GRID_LOOP_WATCHDOG_THRESHOLD_SECONDS", 2.0),
            interval=_env_float("GRID_LOOP_WATCHDOG_INTERVAL_SECONDS", 0.5),
            cooldown=_env_float("GRID_LOOP_WATCHDOG_COOLDOWN_SECONDS", 60.0),
        )

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="event-loop-watchdog", daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def loop_stack(self) -> str:
        """The loop thread's current Python stack (innermost last)."""
        frame = sys._current_frames().get(self._loop_ident)
        if frame is None:
            return "<loop thread not found>"
        return "".join(traceback.format_stack(frame, limit=_STACK_FRAMES))

    def _run(self) -> None:
        while not self._stop.is_set():
            answered = threading.Event()
            sent_at = time.monotonic()
            try:
                self._loop.call_soon_threadsafe(answered.set)
            except RuntimeError:
                return  # loop closed
            if not answered.wait(self.threshold):
                self._on_stall(sent_at, answered)
            self._stop.wait(self.interval)

    def _on_stall(self, sent_at: float, answered: threading.Event) -> None:
        self.stalls_detected += 1
        now = time.monotonic()
        if now - self._last_stack_log >= self.cooldown:
            self._last_stack_log = now
            stack = self.loop_stack()
            self.last_stack = stack
            log.warning(
                "Event loop blocked > {t:.1f}s — every request is waiting. "
                "Loop thread stack (innermost last):\n{stack}",
                t=self.threshold, stack=stack,
            )
        else:
            log.debug("Event loop blocked > {t:.1f}s (stack log rate-limited)", t=self.threshold)
        # Wait for recovery so one stall is reported once, with its length.
        while not answered.wait(1.0):
            if self._stop.is_set() or self._loop.is_closed():
                return
        log.warning(
            "Event loop recovered after a {s:.1f}s stall",
            s=time.monotonic() - sent_at,
        )
