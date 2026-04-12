"""Background retry scheduler for dead-letter entries.

Periodically scans for due retries and re-invokes their handlers. Successful
retries are marked resolved. Failures bump the retry counter and schedule
the next attempt per ``RETRY_SCHEDULE`` in ``contracts.dead_letter``.
"""
from __future__ import annotations

import threading
from typing import Any

from loguru import logger as log

from contracts.dead_letter import (
    DeadLetterEntry,
    bump_retry,
    mark_resolved,
    pending_retries,
)
from contracts.router import resolve_handler
from contracts.schemas import ALL_CONTRACTS


_CONTRACTS_BY_NAME: dict[str, type] = {cls.__name__: cls for cls in ALL_CONTRACTS}


class RetryScheduler:
    """Runs dead-letter retries on a fixed cadence."""

    def __init__(self, engine: Any, poll_interval_s: float = 30.0) -> None:
        self._engine = engine
        self._poll_interval = poll_interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="contracts-retry", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception as exc:
                log.warning("retry scheduler loop error: {e}", e=str(exc))
            self._stop.wait(self._poll_interval)

    def run_once(self) -> None:
        entries = pending_retries(self._engine)
        for entry in entries:
            self._attempt(entry)

    def _attempt(self, entry: DeadLetterEntry) -> None:
        contract_cls = _CONTRACTS_BY_NAME.get(entry.contract_type)
        if contract_cls is None:
            log.warning(
                "retry: unknown contract {ct} on entry {id}",
                ct=entry.contract_type, id=entry.id,
            )
            return

        try:
            contract = contract_cls(**entry.payload)
            handler = resolve_handler(entry.consumer)
            handler(contract, engine=self._engine)
        except Exception as exc:
            log.info(
                "retry failed for entry {id} (attempt {rc}): {e}",
                id=entry.id, rc=entry.retry_count, e=str(exc),
            )
            bump_retry(self._engine, entry.id, entry.retry_count)
            return

        mark_resolved(self._engine, entry.id)
