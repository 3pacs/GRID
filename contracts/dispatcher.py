"""Contract dispatcher.

Subscribes to every channel in ``contracts.router.ROUTES``, validates raw
payloads against the registered Pydantic schemas, and invokes each handler
in a bounded thread pool. Failures are written to the dead-letter store.
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, Callable
from uuid import UUID

from loguru import logger as log
from pydantic import ValidationError

from contracts.channels import channel_for, contract_for_channel
from contracts.router import ROUTES, resolve_handler
from contracts.schemas import BaseContract


DeadLetterWriter = Callable[..., Any]


class Dispatcher:
    """Forwards contract events from the bus to registered handlers."""

    def __init__(
        self,
        bus,
        engine,
        dead_letter_writer: DeadLetterWriter,
        pool_size: int = 8,
    ) -> None:
        self._bus = bus
        self._engine = engine
        self._write_dead_letter = dead_letter_writer
        self._pool = ThreadPoolExecutor(max_workers=pool_size)
        self._pending: set = set()
        self._pending_lock = threading.Lock()

    # ---- lifecycle ----

    def start(self) -> None:
        """Subscribe to every channel represented in ROUTES, plus every
        contract channel (so schema-invalid payloads on unmapped channels
        still land in dead-letter)."""
        subscribed: set[str] = set()
        for contract_type in ROUTES:
            ch = channel_for(contract_type)
            self._bus.subscribe(ch, self._on_event)
            subscribed.add(ch)

        # Also subscribe to every known contract channel — Phase 1 ROUTES is
        # empty, so we still want validation to fire for any rogue payload.
        from contracts.schemas import ALL_CONTRACTS
        for cls in ALL_CONTRACTS:
            ch = channel_for(cls)
            if ch not in subscribed:
                self._bus.subscribe(ch, self._on_event)

    def wait_idle(self, timeout: float = 5.0) -> None:
        """Block until every in-flight handler has completed.

        Used by tests to assert on handler side-effects after emitting.
        """
        with self._pending_lock:
            pending = list(self._pending)
        if pending:
            wait(pending, timeout=timeout)

    # ---- event handling ----

    def _on_event(self, event) -> None:
        # Events from FakeBus are dicts; events from the real bus are
        # Event dataclass instances with a ``.payload`` attribute.
        if isinstance(event, dict):
            channel = event["channel"]
            raw_payload = event["payload"]
        else:
            channel = event.channel
            raw_payload = event.payload

        contract_cls = contract_for_channel(channel)
        if contract_cls is None:
            log.warning("dispatcher: unknown channel {ch}", ch=channel)
            return

        try:
            contract = contract_cls(**raw_payload)
        except ValidationError as e:
            self._write_dead_letter(
                event_id=_safe_uuid(raw_payload.get("event_id")),
                contract_type=contract_cls.__name__,
                payload=raw_payload,
                consumer="<schema>",
                error_type="SCHEMA_INVALID",
                error_detail=str(e),
                correlation_id=_safe_uuid(raw_payload.get("correlation_id")),
            )
            return

        for handler_path in ROUTES.get(contract_cls, []):
            self._submit(contract, handler_path)

    def _submit(self, contract: BaseContract, handler_path: str) -> None:
        fut = self._pool.submit(self._invoke, contract, handler_path)
        with self._pending_lock:
            self._pending.add(fut)
        fut.add_done_callback(self._drop_pending)

    def _drop_pending(self, fut) -> None:
        with self._pending_lock:
            self._pending.discard(fut)

    def _invoke(self, contract: BaseContract, handler_path: str) -> None:
        try:
            handler = resolve_handler(handler_path)
            handler(contract, engine=self._engine)
        except Exception as exc:
            self._write_dead_letter(
                event_id=contract.event_id,
                contract_type=type(contract).__name__,
                payload=contract.model_dump(mode="json"),
                consumer=handler_path,
                error_type="CONSUMER_EXCEPTION",
                error_detail=f"{type(exc).__name__}: {exc}",
                correlation_id=contract.correlation_id,
            )


def _safe_uuid(value) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        return None
