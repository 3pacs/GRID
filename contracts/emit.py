"""Emit helpers for the contracts layer.

``emit(contract)`` writes the contract to ``contracts_audit`` and forwards the
serialised payload to the existing event bus. ``pull_lifecycle()`` is a
context manager that wraps puller bodies and emits STARTED / COMPLETED /
FAILED ``PullLifecycle`` contracts.
"""
from __future__ import annotations

import hashlib
import json
import time
from contextlib import contextmanager
from typing import Any, Iterator
from uuid import UUID

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from contracts import observability as _obs
from contracts.channels import channel_for
from contracts.correlation import (
    correlation_scope,
    get_current_correlation_id,
)
from contracts.schemas import BaseContract, PullLifecycle

# Late-bound to allow monkeypatching in tests.
from events.bus import bus  # noqa: E402


def _get_engine() -> Engine:
    """Return the shared database engine.

    Resolved lazily so that importing contracts.emit does not force the API
    engine to initialise at import time.
    """
    from api.dependencies import get_db_engine

    return get_db_engine()


def _serialise(contract: BaseContract) -> dict[str, Any]:
    """Pydantic serialisation using ``model_dump(mode='json')`` so UUID /
    Decimal / datetime are JSON-safe."""
    return contract.model_dump(mode="json")


def _payload_hash(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


# PostgreSQL rejects NOTIFY payloads over 8000 bytes; keep headroom.
NOTIFY_MAX_BYTES = 7800
_NOTIFY_ENVELOPE_KEYS = ("ticker", "actor_id", "actor_hint", "canonical_name", "prediction_id")


def notify_payload(contract: BaseContract, payload: dict[str, Any]) -> str:
    """JSON for ``pg_notify``: the full contract, or a small envelope when the
    serialised contract would exceed the NOTIFY size limit.

    The envelope always carries ``event_id`` (for echo de-duplication in
    ``api/routers/sse.py``), the contract type, and the identity keys the
    canvas matches nodes on.
    """
    blob = json.dumps(payload, default=str)
    if len(blob.encode("utf-8")) <= NOTIFY_MAX_BYTES:
        return blob
    envelope: dict[str, Any] = {
        "event_id": str(contract.event_id),
        "contract_type": type(contract).__name__,
        "producer_module": contract.producer_module,
        "timestamp": payload.get("timestamp"),
        "truncated": True,
    }
    for key in _NOTIFY_ENVELOPE_KEYS:
        if key in payload:
            envelope[key] = payload[key]
    return json.dumps(envelope, default=str)


def _write_audit(
    engine: Engine,
    contract: BaseContract,
    payload_hash: str,
    *,
    channel: str | None = None,
    notify_json: str | None = None,
) -> None:
    """Write the audit row and, in the same transaction, ``pg_notify``.

    Issuing the notify inside the audit transaction makes it transactional:
    PostgreSQL delivers it only if the audit row commits, so listeners
    (``events/bus.py`` in the API process → SSE) never see a contract that
    was not recorded. This is the cross-process leg the April v5 plan
    designed and never wired (V5-TRANSFORMATION.md §5 D1, R1.1).
    """
    sql = text(
        """
        INSERT INTO contracts_audit (
            event_id, contract_type, producer_module,
            correlation_id, emitted_at, dispatched_to,
            payload_hash, schema_version
        ) VALUES (
            :event_id, :contract_type, :producer_module,
            :correlation_id, :emitted_at, :dispatched_to,
            :payload_hash, :schema_version
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql.bindparams(
                event_id=str(contract.event_id),
                contract_type=type(contract).__name__,
                producer_module=contract.producer_module,
                correlation_id=str(contract.correlation_id),
                emitted_at=contract.timestamp,
                dispatched_to=[],
                payload_hash=payload_hash,
                schema_version=contract.schema_version,
            )
        )
        if channel and notify_json is not None:
            conn.execute(
                text("SELECT pg_notify(:channel, :payload)"),
                {"channel": channel, "payload": notify_json},
            )


def emit(contract: BaseContract) -> UUID:
    """Emit a contract.

    1. Serialise to JSON-safe dict.
    2. Compute payload_hash for idempotency detection.
    3. Write a row to ``contracts_audit`` and ``pg_notify`` the channel in
       the same transaction (cross-process delivery to the SSE listener).
    4. Forward to the local event bus on the contract's channel.

    Returns the event id.
    """
    payload = _serialise(contract)
    payload_hash = _payload_hash(payload)
    channel = channel_for(type(contract))

    try:
        _write_audit(
            _get_engine(),
            contract,
            payload_hash,
            channel=channel,
            notify_json=notify_payload(contract, payload),
        )
    except Exception as exc:
        # Never let audit-write failure block the emit path — the dispatcher
        # will still attempt delivery via the bus, and the dead-letter store
        # will capture downstream failures.
        log.warning(
            "contracts.emit: audit write failed for {ct}: {e}",
            ct=type(contract).__name__, e=str(exc),
        )

    bus.emit_sync(channel, payload)
    _obs.emitted(type(contract).__name__)
    return contract.event_id


# ---------- pull lifecycle ----------


@contextmanager
def pull_lifecycle(puller_name: str) -> Iterator[dict[str, int]]:
    """Wrap a puller block and emit STARTED / COMPLETED / FAILED contracts.

    Example::

        with pull_lifecycle("fred") as rows:
            for r in fetch_rows():
                insert(r)
                rows["count"] += 1
    """
    # Use the ambient correlation scope if present, otherwise spawn one.
    ambient = get_current_correlation_id()
    if ambient is None:
        scope_cm = correlation_scope()
    else:
        scope_cm = correlation_scope(ambient)

    with scope_cm as cid:
        started_at = time.time()
        emit(
            PullLifecycle(
                producer_module=f"ingestion.{puller_name}",
                correlation_id=cid,
                puller_name=puller_name,
                state="STARTED",
            )
        )
        rows: dict[str, int] = {"count": 0}
        try:
            yield rows
        except Exception as exc:
            emit(
                PullLifecycle(
                    producer_module=f"ingestion.{puller_name}",
                    correlation_id=cid,
                    puller_name=puller_name,
                    state="FAILED",
                    row_count=rows.get("count", 0),
                    duration_s=time.time() - started_at,
                    error=str(exc),
                )
            )
            raise
        else:
            emit(
                PullLifecycle(
                    producer_module=f"ingestion.{puller_name}",
                    correlation_id=cid,
                    puller_name=puller_name,
                    state="COMPLETED",
                    row_count=rows.get("count", 0),
                    duration_s=time.time() - started_at,
                )
            )
