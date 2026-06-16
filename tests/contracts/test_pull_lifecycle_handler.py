"""Regression tests for contracts.handlers.pull_lifecycle.on_pull_lifecycle.

Previously the handler read ``evt.status`` / ``evt.rows`` while the
``PullLifecycle`` schema defines ``state`` / ``row_count``. Because
``BaseContract`` is ``extra="forbid"`` + frozen, ``getattr(evt, "status", "")``
returned ``""`` and the FAILED branch never fired — puller-failure warnings
were silently dropped at this consumer (PUNCH-LIST-2026-05-13 contracts/ [P0]).
"""
from __future__ import annotations

from uuid import uuid4

import pytest
from loguru import logger as log

from contracts.handlers.pull_lifecycle import on_pull_lifecycle
from contracts.schemas import PullLifecycle


@pytest.fixture
def loguru_records():
    """Capture loguru records into a list (the handler uses loguru, not stdlib)."""
    records: list[dict] = []
    sink_id = log.add(
        lambda msg: records.append(
            {"level": msg.record["level"].name, "message": msg.record["message"]}
        ),
        level="DEBUG",
    )
    try:
        yield records
    finally:
        log.remove(sink_id)


def _make(state: str, **overrides) -> PullLifecycle:
    base = {
        "producer_module": "test",
        "correlation_id": uuid4(),
        "puller_name": "fred",
        "state": state,
    }
    base.update(overrides)
    return PullLifecycle(**base)


def test_failed_state_emits_warning(loguru_records):
    evt = _make("FAILED", row_count=0, error="boom")
    on_pull_lifecycle(evt, engine=None)
    warnings = [r for r in loguru_records if r["level"] == "WARNING"]
    assert warnings, "expected a WARNING record for FAILED state"
    text = " ".join(r["message"] for r in warnings)
    assert "failed" in text.lower()
    assert "fred" in text
    assert "boom" in text


def test_completed_state_does_not_warn(loguru_records):
    evt = _make("COMPLETED", row_count=42)
    on_pull_lifecycle(evt, engine=None)
    assert not any(r["level"] == "WARNING" for r in loguru_records), (
        "COMPLETED should not emit a warning"
    )


def test_started_state_does_not_warn(loguru_records):
    evt = _make("STARTED")
    on_pull_lifecycle(evt, engine=None)
    assert not any(r["level"] == "WARNING" for r in loguru_records), (
        "STARTED should not emit a warning"
    )


def test_conflict_detected_state_does_not_warn(loguru_records):
    evt = _make("CONFLICT_DETECTED", row_count=5)
    on_pull_lifecycle(evt, engine=None)
    assert not any(r["level"] == "WARNING" for r in loguru_records), (
        "CONFLICT_DETECTED should not emit a warning"
    )


def test_row_count_surfaced_in_failed_warning(loguru_records):
    evt = _make("FAILED", row_count=7, error="db down")
    on_pull_lifecycle(evt, engine=None)
    warnings = [r for r in loguru_records if r["level"] == "WARNING"]
    assert warnings
    text = " ".join(r["message"] for r in warnings)
    assert "row_count=7" in text


def test_handler_reads_schema_fields_not_legacy_aliases(loguru_records):
    """Guards against re-introducing evt.status / evt.rows lookups.

    The schema is frozen + extra="forbid"; legacy names silently default to
    "" / None, so the FAILED branch never fires. A regression here would
    look like: WARNING absent on a FAILED contract.
    """
    evt = _make("FAILED", row_count=1, error="x")
    # Sanity: the schema does not have the legacy attribute names.
    assert not hasattr(evt, "status")
    assert not hasattr(evt, "rows")
    on_pull_lifecycle(evt, engine=None)
    assert any(r["level"] == "WARNING" for r in loguru_records)
