from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from contracts.dead_letter import DeadLetterEntry
from contracts.retry_scheduler import RetryScheduler


def _entry(retry_count: int = 0) -> DeadLetterEntry:
    return DeadLetterEntry(
        id=1,
        event_id=uuid4(),
        contract_type="PullLifecycle",
        payload={
            "producer_module": "t",
            "correlation_id": str(uuid4()),
            "puller_name": "fred",
            "state": "COMPLETED",
        },
        consumer="contracts.handlers.alerts.on_pull_lifecycle",
        error_type="CONSUMER_EXCEPTION",
        error_detail="x",
        retry_count=retry_count,
        next_retry_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        failed_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
    )


def test_retry_scheduler_marks_resolved_on_success(monkeypatch):
    engine = MagicMock()
    pending = [_entry()]

    monkeypatch.setattr(
        "contracts.retry_scheduler.pending_retries",
        lambda engine, now=None: pending,
    )
    resolved: list[int] = []
    monkeypatch.setattr(
        "contracts.retry_scheduler.mark_resolved",
        lambda engine, entry_id: resolved.append(entry_id),
    )
    bumped: list = []
    monkeypatch.setattr(
        "contracts.retry_scheduler.bump_retry",
        lambda *a, **kw: bumped.append((a, kw)),
    )

    handler_mock = MagicMock()
    monkeypatch.setattr(
        "contracts.retry_scheduler.resolve_handler", lambda path: handler_mock
    )

    sched = RetryScheduler(engine=engine)
    sched.run_once()

    assert resolved == [1]
    assert bumped == []
    handler_mock.assert_called_once()


def test_retry_scheduler_bumps_on_failure(monkeypatch):
    engine = MagicMock()
    pending = [_entry(retry_count=0)]

    monkeypatch.setattr(
        "contracts.retry_scheduler.pending_retries",
        lambda engine, now=None: pending,
    )
    resolved: list[int] = []
    monkeypatch.setattr(
        "contracts.retry_scheduler.mark_resolved",
        lambda engine, entry_id: resolved.append(entry_id),
    )
    bumped: list = []
    monkeypatch.setattr(
        "contracts.retry_scheduler.bump_retry",
        lambda engine, entry_id, retry_count: bumped.append(
            (entry_id, retry_count)
        ),
    )

    def broken(*args, **kwargs):
        raise RuntimeError("still broken")

    monkeypatch.setattr(
        "contracts.retry_scheduler.resolve_handler", lambda path: broken
    )

    sched = RetryScheduler(engine=engine)
    sched.run_once()

    assert resolved == []
    assert bumped == [(1, 0)]
