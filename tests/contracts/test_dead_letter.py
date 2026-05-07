from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import text

import pytest

from contracts.dead_letter import (
    RETRY_SCHEDULE,
    mark_resolved,
    record_failure,
    pending_retries,
    schedule_next_retry,
)


@pytest.mark.integration
def test_record_failure_writes_row(pg_engine):
    _reset_tables(pg_engine)

    eid = uuid4()
    record_failure(
        pg_engine,
        event_id=eid,
        contract_type="PullLifecycle",
        payload={"puller_name": "fred", "state": "STARTED"},
        consumer="contracts.handlers.alerts.on_pull_lifecycle",
        error_type="CONSUMER_EXCEPTION",
        error_detail="boom",
        correlation_id=uuid4(),
    )

    with pg_engine.begin() as conn:
        row = conn.execute(
            text("SELECT event_id, retry_count, consumer FROM contracts_dead_letter")
        ).fetchone()
    assert str(row[0]) == str(eid)
    assert row[1] == 0
    assert row[2] == "contracts.handlers.alerts.on_pull_lifecycle"


@pytest.mark.integration
def test_pending_retries_returns_due_entries_only(pg_engine):
    _reset_tables(pg_engine)
    now = datetime.now(timezone.utc)
    _insert_row(pg_engine, retries=0, next_retry_at=now - timedelta(seconds=10))
    _insert_row(pg_engine, retries=0, next_retry_at=now + timedelta(hours=1))
    _insert_row(pg_engine, retries=0, next_retry_at=None)

    due = pending_retries(pg_engine, now=now)
    assert len(due) == 1


@pytest.mark.integration
def test_mark_resolved_sets_resolved_at(pg_engine):
    _reset_tables(pg_engine)
    entry_id = _insert_row(pg_engine, retries=0)

    mark_resolved(pg_engine, entry_id)

    with pg_engine.begin() as conn:
        row = conn.execute(
            text("SELECT resolved_at FROM contracts_dead_letter WHERE id = :id")
            .bindparams(id=entry_id)
        ).fetchone()
    assert row[0] is not None


def test_schedule_next_retry_first_attempt():
    now = datetime(2026, 4, 11, 12, 0, 0, tzinfo=timezone.utc)
    when = schedule_next_retry(retry_count=0, now=now)
    assert when == now + timedelta(seconds=RETRY_SCHEDULE[0])


def test_schedule_next_retry_second_attempt():
    now = datetime(2026, 4, 11, 12, 0, 0, tzinfo=timezone.utc)
    when = schedule_next_retry(retry_count=1, now=now)
    assert when == now + timedelta(seconds=RETRY_SCHEDULE[1])


def test_schedule_next_retry_exhausted_returns_none():
    assert schedule_next_retry(retry_count=len(RETRY_SCHEDULE)) is None


# ---- helpers ----


def _reset_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM contracts_dead_letter"))
        conn.execute(text("DELETE FROM contracts_audit"))


def _insert_row(engine, retries: int, next_retry_at=None) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO contracts_dead_letter (
                    event_id, contract_type, payload, consumer,
                    error_type, error_detail, retry_count, next_retry_at
                ) VALUES (
                    :eid, 'PullLifecycle', '{}'::jsonb, 'h',
                    'CONSUMER_EXCEPTION', 'x', :rc, :nra
                ) RETURNING id
                """
            ).bindparams(eid=str(uuid4()), rc=retries, nra=next_retry_at)
        )
        return result.fetchone()[0]
