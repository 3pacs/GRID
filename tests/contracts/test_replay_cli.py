from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from contracts.dead_letter import DeadLetterEntry
from contracts.replay import replay_entry, replay_many, build_parser


def _entry(entry_id: int = 1, ct: str = "PullLifecycle") -> DeadLetterEntry:
    return DeadLetterEntry(
        id=entry_id,
        event_id=uuid4(),
        contract_type=ct,
        payload={
            "producer_module": "t",
            "correlation_id": str(uuid4()),
            "puller_name": "fred",
            "state": "COMPLETED",
        },
        consumer="contracts.handlers.alerts.on_pull_lifecycle",
        error_type="CONSUMER_EXCEPTION",
        error_detail="x",
        retry_count=1,
        next_retry_at=None,
        failed_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
    )


def test_replay_entry_marks_resolved_on_success(monkeypatch):
    engine = MagicMock()
    ok = MagicMock()
    monkeypatch.setattr("contracts.replay.resolve_handler", lambda p: ok)
    resolved: list = []
    monkeypatch.setattr(
        "contracts.replay.mark_resolved",
        lambda engine, entry_id: resolved.append(entry_id),
    )

    result = replay_entry(engine, _entry(entry_id=7))
    assert result is True
    assert resolved == [7]
    ok.assert_called_once()


def test_replay_entry_returns_false_on_failure(monkeypatch):
    engine = MagicMock()

    def broken(*a, **k):
        raise RuntimeError("still broken")

    monkeypatch.setattr("contracts.replay.resolve_handler", lambda p: broken)
    monkeypatch.setattr(
        "contracts.replay.mark_resolved", lambda *a, **k: pytest.fail("should not mark")
    )
    result = replay_entry(engine, _entry())
    assert result is False


def test_replay_many_counts_successes_and_failures(monkeypatch):
    engine = MagicMock()

    outcomes = iter([True, False, True])
    monkeypatch.setattr(
        "contracts.replay.replay_entry",
        lambda engine, entry: next(outcomes),
    )
    report = replay_many(engine, [_entry(1), _entry(2), _entry(3)])
    assert report == {"success": 2, "failed": 1}


def test_cli_parser_accepts_flags():
    parser = build_parser()
    args = parser.parse_args(["--contract", "PullLifecycle", "--limit", "10"])
    assert args.contract == "PullLifecycle"
    assert args.limit == 10
