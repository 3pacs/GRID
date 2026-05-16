"""Tests for the idle-fleet goal queue + worker — Day 1 PoC.

Covers:

* Pure-unit (no DB):
  - DutyCycleTracker enforcement.
  - Cloud-LLM refusal default + per-goal + worker-ceiling combinations.
  - Handler dispatch (unknown goal_type -> failed; known dispatches).
  - Hardware-tier ordering helpers.

* Integration (requires Postgres, skipped via ``pg_engine`` fixture
  otherwise):
  - claim_goal no-double-claim under concurrent claimants.
  - claim_goal hardware-tier filtering.
  - submit_result write-back transitions to 'done' + appends to
    goal_results.
  - mark_failed retries until max_attempts, then quarantines.
  - enqueue dedupe via the partial UNIQUE index.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text


# ============================================================
# Pure-unit tests (no DB)
# ============================================================


def test_tier_index_ordering() -> None:
    from intelligence.goal_queue import TIER_ORDER, _tier_index

    assert TIER_ORDER == ("cpu", "medium_gpu", "large_gpu", "vision")
    assert _tier_index("cpu") < _tier_index("medium_gpu")
    assert _tier_index("medium_gpu") < _tier_index("large_gpu")
    assert _tier_index("large_gpu") < _tier_index("vision")


def test_tier_index_rejects_unknown() -> None:
    from intelligence.goal_queue import _tier_index

    with pytest.raises(ValueError):
        _tier_index("quantum")


def test_duty_cycle_tracker_admits_when_idle() -> None:
    from scripts.goal_worker import DutyCycleTracker

    duty = DutyCycleTracker.create(window_s=100, cap=0.5)
    assert duty.busy_fraction() == 0.0
    assert duty.admits()


def test_duty_cycle_tracker_blocks_over_cap() -> None:
    from scripts.goal_worker import DutyCycleTracker

    duty = DutyCycleTracker.create(window_s=100, cap=0.5)
    now = time.monotonic()
    # 60s busy in a 100s window = 60% busy > 50% cap.
    duty.record(now - 60, now)
    assert duty.busy_fraction(now) == pytest.approx(0.6, abs=0.01)
    assert not duty.admits()


def test_duty_cycle_tracker_respects_per_goal_override() -> None:
    from scripts.goal_worker import DutyCycleTracker

    duty = DutyCycleTracker.create(window_s=100, cap=0.5)
    now = time.monotonic()
    duty.record(now - 40, now)  # 40% busy
    # global cap 50% -> admits
    assert duty.admits()
    # per-goal cap 25% -> blocked
    assert not duty.admits(cap_override=0.25)


def test_duty_cycle_tracker_validates_inputs() -> None:
    from scripts.goal_worker import DutyCycleTracker

    with pytest.raises(ValueError):
        DutyCycleTracker.create(window_s=0, cap=0.5)
    with pytest.raises(ValueError):
        DutyCycleTracker.create(window_s=100, cap=0.0)
    with pytest.raises(ValueError):
        DutyCycleTracker.create(window_s=100, cap=1.5)


def _make_goal(**overrides):
    """Build a Goal dataclass with sensible defaults for unit tests."""
    from intelligence.goal_queue import Goal

    now = datetime.now(timezone.utc)
    base = dict(
        id=1,
        goal_type="score_active_hypothesis",
        target_id="h-abc",
        payload={},
        priority=100,
        hardware_tier="cpu",
        state="claimed",
        allow_cloud=False,
        max_duty_cycle=None,
        claimed_by="test-node",
        claimed_at=now,
        lease_expires_at=now,
        attempts=1,
        max_attempts=3,
        last_error=None,
        dedupe_window="global",
        depth=0,
        created_at=now,
        updated_at=now,
    )
    base.update(overrides)
    return Goal(**base)


def test_cloud_llm_refused_when_goal_disallows(monkeypatch) -> None:
    from scripts.goal_worker import CloudLLMRefused, _check_cloud_permission

    monkeypatch.setenv("GRID_GOAL_WORKER_ALLOW_CLOUD", "1")
    goal = _make_goal(allow_cloud=False)
    with pytest.raises(CloudLLMRefused):
        _check_cloud_permission(goal)


def test_cloud_llm_refused_when_worker_ceiling_zero(monkeypatch) -> None:
    from scripts.goal_worker import CloudLLMRefused, _check_cloud_permission

    monkeypatch.delenv("GRID_GOAL_WORKER_ALLOW_CLOUD", raising=False)
    goal = _make_goal(allow_cloud=True)
    with pytest.raises(CloudLLMRefused):
        _check_cloud_permission(goal)


def test_cloud_llm_allowed_when_both_explicit(monkeypatch) -> None:
    from scripts.goal_worker import _check_cloud_permission

    monkeypatch.setenv("GRID_GOAL_WORKER_ALLOW_CLOUD", "1")
    goal = _make_goal(allow_cloud=True)
    # Should not raise.
    _check_cloud_permission(goal)


def test_execute_goal_dispatch_unknown_handler() -> None:
    from scripts.goal_worker import execute_goal

    engine = MagicMock()
    goal = _make_goal(goal_type="not_a_real_handler")
    outcome, payload, _ = execute_goal(engine, goal, node_id="test-node")
    assert outcome == "failed"
    assert isinstance(payload, str)
    assert "no handler" in payload


def test_execute_goal_dispatch_score_active_hypothesis(monkeypatch) -> None:
    """Dispatch table sends ``score_active_hypothesis`` goals to the
    HypothesisGenerator wrapper."""
    from scripts.goal_worker import HANDLERS, execute_goal

    captured = {}

    def fake_handler(engine, goal):
        captured["engine"] = engine
        captured["goal"] = goal
        return {"outcome": "confirmed", "confidence": 0.9}

    monkeypatch.setitem(HANDLERS, "score_active_hypothesis", fake_handler)

    engine = MagicMock()
    goal = _make_goal()
    outcome, payload, duration_ms = execute_goal(engine, goal, node_id="n1")
    assert outcome == "done"
    assert payload == {"outcome": "confirmed", "confidence": 0.9}
    assert duration_ms >= 0
    assert captured["goal"].id == goal.id


def test_execute_goal_handler_exception_captured() -> None:
    from scripts.goal_worker import HANDLERS, execute_goal

    def blowup(engine, goal):
        raise RuntimeError("kaboom")

    original = HANDLERS.get("score_active_hypothesis")
    HANDLERS["score_active_hypothesis"] = blowup
    try:
        engine = MagicMock()
        goal = _make_goal()
        outcome, payload, _ = execute_goal(engine, goal, node_id="n1")
        assert outcome == "failed"
        assert "RuntimeError" in payload
        assert "kaboom" in payload
    finally:
        if original is not None:
            HANDLERS["score_active_hypothesis"] = original


def test_execute_goal_dry_run_does_not_call_handler() -> None:
    from scripts.goal_worker import HANDLERS, execute_goal

    def explode(engine, goal):
        raise AssertionError("handler must not be called in dry-run")

    original = HANDLERS.get("score_active_hypothesis")
    HANDLERS["score_active_hypothesis"] = explode
    try:
        engine = MagicMock()
        goal = _make_goal()
        outcome, payload, _ = execute_goal(
            engine, goal, node_id="n1", dry_run=True,
        )
        assert outcome == "done"
        assert isinstance(payload, dict) and payload.get("dry_run") is True
    finally:
        if original is not None:
            HANDLERS["score_active_hypothesis"] = original


def test_enqueue_goal_rejects_bad_inputs() -> None:
    from intelligence.goal_queue import enqueue_goal

    engine = MagicMock()
    with pytest.raises(ValueError):
        enqueue_goal(
            engine,
            goal_type="x",
            target_id="t",
            hardware_tier="banana",
        )
    with pytest.raises(ValueError):
        enqueue_goal(
            engine,
            goal_type="x",
            target_id="t",
            depth=5,
        )
    with pytest.raises(ValueError):
        enqueue_goal(
            engine,
            goal_type="x",
            target_id="t",
            max_duty_cycle=1.5,
        )


# ============================================================
# Integration tests (Postgres required — auto-skip if absent)
# ============================================================


@pytest.fixture
def goal_queue_schema(pg_engine):
    """Idempotently create the goal_queue + goal_results tables for this
    test session. Cleans rows from previous runs but does not drop the
    tables — a real migration is the production path.
    """
    ddl_queue = """
        CREATE TABLE IF NOT EXISTS goal_queue (
            id                  BIGSERIAL PRIMARY KEY,
            goal_type           TEXT NOT NULL,
            target_id           TEXT NOT NULL,
            payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
            priority            INTEGER NOT NULL DEFAULT 100,
            hardware_tier       TEXT NOT NULL DEFAULT 'cpu',
            state               TEXT NOT NULL DEFAULT 'claimable',
            allow_cloud         BOOLEAN NOT NULL DEFAULT FALSE,
            max_duty_cycle      NUMERIC,
            claimed_by          TEXT,
            claimed_at          TIMESTAMPTZ,
            lease_expires_at    TIMESTAMPTZ,
            attempts            INTEGER NOT NULL DEFAULT 0,
            max_attempts        INTEGER NOT NULL DEFAULT 3,
            last_error          TEXT,
            dedupe_window       TEXT NOT NULL DEFAULT 'global',
            depth               INTEGER NOT NULL DEFAULT 0,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """
    ddl_dedupe_idx = """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_goal_queue_open_dedupe
            ON goal_queue (goal_type, target_id, dedupe_window)
            WHERE state IN ('claimable','claimed');
    """
    ddl_results = """
        CREATE TABLE IF NOT EXISTS goal_results (
            id                  BIGSERIAL PRIMARY KEY,
            goal_id             BIGINT NOT NULL,
            goal_type           TEXT NOT NULL,
            target_id           TEXT NOT NULL,
            node_id             TEXT NOT NULL,
            state               TEXT NOT NULL,
            result_summary      JSONB NOT NULL DEFAULT '{}'::jsonb,
            error_message       TEXT,
            duration_ms         INTEGER,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """

    with pg_engine.begin() as conn:
        conn.execute(text(ddl_queue))
        conn.execute(text(ddl_dedupe_idx))
        conn.execute(text(ddl_results))

    # Use a unique dedupe_window per test so rows from prior runs don't
    # interfere with the partial unique index. Returned for tests to
    # parameterise their inserts.
    window = f"test-{uuid.uuid4().hex[:8]}"

    yield pg_engine, window

    # Best-effort cleanup of rows we created.
    with pg_engine.begin() as conn:
        conn.execute(
            text("DELETE FROM goal_results WHERE target_id LIKE :p"),
            {"p": f"{window}%"},
        )
        conn.execute(
            text("DELETE FROM goal_queue WHERE dedupe_window = :w"),
            {"w": window},
        )


def test_enqueue_goal_dedupes_open_rows(goal_queue_schema) -> None:
    from intelligence.goal_queue import enqueue_goal

    engine, window = goal_queue_schema
    first = enqueue_goal(
        engine,
        goal_type="score_active_hypothesis",
        target_id=f"{window}-h-1",
        dedupe_window=window,
    )
    assert first is not None
    dup = enqueue_goal(
        engine,
        goal_type="score_active_hypothesis",
        target_id=f"{window}-h-1",
        dedupe_window=window,
    )
    assert dup is None, "second open enqueue should dedupe"


def test_claim_goal_no_double_claim(goal_queue_schema) -> None:
    from intelligence.goal_queue import claim_goal, enqueue_goal

    engine, window = goal_queue_schema
    enqueue_goal(
        engine,
        goal_type="score_active_hypothesis",
        target_id=f"{window}-h-1",
        dedupe_window=window,
    )
    g1 = claim_goal(engine, node_id="nodeA", hardware_tier="cpu")
    assert g1 is not None
    assert g1.claimed_by == "nodeA"
    assert g1.state == "claimed"
    assert g1.attempts == 1
    g2 = claim_goal(engine, node_id="nodeB", hardware_tier="cpu")
    # No more eligible rows in this window — nodeB sees nothing for the
    # row nodeA already claimed.
    # Other tests may have inserted rows; verify nodeA's specific id is
    # not re-claimed.
    assert g2 is None or g2.id != g1.id


def test_claim_goal_respects_hardware_tier(goal_queue_schema) -> None:
    from intelligence.goal_queue import claim_goal, enqueue_goal

    engine, window = goal_queue_schema
    enqueue_goal(
        engine,
        goal_type="score_active_hypothesis",
        target_id=f"{window}-h-big",
        hardware_tier="large_gpu",
        dedupe_window=window,
    )
    # cpu-tier node must NOT see the large_gpu goal.
    g = claim_goal(
        engine, node_id="koala", hardware_tier="cpu",
        goal_types=["score_active_hypothesis"],
    )
    assert g is None or g.target_id != f"{window}-h-big"
    # large_gpu node DOES see it.
    g2 = claim_goal(
        engine, node_id="gridz4", hardware_tier="large_gpu",
        goal_types=["score_active_hypothesis"],
    )
    assert g2 is not None
    assert g2.target_id == f"{window}-h-big"
    assert g2.hardware_tier == "large_gpu"


def test_submit_result_writes_back(goal_queue_schema) -> None:
    from intelligence.goal_queue import (
        claim_goal,
        enqueue_goal,
        submit_result,
    )

    engine, window = goal_queue_schema
    enqueue_goal(
        engine,
        goal_type="score_active_hypothesis",
        target_id=f"{window}-h-2",
        dedupe_window=window,
    )
    g = claim_goal(engine, node_id="nodeA", hardware_tier="cpu")
    assert g is not None
    submit_result(
        engine,
        goal_id=g.id,
        node_id="nodeA",
        result_summary={"outcome": "confirmed", "confidence": 0.82},
        duration_ms=42,
    )

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT state FROM goal_queue WHERE id = :id"),
            {"id": g.id},
        ).fetchone()
        assert row is not None and row[0] == "done"

        res = conn.execute(
            text(
                "SELECT state, result_summary, duration_ms "
                "FROM goal_results WHERE goal_id = :id"
            ),
            {"id": g.id},
        ).fetchone()
        assert res is not None
        assert res[0] == "done"
        summary = res[1] if isinstance(res[1], dict) else json.loads(res[1])
        assert summary["outcome"] == "confirmed"
        assert res[2] == 42


def test_submit_result_rejects_foreign_claimer(goal_queue_schema) -> None:
    from intelligence.goal_queue import claim_goal, enqueue_goal, submit_result

    engine, window = goal_queue_schema
    enqueue_goal(
        engine,
        goal_type="score_active_hypothesis",
        target_id=f"{window}-h-3",
        dedupe_window=window,
    )
    g = claim_goal(engine, node_id="nodeA", hardware_tier="cpu")
    assert g is not None
    with pytest.raises(RuntimeError):
        submit_result(
            engine, goal_id=g.id, node_id="nodeB", result_summary={}
        )


def test_mark_failed_retries_then_quarantines(goal_queue_schema) -> None:
    from intelligence.goal_queue import (
        claim_goal,
        enqueue_goal,
        mark_failed,
    )

    engine, window = goal_queue_schema
    enqueue_goal(
        engine,
        goal_type="score_active_hypothesis",
        target_id=f"{window}-h-4",
        dedupe_window=window,
        # default max_attempts = 3
    )
    states: list[str] = []
    for attempt in range(3):
        g = claim_goal(engine, node_id="nodeA", hardware_tier="cpu")
        assert g is not None, f"attempt {attempt}: nothing to claim"
        new_state = mark_failed(
            engine, goal_id=g.id, node_id="nodeA",
            reason=f"transient error {attempt}",
        )
        states.append(new_state)
    assert states[:2] == ["claimable", "claimable"]
    assert states[2] == "quarantined"


def test_reap_expired_leases(goal_queue_schema) -> None:
    from intelligence.goal_queue import (
        claim_goal,
        enqueue_goal,
        reap_expired_leases,
    )

    engine, window = goal_queue_schema
    enqueue_goal(
        engine,
        goal_type="score_active_hypothesis",
        target_id=f"{window}-h-5",
        dedupe_window=window,
    )
    g = claim_goal(
        engine, node_id="nodeA", hardware_tier="cpu", lease_seconds=1,
    )
    assert g is not None
    # Force lease into the past.
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE goal_queue "
                "SET lease_expires_at = NOW() - INTERVAL '1 minute' "
                "WHERE id = :id"
            ),
            {"id": g.id},
        )
    n = reap_expired_leases(engine)
    assert n >= 1
    with engine.connect() as conn:
        state = conn.execute(
            text("SELECT state FROM goal_queue WHERE id = :id"),
            {"id": g.id},
        ).scalar()
    assert state == "claimable"
