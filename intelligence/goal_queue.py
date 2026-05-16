"""Goal queue — Day 1 of the idle-fleet agent-loop PoC.

Thin SQLAlchemy wrapper around the ``goal_queue`` + ``goal_results`` tables.
Used by ``scripts/goal_worker.py`` (per-node daemon) and by enqueue scripts
such as ``scripts/seed_goals_hypo_scoring.py``.

Public surface
--------------

* :class:`Goal` — frozen dataclass shaped after a ``goal_queue`` row.
* :func:`enqueue_goal` — insert a new goal (idempotent via the partial
  unique index in the migration).
* :func:`claim_goal` — atomically claim the next eligible goal for a
  node (``FOR UPDATE SKIP LOCKED``). Honors hardware-tier ordering
  (a ``large_gpu`` node will happily run ``cpu`` goals; a ``cpu`` node
  will not run ``large_gpu`` goals).
* :func:`submit_result` — write a success row to ``goal_results`` and
  transition the queue row to ``done``.
* :func:`mark_failed` — record a transient failure; either resets the
  goal to ``claimable`` (with attempts incremented) or quarantines it
  if ``max_attempts`` is exhausted.
* :func:`extend_lease` — heartbeat helper for long-running handlers.
* :func:`reap_expired_leases` — sweeper that resets goals whose worker
  died mid-execution (called by the worker on startup; a dedicated
  reaper service is Day 2 work).

All SQL is parameterised via SQLAlchemy ``text()`` per
``.claude/rules/security.md`` — no f-string interpolation.

Locked decisions honored (see ``docs/planning/IDLE-FLEET-AGENT-LOOP.md``):
  #1 LLM tier policy — workers refuse cloud calls without
     ``allow_cloud=True`` on the goal row; this module surfaces the flag
     unchanged. The refusal lives in the worker (not here) so this
     module remains a thin data layer.
  #3 Queue host — ``griddb`` Postgres, reuses ``db.get_engine``.
  #4 Per-node compute budget cap — surfaces ``max_duty_cycle`` for the
     worker to honor.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# --- Hardware-tier ordering ------------------------------------------------
# Higher index = more capable. A node of tier T will run goals of tier <= T.
TIER_ORDER: tuple[str, ...] = ("cpu", "medium_gpu", "large_gpu", "vision")


def _tier_index(tier: str) -> int:
    try:
        return TIER_ORDER.index(tier)
    except ValueError as exc:
        raise ValueError(
            f"unknown hardware_tier {tier!r}; expected one of {TIER_ORDER}"
        ) from exc


VALID_STATES: frozenset[str] = frozenset(
    ("claimable", "claimed", "done", "failed", "quarantined")
)


@dataclass(frozen=True)
class Goal:
    """Immutable view of a single ``goal_queue`` row."""

    id: int
    goal_type: str
    target_id: str
    payload: dict[str, Any]
    priority: int
    hardware_tier: str
    state: str
    allow_cloud: bool
    max_duty_cycle: Optional[float]
    claimed_by: Optional[str]
    claimed_at: Optional[datetime]
    lease_expires_at: Optional[datetime]
    attempts: int
    max_attempts: int
    last_error: Optional[str]
    dedupe_window: str
    depth: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: Any) -> "Goal":
        # SQLAlchemy returns Rows that look like named tuples; index by attr.
        payload = row.payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        return cls(
            id=row.id,
            goal_type=row.goal_type,
            target_id=row.target_id,
            payload=payload or {},
            priority=row.priority,
            hardware_tier=row.hardware_tier,
            state=row.state,
            allow_cloud=bool(row.allow_cloud),
            max_duty_cycle=(
                float(row.max_duty_cycle) if row.max_duty_cycle is not None else None
            ),
            claimed_by=row.claimed_by,
            claimed_at=row.claimed_at,
            lease_expires_at=row.lease_expires_at,
            attempts=row.attempts,
            max_attempts=row.max_attempts,
            last_error=row.last_error,
            dedupe_window=row.dedupe_window,
            depth=row.depth,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


# --- enqueue ---------------------------------------------------------------


def enqueue_goal(
    engine: Engine,
    *,
    goal_type: str,
    target_id: str,
    payload: dict[str, Any] | None = None,
    priority: int = 100,
    hardware_tier: str = "cpu",
    allow_cloud: bool = False,
    max_duty_cycle: float | None = None,
    max_attempts: int = 3,
    dedupe_window: str = "global",
    depth: int = 0,
) -> int | None:
    """Insert a new goal. Returns the new ``id`` or ``None`` if a duplicate
    open goal already exists (caught by the partial unique index).

    Locked decision #1: ``allow_cloud`` defaults to ``False``. Pass ``True``
    only with Anik's explicit approval per goal.
    """
    if hardware_tier not in TIER_ORDER:
        raise ValueError(f"hardware_tier must be one of {TIER_ORDER}")
    if depth < 0 or depth > 3:
        raise ValueError("depth must be in [0, 3]")
    if max_duty_cycle is not None and not (0.0 < max_duty_cycle <= 1.0):
        raise ValueError("max_duty_cycle must be in (0.0, 1.0]")

    payload_json = json.dumps(payload or {})

    stmt = text(
        """
        INSERT INTO goal_queue (
            goal_type, target_id, payload, priority, hardware_tier,
            allow_cloud, max_duty_cycle, max_attempts,
            dedupe_window, depth
        )
        VALUES (
            :goal_type, :target_id, CAST(:payload AS JSONB), :priority,
            :hardware_tier, :allow_cloud, :max_duty_cycle, :max_attempts,
            :dedupe_window, :depth
        )
        ON CONFLICT DO NOTHING
        RETURNING id
        """
    )
    with engine.begin() as conn:
        row = conn.execute(
            stmt,
            {
                "goal_type": goal_type,
                "target_id": target_id,
                "payload": payload_json,
                "priority": priority,
                "hardware_tier": hardware_tier,
                "allow_cloud": allow_cloud,
                "max_duty_cycle": max_duty_cycle,
                "max_attempts": max_attempts,
                "dedupe_window": dedupe_window,
                "depth": depth,
            },
        ).fetchone()
    if row is None:
        log.debug(
            "goal_queue: dedupe-skip {gt}/{tid}/{dw}",
            gt=goal_type, tid=target_id, dw=dedupe_window,
        )
        return None
    return int(row[0])


def enqueue_many(
    engine: Engine,
    goals: Iterable[dict[str, Any]],
) -> list[int]:
    """Bulk enqueue helper. Returns the list of newly created ids
    (skipping duplicates)."""
    new_ids: list[int] = []
    for spec in goals:
        new_id = enqueue_goal(engine, **spec)
        if new_id is not None:
            new_ids.append(new_id)
    return new_ids


# --- claim -----------------------------------------------------------------


def claim_goal(
    engine: Engine,
    *,
    node_id: str,
    hardware_tier: str,
    lease_seconds: int = 600,
    goal_types: Iterable[str] | None = None,
) -> Goal | None:
    """Atomically claim the highest-priority eligible goal.

    Uses ``SELECT ... FOR UPDATE SKIP LOCKED`` so concurrent workers
    never collide on the same row. Tier eligibility: a node will claim
    any goal whose declared ``hardware_tier`` is at or below its own
    in :data:`TIER_ORDER`.

    Returns ``None`` when the queue has nothing eligible.
    """
    node_tier_idx = _tier_index(hardware_tier)
    eligible_tiers = list(TIER_ORDER[: node_tier_idx + 1])

    # If goal_types filter is supplied, restrict to those types only.
    goal_types_list = list(goal_types) if goal_types else None

    select_sql = """
        SELECT id
        FROM goal_queue
        WHERE state = 'claimable'
          AND hardware_tier = ANY(:tiers)
    """
    if goal_types_list is not None:
        select_sql += " AND goal_type = ANY(:goal_types)"
    select_sql += """
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    """

    params: dict[str, Any] = {"tiers": eligible_tiers}
    if goal_types_list is not None:
        params["goal_types"] = goal_types_list

    update_sql = """
        UPDATE goal_queue
        SET state = 'claimed',
            claimed_by = :node_id,
            claimed_at = NOW(),
            lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
            attempts = attempts + 1,
            updated_at = NOW()
        WHERE id = :id
        RETURNING id, goal_type, target_id, payload, priority,
                  hardware_tier, state, allow_cloud, max_duty_cycle,
                  claimed_by, claimed_at, lease_expires_at, attempts,
                  max_attempts, last_error, dedupe_window, depth,
                  created_at, updated_at
    """

    with engine.begin() as conn:
        picked = conn.execute(text(select_sql), params).fetchone()
        if picked is None:
            return None
        goal_id = int(picked[0])
        updated = conn.execute(
            text(update_sql),
            {
                "id": goal_id,
                "node_id": node_id,
                "lease_seconds": lease_seconds,
            },
        ).fetchone()

    if updated is None:
        return None
    return Goal.from_row(updated)


# --- result + failure ------------------------------------------------------


def submit_result(
    engine: Engine,
    *,
    goal_id: int,
    node_id: str,
    result_summary: dict[str, Any] | None = None,
    duration_ms: int | None = None,
) -> None:
    """Mark a claimed goal as done and append a ``goal_results`` row."""
    summary_json = json.dumps(result_summary or {})

    fetch_sql = text(
        """
        SELECT goal_type, target_id, state, claimed_by
        FROM goal_queue
        WHERE id = :id
        FOR UPDATE
        """
    )
    update_sql = text(
        """
        UPDATE goal_queue
        SET state = 'done',
            updated_at = NOW(),
            lease_expires_at = NULL
        WHERE id = :id
        """
    )
    insert_sql = text(
        """
        INSERT INTO goal_results (
            goal_id, goal_type, target_id, node_id, state,
            result_summary, duration_ms
        )
        VALUES (
            :goal_id, :goal_type, :target_id, :node_id, 'done',
            CAST(:summary AS JSONB), :duration_ms
        )
        """
    )

    with engine.begin() as conn:
        row = conn.execute(fetch_sql, {"id": goal_id}).fetchone()
        if row is None:
            raise LookupError(f"goal {goal_id} not found")
        gt, tid, state, claimer = row
        if state != "claimed":
            raise RuntimeError(
                f"goal {goal_id} state={state!r}, cannot submit result"
            )
        if claimer != node_id:
            raise RuntimeError(
                f"goal {goal_id} claimed_by={claimer!r}, not {node_id!r}"
            )
        conn.execute(update_sql, {"id": goal_id})
        conn.execute(
            insert_sql,
            {
                "goal_id": goal_id,
                "goal_type": gt,
                "target_id": tid,
                "node_id": node_id,
                "summary": summary_json,
                "duration_ms": duration_ms,
            },
        )


def mark_failed(
    engine: Engine,
    *,
    goal_id: int,
    node_id: str,
    reason: str,
    duration_ms: int | None = None,
) -> str:
    """Record a failure. Returns the new queue state — either
    ``'claimable'`` (retry available) or ``'quarantined'`` (max attempts
    reached).
    """
    fetch_sql = text(
        """
        SELECT goal_type, target_id, state, attempts, max_attempts, claimed_by
        FROM goal_queue
        WHERE id = :id
        FOR UPDATE
        """
    )
    with engine.begin() as conn:
        row = conn.execute(fetch_sql, {"id": goal_id}).fetchone()
        if row is None:
            raise LookupError(f"goal {goal_id} not found")
        gt, tid, state, attempts, max_attempts, claimer = row
        if state != "claimed":
            raise RuntimeError(
                f"goal {goal_id} state={state!r}, cannot mark failed"
            )
        if claimer != node_id:
            raise RuntimeError(
                f"goal {goal_id} claimed_by={claimer!r}, not {node_id!r}"
            )

        if attempts >= max_attempts:
            new_state = "quarantined"
        else:
            new_state = "claimable"

        conn.execute(
            text(
                """
                UPDATE goal_queue
                SET state = :new_state,
                    last_error = :reason,
                    claimed_by = CASE WHEN :new_state = 'claimable'
                                      THEN NULL ELSE claimed_by END,
                    claimed_at = CASE WHEN :new_state = 'claimable'
                                      THEN NULL ELSE claimed_at END,
                    lease_expires_at = NULL,
                    updated_at = NOW()
                WHERE id = :id
                """
            ),
            {"id": goal_id, "new_state": new_state, "reason": reason},
        )
        # Record terminal failures only; transient retries don't write
        # a results row (we'd flood the table during a node hiccup).
        if new_state == "quarantined":
            conn.execute(
                text(
                    """
                    INSERT INTO goal_results (
                        goal_id, goal_type, target_id, node_id, state,
                        result_summary, error_message, duration_ms
                    )
                    VALUES (
                        :goal_id, :goal_type, :target_id, :node_id,
                        'quarantined', CAST('{}' AS JSONB), :reason,
                        :duration_ms
                    )
                    """
                ),
                {
                    "goal_id": goal_id,
                    "goal_type": gt,
                    "target_id": tid,
                    "node_id": node_id,
                    "reason": reason,
                    "duration_ms": duration_ms,
                },
            )

    log.info(
        "goal_queue: goal_id={gid} -> {state} after {n}/{m} attempts ({why})",
        gid=goal_id, state=new_state, n=attempts, m=max_attempts, why=reason,
    )
    return new_state


# --- lease housekeeping ----------------------------------------------------


def extend_lease(
    engine: Engine,
    *,
    goal_id: int,
    node_id: str,
    lease_seconds: int = 600,
) -> bool:
    """Push a claimed goal's lease forward. Returns False if the goal is
    no longer claimed by ``node_id`` (e.g. lease was reaped)."""
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                UPDATE goal_queue
                SET lease_expires_at = NOW() + make_interval(
                        secs => :lease_seconds
                    ),
                    updated_at = NOW()
                WHERE id = :id
                  AND state = 'claimed'
                  AND claimed_by = :node_id
                RETURNING id
                """
            ),
            {
                "id": goal_id,
                "node_id": node_id,
                "lease_seconds": lease_seconds,
            },
        ).fetchone()
    return row is not None


def reap_expired_leases(engine: Engine) -> int:
    """Reset any ``claimed`` rows whose lease has expired back to
    ``claimable``. Returns the number of rows reset."""
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                UPDATE goal_queue
                SET state = 'claimable',
                    claimed_by = NULL,
                    claimed_at = NULL,
                    lease_expires_at = NULL,
                    last_error = COALESCE(last_error, '') ||
                                 ' [reaped expired lease]',
                    updated_at = NOW()
                WHERE state = 'claimed'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < NOW()
                """
            )
        )
    n = result.rowcount or 0
    if n:
        log.warning("goal_queue: reaped {n} expired leases", n=n)
    return n


# --- introspection ---------------------------------------------------------


def queue_stats(engine: Engine) -> dict[str, int]:
    """Return per-state counts. Useful for ops dashboards and tests."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT state, COUNT(*) AS n
                FROM goal_queue
                GROUP BY state
                """
            )
        ).fetchall()
    return {state: int(n) for state, n in rows}


def recent_results(
    engine: Engine,
    *,
    node_id: str | None = None,
    since_minutes: int = 60,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Recent ``goal_results`` rows, used by the worker for hourly
    agent_hub reports."""
    sql = """
        SELECT id, goal_id, goal_type, target_id, node_id, state,
               result_summary, error_message, duration_ms, created_at
        FROM goal_results
        WHERE created_at > NOW() - make_interval(mins => :mins)
    """
    params: dict[str, Any] = {"mins": since_minutes}
    if node_id is not None:
        sql += " AND node_id = :node_id"
        params["node_id"] = node_id
    sql += " ORDER BY created_at DESC LIMIT :limit"
    params["limit"] = limit

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        summary = r.result_summary
        if isinstance(summary, str):
            summary = json.loads(summary)
        out.append(
            {
                "id": r.id,
                "goal_id": r.goal_id,
                "goal_type": r.goal_type,
                "target_id": r.target_id,
                "node_id": r.node_id,
                "state": r.state,
                "result_summary": summary,
                "error_message": r.error_message,
                "duration_ms": r.duration_ms,
                "created_at": r.created_at,
            }
        )
    return out
