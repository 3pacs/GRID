"""Per-node idle-fleet goal worker — Day 1 of IDLE-FLEET-AGENT-LOOP PoC.

Long-running daemon. Polls ``goal_queue`` on grid-svr Postgres, claims
the next eligible goal for this node's hardware tier, dispatches to the
appropriate handler, writes results back, repeats. Designed for systemd
supervision (see ``deploy/systemd/grid-goal-worker.service.template``).

Hardware-tier eligibility, lease semantics, dedupe, and retry/quarantine
all live in ``intelligence/goal_queue.py``. This script is the *runtime
loop*: scheduling, duty-cycle enforcement, handler dispatch, cloud-LLM
refusal, heartbeat, agent_hub reporting.

Day 1 ships exactly one handler:

* ``score_active_hypothesis`` — wraps
  ``intelligence.hypothesis_engine.HypothesisGenerator.score_hypothesis``.
  Day 2-4 handlers (postmortem, options scan, sector expand) plug in
  via the same ``HANDLERS`` registry.

Locked decisions honored:
  #1 Cloud LLMs refused by default. Worker drops a goal back to the
     queue with ``last_error = 'cloud LLM requested but allow_cloud=False'``
     unless the row explicitly sets ``allow_cloud = TRUE``.
  #2 p9d ComfyUI co-scheduling skipped in Day 1 (TODO marker only).
  #3 grid-svr Postgres via ``db.get_engine``.
  #4 50% duty-cycle default; per-goal override via ``max_duty_cycle``.

Configuration (env vars, set by systemd unit):
  GRID_GOAL_WORKER_NODE_ID         e.g. "gridz4", "ocr-node", "koala"
  GRID_GOAL_WORKER_HARDWARE_TIER   one of cpu/medium_gpu/large_gpu/vision
  GRID_GOAL_WORKER_POLL_SECONDS    default 30
  GRID_GOAL_WORKER_LEASE_SECONDS   default 600
  GRID_GOAL_WORKER_MAX_DUTY_CYCLE  default 0.5
  GRID_GOAL_WORKER_DUTY_WINDOW_S   rolling window, default 300
  GRID_GOAL_WORKER_HEARTBEAT_SEC   default 60
  GRID_GOAL_WORKER_GOAL_TYPES      optional comma-list filter
  GRID_GOAL_WORKER_ALLOW_CLOUD     hard ceiling; even if a goal sets
                                    allow_cloud, the worker still
                                    refuses unless this is ``1``.
  GRID_GOAL_WORKER_DRY_RUN         if ``1``, claim+log+release goals
                                    without executing handlers.
"""

from __future__ import annotations

import os
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Deque

from loguru import logger as log
from sqlalchemy.engine import Engine

from db import get_engine
from intelligence.goal_queue import (
    Goal,
    claim_goal,
    extend_lease,
    mark_failed,
    reap_expired_leases,
    recent_results,
    submit_result,
)


# --- Cloud-LLM refusal -----------------------------------------------------


class CloudLLMRefused(RuntimeError):
    """Raised by a handler if it would have to call a cloud LLM but
    ``goal.allow_cloud`` (and/or the worker's global ceiling) is False."""


def _worker_allows_cloud() -> bool:
    """Hard ceiling. Defaults to False per locked decision #1."""
    return os.getenv("GRID_GOAL_WORKER_ALLOW_CLOUD", "").strip() in ("1", "true", "True")


def _check_cloud_permission(goal: Goal) -> None:
    """Raise :class:`CloudLLMRefused` unless both the goal row and the
    worker config explicitly allow cloud calls."""
    if not goal.allow_cloud:
        raise CloudLLMRefused(
            f"goal {goal.id} ({goal.goal_type}): allow_cloud=False, refusing cloud LLM"
        )
    if not _worker_allows_cloud():
        raise CloudLLMRefused(
            f"goal {goal.id} ({goal.goal_type}): worker has GRID_GOAL_WORKER_ALLOW_CLOUD=0, refusing cloud LLM"
        )


# --- Duty-cycle tracker ----------------------------------------------------


@dataclass
class DutyCycleTracker:
    """Rolling-window duty-cycle limiter.

    Tracks (start, end) pairs for completed work intervals. When asked
    whether to admit a new claim, computes the fraction of the rolling
    window covered by past work and refuses if it would exceed the cap.
    """

    window_s: int
    cap: float
    _intervals: Deque[tuple[float, float]]

    @classmethod
    def create(cls, window_s: int, cap: float) -> "DutyCycleTracker":
        if not (0.0 < cap <= 1.0):
            raise ValueError(f"cap must be in (0.0, 1.0], got {cap}")
        if window_s <= 0:
            raise ValueError(f"window_s must be positive, got {window_s}")
        return cls(window_s=window_s, cap=cap, _intervals=deque())

    def _trim(self, now: float) -> None:
        cutoff = now - self.window_s
        while self._intervals and self._intervals[0][1] < cutoff:
            self._intervals.popleft()

    def record(self, start: float, end: float) -> None:
        if end < start:
            return
        self._intervals.append((start, end))
        self._trim(end)

    def busy_fraction(self, now: float | None = None) -> float:
        now = time.monotonic() if now is None else now
        self._trim(now)
        cutoff = now - self.window_s
        total = 0.0
        for s, e in self._intervals:
            s_eff = max(s, cutoff)
            e_eff = min(e, now)
            if e_eff > s_eff:
                total += e_eff - s_eff
        return total / self.window_s

    def admits(self, cap_override: float | None = None) -> bool:
        cap = cap_override if cap_override is not None else self.cap
        return self.busy_fraction() < cap

    def sleep_until_admits(self, cap_override: float | None = None) -> float:
        """Return the seconds the worker should sleep before re-checking.
        Conservative: returns at most ``window_s`` so we always re-poll."""
        cap = cap_override if cap_override is not None else self.cap
        frac = self.busy_fraction()
        if frac < cap:
            return 0.0
        # We're over-cap; sleep until the oldest interval's tail falls
        # off the back of the window, then re-check.
        if not self._intervals:
            return 0.0
        oldest_end = self._intervals[0][1]
        slack = (oldest_end + self.window_s) - time.monotonic()
        return max(1.0, min(slack, float(self.window_s)))


# --- Handlers --------------------------------------------------------------

HandlerResult = dict[str, Any]
HandlerFn = Callable[[Engine, Goal], HandlerResult]


def handle_score_active_hypothesis(engine: Engine, goal: Goal) -> HandlerResult:
    """Score a single ``discovered_hypotheses`` row.

    Wraps :meth:`intelligence.hypothesis_engine.HypothesisGenerator.score_hypothesis`.
    Idempotent: if the hypothesis row's ``last_tested`` advanced after
    we claimed but before we ran (e.g. the hermes cron beat us to it),
    we still call ``score_hypothesis``; the engine handles a fresh
    Bayesian update and writes the latest result. The dedupe index on
    ``goal_queue`` prevents two open goals for the same hypothesis, so
    we don't double-write the queue, but the underlying scoring is
    cheap and safe to re-run.

    score_hypothesis is purely local (boost-log lookup + Bayesian math
    + DB updates), so cloud-LLM refusal is automatic — there's nothing
    to refuse. We still call ``_check_cloud_permission`` if the goal
    row declares ``allow_cloud`` to keep the contract explicit.
    """
    if goal.allow_cloud:
        # Defensive: the score path doesn't call cloud LLMs, but if
        # someone sets allow_cloud=True on this goal type we still
        # gate it on the worker's global ceiling so behaviour is
        # consistent across handlers.
        _check_cloud_permission(goal)

    # Lazy import to keep worker startup cheap when handler is unused.
    from intelligence.hypothesis_engine import HypothesisGenerator

    hypothesis_id = goal.target_id
    generator = HypothesisGenerator(engine)
    result = generator.score_hypothesis(hypothesis_id)

    summary: HandlerResult = {
        "hypothesis_id": hypothesis_id,
        "outcome": result.get("outcome"),
        "status": result.get("status"),
        "confidence": result.get("confidence"),
        "times_tested": result.get("times_tested"),
        "times_correct": result.get("times_correct"),
        "kill_reason": result.get("kill_reason"),
    }
    if "error" in result:
        summary["error"] = result["error"]
    return summary


def handle_hermes_diagnose_source(engine: Engine, goal: Goal) -> HandlerResult:
    """Run the Hermes source-doctor subagent for one source."""
    from scripts.hermes_fixers import _inspect_source

    return _inspect_source(engine, goal.target_id)


def handle_hermes_scout_free_data(engine: Engine, goal: Goal) -> HandlerResult:
    """Run the Hermes free-data scout subagent for one source."""
    from scripts.hermes_fixers import _scout_free_data_sources
    from scripts.hermes_health import OperatorState

    state = OperatorState()
    state.cycle_count = int(goal.payload.get("requested_cycle") or 0)
    return _scout_free_data_sources(engine, goal.target_id, state)


def handle_hermes_wiring_audit(engine: Engine, goal: Goal) -> HandlerResult:
    """Run the Hermes wiring-auditor subagent."""
    from scripts.hermes_fixers import _run_wiring_audit_summary

    result = _run_wiring_audit_summary()
    result["target_id"] = goal.target_id
    return result


HANDLERS: dict[str, HandlerFn] = {
    "score_active_hypothesis": handle_score_active_hypothesis,
    "hermes_diagnose_source": handle_hermes_diagnose_source,
    "hermes_scout_free_data": handle_hermes_scout_free_data,
    "hermes_wiring_audit": handle_hermes_wiring_audit,
}


# --- Worker loop -----------------------------------------------------------


@dataclass
class WorkerConfig:
    node_id: str
    hardware_tier: str
    poll_seconds: int
    lease_seconds: int
    max_duty_cycle: float
    duty_window_s: int
    heartbeat_seconds: int
    goal_types: tuple[str, ...] | None
    dry_run: bool


def load_config_from_env() -> WorkerConfig:
    node_id = os.getenv("GRID_GOAL_WORKER_NODE_ID")
    if not node_id:
        raise SystemExit("GRID_GOAL_WORKER_NODE_ID is required")
    tier = os.getenv("GRID_GOAL_WORKER_HARDWARE_TIER", "cpu")

    def _int(name: str, default: int) -> int:
        v = os.getenv(name)
        return int(v) if v else default

    def _float(name: str, default: float) -> float:
        v = os.getenv(name)
        return float(v) if v else default

    raw_types = os.getenv("GRID_GOAL_WORKER_GOAL_TYPES", "").strip()
    goal_types: tuple[str, ...] | None
    if raw_types:
        goal_types = tuple(s.strip() for s in raw_types.split(",") if s.strip())
    else:
        goal_types = None

    dry_run = os.getenv("GRID_GOAL_WORKER_DRY_RUN", "").strip() in ("1", "true", "True")

    return WorkerConfig(
        node_id=node_id,
        hardware_tier=tier,
        poll_seconds=_int("GRID_GOAL_WORKER_POLL_SECONDS", 30),
        lease_seconds=_int("GRID_GOAL_WORKER_LEASE_SECONDS", 600),
        max_duty_cycle=_float("GRID_GOAL_WORKER_MAX_DUTY_CYCLE", 0.5),
        duty_window_s=_int("GRID_GOAL_WORKER_DUTY_WINDOW_S", 300),
        heartbeat_seconds=_int("GRID_GOAL_WORKER_HEARTBEAT_SEC", 60),
        goal_types=goal_types,
        dry_run=dry_run,
    )


class _ShutdownFlag:
    """SIGTERM/SIGINT-driven shutdown latch."""

    def __init__(self) -> None:
        self._down = False

    def trip(self, *_: Any) -> None:
        log.warning("goal_worker: shutdown signal received")
        self._down = True

    def is_down(self) -> bool:
        return self._down


def _heartbeat(cfg: WorkerConfig, duty: DutyCycleTracker) -> None:
    log.info(
        "goal_worker heartbeat node={node} tier={tier} duty={duty:.0%}/{cap:.0%} types={types}",
        node=cfg.node_id,
        tier=cfg.hardware_tier,
        duty=duty.busy_fraction(),
        cap=cfg.max_duty_cycle,
        types=cfg.goal_types or "*",
    )


def _report_to_hub(cfg: WorkerConfig, engine: Engine) -> None:
    """Optional best-effort hourly report. Wrapped in try/except —
    we never let a reporting failure kill the worker."""
    try:
        results = recent_results(
            engine, node_id=cfg.node_id, since_minutes=60, limit=500
        )
        if not results:
            return
        counts: dict[str, int] = {}
        for r in results:
            counts[r["state"]] = counts.get(r["state"], 0) + 1
        log.info(
            "goal_worker hub-report node={node} hour-counts={counts}",
            node=cfg.node_id, counts=counts,
        )
        # The actual report_to_hub.sh wrapper is per-host and called by
        # systemd OnSuccess or by a cron entry — keeping the worker
        # loop pure-Python avoids subprocess fork costs. Day 2: shell
        # out to ``agent-report goal-worker <node>-<hour> body.md``.
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("goal_worker hub-report failed: {e}", e=str(exc))


def execute_goal(
    engine: Engine,
    goal: Goal,
    *,
    node_id: str,
    dry_run: bool = False,
) -> tuple[str, dict[str, Any] | str, int]:
    """Run a single goal end-to-end.

    Returns a tuple ``(outcome, payload, duration_ms)`` where:
      * outcome is ``'done'`` or ``'failed'``;
      * payload is the handler result dict on success, or the error
        message string on failure.
    """
    start = time.monotonic()

    handler = HANDLERS.get(goal.goal_type)
    if handler is None:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        return ("failed", f"no handler for goal_type={goal.goal_type!r}", elapsed_ms)

    if dry_run:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        return ("done", {"dry_run": True, "goal_type": goal.goal_type}, elapsed_ms)

    try:
        result = handler(engine, goal)
    except CloudLLMRefused as exc:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        return ("failed", f"cloud-refused: {exc}", elapsed_ms)
    except Exception as exc:  # pylint: disable=broad-except
        elapsed_ms = int((time.monotonic() - start) * 1000)
        return ("failed", f"{type(exc).__name__}: {exc}", elapsed_ms)

    elapsed_ms = int((time.monotonic() - start) * 1000)
    return ("done", result, elapsed_ms)


def run(cfg: WorkerConfig, engine: Engine | None = None) -> None:
    """Main loop. Blocks until SIGTERM/SIGINT or fatal error."""
    if engine is None:
        engine = get_engine()

    duty = DutyCycleTracker.create(cfg.duty_window_s, cfg.max_duty_cycle)
    shutdown = _ShutdownFlag()
    signal.signal(signal.SIGTERM, shutdown.trip)
    signal.signal(signal.SIGINT, shutdown.trip)

    log.info(
        "goal_worker: starting node={n} tier={t} poll={p}s cap={c:.0%} types={ty}",
        n=cfg.node_id, t=cfg.hardware_tier, p=cfg.poll_seconds,
        c=cfg.max_duty_cycle, ty=cfg.goal_types or "*",
    )

    # On startup, reap any leases this node abandoned in a prior crash.
    try:
        reap_expired_leases(engine)
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("goal_worker: startup reap failed: {e}", e=str(exc))

    last_heartbeat = 0.0
    last_hub_report = 0.0

    while not shutdown.is_down():
        now_m = time.monotonic()
        if now_m - last_heartbeat >= cfg.heartbeat_seconds:
            _heartbeat(cfg, duty)
            last_heartbeat = now_m
        if now_m - last_hub_report >= 3600:
            _report_to_hub(cfg, engine)
            last_hub_report = now_m

        cap_override = None  # may be overridden by goal.max_duty_cycle below
        if not duty.admits(cap_override):
            sleep_s = duty.sleep_until_admits(cap_override)
            log.debug(
                "goal_worker: duty-cycle paused, sleeping {s:.0f}s",
                s=sleep_s,
            )
            _interruptible_sleep(sleep_s, shutdown)
            continue

        try:
            goal = claim_goal(
                engine,
                node_id=cfg.node_id,
                hardware_tier=cfg.hardware_tier,
                lease_seconds=cfg.lease_seconds,
                goal_types=cfg.goal_types,
            )
        except Exception as exc:  # pragma: no cover — defensive
            log.warning("goal_worker: claim_goal raised: {e}", e=str(exc))
            _interruptible_sleep(cfg.poll_seconds, shutdown)
            continue

        if goal is None:
            _interruptible_sleep(cfg.poll_seconds, shutdown)
            continue

        # Per-goal duty-cycle override (locked decision #4).
        cap_override = goal.max_duty_cycle
        if cap_override is not None and not duty.admits(cap_override):
            # Release goal back to queue: a node currently over its
            # per-goal cap shouldn't sit on the lease.
            try:
                mark_failed(
                    engine,
                    goal_id=goal.id,
                    node_id=cfg.node_id,
                    reason=(
                        f"duty-cycle cap reached "
                        f"({duty.busy_fraction():.0%} >= {cap_override:.0%})"
                    ),
                )
            except Exception as exc:  # pragma: no cover — defensive
                log.warning("goal_worker: release-on-cap failed: {e}", e=str(exc))
            _interruptible_sleep(cfg.poll_seconds, shutdown)
            continue

        log.info(
            "goal_worker: claimed goal_id={gid} type={gt} target={tid} attempt={a}",
            gid=goal.id, gt=goal.goal_type, tid=goal.target_id, a=goal.attempts,
        )

        wall_start = time.monotonic()
        outcome, payload, duration_ms = execute_goal(
            engine, goal, node_id=cfg.node_id, dry_run=cfg.dry_run,
        )
        duty.record(wall_start, time.monotonic())

        try:
            if outcome == "done":
                summary = payload if isinstance(payload, dict) else {"result": str(payload)}
                submit_result(
                    engine,
                    goal_id=goal.id,
                    node_id=cfg.node_id,
                    result_summary=summary,
                    duration_ms=duration_ms,
                )
                log.info(
                    "goal_worker: done goal_id={gid} dur_ms={d}",
                    gid=goal.id, d=duration_ms,
                )
            else:
                err = payload if isinstance(payload, str) else str(payload)
                mark_failed(
                    engine,
                    goal_id=goal.id,
                    node_id=cfg.node_id,
                    reason=err,
                    duration_ms=duration_ms,
                )
        except Exception as exc:  # pragma: no cover — defensive
            log.warning(
                "goal_worker: result write-back failed for goal_id={gid}: {e}",
                gid=goal.id, e=str(exc),
            )

    log.info("goal_worker: shutting down cleanly")


def _interruptible_sleep(seconds: float, shutdown: _ShutdownFlag) -> None:
    if seconds <= 0:
        return
    # Wake every second so SIGTERM is responsive.
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if shutdown.is_down():
            return
        time.sleep(min(1.0, end - time.monotonic()))


def main() -> int:
    cfg = load_config_from_env()
    run(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
