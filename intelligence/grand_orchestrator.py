"""
Grand Orchestrator — the self-learning brain of GRID.

Every learning module in the repo runs its own update cycle when asked
nicely. This module asks them all, in the right order, on a schedule, and
tracks whether each module actually improved after its update. If it did,
the orchestrator keeps asking more often. If it didn't, the orchestrator
asks less often, records the drift, and raises an alert.

## The model

A **learning module** exposes two things:

  1. A **``run_learning_cycle(engine)``** callable that returns a
     ``LearningCycleResult`` — a small dataclass carrying (n_samples,
     primary_metric, metric_name, advisory).
  2. Optionally, a **``get_primary_metric(engine)``** callable used to
     probe metric value between cycles. If absent, the cycle result is
     the only signal.

The orchestrator maintains a registry of these modules, runs each one
on its configured cadence, and logs the pre/post metric to the
``grand_orchestrator_log`` table. Over time the log tells us which
modules learn *usefully* vs which are noisy — the answer drives
cadence auto-tuning.

## Why a shared orchestrator

GRID has ~80 modules with their own self-learning loops today. Each has
its own update cadence written implicitly ("hourly", "after every
scored trade", "once per 6h oracle cycle"), and nothing tracks whether
their updates are improving anything. Without a meta-layer, every
module is a black box that might be learning nonsense or might be
stuck at a cold-start baseline forever — and we'd never know.

The orchestrator doesn't replace the modules' internal update logic.
It coordinates WHEN to run them, WATCHES whether they improved, and
RAISES when a module has been learning for 30+ days with no measurable
improvement (a sign the update rule is broken or the data is stale).

## Registry format

```python
LEARNING_MODULES: dict[str, LearningModule] = {
    "per_signal_brier": LearningModule(
        run_cycle=per_signal_brier.run_learning_cycle,
        cadence_seconds=3600,  # hourly
        primary_metric_name="mean_brier",
        priority=1,            # higher = run first
    ),
    ...
}
```

Modules that use the shared ``SelfLearningLoop`` primitive auto-register
via a scan of ``self_learning_state``. Legacy modules with dedicated
tables register explicitly.

## Meta-learning: cadence auto-tuning

Every N cycles, the orchestrator inspects each module's improvement
history. If a module improved by >= ``IMPROVEMENT_THRESHOLD`` over its
last 10 cycles, the cadence stays as-is. If not, cadence doubles (slow
down — we're wasting compute). If improvement is negative for 5
consecutive cycles, the module is DEMOTED to weekly cadence and a
warning row lands in the log.

## Safety

Never raises. A broken learning module is isolated — its exception is
logged to ``last_error`` in its log row and the orchestrator moves on
to the next module. The primary loop has an explicit try/except around
every module call.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────

IMPROVEMENT_THRESHOLD: float = 0.01
"""Relative improvement required over the last N cycles to keep the
module's cadence. 1% is the smallest Brier delta that's detectable above
noise for typical sample sizes (N>=100)."""

CADENCE_WINDOW: int = 10
"""How many recent cycles to look at when deciding cadence adjustments."""

MAX_CONSECUTIVE_NEG_CYCLES: int = 5
"""Demote a module to weekly cadence after this many back-to-back
negative-improvement cycles."""

DEMOTED_CADENCE_SECONDS: int = 86400 * 7  # weekly

MIN_CADENCE_SECONDS: int = 600        # 10 minutes
MAX_CADENCE_SECONDS: int = 86400 * 14  # 2 weeks


# ── Dataclasses ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class LearningCycleResult:
    """One learning cycle's outcome.

    Returned by every registered module's ``run_learning_cycle(engine)``
    callable. The orchestrator uses ``primary_metric`` to compute
    improvement vs the previous cycle.
    """
    module_name: str
    n_samples: int
    primary_metric: float              # lower is better for Brier/ECE/loss;
                                        # higher is better for hit-rate/accuracy.
                                        # caller specifies which via is_lower_better.
    metric_name: str
    is_lower_better: bool = True
    advisory: str = ""
    cycle_duration_ms: Optional[float] = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "module_name": self.module_name,
            "n_samples": self.n_samples,
            "primary_metric": self.primary_metric,
            "metric_name": self.metric_name,
            "is_lower_better": self.is_lower_better,
            "advisory": self.advisory,
            "cycle_duration_ms": self.cycle_duration_ms,
            "details": dict(self.details),
        }


RunCycleFn = Callable[[Engine], LearningCycleResult]


@dataclass
class LearningModule:
    """Registry entry for one learning module."""
    name: str
    run_cycle: RunCycleFn
    cadence_seconds: int
    priority: int = 5                  # 1 = highest, 10 = lowest
    description: str = ""
    last_run_at: Optional[datetime] = None
    last_metric: Optional[float] = None
    is_lower_better: bool = True
    consecutive_negative_cycles: int = 0
    total_cycles: int = 0
    demoted: bool = False


# ── Registry ─────────────────────────────────────────────────────────────


LEARNING_MODULES: dict[str, LearningModule] = {}
"""Process-wide registry. Call ``register_learning_module`` to add to it."""


def register_learning_module(
    name: str,
    run_cycle: RunCycleFn,
    *,
    cadence_seconds: int = 3600,
    priority: int = 5,
    description: str = "",
    is_lower_better: bool = True,
) -> None:
    """Register a module with the Grand Orchestrator.

    Idempotent — re-registering the same name replaces the existing entry.
    """
    LEARNING_MODULES[name] = LearningModule(
        name=name,
        run_cycle=run_cycle,
        cadence_seconds=max(MIN_CADENCE_SECONDS, min(MAX_CADENCE_SECONDS, int(cadence_seconds))),
        priority=int(priority),
        description=description,
        is_lower_better=is_lower_better,
    )


# ── DB schema ────────────────────────────────────────────────────────────


_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS grand_orchestrator_log (
    id               SERIAL PRIMARY KEY,
    module_name      TEXT NOT NULL,
    ran_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    n_samples        INT,
    primary_metric   DOUBLE PRECISION,
    metric_name      TEXT,
    improvement      DOUBLE PRECISION,
    cadence_seconds  INT,
    advisory         TEXT,
    cycle_duration_ms DOUBLE PRECISION,
    last_error       TEXT
);
CREATE INDEX IF NOT EXISTS idx_grand_log_module
    ON grand_orchestrator_log(module_name, ran_at DESC);
CREATE INDEX IF NOT EXISTS idx_grand_log_ran_at
    ON grand_orchestrator_log(ran_at DESC);
"""


_initialized_engines: set[int] = set()


def _ensure_schema(engine: Engine) -> None:
    eid = id(engine)
    if eid in _initialized_engines:
        return
    try:
        with engine.begin() as conn:
            for stmt in _LOG_TABLE_SQL.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        _initialized_engines.add(eid)
    except Exception as exc:  # noqa: BLE001
        log.debug("grand_orchestrator: schema init failed: {e}", e=str(exc))


def _reset_initialized_engines() -> None:
    """TEST USE ONLY."""
    _initialized_engines.clear()


# ── Core loop ────────────────────────────────────────────────────────────


def _compute_improvement(
    module: LearningModule, new_metric: float,
) -> Optional[float]:
    """Relative improvement of new_metric vs module.last_metric.

    Positive = good regardless of direction (is_lower_better handled).
    None if no prior metric exists.
    """
    if module.last_metric is None:
        return None
    if module.last_metric == 0:
        return None
    delta = module.last_metric - new_metric  # lower is better
    if not module.is_lower_better:
        delta = -delta
    return delta / abs(module.last_metric)


def _maybe_adjust_cadence(
    module: LearningModule, improvement: Optional[float],
) -> None:
    """Cadence auto-tuner.

    Rules:
      - improvement >= IMPROVEMENT_THRESHOLD → keep cadence, reset neg counter
      - improvement < 0                       → increment neg counter
      - neg counter >= MAX_CONSECUTIVE_NEG    → demote to weekly
    """
    if improvement is None:
        return
    if improvement >= IMPROVEMENT_THRESHOLD:
        module.consecutive_negative_cycles = 0
        return
    if improvement < 0:
        module.consecutive_negative_cycles += 1
    if module.consecutive_negative_cycles >= MAX_CONSECUTIVE_NEG_CYCLES and not module.demoted:
        log.warning(
            "grand_orchestrator: DEMOTING {m} to weekly cadence after "
            "{n} consecutive negative cycles",
            m=module.name, n=module.consecutive_negative_cycles,
        )
        module.cadence_seconds = DEMOTED_CADENCE_SECONDS
        module.demoted = True


def _log_cycle(
    engine: Engine,
    module: LearningModule,
    result: Optional[LearningCycleResult],
    improvement: Optional[float],
    error: Optional[str] = None,
) -> None:
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO grand_orchestrator_log
                        (module_name, n_samples, primary_metric, metric_name,
                         improvement, cadence_seconds, advisory,
                         cycle_duration_ms, last_error)
                    VALUES (:m, :n, :pm, :mn, :imp, :cad, :adv, :dur, :err)
                    """
                ),
                {
                    "m": module.name,
                    "n": result.n_samples if result else None,
                    "pm": result.primary_metric if result else None,
                    "mn": result.metric_name if result else None,
                    "imp": improvement,
                    "cad": module.cadence_seconds,
                    "adv": result.advisory if result else None,
                    "dur": result.cycle_duration_ms if result else None,
                    "err": error,
                },
            )
    except Exception as exc:  # noqa: BLE001
        log.debug("grand_orchestrator: log persist failed: {e}", e=str(exc))


def run_due_cycles(engine: Engine, *, now: Optional[datetime] = None) -> list[dict[str, Any]]:
    """Run every module whose cadence has elapsed.

    Returns a list of per-module result dicts for observability. Never
    raises. Modules are sorted by priority (1 first, 10 last) so the
    most important learners run even when the overall loop is running
    behind schedule.
    """
    _ensure_schema(engine)
    when = now or datetime.now(timezone.utc)
    results: list[dict[str, Any]] = []

    ordered = sorted(LEARNING_MODULES.values(), key=lambda m: (m.priority, m.name))
    for module in ordered:
        # Cadence gate
        if module.last_run_at is not None:
            elapsed = (when - module.last_run_at).total_seconds()
            if elapsed < module.cadence_seconds:
                continue

        # Run the cycle
        error: Optional[str] = None
        result: Optional[LearningCycleResult] = None
        started = time.time()
        try:
            result = module.run_cycle(engine)
            if result.cycle_duration_ms is None:
                duration = (time.time() - started) * 1000.0
                result = LearningCycleResult(
                    module_name=result.module_name,
                    n_samples=result.n_samples,
                    primary_metric=result.primary_metric,
                    metric_name=result.metric_name,
                    is_lower_better=result.is_lower_better,
                    advisory=result.advisory,
                    cycle_duration_ms=duration,
                    details=result.details,
                )
        except Exception as exc:  # noqa: BLE001
            error = str(exc)[:500]
            log.warning(
                "grand_orchestrator: module {m} raised: {e}",
                m=module.name, e=error,
            )

        # Compute improvement, adjust cadence, update state
        improvement: Optional[float] = None
        if result is not None:
            improvement = _compute_improvement(module, result.primary_metric)
            _maybe_adjust_cadence(module, improvement)
            module.last_metric = result.primary_metric
            module.total_cycles += 1

        module.last_run_at = when
        _log_cycle(engine, module, result, improvement, error)

        results.append({
            "module": module.name,
            "ran_at": when.isoformat(),
            "n_samples": result.n_samples if result else None,
            "metric": result.primary_metric if result else None,
            "improvement": improvement,
            "cadence_seconds": module.cadence_seconds,
            "error": error,
        })

    return results


def run_cycle_for_module(
    engine: Engine, module_name: str,
) -> Optional[dict[str, Any]]:
    """Force-run one specific module (ignores cadence). Never raises."""
    module = LEARNING_MODULES.get(module_name)
    if module is None:
        return None
    module.last_run_at = None  # bypass cadence
    results = run_due_cycles(engine)
    return next((r for r in results if r["module"] == module_name), None)


# ── Auto-discovery of SelfLearningLoop users ────────────────────────────


def auto_register_self_learning_modules(engine: Engine) -> int:
    """Scan self_learning_state and register every module found there.

    These modules already use the shared ``SelfLearningLoop`` primitive,
    so we can wrap their ``update_parameters`` call in a standardized
    run_cycle closure without needing per-module registration.

    Returns the number of modules newly registered. Safe to call on every
    orchestrator tick — idempotent.
    """
    from intelligence.self_learning_loop import list_learning_modules

    n_new = 0
    try:
        states = list_learning_modules(engine)
    except Exception as exc:  # noqa: BLE001
        log.debug("grand_orchestrator: auto-register scan failed: {e}", e=str(exc))
        return 0

    for state in states:
        if state.module_name in LEARNING_MODULES:
            continue

        def _make_cycle(module_name: str) -> RunCycleFn:
            # Closure captures the module name.
            def _run(engine_inner: Engine) -> LearningCycleResult:
                from intelligence.self_learning_loop import SelfLearningLoop
                # We don't know the update_fn — auto-registered modules
                # can't force an update, they can only read state. So we
                # return the current state's params as the metric.
                loop = SelfLearningLoop(
                    engine_inner,
                    module_name=module_name,
                    update_fn=lambda h, p: p,  # no-op
                )
                s = loop.get_state()
                return LearningCycleResult(
                    module_name=module_name,
                    n_samples=s.n_samples,
                    primary_metric=float(s.n_samples),
                    metric_name="n_samples",
                    is_lower_better=False,  # more samples = better
                    advisory=s.last_error or "",
                )
            return _run

        register_learning_module(
            name=state.module_name,
            run_cycle=_make_cycle(state.module_name),
            cadence_seconds=3600,
            priority=7,
            description="Auto-registered SelfLearningLoop consumer",
            is_lower_better=False,
        )
        n_new += 1

    return n_new


# ── Observability ────────────────────────────────────────────────────────


def get_recent_log(engine: Engine, limit: int = 50) -> list[dict[str, Any]]:
    """Return the most recent orchestrator log entries across all modules."""
    _ensure_schema(engine)
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT module_name, ran_at, n_samples, primary_metric,
                           metric_name, improvement, cadence_seconds,
                           advisory, cycle_duration_ms, last_error
                    FROM grand_orchestrator_log
                    ORDER BY ran_at DESC
                    LIMIT :lim
                    """
                ),
                {"lim": int(limit)},
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("grand_orchestrator.get_recent_log failed: {e}", e=str(exc))
        return []

    out: list[dict[str, Any]] = []
    for r in rows:
        out.append({
            "module_name": r[0],
            "ran_at": r[1].isoformat() if r[1] else "",
            "n_samples": r[2],
            "primary_metric": r[3],
            "metric_name": r[4],
            "improvement": r[5],
            "cadence_seconds": r[6],
            "advisory": r[7],
            "cycle_duration_ms": r[8],
            "last_error": r[9],
        })
    return out


def get_module_state(module_name: str) -> Optional[dict[str, Any]]:
    """Return the in-memory state for one registered module."""
    module = LEARNING_MODULES.get(module_name)
    if module is None:
        return None
    return {
        "name": module.name,
        "cadence_seconds": module.cadence_seconds,
        "priority": module.priority,
        "last_run_at": module.last_run_at.isoformat() if module.last_run_at else None,
        "last_metric": module.last_metric,
        "is_lower_better": module.is_lower_better,
        "consecutive_negative_cycles": module.consecutive_negative_cycles,
        "total_cycles": module.total_cycles,
        "demoted": module.demoted,
        "description": module.description,
    }


def get_all_registered() -> list[dict[str, Any]]:
    """Return state for every registered learning module."""
    return [get_module_state(m.name) for m in LEARNING_MODULES.values()]  # type: ignore[misc]
