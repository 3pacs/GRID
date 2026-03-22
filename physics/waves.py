"""
GRID wave-based pipeline execution.

Adapted from Get Physics Done's wave-based parallel execution model.
Groups workflows into dependency-ordered waves so that independent tasks
within each wave can run concurrently, while dependent waves execute
sequentially.

Example wave ordering for GRID:
  Wave 0: All ingestion pullers (no internal dependencies)
  Wave 1: Conflict resolution, feature computation (depend on ingestion)
  Wave 2: Clustering, orthogonality audit (depend on features)
  Wave 3: Backtesting, physics verification (depend on clustering)
  Wave 4: Model promotion decisions (depend on validation)
"""

from __future__ import annotations

import concurrent.futures
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from loguru import logger as log


@dataclass
class WaveTask:
    """A single task within a wave."""

    name: str
    callable: Callable[..., Any]
    kwargs: dict[str, Any] = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    result: Any = None
    status: str = "pending"  # pending | running | success | failed
    error: str | None = None
    duration_ms: float = 0.0


def build_execution_waves(tasks: list[WaveTask]) -> list[list[WaveTask]]:
    """Group tasks into dependency-ordered waves.

    Uses topological sorting: tasks with no unmet dependencies form the
    next wave.  Tasks within a wave have no mutual dependencies and can
    execute in parallel.

    Parameters:
        tasks: List of WaveTask objects with dependency declarations.

    Returns:
        list[list[WaveTask]]: Waves ordered by dependency depth.
            Wave 0 has no dependencies, wave 1 depends only on wave 0, etc.

    Raises:
        ValueError: If a circular dependency is detected.
    """
    task_map = {t.name: t for t in tasks}
    remaining = set(task_map.keys())
    completed: set[str] = set()
    waves: list[list[WaveTask]] = []

    iteration_limit = len(tasks) + 1
    iterations = 0

    while remaining:
        iterations += 1
        if iterations > iteration_limit:
            raise ValueError(
                f"Circular dependency detected among: {remaining}"
            )

        # Find tasks whose dependencies are all satisfied
        ready = []
        for name in list(remaining):
            task = task_map[name]
            unmet = [d for d in task.depends_on if d in remaining]
            if not unmet:
                ready.append(task)

        if not ready:
            raise ValueError(
                f"Circular dependency: no tasks ready among {remaining}. "
                f"Check depends_on fields."
            )

        waves.append(ready)
        for t in ready:
            remaining.discard(t.name)
            completed.add(t.name)

    log.info(
        "Built {w} execution waves from {t} tasks",
        w=len(waves),
        t=len(tasks),
    )
    for i, wave in enumerate(waves):
        log.debug(
            "  Wave {i}: {names}",
            i=i,
            names=[t.name for t in wave],
        )

    return waves


def execute_waves(
    waves: list[list[WaveTask]],
    max_workers: int = 4,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute waves sequentially, tasks within each wave in parallel.

    Parameters:
        waves: Dependency-ordered waves from build_execution_waves().
        max_workers: Max concurrent tasks per wave.
        dry_run: If True, log what would run without executing.

    Returns:
        dict: Execution summary with per-task results.
    """
    summary: dict[str, Any] = {
        "total_waves": len(waves),
        "total_tasks": sum(len(w) for w in waves),
        "wave_results": [],
        "success_count": 0,
        "failure_count": 0,
        "total_duration_ms": 0.0,
    }

    overall_start = time.monotonic()

    for wave_idx, wave in enumerate(waves):
        wave_start = time.monotonic()
        task_names = [t.name for t in wave]
        log.info(
            "Executing wave {i}/{n}: {tasks}",
            i=wave_idx,
            n=len(waves) - 1,
            tasks=task_names,
        )

        if dry_run:
            for task in wave:
                task.status = "skipped"
            summary["wave_results"].append({
                "wave": wave_idx,
                "tasks": task_names,
                "status": "dry_run",
            })
            continue

        if len(wave) == 1:
            # Single task — no need for thread pool
            task = wave[0]
            _execute_single_task(task)
        else:
            # Parallel execution within wave
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(max_workers, len(wave))
            ) as executor:
                futures = {
                    executor.submit(_execute_single_task, task): task
                    for task in wave
                }
                concurrent.futures.wait(futures)

        wave_duration = (time.monotonic() - wave_start) * 1000
        wave_results = []
        for task in wave:
            wave_results.append({
                "name": task.name,
                "status": task.status,
                "duration_ms": round(task.duration_ms, 1),
                "error": task.error,
            })
            if task.status == "success":
                summary["success_count"] += 1
            else:
                summary["failure_count"] += 1

        summary["wave_results"].append({
            "wave": wave_idx,
            "tasks": wave_results,
            "duration_ms": round(wave_duration, 1),
        })

        # Check for critical failures — abort remaining waves if needed
        failed_tasks = [t for t in wave if t.status == "failed"]
        if failed_tasks:
            log.warning(
                "Wave {i} had {n} failures: {names}",
                i=wave_idx,
                n=len(failed_tasks),
                names=[t.name for t in failed_tasks],
            )

    summary["total_duration_ms"] = round(
        (time.monotonic() - overall_start) * 1000, 1
    )

    log.info(
        "Wave execution complete: {ok} succeeded, {fail} failed in {ms:.0f}ms",
        ok=summary["success_count"],
        fail=summary["failure_count"],
        ms=summary["total_duration_ms"],
    )

    return summary


def _execute_single_task(task: WaveTask) -> None:
    """Execute a single task, updating its status and timing."""
    task.status = "running"
    start = time.monotonic()
    try:
        task.result = task.callable(**task.kwargs)
        task.status = "success"
    except Exception as exc:
        task.status = "failed"
        task.error = str(exc)
        log.error("Task {n} failed: {e}", n=task.name, e=str(exc))
    finally:
        task.duration_ms = (time.monotonic() - start) * 1000


def build_grid_pipeline_waves(
    enabled_workflows: list[dict],
) -> list[list[WaveTask]]:
    """Build wave execution plan from enabled GRID workflows.

    Reads the depends_on field from each workflow and constructs
    WaveTask objects.  The actual callable must be resolved separately
    (via workflow runner or puller registry).

    Parameters:
        enabled_workflows: List of parsed workflow dicts from loader.

    Returns:
        list[list[WaveTask]]: Dependency-ordered waves.
    """
    tasks: list[WaveTask] = []

    for wf in enabled_workflows:
        task = WaveTask(
            name=wf["name"],
            callable=lambda **kw: None,  # Placeholder — resolved at runtime
            depends_on=wf.get("depends_on", []),
        )
        tasks.append(task)

    return build_execution_waves(tasks)
