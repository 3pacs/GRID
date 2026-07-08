"""Tests for physics.waves wave construction."""

from __future__ import annotations

import pytest

from physics.waves import WaveTask, build_execution_waves


def _noop() -> None:
    return None


def _wave_names(waves: list[list[WaveTask]]) -> list[list[str]]:
    return [sorted(task.name for task in wave) for wave in waves]


def test_wave_task_defaults_are_pending_and_isolated() -> None:
    task = WaveTask(name="ingest", callable=_noop)

    assert task.kwargs == {}
    assert task.depends_on == []
    assert task.result is None
    assert task.status == "pending"
    assert task.error is None
    assert task.duration_ms == 0.0


def test_build_execution_waves_groups_tasks_by_dependency_order() -> None:
    tasks = [
        WaveTask(name="ingest_prices", callable=_noop),
        WaveTask(name="ingest_news", callable=_noop),
        WaveTask(
            name="features",
            callable=_noop,
            depends_on=["ingest_prices", "ingest_news"],
        ),
        WaveTask(name="cluster", callable=_noop, depends_on=["features"]),
        WaveTask(name="backtest", callable=_noop, depends_on=["cluster"]),
    ]

    waves = build_execution_waves(tasks)

    assert _wave_names(waves) == [
        ["ingest_news", "ingest_prices"],
        ["features"],
        ["cluster"],
        ["backtest"],
    ]


def test_build_execution_waves_puts_isolated_tasks_in_first_wave() -> None:
    tasks = [
        WaveTask(name="source", callable=_noop),
        WaveTask(name="derived", callable=_noop, depends_on=["source"]),
        WaveTask(name="independent_audit", callable=_noop),
    ]

    waves = build_execution_waves(tasks)

    assert _wave_names(waves) == [
        ["independent_audit", "source"],
        ["derived"],
    ]


def test_build_execution_waves_raises_when_cycle_leaves_no_ready_tasks() -> None:
    tasks = [
        WaveTask(name="alpha", callable=_noop, depends_on=["beta"]),
        WaveTask(name="beta", callable=_noop, depends_on=["alpha"]),
    ]

    with pytest.raises(ValueError, match="Circular dependency: no tasks ready"):
        build_execution_waves(tasks)
