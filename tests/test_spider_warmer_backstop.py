"""Spider graph warmer: the periodic reload is a backstop counted from the
last reload of any kind (2026-11-07).

The hourly spider oneshot already POSTs /spider/graph/reload after each run,
and a reload costs ~20 s of GIL-contended work inside grid-api, so the old
fixed 15-minute timer reloaded a graph that had usually just been loaded.
"""

from __future__ import annotations

import threading
from collections.abc import Callable

import pytest

from api.routers import intelligence_spider as spider


class _Stop(Exception):
    pass


def test_seconds_until_refresh_counts_from_the_last_reload() -> None:
    assert spider._seconds_until_refresh(1000.0, None, 3600) == 0.0
    assert spider._seconds_until_refresh(1000.0, 900.0, 3600) == 3500.0
    assert spider._seconds_until_refresh(5000.0, 900.0, 3600) == 0.0


def test_default_backstop_is_an_hour(monkeypatch) -> None:
    monkeypatch.delenv("GRID_SPIDER_GRAPH_REFRESH_SEC", raising=False)
    assert spider._refresh_interval_seconds() == 3600


def _drive(
    monkeypatch,
    *,
    reload_succeeds: bool,
    max_sleeps: int,
    on_sleep: Callable[[int, float], None] | None = None,
) -> tuple[list[float], list[float]]:
    """Run the warmer loop inline on a fake clock; return (sleeps, reload times).

    The warmup load happens at t=0. ``on_sleep(i, t)`` runs during the i-th
    sleep (e.g. to simulate a spider-triggered reload).
    """
    clock = {"t": 0.0}
    sleeps: list[float] = []
    reloads: list[float] = []

    def fake_sleep(s: float) -> None:
        sleeps.append(s)
        if len(sleeps) >= max_sleeps:
            raise _Stop
        if on_sleep is not None:
            on_sleep(len(sleeps), clock["t"])
        clock["t"] += s

    def initial_load(engine) -> None:  # noqa: ANN001
        spider._graph_loaded_at = clock["t"]

    def refresh(engine) -> dict:  # noqa: ANN001
        reloads.append(clock["t"])
        if reload_succeeds:
            spider._graph_loaded_at = clock["t"]
        return {}

    class _Inline:
        def __init__(self, target, **kwargs) -> None:  # noqa: ANN001
            self.target = target

        def start(self) -> None:
            with pytest.raises(_Stop):
                self.target()

    monkeypatch.setattr(spider.time, "time", lambda: clock["t"])
    monkeypatch.setattr(spider.time, "sleep", fake_sleep)
    monkeypatch.setattr(spider, "get_db_engine", lambda: object())
    monkeypatch.setattr(spider, "_refresh_interval_seconds", lambda: 3600)
    monkeypatch.setattr(spider, "_get_or_load_graph", initial_load)
    monkeypatch.setattr(spider, "refresh_graph", refresh)
    monkeypatch.setattr(spider, "_graph_loaded_at", None)
    monkeypatch.setattr(threading, "Thread", _Inline)
    spider.warm_graph_async()
    return sleeps, reloads


def test_backstop_reloads_once_per_interval(monkeypatch) -> None:
    sleeps, reloads = _drive(monkeypatch, reload_succeeds=True, max_sleeps=4)
    assert sleeps == [3600.0, 3600.0, 3600.0, 3600.0]
    assert reloads == [3600.0, 7200.0, 10800.0]


def test_a_spider_reload_pushes_the_backstop_out(monkeypatch) -> None:
    def spider_posts_reload(i: int, t: float) -> None:
        if i == 1:  # 40 minutes into the first wait
            spider._graph_loaded_at = t + 2400

    sleeps, reloads = _drive(monkeypatch, reload_succeeds=True, max_sleeps=3,
                             on_sleep=spider_posts_reload)
    # Woke at 3600, saw the 2400 reload, waited until 6000 instead of reloading.
    assert sleeps == [3600.0, 2400.0, 3600.0]
    assert reloads == [6000.0]


def test_a_failing_reload_does_not_spin(monkeypatch) -> None:
    sleeps, reloads = _drive(monkeypatch, reload_succeeds=False, max_sleeps=4)
    # _graph_loaded_at stays at the warmup load; each failure waits a full interval.
    assert sleeps == [3600.0, 3600.0, 3600.0, 3600.0]
    assert reloads == [3600.0, 7200.0, 10800.0]
