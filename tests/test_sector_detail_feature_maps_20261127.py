"""The sector detail request never rebuilds the sector-independent feature maps inline (#959 probe finding).

Observed 2026-11-27: opening the Technology sector page ran past Cloudflare's ~100 s limit (524) and the journal
had no completed /sectors/<name>/detail request in 72 h. The z-scores/values come from one live snapshot plus a
504-day matrix for every model-eligible feature; they were rebuilt inline on every request. They are now warmed
in the background and the request path only reads the cache.
"""
from __future__ import annotations

import inspect
import os
from unittest.mock import MagicMock

import pandas as pd
import pytest

os.environ.setdefault("DB_PASSWORD", "ci_placeholder_pw")

from api.routers import flows


@pytest.fixture(autouse=True)
def _clean_caches(monkeypatch):
    flows._feature_maps_cache.clear()
    flows._feature_maps_stale_cache.clear()
    monkeypatch.setattr(flows, "_ensure_sector_warm_thread", lambda: None)   # no background thread in tests
    yield
    flows._feature_maps_cache.clear()
    flows._feature_maps_stale_cache.clear()


def test_the_detail_handler_does_not_compute_feature_maps_inline():
    source = inspect.getsource(flows.get_sector_detail)
    for forbidden in ("LiveInference", "get_feature_snapshot", "get_feature_matrix", "get_pit_store"):
        assert forbidden not in source
    assert "_feature_maps_for_request()" in source


def test_cold_cache_is_unavailable_and_never_computes(monkeypatch):
    def boom(*_a, **_k):
        raise AssertionError("the request path must not compute the maps")

    monkeypatch.setattr(flows, "_compute_feature_maps", boom)
    z, v, state = flows._feature_maps_for_request()
    assert z == {} and v == {}
    assert state == {"state": "unavailable", "age_seconds": None}


def test_warm_fills_both_tiers_and_the_request_reads_fresh_then_stale(monkeypatch):
    monkeypatch.setattr(flows, "get_db_engine", lambda: MagicMock())
    monkeypatch.setattr(flows, "_compute_feature_maps", lambda _e: ({"f1": 1.5}, {"f1": 10.0}))
    assert flows._warm_feature_maps() is True
    z, v, state = flows._feature_maps_for_request()
    assert (z, v, state["state"]) == ({"f1": 1.5}, {"f1": 10.0}, "fresh")
    # The fresh tier expires first; the 6 h stale tier still answers, and says so.
    flows._feature_maps_cache.clear()
    z, v, state = flows._feature_maps_for_request()
    assert (z, v, state["state"]) == ({"f1": 1.5}, {"f1": 10.0}, "stale")
    assert isinstance(state["age_seconds"], int) and state["age_seconds"] >= 0


def test_a_failed_warm_is_not_cached_and_keeps_the_previous_maps(monkeypatch):
    monkeypatch.setattr(flows, "get_db_engine", lambda: MagicMock())
    monkeypatch.setattr(flows, "_compute_feature_maps", lambda _e: ({"f1": 1.5}, {"f1": 10.0}))
    assert flows._warm_feature_maps() is True

    def fail(_e):
        raise RuntimeError("db blip")

    monkeypatch.setattr(flows, "_compute_feature_maps", fail)
    assert flows._warm_feature_maps() is False          # never raises
    z, _v, state = flows._feature_maps_for_request()
    assert z == {"f1": 1.5} and state["state"] == "fresh"   # the good maps are not replaced by "no features"


def test_a_failed_first_warm_stays_unavailable_not_cached_as_empty(monkeypatch):
    monkeypatch.setattr(flows, "get_db_engine", lambda: MagicMock())
    monkeypatch.setattr(flows, "_compute_feature_maps", lambda _e: (_ for _ in ()).throw(RuntimeError("x")))
    assert flows._warm_feature_maps() is False
    assert flows._feature_maps_for_request()[2]["state"] == "unavailable"


def test_the_sector_list_warm_no_longer_depends_on_or_runs_the_feature_maps(monkeypatch):
    # Codex (review 6085440765): a failed list warm made zero feature attempts, and the list cycle waited for the
    # feature warm. The list warm is now independent of the maps in both directions.
    monkeypatch.setattr(flows, "_warm_feature_maps",
                        lambda: (_ for _ in ()).throw(AssertionError("the list warm must not run the maps")))
    monkeypatch.setattr(flows, "_compute_sectors_payload", lambda: {"sectors": {"Technology": {}}})
    monkeypatch.setattr(flows, "_persist_sectors_snapshot", lambda _r: None)
    assert flows._sector_warm_cycle() == max(flows._SECTOR_CACHE_TTL - 60.0, flows._SECTOR_WARM_RETRY_SECONDS)


def test_a_failed_list_warm_does_not_stop_the_feature_maps_warm(monkeypatch):
    calls = []
    monkeypatch.setattr(flows, "_compute_sectors_payload", lambda: (_ for _ in ()).throw(RuntimeError("list down")))
    monkeypatch.setattr(flows, "get_db_engine", lambda: MagicMock())
    monkeypatch.setattr(flows, "_compute_feature_maps", lambda _e: calls.append("maps") or ({"f": 1.0}, {"f": 2.0}))
    assert flows._sector_warm_cycle() == flows._SECTOR_WARM_RETRY_SECONDS        # the list failed...
    assert flows._feature_maps_warm_cycle() > 0                                    # ...the maps still warm on their own
    assert calls == ["maps"] and flows._feature_maps_for_request()[2]["state"] == "fresh"


def test_the_two_warms_run_on_separate_threads(monkeypatch):
    started = []

    class _Thread:
        def __init__(self, target=None, daemon=None, name=None):
            started.append((name, target))

        def start(self):
            pass

    monkeypatch.undo()                                  # the autouse fixture stubs _ensure_sector_warm_thread
    monkeypatch.setattr(flows, "_sector_warm_thread_started", False)
    monkeypatch.setattr(flows, "_load_persisted_sectors_snapshot", lambda: None)
    monkeypatch.setattr(flows.threading, "Thread", _Thread)
    flows._ensure_sector_warm_thread()
    assert dict(started) == {"sector-flow-warm": flows._sector_warm_loop,
                             "sector-feature-maps-warm": flows._feature_maps_warm_loop}
    monkeypatch.setattr(flows, "_sector_warm_thread_started", False)   # leave the module as a fresh process


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now


def test_the_feature_warm_cadence_keeps_the_fill_interval_inside_the_fresh_ttl(monkeypatch):
    # Codex's numbers: a 70 s maps warm. Scheduled relative to its own start, start-to-start is 70 + sleep.
    clock = _Clock()
    monkeypatch.setattr(flows.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(flows, "get_db_engine", lambda: MagicMock())

    def slow(_e):
        clock.now += 70.0
        return {"f": 1.0}, {"f": 2.0}

    monkeypatch.setattr(flows, "_compute_feature_maps", slow)
    sleep_s = flows._feature_maps_warm_cycle()
    assert sleep_s == pytest.approx(flows._FEATURE_MAPS_TTL - 60.0 - 70.0)           # 170 s
    assert 70.0 + sleep_s <= flows._FEATURE_MAPS_TTL                                  # 240 s <= 300 s fresh TTL


def test_a_warm_longer_than_the_window_retries_at_the_floor_not_negative(monkeypatch):
    clock = _Clock()
    monkeypatch.setattr(flows.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(flows, "get_db_engine", lambda: MagicMock())

    def very_slow(_e):
        clock.now += 400.0
        return {}, {}

    monkeypatch.setattr(flows, "_compute_feature_maps", very_slow)
    assert flows._feature_maps_warm_cycle() == flows._SECTOR_WARM_RETRY_SECONDS


def test_a_failed_maps_warm_retries_after_the_short_interval(monkeypatch):
    monkeypatch.setattr(flows, "get_db_engine", lambda: MagicMock())
    monkeypatch.setattr(flows, "_compute_feature_maps", lambda _e: (_ for _ in ()).throw(RuntimeError("x")))
    assert flows._feature_maps_warm_cycle() == flows._SECTOR_WARM_RETRY_SECONDS


def test_compute_feature_maps_keeps_the_z_score_formula(monkeypatch):
    import api.dependencies as deps
    import inference.live as live

    class _Snapshot:
        empty = False

        def to_dict(self, _orient):
            return [{"name": "a", "value": 3.0}, {"name": "b", "value": 7.5}]

    class _Li:
        def __init__(self, *_a):
            pass

        def get_feature_snapshot(self):
            return _Snapshot()

    hist = pd.DataFrame({1: list(range(30)), 2: [5.0] * 30})      # col 2 is constant: std 0 -> treated as 1
    pit = MagicMock()
    pit.get_feature_matrix.return_value = hist
    monkeypatch.setattr(deps, "get_pit_store", lambda: pit)
    monkeypatch.setattr(live, "LiveInference", _Li)
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [(1, "a"), (2, "b")]

    z, v = flows._compute_feature_maps(engine)
    expected_a = round((29 - hist[1].mean()) / hist[1].std(), 3)
    assert z["a"] == expected_a
    assert z["b"] == 0.0                   # (5 - 5) / 1
    assert v == {"a": 3.0, "b": 7.5}


def test_compute_feature_maps_raises_instead_of_returning_empty_on_failure(monkeypatch):
    import api.dependencies as deps

    def fail():
        raise RuntimeError("pit unavailable")

    monkeypatch.setattr(deps, "get_pit_store", fail)
    with pytest.raises(RuntimeError):
        flows._compute_feature_maps(MagicMock())
