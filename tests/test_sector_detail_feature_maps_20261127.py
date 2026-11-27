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


def test_the_sector_warm_cycle_warms_the_feature_maps_and_a_maps_failure_does_not_fail_it(monkeypatch):
    calls = []
    monkeypatch.setattr(flows, "_compute_sectors_payload", lambda: {"sectors": {"Technology": {}}})
    monkeypatch.setattr(flows, "_persist_sectors_snapshot", lambda _r: None)
    monkeypatch.setattr(flows, "_warm_feature_maps", lambda: calls.append("maps") or False)
    sleep_s = flows._sector_warm_cycle()
    assert calls == ["maps"]
    assert sleep_s == max(flows._SECTOR_CACHE_TTL - 60.0, flows._SECTOR_WARM_RETRY_SECONDS)   # still the success cadence


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
