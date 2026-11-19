"""/flows/money-map (~18 s cold) and /intelligence/levers (~10 s cold) serve a
background-precomputed copy and fall back to the live build only before the
job's first result (the job runs in grid-intelligence, which restarts apart
from grid-api)."""

from __future__ import annotations

import asyncio
import sys
import types
from datetime import datetime, timezone

import pytest

from intelligence import api_precompute as ap

NOW = datetime(2026, 11, 7, 23, 0, tzinfo=timezone.utc)
META = {"computed_at": NOW.isoformat(), "stale": False}


def test_jobs_declare_both_keys() -> None:
    assert ap.JOB_BY_KEY["flows.money_map"].name == "money_map"
    assert ap.JOB_BY_KEY["intelligence.levers"].name == "levers"
    for name in ("money_map", "levers"):
        job = ap.JOBS_BY_NAME[name]
        assert job.cadence_s <= 15 * 60 < job.stale_after_s


def test_build_money_map_wraps_build_flow_map(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "analysis.money_flow", types.SimpleNamespace(
        build_flow_map=lambda engine: {"layers": [1], "engine": engine}))
    [out] = ap.build_money_map(ap.BuildContext(engine="E", now=NOW))
    assert (out.key, out.params, out.as_of) == ("flows.money_map", {}, NOW)
    assert out.payload == {"layers": [1], "engine": "E"}


def test_build_levers_truncates_cross_domain_actors(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "intelligence.global_levers", types.SimpleNamespace(
        get_lever_hierarchy=lambda _e: {"hierarchy": {"energy": {}}, "summary": {}},
        find_cross_domain_actors=lambda _e: [{"a": i} for i in range(30)]))
    [out] = ap.build_levers(ap.BuildContext(engine=object(), now=NOW))
    assert out.key == "intelligence.levers"
    assert out.payload["hierarchy"] == {"energy": {}}
    assert len(out.payload["cross_domain_actors"]) == 20


@pytest.fixture
def stored(monkeypatch):
    import api.precompute_serving as serving

    bodies: dict[str, dict] = {}
    monkeypatch.setattr(serving, "read_precomputed",
                        lambda key, params=None: (dict(bodies[key]) if key in bodies else None, META))
    return bodies


def test_money_map_serves_the_stored_copy_without_building(monkeypatch, stored) -> None:
    from api.routers import flows

    stored["flows.money_map"] = {"layers": ["stored"]}
    monkeypatch.setitem(sys.modules, "analysis.money_flow", types.SimpleNamespace(
        build_flow_map=lambda _e: pytest.fail("live build ran")))
    monkeypatch.setattr(flows, "_provenance", lambda tables: {"tables": list(tables)})
    body = flows.get_money_map(_token="t")
    assert body["layers"] == ["stored"]
    assert body["precompute"] == META
    assert "provenance" in body


def test_money_map_falls_back_to_live_build_before_first_result(monkeypatch, stored) -> None:
    from api.routers import flows

    monkeypatch.setitem(sys.modules, "analysis.money_flow", types.SimpleNamespace(
        build_flow_map=lambda _e: {"layers": ["live"]}))
    monkeypatch.setattr(flows, "get_db_engine", lambda: object())
    monkeypatch.setattr(flows, "_provenance", lambda tables: {})
    monkeypatch.setattr(flows, "_money_map_cache", flows.TTLCache(ttl=900, max_size=1))
    assert flows.get_money_map(_token="t")["layers"] == ["live"]


def test_levers_serves_the_stored_copy_without_building(monkeypatch, stored) -> None:
    from api.routers import intelligence_deepdive as dd

    stored["intelligence.levers"] = {"hierarchy": {"stored": 1}, "cross_domain_actors": []}
    monkeypatch.setitem(sys.modules, "intelligence.global_levers", types.SimpleNamespace(
        get_lever_hierarchy=lambda _e: pytest.fail("live build ran"),
        find_cross_domain_actors=lambda _e: []))
    monkeypatch.setattr(dd, "_lever_cache", dd.TTLCache(ttl=600, max_size=1))
    body = asyncio.run(dd.get_levers(_token="t"))
    assert body["hierarchy"] == {"stored": 1}
    assert body["precompute"] == META
