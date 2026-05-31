"""Offline tests for the community-list cache + fallback layer.

The /api/v1/intelligence/communities endpoint previously called the live
aggregation (store.graph.get_community_list) on every request — a GROUP BY
over ~2.7M actor_analytics rows plus a 44K-query N+1 for top members. The
endpoint now reads the materialized ``community_summary`` table with two
layers of protection (an in-process TTL cache and a graceful live fallback).

These tests exercise the cache/fallback decision logic with a mocked engine —
no live PostgreSQL required.
"""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

try:
    import api.auth  # noqa: F401
except Exception:
    _auth_stub = ModuleType("api.auth")
    _auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.auth"] = _auth_stub
try:
    import api.dependencies  # noqa: F401
except Exception:
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub

from api.routers import intelligence_actors as ia  # noqa: E402


# ── Fake engine ────────────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, *, has_table, summary_rows):
        self._has_table = has_table
        self._summary_rows = summary_rows
        self.summary_queries = 0

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt, params=None):
        sql = str(getattr(stmt, "text", stmt))
        if "to_regclass" in sql:
            try:
                name = (stmt.compile().params or {}).get("n")
            except Exception:
                name = None
            exists = self._has_table and name == "community_summary"
            return _FakeResult([(name if exists else None,)])
        if "FROM community_summary" in sql:
            self.summary_queries += 1
            return _FakeResult(self._summary_rows)
        return _FakeResult([])


class _FakeEngine:
    def __init__(self, conn):
        self.conn = conn
        self.connect_calls = 0

    def connect(self):
        self.connect_calls += 1
        return self.conn


_SUMMARY_ROWS = [
    # (community_id, member_count, max_pagerank, top_member, top_category)
    (7, 1200, 0.0123, "BlackRock", "fund"),
    (3, 800, 0.0099, "JPMorgan", "corporation"),
]


@pytest.fixture(autouse=True)
def _clear_cache():
    ia._community_list_cache.clear()
    yield
    ia._community_list_cache.clear()


# ── Cache hit (table populated) ──────────────────────────────────────────────


def test_reads_from_summary_table_when_populated():
    engine = _FakeEngine(_FakeConn(has_table=True, summary_rows=_SUMMARY_ROWS))
    communities, source = ia._load_community_list(engine)

    assert source == "cache"
    assert len(communities) == 2
    # largest-first ordering preserved, shape mapped correctly
    assert communities[0]["community_id"] == 7
    assert communities[0]["member_count"] == 1200
    assert communities[0]["top_member"] == "BlackRock"
    assert communities[0]["top_category"] == "fund"
    assert isinstance(communities[0]["max_pagerank"], float)


def test_second_call_is_served_from_ttl_cache_without_requerying():
    conn = _FakeConn(has_table=True, summary_rows=_SUMMARY_ROWS)
    engine = _FakeEngine(conn)

    first, first_src = ia._load_community_list(engine)
    assert first_src == "cache"
    assert conn.summary_queries == 1

    second, second_src = ia._load_community_list(engine)
    assert second_src == "ttl"
    # No additional DB query — TTL cache served it.
    assert conn.summary_queries == 1
    assert second == first


# ── Fallback to live aggregation ─────────────────────────────────────────────


def test_falls_back_to_live_when_table_missing(monkeypatch):
    engine = _FakeEngine(_FakeConn(has_table=False, summary_rows=[]))

    live_rows = [{"community_id": 1, "member_count": 5, "max_pagerank": 0.1,
                  "top_member": "X", "top_category": "y"}]
    called = {"n": 0}

    def _fake_live(engine=None):
        called["n"] += 1
        return live_rows

    # store.graph is imported lazily inside the fallback branch.
    fake_store_graph = ModuleType("store.graph")
    fake_store_graph.get_community_list = _fake_live  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "store.graph", fake_store_graph)

    communities, source = ia._load_community_list(engine)
    assert source == "live"
    assert communities == live_rows
    assert called["n"] == 1


def test_falls_back_to_live_when_table_empty(monkeypatch):
    # Table exists but has no rows yet (not materialized) → live fallback.
    engine = _FakeEngine(_FakeConn(has_table=True, summary_rows=[]))

    fake_store_graph = ModuleType("store.graph")
    fake_store_graph.get_community_list = lambda engine=None: [  # type: ignore[attr-defined]
        {"community_id": 9, "member_count": 1, "max_pagerank": 0.0,
         "top_member": None, "top_category": None}
    ]
    monkeypatch.setitem(sys.modules, "store.graph", fake_store_graph)

    communities, source = ia._load_community_list(engine)
    assert source == "live"
    assert communities[0]["community_id"] == 9


def test_live_result_is_cached_for_subsequent_calls(monkeypatch):
    engine = _FakeEngine(_FakeConn(has_table=False, summary_rows=[]))
    calls = {"n": 0}

    def _fake_live(engine=None):
        calls["n"] += 1
        return [{"community_id": 1, "member_count": 5}]

    fake_store_graph = ModuleType("store.graph")
    fake_store_graph.get_community_list = _fake_live  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "store.graph", fake_store_graph)

    _, src1 = ia._load_community_list(engine)
    _, src2 = ia._load_community_list(engine)
    assert src1 == "live"
    assert src2 == "ttl"            # cached after first live load
    assert calls["n"] == 1          # live aggregation ran only once


# ── _read_community_summary unit behavior ────────────────────────────────────


def test_read_summary_returns_none_when_table_absent():
    engine = _FakeEngine(_FakeConn(has_table=False, summary_rows=[]))
    assert ia._read_community_summary(engine) is None


def test_read_summary_returns_none_on_query_error():
    class _BoomConn(_FakeConn):
        def execute(self, stmt, params=None):
            sql = str(getattr(stmt, "text", stmt))
            if "to_regclass" in sql:
                return _FakeResult([("community_summary",)])
            raise RuntimeError("boom")

    engine = _FakeEngine(_BoomConn(has_table=True, summary_rows=_SUMMARY_ROWS))
    # Treated as a miss, not raised.
    assert ia._read_community_summary(engine) is None


def test_cache_constants_wired():
    from utils.ttl_cache import TTLCache
    assert isinstance(ia._community_list_cache, TTLCache)
    assert ia._community_list_cache._ttl == ia._COMMUNITY_LIST_TTL


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
