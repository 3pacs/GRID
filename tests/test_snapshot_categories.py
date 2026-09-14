"""Tests for data-derived snapshot categories.

Context: ``GET /api/v1/snapshots/latest/{category}`` gated on a hardcoded
eight-value tuple (``AnalyticalSnapshotStore.CATEGORIES``) and returned HTTP
400 for everything else. Most of what is written to ``analytical_snapshots``
was outside that tuple — ``sleuth_investigation``, ``alpha101``,
``strategy151``, ``research_sweep``, ``sector_flows``, ``human_llm_insight``,
plus ``congressional_trade`` / ``opensanctions`` / ``crypto_price`` from
writers that bypass ``save_snapshot`` entirely — so a dashboard could read
only 8 of them. Two writers build the category at runtime
(f"mcp_research_{task_type}", f"llm_task_{task_type}"), so no maintained
literal can ever be correct.

The fix reads the category set from the table and drops the membership gate.
These tests cover both halves plus the cache that keeps the ``GROUP BY`` off
the hot path.

Uses the MagicMock-engine pattern from tests/test_snapshots.py (store level)
and the minimal-app + ``dependency_overrides`` pattern from
tests/test_api_conviction.py (router level) — no live PostgreSQL.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_auth
from api.routers import snapshots as snapshots_router
from api.routers.snapshots import router as snapshots_api_router
from store import snapshots as snapshots_store
from store.snapshots import AnalyticalSnapshotStore
from utils.ttl_cache import TTLCache


# Categories that are genuinely written to the table but were absent from the
# old tuple. Every one of these 400'd before the fix.
CATEGORIES_OUTSIDE_THE_OLD_TUPLE = [
    "sleuth_investigation",     # intelligence/sleuth.py
    "alpha101",                 # features/alpha101.py
    "strategy151",              # trading/strategy151.py
    "research_sweep",           # analysis/research_agent.py
    "sector_flows",             # api/routers/flows.py
    "human_llm_insight",        # scripts/assimilator.py
    "congressional_trade",      # scripts/parse_datasets.py (direct INSERT)
    "opensanctions",            # scripts/parse_datasets.py (direct INSERT)
    "crypto_price",             # ingestion/openbb_pipeline.py (direct INSERT)
    "mcp_research_deep_dive",   # mcp_server.py — built at runtime
    "llm_task_thesis",          # orchestration/llm_taskqueue.py — runtime
]


# ── Fixtures / helpers ────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _clear_category_cache():
    """The category cache is process-wide; keep it from leaking across tests."""
    snapshots_store.clear_category_cache()
    yield
    snapshots_store.clear_category_cache()


def _sql_text(stmt) -> str:
    return str(getattr(stmt, "text", stmt))


def _make_engine(category_rows=None, latest_rows=None):
    """MagicMock engine that answers the category GROUP BY and `get_latest`.

    Returns (engine, conn) so callers can assert on what SQL ran.
    """
    category_rows = category_rows if category_rows is not None else []
    latest_rows = latest_rows if latest_rows is not None else []

    engine = MagicMock()
    conn = MagicMock()

    def execute(stmt, *args, **kwargs):
        sql = _sql_text(stmt)
        result = MagicMock()
        if "GROUP BY category" in sql:
            result.fetchall.return_value = category_rows
        elif "SELECT id, snapshot_date" in sql:
            result.fetchall.return_value = latest_rows
        else:
            result.fetchall.return_value = []
        result.fetchone.return_value = (1,)
        return result

    conn.execute.side_effect = execute
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _build_client(engine, monkeypatch) -> TestClient:
    """Minimal app with only the snapshots router mounted.

    ``get_db_engine`` is called directly inside the handlers (not via
    ``Depends``), so it is monkeypatched on the router module; ``require_auth``
    is a real dependency and is overridden.
    """
    monkeypatch.setattr(snapshots_router, "get_db_engine", lambda: engine)
    app = FastAPI()
    app.include_router(snapshots_api_router)
    app.dependency_overrides[require_auth] = lambda: {"sub": "test-user"}
    return TestClient(app)


# ── The bug: /latest/{category} 400'd almost everything ───────────────────


@pytest.mark.parametrize("category", CATEGORIES_OUTSIDE_THE_OLD_TUPLE)
def test_latest_does_not_reject_categories_outside_the_pipeline_tuple(
    category, monkeypatch,
):
    """RED before the fix: each of these returned 400 'Unknown category'."""
    engine, _ = _make_engine(
        latest_rows=[(7, date(2026, 9, 14), date(2026, 9, 14), None, {"k": 1}, None, None)],
    )
    client = _build_client(engine, monkeypatch)

    resp = client.get(f"/api/v1/snapshots/latest/{category}")

    assert resp.status_code == 200, resp.text
    assert resp.json()[0]["id"] == 7


def test_latest_returns_empty_list_for_a_category_with_no_rows(monkeypatch):
    """No rows is not an error — same contract as /history, which never gated."""
    engine, _ = _make_engine(latest_rows=[])
    client = _build_client(engine, monkeypatch)

    resp = client.get("/api/v1/snapshots/latest/category_that_has_no_rows_yet")

    assert resp.status_code == 200
    assert resp.json() == []


def test_latest_passes_category_as_a_bound_parameter(monkeypatch):
    """The category reaches SQL as a bound param, never interpolated."""
    engine, conn = _make_engine(latest_rows=[])
    client = _build_client(engine, monkeypatch)

    client.get("/api/v1/snapshots/latest/sector_flows")

    select_calls = [
        c for c in conn.execute.call_args_list
        if "SELECT id, snapshot_date" in _sql_text(c.args[0])
    ]
    assert len(select_calls) == 1
    stmt, params = select_calls[0].args[0], select_calls[0].args[1]
    assert params["cat"] == "sector_flows"
    assert "sector_flows" not in _sql_text(stmt)


def test_latest_still_validates_parameter_shape_at_the_boundary(monkeypatch):
    """Membership is not validated, but the value's shape still is."""
    engine, _ = _make_engine()
    client = _build_client(engine, monkeypatch)

    resp = client.get("/api/v1/snapshots/latest/" + "x" * 500)

    assert resp.status_code == 422


def test_latest_still_clamps_n(monkeypatch):
    engine, _ = _make_engine()
    client = _build_client(engine, monkeypatch)

    assert client.get("/api/v1/snapshots/latest/clustering?n=51").status_code == 422
    assert client.get("/api/v1/snapshots/latest/clustering?n=0").status_code == 422


# ── /categories reports what is in the table ──────────────────────────────


def test_categories_endpoint_reports_rows_not_the_hardcoded_tuple(monkeypatch):
    """RED before the fix: /categories returned the 8-value tuple as list[str]."""
    engine, _ = _make_engine(
        category_rows=[
            ("congressional_trade", 5000, date(2026, 9, 12)),
            ("opensanctions", 12282, date(2026, 9, 13)),
            ("sector_flows", 50, date(2026, 9, 14)),
        ],
    )
    client = _build_client(engine, monkeypatch)

    resp = client.get("/api/v1/snapshots/categories")

    assert resp.status_code == 200
    body = resp.json()
    names = [e["category"] for e in body["entries"]]
    assert names == ["congressional_trade", "opensanctions", "sector_flows"]
    # None of these are in the canonical pipeline tuple — the point of the fix.
    assert not set(names) & set(AnalyticalSnapshotStore.PIPELINE_CATEGORIES)
    assert body["entries"][1]["snapshot_count"] == 12282
    assert body["entries"][2]["latest_snapshot_date"] == "2026-09-14"


def test_categories_endpoint_uses_the_list_endpoint_envelope(monkeypatch):
    """total/limit/offset/has_more, per .claude/rules/security.md."""
    engine, _ = _make_engine(
        category_rows=[(f"cat_{i}", i, date(2026, 9, 14)) for i in range(5)],
    )
    client = _build_client(engine, monkeypatch)

    body = client.get("/api/v1/snapshots/categories?limit=2&offset=1").json()

    assert body["total"] == 5
    assert body["limit"] == 2
    assert body["offset"] == 1
    assert body["has_more"] is True
    assert [e["category"] for e in body["entries"]] == ["cat_1", "cat_2"]

    last = client.get("/api/v1/snapshots/categories?limit=2&offset=4").json()
    assert last["has_more"] is False
    assert [e["category"] for e in last["entries"]] == ["cat_4"]


def test_categories_endpoint_is_empty_when_the_table_is_empty(monkeypatch):
    engine, _ = _make_engine(category_rows=[])
    client = _build_client(engine, monkeypatch)

    body = client.get("/api/v1/snapshots/categories").json()

    assert body == {
        "entries": [], "total": 0, "limit": 100, "offset": 0, "has_more": False,
    }


# ── store.list_categories() ───────────────────────────────────────────────


def test_list_categories_queries_the_table_with_no_interpolation():
    engine, conn = _make_engine(
        category_rows=[("alpha101", 3, date(2026, 9, 14))],
    )
    store = AnalyticalSnapshotStore(db_engine=engine)

    result = store.list_categories()

    assert result == [
        {
            "category": "alpha101",
            "snapshot_count": 3,
            "latest_snapshot_date": "2026-09-14",
        },
    ]
    group_by = [
        c for c in conn.execute.call_args_list
        if "GROUP BY category" in _sql_text(c.args[0])
    ]
    assert len(group_by) == 1
    sql = _sql_text(group_by[0].args[0])
    assert "FROM analytical_snapshots" in sql
    # No user input reaches this statement at all, and no format placeholders
    # were left behind in the string.
    assert "{" not in sql and "%" not in sql


def test_list_categories_tolerates_a_null_latest_date():
    engine, _ = _make_engine(category_rows=[("orphan", 1, None)])
    store = AnalyticalSnapshotStore(db_engine=engine)

    assert store.list_categories()[0]["latest_snapshot_date"] is None


def test_list_categories_returns_empty_and_warns_when_table_is_unreachable(monkeypatch):
    """Missing table / DB blip is operational, so it warns rather than errors
    (CLAUDE.md: log.error is for unhandled application bugs, and errors.jsonl
    is the canonical health signal). Asserted on the loguru logger directly —
    loguru does not propagate to pytest's caplog."""
    engine, conn = _make_engine()
    store = AnalyticalSnapshotStore(db_engine=engine)

    def boom(stmt, *args, **kwargs):
        if "GROUP BY category" in _sql_text(stmt):
            raise RuntimeError('relation "analytical_snapshots" does not exist')
        return MagicMock()

    conn.execute.side_effect = boom
    fake_log = MagicMock()
    monkeypatch.setattr(snapshots_store, "log", fake_log)

    assert store.list_categories() == []

    assert fake_log.warning.call_count == 1
    fake_log.error.assert_not_called()


# ── Caching ───────────────────────────────────────────────────────────────


def test_category_cache_is_the_shared_ttlcache_utility():
    assert isinstance(snapshots_store._category_cache, TTLCache)


def test_list_categories_is_cached_across_store_instances():
    """The router builds a new store per request, so the cache has to outlive
    the instance or it never hits."""
    engine, conn = _make_engine(category_rows=[("clustering", 1, date(2026, 9, 14))])

    AnalyticalSnapshotStore(db_engine=engine).list_categories()
    AnalyticalSnapshotStore(db_engine=engine).list_categories()
    AnalyticalSnapshotStore(db_engine=engine).list_categories()

    group_by = [
        c for c in conn.execute.call_args_list
        if "GROUP BY category" in _sql_text(c.args[0])
    ]
    assert len(group_by) == 1


def test_use_cache_false_always_requeries():
    engine, conn = _make_engine(category_rows=[("clustering", 1, date(2026, 9, 14))])
    store = AnalyticalSnapshotStore(db_engine=engine)

    store.list_categories()
    store.list_categories(use_cache=False)

    group_by = [
        c for c in conn.execute.call_args_list
        if "GROUP BY category" in _sql_text(c.args[0])
    ]
    assert len(group_by) == 2


def test_clear_category_cache_forces_a_requery():
    engine, conn = _make_engine(category_rows=[("clustering", 1, date(2026, 9, 14))])
    store = AnalyticalSnapshotStore(db_engine=engine)

    store.list_categories()
    snapshots_store.clear_category_cache()
    store.list_categories()

    group_by = [
        c for c in conn.execute.call_args_list
        if "GROUP BY category" in _sql_text(c.args[0])
    ]
    assert len(group_by) == 2


def test_saving_a_brand_new_category_invalidates_the_cache():
    """A first-ever write must be visible to /categories immediately in-process,
    not after the TTL — that lag is how writers and readers drift apart."""
    engine, conn = _make_engine(category_rows=[("clustering", 1, date(2026, 9, 14))])
    store = AnalyticalSnapshotStore(db_engine=engine)
    store.list_categories()

    store.save_snapshot(category="brand_new_category", payload={})

    store.list_categories()
    group_by = [
        c for c in conn.execute.call_args_list
        if "GROUP BY category" in _sql_text(c.args[0])
    ]
    assert len(group_by) == 2


def test_saving_a_known_category_leaves_the_cache_intact():
    """The steady state is writes of categories that already exist; those must
    not invalidate, or the cache never serves anything."""
    engine, conn = _make_engine(category_rows=[("clustering", 1, date(2026, 9, 14))])
    store = AnalyticalSnapshotStore(db_engine=engine)
    store.list_categories()

    store.save_snapshot(category="clustering", payload={})

    store.list_categories()
    group_by = [
        c for c in conn.execute.call_args_list
        if "GROUP BY category" in _sql_text(c.args[0])
    ]
    assert len(group_by) == 1


def test_save_snapshot_does_not_populate_the_cache_on_a_cold_process():
    """Invalidation must not turn into an accidental read: a cold cache stays
    cold rather than triggering a GROUP BY on the write path."""
    engine, conn = _make_engine(category_rows=[])
    store = AnalyticalSnapshotStore(db_engine=engine)

    store.save_snapshot(category="anything", payload={})

    assert not [
        c for c in conn.execute.call_args_list
        if "GROUP BY category" in _sql_text(c.args[0])
    ]


# ── The misleading name must not come back ────────────────────────────────


def test_pipeline_categories_is_not_named_categories():
    """``CATEGORIES`` read as "every category there is" and that is exactly how
    it got used as a validation gate. The name must stay gone."""
    assert not hasattr(AnalyticalSnapshotStore, "CATEGORIES")
    assert "pipeline_summary" in AnalyticalSnapshotStore.PIPELINE_CATEGORIES


def test_pipeline_categories_covers_what_extract_metrics_summarizes():
    """The tuple's remaining job is documenting the canonical pipeline set, so
    it must stay in step with the pipeline writer it documents."""
    store = AnalyticalSnapshotStore(db_engine=_make_engine()[0])
    pipeline_written = {
        "conflict_resolution", "feature_engineering", "orthogonality",
        "regime_detection", "options_scan", "feature_importance",
        "pipeline_summary",
    }
    assert pipeline_written <= set(store.PIPELINE_CATEGORIES)


def test_no_module_gates_on_a_hardcoded_category_list():
    """Regression guard: the router must not reintroduce a membership check."""
    import inspect

    source = inspect.getsource(snapshots_router)
    assert "not in store.CATEGORIES" not in source
    assert "Unknown category" not in source
