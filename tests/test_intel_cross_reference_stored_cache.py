"""Stored-results fast path for ``/api/v1/intelligence/cross-reference``.

The endpoint used to run the full expensive ``run_all_checks`` on the
first request (and again whenever a cold worker's in-memory cache was
empty). ``run_all_checks`` already persists every check to
``cross_reference_checks`` with a timestamp, so we added:

  * ``intelligence.cross_reference.load_recent_report`` — reconstructs
    the most recent *fresh* batch from the DB without re-running scans.
  * endpoint wiring — try the stored result on the fast path, fall back
    to full compute (which persists) on a miss.

Two layers of tests:
  1. ``load_recent_report`` against in-memory SQLite (real
     reconstruction: hit / stale / empty / red-flag + summary shape /
     only-latest-batch).
  2. Endpoint hit / miss / fallback with ``run_all_checks`` and
     ``load_recent_report`` mocked, asserting an identical response
     shape and that compute is skipped on a stored hit.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import create_engine, text

import intelligence.cross_reference as xref
from api.routers import intel_cross_reference as router
from intelligence.cross_reference import (
    CrossRefCheck,
    LieDetectorReport,
    load_recent_report,
)


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────


def _check(name="GDP vs Rail", category="gdp", assessment="consistent",
           zscore=0.5, conf=0.9, checked_at="2026-05-26T00:00:00+00:00"):
    return CrossRefCheck(
        name=name,
        category=category,
        official_source="BEA",
        official_value=100.0,
        physical_source="AAR Rail",
        physical_value=98.0,
        expected_relationship="positive_correlation",
        actual_divergence=zscore,
        assessment=assessment,
        implication="some implication",
        confidence=conf,
        checked_at=checked_at,
    )


@pytest.fixture(autouse=True)
def _clear_inmem_cache():
    """The endpoint also has an in-memory TTL dict; clear it so each
    test starts cold and actually exercises ``_compute``."""
    router._cache.clear()
    router._cache_locks.clear()
    yield
    router._cache.clear()
    router._cache_locks.clear()


@pytest.fixture
def sqlite_engine():
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        # Mirror the columns load_recent_report reads. checked_at stored
        # as ISO text; SQLite has no native TIMESTAMPTZ.
        conn.execute(text("""
            CREATE TABLE cross_reference_checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT, category TEXT,
                official_source TEXT, official_value REAL,
                physical_source TEXT, physical_value REAL,
                divergence_zscore REAL,
                assessment TEXT, implication TEXT, confidence REAL,
                checked_at TEXT NOT NULL
            )
        """))
    yield eng
    eng.dispose()


def _insert_batch(engine, checked_at_iso: str, checks):
    with engine.begin() as conn:
        for c in checks:
            conn.execute(
                text(
                    "INSERT INTO cross_reference_checks "
                    "(name, category, official_source, official_value, "
                    "physical_source, physical_value, divergence_zscore, "
                    "assessment, implication, confidence, checked_at) "
                    "VALUES (:n,:cat,:os,:ov,:ps,:pv,:z,:a,:i,:c,:ts)"
                ),
                {
                    "n": c.name, "cat": c.category, "os": c.official_source,
                    "ov": c.official_value, "ps": c.physical_source,
                    "pv": c.physical_value, "z": c.actual_divergence,
                    "a": c.assessment, "i": c.implication, "c": c.confidence,
                    "ts": checked_at_iso,
                },
            )


# ─────────────────────────────────────────────────────────────────
# 1. load_recent_report — real reconstruction against SQLite
# ─────────────────────────────────────────────────────────────────


def test_load_recent_report_empty_table_returns_none(sqlite_engine):
    # ensure_tables would CREATE on a real PG; here the table already
    # exists and is empty. Patch ensure_tables to a no-op so the SQLite
    # schema (which we control) is used as-is.
    sqlite_engine.dispose  # noqa: B018 - keep ref
    import intelligence.cross_reference as x
    orig = x.ensure_tables
    x.ensure_tables = lambda engine: None
    try:
        assert load_recent_report(sqlite_engine, max_age_seconds=600) is None
    finally:
        x.ensure_tables = orig


def test_load_recent_report_fresh_batch_hits(sqlite_engine, monkeypatch):
    monkeypatch.setattr(xref, "ensure_tables", lambda engine: None)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    ts = now.isoformat()
    checks = [
        _check(name="A", category="gdp", assessment="consistent", checked_at=ts),
        _check(name="B", category="trade", assessment="major_divergence",
               zscore=2.6, checked_at=ts),
        _check(name="C", category="trade", assessment="contradiction",
               zscore=3.4, checked_at=ts),
    ]
    _insert_batch(sqlite_engine, ts, checks)

    report = load_recent_report(sqlite_engine, max_age_seconds=600)
    assert report is not None
    assert len(report.checks) == 3
    # Red flags = major_divergence + contradiction.
    assert len(report.red_flags) == 2
    assert {c.name for c in report.red_flags} == {"B", "C"}
    # Stored result has no narrative (served lazily).
    assert report.narrative == ""
    # Summary shape matches run_all_checks.
    assert report.summary["total_checks"] == 3
    assert report.summary["red_flag_count"] == 2
    assert report.summary["consistent_count"] == 1
    assert report.summary["categories"]["trade"]["major"] == 1
    assert report.summary["categories"]["trade"]["contradiction"] == 1


def test_load_recent_report_stale_batch_returns_none(sqlite_engine, monkeypatch):
    monkeypatch.setattr(xref, "ensure_tables", lambda engine: None)
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).replace(microsecond=0)
    ts = old.isoformat()
    _insert_batch(sqlite_engine, ts, [_check(checked_at=ts)])
    # 2h old, budget 600s → stale → None (caller recomputes).
    assert load_recent_report(sqlite_engine, max_age_seconds=600) is None


def test_load_recent_report_loads_only_latest_batch(sqlite_engine, monkeypatch):
    monkeypatch.setattr(xref, "ensure_tables", lambda engine: None)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    newer = now.isoformat()
    older = (now - timedelta(minutes=5)).isoformat()
    _insert_batch(sqlite_engine, older, [_check(name="OLD", checked_at=older),
                                         _check(name="OLD2", checked_at=older)])
    _insert_batch(sqlite_engine, newer, [_check(name="NEW", checked_at=newer)])

    report = load_recent_report(sqlite_engine, max_age_seconds=600)
    assert report is not None
    # Only the freshest batch is reconstructed, not the union.
    assert [c.name for c in report.checks] == ["NEW"]


def test_load_recent_report_zero_budget_returns_none(sqlite_engine):
    assert load_recent_report(sqlite_engine, max_age_seconds=0) is None


# ─────────────────────────────────────────────────────────────────
# 2. Endpoint wiring — hit / miss / fallback (mocked compute + store)
# ─────────────────────────────────────────────────────────────────


def _report(checks, narrative=""):
    red = [c for c in checks if c.assessment in ("major_divergence", "contradiction")]
    return LieDetectorReport(
        checks=checks, red_flags=red, narrative=narrative,
        generated_at="2026-05-26T00:00:00+00:00",
        summary=xref._summarize_checks(checks),
    )


def test_endpoint_serves_stored_result_without_compute(monkeypatch):
    """fast=True + fresh stored batch → serve it, NEVER run_all_checks."""
    stored = _report([_check(name="STORED")])
    run_calls = {"n": 0}

    def _run(engine, skip_narrative=False):
        run_calls["n"] += 1
        return _report([_check(name="FRESH_COMPUTE")])

    monkeypatch.setattr(router, "get_db_engine", lambda: object())
    monkeypatch.setattr(xref, "load_recent_report", lambda engine, max_age_seconds: stored)
    monkeypatch.setattr(xref, "run_all_checks", _run)

    out = asyncio.run(router.get_cross_reference(fast=True, _token="t"))

    assert run_calls["n"] == 0, "stored hit must not trigger full compute"
    assert out["checks"][0]["name"] == "STORED"
    assert out["narrative_pending"] is True
    # Identical response shape.
    assert set(out.keys()) == {
        "checks", "red_flags", "narrative", "summary",
        "generated_at", "narrative_pending",
    }


def test_endpoint_falls_back_to_compute_on_store_miss(monkeypatch):
    """fast=True + no stored result (None) → run_all_checks fallback."""
    run_calls = {"n": 0}

    def _run(engine, skip_narrative=False):
        run_calls["n"] += 1
        assert skip_narrative is True  # fast path skips narrative
        return _report([_check(name="COMPUTED")])

    monkeypatch.setattr(router, "get_db_engine", lambda: object())
    monkeypatch.setattr(xref, "load_recent_report", lambda engine, max_age_seconds: None)
    monkeypatch.setattr(xref, "run_all_checks", _run)

    out = asyncio.run(router.get_cross_reference(fast=True, _token="t"))

    assert run_calls["n"] == 1, "store miss must fall back to full compute"
    assert out["checks"][0]["name"] == "COMPUTED"
    assert out["narrative_pending"] is True


def test_endpoint_fast_false_always_computes_and_skips_store(monkeypatch):
    """fast=False (narrative wanted) must NOT use the narrative-less
    stored result; always recompute with the LLM narrative."""
    store_calls = {"n": 0}
    run_calls = {"n": 0}

    def _load(engine, max_age_seconds):
        store_calls["n"] += 1
        return _report([_check(name="STORED")])

    def _run(engine, skip_narrative=False):
        run_calls["n"] += 1
        assert skip_narrative is False
        return _report([_check(name="COMPUTED")], narrative="the prose")

    monkeypatch.setattr(router, "get_db_engine", lambda: object())
    monkeypatch.setattr(xref, "load_recent_report", _load)
    monkeypatch.setattr(xref, "run_all_checks", _run)

    out = asyncio.run(router.get_cross_reference(fast=False, _token="t"))

    assert store_calls["n"] == 0, "fast=False must not consult the store"
    assert run_calls["n"] == 1
    assert out["checks"][0]["name"] == "COMPUTED"
    assert out["narrative"] == "the prose"
    assert out["narrative_pending"] is False


def test_endpoint_inmem_cache_short_circuits_second_call(monkeypatch):
    """Second identical call within TTL is served from the in-memory
    cache — neither the store nor compute is touched again."""
    store_calls = {"n": 0}

    def _load(engine, max_age_seconds):
        store_calls["n"] += 1
        return _report([_check(name="STORED")])

    monkeypatch.setattr(router, "get_db_engine", lambda: object())
    monkeypatch.setattr(xref, "load_recent_report", _load)
    monkeypatch.setattr(xref, "run_all_checks",
                        lambda engine, skip_narrative=False: _report([]))

    first = asyncio.run(router.get_cross_reference(fast=True, _token="t"))
    second = asyncio.run(router.get_cross_reference(fast=True, _token="t"))

    assert store_calls["n"] == 1, "second call should hit in-memory cache"
    assert first == second


def test_endpoint_swallows_errors_and_returns_safe_shape(monkeypatch):
    """If the store and compute both blow up, the endpoint still returns
    the documented safe empty shape (never 500s the lever page)."""
    def _boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(router, "get_db_engine", lambda: object())
    monkeypatch.setattr(xref, "load_recent_report", _boom)
    monkeypatch.setattr(xref, "run_all_checks", _boom)

    out = asyncio.run(router.get_cross_reference(fast=True, _token="t"))
    assert out["checks"] == []
    assert out["red_flags"] == []
    assert "error" in out
