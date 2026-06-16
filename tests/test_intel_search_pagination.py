"""Regression guard for pagination metadata on /api/v1/intel/search.

Per ``.claude/rules/security.md``, list endpoints must return ``total``,
``limit``, ``offset``, and ``has_more`` in their response envelope so
clients can paginate without re-computing. This test pins those fields
for ``intel_search`` (the broadest user-facing query surface on the paid
Intelligence product router).

The whole test is fully offline — there is NO live DB. We monkeypatch
``api.routers.intel.get_db_engine`` with a fake recording engine whose
``fetchone()`` returns a fixed COUNT value (so we can assert the
aggregated ``total`` is correct) and whose ``fetchall()`` returns an
empty list (so the response is still well-formed).
"""
from __future__ import annotations

import sys
from types import ModuleType

import pytest


# ---------------------------------------------------------------------------
# Import-time stubs (mirrors tests/test_intel_product_sql_injection.py).
# ---------------------------------------------------------------------------
try:
    import api.auth  # noqa: F401
except Exception:  # pragma: no cover - env-dependent
    _auth_stub = ModuleType("api.auth")
    _auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.auth"] = _auth_stub

try:
    import api.dependencies  # noqa: F401
except Exception:  # pragma: no cover - env-dependent
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub


from api.routers import intel as intel_router  # noqa: E402


COUNT_PER_CATEGORY = 50


class _CountThenEmptyResult:
    """SELECT COUNT(*) → returns (50,) from fetchone().
    SELECT data    → returns [] from fetchall().
    The handler runs both kinds of queries; this fake serves both."""

    def __init__(self, count_value: int) -> None:
        self._count_value = count_value

    def fetchone(self):
        return (self._count_value,)

    def fetchall(self):
        return []


class _RecordingConnection:
    def __init__(self, calls: list[tuple[str, dict]], count_value: int) -> None:
        self._calls = calls
        self._count_value = count_value

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, clause, params=None):
        sql = getattr(clause, "text", clause)
        self._calls.append((sql, dict(params or {})))
        return _CountThenEmptyResult(self._count_value)


class _RecordingEngine:
    def __init__(self, count_value: int = COUNT_PER_CATEGORY) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._count_value = count_value

    def connect(self):
        return _RecordingConnection(self.calls, self._count_value)


@pytest.fixture
def recording_engine(monkeypatch) -> _RecordingEngine:
    eng = _RecordingEngine()
    monkeypatch.setattr(intel_router, "get_db_engine", lambda: eng)
    return eng


def test_search_response_includes_pagination_metadata(recording_engine):
    """Even when no rows match (empty result list), pagination meta must
    still surface so the client knows the page geometry."""
    resp = intel_router.intel_search(
        q="acme", type="all", limit=25, offset=0, _token="t"
    )
    meta = resp["meta"]
    # 3 categories × 50 each = 150 total
    assert meta["total"] == COUNT_PER_CATEGORY * 3
    assert meta["limit"] == 25
    assert meta["offset"] == 0
    assert meta["has_more"] is True


def test_search_has_more_false_when_offset_plus_limit_covers_total(
    recording_engine,
):
    """offset + limit >= total ⇒ has_more must be False."""
    # 3 categories × 50 = 150 total. With offset=100 and limit=100,
    # offset+limit=200 >= 150, so has_more is False.
    resp = intel_router.intel_search(
        q="acme", type="all", limit=100, offset=100, _token="t"
    )
    meta = resp["meta"]
    assert meta["total"] == COUNT_PER_CATEGORY * 3
    assert meta["has_more"] is False


def test_search_single_type_only_counts_that_category(recording_engine):
    """When type='ticker', only the ticker COUNT runs; total ≠ 3× count."""
    resp = intel_router.intel_search(
        q="AAPL", type="ticker", limit=10, offset=0, _token="t"
    )
    meta = resp["meta"]
    assert meta["total"] == COUNT_PER_CATEGORY  # only one branch counted
    assert meta["type"] == "ticker"
    assert meta["limit"] == 10
    assert meta["offset"] == 0


def test_search_empty_response_still_has_pagination_meta(monkeypatch):
    """When every category returns 0 rows AND 0 count, the empty envelope
    must still carry total/limit/offset/has_more for client uniformity."""
    eng = _RecordingEngine(count_value=0)
    monkeypatch.setattr(intel_router, "get_db_engine", lambda: eng)
    resp = intel_router.intel_search(
        q="zzznomatch", type="all", limit=25, offset=0, _token="t"
    )
    meta = resp["meta"]
    assert meta["total"] == 0
    assert meta["limit"] == 25
    assert meta["offset"] == 0
    assert meta["has_more"] is False
