"""Regression tests for the /api/v1/intel/predictions/active pagination envelope.

Locks in the `has_more` field added to the success envelope per
`.claude/rules/security.md` ("List endpoints must return `total` ... plus
`limit`, `offset`, and `has_more`"). Mirrors the canonical pattern at
`api/routers/journal.py::get_all`.

Stubs api.auth and api.dependencies to avoid heavy transitive deps that may
not be installed in lightweight CI environments.
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


class _FakeResult:
    def __init__(self, total: int) -> None:
        self._total = total

    def fetchone(self):
        return (self._total,)

    def fetchall(self):
        return []


class _FakeConn:
    def __init__(self, total: int) -> None:
        self._total = total

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, *args, **kwargs):
        return _FakeResult(self._total)


class _FakeEngine:
    def __init__(self, total: int) -> None:
        self._total = total

    def connect(self):
        return _FakeConn(self._total)


@pytest.fixture
def active_predictions():
    from api.routers import intel

    def _call(*, total: int, limit: int, offset: int):
        intel.get_db_engine = lambda: _FakeEngine(total)  # type: ignore[attr-defined]
        return intel.intel_predictions_active(
            ticker=None,
            model=None,
            limit=limit,
            offset=offset,
            _token="test-token",
        )

    return _call


class TestActivePredictionsPaginationEnvelope:
    def test_meta_includes_all_pagination_keys(self, active_predictions):
        meta = active_predictions(total=120, limit=50, offset=0)["meta"]
        for key in ("total", "limit", "offset", "has_more"):
            assert key in meta, f"missing pagination key: {key}"

    def test_has_more_true_when_more_pages_remain(self, active_predictions):
        meta = active_predictions(total=120, limit=50, offset=0)["meta"]
        assert meta["total"] == 120
        assert meta["limit"] == 50
        assert meta["offset"] == 0
        assert meta["has_more"] is True

    def test_has_more_false_on_last_page(self, active_predictions):
        meta = active_predictions(total=120, limit=50, offset=100)["meta"]
        assert meta["has_more"] is False

    def test_has_more_false_at_exact_boundary(self, active_predictions):
        # offset + limit == total → no further pages
        meta = active_predictions(total=100, limit=50, offset=50)["meta"]
        assert meta["has_more"] is False
