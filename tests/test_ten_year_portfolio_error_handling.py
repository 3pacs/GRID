"""Regression guards for error-handling hygiene on ten_year_portfolio router.

Two HIGH findings from the 2026-05-29 code review of PR #276:

1. ``/weekly`` (GET) caught broad ``Exception`` and returned ``str(exc)``
   verbatim in the response body, leaking internal DB / SQL / DSN
   fragments to the client. Violates ``.claude/rules/common/security.md``
   ("Error messages don't leak sensitive data"). The handler now returns
   a generic ``"Ten-year portfolio query failed."`` message; full detail
   is still logged server-side via ``log.warning``.

2. ``/export.xlsx`` (GET) had no try/except wrapping, while the sibling
   ``/workbook/export.xlsx`` (POST) at line ~380 already did. A failure
   in ``_load_price_history`` (statement timeout, DSN issue, etc.) would
   surface as a raw 500 with whatever stacktrace FastAPI's default
   exception handler chose to expose. Now wrapped to raise
   ``HTTPException(500, "Workbook export failed.")``.

The tests run fully offline — no live DB, no auth — by stubbing
``api.auth`` and ``api.dependencies`` at import time and monkeypatching
``_load_price_history`` on the router module to raise a sentinel error
that mimics the kind of message a real DB failure would emit.
"""
from __future__ import annotations

import asyncio
import sys
from types import ModuleType

import pytest


# ---------------------------------------------------------------------------
# Import-time stubs (mirrors tests/test_intel_search_pagination.py).
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


from fastapi import HTTPException  # noqa: E402

from api.routers import ten_year_portfolio as typ_router  # noqa: E402


# A message a real DB failure might surface, with bits that absolutely
# must NOT be echoed to the client: a DSN-like fragment and an SQL
# snippet. If the response body contains either substring, the leak
# has regressed.
SENTINEL_ERROR_MSG = (
    "psycopg2.OperationalError: connection to postgresql://grid:hunter2"
    "@db-internal.local:5432/griddb failed: timeout on "
    "SELECT name, obs_date FROM resolved_series"
)


def _raise_sentinel(*_args, **_kwargs):
    raise RuntimeError(SENTINEL_ERROR_MSG)


def _call(handler, **kwargs):
    """Invoke an async router handler synchronously."""
    return asyncio.get_event_loop().run_until_complete(
        handler(**kwargs)
    ) if not asyncio.iscoroutine(handler) else asyncio.get_event_loop().run_until_complete(handler)


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# /weekly — must not leak raw exception text to the response body.
# ---------------------------------------------------------------------------
def test_weekly_returns_generic_error_on_failure(monkeypatch):
    monkeypatch.setattr(typ_router, "_load_price_history", _raise_sentinel)

    resp = _run(typ_router.weekly_ten_year_portfolio(
        capital=1_000_000.0,
        years=10,
        profile=None,
        engine=object(),  # unused — _load_price_history raises before touching it
    ))

    assert resp["status"] == "error"
    assert resp["error"] == "Ten-year portfolio query failed."
    # Critical: the raw exception message (which carries DSN + SQL) must
    # NOT appear anywhere in the response.
    assert "psycopg2" not in resp["error"]
    assert "postgresql://" not in resp["error"]
    assert "SELECT" not in resp["error"]
    assert "griddb" not in resp["error"]


def test_weekly_still_returns_ok_envelope_on_success(monkeypatch):
    """Sanity: the error-message rewording didn't break the happy path."""

    def _fake_history(_engine, *, years):
        # Empty history → recommendation builder returns ranked_candidates=0,
        # which the handler converts into the well-known empty envelope.
        return {}

    monkeypatch.setattr(typ_router, "_load_price_history", _fake_history)

    resp = _run(typ_router.weekly_ten_year_portfolio(
        capital=1_000_000.0,
        years=10,
        profile=None,
        engine=object(),
    ))

    # Either the empty branch fires or the ok branch fires — but it must
    # not be the error branch with a generic message.
    assert resp["status"] in ("ok", "empty")
    if resp["status"] == "empty":
        assert "No eligible" in resp["message"]


# ---------------------------------------------------------------------------
# /export.xlsx — must wrap exceptions in HTTPException(500, generic).
# ---------------------------------------------------------------------------
def test_export_xlsx_raises_http_500_on_failure(monkeypatch):
    monkeypatch.setattr(typ_router, "_load_price_history", _raise_sentinel)

    with pytest.raises(HTTPException) as exc_info:
        _run(typ_router.export_current_model_workbook(
            capital=1_000_000.0,
            years=10,
            engine=object(),
        ))

    assert exc_info.value.status_code == 500
    assert exc_info.value.detail == "Workbook export failed."
    # Detail must NOT echo the raw exception fragments.
    assert "psycopg2" not in exc_info.value.detail
    assert "postgresql://" not in exc_info.value.detail
    assert "SELECT" not in exc_info.value.detail


def test_export_xlsx_propagates_existing_http_exceptions(monkeypatch):
    """A pre-existing HTTPException raised inside the handler (e.g. 4xx
    from a downstream validation step) must pass through unchanged — not
    be re-wrapped as a 500."""

    def _raise_404(*_args, **_kwargs):
        raise HTTPException(status_code=404, detail="Specific upstream error.")

    monkeypatch.setattr(typ_router, "_load_price_history", _raise_404)

    with pytest.raises(HTTPException) as exc_info:
        _run(typ_router.export_current_model_workbook(
            capital=1_000_000.0,
            years=10,
            engine=object(),
        ))

    # The original 404 (and its detail) survived — proves the
    # ``except HTTPException: raise`` clause is in place.
    assert exc_info.value.status_code == 404
    assert exc_info.value.detail == "Specific upstream error."
