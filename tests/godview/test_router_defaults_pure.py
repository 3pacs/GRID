"""Pure test (no database) for the include_inferred/as_of default-binding bug.

Root cause (2026-09-18, real-Postgres run, composition 783ff735):
``api/routers/godview_pillars.py``'s ``get_cftc_pillar``/``get_fed_liquidity_pillar``
used a bare ``param: bool = Query(default=False)`` signature. FastAPI's own
request handling substitutes the real value for that when invoked over ASGI,
but a DIRECT Python call to the route function -- exactly what
tests/godview/test_cftc_pillar_api_db.py does to avoid standing up a full
ASGI server -- leaves the parameter bound to the ``Query`` marker OBJECT,
which is truthy, so ``include_inferred`` silently evaluated as "admit
inferred rows" even at the documented "false" default. Fixed by switching
to ``Annotated[bool, Query(...)] = False``.

This file uses a fake ``read_cftc_pillar`` (never touches a real database)
to assert the fix at the unit level: calling the route function directly,
with ``include_inferred`` OMITTED, must pass a real Python ``False`` through
to the store-layer reader -- not the ``Query`` marker object.
"""

from __future__ import annotations

import os
from datetime import date

# api.routers.godview_pillars (imported inside the test bodies below, to
# match how tests/godview/test_cftc_pillar_api_db.py already does it) drags
# in api.dependencies -> db -> config.settings, whose Settings() raises at
# *import* time if DB_PASSWORD is unset -- a pre-existing environment gap,
# unrelated to this fix, that otherwise makes this file fail to even collect
# in a shell with no .env sourced (this repo's own DB-gated godview tests
# dodge it by skipping, via the godview_pg_engine fixture, BEFORE reaching
# that import). These tests never open a real connection (the engine is a
# bare stub), so a placeholder value here is safe and changes no real
# behaviour; os.environ.setdefault leaves a genuinely configured environment
# untouched.
os.environ.setdefault("DB_PASSWORD", "test-only-placeholder-unused")
os.environ.setdefault("ENVIRONMENT", "development")

from godview.cftc_pillar import PillarReadResult


def test_get_cftc_pillar_omitted_include_inferred_resolves_to_a_plain_false(monkeypatch):
    import api.routers.godview_pillars as router_module

    captured: dict[str, object] = {}

    def fake_read_cftc_pillar(conn, as_of, *, contracts, include_inferred):
        captured["include_inferred"] = include_inferred
        captured["type"] = type(include_inferred)
        return PillarReadResult(state="never_configured")

    monkeypatch.setattr(router_module, "get_db_engine", lambda: _FakeEngine())
    monkeypatch.setattr(router_module, "_table_exists", lambda conn, name: True)
    monkeypatch.setattr(router_module, "read_cftc_pillar", fake_read_cftc_pillar)

    # Deliberately do NOT pass include_inferred -- this is exactly how the
    # DB-gated API tests (and any other direct caller) invoke the route.
    router_module.get_cftc_pillar(as_of=date(2026, 1, 1), _token="test")

    assert captured["include_inferred"] is False
    assert captured["type"] is bool  # not fastapi.params.Query


def test_get_fed_liquidity_pillar_omitted_include_inferred_resolves_to_a_plain_false(monkeypatch):
    import api.routers.godview_pillars as router_module
    from godview.fed_liquidity_pillar import PillarReadResult as FedPillarReadResult

    captured: dict[str, object] = {}

    def fake_read_fed_liquidity_pillar(conn, as_of, *, include_inferred):
        captured["include_inferred"] = include_inferred
        captured["type"] = type(include_inferred)
        return FedPillarReadResult(state="never_configured")

    monkeypatch.setattr(router_module, "get_db_engine", lambda: _FakeEngine())
    monkeypatch.setattr(router_module, "_table_exists", lambda conn, name: True)
    monkeypatch.setattr(router_module, "read_fed_liquidity_pillar", fake_read_fed_liquidity_pillar)

    router_module.get_fed_liquidity_pillar(as_of=date(2026, 1, 1), _token="test")

    assert captured["include_inferred"] is False
    assert captured["type"] is bool


class _FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    def connect(self):
        return _FakeConn()
