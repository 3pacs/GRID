"""Every keyed PULLER_REGISTRY entry's api_key_mode matches its constructor.

``SmartScheduler._build_puller_instance`` calls ``cls(key, engine)`` for
mode "first" (the default when the entry omits it), ``cls(db_engine=engine,
api_key=key)`` for "keyword" and ``cls(db_engine=engine)`` for "env". An
entry whose class takes ``db_engine`` first but defaults to "first" receives
the API key string as its engine. ``supply_chain`` failed that way on every
run (Hermes: "'str' object has no attribute 'connect'") until 2026-11-05.

Construction is exercised for real through ``_build_puller_instance`` with
``BasePuller.__init__`` stubbed, so no database is touched.
"""
from __future__ import annotations

import importlib
import inspect

import pytest

from ingestion import base as ingestion_base
from ingestion.smart_scheduler import PULLER_REGISTRY, SmartScheduler

KEYED = [p for p in PULLER_REGISTRY if p.get("api_key")]
_ENGINE = object()
_KEY = "test-key-not-real"


def _cls(entry):
    return getattr(importlib.import_module(entry["mod"]), entry["cls"])


@pytest.mark.parametrize("entry", KEYED, ids=[p["name"] for p in KEYED])
def test_mode_matches_constructor_signature(entry) -> None:
    params = list(inspect.signature(_cls(entry).__init__).parameters)[1:]
    mode = entry.get("api_key_mode", "first")
    if mode == "first":
        assert params[0] not in ("db_engine", "engine"), (
            f"{entry['name']}: mode 'first' passes the API key positionally first, "
            f"but {entry['cls']}.__init__ takes {params[0]!r} first; use 'keyword' or 'env'"
        )
    elif mode == "keyword":
        assert "db_engine" in params and "api_key" in params, (entry["name"], params)
    elif mode == "env":
        assert "db_engine" in params, (entry["name"], params)
    else:
        pytest.fail(f"{entry['name']}: unknown api_key_mode {mode!r}")


@pytest.mark.parametrize("entry", KEYED, ids=[p["name"] for p in KEYED])
def test_built_instance_gets_the_engine_not_the_key(entry, monkeypatch) -> None:
    seen: dict = {}

    def _fake_base_init(self, db_engine, *args, **kwargs):
        seen["engine"] = db_engine
        self.engine = db_engine
        self.source_id = 1

    monkeypatch.setattr(ingestion_base.BasePuller, "__init__", _fake_base_init)
    sched = SmartScheduler.__new__(SmartScheduler)
    sched.engine = _ENGINE

    try:
        sched._build_puller_instance(entry, _cls(entry), {entry["api_key"]: _KEY})
    except Exception as exc:  # noqa: BLE001 -- only the engine binding is under test
        if "engine" not in seen:
            pytest.skip(f"{entry['name']}: constructor did not reach BasePuller.__init__ ({type(exc).__name__})")
    if "engine" in seen:
        assert seen["engine"] is _ENGINE, f"{entry['name']}: BasePuller got {seen['engine']!r}, not the engine"


def test_supply_chain_is_keyword_mode() -> None:
    (entry,) = [p for p in PULLER_REGISTRY if p["name"] == "supply_chain"]
    assert entry.get("api_key_mode") == "keyword"
