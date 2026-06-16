"""Regression tests for `api.dependencies.clear_singletons`.

Pins the PUNCH-LIST-2026-05-13 line 46 bug: prior implementation disposed
the api-level `_db_engine` pointer but never reset `db._engine`, so the
next `get_db_engine()` returned a *disposed* engine. Tests verify that
clearing now cascades to the underlying `db._engine` singleton.
"""

from __future__ import annotations

import importlib
import sys
import types
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def fake_db_module(monkeypatch: pytest.MonkeyPatch) -> types.ModuleType:
    """Provide a minimal `db` module that imitates `get_engine` / `clear_engine`
    without needing a real SQLAlchemy engine or live database.

    We mutate the `_engine` module-level attribute in tests to simulate the
    real lazy-singleton behavior.
    """
    fake = types.ModuleType("db")
    fake._engine = None  # type: ignore[attr-defined]

    def _get_engine():
        if fake._engine is None:  # type: ignore[attr-defined]
            fake._engine = MagicMock(name="fake_engine")  # type: ignore[attr-defined]
        return fake._engine  # type: ignore[attr-defined]

    def _clear_engine():
        eng = fake._engine  # type: ignore[attr-defined]
        if eng is not None:
            eng.dispose()
        fake._engine = None  # type: ignore[attr-defined]

    fake.get_engine = _get_engine  # type: ignore[attr-defined]
    fake.clear_engine = _clear_engine  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "db", fake)

    # Force `api.dependencies` to be re-executed so it re-binds `import db`
    # and `from db import get_engine` against `fake`. Popping the
    # `sys.modules["api.dependencies"]` entry alone is NOT enough when the
    # full suite has already imported the real module: the parent `api`
    # package object retains a `dependencies` attribute pointing at the
    # stale module, so `from api import dependencies` would resolve to that
    # cached (real-db-bound) module instead of re-importing. We must drop
    # both the sys.modules entry AND the package attribute, then re-import.
    # All mutations go through monkeypatch so teardown restores the real
    # module and prevents this fixture from polluting later tests.
    api_pkg = importlib.import_module("api")
    if "api.dependencies" in sys.modules:
        monkeypatch.delitem(sys.modules, "api.dependencies")
    if hasattr(api_pkg, "dependencies"):
        monkeypatch.delattr(api_pkg, "dependencies")
    # Re-import now (against `fake`) and register it so subsequent
    # `from api import dependencies` calls in the test bodies see the
    # freshly-bound module; monkeypatch restores the original on teardown.
    fresh = importlib.import_module("api.dependencies")
    monkeypatch.setitem(sys.modules, "api.dependencies", fresh)
    monkeypatch.setattr(api_pkg, "dependencies", fresh)
    return fake


def test_clear_singletons_resets_db_module_engine(fake_db_module):
    """clear_singletons() must null out `db._engine` (not just the api copy)."""
    from api import dependencies as deps

    deps.clear_singletons()  # start clean
    eng = deps.get_db_engine()
    assert eng is fake_db_module._engine
    assert deps._db_engine is fake_db_module._engine

    deps.clear_singletons()

    assert fake_db_module._engine is None, (
        "clear_singletons must reset db._engine — otherwise next get_engine() "
        "returns a disposed engine"
    )
    assert deps._db_engine is None


def test_clear_singletons_disposes_engine_before_clearing(fake_db_module):
    """Engine.dispose() must be called once before the pointer is dropped."""
    from api import dependencies as deps

    deps.clear_singletons()
    eng = deps.get_db_engine()
    assert eng.dispose.call_count == 0

    deps.clear_singletons()

    assert eng.dispose.call_count == 1


def test_clear_singletons_next_call_returns_fresh_engine(fake_db_module):
    """After clear, get_db_engine() must build a NEW engine, not reuse stale."""
    from api import dependencies as deps

    deps.clear_singletons()
    first = deps.get_db_engine()
    deps.clear_singletons()
    second = deps.get_db_engine()

    assert first is not second, (
        "stale engine still served after clear_singletons — regression of "
        "PUNCH-LIST-2026-05-13 line 46"
    )


def test_clear_singletons_resets_dependent_stores(fake_db_module):
    """Companion singletons (pit, journal, registry, astrogrid) also clear."""
    from api import dependencies as deps

    deps.clear_singletons()

    with (
        patch.object(deps, "PITStore", return_value=MagicMock(name="pit")),
        patch.object(deps, "DecisionJournal", return_value=MagicMock(name="journal")),
        patch.object(deps, "ModelRegistry", return_value=MagicMock(name="registry")),
        patch.object(deps, "AstroGridStore", return_value=MagicMock(name="astro")),
    ):
        deps.get_pit_store()
        deps.get_journal()
        deps.get_model_registry()
        deps.get_astrogrid_store()

        assert deps._pit_store is not None
        assert deps._journal is not None
        assert deps._model_registry is not None
        assert deps._astrogrid_store is not None

        deps.clear_singletons()

        assert deps._pit_store is None
        assert deps._journal is None
        assert deps._model_registry is None
        assert deps._astrogrid_store is None


def test_clear_singletons_idempotent_on_cold_state(fake_db_module):
    """Calling clear_singletons() before any engine exists must not raise."""
    from api import dependencies as deps

    deps._db_engine = None
    fake_db_module._engine = None

    deps.clear_singletons()  # should be a no-op, not an AttributeError
    assert deps._db_engine is None
    assert fake_db_module._engine is None
