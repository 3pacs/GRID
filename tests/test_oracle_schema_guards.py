from __future__ import annotations

import pytest

from oracle.engine import OracleEngine
from oracle.model_factory import ModelFactory
from schema_guard import reset_for_tests


class _FakeEngine:
    pass


@pytest.fixture(autouse=True)
def _reset_schema_guard():
    reset_for_tests()
    yield
    reset_for_tests()


def test_model_factory_schema_ensure_runs_once_per_process(monkeypatch):
    calls: list[object] = []

    def fake_ensure(self):
        calls.append(self.engine)

    monkeypatch.setattr(ModelFactory, "_ensure_columns", fake_ensure)

    first = _FakeEngine()
    second = _FakeEngine()

    ModelFactory(first)
    ModelFactory(second)

    assert calls == [first]


def test_model_factory_schema_ensure_retries_after_failure(monkeypatch):
    calls = 0

    def fake_ensure(self):
        nonlocal calls
        calls += 1
        raise RuntimeError("ddl lock timeout")

    monkeypatch.setattr(ModelFactory, "_ensure_columns", fake_ensure)

    with pytest.raises(RuntimeError):
        ModelFactory(_FakeEngine())
    with pytest.raises(RuntimeError):
        ModelFactory(_FakeEngine())

    assert calls == 2


def test_oracle_engine_schema_ensure_runs_once_per_process(monkeypatch):
    calls: list[object] = []

    def fake_ensure(self):
        calls.append(self.engine)

    monkeypatch.setattr(OracleEngine, "_ensure_tables", fake_ensure)
    monkeypatch.setattr(OracleEngine, "_load_models", lambda self: [])

    first = _FakeEngine()
    second = _FakeEngine()

    OracleEngine(first)
    OracleEngine(second)

    assert calls == [first]


def test_oracle_engine_schema_ensure_retries_after_failure(monkeypatch):
    calls = 0

    def fake_ensure(self):
        nonlocal calls
        calls += 1
        raise RuntimeError("ddl lock timeout")

    monkeypatch.setattr(OracleEngine, "_ensure_tables", fake_ensure)
    monkeypatch.setattr(OracleEngine, "_load_models", lambda self: [])

    with pytest.raises(RuntimeError):
        OracleEngine(_FakeEngine())
    with pytest.raises(RuntimeError):
        OracleEngine(_FakeEngine())

    assert calls == 2
