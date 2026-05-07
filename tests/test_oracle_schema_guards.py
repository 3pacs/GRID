from __future__ import annotations

import threading

from oracle.engine import OracleEngine
from oracle.model_factory import ModelFactory


class _FakeEngine:
    pass


def test_model_factory_schema_ensure_runs_once_per_process(monkeypatch):
    calls: list[object] = []

    def fake_ensure(self):
        calls.append(self.engine)

    monkeypatch.setattr(ModelFactory, "_columns_ensured", False)
    monkeypatch.setattr(ModelFactory, "_ensure_lock", threading.Lock())
    monkeypatch.setattr(ModelFactory, "_ensure_columns", fake_ensure)

    first = _FakeEngine()
    second = _FakeEngine()

    ModelFactory(first)
    ModelFactory(second)

    assert calls == [first]
    assert ModelFactory._columns_ensured is True


def test_model_factory_schema_ensure_retries_after_failure(monkeypatch):
    calls = 0

    def fake_ensure(self):
        nonlocal calls
        calls += 1
        raise RuntimeError("ddl lock timeout")

    monkeypatch.setattr(ModelFactory, "_columns_ensured", False)
    monkeypatch.setattr(ModelFactory, "_ensure_lock", threading.Lock())
    monkeypatch.setattr(ModelFactory, "_ensure_columns", fake_ensure)

    try:
        ModelFactory(_FakeEngine())
    except RuntimeError:
        pass

    assert calls == 1
    assert ModelFactory._columns_ensured is False


def test_oracle_engine_schema_ensure_runs_once_per_process(monkeypatch):
    calls: list[object] = []

    def fake_ensure(self):
        calls.append(self.engine)

    monkeypatch.setattr(OracleEngine, "_tables_ensured", False)
    monkeypatch.setattr(OracleEngine, "_ensure_lock", threading.Lock())
    monkeypatch.setattr(OracleEngine, "_ensure_tables", fake_ensure)
    monkeypatch.setattr(OracleEngine, "_load_models", lambda self: [])

    first = _FakeEngine()
    second = _FakeEngine()

    OracleEngine(first)
    OracleEngine(second)

    assert calls == [first]
    assert OracleEngine._tables_ensured is True


def test_oracle_engine_schema_ensure_retries_after_failure(monkeypatch):
    calls = 0

    def fake_ensure(self):
        nonlocal calls
        calls += 1
        raise RuntimeError("ddl lock timeout")

    monkeypatch.setattr(OracleEngine, "_tables_ensured", False)
    monkeypatch.setattr(OracleEngine, "_ensure_lock", threading.Lock())
    monkeypatch.setattr(OracleEngine, "_ensure_tables", fake_ensure)
    monkeypatch.setattr(OracleEngine, "_load_models", lambda self: [])

    try:
        OracleEngine(_FakeEngine())
    except RuntimeError:
        pass

    assert calls == 1
    assert OracleEngine._tables_ensured is False
