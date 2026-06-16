from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from intelligence.hypothesis_engine import HypothesisGenerator


class _CaptureConn:
    def __init__(self, statements: list[tuple[str, dict]]):
        self._statements = statements

    def execute(self, query, params):
        self._statements.append((str(query), params))
        return SimpleNamespace(scalar=lambda: 0)


class _CaptureEngine:
    def __init__(self):
        self.statements: list[tuple[str, dict]] = []

    def connect(self):
        engine = self

        class _Ctx:
            def __enter__(self):
                return _CaptureConn(engine.statements)

            def __exit__(self, exc_type, exc, tb):
                return False

        return _Ctx()


def test_follow_on_activity_uses_bind_safe_date_cast_for_actor():
    engine = _CaptureEngine()
    generator = object.__new__(HypothesisGenerator)
    generator.engine = engine

    result = generator._check_follow_on_activity(
        {"watch_actor": "Acme", "window_days": 7},
        datetime(2026, 5, 24, tzinfo=timezone.utc),
    )

    assert result == "inconclusive"
    sql, params = engine.statements[0]
    assert ":since::date" not in sql
    assert "CAST(:since AS date)" in sql
    assert params["since"] == datetime(2026, 5, 24, tzinfo=timezone.utc)


def test_follow_on_activity_uses_bind_safe_date_cast_for_category():
    engine = _CaptureEngine()
    generator = object.__new__(HypothesisGenerator)
    generator.engine = engine

    result = generator._check_follow_on_activity(
        {"watch_category": "source-audit", "window_days": 7},
        datetime(2026, 5, 24, tzinfo=timezone.utc),
    )

    assert result == "inconclusive"
    sql, params = engine.statements[0]
    assert ":since::date" not in sql
    assert "CAST(:since AS date)" in sql
    assert params["since"] == datetime(2026, 5, 24, tzinfo=timezone.utc)
