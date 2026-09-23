"""Regression coverage for the authenticated, read-only sentiment GET."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timezone

# The router imports the application settings, but this unit test replaces the
# engine before any connection is opened.
os.environ.setdefault("DB_PASSWORD", "test-password")

from api.routers import briefing


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Connection:
    def __init__(self, row):
        self.row = row
        self.statements: list[str] = []

    def execute(self, statement):
        self.statements.append(str(statement))
        return _Result(self.row)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Engine:
    def __init__(self, row):
        self.connection = _Connection(row)

    def connect(self):
        return self.connection


def test_current_sentiment_reads_latest_prediction_without_bootstrap(monkeypatch):
    engine = _Engine((
        0.42,
        "BULLISH",
        json.dumps([{"name": "momentum", "score": 0.6, "weight": 0.08}]),
        json.dumps({"momentum": 0.08}),
        7,
        date(2026, 9, 22),
        datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc),
    ))
    monkeypatch.setattr(briefing, "get_engine", lambda: engine)

    payload = asyncio.run(briefing.get_current_sentiment())

    assert payload | {"served_at": "ignored"} == {
        "score": 0.42,
        "label": "BULLISH",
        "components": [{"name": "momentum", "score": 0.6, "weight": 0.08}],
        "weights": {"momentum": 0.08},
        "weights_version": 7,
        "prediction_date": "2026-09-22",
        "timestamp": "2026-09-22 12:00:00+00:00",
        "created_at": "2026-09-22 12:00:00+00:00",
        "served_at": "ignored",
        "available": True,
        "source": "latest_precomputed_prediction",
        "context": "Latest persisted sentiment prediction for 2026-09-22 (created 2026-09-22 12:00:00+00:00).",
    }
    assert payload["served_at"] != payload["created_at"]
    sql = engine.connection.statements[0].upper()
    assert sql.lstrip().startswith("SELECT")
    assert "CREATE TABLE" not in sql
    assert "INSERT" not in sql
    assert "UPDATE" not in sql
    assert "DELETE" not in sql


def test_current_sentiment_reports_absent_prediction_without_writing(monkeypatch):
    engine = _Engine(None)
    monkeypatch.setattr(briefing, "get_engine", lambda: engine)

    payload = asyncio.run(briefing.get_current_sentiment())

    assert payload == {
        "error": "No persisted sentiment prediction found",
        "available": False,
        "source": "latest_precomputed_prediction",
    }
    assert engine.connection.statements[0].lstrip().upper().startswith("SELECT")


def test_current_sentiment_sanitizes_engine_initialization_failure(monkeypatch):
    monkeypatch.setattr(
        briefing,
        "get_engine",
        lambda: (_ for _ in ()).throw(RuntimeError("database password=not-for-clients")),
    )

    payload = asyncio.run(briefing.get_current_sentiment())

    assert payload == {
        "error": "Persisted sentiment is temporarily unavailable",
        "available": False,
        "source": "latest_precomputed_prediction",
    }
