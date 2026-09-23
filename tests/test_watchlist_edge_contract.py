"""Contract coverage for the read-only watchlist edge response."""

from __future__ import annotations

from datetime import datetime, timezone
import os

os.environ.setdefault("DB_PASSWORD", "test-password")

from api.routers import watchlist_overview


class _Result:
    def __init__(self, rows=(), scalar_value=False):
        self.rows = rows
        self.scalar_value = scalar_value

    def fetchall(self):
        return self.rows

    def scalar(self):
        return self.scalar_value


class _Connection:
    def __init__(self):
        now = datetime.now(timezone.utc)
        self.core = [("congressional", "A", "SELL", now, None, {"amount": "$1K"})]
        self.convergence = [
            ("congressional", "A", "SELL", now, None),
            ("insider", "B", "SELL", now, 0.8),
            ("darkpool", "C", "SELL", now, 0.6),
        ]

    def execute(self, statement, *_args):
        sql = str(statement)
        if "outcome IN ('PENDING', 'CORRECT')" in sql:
            return _Result(self.convergence)
        if "source_type IN ('congressional', 'insider', 'darkpool')" in sql:
            return _Result(self.core)
        if "information_schema.tables" in sql:
            return _Result(scalar_value=False)
        return _Result()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Engine:
    def connect(self):
        return _Connection()


def test_edge_reads_persisted_signal_types_and_marks_unscored_convergence(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("GET edge must not use an initializing helper")

    monkeypatch.setattr(watchlist_overview, "get_insider_edge", forbidden, raising=False)
    monkeypatch.setattr(watchlist_overview, "detect_convergence", forbidden, raising=False)

    payload = watchlist_overview.get_ticker_edge("test", user={}, engine=_Engine())

    assert payload["status"] == "partial"
    assert payload["congressional"][0]["action"] == "SELL"
    assert payload["congressional"][0]["trust_score"] is None
    assert payload["convergence"] == {
        "direction": "bearish",
        "direction_basis": "inferred_from_signal_types",
        "signal_type": "SELL",
        "source_count": 3,
        "scored_source_count": 2,
        "confidence": 0.7,
        "confidence_basis": "mean_trust_of_scored_sources",
        "status": "detected",
    }
    assert "independent sources bearish" in payload["edge_summary"]
    assert payload["availability"]["lever_pullers"]["status"] == "available"
    assert payload["availability"]["actor_context"]["status"] == "available"


def test_edge_missing_signal_sources_is_explicitly_unavailable():
    class MissingConnection:
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("relation signal_sources does not exist")

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class MissingEngine:
        def connect(self):
            return MissingConnection()

    payload = watchlist_overview.get_ticker_edge("test", user={}, engine=MissingEngine())

    assert payload["status"] == "unavailable"
    assert payload["reason"] == "signal_sources_unavailable"
    assert payload["convergence"]["status"] == "unavailable"
