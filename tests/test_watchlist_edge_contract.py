"""Contract coverage for truthful watchlist edge fields."""

from __future__ import annotations

import os

os.environ.setdefault("DB_PASSWORD", "test-password")

from api.routers import watchlist_overview
from intelligence import trust_scorer


class _Result:
    def __init__(self, rows=(), scalar_value=False):
        self.rows = rows
        self.scalar_value = scalar_value

    def fetchall(self):
        return self.rows

    def scalar(self):
        return self.scalar_value


class _Connection:
    def execute(self, statement, *_args):
        if "information_schema.tables" in str(statement):
            return _Result(scalar_value=False)
        return _Result()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _Engine:
    def connect(self):
        return _Connection()


def test_edge_uses_signal_type_and_marks_unscored_convergence(monkeypatch):
    monkeypatch.setattr(watchlist_overview, "get_insider_edge", None, raising=False)
    monkeypatch.setattr(
        trust_scorer,
        "get_insider_edge",
        lambda *_args: {"congressional": [{"member": "A", "signal_type": "SELL", "trust_score": None}], "insider": [], "darkpool": []},
    )
    monkeypatch.setattr(
        trust_scorer,
        "detect_convergence",
        lambda *_args, **_kwargs: [{"signal_type": "SELL", "source_count": 2, "combined_confidence": None}],
    )

    payload = watchlist_overview.get_ticker_edge("test", user={}, engine=_Engine())

    assert payload["congressional"][0]["action"] == "SELL"
    assert payload["congressional"][0]["trust_score"] is None
    assert payload["convergence"] == {
        "direction": None,
        "signal_type": "SELL",
        "source_count": 2,
        "confidence": None,
        "status": "detected",
    }
    assert "leaning SELL" in payload["edge_summary"]
