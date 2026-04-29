"""Tests for durable signal backlinker cursor semantics."""

from __future__ import annotations

from intelligence import signal_backlinker as sb


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Connection:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((str(sql), params or {}))
        return _Result(self.rows)


class _Context:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, exc_type, exc, tb):
        return False


class _Engine:
    def __init__(self, rows):
        self.conn = _Connection(rows)

    def connect(self):
        return _Context(self.conn)


def _row(signal_id: int) -> tuple:
    return (
        signal_id,
        "news_event",
        "AAPL",
        "Example Actor",
        "bullish",
        0.1,
        0.7,
        None,
        {},
        None,
    )


def test_backlinker_fetch_after_cursor_does_not_apply_lookback(monkeypatch):
    engine = _Engine([])
    monkeypatch.setattr(sb, "ensure_backlinker_state", lambda engine: None)
    monkeypatch.setattr(sb, "_get_cursor", lambda engine: 539)

    stats = sb.backlink_signals(engine, batch_size=25, since_minutes=1)

    sql, params = engine.conn.calls[0]
    assert "created_at >=" not in sql
    assert "mins" not in params
    assert params == {"cursor": 539, "lim": 25}
    assert stats["last_signal_id"] == 539


def test_backlinker_does_not_advance_cursor_past_failed_row(monkeypatch):
    engine = _Engine([_row(540), _row(541)])
    cursor_updates = []
    monkeypatch.setattr(sb, "ensure_backlinker_state", lambda engine: None)
    monkeypatch.setattr(sb, "_get_cursor", lambda engine: 539)
    monkeypatch.setattr(sb, "_set_cursor", lambda engine, value: cursor_updates.append(value))
    monkeypatch.setattr(sb, "_process_signal", lambda engine, row: {"errors": 1})

    stats = sb.backlink_signals(engine, batch_size=25)

    assert cursor_updates == []
    assert stats["errors"] == 1
    assert stats["last_signal_id"] == 539


def test_backlinker_advances_through_noise_and_success(monkeypatch):
    engine = _Engine([_row(540), _row(541)])
    cursor_updates = []
    outcomes = [
        {"skipped_noise": 1},
        {"actors_found": 1, "connections_created": 1, "actor_id": "actor", "actor_name": "Example Actor"},
    ]
    monkeypatch.setattr(sb, "ensure_backlinker_state", lambda engine: None)
    monkeypatch.setattr(sb, "_get_cursor", lambda engine: 539)
    monkeypatch.setattr(sb, "_set_cursor", lambda engine, value: cursor_updates.append(value))
    monkeypatch.setattr(sb, "_process_signal", lambda engine, row: outcomes.pop(0))

    stats = sb.backlink_signals(engine, batch_size=25)

    assert cursor_updates == [541]
    assert stats["skipped_noise"] == 1
    assert stats["actors_found"] == 1
    assert stats["connections_created"] == 1
    assert stats["last_signal_id"] == 541
    assert stats["touched_actors"] == {"actor": "Example Actor"}
