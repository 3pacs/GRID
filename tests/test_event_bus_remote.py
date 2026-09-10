"""Cross-process delivery semantics of ``events/bus.py`` (2026-09-10).

The bus now distinguishes in-process subscribers (default) from
listener-delivered ones (``remote=True``): the contracts dispatcher must see
each contract exactly once, while the SSE router wants everything that
arrives through ``pg_notify`` from other processes.
"""
from __future__ import annotations

import json

from events.bus import EventBus, RecentEventIds, normalize_listen_dsn
from events.channels import REGIME_CHANGE, SIGNAL_FIRE


def test_local_subscriber_receives_in_process_emit_only() -> None:
    bus = EventBus()
    local: list = []
    bus.subscribe(SIGNAL_FIRE, local.append)

    bus.emit_sync(SIGNAL_FIRE, {"ticker": "NVDA"})
    bus._on_pg_notify(None, 1, SIGNAL_FIRE, json.dumps({"ticker": "AMD"}))

    assert [e.payload["ticker"] for e in local] == ["NVDA"]


def test_remote_subscriber_receives_both_paths() -> None:
    bus = EventBus()
    remote: list = []
    bus.subscribe(SIGNAL_FIRE, remote.append, remote=True)

    bus.emit_sync(SIGNAL_FIRE, {"ticker": "NVDA"})
    bus._on_pg_notify(None, 1, SIGNAL_FIRE, json.dumps({"ticker": "AMD"}))

    assert [e.payload["ticker"] for e in remote] == ["NVDA", "AMD"]


def test_pg_notify_respects_channel_and_bad_json() -> None:
    bus = EventBus()
    remote: list = []
    bus.subscribe(SIGNAL_FIRE, remote.append, remote=True)

    bus._on_pg_notify(None, 1, REGIME_CHANGE, json.dumps({"to": "CRISIS"}))
    assert remote == []

    bus._on_pg_notify(None, 1, SIGNAL_FIRE, "not json")
    assert remote[0].payload == {"raw": "not json"}


def test_unsubscribe_removes_callback() -> None:
    bus = EventBus()
    seen: list = []
    bus.subscribe(SIGNAL_FIRE, seen.append, remote=True)
    # A fresh bound-method object each access — unsubscribe must match by
    # equality, which is exactly what the SSE router relies on.
    bus.unsubscribe(SIGNAL_FIRE, seen.append)
    bus.emit_sync(SIGNAL_FIRE, {"ticker": "SPY"})
    assert seen == []
    # Unsubscribing an unknown callback / channel is a no-op.
    bus.unsubscribe("grid_nonexistent", seen.append)


def test_subscriber_exception_does_not_break_fan_out() -> None:
    bus = EventBus()
    seen: list = []

    def boom(_event) -> None:
        raise RuntimeError("subscriber failed")

    bus.subscribe(SIGNAL_FIRE, boom)
    bus.subscribe(SIGNAL_FIRE, seen.append)
    bus.emit_sync(SIGNAL_FIRE, {"ticker": "QQQ"})
    assert len(seen) == 1


def test_recent_event_ids_dedups_and_bounds() -> None:
    recent = RecentEventIds(capacity=3)
    assert recent.seen_before("a") is False
    assert recent.seen_before("a") is True
    assert recent.seen_before(None) is False
    assert recent.seen_before("") is False
    for key in ("b", "c", "d"):
        assert recent.seen_before(key) is False
    # "a" was evicted once capacity (3) was exceeded.
    assert len(recent) == 3
    assert recent.seen_before("a") is False


def test_normalize_listen_dsn_strips_sqlalchemy_driver() -> None:
    assert normalize_listen_dsn("postgresql+psycopg2://u:p@h:5432/db") == "postgresql://u:p@h:5432/db"
    assert normalize_listen_dsn("postgresql://u:p@h/db") == "postgresql://u:p@h/db"


def test_start_records_requested_channels_without_asyncpg(monkeypatch) -> None:
    """start() must remember the channel set even when asyncpg is absent, so
    a later successful connect listens on the right channels."""
    import asyncio
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "asyncpg":
            raise ImportError("no asyncpg in test")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    bus = EventBus()
    asyncio.run(bus.start("postgresql://u:p@h/db", channels=[SIGNAL_FIRE, "grid_contracts_signal_fired", SIGNAL_FIRE]))
    assert bus._channels == (SIGNAL_FIRE, "grid_contracts_signal_fired")
    assert bus.listening is False
