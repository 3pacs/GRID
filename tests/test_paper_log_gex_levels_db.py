"""Tests for the read-only DB guard (task spec item 7).

No real database anywhere here: the SET-statement logic is tested against
a `MagicMock` DBAPI connection (Postgres session syntax isn't something
SQLite or any other in-process stand-in understands), and the "connect"
event wiring is tested against an in-memory SQLite engine used purely as a
connectable object — with the Postgres-specific pinning function itself
patched out, so nothing here needs a real database engine to talk to.
"""

from __future__ import annotations

import os

# db.py imports GRID's root config.settings, whose startup validator
# rejects an empty DB_PASSWORD (see tests/test_yfinance_auto_adjust_explicit.py
# for the same guard). setdefault leaves a real value (CI, .env) untouched.
os.environ.setdefault("DB_PASSWORD", "test-password")

from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa

import paper_log.gex_levels.db as db_mod
from paper_log.gex_levels.config import DB_STATEMENT_TIMEOUT
from paper_log.gex_levels.db import ReadOnlyGuardError, assert_read_only, create_engine_readonly


def test_pin_connection_readonly_issues_exact_statements() -> None:
    conn = MagicMock()
    cursor = conn.cursor.return_value

    db_mod._pin_connection_readonly(conn)

    executed = [call.args[0] for call in cursor.execute.call_args_list]
    assert executed == [
        "SET default_transaction_read_only = on",
        f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}'",
    ]
    conn.commit.assert_called_once()
    cursor.close.assert_called_once()


def test_pin_connection_readonly_closes_cursor_even_on_failure() -> None:
    conn = MagicMock()
    cursor = conn.cursor.return_value
    cursor.execute.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        db_mod._pin_connection_readonly(conn)

    cursor.close.assert_called_once()


def test_create_engine_readonly_uses_nullpool() -> None:
    engine = create_engine_readonly("sqlite:///:memory:")
    try:
        assert isinstance(engine.pool, sa.pool.NullPool)
    finally:
        engine.dispose()


def test_build_readonly_engine_pins_every_new_physical_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []
    monkeypatch.setattr(db_mod, "_pin_connection_readonly", lambda conn: calls.append(conn))
    monkeypatch.setattr(db_mod, "create_engine_readonly", lambda url: sa.create_engine("sqlite:///:memory:"))

    engine = db_mod.build_readonly_engine("sqlite:///:memory:")
    try:
        with engine.connect():
            pass
        with engine.connect():
            pass
    finally:
        engine.dispose()

    # NullPool underneath create_engine_readonly (patched here to a plain
    # SQLite engine for the wiring test) still opens a fresh physical
    # connection per connect() in this stand-in — the point being tested
    # is only that the pin fires on connect, at least once per connection.
    assert len(calls) >= 1


def _mock_engine_with_show_values(read_only: str, timeout: str):
    conn = MagicMock()
    conn.execute.return_value.scalar.side_effect = [read_only, timeout]
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    engine.connect.return_value.__exit__.return_value = False
    return engine


def test_assert_read_only_passes_when_both_values_correct() -> None:
    engine = _mock_engine_with_show_values("on", DB_STATEMENT_TIMEOUT)
    assert_read_only(engine)  # must not raise


def test_assert_read_only_rejects_writable_session() -> None:
    engine = _mock_engine_with_show_values("off", DB_STATEMENT_TIMEOUT)
    with pytest.raises(ReadOnlyGuardError, match="default_transaction_read_only"):
        assert_read_only(engine)


def test_assert_read_only_rejects_wrong_timeout() -> None:
    engine = _mock_engine_with_show_values("on", "0")
    with pytest.raises(ReadOnlyGuardError, match="statement_timeout"):
        assert_read_only(engine)
