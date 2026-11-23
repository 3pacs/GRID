"""Real-PostgreSQL proof: the signal executor's exposure reservation is serialized.

Codex review 6011903757 (at 316bff53) held executor enablement because
``execute_signals`` read total open exposure once and applied the
``MAX_GROSS_EXPOSURE`` cap outside the trade insert's transaction. A scheduled
run (grid-scheduler) and a manual ``POST /execute-signals`` (grid-api) are
separate processes: both could read 0.85, each allow 0.10 and breach 1.00, or
both open the same strategy's entry.

The fix makes the reservation one step on one transaction: a fixed-key
``pg_advisory_xact_lock``, then the open-entry re-check and the exposure
re-read, then the insert. Here run A reserves s1 on its own connection and
holds its transaction open with the lock; run B, a full ``execute_signals``
on a second connection, must wait on that lock, then see A's committed entry
(no duplicate s1) and the exposure it added (s2 refused at the cap).

Throwaway schema per test, dropped afterwards. Skips without
``GRID_TEST_DB_URL``; the CI step "Signal executor exposure lock PostgreSQL
contract" fails on any skip. No production table, position or venue is touched.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from trading import signal_executor as se
from trading.paper_engine import PaperTradingEngine

pytestmark = pytest.mark.xdist_group("postgres")


@pytest.fixture
def pg_engine() -> Engine:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for the disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("the exposure-lock proof requires a local disposable test database")

    schema = "sig_exec_lock_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()


def _exposure_lock_waiters(engine: Engine) -> int:
    """Backends waiting for the executor's exposure lock (this key only)."""
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT count(*) FROM pg_locks "
            "WHERE locktype = 'advisory' AND NOT granted AND objsubid = 1 "
            "  AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) "
            "  AND ((classid::bigint << 32) | objid::bigint) = hashtextextended(:key, 0)"
        ), {"key": se.EXPOSURE_LOCK_KEY}).scalar_one()


def _exposure_lock_holders(engine: Engine) -> int:
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT count(*) FROM pg_locks "
            "WHERE locktype = 'advisory' AND granted AND objsubid = 1 "
            "  AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) "
            "  AND ((classid::bigint << 32) | objid::bigint) = hashtextextended(:key, 0)"
        ), {"key": se.EXPOSURE_LOCK_KEY}).scalar_one()


def _seed(engine: Engine) -> PaperTradingEngine:
    """Paper tables, a PAUSED strategy holding 0.85 gross, and two ACTIVE strategies."""
    pe = PaperTradingEngine(engine)  # creates paper_trades / paper_strategies in the schema
    with engine.begin() as conn:
        for sid, status, follower, horizon in (
            ("s_base", "PAUSED", "base_close", 30),
            ("s1", "ACTIVE", "qqq_close", None),
            ("s2", "ACTIVE", "iwm_close", None),
        ):
            conn.execute(text(
                "INSERT INTO paper_strategies (id, leader, follower, status, horizon_days) "
                "VALUES (:id, 'spy_close', :follower, :status, :horizon)"
            ), {"id": sid, "follower": follower, "status": status, "horizon": horizon})
        conn.execute(text(
            "INSERT INTO paper_trades (strategy_id, ticker, direction, entry_price, entry_date, "
            "position_size, status) VALUES ('s_base', 'base_close', 'LONG', 50.0, :d, 0.85, 'OPEN')"
        ), {"d": date.today()})
    return pe


def _open_by_strategy(engine: Engine) -> dict[str, int]:
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT strategy_id, count(*) FROM paper_trades WHERE status = 'OPEN' "
            "GROUP BY strategy_id ORDER BY strategy_id"
        )).fetchall()
    return {r[0]: int(r[1]) for r in rows}


def _gross(engine: Engine) -> float:
    with engine.connect() as conn:
        return se._get_open_gross_exposure(conn)


def test_second_run_waits_on_the_lock_then_respects_cap_and_open_entry(pg_engine, monkeypatch) -> None:
    pe_a = _seed(pg_engine)
    pe_b = PaperTradingEngine(pg_engine)  # built (and its DDL run) before A holds anything

    today = date.today()
    prices = {
        "spy_close": [(today, 103.0), (today - timedelta(days=1), 100.0)],  # +3 % leader
        "qqq_close": [(today, 500.0)],
        "iwm_close": [(today, 200.0)],
        "base_close": [(today, 50.0)],
    }
    breaker = MagicMock()
    breaker.should_execute.return_value = True
    breaker.last_error = None
    monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: pe_b)
    monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
    monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2, **_k: prices[name][:n])
    monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)

    # Run A: reserve s1 (0.85 -> 0.95) on its own connection and hold the
    # transaction, and with it the exposure lock, open.
    conn_a = pg_engine.connect()
    tx_a = conn_a.begin()
    b_out: dict[str, Any] = {}
    run_b: threading.Thread | None = None
    try:
        trade_a, skip_a, gross_a = se._reserve_and_open_trade(
            conn_a, pe_a, strategy_id="s1", ticker="qqq_close", direction="LONG",
            entry_price=500.0, position_size=0.10, signal_strength=0.03, hypothesis_id=None,
        )
        assert skip_a is None and trade_a > 0
        assert gross_a == pytest.approx(0.85)
        assert _exposure_lock_holders(pg_engine) == 1

        # Run B: a whole execute_signals on a second connection. Its snapshot
        # cannot see A's uncommitted s1 entry (0.85, no open s1), so only the
        # lock stops it.
        def body() -> None:
            try:
                b_out["summary"] = se.execute_signals(pg_engine)
            except Exception as exc:  # noqa: BLE001 - surfaced by the asserts below
                b_out["error"] = exc

        run_b = threading.Thread(target=body, daemon=True)
        run_b.start()
        deadline = time.monotonic() + 15
        while _exposure_lock_waiters(pg_engine) == 0 and run_b.is_alive() and time.monotonic() < deadline:
            time.sleep(0.05)
        b_waited = _exposure_lock_waiters(pg_engine) == 1 and run_b.is_alive()
        tx_a.commit()
    finally:
        if tx_a.is_active:
            tx_a.rollback()
        conn_a.close()
        if run_b is not None:
            run_b.join(60)

    assert "error" not in b_out, b_out
    assert b_waited, "the second run did not wait for the exposure lock"
    summary = b_out["summary"]
    assert summary["trades_opened"] == 0, summary
    skips = {d["strategy_id"]: d for d in summary["details"] if d.get("action") == "SKIP"}
    # s1: A's committed entry is seen on the re-check under the lock.
    assert skips["s1"]["reason"] == "open_entry_exists"
    # s2: B's snapshot said 0.85; the re-read under the lock says 0.95, and
    # 0.95 + 0.10 > MAX_GROSS_EXPOSURE.
    assert skips["s2"]["reason"] == "max_gross_exposure"
    assert skips["s2"]["current_gross_exposure"] == pytest.approx(0.95)

    assert _open_by_strategy(pg_engine) == {"s1": 1, "s_base": 1}
    assert _gross(pg_engine) == pytest.approx(0.95)
    assert _gross(pg_engine) <= se.MAX_GROSS_EXPOSURE
    # Transaction-scoped: nothing is left holding the lock.
    assert _exposure_lock_holders(pg_engine) == 0


def test_uncontended_runs_fill_to_the_cap_and_release_the_lock(pg_engine, monkeypatch) -> None:
    # Control: without a concurrent holder, one run opens what fits (s1 at
    # 0.95) and refuses what does not (s2), and leaves no lock behind.
    pe = _seed(pg_engine)
    today = date.today()
    prices = {
        "spy_close": [(today, 103.0), (today - timedelta(days=1), 100.0)],
        "qqq_close": [(today, 500.0)],
        "iwm_close": [(today, 200.0)],
        "base_close": [(today, 50.0)],
    }
    breaker = MagicMock()
    breaker.should_execute.return_value = True
    breaker.last_error = None
    monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: pe)
    monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
    monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2, **_k: prices[name][:n])
    monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)

    summary = se.execute_signals(pg_engine)

    assert summary["trades_opened"] == 1
    assert [d["strategy_id"] for d in summary["details"] if d.get("action") == "OPEN"] == ["s1"]
    assert _open_by_strategy(pg_engine) == {"s1": 1, "s_base": 1}
    assert _gross(pg_engine) == pytest.approx(0.95)
    assert _exposure_lock_holders(pg_engine) == 0

    # A second run sees s1 open and s2 still over the cap: nothing new.
    again = se.execute_signals(pg_engine)
    assert again["trades_opened"] == 0
    assert _open_by_strategy(pg_engine) == {"s1": 1, "s_base": 1}
