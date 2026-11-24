"""Real-PostgreSQL proof: only one of two overlapping exits closes a paper trade.

Codex review 6031218970 (at f48edfeb) held executor operation because the
widened exit scan wrote the close and the strategy counters without an atomic
OPEN-state winner. ``PaperTradingEngine.close_trade`` read the trade with
``status = 'OPEN'`` and then updated it ``WHERE id = :id``. Under READ
COMMITTED a second run's read still saw OPEN while the first run's close was
uncommitted. Its UPDATE waited on the row lock, then matched the committed
CLOSED row on ``id`` alone, rewrote the exit and moved the strategy's trade
count, wins/losses, P&L and capital a second time.

The fix makes the close a claim, ``UPDATE ... WHERE id = :id AND status =
'OPEN' RETURNING ...``, and writes P&L and counters only when the claim
returned the row. Here run A closes the trade and is held at COMMIT, with its
row lock still held. Run B, on a second connection, starts its own close, and
the test checks that B is blocked behind A. Then A commits: B must report the
trade already closed, and every counter must have moved once.

Throwaway schema per test, dropped afterwards. Skips without
``GRID_TEST_DB_URL``; the CI step "Signal executor exposure lock and close
race PostgreSQL contract" fails on any skip. No production table, position
or venue is touched.
"""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

from trading import signal_executor as se
from trading.paper_engine import PaperTradingEngine

pytestmark = pytest.mark.xdist_group("postgres")

_ENTRY_PRICE = 100.0
_A_EXIT = 110.0  # run A's exit: +10 % on a 0.10 position = +100.00
_B_EXIT = 120.0  # run B's exit, which must never be written


@pytest.fixture
def pg_url_schema() -> tuple[str, str]:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        pytest.skip("GRID_TEST_DB_URL is required for the disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("the close-race proof requires a local disposable test database")

    schema = "sig_exec_close_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    try:
        yield url, schema
    finally:
        with admin.begin() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        admin.dispose()


def _schema_engine(url: str, schema: str) -> Engine:
    return create_engine(url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})


def _blocked_backends(engine: Engine) -> int:
    """Backends in this database currently waiting on another backend's lock."""
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() "
            "  AND cardinality(pg_blocking_pids(pid)) > 0"
        )).scalar_one()


def _seed(engine: Engine) -> int:
    """One ACTIVE strategy with one expired OPEN 0.10 LONG trade. Returns the trade id."""
    PaperTradingEngine(engine)  # creates paper_trades / paper_strategies in the schema
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO paper_strategies (id, leader, follower, status, horizon_days) "
            "VALUES ('s1', 'spy_close', 'qqq_close', 'ACTIVE', 1)"
        ))
        return conn.execute(text(
            "INSERT INTO paper_trades (strategy_id, ticker, direction, entry_price, entry_date, "
            "position_size, status) VALUES ('s1', 'qqq_close', 'LONG', :px, :d, 0.10, 'OPEN') "
            "RETURNING id"
        ), {"px": _ENTRY_PRICE, "d": date.today() - timedelta(days=5)}).scalar_one()


def _trade(engine: Engine, trade_id: int) -> dict[str, Any]:
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, exit_price, pnl, pnl_pct FROM paper_trades WHERE id = :id"
        ), {"id": trade_id}).mappings().one()
    return dict(row)


def _strategy(engine: Engine) -> dict[str, Any]:
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT total_trades, wins, losses, total_pnl, capital, high_water_mark, "
            "max_drawdown, status FROM paper_strategies WHERE id = 's1'"
        )).mappings().one()
    return dict(row)


def _race(
    url: str,
    schema: str,
    trade_id: int,
    run_b: Callable[[], Any],
    probe: Engine,
) -> tuple[dict, dict]:
    """Close *trade_id* in run A, hold A at COMMIT, run B, then let A commit.

    Run A is ``close_trade`` on its own engine, held inside SQLAlchemy's
    commit event, which fires before the DBAPI commit, so A's row lock is
    still held. Returns ``(a_out, b_out)``, each ``{"result": ...}`` or
    ``{"error": exc}``, plus ``b_out["waited"]``: whether B was seen blocked
    behind A before A committed.
    """
    engine_a = _schema_engine(url, schema)
    pe_a = PaperTradingEngine(engine_a)  # DDL commits before the hold is armed
    at_commit = threading.Event()
    release = threading.Event()

    def hold_commit(_conn) -> None:
        if not at_commit.is_set():
            at_commit.set()
            release.wait(30)

    event.listen(engine_a, "commit", hold_commit)
    a_out: dict[str, Any] = {}
    b_out: dict[str, Any] = {}

    def body_a() -> None:
        try:
            a_out["result"] = pe_a.close_trade(trade_id, _A_EXIT, notes="run A")
        except Exception as exc:  # noqa: BLE001 - surfaced by the asserts
            a_out["error"] = exc

    def body_b() -> None:
        try:
            b_out["result"] = run_b()
        except Exception as exc:  # noqa: BLE001 - surfaced by the asserts
            b_out["error"] = exc

    thread_a = threading.Thread(target=body_a, daemon=True)
    thread_b = threading.Thread(target=body_b, daemon=True)
    try:
        thread_a.start()
        assert at_commit.wait(15), "run A never reached COMMIT"
        thread_b.start()
        deadline = time.monotonic() + 15
        while _blocked_backends(probe) == 0 and thread_b.is_alive() and time.monotonic() < deadline:
            time.sleep(0.05)
        b_out["waited"] = _blocked_backends(probe) >= 1 and thread_b.is_alive()
    finally:
        release.set()
        thread_a.join(30)
        thread_b.join(60)
        event.remove(engine_a, "commit", hold_commit)
        engine_a.dispose()
    return a_out, b_out


def test_two_overlapping_closes_one_winner_counters_move_once(pg_url_schema) -> None:
    url, schema = pg_url_schema
    engine_b = _schema_engine(url, schema)
    try:
        trade_id = _seed(engine_b)
        pe_b = PaperTradingEngine(engine_b)
        a_out, b_out = _race(
            url, schema, trade_id,
            lambda: pe_b.close_trade(trade_id, _B_EXIT, notes="run B"),
            engine_b,
        )

        assert "error" not in a_out and "error" not in b_out, (a_out, b_out)
        assert b_out["waited"], "run B was not blocked behind run A's uncommitted close"
        assert "error" not in a_out["result"], a_out
        assert a_out["result"]["pnl"] == pytest.approx(100.0)
        # The loser reports the trade already closed.
        assert "error" in b_out["result"], b_out
        assert b_out["result"].get("reason") == "already_closed", b_out

        # Run A's close stands; run B's exit was never written.
        trade = _trade(engine_b, trade_id)
        assert trade["status"] == "CLOSED"
        assert trade["exit_price"] == pytest.approx(_A_EXIT)
        assert trade["pnl"] == pytest.approx(100.0)
        assert trade["pnl_pct"] == pytest.approx(0.10)

        # Every strategy counter moved exactly once.
        strat = _strategy(engine_b)
        assert strat["total_trades"] == 1
        assert strat["wins"] == 1 and strat["losses"] == 0
        assert strat["total_pnl"] == pytest.approx(100.0)
        assert strat["capital"] == pytest.approx(10_100.0)
        assert strat["high_water_mark"] == pytest.approx(10_100.0)
        assert strat["max_drawdown"] == pytest.approx(0.0)
    finally:
        engine_b.dispose()


def test_overlapping_executor_run_reports_already_closed_and_counts_nothing(
    pg_url_schema, monkeypatch,
) -> None:
    # Run B is a whole execute_signals: it listed the trade as OPEN (A's close
    # was uncommitted) and tried to exit it. It must not count the close,
    # report P&L or kill anything; it records SKIP_CLOSE / already_closed.
    url, schema = pg_url_schema
    engine_b = _schema_engine(url, schema)
    try:
        trade_id = _seed(engine_b)
        pe_b = PaperTradingEngine(engine_b)
        today = date.today()
        prices = {
            "spy_close": [(today, 100.1), (today - timedelta(days=1), 100.0)],  # no entry signal
            "qqq_close": [(today, _B_EXIT)],
        }
        breaker = MagicMock()
        breaker.should_execute.return_value = True
        breaker.last_error = None
        monkeypatch.setattr(se, "PaperTradingEngine", lambda _e: pe_b)
        monkeypatch.setattr(se, "StrategyCircuitBreaker", lambda _e: breaker)
        monkeypatch.setattr(se, "_get_latest_prices", lambda _c, name, n=2, **_k: prices[name][:n])
        monkeypatch.setattr(se, "_get_expected_lag", lambda *_a: 1)
        # The seeded trade is a post-cutover position of an ACTIVE strategy.
        monkeypatch.setattr(se, "LEGACY_POSITION_CUTOVER", date(2000, 1, 1), raising=False)

        a_out, b_out = _race(url, schema, trade_id, lambda: se.execute_signals(engine_b), engine_b)

        assert "error" not in a_out and "error" not in b_out, (a_out, b_out)
        assert b_out["waited"], "run B was not blocked behind run A's uncommitted close"
        summary = b_out["result"]
        assert summary["trades_closed"] == 0, summary
        assert summary["strategies_killed"] == 0, summary
        assert not [d for d in summary["details"] if d.get("action") == "CLOSE"], summary
        assert {
            "action": "SKIP_CLOSE", "strategy_id": "s1", "trade_id": trade_id,
            "ticker": "qqq_close", "reason": "already_closed",
        } in summary["details"], summary["details"]

        trade = _trade(engine_b, trade_id)
        assert trade["exit_price"] == pytest.approx(_A_EXIT)
        strat = _strategy(engine_b)
        assert strat["total_trades"] == 1
        assert strat["total_pnl"] == pytest.approx(100.0)
        assert strat["capital"] == pytest.approx(10_100.0)
        assert strat["status"] == "ACTIVE"
    finally:
        engine_b.dispose()
