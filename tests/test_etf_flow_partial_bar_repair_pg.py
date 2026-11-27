"""The ETF flow partial-bar repair on PostgreSQL, with a second writer running.

Two services run the ETF flow writer, and the repair is a third writer of the
same series. All three take the ETF flow write lock of GRID #1501 around their
read-and-insert, so none of them builds a row on a read the other has already
made stale. Each case runs two real sessions at once; the lock cases have a
control with the lock taken away that shows the fault. Plus: planning reads
through a session that refuses writes, and a read for an instant before the
repair sees what GRID held then.

Needs ``GRID_TEST_DB_URL`` (a local disposable database); with
``REQUIRE_ETF_FLOWS_LOCK_PG=1`` a missing database fails instead of skipping.
"""
from __future__ import annotations

import os
import threading
from datetime import date, timedelta, timezone
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

import scripts.repair_etf_flow_partial_bars as mod
from ingestion.altdata import institutional_flows as inst
from store.observations import read_window_known_at
from tests import etf_flow_repair_support as m
from tests import thirteenf_holdings_support as s

SESSION_DAY = date(2026, 7, 20)
FROM = date(2026, 4, 1)
pytestmark = pytest.mark.xdist_group("postgres")


def _pg_url() -> str:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get("REQUIRE_ETF_FLOWS_LOCK_PG") == "1":
            pytest.fail("GRID_TEST_DB_URL is required for the ETF repair PG proof (REQUIRE_ETF_FLOWS_LOCK_PG=1)")
        pytest.skip("GRID_TEST_DB_URL is required for the PostgreSQL repair proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("the ETF repair PG proof requires a local disposable test database")
    return url


@pytest.fixture(autouse=True)
def _one_etf(monkeypatch):
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY"])
    monkeypatch.setattr(inst, "_YF_RATE_DELAY", 0)
    monkeypatch.setattr(inst, "_new_york_today", lambda: SESSION_DAY)
    monkeypatch.setattr(mod, "PROVIDER_DELAY_S", 0)
    monkeypatch.setattr(mod, "_sleep", lambda _s: None)


@pytest.fixture()
def market():
    return m.Market(end=date(2026, 7, 17), periods=140,
                    dividends={date(2026, 5, 15): 1.5, date(2026, 6, 19): 1.2})


@pytest.fixture()
def pg(market):
    """A private schema with the writer's tables and partial-bar history; yields an engine factory."""
    url = _pg_url()
    schema = "etfr_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engines = []

    def _engine():
        engines.append(create_engine(url, connect_args={"options": f"-csearch_path={schema}"}))
        return engines[-1]

    first = _engine()
    with first.begin() as conn:
        for ddl in s.DDL_PG:
            conn.execute(text(ddl))
    s.seed_base(first)
    for d in market.sessions:
        if FROM <= d <= date(2026, 7, 2):
            price = float(market.raw[pd.Timestamp(d)]) * 1.01
            m.seed_row(first, "SPY", d, m.old_writer_values(market, d, (0.3, price), seen_on=d), m.ny(d, 14, 30))
    yield _engine
    for engine in engines:
        engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _plan(engine, market):
    return mod.build_plan(engine, m.FakeYF(market), from_date=FROM, session_day=SESSION_DAY)


def _repair_rows(engine, run_id: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text(
            "SELECT COUNT(*) FROM raw_series WHERE raw_payload->'repair'->>'run_id' = :r"), {"r": run_id}).scalar()


def _broken_chains(engine) -> list[tuple]:
    """(series, date) whose vintages do not each revise the one written before."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT series_id, obs_date, value, raw_payload->>'revises_value' FROM raw_series "
            "ORDER BY series_id, obs_date, pull_timestamp, id")).fetchall()
    broken, prev = [], {}
    for sid, d, value, revises in rows:
        key = (sid, d)
        if key in prev and (revises is None or float(revises) != prev[key]):
            broken.append(key)
        prev[key] = float(value)
    return broken


MEET_SERIES = "ETF_FLOW:SPY:5d"


def _meet_after_first_read(monkeypatch, barrier: threading.Barrier) -> None:
    """Each thread, at its first read of ``MEET_SERIES``, waits for the other (or 1.5 s).

    One series for both, so without the lock the two reads of it overlap
    every time; with it, the second read waits for the first writer's commit.
    """
    paused = threading.local()

    def _reads_meet_series(a) -> bool:
        return any(x == MEET_SERIES or (isinstance(x, list) and MEET_SERIES in x) for x in a)

    def _wrap(real):
        def _inner(*a, **kw):
            out = real(*a, **kw)
            if _reads_meet_series(a) and not getattr(paused, "done", False):
                paused.done = True
                try:
                    barrier.wait()
                except threading.BrokenBarrierError:
                    pass
            return out
        return _inner

    monkeypatch.setattr(mod, "newest_vintages", _wrap(mod.newest_vintages))
    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_latest_stored_etf_values",
                        _wrap(inst.InstitutionalFlowsPuller._latest_stored_etf_values))


def _no_lock(monkeypatch) -> None:
    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_serialize_etf_write",
                        staticmethod(lambda conn, budget=None: None))


def _run_both(jobs: dict) -> dict:
    out: dict = {}

    def _go(name, fn):
        try:
            out[name] = fn()
        except Exception as exc:                          # surfaced by the assertions
            out[name] = exc

    threads = [threading.Thread(target=_go, args=(n, f), name=n) for n, f in jobs.items()]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads)
    for name, value in out.items():
        assert not isinstance(value, Exception), (name, value)
    return out


# ── Planning reads only ──────────────────────────────────────────────────


def test_planning_reads_in_a_transaction_that_refuses_writes(pg, market):
    engine = pg()
    with pytest.raises(DBAPIError, match="read-only"):
        with mod.read_only_connection(engine) as conn:
            conn.execute(text("INSERT INTO source_catalog (id, name) VALUES (99, 'x')"))
    with engine.connect() as conn:
        before = conn.execute(text("SELECT COUNT(*), MAX(id) FROM raw_series")).one()
    plan = _plan(engine, market)
    assert plan["summary"]["rows"] > 0
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*), MAX(id) FROM raw_series")).one() == before


# ── A second writer during the repair ────────────────────────────────────


def test_the_scheduled_writer_waits_for_the_repair_and_writes_after_it(pg, market, monkeypatch):
    plan = _plan(pg(), market)
    m.install_yfinance(monkeypatch, m.FakeYF(market))
    repair_holds_lock = threading.Event()
    release = threading.Event()
    real = mod.newest_vintages

    def _hold(*a, **kw):
        out = real(*a, **kw)
        if threading.current_thread().name == "repair" and not repair_holds_lock.is_set():
            repair_holds_lock.set()
            release.wait(timeout=3)
        return out

    monkeypatch.setattr(mod, "newest_vintages", _hold)

    def _writer():
        repair_holds_lock.wait(timeout=10)
        with pg().connect() as conn:                     # the clock the rows are dated by
            started = conn.execute(text("SELECT clock_timestamp()")).scalar()
        threading.Timer(1.0, release.set).start()
        return started, inst.InstitutionalFlowsPuller(pg()).pull_etf_recent()

    out = _run_both({"repair": lambda: mod.run_writes(pg(), plan, session_day=SESSION_DAY), "writer": _writer})
    assert out["repair"]["status"] == "DONE"
    assert out["repair"]["totals"] == {"written": len(plan["items"])}
    started, results = out["writer"]
    assert {r["status"] for r in results} == {"SUCCESS"}
    engine = pg()
    with engine.connect() as conn:
        repair_last = conn.execute(text(
            "SELECT MAX(pull_timestamp) FROM raw_series WHERE raw_payload->'repair'->>'run_id' = :r "
            "AND series_id = 'ETF_FLOW:SPY:20d'"), {"r": plan["run_id"]}).scalar()
        writer_rows = conn.execute(text(
            "SELECT obs_date, COUNT(*), MIN(pull_timestamp) FROM raw_series WHERE obs_date >= :f "
            "AND series_id = 'ETF_FLOW:SPY:20d' GROUP BY obs_date"), {"f": SESSION_DAY - timedelta(days=14)}).fetchall()
    assert writer_rows and all(n == 1 for _, n, _ in writer_rows)          # each recent date once
    first_writer = min(t for _, _, t in writer_rows)
    assert first_writer > repair_last                                         # dated after the repair's rows
    assert first_writer >= started + timedelta(seconds=0.9)                   # it waited for the lock
    assert mod.verify_plan(engine, plan)["ok"]
    assert _broken_chains(engine) == []


@pytest.mark.parametrize("locked", [True, False], ids=["one_writer_at_a_time", "control_no_lock"])
def test_a_writer_revision_and_the_repair_never_build_on_the_same_read(pg, market, monkeypatch, locked):
    """The authorized wide revision (``revise_from``) and the repair, on the same old dates at once."""
    plan = _plan(pg(), market)
    m.install_yfinance(monkeypatch, m.FakeYF(market))
    if not locked:
        _no_lock(monkeypatch)
    _meet_after_first_read(monkeypatch, threading.Barrier(2, timeout=1.5))
    out = _run_both({
        "repair": lambda: mod.run_writes(pg(), plan, session_day=SESSION_DAY),
        "writer": lambda: inst.InstitutionalFlowsPuller(pg()).pull_etf_only(start_date=FROM, revise_from=FROM),
    })
    assert out["repair"]["status"] == "DONE"
    broken = _broken_chains(pg())
    if locked:
        assert broken == []
        totals = out["repair"]["totals"]
        assert totals.get("written", 0) + totals.get("superseded", 0) == len(plan["items"])
    else:
        assert broken                                       # two rows revising the same vintage


@pytest.mark.parametrize("locked", [True, False], ids=["one_writer_at_a_time", "control_no_lock"])
def test_two_repairs_of_one_plan_write_each_row_once(pg, market, monkeypatch, locked):
    plan = _plan(pg(), market)
    if not locked:
        _no_lock(monkeypatch)
    _meet_after_first_read(monkeypatch, threading.Barrier(2, timeout=1.5))
    _run_both({name: (lambda: mod.run_writes(pg(), plan, session_day=SESSION_DAY)) for name in ("a", "b")})
    written = _repair_rows(pg(), plan["run_id"])
    if locked:
        assert written == len(plan["items"])
        assert mod.verify_plan(pg(), plan)["ok"]
    else:
        assert written > len(plan["items"])
        assert not mod.verify_plan(pg(), plan)["ok"]


# ── What a read for a past instant sees ──────────────────────────────────


def test_rows_are_dated_when_written_and_a_past_instant_still_reads_the_old_value(pg, market):
    engine = pg()
    plan = _plan(engine, market)
    with engine.connect() as conn:
        before = conn.execute(text("SELECT clock_timestamp()")).scalar()
    mod.run_writes(engine, plan, session_day=SESSION_DAY)
    with engine.connect() as conn:
        after = conn.execute(text("SELECT clock_timestamp()")).scalar()
        first_repair = conn.execute(text(
            "SELECT MIN(pull_timestamp) FROM raw_series WHERE raw_payload ? 'repair'")).scalar()
    assert before < first_repair < after
    item = next(i for i in plan["items"] if i["metric"] == "5d")
    d = date.fromisoformat(item["obs_date"])
    today = after.astimezone(timezone.utc).date()
    with engine.connect() as conn:
        then = read_window_known_at(conn, "ETF_FLOW:SPY:5d", as_of=today, lag=None, start=d, as_of_ts=before)
        now = read_window_known_at(conn, "ETF_FLOW:SPY:5d", as_of=today, lag=None, start=d, as_of_ts=after)
    assert {o.obs_date: o.value for o in then}[d] == item["revises_value"]
    assert {o.obs_date: o.value for o in now}[d] == item["value"]
