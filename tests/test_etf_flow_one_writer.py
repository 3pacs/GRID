"""The ETF flow writer: a short bounded run, and one writer at a time.

Codex review 6085724628 on the 05:35Z slot (GRID #1501) accepted the slot and
its session gate and named two things it would not accept on source alone:

1. The slot ran ``pull_etf_only()``: eleven downloads of every bar since 2020,
   with no deadline, on the scheduler's one thread. ``pull_etf_recent`` is the
   short run (the revision window) and takes the caller's deadline.
2. The writer reads a series' stored values and then inserts, and two services
   run it. ``raw_series``' unique key includes ``pull_timestamp``, so it does
   not stop a date being stored twice, and two revisions could each be built
   on the same stale read. The read-and-insert now runs under a
   transaction-level advisory lock, and its rows are dated with the database
   clock read after the wait.

The second half is shown on PostgreSQL with two real sessions; each of those
cases has a control with the lock taken away that shows the fault.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import types
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from ingestion.altdata import institutional_flows as inst
from tests import thirteenf_holdings_support as s

TODAY = date(2026, 11, 20)            # a Friday; New York's current date
LAST = date(2026, 11, 19)             # the newest finished session
SERIES_5D = "ETF_FLOW:SPY:5d"
CLOSE = 100.0
FINAL_VOLUME = 1_000_000


def _bars(last_volume: int = FINAL_VOLUME, periods: int = 60) -> pd.DataFrame:
    """Finished sessions ending at ``LAST``; the last one has ``last_volume``."""
    days = pd.bdate_range(end=pd.Timestamp(LAST), periods=periods)
    volumes = [FINAL_VOLUME] * (len(days) - 1) + [last_volume]
    return pd.DataFrame({"Close": [CLOSE] * len(days), "Volume": volumes}, index=days)


def _five_day(last_volume: int) -> float:
    return (4 * FINAL_VOLUME + last_volume) * CLOSE


@pytest.fixture(autouse=True)
def _one_etf_no_pauses(monkeypatch):
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY"])
    monkeypatch.setattr(inst, "_YF_RATE_DELAY", 0)
    monkeypatch.setattr(inst, "_new_york_today", lambda: TODAY)


def _yfinance(monkeypatch, download) -> None:
    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(download=download))


@pytest.fixture()
def engine():
    return s.make_sqlite_engine()


def _dates(engine, series_id: str = SERIES_5D) -> list[date]:
    with engine.connect() as conn:
        found = conn.execute(text(
            "SELECT obs_date FROM raw_series WHERE series_id = :sid ORDER BY obs_date, id"),
            {"sid": series_id}).fetchall()
    return [d if isinstance(d, date) else date.fromisoformat(str(d)[:10]) for (d,) in found]


# ── 1. The short run ───────────────────────────────────────────────────────


def test_the_short_run_fetches_six_weeks_and_stores_only_the_revision_window(engine, monkeypatch):
    asked: list[dict] = []

    def _download(ticker, **kwargs):
        asked.append({"ticker": ticker, **kwargs})
        return _bars()

    _yfinance(monkeypatch, _download)

    results = inst.InstitutionalFlowsPuller(engine).pull_etf_recent()

    window_start = TODAY - timedelta(days=inst._ETF_REVISION_DAYS)
    assert [a["start"] for a in asked] == [str(window_start - timedelta(days=30))]   # not 2020
    stored = _dates(engine)
    assert stored and min(stored) >= window_start and max(stored) == LAST
    assert len(stored) == len(set(stored)) == 10          # ten sessions in fourteen days
    assert {r["status"] for r in results} == {"SUCCESS"}


def test_the_wide_run_still_reaches_back(engine, monkeypatch):
    """``pull_etf_only`` is unchanged: the 20:00Z run fills what the short one leaves."""
    asked: list[str] = []
    _yfinance(monkeypatch, lambda ticker, **kw: asked.append(kw["start"]) or _bars())

    inst.InstitutionalFlowsPuller(engine).pull_etf_only()

    assert asked == [str(date(2020, 1, 1) - timedelta(days=30))]
    assert min(_dates(engine)) < TODAY - timedelta(days=inst._ETF_REVISION_DAYS)


def test_the_short_run_starts_no_etf_after_its_deadline(engine, monkeypatch):
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY", "QQQ", "IWM"])
    clock = {"now": 1000.0}
    monkeypatch.setattr(inst, "_monotonic", lambda: clock["now"])
    asked: list[str] = []

    def _download(ticker, **_kwargs):
        asked.append(ticker)
        clock["now"] += 200.0                              # each ETF takes 200 s
        return _bars()

    _yfinance(monkeypatch, _download)

    results = inst.InstitutionalFlowsPuller(engine).pull_etf_recent(deadline_at=1300.0)

    assert asked == ["SPY", "QQQ"]                         # IWM would start at 1400
    (stop,) = [r for r in results if r.get("stopped_by_budget")]
    assert stop["status"] == "PARTIAL"
    assert (stop["items_read"], stop["items_not_attempted"]) == (2, 1)
    assert _dates(engine, "ETF_FLOW:QQQ:5d") and not _dates(engine, "ETF_FLOW:IWM:5d")


def test_a_deadline_already_past_downloads_nothing(engine, monkeypatch):
    monkeypatch.setattr(inst, "_monotonic", lambda: 2000.0)
    _yfinance(monkeypatch, lambda *_a, **_k: pytest.fail("no download may start after the deadline"))

    (stop,) = inst.InstitutionalFlowsPuller(engine).pull_etf_recent(deadline_at=1999.0)

    assert stop["stopped_by_budget"] is True and stop["items_read"] == 0
    assert _dates(engine) == []


# ── 2. The lock and the receipt time, without a database ───────────────────


class _Conn:
    """Records what ``_serialize_etf_write`` sends; answers like PostgreSQL."""

    def __init__(self, dialect: str = "postgresql", lock_error: Exception | None = None) -> None:
        self.dialect = types.SimpleNamespace(name=dialect)
        self.lock_error = lock_error
        self.sent: list[tuple[str, dict]] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.sent.append((sql, dict(params or {})))
        if "pg_advisory_xact_lock" in sql and self.lock_error is not None:
            raise self.lock_error
        answer = "0" if "current_setting" in sql else datetime(2026, 11, 20, 5, 40, tzinfo=timezone.utc)
        return types.SimpleNamespace(scalar=lambda: answer)


def _budget_with(seconds_left: float | None):
    return types.SimpleNamespace(time_left=lambda: seconds_left)


def test_other_dialects_take_no_lock_and_keep_the_default_timestamp():
    conn = _Conn(dialect="sqlite")
    assert inst.InstitutionalFlowsPuller._serialize_etf_write(conn) is None
    assert conn.sent == []


def test_the_lock_is_transaction_level_and_the_receipt_time_is_read_after_it():
    conn = _Conn()
    received = inst.InstitutionalFlowsPuller._serialize_etf_write(conn)

    sent = [sql for sql, _ in conn.sent]
    lock = next(i for i, sql in enumerate(sent) if "pg_advisory_xact_lock" in sql)
    clock = next(i for i, sql in enumerate(sent) if "clock_timestamp()" in sql)
    assert lock < clock and received == datetime(2026, 11, 20, 5, 40, tzinfo=timezone.utc)
    assert conn.sent[lock][1] == {"k": inst._ETF_WRITE_LOCK_KEY}
    assert inst._ETF_WRITE_LOCK_KEY != inst._13F_RUN_LOCK_KEY
    assert not any("pg_advisory_lock(" in sql or "pg_advisory_unlock" in sql for sql in sent)
    # lock_timeout is set for the wait only and put back for the rest of the transaction.
    timeouts = [params["w"] for sql, params in conn.sent if "set_config('lock_timeout'" in sql]
    assert timeouts == [f"{inst._ETF_WRITE_LOCK_WAIT_S}s", "0"]


@pytest.mark.parametrize("left, wait", [(None, "30s"), (500.0, "30s"), (7.9, "7s"), (0.2, "1s")])
def test_the_wait_is_never_longer_than_what_the_run_has_left(left, wait):
    conn = _Conn()
    inst.InstitutionalFlowsPuller._serialize_etf_write(conn, _budget_with(left))
    assert [p["w"] for sql, p in conn.sent if "set_config('lock_timeout'" in sql][0] == wait


def test_a_wait_that_runs_out_is_reported_as_another_writer_and_other_errors_are_not():
    busy = RuntimeError("lock timeout")
    busy.orig = types.SimpleNamespace(pgcode="55P03")
    with pytest.raises(inst._EtfWriteLockBusy, match="another ETF flow writer held the write lock"):
        inst.InstitutionalFlowsPuller._serialize_etf_write(_Conn(lock_error=busy))

    other = RuntimeError("server closed the connection")
    other.orig = types.SimpleNamespace(pgcode="08006")
    with pytest.raises(RuntimeError) as raised:
        inst.InstitutionalFlowsPuller._serialize_etf_write(_Conn(lock_error=other))
    assert raised.value is other


def test_insert_raw_stores_the_receipt_time_it_is_given_and_is_unchanged_without_one(engine):
    puller = inst.InstitutionalFlowsPuller(engine)
    given = datetime(2026, 11, 20, 5, 40, 1)
    with engine.begin() as conn:
        puller._insert_raw(conn, "T:explicit", LAST, 1.0, received_at=given)
        puller._insert_raw(conn, "T:default", LAST, 1.0)
        explicit, default = (conn.execute(text(
            "SELECT pull_timestamp FROM raw_series WHERE series_id = :sid"), {"sid": sid}).scalar()
            for sid in ("T:explicit", "T:default"))
    assert str(explicit).startswith("2026-11-20 05:40:01")
    assert default is not None and not str(default).startswith("2026-11-20 05:40:01")

    sent: list[str] = []
    recorder = types.SimpleNamespace(execute=lambda statement, params: sent.append(str(statement)))
    puller._insert_raw(recorder, "T:default", LAST, 1.0)
    assert "pull_timestamp" not in sent[0]                 # every other puller's statement is as it was


# ── 3. Two real sessions on PostgreSQL ─────────────────────────────────────


def _pg_url() -> str:
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get("REQUIRE_ETF_FLOWS_LOCK_PG") == "1":
            pytest.fail("GRID_TEST_DB_URL is required for the ETF write-lock PG proof (REQUIRE_ETF_FLOWS_LOCK_PG=1)")
        pytest.skip("GRID_TEST_DB_URL is required for the PostgreSQL lock proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("the ETF write-lock PG proof requires a local disposable test database")
    return url


@pytest.fixture()
def pg():
    """A private schema with the writer's tables; yields a factory of engines on it."""
    url = _pg_url()
    schema = "etfw_" + uuid4().hex[:12]
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
    yield _engine
    for engine in engines:
        engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _vintages(engine, obs_date: date = LAST) -> list[dict]:
    """Every stored row of the 5d series for one date, in the order written."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, value, pull_timestamp, raw_payload FROM raw_series "
            "WHERE series_id = :sid AND obs_date = :d ORDER BY id"),
            {"sid": SERIES_5D, "d": obs_date}).fetchall()
    return [{"id": r[0], "value": float(r[1]), "received": r[2],
             "revises": (r[3] if isinstance(r[3], dict) else json.loads(r[3])).get("revises_value")}
            for r in rows]


def _two_writers_at_once(pg, monkeypatch, frames: dict[str, pd.DataFrame], *, locked: bool) -> dict[str, list]:
    """Run the real writer in two sessions that both reach "what is stored?"
    before either inserts, when nothing keeps them apart.

    Each writer pauses at its first read of the stored values until the other
    is there too (or 1.5 s pass). With the lock only one can be inside the
    transaction, so the pause runs out and the other reads after its commit.
    """
    if not locked:
        monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_serialize_etf_write",
                            staticmethod(lambda conn, budget=None: None))
    _yfinance(monkeypatch, lambda *_a, **_k: frames[threading.current_thread().name].copy())
    both_reading = threading.Barrier(2, timeout=1.5)
    paused = threading.local()
    real_read = inst.InstitutionalFlowsPuller._latest_stored_etf_values

    def _read_then_meet(self, conn, series_id, floor):
        stored = real_read(self, conn, series_id, floor)
        if not getattr(paused, "done", False):
            paused.done = True
            try:
                both_reading.wait()
            except threading.BrokenBarrierError:
                pass
        return stored

    monkeypatch.setattr(inst.InstitutionalFlowsPuller, "_latest_stored_etf_values", _read_then_meet)
    out: dict[str, list] = {}

    def _run(name: str) -> None:
        try:
            out[name] = inst.InstitutionalFlowsPuller(pg()).pull_etf_recent()
        except Exception as exc:                           # surfaced by the caller's assertions
            out[name] = [{"status": "RAISED", "error": repr(exc)}]

    threads = [threading.Thread(target=_run, args=(name,), name=name) for name in frames]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert not any(thread.is_alive() for thread in threads)
    assert all(item["status"] == "SUCCESS" for items in out.values() for item in items), out
    return out


@pytest.mark.xdist_group("postgres")
@pytest.mark.parametrize("locked", [True, False], ids=["one_writer_at_a_time", "control_no_lock"])
def test_two_real_sessions_store_a_new_date_once(pg, monkeypatch, locked):
    out = _two_writers_at_once(pg, monkeypatch, {"hermes": _bars(), "scheduler": _bars()}, locked=locked)

    engine = pg()
    stored = _dates(engine)
    written = sorted(next(i for i in items if i["feature"] == SERIES_5D)["rows_inserted"] for items in out.values())
    if locked:
        assert len(stored) == len(set(stored)) == 10       # each date once
        assert written == [0, 10]                          # the second writer found them stored
    else:
        # The unique key (series, source, date, pull_timestamp) does not stop this.
        assert len(stored) == 20 and len(set(stored)) == 10
        assert written == [10, 10]


@pytest.mark.xdist_group("postgres")
@pytest.mark.parametrize("locked", [True, False], ids=["one_writer_at_a_time", "control_no_lock"])
def test_two_real_sessions_revising_the_same_date_build_on_each_other(pg, monkeypatch, locked):
    """The date was stored from a partial bar (300,000 shares). Two writers now
    see different volumes for it: 900,000 and the final 1,000,000."""
    _yfinance(monkeypatch, lambda *_a, **_k: _bars(last_volume=300_000))
    inst.InstitutionalFlowsPuller(pg()).pull_etf_recent()
    assert [v["value"] for v in _vintages(pg())] == [_five_day(300_000)]

    _two_writers_at_once(
        pg, monkeypatch,
        {"hermes": _bars(last_volume=900_000), "scheduler": _bars(last_volume=FINAL_VOLUME)},
        locked=locked,
    )

    first, second, third = _vintages(pg())
    assert first["value"] == _five_day(300_000) and second["revises"] == first["value"]
    assert {second["value"], third["value"]} == {_five_day(900_000), _five_day(FINAL_VOLUME)}
    if locked:
        assert third["revises"] == second["value"]         # built on what the other committed
        assert first["received"] < second["received"] < third["received"]
    else:
        assert third["revises"] == first["value"]          # both revised the same stale read


@pytest.mark.xdist_group("postgres")
def test_a_real_session_that_waited_is_dated_after_the_writer_it_waited_for(pg):
    """The waiting writer's transaction can be the older one. Its rows take
    the clock read after the wait, so the newest row by receipt time is the
    one written last."""
    older, newer = pg().connect(), pg().connect()
    try:
        older_tx = older.begin()
        older_started = older.execute(text("SELECT now()")).scalar()      # the older transaction's NOW()

        newer_tx = newer.begin()
        newer_received = inst.InstitutionalFlowsPuller._serialize_etf_write(newer)
        newer_tx.commit()                                                 # releases the lock

        older_received = inst.InstitutionalFlowsPuller._serialize_etf_write(older)
        assert older.execute(text("SELECT now()")).scalar() == older_started
        older_tx.commit()

        assert older_started < newer_received < older_received
    finally:
        older.close()
        newer.close()


@pytest.mark.xdist_group("postgres")
def test_a_real_session_that_cannot_get_the_lock_writes_nothing_and_says_so(pg, monkeypatch):
    monkeypatch.setattr(inst, "_ETF_WRITE_LOCK_WAIT_S", 1)
    _yfinance(monkeypatch, lambda *_a, **_k: _bars())
    holder = pg().connect()
    try:
        held = holder.begin()
        holder.execute(text("SELECT pg_advisory_xact_lock(:k)"), {"k": inst._ETF_WRITE_LOCK_KEY})

        (item,) = inst.InstitutionalFlowsPuller(pg()).pull_etf_recent()
        assert item["status"] == "FAILED" and item["feature"] == "ETF_FLOW:SPY"
        assert "another ETF flow writer held the write lock for over 1s" in item["error"]
        assert _dates(pg()) == []

        held.rollback()                                    # the lock goes with the transaction
        results = inst.InstitutionalFlowsPuller(pg()).pull_etf_recent()
        assert {r["status"] for r in results} == {"SUCCESS"} and len(_dates(pg())) == 10
    finally:
        holder.close()
