"""Tiingo daily prices: incremental, set-based, single-flight, off the scheduler loop.

Stale-sources audit 2026-09-29, root cause (2). The Tiingo price pull sat in
grid-scheduler's sequential weekday "daily" group and took 10-14h per run:
its "incremental" start came from a source-wide MAX(obs_date) that hit the
120s statement timeout every run and fell back to 1990-01-01, so every
ticker re-pulled its full history through a per-row INSERT. Everything
after it in the group, and every later schedule job, waited.

All engines, HTTP responses and pullers here are local fakes: no network,
no production database. The real-PostgreSQL proof of the set-based insert
and the advisory lock is tests/test_tiingo_prices_pg.py.
"""

from __future__ import annotations

import inspect
import threading
import time
from datetime import date, datetime, timedelta, timezone
from types import ModuleType
from unittest.mock import MagicMock

import pytest
import schedule
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

import ingestion.scheduler as sched
import ingestion.tiingo_pull as tp
from ingestion.tiingo_pull import TiingoPuller, _RateLimiter, expected_latest_session


# ── helpers ──────────────────────────────────────────────────────────────


def _bare_puller() -> TiingoPuller:
    """A TiingoPuller without BasePuller's DB-backed __init__."""
    puller = TiingoPuller.__new__(TiingoPuller)
    puller.engine = MagicMock()
    puller.source_id = 524
    return puller


class _FakeIncremental(TiingoPuller):
    """pull_incremental with DB lookups, the lock and HTTP replaced."""

    def __init__(self, latest: dict[str, date | None], *, lock_free: bool = True,
                 fetch_delay_s: float = 0.0, lookup_error: set[str] | None = None) -> None:
        self.engine = MagicMock()
        self.source_id = 524
        self.latest = latest
        self.lock_free = lock_free
        self.fetch_delay_s = fetch_delay_s
        self.lookup_error = lookup_error or set()
        self.fetched: list[tuple[str, str]] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self._mu = threading.Lock()
        self.released = False

    def _latest_tiingo_obs(self, ticker: str) -> date | None:
        if ticker in self.lookup_error:
            raise RuntimeError("statement timeout")
        return self.latest.get(ticker)

    def _try_acquire_advisory_lock(self):
        return object() if self.lock_free else None

    def _release_advisory_lock(self, conn) -> None:
        self.released = True

    def pull_ticker(self, ticker, start_date="2020-01-01", end_date=None, limiter=None):
        with self._mu:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            self.fetched.append((ticker, str(start_date)))
        time.sleep(self.fetch_delay_s)
        with self._mu:
            self.in_flight -= 1
        return {"ticker": ticker, "status": "SUCCESS", "rows_inserted": 6, "errors": []}


# Tuesday 2026-09-29, 20:00 ET (EDT): that day's EOD bar should exist.
_AFTER_CLOSE = datetime(2026, 9, 30, 0, 0, tzinfo=timezone.utc)
_SESSION = date(2026, 9, 29)


# ── expected_latest_session ──────────────────────────────────────────────


@pytest.mark.parametrize("now, expected", [
    (datetime(2026, 9, 29, 21, 30, tzinfo=timezone.utc), date(2026, 9, 28)),  # 17:30 EDT: not yet
    (datetime(2026, 9, 29, 22, 5, tzinfo=timezone.utc), date(2026, 9, 29)),   # 18:05 EDT
    (datetime(2026, 12, 1, 22, 30, tzinfo=timezone.utc), date(2026, 11, 30)),  # 17:30 EST: not yet
    (datetime(2026, 12, 1, 23, 30, tzinfo=timezone.utc), date(2026, 12, 1)),   # 18:30 EST (the slot)
    (datetime(2026, 10, 3, 15, 0, tzinfo=timezone.utc), date(2026, 10, 2)),    # Saturday -> Friday
    (datetime(2026, 9, 29, 3, 0, tzinfo=timezone.utc), date(2026, 9, 28)),     # Mon 23:00 EDT
])
def test_expected_latest_session(now, expected) -> None:
    assert expected_latest_session(now) == expected


def test_scheduler_slot_is_after_the_eod_cutoff_in_both_edt_and_est() -> None:
    hh, mm = (int(x) for x in sched.TIINGO_PRICES_SLOT_UTC.split(":"))
    for day in (date(2026, 7, 1), date(2026, 12, 2)):  # EDT, EST (both Wednesdays)
        at = datetime(day.year, day.month, day.day, hh, mm, tzinfo=timezone.utc)
        assert expected_latest_session(at) == day


# ── pull_incremental ─────────────────────────────────────────────────────


def test_incremental_skips_current_and_never_falls_back_to_full_history() -> None:
    puller = _FakeIncremental({
        "AAPL": _SESSION,                    # already current -> no HTTP call
        "MSFT": _SESSION - timedelta(days=1),
        "NEWCO": None,                       # no TIINGO rows yet
    }, lookup_error={"BROKEN"})
    out = puller.pull_incremental(["AAPL", "MSFT", "NEWCO", "BROKEN"], now=_AFTER_CLOSE)

    starts = dict(puller.fetched)
    assert "AAPL" not in starts
    assert starts["MSFT"] == (_SESSION - timedelta(days=1 + tp.ROUTINE_OVERLAP_DAYS)).isoformat()
    assert starts["NEWCO"] == tp.NEW_TICKER_START
    # A failed lookup must fail cheap -- a bounded recent window, never 1990.
    assert date.fromisoformat(starts["BROKEN"]) >= _SESSION - timedelta(days=60)
    assert all(not s.startswith("19") for s in starts.values())
    assert out["status"] == "SUCCESS"
    assert out["current"] == 1 and out["fetched"] == 3 and out["rows_inserted"] == 18
    assert puller.released


def test_incremental_is_skipped_while_another_process_holds_the_lock() -> None:
    puller = _FakeIncremental({"AAPL": None}, lock_free=False)
    out = puller.pull_incremental(["AAPL"], now=_AFTER_CLOSE)
    assert out["status"] == "SKIPPED"
    assert puller.fetched == []


def test_incremental_is_single_flight_within_a_process() -> None:
    first = _FakeIncremental({f"T{i}": None for i in range(6)}, fetch_delay_s=0.2)
    second = _FakeIncremental({"AAPL": None})
    box: dict = {}
    t = threading.Thread(target=lambda: box.update(
        out=first.pull_incremental(list(first.latest), now=_AFTER_CLOSE, max_workers=1)))
    t.start()
    time.sleep(0.05)
    assert second.pull_incremental(["AAPL"], now=_AFTER_CLOSE)["status"] == "SKIPPED"
    t.join()
    assert box["out"]["status"] == "SUCCESS"


def test_incremental_concurrency_is_bounded() -> None:
    puller = _FakeIncremental({f"T{i}": None for i in range(12)}, fetch_delay_s=0.05)
    puller.pull_incremental(list(puller.latest), now=_AFTER_CLOSE, max_workers=3)
    assert len(puller.fetched) == 12
    assert 1 < puller.max_in_flight <= 3


def test_incremental_stops_cleanly_on_budget_and_reports_partial() -> None:
    puller = _FakeIncremental({f"T{i}": None for i in range(10)})
    calls = {"n": 0}

    def budget() -> bool:
        calls["n"] += 1
        return calls["n"] <= 4

    out = puller.pull_incremental(list(puller.latest), now=_AFTER_CLOSE,
                                  max_workers=1, should_continue=budget)
    assert out["status"] == "PARTIAL"
    assert out["fetched"] == 4 and out["unattempted"] == 6


def test_incremental_reports_failed_when_most_fetches_fail() -> None:
    class _Failing(_FakeIncremental):
        def pull_ticker(self, ticker, start_date="2020-01-01", end_date=None, limiter=None):
            return {"ticker": ticker, "status": "FAILED", "rows_inserted": 0, "errors": ["401"]}

    puller = _Failing({f"T{i}": None for i in range(12)})
    out = puller.pull_incremental(list(puller.latest), now=_AFTER_CLOSE)
    assert out["status"] == "FAILED" and "12/12" in out["error"]


def test_rate_limiter_spaces_request_starts_across_threads() -> None:
    limiter = _RateLimiter(0.05)
    stamps: list[float] = []
    mu = threading.Lock()

    def hit() -> None:
        limiter.wait()
        with mu:
            stamps.append(time.monotonic())

    threads = [threading.Thread(target=hit) for _ in range(6)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    stamps.sort()
    gaps = [b - a for a, b in zip(stamps, stamps[1:])]
    assert min(gaps) >= 0.04


def test_pull_ticker_retries_http_429(monkeypatch) -> None:
    responses = [
        MagicMock(status_code=429, headers={"Retry-After": "0"}),
        MagicMock(status_code=200, headers={}),
    ]
    responses[1].json.return_value = [
        {"date": "2026-09-29T00:00:00.000Z", "open": 1.0, "high": 2.0, "low": 0.5,
         "close": 1.5, "volume": 10, "adjClose": 1.5},
    ]
    responses[1].raise_for_status.return_value = None
    monkeypatch.setattr(tp.requests, "get", MagicMock(side_effect=responses))
    monkeypatch.setattr(tp.time, "sleep", lambda _s: None)
    puller = _bare_puller()
    puller._insert_rows = MagicMock(return_value=6)
    out = puller.pull_ticker("AAPL", start_date="2026-09-20")
    assert out["status"] == "SUCCESS" and out["rows_inserted"] == 6
    assert tp.requests.get.call_count == 2


def test_insert_rows_is_one_set_based_statement_with_the_same_dedupe_rule() -> None:
    puller = _bare_puller()
    conn = puller.engine.begin.return_value.__enter__.return_value
    conn.execute.return_value.rowcount = 2
    rows = [
        {"sid": "YF:AAPL:close", "src": 524, "od": date(2026, 9, 28), "val": 1.0},
        {"sid": "YF:AAPL:close", "src": 524, "od": date(2026, 9, 29), "val": 2.0},
        {"sid": "YF:AAPL:open", "src": 524, "od": date(2026, 9, 29), "val": 3.0},
    ]
    assert puller._insert_rows(rows) == 2
    conn.execute.assert_called_once()
    sql = " ".join(str(conn.execute.call_args[0][0]).split())
    params = conn.execute.call_args[0][1]
    assert "unnest(" in sql and "DISTINCT ON (u.sid, u.od)" in sql
    assert ("WHERE NOT EXISTS ( SELECT 1 FROM raw_series r WHERE r.series_id = v.sid "
            "AND r.source_id = :src AND r.obs_date = v.od AND r.pull_status = 'SUCCESS')") in sql
    assert params["src"] == 524
    assert params["sids"] == ["YF:AAPL:close", "YF:AAPL:close", "YF:AAPL:open"]
    assert params["ods"] == [date(2026, 9, 28), date(2026, 9, 29), date(2026, 9, 29)]
    assert params["vals"] == [1.0, 2.0, 3.0]


def test_insert_rows_chunks_large_histories(monkeypatch) -> None:
    monkeypatch.setattr(tp, "_INSERT_CHUNK_ROWS", 2)
    puller = _bare_puller()
    conn = puller.engine.begin.return_value.__enter__.return_value
    conn.execute.return_value.rowcount = 1
    rows = [{"sid": "YF:X:close", "od": date(2026, 9, d), "val": float(d)} for d in range(1, 6)]
    assert puller._insert_rows(rows) == 3
    assert conn.execute.call_count == 3
    puller.engine.begin.assert_called_once()  # one transaction


# ── grid-scheduler wiring ────────────────────────────────────────────────


def test_tiingo_prices_is_not_in_the_sequential_daily_group() -> None:
    source = inspect.getsource(sched._get_pullers_for_group)
    assert "TiingoPuller" not in source
    assert '"Tiingo_Prices"' not in source


class _SchedEngine:
    def dispose(self) -> None:
        pass


def _registered_jobs(monkeypatch) -> schedule.Scheduler:
    import sys

    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: _SchedEngine()
    monkeypatch.setitem(sys.modules, "db", fake_db)
    jobs = schedule.Scheduler()
    monkeypatch.setattr(sched, "schedule", jobs)
    monkeypatch.setattr(sched.time, "sleep", lambda _s: None)

    def _stop():
        raise KeyboardInterrupt()

    monkeypatch.setattr(jobs, "run_pending", _stop)
    sched.start_scheduler()
    return jobs


def test_tiingo_has_its_own_weekday_utc_slot(monkeypatch) -> None:
    jobs = _registered_jobs(monkeypatch)
    tiingo = [j for j in jobs.jobs if j.job_func.func is sched.start_tiingo_prices_worker]
    assert sorted(j.start_day for j in tiingo) == sorted(
        ["monday", "tuesday", "wednesday", "thursday", "friday"])
    for j in tiingo:
        assert j.at_time.strftime("%H:%M") == sched.TIINGO_PRICES_SLOT_UTC
        assert j.at_time_zone.zone == "UTC"
    daily_groups = [j for j in jobs.jobs
                    if j.job_func.func is sched.run_pull_group and j.job_func.args[0] == "daily"]
    assert len(daily_groups) == 5
    domestic = [j for j in jobs.jobs if j.job_func.func is sched.run_daily_pulls]
    assert len(domestic) == 28
    assert {j.job_func.keywords["slot"] for j in domestic} == {"13:30", "16:00", "20:00", "22:00"}


def test_slow_tiingo_does_not_block_the_options_slot_or_other_jobs(monkeypatch) -> None:
    """A Tiingo run that takes 'hours' must not hold the schedule loop."""
    jobs = _registered_jobs(monkeypatch)
    tiingo_job = next(j for j in jobs.jobs if j.job_func.func is sched.start_tiingo_prices_worker)
    options_job = next(j for j in jobs.jobs if j.job_func.func is sched.run_daily_pulls
                       and j.job_func.keywords["slot"] == "13:30")
    group_job = next(j for j in jobs.jobs if j.job_func.func is sched.run_pull_group
                     and j.job_func.args[0] == "daily")
    release = threading.Event()
    started = threading.Event()
    ran: list[str] = []

    class _SlowTiingo:
        def pull_incremental(self):
            started.set()
            release.wait(10)
            return {"status": "SUCCESS", "rows_inserted": 0}

    monkeypatch.setattr(sched, "run_tiingo_prices",
                        lambda engine, puller=None: _SlowTiingo().pull_incremental())
    monkeypatch.setattr(sched, "run_daily_pulls",
                        lambda start_date=None, slot=None: ran.append(f"domestic@{slot}"))
    monkeypatch.setattr(sched, "run_pull_group",
                        lambda group, engine, **_kw: ran.append(f"group:{group}"))
    monkeypatch.setattr(sched, "_tiingo_worker", None)

    # Call each registered job's target with its registered arguments (the
    # module-level callables are the monkeypatched fakes above).
    t0 = time.monotonic()
    assert sched.start_tiingo_prices_worker(tiingo_job.job_func.args[0]) is True
    assert time.monotonic() - t0 < 1.0          # returned at once
    assert started.wait(2)                       # and Tiingo really is running
    sched.run_daily_pulls(**options_job.job_func.keywords)
    sched.run_pull_group("daily", group_job.job_func.args[1])
    assert ran == ["domestic@13:30", "group:daily"]
    assert sched._tiingo_worker.is_alive()       # still mid-pull while they ran
    # A second slot while the first worker is alive starts nothing.
    assert sched.start_tiingo_prices_worker(tiingo_job.job_func.args[0]) is False
    release.set()
    sched._tiingo_worker.join(5)
    assert not sched._tiingo_worker.is_alive()


# ── run_tiingo_prices bookkeeping (real SQLite) ──────────────────────────


def _sqlite():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=StaticPool)

    @event.listens_for(engine, "connect")
    def _now(dbapi_conn, _rec) -> None:
        dbapi_conn.create_function(
            "NOW", 0, lambda: datetime.now(timezone.utc).isoformat(sep=" "))

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, "
                          "name TEXT, last_pull_at TIMESTAMP)"))
        conn.execute(text("CREATE TABLE pull_log (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                          "puller_name TEXT, source_id INTEGER, started_at TIMESTAMP, "
                          "completed_at TIMESTAMP, status TEXT, rows_inserted INTEGER, "
                          "error_message TEXT, node_name TEXT)"))
        conn.execute(text("INSERT INTO source_catalog (id, name) VALUES (524, 'TIINGO')"))
    return engine


class _Result:
    SOURCE_NAME = "TIINGO"

    def __init__(self, out):
        self.out = out

    def pull_incremental(self):
        return self.out


@pytest.mark.parametrize("out, log_status, bumped", [
    ({"status": "SUCCESS", "rows_inserted": 7968}, "SUCCESS", True),
    ({"status": "PARTIAL", "rows_inserted": 12}, "PARTIAL", False),
    ({"status": "FAILED", "rows_inserted": 0, "error": "900/1000 failed"}, "FAILED", False),
    ({"status": "SKIPPED", "skipped_reason": "lock held"}, None, False),
])
def test_run_tiingo_prices_records_outcome(out, log_status, bumped) -> None:
    engine = _sqlite()
    sched.run_tiingo_prices(engine, puller=_Result(out))
    with engine.connect() as conn:
        logs = conn.execute(text("SELECT puller_name, source_id, status, rows_inserted "
                                 "FROM pull_log")).fetchall()
        last = conn.execute(text("SELECT last_pull_at FROM source_catalog")).scalar()
    if log_status is None:
        assert logs == []
    else:
        assert [(r[0], r[1], r[2]) for r in logs] == [("Tiingo_Prices", 524, log_status)]
        assert logs[0][3] == out.get("rows_inserted", 0)
    assert (last is not None) is bumped


# ── domestic slot lateness guard ─────────────────────────────────────────


def test_slot_lateness() -> None:
    assert sched._slot_lateness("13:30", datetime(2026, 9, 29, 13, 45)) == timedelta(minutes=15)
    # A 22:00 slot released at 07:30 the next morning is 9.5h late.
    assert sched._slot_lateness("22:00", datetime(2026, 9, 30, 7, 30)) == timedelta(hours=9, minutes=30)


def test_run_daily_pulls_skips_a_grossly_late_slot(monkeypatch) -> None:
    import ingestion.market_calendar as mc

    monkeypatch.setattr(sched, "_slot_lateness", lambda slot, now=None: timedelta(hours=9))
    monkeypatch.setattr(mc, "is_market_open", MagicMock(side_effect=AssertionError("ran")))
    sched.run_daily_pulls(start_date="2026-09-29", slot="22:00")  # returns without pulling
