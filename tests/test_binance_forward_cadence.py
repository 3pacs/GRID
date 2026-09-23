"""Forward-only Binance scheduling and required-source failure tests.

All engines and provider bars are local fakes. No HTTP or production DB call.
"""

from __future__ import annotations

import builtins
import sys
from datetime import date, datetime, timedelta, timezone
from types import ModuleType
from zoneinfo import ZoneInfo

import pytest
import schedule

import ingestion.scheduler as sched


class _Result:
    def __init__(self, row=None):
        self.row = row

    def fetchone(self):
        return self.row

    def fetchall(self):
        return []


class _Connection:
    def __init__(self, engine):
        self.engine = engine
        self.finished_log = False

    def __enter__(self):
        if self.engine.fail_begin:
            raise ConnectionError("fake database unavailable")
        self.original_rows = [dict(row) for row in self.engine.rows]
        return self

    def __exit__(self, exc_type, *_args):
        if exc_type is None and self.finished_log and self.engine.fail_finish_commit:
            self.engine.rows = self.original_rows
            raise ConnectionError("fake finish commit failed")
        return False

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).lower().split())
        if "insert into pull_log" in sql:
            row = dict(params)
            row["status"] = "RUNNING" if "'running'" in sql else "FAILED"
            self.engine.rows.append(row)
            return _Result((len(self.engine.rows),))
        if "update pull_log set" in sql:
            self.finished_log = True
            if self.engine.missing_finish_row:
                return _Result()
            self.engine.rows[params["id"] - 1].update(dict(params))
            return _Result((params["id"],))
        if "select rows_inserted from pull_log" in sql:
            return _Result()
        if "update source_catalog set last_pull_at" in sql or "insert into event_bus" in sql:
            return _Result()
        if "from raw_series" in sql:
            return _Result()
        raise AssertionError(f"unexpected fake SQL: {sql[:80]}")


class _Engine:
    def __init__(self, fail_begin=False, missing_finish_row=False,
                 fail_finish_commit=False):
        self.fail_begin = fail_begin
        self.missing_finish_row = missing_finish_row
        self.fail_finish_commit = fail_finish_commit
        self.rows = []

    def begin(self):
        return _Connection(self)

    def connect(self):
        return _Connection(self)


def _raise(error):
    raise error


def _fake_schedule_clock(monkeypatch, clock, host_zone):
    """Give schedule a moving UTC clock and a controlled host-local zone."""
    real_datetime = datetime
    local_zone = ZoneInfo(host_zone)

    class _ClockDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            at = cls.fromtimestamp(clock["now"].timestamp(), tz or local_zone)
            return at if tz is not None else at.replace(tzinfo=None)

        def astimezone(self, tz=None):
            return super().astimezone(tz or local_zone)

    monkeypatch.setattr(schedule.datetime, "datetime", _ClockDateTime)
    return _ClockDateTime


def test_single_binance_owner_is_seven_day_utc_job(monkeypatch):
    engine = _Engine()
    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: engine
    monkeypatch.setitem(sys.modules, "db", fake_db)
    jobs = schedule.Scheduler()
    monkeypatch.setattr(sched, "schedule", jobs)
    monkeypatch.setattr(sched.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(jobs, "run_pending", lambda: _raise(KeyboardInterrupt()))

    sched.start_scheduler()

    crypto = [job for job in jobs.jobs if job.job_func.func is sched.run_daily_binance_close]
    assert len(crypto) == 1
    job = crypto[0]
    assert job.unit == "days" and job.interval == 1 and job.start_day is None
    assert job.at_time.hour == 20 and job.at_time.minute == 0
    assert job.at_time_zone.zone == "UTC"
    assert jobs.jobs.index(job) < min(
        i for i, other in enumerate(jobs.jobs)
        if other.job_func.func is sched.run_daily_pulls and other.at_time.hour == 20
    )
    assert not any(
        other.job_func.func is sched.run_pull_group and other.job_func.args[0] == "crypto"
        for other in jobs.jobs
    )

    calls = []
    monkeypatch.setattr(sched, "run_pull_group", lambda group, eng: calls.append((group, eng)))
    job.job_func()
    assert calls == [("crypto", engine)]


def test_crypto_registration_survives_extended_engine_setup_failure(monkeypatch):
    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: _raise(ConnectionError("fake database unavailable"))
    monkeypatch.setitem(sys.modules, "db", fake_db)
    jobs = schedule.Scheduler()
    monkeypatch.setattr(sched, "schedule", jobs)
    monkeypatch.setattr(sched.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(jobs, "run_pending", lambda: _raise(KeyboardInterrupt()))

    sched.start_scheduler()
    crypto = [job for job in jobs.jobs if job.job_func.func is sched.run_daily_binance_close]
    assert len(crypto) == 1
    failed = crypto[0].job_func()
    assert failed["failure_count"] == 1
    assert failed["results"][0]["status"] == "FAILED"
    assert failed["results"][0]["error"] == "engine_unavailable:ConnectionError"
    assert failed["results"][0]["completed_log_persisted"] is False
    assert failed["results"][0]["provider_write_state"] == "not_started"


@pytest.mark.parametrize("host_zone", ["UTC", "America/Los_Angeles"])
def test_registered_job_executes_seven_utc_days_with_competing_local_jobs(monkeypatch, host_zone):
    from ingestion.altdata import binance_puller

    real_binance = binance_puller.BinancePuller
    clock = {"now": datetime(2026, 9, 21, 19, tzinfo=timezone.utc)}
    clock_datetime = _fake_schedule_clock(monkeypatch, clock, host_zone)
    monkeypatch.setattr(binance_puller, "datetime", clock_datetime)
    engine = _Engine()
    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: engine
    monkeypatch.setitem(sys.modules, "db", fake_db)
    close_dates = []
    domestic_runs = []

    def bar(day):
        start = int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp() * 1000)
        return [start, "100", "110", "90", "105", "1", start + 86_400_000 - 1]

    class _FakeBinance:
        source_id = 123

        def pull(self):
            puller = real_binance.__new__(real_binance)
            puller.engine = engine
            puller.source_id = self.source_id
            yesterday = clock["now"].date() - timedelta(days=1)
            puller._fetch_klines = lambda _symbol: [bar(yesterday), bar(clock["now"].date())]
            puller._insert_raw = lambda **kwargs: close_dates.append(kwargs["obs_date"])
            return {"rows_inserted": puller._pull_klines("BTCUSDT")}

    monkeypatch.setattr(binance_puller, "BinancePuller", lambda _engine: _FakeBinance())
    fake_domestic = lambda **_kwargs: domestic_runs.append(clock["now"])
    monkeypatch.setattr(sched, "run_daily_pulls", fake_domestic)
    jobs = schedule.Scheduler()
    monkeypatch.setattr(sched, "schedule", jobs)
    real_run_pending = jobs.run_pending
    selected = []

    def run_selected_jobs():
        if not selected:
            crypto = [job for job in jobs.jobs
                      if job.job_func.func is sched.run_daily_binance_close]
            domestic = [job for job in jobs.jobs
                        if job.job_func.func is fake_domestic and job.at_time.hour == 20]
            assert len(crypto) == 1 and len(domestic) == 7
            jobs.jobs[:] = crypto + domestic
            selected.extend(jobs.jobs)
        real_run_pending()

    def advance_clock(seconds):
        if seconds == 90:
            clock["now"] = datetime(2026, 9, 21, 20, tzinfo=timezone.utc)
        elif seconds == 60:
            if len(engine.rows) == 7:
                raise KeyboardInterrupt()
            clock["now"] += timedelta(days=1)
        else:
            raise AssertionError(f"unexpected scheduler sleep: {seconds}")

    monkeypatch.setattr(jobs, "run_pending", run_selected_jobs)
    monkeypatch.setattr(sched.time, "sleep", advance_clock)
    sched.start_scheduler()
    assert len(selected) == 8
    assert len(engine.rows) == 7
    assert all(row["status"] == "SUCCESS" for row in engine.rows)

    assert len(close_dates) == 35  # five OHLCV fields per UTC close
    for day in range(7):
        assert close_dates[day * 5:(day + 1) * 5] == [
            date(2026, 9, 20) + timedelta(days=day)
        ] * 5
    if host_zone == "UTC":
        assert len(domestic_runs) == 7
    else:
        # A host-local 20:00 job is due at a different UTC instant. Its
        # registration order cannot promise priority at Binance's UTC slot.
        assert len(domestic_runs) == 6
        assert domestic_runs[0].date() == date(2026, 9, 22)


@pytest.mark.parametrize("weekday", range(7))
def test_daily_job_next_run_is_20_utc_on_every_weekday(monkeypatch, weekday):
    real_datetime = datetime
    frozen = real_datetime(2026, 9, 21, 19, 0, tzinfo=timezone.utc) + timedelta(days=weekday)

    class _FrozenDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return frozen.astimezone().replace(tzinfo=None)
            return frozen.astimezone(tz)

    monkeypatch.setattr(schedule.datetime, "datetime", _FrozenDateTime)
    job = schedule.Scheduler().every().day.at("20:00", "UTC").do(lambda: None)
    expected = frozen.replace(hour=20).astimezone().replace(tzinfo=None)
    assert job.next_run == expected


def test_daily_group_does_not_import_binance_or_own_its_close(monkeypatch):
    original_import = builtins.__import__
    imported = []

    def block_puller_imports(name, *args, **kwargs):
        if name.startswith("ingestion."):
            imported.append(name)
            raise ImportError("offline fake")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", block_puller_imports)
    pullers = sched._get_pullers_for_group("daily", _Engine(), {})
    assert all(name != "Binance_Crypto" for name, *_ in pullers)
    assert "ingestion.altdata.binance_puller" not in imported


def test_constructor_failure_is_failed_summary_and_durable_pull_log(monkeypatch):
    from ingestion.altdata import binance_puller

    monkeypatch.setattr(
        binance_puller, "BinancePuller",
        lambda _engine: _raise(RuntimeError("fake constructor failure")),
    )
    engine = _Engine()
    summary = sched.run_pull_group("crypto", engine, config={})

    assert summary["success_count"] == 0
    assert summary["failure_count"] == 1
    assert summary["results"] == [{
        "puller": "Binance_Crypto", "status": "FAILED",
        "error": "initialization_failed:RuntimeError", "pull_log_id": 1,
    }]
    assert len(engine.rows) == 1
    assert engine.rows[0]["name"] == "Binance_Crypto"
    assert engine.rows[0]["error"] == "initialization_failed:RuntimeError"
    assert "fake constructor failure" not in str(engine.rows[0])


def test_import_failure_is_visible_but_db_outage_cannot_claim_persisted_log(monkeypatch):
    monkeypatch.setattr(
        sched, "_get_pullers_for_group",
        lambda *_args: _raise(ImportError("fake missing module")),
    )
    engine = _Engine()
    summary = sched.run_pull_group("crypto", engine, config={})
    assert summary["results"][0]["error"] == "initialization_failed:ImportError"
    assert len(engine.rows) == 1

    down = _Engine(fail_begin=True)
    with pytest.raises(RuntimeError, match="could not be persisted"):
        sched.run_pull_group("crypto", down, config={})
    assert down.rows == []


def test_crypto_cycle_requires_persisted_start_and_finish(monkeypatch):
    from ingestion.altdata import binance_puller
    from ingestion.pull_context import PullContext, PullLogPersistenceError

    class _FakeBinance:
        source_id = 123

        def pull(self):
            return {"rows_inserted": 2}

    monkeypatch.setattr(binance_puller, "BinancePuller", lambda _engine: _FakeBinance())
    engine = _Engine()
    summary = sched.run_pull_group("crypto", engine, config={})
    assert summary["success_count"] == 1
    assert summary["results"][0]["puller"] == "Binance_Crypto"
    assert engine.rows[0]["status"] == "SUCCESS"
    assert engine.rows[0]["rows"] == 2

    unavailable = _Engine(fail_begin=True)
    with pytest.raises(PullLogPersistenceError, match="pull_log start unavailable"):
        sched.run_pull_group("crypto", unavailable, config={})
    assert unavailable.rows == []

    finish_down = _Engine()
    with pytest.raises(PullLogPersistenceError, match="pull_log finish unavailable"):
        with PullContext(
            finish_down, "Binance_Crypto", source_id=123,
            require_persisted_log=True,
        ):
            finish_down.fail_begin = True
    assert len(finish_down.rows) == 1

    for broken in (_Engine(missing_finish_row=True), _Engine(fail_finish_commit=True)):
        with pytest.raises(PullLogPersistenceError, match="pull_log finish unavailable"):
            with PullContext(
                broken, "Binance_Crypto", source_id=123,
                require_persisted_log=True,
            ) as ctx:
                ctx.record_rows(2)
        assert len(broken.rows) == 1
        assert broken.rows[0].get("status") != "SUCCESS"


def test_unverified_finish_failure_is_failed_cycle_without_scheduler_retry(monkeypatch):
    from ingestion.altdata import binance_puller

    class _FakeBinance:
        source_id = 123

        def pull(self):
            return {"rows_inserted": 2}

    monkeypatch.setattr(binance_puller, "BinancePuller", lambda _engine: _FakeBinance())
    clock = {"now": datetime(2026, 9, 21, 19, tzinfo=timezone.utc)}
    _fake_schedule_clock(monkeypatch, clock, "America/Los_Angeles")
    engine = _Engine(fail_finish_commit=True)
    fake_db = ModuleType("db")
    fake_db.get_engine = lambda: engine
    monkeypatch.setitem(sys.modules, "db", fake_db)
    results = []
    other_jobs = []
    jobs = schedule.Scheduler()
    jobs.every().day.at("20:00", "UTC").do(
        lambda: results.append(sched.run_daily_binance_close())
    )
    jobs.every().day.at("20:00", "UTC").do(lambda: other_jobs.append(clock["now"]))

    clock["now"] = datetime(2026, 9, 21, 20, tzinfo=timezone.utc)
    jobs.run_pending()
    assert len(results) == 1
    assert results[0]["failure_count"] == 1
    assert results[0]["results"][0]["completed_log_persisted"] is False
    assert results[0]["results"][0]["provider_write_state"] == "unknown"
    assert results[0]["results"][0]["error"] == "cycle_unverified:PullLogPersistenceError"
    assert len(other_jobs) == 1
    assert engine.rows[0]["status"] == "RUNNING"

    clock["now"] += timedelta(minutes=1)
    jobs.run_pending()
    assert len(results) == 1
    assert len(other_jobs) == 1


def test_monday_provider_bars_keep_only_sunday_close(monkeypatch):
    from ingestion.altdata import binance_puller

    monday = datetime(2026, 9, 28, 20, 5, tzinfo=timezone.utc)

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return monday if tz is None else monday.astimezone(tz)

    monkeypatch.setattr(binance_puller, "datetime", _FrozenDateTime)
    fake = binance_puller.BinancePuller.__new__(binance_puller.BinancePuller)
    fake.engine = _Engine()
    fake.source_id = 123
    inserted = []
    monkeypatch.setattr(fake, "_insert_raw", lambda **kwargs: inserted.append(kwargs))

    def bar(day):
        start = int(datetime.combine(day, datetime.min.time(), timezone.utc).timestamp() * 1000)
        return [start, "100", "110", "90", "105", "1", start + 86_400_000 - 1]

    monkeypatch.setattr(fake, "_fetch_klines", lambda _symbol: [
        bar(date(2026, 9, 25)), bar(date(2026, 9, 26)), bar(date(2026, 9, 27)),
    ])
    assert fake._pull_klines("BTCUSDT") == 5
    assert {row["obs_date"] for row in inserted} == {date(2026, 9, 27)}
