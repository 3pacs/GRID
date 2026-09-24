"""A ticker/day chain is one complete, replaceable provider capture."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Event, Lock, Thread
from typing import Any, Self

import pytest

from ingestion import options


class _Result:
    def __init__(self, row: tuple) -> None:
        self.row = row

    def fetchone(self) -> tuple:
        return self.row


class _DB:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.rows: list[dict] = []
        self.fail_after_inserts: int | None = None

    def begin(self) -> Self:
        return self

    def __enter__(self) -> Self:
        self._before = [row.copy() for row in self.rows]
        self._insert_count = 0
        return self

    def __exit__(self, exc_type: object, *_args: object) -> None:
        if exc_type is not None:
            self.rows = self._before

    def execute(self, statement: Any, params: dict | None = None) -> _Result:
        sql = str(statement)
        values = params or {}
        self.calls.append((sql, values))
        if "MAX(capture_started_at)" in sql:
            starts = [row["started_at"] for row in self.rows if row["ticker"] == values["ticker"]
                      and row["snap_date"] == values["snap_date"] and row.get("started_at")]
            return _Result((max(starts) if starts else None,))
        if "DELETE FROM options_snapshots" in sql:
            self.rows = [row for row in self.rows if not (
                row["ticker"] == values["ticker"] and row["snap_date"] == values["snap_date"]
            )]
        elif "INSERT INTO options_snapshots" in sql:
            self.rows.append(values.copy())
            self._insert_count += 1
            if self._insert_count == self.fail_after_inserts:
                raise RuntimeError("simulated insert failure")
        return _Result((None,))


class _SerializedDB(_DB):
    """A transaction mutex makes an early DB checkout observable offline."""

    def __init__(self) -> None:
        super().__init__()
        self._mutex = Lock()

    def begin(self) -> _Transaction:
        return _Transaction(self)


class _Transaction:
    def __init__(self, db: _SerializedDB) -> None:
        self.db = db

    def __enter__(self) -> _DB:
        self.db._mutex.acquire()
        return self.db.__enter__()

    def __exit__(self, exc_type: object, *args: object) -> None:
        try:
            self.db.__exit__(exc_type, *args)
        finally:
            self.db._mutex.release()


class _Yahoo:
    def __init__(self, expirations: list[int], strikes: list[float], *, fail_second: bool = False) -> None:
        self.expirations = expirations
        self.strikes = strikes
        self.fail_second = fail_second
        self.final_response_at: datetime | None = None
        self.calls = 0

    def get_options(self, _ticker: str, _expiry: int | None = None) -> dict | None:
        self.calls += 1
        self.final_response_at = datetime.now(timezone.utc)
        if self.calls == 2 and self.fail_second:
            return None
        opts = [{
            "strike": strike, "volume": 3, "openInterest": 10,
            "impliedVolatility": 0.2, "lastPrice": 2.0,
            "bid": 1.0, "ask": 3.0, "inTheMoney": False,
        } for strike in self.strikes]
        return {
            "quote": {"regularMarketPrice": 100.0},
            "expirations": self.expirations,
            "calls": opts, "puts": opts,
        }


@pytest.fixture
def puller(monkeypatch: pytest.MonkeyPatch) -> options.OptionsPuller:
    obj = options.OptionsPuller.__new__(options.OptionsPuller)
    obj.engine = _DB()
    monkeypatch.setattr(obj, "_push_to_resolved", lambda *_args: None)
    monkeypatch.setattr(options.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(options, "compute_max_pain", lambda *_args: 100.0)
    monkeypatch.setattr(options, "compute_iv_skew", lambda *_args: 0.0)
    monkeypatch.setattr(options, "_compute_atm_iv", lambda *_args: 0.2)
    monkeypatch.setattr(options, "_compute_wing_iv", lambda *_args, **_kwargs: 0.2)
    monkeypatch.setattr(options, "_compute_oi_concentration", lambda *_args: 0.5)
    return obj


def _run(puller: options.OptionsPuller, strikes: list[float], *, fail_second: bool = False):
    now = datetime.now(timezone.utc)
    expirations = [int((now + timedelta(days=days)).timestamp()) for days in (10, 20)]
    yahoo = _Yahoo(expirations, strikes, fail_second=fail_second)
    puller._yahoo = yahoo
    result = puller._pull_ticker("SPY", now.date().isoformat())
    return result, yahoo


def test_completed_batch_time_follows_final_provider_response(
    puller: options.OptionsPuller,
) -> None:
    result, yahoo = _run(puller, [100.0])
    db = puller.engine
    assert result["status"] == "SUCCESS"
    assert yahoo.calls == 2
    assert len(db.rows) == 4
    assert len({row["batch_id"] for row in db.rows}) == 1
    assert len({row["started_at"] for row in db.rows}) == 1
    assert len({row["completed_at"] for row in db.rows}) == 1
    assert yahoo.final_response_at is not None
    assert db.rows[0]["completed_at"] >= yahoo.final_response_at
    sql = [statement for statement, _ in db.calls]
    assert next(i for i, s in enumerate(sql) if "pg_advisory_xact_lock" in s) < next(
        i for i, s in enumerate(sql) if "DELETE FROM options_snapshots" in s
    ) < next(i for i, s in enumerate(sql) if "INSERT INTO options_snapshots" in s)


def test_second_complete_pull_replaces_added_and_removed_strikes(
    puller: options.OptionsPuller,
) -> None:
    assert _run(puller, [100.0, 110.0])[0]["status"] == "SUCCESS"
    first_batch = {row["batch_id"] for row in puller.engine.rows}
    assert _run(puller, [100.0, 120.0])[0]["status"] == "SUCCESS"
    rows = puller.engine.rows
    assert len(rows) == 8  # two expiries, calls and puts, two strikes
    assert {row["strike"] for row in rows} == {100.0, 120.0}
    assert len({row["batch_id"] for row in rows}) == 1
    assert {row["batch_id"] for row in rows} != first_batch
    assert len({row["completed_at"] for row in rows}) == 1


def test_failed_second_pull_preserves_first_complete_batch(
    puller: options.OptionsPuller,
) -> None:
    assert _run(puller, [100.0])[0]["status"] == "SUCCESS"
    prior = [row.copy() for row in puller.engine.rows]
    assert _run(puller, [100.0, 120.0], fail_second=True)[0]["status"] == "FAILED"
    assert puller.engine.rows == prior


def test_capture_deadline_fails_closed_without_database_checkout(
    puller: options.OptionsPuller, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(options, "MAX_CAPTURE_SECONDS", 0)
    assert _run(puller, [100.0])[0]["status"] == "FAILED"
    assert puller.engine.calls == []
    assert puller.engine.rows == []


def test_insert_failure_rolls_back_delete_and_partial_new_batch(
    puller: options.OptionsPuller,
) -> None:
    assert _run(puller, [100.0])[0]["status"] == "SUCCESS"
    prior = [row.copy() for row in puller.engine.rows]
    puller.engine.fail_after_inserts = 2
    assert _run(puller, [100.0, 120.0])[0]["status"] == "FAILED"
    assert puller.engine.rows == prior


def test_older_overlapping_worker_cannot_replace_newer_capture(
    puller: options.OptionsPuller,
) -> None:
    db = _SerializedDB()
    puller.engine = db
    newer = options.OptionsPuller.__new__(options.OptionsPuller)
    newer.engine = db
    newer._push_to_resolved = lambda *_args: None
    now = datetime.now(timezone.utc)
    expirations = [int((now + timedelta(days=days)).timestamp()) for days in (10, 20)]
    old_yahoo = _Yahoo(expirations, [100.0])
    newer._yahoo = _Yahoo(expirations, [120.0])
    old_first_page = old_yahoo.get_options
    first_page_entered = Event()
    release_old = Event()
    newer_done = Event()
    results: dict[str, dict] = {}

    def blocked_first_page(ticker: str, expiry: int | None = None) -> dict | None:
        if old_yahoo.calls == 0:
            first_page_entered.set()
            assert release_old.wait(5), "test did not release the first provider page"
        return old_first_page(ticker, expiry)

    old_yahoo.get_options = blocked_first_page
    puller._yahoo = old_yahoo

    def run(name: str, worker: options.OptionsPuller) -> None:
        results[name] = worker._pull_ticker("SPY", now.date().isoformat())
        if name == "newer":
            newer_done.set()

    old_thread = Thread(target=run, args=("older", puller), daemon=True)
    new_thread = Thread(target=run, args=("newer", newer), daemon=True)
    old_thread.start()
    try:
        assert first_page_entered.wait(5)
        new_thread.start()
        assert newer_done.wait(5), "provider capture held the DB transaction or lock"
    finally:
        release_old.set()
        old_thread.join(5)
        if new_thread.ident is not None:
            new_thread.join(5)

    assert not old_thread.is_alive() and not new_thread.is_alive()
    assert results["newer"]["status"] == "SUCCESS"
    assert results["older"]["status"] == "SKIPPED"
    assert {row["strike"] for row in db.rows} == {120.0}
    assert len({row["batch_id"] for row in db.rows}) == 1


def test_complete_pull_replaces_preexisting_legacy_rows(puller: options.OptionsPuller) -> None:
    puller.engine.rows = [{
        "ticker": "SPY", "snap_date": datetime.now(timezone.utc).date().isoformat(),
        "strike": 999.0, "batch_id": None, "started_at": None, "completed_at": None,
    }]
    assert _run(puller, [100.0], fail_second=True)[0]["status"] == "FAILED"
    assert puller.engine.rows[0]["batch_id"] is None  # still reader-unavailable
    assert _run(puller, [100.0])[0]["status"] == "SUCCESS"
    assert {row["strike"] for row in puller.engine.rows} == {100.0}
    assert all(row["batch_id"] and row["started_at"] and row["completed_at"]
               for row in puller.engine.rows)
