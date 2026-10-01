"""Every options capture is one complete, immutable, append-only batch.

Offline fake of the options_append_only_20260930 store: ``rows`` models
``options_snapshots_all`` and ``batches`` models ``options_capture_batches``.
No writer statement may DELETE or UPDATE either one.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Event, Lock, Thread, current_thread
from typing import Any, Self

import pytest

from ingestion import options

SESSION_NOW = datetime(2026, 9, 25, 19, tzinfo=timezone.utc)


class _Result:
    def __init__(self, row: tuple) -> None:
        self.row = row
        self.rowcount = 1

    def fetchone(self) -> tuple:
        return self.row


class _DB:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self.rows: list[dict] = []
        self.batches: list[dict] = []
        self.fail_after_inserts: int | None = None
        self.next_xid = 0
        self.clock_offsets: dict[str, timedelta] = {}
        self.allocations: list[tuple[str, int, datetime]] = []

    def begin(self) -> Self:
        return self

    def __enter__(self) -> Self:
        self._before = [row.copy() for row in self.rows]
        self._before_batches = [batch.copy() for batch in self.batches]
        self._insert_count = 0
        return self

    def __exit__(self, exc_type: object, *_args: object) -> None:
        if exc_type is not None:
            self.rows = self._before
            self.batches = self._before_batches

    def execute(self, statement: Any, params: dict | None = None) -> _Result:
        sql = str(statement)
        values = params or {}
        self.calls.append((sql, values))
        if "txid_current()" in sql:
            self.next_xid += 1
            started = SESSION_NOW + self.clock_offsets.get(
                current_thread().name, timedelta(),
            )
            self.allocations.append((current_thread().name, self.next_xid, started))
            return _Result((self.next_xid, started))
        if "SELECT clock_timestamp()" in sql:
            return _Result((SESSION_NOW + timedelta(minutes=1) + self.clock_offsets.get(
                current_thread().name, timedelta(),
            ),))
        if "MAX(capture_ordinal) FROM options_capture_batches" in sql:
            ordinals = [b["ordinal"] for b in self.batches if b["ticker"] == values["ticker"]
                        and b["snap_date"] == values["snap_date"]]
            return _Result((max(ordinals) if ordinals else None,))
        if "DELETE" in sql.upper().split() or sql.lstrip().upper().startswith("UPDATE OPTIONS"):
            raise AssertionError(f"append-only store mutated: {sql}")
        if "INSERT INTO options_capture_batches" in sql:
            self.batches.append(values.copy())
        elif "INSERT INTO options_snapshots_all" in sql:
            assert any(b["batch_id"] == values["batch_id"] for b in self.batches), \
                "row written before its batch was registered"
            self.rows.append(values.copy())
            self._insert_count += 1
            if self._insert_count == self.fail_after_inserts:
                raise RuntimeError("simulated insert failure")
        return _Result((None,))


def _visible(db: _DB, ticker: str = "SPY") -> list[dict]:
    """The ``options_snapshots`` view: the highest-ordinal registered batch."""
    day = SESSION_NOW.date().isoformat()
    registered = [b for b in db.batches if b["ticker"] == ticker and b["snap_date"] == day]
    if not registered:
        return [r for r in db.rows if r["ticker"] == ticker and r["snap_date"] == day]
    latest = max(registered, key=lambda b: b["ordinal"])["batch_id"]
    return [r for r in db.rows if r.get("batch_id") == latest]


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
        self.final_response_at = SESSION_NOW
        if self.calls == 2 and self.fail_second:
            return None
        opts = [{
            "strike": strike, "volume": 3, "openInterest": 10,
            "impliedVolatility": 0.2, "lastPrice": 2.0,
            "bid": 1.0, "ask": 3.0, "inTheMoney": False,
        } for strike in self.strikes]
        return {
            "quote": {"regularMarketPrice": 100.0,
                      "regularMarketTime": int((SESSION_NOW - timedelta(hours=2)).timestamp())},
            "expirations": self.expirations,
            "calls": opts, "puts": opts,
        }


@pytest.fixture
def puller(monkeypatch: pytest.MonkeyPatch) -> options.OptionsPuller:
    obj = options.OptionsPuller.__new__(options.OptionsPuller)
    obj.engine = _DB()
    monkeypatch.setattr(obj, "_push_to_resolved", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(options.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(options, "_utc_now", lambda: SESSION_NOW)
    monkeypatch.setattr(options, "compute_max_pain", lambda *_args: 100.0)
    monkeypatch.setattr(options, "compute_iv_skew", lambda *_args: 0.0)
    monkeypatch.setattr(options, "_compute_atm_iv", lambda *_args: 0.2)
    monkeypatch.setattr(options, "_compute_wing_iv", lambda *_args, **_kwargs: 0.2)
    monkeypatch.setattr(options, "_compute_oi_concentration", lambda *_args: 0.5)
    return obj


def _run(puller: options.OptionsPuller, strikes: list[float], *, fail_second: bool = False):
    now = SESSION_NOW
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
    assert result["snapshots_inserted"] == 4
    assert result["rows_inserted"] == 5  # four snapshot writes + one signal upsert
    assert len({row["batch_id"] for row in db.rows}) == 1
    assert len({row["ordinal"] for row in db.rows}) == 1
    assert len({row["started_at"] for row in db.rows}) == 1
    assert len({row["completed_at"] for row in db.rows}) == 1
    assert all(row["provider_regular_market_at"].date() == SESSION_NOW.date()
               for row in db.rows)
    assert yahoo.final_response_at is not None
    assert db.rows[0]["completed_at"] >= yahoo.final_response_at
    sql = [statement for statement, _ in db.calls]
    assert "txid_current()" in sql[0]
    assert not any("nextval(" in statement for statement in sql)
    assert not any("DELETE" in statement.upper() for statement in sql)
    assert next(i for i, s in enumerate(sql) if "pg_advisory_xact_lock" in s) < next(
        i for i, s in enumerate(sql) if "INSERT INTO options_capture_batches" in s
    ) < next(i for i, s in enumerate(sql) if "INSERT INTO options_snapshots_all" in s)
    assert result["capture_batch_id"] == db.rows[0]["batch_id"]
    assert result["capture_ordinal"] == db.rows[0]["ordinal"]
    assert result["latest_batch"] is True
    (batch,) = db.batches
    assert batch["row_count"] == 4 and batch["source"] == "options_puller"
    assert batch["batch_id"] == result["capture_batch_id"]


def test_cancel_before_publish_does_not_write_or_advance_freshness(puller):
    expirations = [int((SESSION_NOW + timedelta(days=10)).timestamp())]
    puller._yahoo = _Yahoo(expirations, [100.0])
    checks = iter([True, True, False])
    result = puller._pull_ticker("SPY", SESSION_NOW.date().isoformat(), should_continue=lambda: next(checks))
    assert result["status"] == "DEFERRED"
    assert result["rows_inserted"] == 0
    assert not puller.engine.rows
    assert not puller.engine.batches
    assert not any("DELETE" in sql.upper() for sql, _ in puller.engine.calls)


def test_cancel_during_publish_rolls_back_ticker(puller):
    expirations = [int((SESSION_NOW + timedelta(days=10)).timestamp())]
    puller._yahoo = _Yahoo(expirations, [100.0, 105.0])
    checks = iter([True] * 5 + [False])
    result = puller._pull_ticker("SPY", SESSION_NOW.date().isoformat(), should_continue=lambda: next(checks))
    assert result["status"] == "DEFERRED"
    assert result["rows_inserted"] == 0
    assert not puller.engine.rows


def test_short_snapshot_insert_rolls_back_whole_batch(puller, monkeypatch):
    """A batch is complete or absent: an unexpected insert count fails closed."""
    execute = puller.engine.execute

    def with_conflict(statement, params=None):
        result = execute(statement, params)
        if "INSERT INTO options_snapshots_all" in str(statement):
            result.rowcount = 0
        return result

    monkeypatch.setattr(puller.engine, "execute", with_conflict)
    result, _ = _run(puller, [100.0])
    assert result["status"] == "FAILED"
    assert puller.engine.rows == [] and puller.engine.batches == []


def test_repeated_provider_contract_is_stored_once_per_batch(puller):
    result, _ = _run(puller, [100.0, 100.0, 105.0])
    assert result["status"] == "SUCCESS"
    assert result["snapshots"] == 12
    assert result["snapshots_inserted"] == 8
    assert puller.engine.batches[0]["row_count"] == 8


def test_explicit_six_expiry_cap_preserves_legacy_gem_scope(
    puller: options.OptionsPuller,
) -> None:
    now = SESSION_NOW
    expirations = [int((now + timedelta(days=10 * n)).timestamp()) for n in range(1, 8)]
    yahoo = _Yahoo(expirations, [100.0])
    puller._yahoo = yahoo

    result = puller._pull_ticker("OPCH", now.date().isoformat(), max_expirations=6)

    assert result["status"] == "SUCCESS"
    assert yahoo.calls == 6
    assert len(puller.engine.rows) == 12  # six complete call/put expiries
    assert len({row["batch_id"] for row in puller.engine.rows}) == 1


@pytest.mark.parametrize("now", [
    datetime(2026, 9, 26, 15, tzinfo=timezone.utc),  # Saturday
    datetime(2026, 9, 7, 19, tzinfo=timezone.utc),   # Labor Day
    datetime(2026, 9, 26, 0, 15, tzinfo=timezone.utc),  # Friday ET, Saturday UTC
    datetime(2026, 9, 25, 0, 15, tzinfo=timezone.utc),  # Friday UTC, Thursday ET
])
def test_non_session_never_initializes_provider_or_publishes(
    puller: options.OptionsPuller, monkeypatch: pytest.MonkeyPatch, now: datetime,
) -> None:
    monkeypatch.setattr(options, "_utc_now", lambda: now)
    monkeypatch.setattr(options, "YahooOptionsClient",
                        lambda: pytest.fail("closed day must not contact provider"))
    result = puller.pull_all(tickers=["SPY"], max_expirations=6)
    assert result == [{"ticker": "SPY", "status": "SKIPPED", "rows_inserted": 0,
                       "reason": "non-equity-session"}]
    assert puller.engine.rows == []


@pytest.mark.parametrize("reported", [
    None,
    int((SESSION_NOW - timedelta(days=1)).timestamp()),
    int((SESSION_NOW + timedelta(minutes=5)).timestamp()),
])
def test_missing_stale_or_future_quote_time_cannot_publish(
    puller: options.OptionsPuller, reported: int | None,
) -> None:
    expirations = [int((SESSION_NOW + timedelta(days=n)).timestamp()) for n in (10, 20)]
    yahoo = _Yahoo(expirations, [100.0])
    original = yahoo.get_options

    def dated(ticker: str, expiry: int | None = None) -> dict | None:
        page = original(ticker, expiry)
        assert page is not None
        if reported is None:
            page["quote"].pop("regularMarketTime")
        else:
            page["quote"]["regularMarketTime"] = reported
        return page

    yahoo.get_options = dated
    puller._yahoo = yahoo
    assert puller._pull_ticker("SPY", SESSION_NOW.date().isoformat())["status"] == "FAILED"
    assert puller.engine.rows == []


def test_second_page_with_stale_quote_time_cannot_publish(
    puller: options.OptionsPuller,
) -> None:
    expirations = [int((SESSION_NOW + timedelta(days=n)).timestamp()) for n in (10, 20)]
    yahoo = _Yahoo(expirations, [100.0])
    original = yahoo.get_options

    def mixed(ticker: str, expiry: int | None = None) -> dict | None:
        page = original(ticker, expiry)
        assert page is not None
        if expiry is not None:
            page["quote"]["regularMarketTime"] = int(
                (SESSION_NOW - timedelta(days=1)).timestamp()
            )
        return page

    yahoo.get_options = mixed
    puller._yahoo = yahoo
    assert puller._pull_ticker("SPY", SESSION_NOW.date().isoformat())["status"] == "FAILED"
    assert puller.engine.rows == []


def test_near_expiry_signal_stays_within_captured_six(
    puller: options.OptionsPuller,
) -> None:
    now = SESSION_NOW
    expirations = [int((now + timedelta(days=1, hours=n)).timestamp()) for n in range(6)]
    expirations.append(int((now + timedelta(days=10)).timestamp()))
    yahoo = _Yahoo(expirations, [100.0])
    puller._yahoo = yahoo

    assert puller._pull_ticker("OPCH", now.date().isoformat(),
                               max_expirations=6)["status"] == "SUCCESS"
    signal_params = next(params for sql, params in puller.engine.calls
                         if "INSERT INTO options_daily_signals" in sql)
    assert signal_params["ne"] == datetime.fromtimestamp(
        expirations[0], timezone.utc,
    ).date().isoformat()


@pytest.mark.parametrize("cap", [0, 13, True, 6.5])
def test_invalid_expiry_cap_fails_before_client_or_provider(
    puller: options.OptionsPuller, cap: object,
) -> None:
    with pytest.raises(ValueError, match="max_expirations"):
        puller.pull_all(tickers=["OPCH"], max_expirations=cap)


def test_second_same_day_pull_keeps_both_batches(
    puller: options.OptionsPuller,
) -> None:
    first, _ = _run(puller, [100.0, 110.0])
    assert first["status"] == "SUCCESS"
    first_rows = [row.copy() for row in puller.engine.rows]
    second, _ = _run(puller, [100.0, 120.0])
    assert second["status"] == "SUCCESS"
    rows = puller.engine.rows
    assert len(rows) == 16  # both batches: two expiries x call/put x two strikes
    assert rows[:8] == first_rows  # the earlier batch is untouched
    assert {b["batch_id"] for b in puller.engine.batches} == {
        first["capture_batch_id"], second["capture_batch_id"]}
    assert [b["ordinal"] for b in puller.engine.batches] == [1, 2]
    visible = _visible(puller.engine)
    assert {row["strike"] for row in visible} == {100.0, 120.0}
    assert {row["batch_id"] for row in visible} == {second["capture_batch_id"]}


def test_failed_second_pull_preserves_first_complete_batch(
    puller: options.OptionsPuller,
) -> None:
    assert _run(puller, [100.0])[0]["status"] == "SUCCESS"
    prior = [row.copy() for row in puller.engine.rows]
    prior_batches = [b.copy() for b in puller.engine.batches]
    assert _run(puller, [100.0, 120.0], fail_second=True)[0]["status"] == "FAILED"
    assert puller.engine.rows == prior
    assert puller.engine.batches == prior_batches


def test_capture_deadline_fails_closed_after_short_xid_checkout(
    puller: options.OptionsPuller, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(options, "MAX_CAPTURE_SECONDS", 0)
    assert _run(puller, [100.0])[0]["status"] == "FAILED"
    assert any("txid_current()" in sql for sql, _ in puller.engine.calls)
    assert not any("options_snapshots" in sql for sql, _ in puller.engine.calls)
    assert puller.engine.rows == []


def test_insert_failure_rolls_back_partial_new_batch_and_keeps_old(
    puller: options.OptionsPuller,
) -> None:
    assert _run(puller, [100.0])[0]["status"] == "SUCCESS"
    prior = [row.copy() for row in puller.engine.rows]
    prior_batches = [b.copy() for b in puller.engine.batches]
    puller.engine.fail_after_inserts = 2
    assert _run(puller, [100.0, 120.0])[0]["status"] == "FAILED"
    assert puller.engine.rows == prior
    assert puller.engine.batches == prior_batches


def test_older_overlapping_worker_is_kept_but_never_becomes_latest(
    puller: options.OptionsPuller,
) -> None:
    db = _SerializedDB()
    db.clock_offsets = {"older": timedelta(hours=1), "newer": -timedelta(hours=1)}
    puller.engine = db
    newer = options.OptionsPuller.__new__(options.OptionsPuller)
    newer.engine = db
    newer._push_to_resolved = lambda *_args, **_kwargs: 0
    now = SESSION_NOW
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

    old_thread = Thread(target=run, args=("older", puller), name="older", daemon=True)
    new_thread = Thread(target=run, args=("newer", newer), name="newer", daemon=True)
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
    assert results["newer"]["latest_batch"] is True
    assert results["older"]["status"] == "SUCCESS"
    assert results["older"]["latest_batch"] is False
    assert [entry[1] for entry in db.allocations] == [1, 2]
    assert db.allocations[0][2] > db.allocations[1][2]  # inverted wall clocks
    assert len({row["batch_id"] for row in db.rows}) == 2  # both captures kept
    assert {row["strike"] for row in _visible(db)} == {120.0}
    # The older capture never overwrote the newer batch's daily signals.
    signal_writes = [params for sql, params in db.calls if "INSERT INTO options_daily_signals" in sql]
    assert len(signal_writes) == 1


def test_complete_pull_supersedes_but_keeps_preexisting_legacy_rows(
    puller: options.OptionsPuller,
) -> None:
    legacy = {
        "ticker": "SPY", "snap_date": SESSION_NOW.date().isoformat(),
        "strike": 999.0, "batch_id": None, "ordinal": None,
        "started_at": None, "completed_at": None,
    }
    puller.engine.rows = [legacy.copy()]
    assert _run(puller, [100.0], fail_second=True)[0]["status"] == "FAILED"
    assert _visible(puller.engine) == [legacy]  # legacy stays shown until superseded
    assert _run(puller, [100.0])[0]["status"] == "SUCCESS"
    assert puller.engine.rows[0] == legacy  # kept, not deleted
    assert {row["strike"] for row in _visible(puller.engine)} == {100.0}
    assert all(row["batch_id"] and row["ordinal"] and row["started_at"] and row["completed_at"]
               for row in _visible(puller.engine))
