from datetime import date, datetime, timezone

from api.gamma_board_schwab import _one_expiry
from api.gamma_time_watchlist import (
    PINNED,
    TrackingLimit,
    set_tracking,
    tracked_symbols,
)
from scripts.record_gamma_time import capture

import pytest

NOW = datetime(2026, 11, 10, 17, 30, 40, tzinfo=timezone.utc)
DAY = NOW.date()


def test_capture_observed_frame_and_provenance():
    panel = dict(
        symbol="QQQ",
        status="available",
        fresh=True,
        session_date=str(DAY),
        expiries=[str(DAY)],
        spot=600,
        source_label="private",
        as_of=NOW.isoformat(),
        earliest_included_quote_at=NOW.isoformat(),
        spot_as_of=NOW.isoformat(),
        cells=[dict(expiry=str(DAY), strike=605, net_gex=-2300)],
    )
    saved = []

    def write(symbol, **kwargs):
        saved.append((symbol, kwargs))
        return True

    assert (
        capture("QQQ", clock=lambda: NOW, read=lambda *a, **k: panel, write=write)
        == "recorded"
    )
    assert saved[0][1]["levels"] == {605: -2300} and saved[0][1]["chain_as_of"] == NOW
    assert (
        capture(
            "QQQ",
            clock=lambda: NOW,
            read=lambda *a, **k: {**panel, "fresh": False},
            write=write,
        )
        == "unqualified"
    )
    assert (
        capture(
            "QQQ",
            clock=lambda: NOW,
            read=lambda *a, **k: {
                **panel,
                "earliest_included_quote_at": "2026-11-10T17:28:39+00:00",
            },
            write=write,
        )
        == "stale"
    )
    assert len(saved) == 1
    assert (
        capture(
            "SPY",
            clock=lambda: NOW.replace(hour=23),
            read=lambda *a, **k: pytest.fail(),
        )
        == "closed"
    )


def test_one_expiry_does_not_substitute_tomorrow_for_zero_dte():
    bucket = {"2026-11-13:3": {}, "2026-11-20:10": {}}
    j = {"callExpDateMap": bucket, "putExpDateMap": bucket}
    with pytest.raises(ValueError):
        _one_expiry("SPX", j, DAY)
    assert list(_one_expiry("AAPL", j, DAY)["callExpDateMap"]) == ["2026-11-13:3"]


def test_watchlist_is_bounded_opt_in_and_pins_stay(monkeypatch, tmp_path):
    monkeypatch.setenv("GRID_GAMMA_TIME_DB", str(tmp_path / "frames.db"))
    assert tracked_symbols() == PINNED and not list(tmp_path.iterdir())
    for s in ("AAPL", "MSFT", "NVDA"):
        assert set_tracking(s, True)
    with pytest.raises(TrackingLimit):
        set_tracking("AMZN", True)
    with pytest.raises(TrackingLimit):
        set_tracking("SPY", False)
    assert not set_tracking("MSFT", False) and set_tracking("AMZN", True)
    assert set(tracked_symbols()) == set(PINNED) | {"AAPL", "NVDA", "AMZN"}


def test_original_spy_store_replays_only_recorded_minutes_and_converts_millions(
    tmp_path,
):
    import sqlite3
    from api.gamma_time_spy import read_spy

    db = tmp_path / "spy.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE strike_gex_meta(ts TEXT,expiry TEXT,spot REAL)")
        conn.execute(
            "CREATE TABLE strike_gex(ts TEXT,expiry TEXT,strike REAL,net_gex_m REAL)"
        )
        for hhmm, bucket in (
            ("14:30", [(750, -2), (751, 0)]),
            ("14:32", [(750, 2.5)]),
            ("21:01", [(750, 88)]),
        ):
            clock = "2026-11-10T" + hhmm + ":00+00:00"
            conn.execute(
                "INSERT INTO strike_gex_meta VALUES(?,?,?)", (clock, str(DAY), 750.05)
            )
            conn.executemany(
                "INSERT INTO strike_gex VALUES(?,?,?,?)",
                [(clock, str(DAY), *row) for row in bucket],
            )
    view = read_spy(DAY, path=db)
    assert len(view["times"]) == 2 and view["times"][1] - view["times"][0] == 120000
    assert view["values"] == [[-2000000, 2500000], [0, None]]


def test_tracking_route_requires_admin_and_board_flag(monkeypatch, tmp_path):
    from secrets import token_urlsafe
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api.routers.gamma_watch import router
    from api.auth import create_token
    from config import settings

    monkeypatch.setenv("GRID_JWT_SECRET", token_urlsafe(48))
    monkeypatch.setenv("GRID_GAMMA_TIME_DB", str(tmp_path / "frames.db"))
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    route = "/api/v1/gamma-watch/time-map/AAPL/tracking"
    auth = lambda role: {
        "Authorization": "Bearer " + create_token(role=role, expires_hours=1)
    }
    assert client.post(route, json={"enabled": True}).status_code == 401
    assert (
        client.post(
            route, json={"enabled": True}, headers=auth("contributor")
        ).status_code
        == 403
    )
    monkeypatch.setattr(settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", False)
    assert (
        client.post(route, json={"enabled": True}, headers=auth("admin")).status_code
        == 503
    )
    assert not list(tmp_path.iterdir())
    monkeypatch.setattr(settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", True)
    assert client.post(route, json={"enabled": True}, headers=auth("admin")).json() == {
        "symbol": "AAPL",
        "tracking": True,
    }
    assert (
        client.post(route, json={"enabled": False}, headers=auth("admin")).json()[
            "tracking"
        ]
        is False
    )


# 09:30 New York after the DST transition.
OPEN = datetime(2026, 11, 10, 14, 30, tzinfo=timezone.utc)


def test_opening_only_retries_missing_pins_once_after_quotes_settle():
    from datetime import timedelta
    from scripts.record_gamma_time import _retry_opening

    clock = [OPEN + timedelta(seconds=7)]
    pauses, calls = [], []

    def pause(seconds):
        pauses.append(seconds)
        clock[0] += timedelta(seconds=seconds)

    def run(symbol, minute, cutoff):
        calls.append((symbol, minute, cutoff, clock[0]))
        return "stale"

    results = {
        "SPX": "opening_data_pending",
        "SPY": "recorded",
        "QQQ": "already_recorded",
        "AAPL": "stale",
    }
    _retry_opening(results, OPEN, clock=lambda: clock[0], pause=pause, run=run)
    assert pauses == [5]
    assert calls == [
        ("SPX", OPEN, OPEN + timedelta(seconds=55), OPEN + timedelta(seconds=12))
    ]
    assert results == {
        "SPX": "stale (opening retry; first=opening_data_pending)",
        "SPY": "recorded",
        "QQQ": "already_recorded",
        "AAPL": "stale",
    }


@pytest.mark.parametrize(
    "status",
    [
        "unavailable:SchwabAuthError",
        "unavailable:OpenInterestUnavailable",
        "ineligible_expiry",
        "unqualified",
        "ambiguous_strikes",
        "unavailable:ValueError",
    ],
)
def test_opening_skips_nonretryable_outcomes(status):
    from scripts.record_gamma_time import _retry_opening

    results = {"SPX": status}
    _retry_opening(
        results,
        OPEN,
        clock=lambda: OPEN,
        pause=lambda _: pytest.fail("unexpected wait"),
        run=lambda *a: pytest.fail("unexpected read"),
    )
    assert results["SPX"] == status


@pytest.mark.parametrize("start_second,oversleep", [(19, 0), (7, 10), (7, 50)])
def test_late_opening_pass_does_not_launch_another_read(start_second, oversleep):
    from datetime import timedelta
    from scripts.record_gamma_time import _retry_opening

    now = [OPEN + timedelta(seconds=start_second)]

    def pause(seconds):
        now[0] += timedelta(seconds=seconds + oversleep)

    results = {"SPX": "stale"}
    _retry_opening(
        results,
        OPEN,
        clock=lambda: now[0],
        pause=pause,
        run=lambda *a: pytest.fail("late read"),
    )
    assert results == {"SPX": "stale"}


def test_opening_retry_writes_real_observation_clock_and_refuses_late_result():
    from datetime import timedelta

    begin, finish = OPEN + timedelta(seconds=12), OPEN + timedelta(seconds=15)

    def panel(*args, **kwargs):
        return {
            "symbol": "SPX",
            "status": "available",
            "fresh": True,
            "session_date": str(OPEN.date()),
            "expiries": [str(OPEN.date())],
            "spot": 7520,
            "source_label": "synthetic",
            "as_of": begin.isoformat(),
            "spot_as_of": begin.isoformat(),
            "earliest_included_quote_at": begin.isoformat(),
            "cells": [{"expiry": str(OPEN.date()), "strike": 7500, "net_gex": 1230}],
        }

    rows = []

    def write(symbol, **kw):
        rows.append(kw)
        return True

    def attempt(times, read=panel):
        return capture(
            "SPX",
            clock=iter(times).__next__,
            read=read,
            write=write,
            opening_minute=OPEN,
            deadline=OPEN + timedelta(seconds=55),
        )

    assert attempt([begin, finish, finish]) == "recorded"
    assert rows[0]["observed_at"] == finish and rows[0]["chain_as_of"] == begin
    assert attempt([begin, OPEN + timedelta(minutes=1)]) == "opening_window_elapsed"
    assert (
        attempt([begin, finish, OPEN + timedelta(seconds=55)])
        == "opening_window_elapsed"
    )
    assert (
        attempt(
            [OPEN + timedelta(minutes=1)], lambda *a, **kw: pytest.fail("late read")
        )
        == "opening_window_elapsed"
    )
    assert len(rows) == 1


def test_opening_value_error_is_retryable_only_for_known_reader_data_gaps():
    from datetime import timedelta
    from scripts.record_gamma_time import capture

    def fail(reason):
        def read(*args, **kwargs):
            raise ValueError(reason)

        return read

    kwargs = dict(
        clock=lambda: OPEN,
        opening_minute=OPEN,
        deadline=OPEN + timedelta(seconds=55),
        write=lambda *a, **kw: pytest.fail("write"),
    )
    assert (
        capture("SPX", read=fail("No dated option quotes in scope"), **kwargs)
        == "opening_data_pending"
    )
    with pytest.raises(ValueError, match="Duplicate contract"):
        capture("SPX", read=fail("Duplicate contract identity"), **kwargs)


def test_cycle_changes_dispatch_only_during_pinned_opening_minute(monkeypatch):
    from datetime import timedelta
    from scripts import record_gamma_time as rec

    monkeypatch.setattr(rec.settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", True)
    monkeypatch.setattr(rec, "tracked_symbols", lambda: ("SPX", "AAPL"))
    calls, retry = [], []

    def record(*args):
        calls.append(args)
        return "recorded"

    monkeypatch.setattr(rec, "_safe_capture", record)
    monkeypatch.setattr(
        rec, "_retry_opening", lambda results, minute: retry.append(minute)
    )
    monkeypatch.setattr(rec, "sleep", lambda seconds: None)  # the 2 s slots (2026-11-25)
    monkeypatch.setattr(rec, "_utc", lambda: OPEN)
    assert rec.cycle() == "SPX=recorded, AAPL=recorded"
    assert (
        ("SPX", OPEN, OPEN + timedelta(minutes=1)) in calls
        and ("AAPL",) in calls
        and retry == [OPEN]
    )
    calls.clear()
    monkeypatch.setattr(rec, "_utc", lambda: OPEN + timedelta(minutes=1))
    assert rec.cycle() == "SPX=recorded, AAPL=recorded"
    assert set(calls) == {("SPX",), ("AAPL",)} and retry == [OPEN]


def test_opening_retry_returns_at_cutoff_while_slow_read_cannot_write_next_minute(
    monkeypatch,
):
    from datetime import timedelta
    from threading import Event
    from scripts import record_gamma_time as rec

    entered, release, done = Event(), Event(), Event()
    now = [OPEN + timedelta(seconds=7)]
    outcomes, writes, budgets = [], [], []

    def pause(seconds):
        now[0] += timedelta(seconds=seconds)

    def slow_read(*a, **kw):
        entered.set()
        assert release.wait(2)
        return {}

    def retry(symbol, minute, cutoff):
        try:
            result = rec.capture(
                symbol,
                clock=lambda: now[0],
                read=slow_read,
                write=lambda *a, **kw: writes.append(kw),
                opening_minute=minute,
                deadline=cutoff,
            )
            outcomes.append(result)
            return result
        finally:
            done.set()

    def wait_until_cutoff(futures, timeout):
        assert entered.wait(1)
        budgets.append(timeout)
        now[0] = OPEN + timedelta(seconds=55)
        return set(), set(futures)

    monkeypatch.setattr(rec, "wait", wait_until_cutoff)
    results = {"SPX": "opening_data_pending"}
    try:
        rec._retry_opening(results, OPEN, clock=lambda: now[0], pause=pause, run=retry)
        assert results == {"SPX": "opening_retry_timed_out"}
        assert budgets == [43.0]
        assert not done.is_set()
    finally:
        now[0] = OPEN + timedelta(minutes=1)
        release.set()
    assert done.wait(1)
    assert outcomes == ["opening_window_elapsed"] and writes == []


# --- 2026-11-25: one /chains start every 2 s, at :30 (the 09:30 opening keeps :00.15) ---


def _slot_waits(monkeypatch, rec, start, symbols):
    """Run cycle() at a fixed clock; return {symbol: seconds it waited for its slot} and the capture arguments."""
    import threading

    monkeypatch.setattr(rec.settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", True)
    monkeypatch.setattr(rec, "tracked_symbols", lambda: symbols)
    monkeypatch.setattr(rec, "_retry_opening", lambda results, minute: None)
    monkeypatch.setattr(rec, "_utc", lambda: start)
    pending, waited, args = {}, {}, {}

    def pause(seconds):
        pending[threading.get_ident()] = seconds

    def record(*a):
        waited[a[0]] = pending.pop(threading.get_ident(), 0)
        args[a[0]] = a
        return "recorded"

    monkeypatch.setattr(rec, "sleep", pause)
    monkeypatch.setattr(rec, "_safe_capture", record)
    status = rec.cycle()
    return status, waited, args


def test_cycle_starts_one_read_every_two_seconds(monkeypatch):
    """Until 11-25 the three pins read together (max_workers=3) and finished within 0.12 s of each other; every
    other client's 429 on 11-23 and 11-24 came 0-12.6 s after that burst."""
    from datetime import timedelta
    from scripts import record_gamma_time as rec

    start = OPEN + timedelta(minutes=5, seconds=30)
    status, waited, args = _slot_waits(monkeypatch, rec, start, ("SPX", "SPY", "QQQ", "AAPL"))
    assert status == "SPX=recorded, SPY=recorded, QQQ=recorded, AAPL=recorded"
    assert waited == {"SPX": 0, "SPY": 2.0, "QQQ": 4.0, "AAPL": 6.0}
    assert all(a == (s,) for s, a in args.items()), "outside the opening: no window"


def test_opening_minute_staggers_the_pins_first(monkeypatch):
    from datetime import timedelta
    from scripts import record_gamma_time as rec

    status, waited, args = _slot_waits(monkeypatch, rec, OPEN + timedelta(seconds=0.15), PINNED + ("AAPL",))
    assert waited == {"SPX": 0, "SPY": 2.0, "QQQ": 4.0, "AAPL": 6.0}
    assert {s: a[1:] for s, a in args.items()} == {
        **{s: (OPEN, OPEN + timedelta(minutes=1)) for s in PINNED},
        "AAPL": (),
    }


def test_opening_retry_staggers_pending_pins_inside_the_cutoff():
    import threading
    from datetime import timedelta
    from scripts.record_gamma_time import _retry_opening

    clock = [OPEN + timedelta(seconds=5)]
    main_pauses, slot_waits, calls = [], {}, []

    def pause(seconds):
        if threading.current_thread() is threading.main_thread():
            main_pauses.append(seconds)
            clock[0] += timedelta(seconds=seconds)
        else:
            slot_waits[threading.get_ident()] = seconds

    def run(symbol, minute, cutoff):
        calls.append((symbol, slot_waits.pop(threading.get_ident(), 0), minute, cutoff))
        return "recorded"

    results = {s: "opening_data_pending" for s in PINNED}
    _retry_opening(results, OPEN, clock=lambda: clock[0], pause=pause, run=run)
    assert main_pauses == [7]   # to :12, as before
    assert sorted(calls) == sorted(
        (s, w, OPEN, OPEN + timedelta(seconds=55)) for s, w in (("SPX", 0), ("SPY", 2.0), ("QQQ", 4.0))
    )
    assert all(r.startswith("recorded (opening retry") for r in results.values())


@pytest.mark.parametrize(
    "now,after,wait",
    [
        # Nov: New York is UTC-5. 09:29:30 ET, the minute just read was 09:29: the opening at 09:30:00.15.
        (datetime(2026, 11, 10, 14, 29, 30, 500000, tzinfo=timezone.utc), "14:29", 29.65),
        # The opening read ended at 09:30:20: next at 09:31:30, never 09:30:30.
        (datetime(2026, 11, 10, 14, 30, 20, tzinfo=timezone.utc), "14:30", 70.0),
        (datetime(2026, 11, 10, 17, 30, 40, tzinfo=timezone.utc), "17:30", 50.0),
        # A wake a moment early (read made at :29.9995) never reads the same minute twice.
        (datetime(2026, 11, 10, 17, 30, 29, 999500, tzinfo=timezone.utc), "17:30", 60.0005),
        # Startup mid-minute (no read made yet): this minute's :30.
        (datetime(2026, 11, 10, 17, 30, 10, tzinfo=timezone.utc), None, 20.0),
        (datetime(2026, 11, 10, 14, 30, 0, 50000, tzinfo=timezone.utc), None, 0.10),
        # July: New York is UTC-4, so the opening is 13:30Z.
        (datetime(2026, 7, 1, 13, 29, 31, tzinfo=timezone.utc), "13:29", 29.15),
        (datetime(2026, 7, 1, 14, 29, 31, tzinfo=timezone.utc), "14:29", 59.0),
    ],
)
def test_next_read_is_the_half_minute_and_the_opening_at_00_15(now, after, wait):
    from scripts.record_gamma_time import next_read

    if after is not None:
        h, m = map(int, after.split(":"))
        after = now.replace(hour=h, minute=m, second=0, microsecond=0)
    assert next_read(now, after=after) == pytest.approx(wait, abs=1e-6)

