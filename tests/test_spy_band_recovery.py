"""Recovery only grades an already-issued flag, using its original post-issuance window."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
import sqlite3
from threading import Event

import pytest

from api import gamma_band_store as store
from config import settings
from scripts import watch_spy_bands as worker

UTC = timezone.utc
M = int(datetime(2026, 11, 13, 15, 10, tzinfo=UTC).timestamp() / 60)


def at(m):
    return datetime.fromtimestamp(m * 60, UTC)


def setup_flag(path):
    store.initialize(path)
    with sqlite3.connect(path) as db:
        db.execute(
            """INSERT INTO flag(day,kind,red,lower,upper,target,close,signal_time,issued_at,bucket,first_bar,deadline)
            VALUES ('2026-11-13','first_cross',100,99,101,101,100.4,?,?,'morning',?,?)""",
            (
                at(M - 1).isoformat(),
                (at(M - 1) + timedelta(seconds=6)).isoformat(),
                M,
                M + 30,
            ),
        )
    return path


def result(path):
    with sqlite3.connect(path) as db:
        return db.execute("SELECT status,outcome_minute FROM grade").fetchall()


def payload(minutes, hit=None):
    return {
        "symbol": "SPY",
        "candles": [
            dict(
                datetime=m * 60000,
                open=100,
                close=100,
                high=102 if m == hit else 100.5,
                low=99,
                volume=1,
            )
            for m in minutes
        ],
    }


@pytest.fixture(autouse=True)
def enable(monkeypatch):
    monkeypatch.setattr(settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", True)


@pytest.mark.parametrize(("hit", "status"), [(M + 12, "hit"), (None, "miss")])
def test_prior_day_restart_recovers_and_grades_only_the_frozen_window(
    tmp_path, hit, status
):
    p = setup_flag(tmp_path / "db")
    now = at(M) + timedelta(days=3)  # Monday startup, Friday flag.
    seen = []

    def fetch(symbol, start, end):
        seen.append((symbol, start, end))
        return payload(range(M - 2, M + 32), hit)

    store.settle_expired(lambda: now, p)
    assert not result(p)
    worker.recover_one(clock=lambda: now, fetch=fetch, journal=p)
    assert seen == [("SPY", at(M), at(M + 30))]
    assert result(p) == [(status, hit + 1 if hit else M + 30)]
    with sqlite3.connect(p) as db:
        assert db.execute(
            "select count(*),min(minute),max(minute) from observed_bar"
        ).fetchone() == (30, M, M + 29)
        assert db.execute("select count(*) from tracker_day").fetchone() == (0,)
        assert db.execute("select count(*) from flag").fetchone() == (1,)


def test_incomplete_response_outage_and_durable_attempt_limit(tmp_path):
    p = setup_flag(tmp_path / "db")
    # A missing minute prevents a miss. A partial response cannot overwrite the frozen bar.
    with sqlite3.connect(p) as db:
        db.execute("insert into observed_bar values('2026-11-13',?,99,100.5)", (M,))
    worker.recover_one(
        clock=lambda: at(M + 35),
        fetch=lambda *a: payload(range(M, M + 29), M),
        journal=p,
    )
    assert result(p) == []
    worker.recover_one(
        clock=lambda: at(M + 36),
        fetch=lambda *a: pytest.fail("too early to retry"),
        journal=p,
    )

    def unavailable(*args):
        raise OSError("example provider failure")

    worker.recover_one(clock=lambda: at(M + 37), fetch=unavailable, journal=p)
    assert result(p) == [("censored", M + 30)]
    worker.recover_one(
        clock=lambda: at(M + 99),
        fetch=lambda *a: pytest.fail("too many reads"),
        journal=p,
    )


def test_slow_remote_read_does_not_hold_write_lock_and_crashed_claims_expire(tmp_path):
    p = setup_flag(tmp_path / "db")
    pending = Event()
    release = Event()

    def fetch(*args):
        pending.set()
        assert release.wait(5)
        return payload([])

    with ThreadPoolExecutor(1) as pool:
        done = pool.submit(
            worker.recover_one, clock=lambda: at(M + 35), fetch=fetch, journal=p
        )
        assert pending.wait(5)
        store.settle_expired(lambda: at(M + 35), p)
        assert store.claim_recovery(at(M + 35), p) is None
        release.set()
        done.result(5)
    # Final attempt died before commit. A fresh worker sees the durable claim.
    assert store.claim_recovery(at(M + 37), p)[1] == 2
    assert store.claim_recovery(at(M + 38), p) is None
    assert result(p) == []
    store.settle_expired(lambda: at(M + 39), p)
    assert result(p) == [("censored", M + 30)]
