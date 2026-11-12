from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json
import secrets
import sqlite3

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import create_token
from api import gamma_band_store as store
from api.routers.gamma_watch import router
from config import settings
from scripts import watch_spy_bands as worker

UTC = timezone.utc
DAY = date(2026, 11, 13)
T = int(datetime(2026, 11, 13, 16, 28, tzinfo=UTC).timestamp() // 60)
END = int(datetime(2026, 11, 13, 21, 0, tzinfo=UTC).timestamp() // 60)
LEVELS = {99: 5, 100: -8, 101: 5}


def stamp(minute, seconds=0):
    return datetime.fromtimestamp(minute * 60 + seconds, UTC)


def candle(low, high, close):
    return dict(open=close, high=high, low=low, close=close)


def tick(path, m, bar, levels=LEVELS, stamp_s=7):
    store.advance(
        day=DAY,
        bars={m: bar},
        latest=m,
        levels=levels,
        session_end=END,
        path=path,
        clock=lambda: stamp(m + 1, stamp_s),
    )


def issue_pair(path):
    store.initialize(path)
    tick(path, T - 1, candle(99.2, 99.8, 99.6))
    tick(path, T, candle(99.6, 100.4, 100.2))
    tick(
        path, T + 1, candle(99.65, 101.3, 99.8)
    )  # target in partial issuance minute only
    tick(path, T + 2, candle(99.6, 100.4, 100.2))
    tick(path, T + 3, candle(99.4, 100.3, 99.7))
    tick(path, T + 3, candle(99.4, 100.3, 99.7))  # restart / duplicate


def test_persisted_pair_with_post_issue_targets_and_time_buckets(tmp_path):
    p = tmp_path / "tracker.db"
    issue_pair(p)
    s = store.snapshot(DAY, path=p)
    assert len(s["events"]) == 2 and all(v["status"] == "pending" for v in s["events"])
    failed, cross = s["events"]
    assert (
        cross["signal_time"],
        cross["issued_at"],
        cross["bucket"],
        cross["target"],
    ) == (stamp(T + 1).isoformat(), stamp(T + 1, 7).isoformat(), "morning", 101)
    assert failed["bucket"] == "midday" and failed["target"] == 99
    tick(p, T + 4, candle(98.9, 100.2, 99.5))
    assert store.snapshot(DAY, path=p)["events"][0]["status"] == "pending"
    tick(p, T + 5, candle(98.9, 99.9, 99.3))
    tick(p, T + 6, candle(99.5, 101.2, 101))
    s = store.snapshot(DAY, path=p)
    assert [(e["kind"], e["status"], e["outcome_time"]) for e in s["events"]] == [
        ("failed_reclaim", "hit", stamp(T + 6).isoformat()),
        ("first_cross", "hit", stamp(T + 7).isoformat()),
    ]
    assert sum(x["hits"] for x in s["stats"] if x["bucket"] == "all") == 2
    assert s["as_of"] == stamp(T + 7).isoformat()
    with sqlite3.connect(p) as db, pytest.raises(sqlite3.IntegrityError):
        db.execute("UPDATE flag SET target=75")


@pytest.mark.parametrize("missing,status", [(False, "miss"), (True, "pending")])
def test_expiry_requires_full_coverage(tmp_path, missing, status):
    p = tmp_path / "tracker.db"
    store.initialize(p)
    tick(p, T - 1, candle(99.2, 99.8, 99.6))
    tick(p, T, candle(99.6, 100.4, 100.2))
    bars = {
        m: candle(99.7, 100.5, 100.3)
        for m in range(T + 2, T + 32)
        if not (missing and m == T + 4)
    }
    store.advance(
        day=DAY,
        bars=bars,
        latest=None,
        levels=None,
        session_end=END,
        clock=lambda: stamp(T + 37),
        path=p,
    )
    assert store.snapshot(DAY, path=p)["events"][0]["status"] == status


def test_worker_reads_prior_frame_and_never_issues_on_historical_backfill(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", True)
    frames = tmp_path / "source.sqlite3"
    journal = tmp_path / "score.sqlite3"
    store.initialize(journal)
    with sqlite3.connect(frames) as con:
        con.execute(
            "CREATE TABLE frames(symbol,session_date,minute,expiry,observed_at,levels)"
        )
        for m in (T - 2, T - 1):
            con.execute(
                "INSERT INTO frames VALUES (?,?,?,?,?,?)",
                (
                    "SPY",
                    str(DAY),
                    m,
                    str(DAY),
                    stamp(m, 4).isoformat(),
                    json.dumps(list(LEVELS.items())),
                ),
            )

    def fetch(sym, start, cut):
        return {
            "symbol": sym,
            "candles": [
                dict(candle(99.2, 100.4, cl), datetime=m * 60000)
                for m, cl in [(T - 1, 99.6), (T, 100.2)]
                if start <= stamp(m) < cut
            ],
        }

    for m in (T - 1, T):
        worker.cycle(
            clock=lambda: stamp(m + 1, 7),
            fetch=fetch,
            frames_path=frames,
            journal=journal,
        )
    assert store.snapshot(DAY, path=journal)["events"][0]["kind"] == "first_cross"
    empty = tmp_path / "cold.sqlite3"
    store.initialize(empty)
    worker.cycle(
        clock=lambda: stamp(T + 3, 7), fetch=fetch, frames_path=frames, journal=empty
    )
    assert not store.snapshot(DAY, path=empty)["events"]


def test_private_endpoint_modes(tmp_path, monkeypatch):
    monkeypatch.setenv("GRID_JWT_SECRET", secrets.token_urlsafe(48))
    monkeypatch.setenv("GRID_GAMMA_BAND_DB", str(tmp_path / "journal.sqlite3"))
    monkeypatch.setattr(settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", True)
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    url = "/api/v1/gamma-watch/spy-band-flags?session_date=2026-11-13"
    assert client.get(url).status_code == 401
    viewer = {"Authorization": "Bearer " + create_token(role="viewer", expires_hours=1)}
    assert client.get(url, headers=viewer).status_code == 403
    auth = {"Authorization": "Bearer " + create_token(role="admin", expires_hours=1)}
    r = client.get(url, headers=auth)
    assert (
        r.json()["status"] == "unavailable"
        and r.headers["Cache-Control"] == "no-store, private"
    )
    assert not Path(tmp_path / "journal.sqlite3").exists()
    store.initialize()
    assert client.get(url, headers=auth).json()["status"] == "available"
    monkeypatch.setattr(settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", False)
    assert client.get(url, headers=auth).json()["status"] == "unavailable"


def test_gap_through_and_shortened_close_window(tmp_path):
    path = tmp_path / "s.db"
    store.initialize(path)
    tick(path, END - 6, candle(99.2, 99.7, 99.6))
    tick(path, END - 5, candle(99.6, 100.4, 100.2))
    for m in range(END - 3, END):
        tick(path, m, candle(100.1, 100.4, 100.2))
    store.settle_expired(lambda: stamp(END + 1), path)
    assert store.snapshot(DAY, path=path)["events"][0]["status"] == "miss"
    other = tmp_path / "gap.db"
    store.initialize(other)
    tick(other, T - 1, candle(99.2, 99.7, 99.6))
    tick(other, T, candle(99.6, 100.4, 100.2))
    tick(other, T + 2, candle(101.1, 101.4, 101.3))
    assert store.snapshot(DAY, path=other)["events"][0]["status"] == "hit"
    with sqlite3.connect(other) as db:
        db.execute("insert or replace into grade values(1,'miss',1,'forged')")
    assert store.snapshot(DAY, path=other)["events"][0]["status"] == "hit"


def test_discovery_session_cannot_enter_forward_journal(tmp_path):
    journal = tmp_path / "score.db"
    store.initialize(journal)
    day = date(2026, 11, 12)
    store.advance(
        day=day,
        bars={},
        latest=None,
        levels=None,
        session_end=END,
        clock=lambda: stamp(T),
        path=journal,
    )
    s = store.snapshot(day, path=journal)
    assert s["events"] == [] and all(not r["hits"] for r in s["stats"])
    with sqlite3.connect(journal) as db:
        assert db.execute("SELECT count(*) FROM tracker_day").fetchone()[0] == 0
