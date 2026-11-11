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
