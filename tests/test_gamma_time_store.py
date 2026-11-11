"""Replay, concurrency and access contracts for recorded minute exposures."""

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
import secrets
import sqlite3
import sys
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from api import gamma_time_store as store
from api.auth import create_token
from api.routers.gamma_watch import router
from config import settings

DAY = date(2026, 11, 10)
T0 = datetime(2026, 11, 10, 14, 30, 13, 123456, tzinfo=timezone.utc)
BASE = dict(
    session_date=DAY,
    observed_at=T0,
    expiry=DAY,
    spot=99.5,
    levels={99.0: 0, 100.0: 123.75},
    source="Synthetic",
)


def put(path, symbol="QQQ", **changes):
    return store.write_frame(symbol, path=path, **{**BASE, **changes})


def test_freeze_and_replay_sparse_frames_and_dates(tmp_path):
    path = tmp_path / "frames.sqlite3"
    assert put(path)
    assert not put(
        path, observed_at=T0 + timedelta(seconds=14), spot=120, levels={105.0: -13}
    )
    assert put(path, observed_at=T0 + timedelta(minutes=3), levels={101: -99, 99: 2})
    data = store.read_time_map("QQQ", DAY, path=path)
    assert data["source"] == "Synthetic" and data["expiry"] == "2026-11-10"
    assert data["times"] == [
        int(T0.timestamp() * 1000),
        int(T0.timestamp() * 1000) + 180000,
    ]
    assert data["strikes"] == [99, 100, 101]
    assert data["values"] == [[0, 2], [123.75, None], [None, -99]]
    assert data["spot"][0] == [data["times"][0], 99.5]
    # The first sample is not visible earlier in its own recording minute.
    assert (
        store.read_time_map("QQQ", DAY, now=T0 - timedelta(seconds=1), path=path)[
            "times"
        ]
        == []
    )
    assert path.stat().st_mode & 0o077 == 0
    with sqlite3.connect(path) as con:
        for sql in ("DELETE FROM frames", "UPDATE frames SET spot=1"):
            with pytest.raises(sqlite3.IntegrityError):
                con.execute(sql)
        con.execute(
            """INSERT OR REPLACE INTO frames SELECT symbol,session_date,minute,observed_at,expiry,1,source,levels,chain_as_of,spot_as_of,oi_retrieved_at FROM frames"""
        )
        assert con.execute("SELECT min(spot) FROM frames").fetchone() == (99.5,)


def test_concurrency_one_first_capture(tmp_path):
    path = tmp_path / "x.sqlite3"
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda i: put(path, spot=100 + i), range(12)))
    assert results.count(True) == 1
    result = store.read_time_map("QQQ", DAY, path=path)
    assert result["spot"][0][1] == 100 + results.index(True)


def test_expiry_source_locked_and_missing_never_creates(tmp_path):
    path = tmp_path / "missing"
    assert store.read_time_map("SPX", DAY, path=path)["status"] == "unavailable"
    assert not path.exists()
    expiry = DAY + timedelta(days=3)
    assert put(path, "AAPL", expiry=expiry)
    with pytest.raises(ValueError, match="source changed"):
        put(path, "AAPL", expiry=expiry, source="Other")
    with pytest.raises(ValueError, match="source changed"):
        put(path, "AAPL", expiry=expiry + timedelta(days=7))
    with pytest.raises(ValueError, match="Expiry outside"):
        put(path, "SPY", expiry=expiry)


@pytest.mark.parametrize(
    "changes",
    [
        {"observed_at": T0.replace(tzinfo=None)},
        {"observed_at": T0 - timedelta(minutes=1)},
        {"observed_at": T0 + timedelta(hours=7)},
        {"observed_at": T0 + timedelta(days=1)},
        {"levels": {}},
        {"levels": {100: float("inf")}},
        {"levels": {False: 0}},
        {"spot": 0},
        {"chain_as_of": T0 + timedelta(seconds=1)},
        {"spot_as_of": T0.replace(tzinfo=None)},
        {"levels": {k: 0 for k in range(1, 1026)}},
    ],
)
def test_invalid_frames_do_not_create_store(tmp_path, changes):
    path = tmp_path / "no-data"
    with pytest.raises(ValueError):
        put(path, **changes)
    assert not path.exists()


def test_previous_session_and_spy_archive_only_on_empty(tmp_path, monkeypatch):
    path = tmp_path / "receipts"
    spy = Mock(return_value={"status": "available", "source": "Archive"})
    monkeypatch.setitem(
        sys.modules, "api.gamma_time_spy", SimpleNamespace(read_spy=spy)
    )
    before_open = datetime(2026, 11, 11, 14, 29, tzinfo=timezone.utc)
    assert store.read_time_map("SPY", now=before_open, path=path)["source"] == "Archive"
    spy.assert_called_once_with(DAY)
    put(path, "SPY")
    assert (
        store.read_time_map("SPY", now=before_open, path=path)["source"] == "Synthetic"
    )
    assert spy.call_count == 1


def test_api_access_no_store_tracking_and_date(monkeypatch, tmp_path):
    monkeypatch.setenv("GRID_JWT_SECRET", secrets.token_urlsafe(48))
    monkeypatch.setenv("GRID_GAMMA_TIME_DB", str(tmp_path / "uncreated.sqlite"))
    monkeypatch.setitem(
        sys.modules,
        "api.gamma_time_watchlist",
        SimpleNamespace(is_tracked=lambda s: s == "QQQ"),
    )
    app = FastAPI()
    app.include_router(router)
    c = TestClient(app)

    def auth(role):
        return {"Authorization": "Bearer " + create_token(role=role, expires_hours=1)}

    url = "/api/v1/gamma-watch/time-map/"
    assert c.get(url + "QQQ").status_code == 401
    assert c.get(url + "QQQ", headers=auth("viewer")).status_code == 403
    assert c.get(url + "aapl", headers=auth("admin")).status_code == 422
    assert (
        c.get(url + "QQQ?session_date=never", headers=auth("admin")).status_code == 422
    )
    monkeypatch.setattr(settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", False)
    assert c.get(url + "QQQ", headers=auth("admin")).json()["tracking"] is False
    monkeypatch.setattr(settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", True)
    res = c.get(url + "QQQ?session_date=2026-11-10", headers=auth("admin"))
    assert (
        res.headers["cache-control"] == "no-store, private"
        and res.json()["tracking"] is True
    )
    assert res.json()["session_start"] == "2026-11-10T14:30:00+00:00"
    assert not (tmp_path / "uncreated.sqlite").exists()


def test_provider_clocks_preserved_on_retry(tmp_path):
    path = tmp_path / "clock"
    earlier = T0 - timedelta(minutes=7)
    assert put(path, chain_as_of=earlier, spot_as_of=earlier, oi_retrieved_at=T0)
    assert not put(path, chain_as_of=T0)
    data = store.read_time_map("QQQ", DAY, path=path)
    assert (data["chain_as_of"], data["spot_as_of"], data["oi_retrieved_at"]) == (
        earlier.isoformat(),
        earlier.isoformat(),
        T0.isoformat(),
    )
