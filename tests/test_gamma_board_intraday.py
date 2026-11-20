"""Real auth, cash-session boundaries, missing candles and bounded quote requests."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import secrets
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from api import gamma_board_intraday as chart
from api.auth import create_token
from api.routers.gamma_watch import router
from config import settings


@pytest.fixture(autouse=True)
def reset():
    chart._CACHE.clear()
    chart._ERRORS.clear()
    chart._STARTS.clear()
    yield
    chart._CACHE.clear()
    chart._ERRORS.clear()
    chart._STARTS.clear()


@pytest.mark.parametrize(
    ("now", "date", "expected"),
    [
        ("2026-11-11T14:29:59+00:00", "2026-11-10", 390),
        ("2026-11-11T14:30:59+00:00", "2026-11-11", 0),
        ("2026-11-11T14:32:55+00:00", "2026-11-11", 2),
        ("2026-11-26T17:00:00+00:00", "2026-11-25", 390),
        ("2026-11-27T18:03:00+00:00", "2026-11-27", 210),
        ("2026-11-29T17:00:00+00:00", "2026-11-27", 210),
        ("2026-03-09T13:31:00+00:00", "2026-03-09", 1),
    ],
)
def test_session_calendar(now, date, expected):
    start, end, cutoff = chart._session(datetime.fromisoformat(now))
    assert start.date().isoformat() == date
    assert (cutoff - start) / chart.MINUTE == expected


def row(start, i, **updates):
    return {
        "datetime": int((start + i * chart.MINUTE).timestamp() * 1000),
        "open": 100,
        "high": 104,
        "low": 99,
        "close": 103,
        "volume": 44,
        **updates,
    }


def test_no_interpolation_or_incomplete_conflicting_bars(monkeypatch):
    now = datetime.fromisoformat("2026-11-11T15:00:20+00:00")
    monkeypatch.setattr(chart, "_clock", lambda: now)
    start, end, cut = chart._session(now)
    rows = [
        row(start, 10),
        row(start, 0),
        row(start, 0),
        row(start, 2),
        row(start, 2, close=101),
        row(start, -1),
        row(start, 3, low=104),
        row(start, 30),
        row(start, 31),
        row(start, 5, volume=-2),
    ]
    reader = Mock(return_value={"symbol": "$SPX", "candles": rows})
    monkeypatch.setattr(chart, "_history", reader)
    out = chart.read_intraday("SPX", True)
    assert [b["time"] for b in out["bars"]] == [
        (start + i * chart.MINUTE).isoformat() for i in [0, 5, 10]
    ]
    assert all(b["volume"] is None for b in out["bars"])
    assert out["expected_bars"] == 30 and out["missing_bars"] == 27
    assert out["as_of"] == (start + 11 * chart.MINUTE).isoformat()
    reader.assert_called_once_with("SPX", start, cut)
    assert not chart._bars(
        "QQQ",
        {"symbol": "QQQ", "candles": [row(start, 0, open=float("nan"))]},
        start,
        cut,
    )
    with pytest.raises(ValueError):
        chart._bars("QQQ", {"symbol": "SPY", "candles": rows}, start, cut)


def test_cache_singleflight_flag_failure_and_symbol_budget(monkeypatch):
    now = datetime.fromisoformat("2026-11-11T15:00:20+00:00")
    monkeypatch.setattr(chart, "_clock", lambda: now)
    start = chart._session(now)[0]
    reader = Mock(return_value={"symbol": "QQQ", "candles": [row(start, 0, volume=0)]})
    monkeypatch.setattr(chart, "_history", reader)
    with ThreadPoolExecutor(max_workers=8) as pool:
        panels = list(pool.map(lambda _: chart.read_intraday("QQQ", True), range(10)))
    assert reader.call_count == 1 and all(p["bars"][0]["volume"] == 0 for p in panels)
    assert not chart.read_intraday("QQQ", False)["bars"]
    monkeypatch.setattr(chart, "_LIMIT_PER_MINUTE", 2)
    reader.side_effect = OSError("private-token")
    fail = chart.read_intraday("SPY", True)
    assert fail["status"] == "unavailable" and "private-token" not in str(fail)
    assert chart.read_intraday("SPY", True) == fail
    assert "limit" in chart.read_intraday("AAPL", True)["reason"]
    assert reader.call_count == 2


def test_before_first_minute_never_fetches(monkeypatch):
    now = datetime.fromisoformat("2026-11-11T14:30:30+00:00")
    monkeypatch.setattr(chart, "_clock", lambda: now)
    reader = Mock()
    monkeypatch.setattr(chart, "_history", reader)
    result = chart.read_intraday("QQQ", True)
    assert (
        not result["bars"] and result["expected_bars"] == 0 and result["session_open"]
    )
    reader.assert_not_called()


def test_intraday_route_private_role_flag_and_validator(monkeypatch):
    monkeypatch.setenv("GRID_JWT_SECRET", secrets.token_urlsafe(48))
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    path = "/api/v1/gamma-watch/intraday/"
    auth = lambda role: dict(
        Authorization="Bearer " + create_token(role=role, expires_hours=1)
    )
    fetch = Mock()
    monkeypatch.setattr(chart, "_history", fetch)
    monkeypatch.setattr(settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", False)
    assert client.get(path + "SPX").status_code == 401
    assert client.get(path + "SPX", headers=auth("viewer")).status_code == 403
    for bad in ("lower", "AAAAAA", "AA.B", "Q1", "*"):
        assert client.get(path + bad, headers=auth("admin")).status_code == 422
    res = client.get(path + "AAPL", headers=auth("admin"))
    assert (
        res.headers["cache-control"] == "no-store, private"
        and res.json()["status"] == "unavailable"
    )
    fetch.assert_not_called()
