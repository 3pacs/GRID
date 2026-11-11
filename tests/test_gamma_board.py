"""Authenticated source gating, coherent captures and refresh behavior."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import sys
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from api import gamma_board as board
from api.auth import create_token
from api.routers.gamma_watch import router
from config import settings


@pytest.fixture(autouse=True)
def fresh_cache():
    board._CACHE.clear()
    yield
    board._CACHE.clear()


def test_admin_only_fixed_symbols_and_private_response(monkeypatch):
    from secrets import token_urlsafe

    monkeypatch.setenv("GRID_JWT_SECRET", token_urlsafe(48))
    app = FastAPI()
    app.include_router(router)
    read = Mock(return_value={"status": "unavailable"})
    monkeypatch.setattr(board, "read_panel", read)
    c = TestClient(app)
    path = "/api/v1/gamma-watch/board/"

    def auth(role):
        return {"Authorization": "Bearer " + create_token(role=role, expires_hours=1)}

    assert c.get(path + "SPY").status_code == 401
    assert c.get(path + "SPY", headers=auth("contributor")).status_code == 403
    assert c.get(path + "AAPL", headers=auth("admin")).status_code == 422
    read.assert_not_called()
    monkeypatch.setattr(settings, "SCHWAB_PRIVATE_GEX_BOARD_ENABLED", False)
    response = c.get(path + "QQQ", headers=auth("admin"))
    assert response.headers["cache-control"] == "no-store, private"
    assert response.headers["X-GRID-Audience"] == "private"
    read.assert_called_once_with("QQQ", False)


def test_single_flight_and_disabled_source_cannot_expose_schwab(monkeypatch):
    now = datetime.now(timezone.utc).isoformat()
    provider = Mock(
        return_value=dict(
            status="available", symbol="SPX", as_of=now, spot_as_of=now, source="Schwab"
        )
    )
    monkeypatch.setitem(
        sys.modules, "api.gamma_board_schwab", SimpleNamespace(read_board=provider)
    )
    with ThreadPoolExecutor(max_workers=6) as workers:
        responses = list(workers.map(lambda _: board.read_panel("SPX", True), range(8)))
    assert all(r["source"] == "Schwab" for r in responses)
    assert provider.call_count == 1
    off = board.read_panel("SPX", False)
    assert off["status"] == "unavailable"
    assert off["spot"] is None
    original = board._CACHE[("SPX", True)][1]
    board._CACHE[("SPX", True)] = (0, original)
    provider.side_effect = OSError("sensitive broker error")
    held = board.read_panel("SPX", True)
    assert held["stale"] and held["as_of"] == now
    assert "sensitive" not in str(held)
    board._CACHE[("SPX", True)] = (
        0,
        {
            **original,
            "as_of": (datetime.now(timezone.utc) - timedelta(days=9)).isoformat(),
        },
    )
    assert board.read_panel("SPX", True)["status"] == "unavailable"


def test_stale_spot_and_half_day_session():
    now = datetime.fromisoformat("2026-11-27T18:00:00+00:00")
    assert board.session_open(now - timedelta(minutes=1))
    assert not board.session_open(now)
    payload = dict(
        as_of=now.isoformat(), spot_as_of=(now - timedelta(hours=3)).isoformat()
    )
    assert board._stamp(payload, now)["stale"]


def test_immutable_batch_price_and_coverage(monkeypatch):
    from unittest.mock import MagicMock

    now = datetime.fromisoformat("2026-11-10T19:30:00+00:00")
    # A missing IV with positive OI cannot contribute a zero-dollar observation.
    batch = dict(
        capture_batch_id="batch",
        capture_completed_at=now,
        snap_date=now.date(),
        spot_price=100.0,
        spot_provider="fixture",
        provider_regular_market_at=now - timedelta(minutes=15),
    )
    con = MagicMock()

    def statement(stmt, params=None):
        result = MagicMock()
        if "options_capture_batches" in str(stmt):
            assert params["symbol"] == "SPY"
            result.mappings.return_value.first.return_value = batch
        if "options_snapshots_all" in str(stmt):
            assert (
                params["id"] == "batch"
                and params["start"] == now.date()
                and params["lo"] == 95.0
            )

            def row(exp, k, side, iv, oi):
                return dict(
                    expiry=exp,
                    strike=k,
                    opt_type=side,
                    implied_vol=iv,
                    open_interest=oi,
                )

            later = now.date() + timedelta(days=2)
            result.mappings.return_value.all.return_value = [
                row(now.date(), 100.0, "call", 0.2, 7),
                row(now.date(), 100.0, "put", 0.2, 2),
                row(later, 99.0, "put", 0.2, 100),
                row(later, 101.0, "call", None, 50),
                row(later, 101.0, "put", 0.2, 500),
            ]
        return result

    con.execute.side_effect = statement
    eng = MagicMock()
    eng.begin.return_value.__enter__.return_value = con
    monkeypatch.setattr(board, "get_db_engine", lambda: eng)
    data = board._database_panel("SPY", now)
    assert data["call_wall"] == 100 and data["put_wall"] == 99
    assert data["contracts"] == 3 and data["excluded_contracts"] == 2
    assert all(cell["strike"] != 101 for cell in data["cells"])
    assert data["cells"][0]["net_gex"] > 0
    assert sum(r["net_gex"] for r in data["cells"]) == pytest.approx(
        data["net_gex"], abs=0.02
    )
    assert str(con.execute.call_args_list[0].args[0]) == "SET TRANSACTION READ ONLY"
