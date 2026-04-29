from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_auth
import api.routers.options as options_module
from api.routers.options import router as options_router
from api.routers.watchlist import router as watchlist_router
import api.routers.watchlist_analysis as watchlist_analysis

_MISSING = object()


def _build_client(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[require_auth] = lambda: "test-token"
    return TestClient(app)


def _result(*, fetchone=_MISSING, fetchall=_MISSING) -> MagicMock:
    result = MagicMock()
    if fetchone is not _MISSING:
        result.fetchone.return_value = fetchone
    if fetchall is not _MISSING:
        result.fetchall.return_value = fetchall
    return result


def _engine_with_results(*results: MagicMock) -> MagicMock:
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.side_effect = list(results)
    engine.connect.return_value.__enter__.return_value = conn
    engine.connect.return_value.__exit__.return_value = False
    engine.begin.return_value.__enter__.return_value = conn
    engine.begin.return_value.__exit__.return_value = False
    return engine


def test_watchlist_analysis_allows_unsaved_ticker(monkeypatch) -> None:
    client = _build_client(watchlist_router)
    engine = _engine_with_results(
        _result(fetchone=None),
        _result(fetchall=[]),
        _result(fetchall=[]),
        _result(fetchone=None),
        _result(fetchall=[]),
        _result(fetchall=[]),
    )

    monkeypatch.setattr(watchlist_analysis, "_get_analysis_cached", lambda *_args: None)
    monkeypatch.setattr(watchlist_analysis, "_init_table", lambda: None)
    monkeypatch.setattr(watchlist_analysis, "get_db_engine", lambda: engine)
    monkeypatch.setattr(
        watchlist_analysis,
        "_resolve_feature_names",
        lambda ticker: [f"{ticker.lower()}_close"],
    )
    monkeypatch.setattr(
        watchlist_analysis,
        "_fetch_live_price",
        lambda _ticker: {"price": 200.0, "prev_close": 198.0, "pct_1d": 0.01, "source": "live"},
    )
    monkeypatch.setattr(watchlist_analysis, "_cache_price_to_db", lambda *_args, **_kwargs: None)

    response = client.get("/api/v1/watchlist/GD/analysis")

    assert response.status_code == 200
    body = response.json()
    assert body["ticker"] == "GD"
    assert body["watchlist_saved"] is False
    assert body["watchlist_item"]["ticker"] == "GD"
    assert body["watchlist_item"]["display_name"]
    assert body["price_source"] in {"live", "yfinance"}


def test_options_recommendations_fall_back_to_saved_rows(monkeypatch) -> None:
    client = _build_client(options_router)
    engine = _engine_with_results(
        _result(
            fetchall=[
                (
                    "NVDA",
                    "CALL",
                    950.0,
                    date(2026, 5, 15),
                    12.5,
                    21.0,
                    7.0,
                    0.68,
                    0.05,
                    0.82,
                    "Dealer squeeze still intact",
                    '{"passed": true}',
                    "Gamma support holding above spot",
                    datetime(2026, 4, 18, 15, 0, tzinfo=timezone.utc),
                    None,
                )
            ]
        )
    )

    monkeypatch.setattr(options_module, "get_db_engine", lambda: engine)

    def _missing_engine(_engine, *, force_refresh: bool = False) -> dict:
        raise ImportError("missing recommender")

    monkeypatch.setattr(options_module, "_generate_recommendations", _missing_engine)

    response = client.get("/api/v1/options/recommendations?ticker=NVDA")

    assert response.status_code == 200
    body = response.json()
    assert body["scan_summary"]["source"] == "persisted"
    assert body["scan_summary"]["fresh_scan"] is False
    assert len(body["recommendations"]) == 1
    assert body["recommendations"][0]["ticker"] == "NVDA"
    assert body["recommendations"][0]["sanity_status"] == {"passed": True}
