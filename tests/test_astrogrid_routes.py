"""
Targeted AstroGrid API route tests.

These are lightweight contract tests for the current AstroGrid router surface.
They intentionally avoid depending on the future learning-loop endpoints.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
_TEST_PASSWORD = "testpassword123"
_TEST_HASH = _pwd_ctx.hash(_TEST_PASSWORD)
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _TEST_HASH)

from api.auth import create_token
from api.main import app

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    token = create_token(expires_hours=1)
    return {"Authorization": f"Bearer {token}"}


def test_astrogrid_core_routes_require_auth() -> None:
    paths = [
        ("get", "/api/v1/astrogrid/overview"),
        ("get", "/api/v1/astrogrid/snapshot"),
        ("get", "/api/v1/astrogrid/scorecard"),
        ("get", "/api/v1/astrogrid/universe"),
        ("get", "/api/v1/astrogrid/ephemeris"),
        ("get", "/api/v1/astrogrid/correlations"),
        ("post", "/api/v1/astrogrid/interpret"),
    ]
    for method, path in paths:
        response = getattr(client, method)(path)
        assert response.status_code == 401, f"{method.upper()} {path} should require auth"


def test_astrogrid_snapshot_invalid_date_returns_error_payload() -> None:
    response = client.get(
        "/api/v1/astrogrid/snapshot",
        params={"date": "not-a-date"},
        headers=_auth_header(),
    )
    assert response.status_code == 200
    data = response.json()
    assert "error" in data
    assert "Invalid date format" in data["error"]


@patch("api.routers.astrogrid_core.get_db_engine")
def test_astrogrid_overview_gracefully_handles_backend_failure(mock_engine) -> None:
    mock_engine.side_effect = Exception("DB unavailable")
    response = client.get("/api/v1/astrogrid/overview", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert "error" in data
    assert "as_of" in data


@patch("api.routers.watchlist._batch_fetch_prices")
@patch("api.routers.astrogrid_core._build_scorecard_evaluation")
@patch("api.routers.astrogrid_core._load_scorecard_history")
@patch("api.routers.astrogrid_core._resolve_scorecard_feature")
@patch("api.routers.astrogrid_core.enrich_astrogrid_scoreable_universe")
@patch("api.routers.astrogrid_core.get_db_engine")
def test_astrogrid_scorecard_returns_shape_with_minimal_data(
    mock_engine,
    mock_enrich_universe,
    mock_resolve_feature,
    mock_load_history,
    mock_build_evaluation,
    mock_quotes,
) -> None:
    mock_quotes.return_value = {}
    mock_resolve_feature.return_value = (None, [])
    mock_load_history.return_value = []
    mock_enrich_universe.return_value = [
        {
            "symbol": "SPY",
            "label": "S&P 500",
            "group": "macro",
            "asset_class": "macro",
            "lookup_ticker": "SPY",
            "price_feature": "spy_full",
            "benchmark_symbol": "SPY",
            "status": "scoreable_now",
            "scoreable_now": True,
            "reason_if_not": None,
        }
    ]
    mock_build_evaluation.return_value = {
        "overall": {
            "total_predictions": 0,
            "scored": 0,
            "pending": 0,
            "hits": 0,
            "misses": 0,
            "partials": 0,
            "accuracy": 0.0,
            "avg_pnl": 0.0,
            "total_pnl": 0.0,
        },
        "by_symbol": [],
    }
    mock_conn = MagicMock()
    mock_engine.return_value.connect.return_value.__enter__ = lambda s: mock_conn
    mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)

    response = client.get("/api/v1/astrogrid/scorecard", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert data["universe"]["id"] == "hybrid_v1"
    assert data["universe"]["assets"][0]["symbol"] == "SPY"
    assert "items" in data
    assert "summary" in data
    assert "evaluation" in data


@patch("api.routers.astrogrid_core.enrich_astrogrid_scoreable_universe")
@patch("api.routers.astrogrid_core.get_db_engine")
def test_astrogrid_universe_route_returns_contract_counts(
    mock_engine,
    mock_enrich_universe,
) -> None:
    mock_enrich_universe.return_value = [
        {
            "symbol": "SPY",
            "asset_class": "equity",
            "price_feature": "spy_full",
            "benchmark_symbol": "SPY",
            "status": "scoreable_now",
            "scoreable_now": True,
            "reason_if_not": None,
        },
        {
            "symbol": "QQQ",
            "asset_class": "equity",
            "price_feature": "qqq_full",
            "benchmark_symbol": "SPY",
            "status": "degraded",
            "scoreable_now": False,
            "reason_if_not": "needs backfill",
        },
    ]
    mock_conn = MagicMock()
    mock_engine.return_value.connect.return_value.__enter__ = lambda s: mock_conn
    mock_engine.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)

    response = client.get("/api/v1/astrogrid/universe", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert data["counts"]["scoreable_now"] == 1
    assert data["counts"]["degraded"] == 1
    assert data["counts"]["total"] == 2
