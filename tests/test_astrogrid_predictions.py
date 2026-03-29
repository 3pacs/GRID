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


@patch("api.routers.astrogrid.publish_astrogrid_prediction")
@patch("api.routers.astrogrid.get_astrogrid_store")
def test_create_prediction_persists_and_returns_postmortem(mock_store_factory, mock_publish) -> None:
    mock_store = MagicMock()
    mock_store.save_prediction.return_value = {
        "prediction_id": "pred-1",
        "call": "press BTC",
        "timing": "now / ingress",
        "setup": "leader rotation",
        "invalidation": "break if regime flips",
        "status": "pending",
        "postmortem": {
            "state": "pending",
            "summary": "Pending swing read on BTC: press BTC. Break if regime flips.",
            "dominant_grid_drivers": ["regime:risk_on"],
            "dominant_mystical_drivers": ["moon:Full Moon"],
            "invalidation_rule": "break if regime flips",
            "feature_family_summary": {"grid": ["regime:risk_on"], "mystical": ["moon:Full Moon"]},
        },
    }
    mock_store_factory.return_value = mock_store
    mock_publish.return_value = {
        "status": "published",
        "oracle_prediction_id": "astrogrid:pred-1",
        "contract": "oracle.publish.v1",
    }

    response = client.post(
        "/api/v1/astrogrid/predictions",
        headers=_auth_header(),
        json={
            "question": "what crypto should i buy right now?",
            "call": "press BTC",
            "timing": "now / ingress",
            "setup": "leader rotation",
            "invalidation": "break if regime flips",
            "note": "keep the leash short",
            "seer": {"confidence": 0.72, "horizon": "days"},
            "snapshot": {"date": "2026-03-28", "lunar": {"phase_name": "Full Moon"}, "nakshatra": {"nakshatra_name": "Magha"}},
            "market_overlay_snapshot": {"regime": {"state": "risk_on"}},
            "engine_outputs": [{"engine_id": "western"}],
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert data["prediction_id"] == "pred-1"
    assert data["postmortem"]["state"] == "pending"
    assert "summary" in data["postmortem"]
    mock_publish.assert_called_once()
    mock_store.save_prediction.assert_called_once()


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_latest_predictions_returns_store_payload(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.list_predictions.return_value = [{"prediction_id": "pred-1"}]
    mock_store_factory.return_value = mock_store

    response = client.get("/api/v1/astrogrid/predictions/latest", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert data["predictions"] == [{"prediction_id": "pred-1"}]


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_postmortems_returns_store_payload(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.list_postmortems.return_value = [{"prediction_id": "pred-1", "postmortem": {"state": "pending"}}]
    mock_store_factory.return_value = mock_store

    response = client.get("/api/v1/astrogrid/postmortems", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert data["postmortems"][0]["postmortem"]["state"] == "pending"


@patch("api.routers.oracle.publish_astrogrid_prediction")
def test_oracle_publish_contract_route(mock_publish) -> None:
    mock_publish.return_value = {
        "status": "published",
        "oracle_prediction_id": "astrogrid:pred-1",
        "contract": "oracle.publish.v1",
    }
    response = client.post(
        "/api/v1/oracle/publish",
        headers=_auth_header(),
        json={
            "prediction_id": "pred-1",
            "question": "what crypto should i buy right now?",
            "call": "press BTC",
            "timing": "now / ingress",
            "invalidation": "break if regime flips",
        },
    )
    assert response.status_code == 200
    assert response.json()["contract"] == "oracle.publish.v1"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_score_predictions_route_returns_store_summary(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.score_predictions.return_value = {
        "evaluation_date": "2026-03-28",
        "candidates": 2,
        "scored": 1,
        "skipped_not_mature": 1,
        "skipped_no_price": 0,
        "verdicts": {"hit": 1, "miss": 0, "partial": 0, "invalidated": 0, "expired": 0},
        "prediction_ids": ["pred-1"],
    }
    mock_store_factory.return_value = mock_store

    response = client.post(
        "/api/v1/astrogrid/predictions/score",
        headers=_auth_header(),
        json={"as_of_date": "2026-03-28", "limit": 25},
    )
    assert response.status_code == 200
    assert response.json()["scored"] == 1
    mock_store.score_predictions.assert_called_once()


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_prediction_scoreboard_route_returns_scoreboard_and_weights(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.build_prediction_scoreboard.return_value = {"overall": {"scored": 3}}
    mock_store.ensure_active_weight_version.return_value = {"version_key": "astrogrid-v1", "status": "active"}
    mock_store_factory.return_value = mock_store

    response = client.get("/api/v1/astrogrid/predictions/scoreboard", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert data["scoreboard"]["overall"]["scored"] == 3
    assert data["weights"]["version_key"] == "astrogrid-v1"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_backtest_run_route_returns_store_payload(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.run_backtests.return_value = {"runs": [{"strategy_variant": "grid_only"}], "count": 1}
    mock_store_factory.return_value = mock_store

    response = client.post(
        "/api/v1/astrogrid/backtest/run",
        headers=_auth_header(),
        json={"strategy_variants": ["grid_only"], "window_start": "2026-01-01", "window_end": "2026-03-01"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["runs"][0]["strategy_variant"] == "grid_only"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_backtest_summary_and_results_routes_use_store(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.get_backtest_summary.return_value = {"latest_by_variant": {"grid_only": {"summary": {}}}, "history": []}
    mock_store.list_backtest_results.return_value = [{"strategy_variant": "grid_only", "metrics": {"verdict": "hit"}}]
    mock_store_factory.return_value = mock_store

    summary_response = client.get("/api/v1/astrogrid/backtest/summary", headers=_auth_header())
    assert summary_response.status_code == 200
    assert "grid_only" in summary_response.json()["latest_by_variant"]

    results_response = client.get(
        "/api/v1/astrogrid/backtest/results",
        headers=_auth_header(),
        params={"strategy_variant": "grid_only"},
    )
    assert results_response.status_code == 200
    assert results_response.json()["results"][0]["strategy_variant"] == "grid_only"


@patch("api.routers.astrogrid.get_astrogrid_store")
def test_current_weights_route_returns_active_version(mock_store_factory) -> None:
    mock_store = MagicMock()
    mock_store.ensure_active_weight_version.return_value = {
        "version_key": "astrogrid-v1",
        "status": "active",
        "grid_weights": {"regime": 0.9},
        "mystical_weights": {"seer": 0.2},
    }
    mock_store_factory.return_value = mock_store

    response = client.get("/api/v1/astrogrid/weights/current", headers=_auth_header())
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "active"
    assert data["grid_weights"]["regime"] == 0.9
