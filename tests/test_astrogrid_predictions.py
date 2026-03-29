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
