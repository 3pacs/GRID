"""Regression tests for the /api/v1/system/freshness stale_sources field.

Prior to this fix, the handler in api/routers/system.py constructed
FreshnessResponse and then attached ``stale_sources`` to ``resp.dict()``
before returning. Because the route is decorated
``response_model=FreshnessResponse`` and the model did not declare
``stale_sources``, FastAPI filtered the field out before serialization.
The frontend staleness-indicator data therefore never reached the client.

These tests pin the field on the response so it cannot regress.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import patch

# Match the test environment from tests/test_api.py so the app imports
# with a valid JWT secret regardless of which test loads first.
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")
os.environ.setdefault(
    "GRID_MASTER_PASSWORD_HASH",
    "$2b$12$abcdefghijklmnopqrstuuFb1mY3p5oXq0rN8sxqf6vV2QcVx1zSi",
)

from fastapi.testclient import TestClient  # noqa: E402

from api.auth import create_token  # noqa: E402
from api.main import app  # noqa: E402

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    return {"Authorization": f"Bearer {create_token(expires_hours=1)}"}


def test_freshness_includes_stale_sources_field_when_empty(mock_engine):
    """Empty DB still surfaces the stale_sources key (default: [])."""
    with patch("api.routers.system.get_db_engine", return_value=mock_engine):
        resp = client.get("/api/v1/system/freshness", headers=_auth_header())
    assert resp.status_code == 200
    data = resp.json()
    assert "stale_sources" in data, (
        "stale_sources must appear in the response — response_model=FreshnessResponse "
        "previously stripped it because the field was not declared on the schema"
    )
    assert data["stale_sources"] == []


def test_freshness_returns_stale_sources_rows(mock_engine):
    """Stale-source rows reach the client with the expected shape."""
    stale_dt = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    conn = mock_engine.connect.return_value.__enter__.return_value
    # First connect() runs the families query (empty); second runs the
    # stale-sources query and must yield the rows below.
    conn.execute.return_value.fetchall.side_effect = [
        [],
        [("fred:UNRATE", stale_dt), ("eia:gasoline_price", None)],
    ]
    with patch("api.routers.system.get_db_engine", return_value=mock_engine):
        resp = client.get("/api/v1/system/freshness", headers=_auth_header())
    assert resp.status_code == 200
    data = resp.json()
    assert data["stale_sources"] == [
        {
            "source": "fred:UNRATE",
            "last_pull": stale_dt.isoformat(),
            "stale": True,
        },
        {
            "source": "eia:gasoline_price",
            "last_pull": None,
            "stale": True,
        },
    ]
