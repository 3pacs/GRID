"""docs/reference/CONFIDENCE_POLICY.md on the catalyst-timeline read path.

``api/routers/valuation.py::catalyst_timeline`` serialised oracle
predictions and discovered hypotheses with ``float(x) if x else None``,
which turned a measured ``0.0`` confidence into ``None`` — the reader then
showed an unscored event where a scored 0% was stored. The check must be
``is not None``: a NULL is unscored, a 0.0 is a measurement.

The route is exercised through the FastAPI app with the engine mocked, so
the pin is on the JSON the PWA actually receives.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

os.environ.setdefault("DB_PASSWORD", "test-password")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _pwd_ctx.hash("testpassword123"))

from fastapi.testclient import TestClient

from api.auth import create_token
from api.main import app

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    return {"Authorization": f"Bearer {create_token(expires_hours=1)}"}


def _today():
    return datetime.now(timezone.utc).date()


def _pred_row(pred_id: int, confidence):
    expiry = _today() + timedelta(days=30)
    # id, prediction_type, direction, target_price, entry_price, expiry,
    # confidence, expected_move_pct, model_name, verdict, actual_price,
    # actual_move_pct, score_notes, created_at
    return (
        pred_id, "price_target", "bullish", 120.0, 100.0, expiry,
        confidence, 0.2, "m", None, None, None, None, _today(),
    )


def _hyp_row(hyp_id: int, confidence):
    # id, thesis, role, confidence, invalidation, status, kill_reason,
    # pattern_type, times_tested, times_correct, created_at
    return (
        hyp_id, "ACME does a thing", "hypothesis", confidence, "inv", "active",
        None, "pattern", 4, 2, _today(),
    )


def _wire_db(mock_db, pred_rows, hyp_rows):
    conn = MagicMock()

    def execute(stmt, *_a, **_kw):
        sql = str(getattr(stmt, "text", stmt))
        result = MagicMock()
        if "FROM oracle_predictions" in sql:
            result.fetchall.return_value = pred_rows
        elif "FROM discovered_hypotheses" in sql:
            result.fetchall.return_value = hyp_rows
        else:
            result.fetchall.return_value = []
            result.fetchone.return_value = None
        return result

    conn.execute.side_effect = execute
    mock_db.return_value.connect.return_value.__enter__ = MagicMock(return_value=conn)
    mock_db.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)


def _get(ticker: str = "ACME") -> dict:
    resp = client.get(
        f"/api/v1/valuation/catalyst-timeline/{ticker}", headers=_auth_header()
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


class TestCatalystTimelineConfidence:
    @patch("api.routers.valuation.get_db_engine")
    def test_null_prediction_confidence_ships_as_null(self, mock_db):
        _wire_db(mock_db, pred_rows=[_pred_row(1, None)], hyp_rows=[])
        events = {e["id"]: e for e in _get()["events"]}
        assert events["pred-1"]["confidence"] is None

    @patch("api.routers.valuation.get_db_engine")
    def test_zero_prediction_confidence_is_a_measurement_not_unscored(self, mock_db):
        _wire_db(mock_db, pred_rows=[_pred_row(2, 0.0)], hyp_rows=[])
        events = {e["id"]: e for e in _get()["events"]}
        assert events["pred-2"]["confidence"] == 0.0
        assert events["pred-2"]["confidence"] is not None

    @patch("api.routers.valuation.get_db_engine")
    def test_zero_hypothesis_confidence_is_a_measurement_not_unscored(self, mock_db):
        _wire_db(mock_db, pred_rows=[], hyp_rows=[_hyp_row(7, 0.0)])
        events = {e["id"]: e for e in _get()["events"]}
        assert events["hyp-7"]["confidence"] == 0.0

    @patch("api.routers.valuation.get_db_engine")
    def test_no_midpoint_is_ever_invented(self, mock_db):
        _wire_db(mock_db, pred_rows=[_pred_row(3, None)], hyp_rows=[_hyp_row(8, None)])
        events = _get()["events"]
        assert len(events) == 2
        for e in events:
            assert e["confidence"] is None
