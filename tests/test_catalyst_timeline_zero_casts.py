"""A stored 0.0 measurement on the catalyst timeline ships as 0.0, not null.

``api/routers/valuation.py::catalyst_timeline`` serialised measurement columns
with ``float(x) if x else None``. That test is truthiness, and ``0.0`` is
falsy, so a milestone whose value impact was measured at exactly zero, or a
scored prediction whose actual move was 0.0%, reached the PWA as ``null`` —
indistinguishable from "never measured". The casts for the five measurement
fields now use ``is not None``:

  milestones:   value_impact_ps, value_impact_pct
  predictions:  expected_move_pct, actual_price, actual_move_pct

``target_price`` / ``entry_price`` are deliberately NOT changed here: a zero
entry price has its own contract (``oracle/entry_price_policy.py`` on the
confidence-policy branch, not yet on main) and is settled before any division
rather than reported as a plain 0.0. ``confidence`` is likewise out of scope —
it is the handoff patch ``handoff/catalyst-timeline-unscored``.

The route is exercised through the FastAPI app with the engine mocked, so the
pin is on the JSON the PWA actually receives.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

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


def _milestone_row(ms_id: int, *, value_impact_ps, value_impact_pct):
    target = _today() + timedelta(days=20)
    # id, milestone_type, description, target_date, actual_date,
    # target_value, target_unit, actual_value, achievement_pct,
    # probability, confidence_source, value_impact_ps, value_impact_pct,
    # status, notes, announced_date
    return (
        ms_id, "EARNINGS_GUIDANCE", "guide", target, None,
        1.0, "USD", None, None,
        None, "ANALYST", value_impact_ps, value_impact_pct,
        "PENDING", None, _today(),
    )


def _pred_row(pred_id: int, *, expected_move_pct, actual_price, actual_move_pct):
    expiry = _today() + timedelta(days=30)
    # id, prediction_type, direction, target_price, entry_price, expiry,
    # confidence, expected_move_pct, model_name, verdict, actual_price,
    # actual_move_pct, score_notes, created_at
    return (
        pred_id, "price_target", "bullish", 120.0, 100.0, expiry,
        0.6, expected_move_pct, "m", "hit", actual_price,
        actual_move_pct, None, _today(),
    )


def _wire_db(mock_db, *, ms_rows=(), pred_rows=()):
    conn = MagicMock()

    def execute(stmt, *_a, **_kw):
        sql = str(getattr(stmt, "text", stmt))
        result = MagicMock()
        if "FROM company_milestones" in sql:
            result.fetchall.return_value = list(ms_rows)
        elif "FROM oracle_predictions" in sql:
            result.fetchall.return_value = list(pred_rows)
        else:
            result.fetchall.return_value = []
            result.fetchone.return_value = None
        return result

    conn.execute.side_effect = execute
    mock_db.return_value.connect.return_value.__enter__ = MagicMock(return_value=conn)
    mock_db.return_value.connect.return_value.__exit__ = MagicMock(return_value=False)


def _events(ticker: str = "ACME") -> dict[str, dict]:
    resp = client.get(
        f"/api/v1/valuation/catalyst-timeline/{ticker}", headers=_auth_header()
    )
    assert resp.status_code == 200, resp.text
    return {e["id"]: e for e in resp.json()["events"]}


# ── milestones ─────────────────────────────────────────────────────────────


class TestMilestoneValueImpactCasts:
    @patch("api.routers.valuation.get_db_engine")
    def test_zero_impact_ships_as_zero(self, mock_db):
        _wire_db(mock_db, ms_rows=[_milestone_row(1, value_impact_ps=0.0, value_impact_pct=0.0)])
        ev = _events()["ms-1"]
        assert ev["value_impact_ps"] == 0.0
        assert ev["value_impact_ps"] is not None
        assert ev["value_impact_pct"] == 0.0
        assert ev["value_impact_pct"] is not None

    @patch("api.routers.valuation.get_db_engine")
    def test_null_impact_ships_as_null(self, mock_db):
        _wire_db(mock_db, ms_rows=[_milestone_row(2, value_impact_ps=None, value_impact_pct=None)])
        ev = _events()["ms-2"]
        assert ev["value_impact_ps"] is None
        assert ev["value_impact_pct"] is None

    @patch("api.routers.valuation.get_db_engine")
    def test_nonzero_impact_unchanged(self, mock_db):
        _wire_db(mock_db, ms_rows=[_milestone_row(3, value_impact_ps=1.25, value_impact_pct=0.05)])
        ev = _events()["ms-3"]
        assert ev["value_impact_ps"] == pytest.approx(1.25)
        assert ev["value_impact_pct"] == pytest.approx(0.05)


# ── oracle predictions ─────────────────────────────────────────────────────


class TestPredictionMeasurementCasts:
    @patch("api.routers.valuation.get_db_engine")
    def test_zero_measurements_ship_as_zero(self, mock_db):
        _wire_db(mock_db, pred_rows=[
            _pred_row(1, expected_move_pct=0.0, actual_price=0.0, actual_move_pct=0.0),
        ])
        ev = _events()["pred-1"]
        for field in ("expected_move_pct", "actual_price", "actual_move_pct"):
            assert ev[field] == 0.0, field
            assert ev[field] is not None, field

    @patch("api.routers.valuation.get_db_engine")
    def test_null_measurements_ship_as_null(self, mock_db):
        _wire_db(mock_db, pred_rows=[
            _pred_row(2, expected_move_pct=None, actual_price=None, actual_move_pct=None),
        ])
        ev = _events()["pred-2"]
        for field in ("expected_move_pct", "actual_price", "actual_move_pct"):
            assert ev[field] is None, field

    @patch("api.routers.valuation.get_db_engine")
    def test_price_casts_are_not_touched_here(self, mock_db):
        # A zero entry/target price stays on its existing path (currently
        # null) until the entry-price contract lands on main. This pin makes
        # a later change to that behaviour deliberate rather than incidental.
        row = list(_pred_row(3, expected_move_pct=0.1, actual_price=101.0, actual_move_pct=0.01))
        row[3] = 0.0  # target_price
        row[4] = 0.0  # entry_price
        _wire_db(mock_db, pred_rows=[tuple(row)])
        ev = _events()["pred-3"]
        assert ev["target_price"] is None
        assert ev["entry_price"] is None
