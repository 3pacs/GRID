"""
Tests for MCP export endpoints (api/routers/mcp_export.py).

Tests all 8 MCP endpoints with mocked database connections.
Validates: auth requirement, query correctness, NaN/None handling,
response shape, and edge cases (empty results, missing actors).
"""

from __future__ import annotations

import math
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Set test environment before importing the app
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
_TEST_PASSWORD = "testpassword123"
if "GRID_MASTER_PASSWORD_HASH" not in os.environ:
    os.environ["GRID_MASTER_PASSWORD_HASH"] = _pwd_ctx.hash(_TEST_PASSWORD)

from api.auth import create_token
from api.main import app

client = TestClient(app)

MCP_PREFIX = "/api/v1/mcp"


def _auth_header() -> dict[str, str]:
    token = create_token(expires_hours=1)
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Helper: mock engine context manager
# ---------------------------------------------------------------------------

def _mock_engine_ctx(mock_engine, rows=None, fetchone_val=None):
    """Configure mock engine to return rows from execute().fetchall() or fetchone()."""
    mock_eng = MagicMock()
    mock_engine.return_value = mock_eng
    mock_conn = MagicMock()
    mock_eng.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_eng.connect.return_value.__exit__ = MagicMock(return_value=False)

    result_mock = MagicMock()
    if rows is not None:
        result_mock.fetchall.return_value = rows
    if fetchone_val is not None:
        result_mock.fetchone.return_value = fetchone_val
    else:
        result_mock.fetchone.return_value = None

    mock_conn.execute.return_value = result_mock
    return mock_conn


# ── Auth Tests ────────────────────────────────────────────────────────────


class TestMCPAuthRequired:
    """All MCP endpoints require authentication."""

    @pytest.mark.parametrize("endpoint", [
        "/trust-score?actor=test",
        "/actor-profile?name=test",
        "/predictions",
        "/prediction-accuracy",
        "/data-freshness",
        "/signal-sources",
        "/wealth-flows",
        "/regime",
    ])
    def test_no_auth_returns_401(self, endpoint):
        response = client.get(f"{MCP_PREFIX}{endpoint}")
        assert response.status_code == 401


# ── 1. Trust Score ────────────────────────────────────────────────────────


class TestMCPTrustScore:

    @patch("api.routers.mcp_export.get_db_engine")
    def test_actor_found(self, mock_engine):
        row = ("ACT001", "Warren Buffett", "tier_1", 0.92, "high",
               0.88, datetime(2026, 3, 15, 10, 0, 0))
        _mock_engine_ctx(mock_engine, fetchone_val=row)

        resp = client.get(
            f"{MCP_PREFIX}/trust-score?actor=Warren+Buffett",
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["actor_id"] == "ACT001"
        assert data["name"] == "Warren Buffett"
        assert data["trust_score"] == 0.92
        assert data["credibility"] == "high"
        assert data["influence_score"] == 0.88
        assert data["window_days"] == 90

    @patch("api.routers.mcp_export.get_db_engine")
    def test_actor_not_found(self, mock_engine):
        _mock_engine_ctx(mock_engine, fetchone_val=None)

        resp = client.get(
            f"{MCP_PREFIX}/trust-score?actor=Nobody",
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is False
        assert "not found" in data["error"]

    @patch("api.routers.mcp_export.get_db_engine")
    def test_nan_trust_score_becomes_none(self, mock_engine):
        row = ("ACT002", "Test Actor", "tier_3", float("nan"), "low",
               float("inf"), datetime(2026, 3, 1))
        _mock_engine_ctx(mock_engine, fetchone_val=row)

        resp = client.get(
            f"{MCP_PREFIX}/trust-score?actor=Test+Actor",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["trust_score"] is None  # NaN → None
        assert data["influence_score"] is None  # Inf → None

    def test_missing_actor_param(self):
        resp = client.get(
            f"{MCP_PREFIX}/trust-score",
            headers=_auth_header(),
        )
        assert resp.status_code == 422  # Missing required param

    @patch("api.routers.mcp_export.get_db_engine")
    def test_custom_window_days(self, mock_engine):
        row = ("ACT001", "Test", "tier_1", 0.5, "medium", 0.6, None)
        _mock_engine_ctx(mock_engine, fetchone_val=row)

        resp = client.get(
            f"{MCP_PREFIX}/trust-score?actor=Test&window_days=180",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["window_days"] == 180


# ── 2. Actor Profile ─────────────────────────────────────────────────────


class TestMCPActorProfile:

    @patch("api.routers.mcp_export.get_db_engine")
    def test_profile_found(self, mock_engine):
        row = (
            "ACT001", "BlackRock", "tier_1", "asset_management",
            Decimal("10000000000"), 0.95, "profit_maximizer",
            ["Vanguard", "State Street"], ["SPY", "QQQ"],
            ["Apple Inc Board"], ["Democratic Party"],
            "high", 0.97, {"founded": 1988},
            datetime(2026, 3, 20),
        )
        _mock_engine_ctx(mock_engine, fetchone_val=row)

        resp = client.get(
            f"{MCP_PREFIX}/actor-profile?name=BlackRock",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        p = data["profile"]
        assert p["id"] == "ACT001"
        assert p["name"] == "BlackRock"
        assert p["category"] == "asset_management"
        assert p["connections"] == ["Vanguard", "State Street"]
        assert p["known_positions"] == ["SPY", "QQQ"]
        assert p["board_seats"] == ["Apple Inc Board"]
        assert p["metadata"] == {"founded": 1988}

    @patch("api.routers.mcp_export.get_db_engine")
    def test_profile_not_found(self, mock_engine):
        _mock_engine_ctx(mock_engine, fetchone_val=None)

        resp = client.get(
            f"{MCP_PREFIX}/actor-profile?name=Ghost",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is False

    @patch("api.routers.mcp_export.get_db_engine")
    def test_null_json_fields_default_to_empty(self, mock_engine):
        row = (
            "ACT002", "Solo", "tier_3", "individual",
            None, None, None,
            None, None, None, None,  # all JSON fields null
            None, None, None,
            datetime(2026, 1, 1),
        )
        _mock_engine_ctx(mock_engine, fetchone_val=row)

        resp = client.get(
            f"{MCP_PREFIX}/actor-profile?name=Solo",
            headers=_auth_header(),
        )
        p = resp.json()["profile"]
        assert p["connections"] == []
        assert p["known_positions"] == []
        assert p["board_seats"] == []
        assert p["political_affiliations"] == []
        assert p["metadata"] == {}
        assert p["aum"] is None


# ── 3. Predictions ───────────────────────────────────────────────────────


class TestMCPPredictions:

    @patch("api.routers.mcp_export.get_db_engine")
    def test_predictions_all(self, mock_engine):
        rows = [
            (1, "AAPL", "ensemble_v3", "bullish", 0.82,
             175.0, 190.0, 170.0, "hit",
             188.5, 0.077, datetime(2026, 3, 1), datetime(2026, 3, 15)),
            (2, "TSLA", "momentum_v2", "bearish", 0.65,
             250.0, 220.0, 260.0, "pending",
             None, None, datetime(2026, 3, 10), None),
        ]
        _mock_engine_ctx(mock_engine, rows=rows)

        resp = client.get(
            f"{MCP_PREFIX}/predictions",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 2
        assert data["predictions"][0]["ticker"] == "AAPL"
        assert data["predictions"][0]["verdict"] == "hit"
        assert data["predictions"][1]["actual_price"] is None

    @patch("api.routers.mcp_export.get_db_engine")
    def test_predictions_by_symbol(self, mock_engine):
        rows = [
            (3, "BTC", "crypto_v1", "bullish", 0.71,
             65000.0, 72000.0, 62000.0, "pending",
             None, None, datetime(2026, 3, 20), None),
        ]
        _mock_engine_ctx(mock_engine, rows=rows)

        resp = client.get(
            f"{MCP_PREFIX}/predictions?symbol=BTC&lookback_days=30",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1
        assert data["predictions"][0]["ticker"] == "BTC"

    @patch("api.routers.mcp_export.get_db_engine")
    def test_predictions_empty(self, mock_engine):
        _mock_engine_ctx(mock_engine, rows=[])

        resp = client.get(
            f"{MCP_PREFIX}/predictions",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 0
        assert data["predictions"] == []


# ── 4. Prediction Accuracy ──────────────────────────────────────────────


class TestMCPPredictionAccuracy:

    @patch("api.routers.mcp_export.get_db_engine")
    def test_accuracy_by_model(self, mock_engine):
        rows = [
            ("ensemble_v3", 100, 65, 15, 20, 0.035),
            ("momentum_v2", 50, 20, 10, 20, -0.012),
        ]
        _mock_engine_ctx(mock_engine, rows=rows)

        resp = client.get(
            f"{MCP_PREFIX}/prediction-accuracy?group_by=model",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["group_by"] == "model"
        assert len(data["accuracy"]) == 2
        grp = data["accuracy"][0]
        assert grp["group"] == "ensemble_v3"
        assert grp["total"] == 100
        assert grp["hits"] == 65
        assert grp["hit_rate"] == 0.65

    def test_invalid_group_by(self):
        resp = client.get(
            f"{MCP_PREFIX}/prediction-accuracy?group_by=invalid",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is False
        assert "group_by" in data["error"]

    @patch("api.routers.mcp_export.get_db_engine")
    def test_accuracy_zero_total(self, mock_engine):
        rows = [("empty_model", 0, 0, 0, 0, None)]
        _mock_engine_ctx(mock_engine, rows=rows)

        resp = client.get(
            f"{MCP_PREFIX}/prediction-accuracy",
            headers=_auth_header(),
        )
        data = resp.json()
        grp = data["accuracy"][0]
        assert grp["hit_rate"] == 0
        assert grp["avg_pnl"] is None


# ── 5. Data Freshness ────────────────────────────────────────────────────


class TestMCPDataFreshness:

    @patch("api.routers.mcp_export.get_db_engine")
    def test_freshness_mixed_statuses(self, mock_engine):
        today = date.today()
        rows = [
            ("FRED", "api", today - timedelta(days=2), today - timedelta(days=3), 5000),
            ("BLS", "scrape", today - timedelta(days=15), today - timedelta(days=30), 200),
            ("ECB", "api", today - timedelta(days=60), today - timedelta(days=90), 100),
            ("EMPTY_SRC", "manual", None, None, 0),
        ]
        _mock_engine_ctx(mock_engine, rows=rows)

        resp = client.get(
            f"{MCP_PREFIX}/data-freshness",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["total"] == 4

        statuses = {s["name"]: s["status"] for s in data["sources"]}
        assert statuses["FRED"] == "fresh"
        assert statuses["BLS"] == "stale"
        assert statuses["ECB"] == "dead"
        assert statuses["EMPTY_SRC"] == "empty"

        # stale_count should be BLS + ECB = 2
        assert data["stale_count"] == 2

    @patch("api.routers.mcp_export.get_db_engine")
    def test_freshness_empty(self, mock_engine):
        _mock_engine_ctx(mock_engine, rows=[])

        resp = client.get(
            f"{MCP_PREFIX}/data-freshness",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["total"] == 0
        assert data["stale_count"] == 0


# ── 6. Signal Sources ────────────────────────────────────────────────────


class TestMCPSignalSources:

    @patch("api.routers.mcp_export.get_db_engine")
    def test_signals_all(self, mock_engine):
        rows = [
            ("AAPL", "insider", "INS_001", "cluster_buy",
             date(2026, 3, 25), 0.88, "CORRECT"),
            ("TSLA", "darkpool", "DP_002", "block_trade",
             date(2026, 3, 24), 0.72, "PENDING"),
        ]
        _mock_engine_ctx(mock_engine, rows=rows)

        resp = client.get(
            f"{MCP_PREFIX}/signal-sources",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 2
        assert data["signals"][0]["ticker"] == "AAPL"
        assert data["signals"][0]["trust_score"] == 0.88

    @patch("api.routers.mcp_export.get_db_engine")
    def test_signals_by_symbol(self, mock_engine):
        rows = [
            ("AAPL", "congressional", "CONG_001", "trade_disclosure",
             date(2026, 3, 20), 0.65, "PENDING"),
        ]
        _mock_engine_ctx(mock_engine, rows=rows)

        resp = client.get(
            f"{MCP_PREFIX}/signal-sources?symbol=AAPL",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1


# ── 7. Wealth Flows ──────────────────────────────────────────────────────


class TestMCPWealthFlows:

    @patch("api.routers.mcp_export.get_db_engine")
    def test_flows_all(self, mock_engine):
        rows = [
            (date(2026, 3, 15), "BlackRock", "Vanguard SPY",
             Decimal("5000000000"), "high", "ETF rebalancing"),
            (date(2026, 3, 10), "Fed", "Treasury Bonds",
             Decimal("20000000000"), "confirmed", "QT runoff"),
        ]
        _mock_engine_ctx(mock_engine, rows=rows)

        resp = client.get(
            f"{MCP_PREFIX}/wealth-flows",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 2
        assert data["flows"][0]["from"] == "BlackRock"
        assert data["flows"][0]["to"] == "Vanguard SPY"

    @patch("api.routers.mcp_export.get_db_engine")
    def test_flows_by_actor(self, mock_engine):
        rows = [
            (date(2026, 3, 1), "Buffett", "Apple Inc",
             Decimal("1000000000"), "confirmed", "13F filing"),
        ]
        _mock_engine_ctx(mock_engine, rows=rows)

        resp = client.get(
            f"{MCP_PREFIX}/wealth-flows?actor=Buffett",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 1

    @patch("api.routers.mcp_export.get_db_engine")
    def test_flows_empty(self, mock_engine):
        _mock_engine_ctx(mock_engine, rows=[])

        resp = client.get(
            f"{MCP_PREFIX}/wealth-flows",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["count"] == 0


# ── 8. Regime ────────────────────────────────────────────────────────────


class TestMCPRegime:

    @patch("api.routers.mcp_export.get_db_engine")
    def test_regime_current_and_history(self, mock_engine):
        mock_conn = MagicMock()
        mock_eng = MagicMock()
        mock_engine.return_value = mock_eng
        mock_eng.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_eng.connect.return_value.__exit__ = MagicMock(return_value=False)

        # First execute → current regime (fetchone)
        current_result = MagicMock()
        current_result.fetchone.return_value = (
            "Expansion", 0.87, datetime(2026, 3, 28, 14, 0, 0)
        )

        # Second execute → history (fetchall)
        history_result = MagicMock()
        history_result.fetchall.return_value = [
            (date(2026, 3, 28), "Expansion", 0.87),
            (date(2026, 3, 27), "Expansion", 0.85),
            (date(2026, 3, 26), "Contraction", 0.72),
        ]

        mock_conn.execute.side_effect = [current_result, history_result]

        resp = client.get(
            f"{MCP_PREFIX}/regime",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["current"]["state"] == "Expansion"
        assert data["current"]["confidence"] == 0.87
        assert len(data["history"]) == 3
        assert data["history"][2]["state"] == "Contraction"

    @patch("api.routers.mcp_export.get_db_engine")
    def test_regime_no_data(self, mock_engine):
        mock_conn = MagicMock()
        mock_eng = MagicMock()
        mock_engine.return_value = mock_eng
        mock_eng.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_eng.connect.return_value.__exit__ = MagicMock(return_value=False)

        current_result = MagicMock()
        current_result.fetchone.return_value = None

        history_result = MagicMock()
        history_result.fetchall.return_value = []

        mock_conn.execute.side_effect = [current_result, history_result]

        resp = client.get(
            f"{MCP_PREFIX}/regime",
            headers=_auth_header(),
        )
        data = resp.json()
        assert data["ok"] is True
        assert data["current"] is None
        assert data["history"] == []


# ── Helper Tests ─────────────────────────────────────────────────────────


class TestSafeHelpers:
    """Test the _safe_float and _safe_iso helpers from mcp_export."""

    def test_safe_float_normal(self):
        from api.routers.mcp_export import _safe_float
        assert _safe_float(3.14) == 3.14
        assert _safe_float(0) == 0.0
        assert _safe_float(Decimal("99.99")) == 99.99

    def test_safe_float_nan_inf(self):
        from api.routers.mcp_export import _safe_float
        assert _safe_float(float("nan")) is None
        assert _safe_float(float("inf")) is None
        assert _safe_float(float("-inf")) is None

    def test_safe_float_none_and_bad(self):
        from api.routers.mcp_export import _safe_float
        assert _safe_float(None) is None
        assert _safe_float("not-a-number") is None

    def test_safe_iso_datetime(self):
        from api.routers.mcp_export import _safe_iso
        dt = datetime(2026, 3, 30, 12, 0, 0)
        assert _safe_iso(dt) == "2026-03-30T12:00:00"

    def test_safe_iso_date(self):
        from api.routers.mcp_export import _safe_iso
        d = date(2026, 3, 30)
        assert _safe_iso(d) == "2026-03-30"

    def test_safe_iso_none(self):
        from api.routers.mcp_export import _safe_iso
        assert _safe_iso(None) is None

    def test_safe_iso_string_fallback(self):
        from api.routers.mcp_export import _safe_iso
        assert _safe_iso("2026-03-30") == "2026-03-30"
