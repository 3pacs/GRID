"""Dedicated coverage for the `/api/v1/system/health` pool-healthy calculation.

Kept separate from tests/test_system_router.py because this targets one
specific behavior change: `pool_healthy` now compares checked_out against
the shared `get_pool_stats()` helper's `capacity` (pool_size + configured
max_overflow) instead of the previous `pool_size + pool.overflow()` — the
old comparison used SQLAlchemy's live overflow counter (which can be
negative) rather than the actual configured ceiling. The response
contract (`pool_healthy`, `pool_size`, `pool_checked_out`, `pool_overflow`
keys, same types) is asserted unchanged.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")
os.environ.setdefault(
    "GRID_MASTER_PASSWORD_HASH",
    "$2b$12$abcdefghijklmnopqrstuuFb1mY3p5oXq0rN8sxqf6vV2QcVx1zSi",
)

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def _engine_with_pool(pool_size: int, max_overflow: int, checked_out: int, overflow: int):
    """A mock engine whose .pool reports the given SQLAlchemy pool counters.

    /health also runs a couple of plain SELECTs before reaching the pool
    check; a MagicMock connection with a generic execute() satisfies those
    without needing a real database.
    """
    engine = MagicMock()
    mock_conn = MagicMock()
    mock_result = MagicMock()
    mock_result.scalar.return_value = 1
    mock_result.fetchone.return_value = (5,)
    mock_conn.execute.return_value = mock_result
    engine.connect.return_value.__enter__.return_value = mock_conn
    engine.connect.return_value.__exit__.return_value = False

    engine.pool = MagicMock()
    engine.pool.size.return_value = pool_size
    engine.pool.checkedout.return_value = checked_out
    engine.pool.checkedin.return_value = pool_size - checked_out
    engine.pool.overflow.return_value = overflow
    engine.pool._max_overflow = max_overflow
    return engine


def test_pool_healthy_true_when_checked_out_below_capacity():
    engine = _engine_with_pool(pool_size=20, max_overflow=30, checked_out=15, overflow=-5)
    with patch("api.routers.system.get_db_engine", return_value=engine):
        resp = client.get("/api/v1/system/health")

    assert resp.status_code == 200
    data = resp.json()
    # Response contract preserved — same keys, same shape.
    assert data["checks"]["pool_healthy"] is True
    assert data["checks"]["pool_size"] == 20
    assert data["checks"]["pool_checked_out"] == 15
    assert data["checks"]["pool_overflow"] == -5
    assert "connection pool exhausted" not in data["degraded_reasons"]


def test_pool_unhealthy_at_full_capacity():
    """checked_out has reached pool_size + max_overflow — the real ceiling,
    not the old (and buggy) pool_size + live-overflow comparison.
    """
    engine = _engine_with_pool(pool_size=20, max_overflow=30, checked_out=50, overflow=30)
    with patch("api.routers.system.get_db_engine", return_value=engine):
        resp = client.get("/api/v1/system/health")

    assert resp.status_code == 200
    data = resp.json()
    assert data["checks"]["pool_healthy"] is False
    assert "connection pool exhausted" in data["degraded_reasons"]


def test_pool_healthy_not_fooled_by_negative_live_overflow():
    """Regression guard for the bug this change fixed: with the old
    `checked_out < pool_size + overflow()` comparison, a negative live
    overflow (fewer live connections than pool_size — the normal resting
    state) could shrink the effective threshold below checked_out and
    report unhealthy even though usage is nowhere near real capacity.
    """
    engine = _engine_with_pool(pool_size=20, max_overflow=30, checked_out=12, overflow=-10)
    with patch("api.routers.system.get_db_engine", return_value=engine):
        resp = client.get("/api/v1/system/health")

    data = resp.json()
    # Old buggy logic: 12 < 20 + (-10) == 12 < 10 == False (wrongly unhealthy).
    # Correct logic: 12 < 20 + 30 == 12 < 50 == True.
    assert data["checks"]["pool_healthy"] is True
