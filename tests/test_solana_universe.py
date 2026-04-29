"""
Tests for trading/solana/universe.py.

rank_to_score is a pure curve function — no mocks. UniverseRegistry
queries are tested against a scripted mock engine.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine import Engine

from trading.solana.universe import (
    UniverseRank,
    UniverseRegistry,
    rank_to_score,
)


# ----------------------------------------------------------------------
# rank_to_score
# ----------------------------------------------------------------------
def test_rank_to_score_top_is_one():
    assert rank_to_score(1, limit=250) == 1.0


def test_rank_to_score_bottom_is_zero():
    assert rank_to_score(250, limit=250) == 0.0


def test_rank_to_score_monotone_decreasing():
    ranks = [1, 5, 10, 25, 50, 100, 250]
    scores = [rank_to_score(r, limit=250) for r in ranks]
    for a, b in zip(scores, scores[1:]):
        assert a > b


def test_rank_to_score_none_returns_zero():
    assert rank_to_score(None, limit=250) == 0.0


def test_rank_to_score_out_of_range_returns_zero():
    assert rank_to_score(0, limit=250) == 0.0
    assert rank_to_score(-5, limit=250) == 0.0
    assert rank_to_score(9999, limit=250) == 0.0


def test_rank_to_score_tier_rough_checks():
    # Log curve — rank=10 should be ~0.58, rank=50 ~0.29, rank=100 ~0.17
    assert rank_to_score(10, limit=250) == pytest.approx(0.5834, abs=0.01)
    assert rank_to_score(50, limit=250) == pytest.approx(0.2917, abs=0.01)
    assert rank_to_score(100, limit=250) == pytest.approx(0.1666, abs=0.01)


def test_rank_to_score_handles_degenerate_limit():
    assert rank_to_score(1, limit=1) == 0.0
    assert rank_to_score(1, limit=0) == 0.0


# ----------------------------------------------------------------------
# UniverseRegistry
# ----------------------------------------------------------------------
NOW = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)


def _mock_engine():
    engine = MagicMock(spec=Engine)
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def test_get_latest_rank_returns_row():
    engine, conn = _mock_engine()
    result = MagicMock()
    result.fetchone.return_value = ("MINT1", 5, 1_500_000.0, NOW)
    conn.execute.return_value = result

    registry = UniverseRegistry(engine)
    rank = registry.get_latest_rank("MINT1")
    assert rank is not None
    assert rank.mint == "MINT1"
    assert rank.rank == 5
    assert rank.volume_24h_usd == 1_500_000.0
    assert rank.snapshot_at == NOW


def test_get_latest_rank_not_found():
    engine, conn = _mock_engine()
    result = MagicMock()
    result.fetchone.return_value = None
    conn.execute.return_value = result

    registry = UniverseRegistry(engine)
    assert registry.get_latest_rank("MINT1") is None


def test_get_latest_rank_empty_input():
    engine, conn = _mock_engine()
    registry = UniverseRegistry(engine)
    assert registry.get_latest_rank("") is None
    conn.execute.assert_not_called()


def test_get_latest_rank_db_error_returns_none():
    engine, conn = _mock_engine()
    conn.execute.side_effect = RuntimeError("db down")
    registry = UniverseRegistry(engine)
    assert registry.get_latest_rank("MINT1") is None


def test_get_latest_snapshot_returns_rows():
    engine, conn = _mock_engine()
    result = MagicMock()
    result.fetchall.return_value = [
        ("M1", 1, 5_000_000.0, NOW),
        ("M2", 2, 3_000_000.0, NOW),
        ("M3", 3, 1_000_000.0, NOW),
    ]
    conn.execute.return_value = result

    registry = UniverseRegistry(engine)
    ranks = registry.get_latest_snapshot(limit=10)
    assert len(ranks) == 3
    assert ranks[0].rank == 1
    assert ranks[0].mint == "M1"


def test_get_latest_snapshot_db_error_returns_empty():
    engine, conn = _mock_engine()
    conn.execute.side_effect = RuntimeError("db down")
    registry = UniverseRegistry(engine)
    assert registry.get_latest_snapshot() == []
