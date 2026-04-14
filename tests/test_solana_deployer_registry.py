"""
Tests for trading/solana/deployer_registry.py.

The pure ``score_deployer`` function is exhaustively unit-tested; the
``DeployerRegistry`` DB glue is exercised with a mock engine whose
execute() returns canned rows.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine import Engine

from trading.solana.deployer_registry import (
    DEFAULT_WEIGHTS,
    DeployerRegistry,
    DeployerScoreWeights,
    DeployerStats,
    _median,
    _normalise_hold,
    _recency_multiplier,
    score_deployer,
)


NOW = datetime(2026, 4, 13, tzinfo=timezone.utc)


def _stats(
    n_launches: int = 5,
    n_graduated: int = 3,
    median_peak: float = 300_000.0,
    best_peak: float = 1_000_000.0,
    avg_hold: float = 900.0,
    last_launch: datetime | None = None,
) -> DeployerStats:
    if last_launch is None:
        last_launch = NOW - timedelta(days=5)
    return DeployerStats(
        wallet="W1",
        n_launches=n_launches,
        n_graduated=n_graduated,
        median_peak_mc_usd=median_peak,
        best_peak_mc_usd=best_peak,
        avg_hold_seconds=avg_hold,
        last_launch_at=last_launch,
    )


# ----------------------------------------------------------------------
# _median
# ----------------------------------------------------------------------
def test_median_odd():
    assert _median([3.0, 1.0, 2.0]) == 2.0


def test_median_even():
    assert _median([1.0, 2.0, 3.0, 4.0]) == 2.5


def test_median_empty():
    assert _median([]) == 0.0


# ----------------------------------------------------------------------
# _normalise_hold
# ----------------------------------------------------------------------
def test_normalise_hold_fast_dump():
    assert _normalise_hold(30) == 0.0


def test_normalise_hold_mid_range():
    v = _normalise_hold(180)  # 3 min
    assert 0.0 < v < 0.5


def test_normalise_hold_long_hold():
    assert _normalise_hold(1200) > 0.5  # 20 min
    assert _normalise_hold(3600) == 1.0  # 60 min


# ----------------------------------------------------------------------
# _recency_multiplier
# ----------------------------------------------------------------------
def test_recency_multiplier_today_is_one():
    r = _recency_multiplier(NOW, NOW, half_life_days=60)
    assert r == pytest.approx(1.0)


def test_recency_multiplier_half_life_is_half():
    past = NOW - timedelta(days=60)
    r = _recency_multiplier(past, NOW, half_life_days=60)
    assert r == pytest.approx(0.5, abs=1e-4)


def test_recency_multiplier_none_returns_zero():
    assert _recency_multiplier(None, NOW, 60) == 0.0


# ----------------------------------------------------------------------
# score_deployer — edge cases
# ----------------------------------------------------------------------
def test_score_deployer_below_min_samples_returns_zero():
    stats = _stats(n_launches=2)
    result = score_deployer(stats, DEFAULT_WEIGHTS, now=NOW)
    assert result.score == 0.0
    assert any("fewer than" in r for r in result.reasons)


def test_score_deployer_strong_signal():
    stats = _stats(
        n_launches=10,
        n_graduated=8,
        median_peak=800_000.0,
        avg_hold=1800.0,
        last_launch=NOW - timedelta(days=2),
    )
    result = score_deployer(stats, DEFAULT_WEIGHTS, now=NOW)
    assert result.score > 0.5
    assert result.components["graduation_rate"] == 0.8
    assert result.components["confidence"] == 1.0
    assert result.components["recency_multiplier"] > 0.9


def test_score_deployer_old_launches_decay():
    stats_recent = _stats(last_launch=NOW - timedelta(days=5))
    stats_stale = _stats(last_launch=NOW - timedelta(days=120))
    recent = score_deployer(stats_recent, DEFAULT_WEIGHTS, now=NOW)
    stale = score_deployer(stats_stale, DEFAULT_WEIGHTS, now=NOW)
    assert recent.score > stale.score


def test_score_deployer_confidence_grows_with_n():
    weights = DeployerScoreWeights()
    stats_small = _stats(n_launches=3, n_graduated=2)
    stats_large = _stats(n_launches=15, n_graduated=10)
    small = score_deployer(stats_small, weights, now=NOW)
    large = score_deployer(stats_large, weights, now=NOW)
    assert large.components["confidence"] >= small.components["confidence"]
    assert large.score > small.score


def test_score_deployer_score_bounded():
    # Extreme inputs must still stay in [0, 1].
    weights = DeployerScoreWeights()
    stats = _stats(
        n_launches=100,
        n_graduated=100,
        median_peak=100_000_000.0,
        avg_hold=7200.0,
        last_launch=NOW,
    )
    result = score_deployer(stats, weights, now=NOW)
    assert 0.0 <= result.score <= 1.0


def test_score_deployer_no_graduations():
    stats = _stats(
        n_launches=10, n_graduated=0, median_peak=5_000.0, avg_hold=60.0
    )
    result = score_deployer(stats, DEFAULT_WEIGHTS, now=NOW)
    assert result.score < 0.2


# ----------------------------------------------------------------------
# DeployerRegistry — schema + CRUD with mock engine
# ----------------------------------------------------------------------
def _scripted_engine():
    engine = MagicMock(spec=Engine)
    conn = MagicMock()
    conn.execute.return_value = MagicMock()
    conn.execute.return_value.fetchone.return_value = None
    conn.execute.return_value.fetchall.return_value = []
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def test_registry_ensures_tables_on_construct():
    engine, conn = _scripted_engine()
    DeployerRegistry(engine)
    sqls = " ".join(str(c.args[0]) for c in conn.execute.call_args_list).lower()
    assert "solana_deployers" in sqls
    assert "solana_deployer_launches" in sqls


def test_registry_get_returns_none_when_missing():
    engine, conn = _scripted_engine()
    registry = DeployerRegistry(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchone.return_value = None
    conn.execute.return_value = result
    assert registry.get("GHOST") is None


def test_registry_get_returns_result():
    engine, conn = _scripted_engine()
    registry = DeployerRegistry(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchone.return_value = (
        "W1", 5, 3, 300_000.0, 1_000_000.0, 900.0, 0.65, NOW, {"graduation_rate": 0.6}
    )
    conn.execute.return_value = result
    score = registry.get("W1")
    assert score is not None
    assert score.score == 0.65
    assert score.stats.n_launches == 5
    assert score.components.get("graduation_rate") == 0.6


def test_registry_refresh_wallet_requires_provider():
    engine, _ = _scripted_engine()
    registry = DeployerRegistry(engine, provider=None)
    with pytest.raises(ValueError, match="DeployInfoProvider"):
        registry.refresh_wallet("W1")


def test_registry_refresh_wallet_happy_path():
    engine, conn = _scripted_engine()
    provider = MagicMock()
    from trading.solana.helius_client import DeployRecord

    provider.list_wallet_deploys.return_value = [
        DeployRecord(
            mint=f"MINT{i}",
            created_at=NOW - timedelta(days=i),
            peak_market_cap_usd=200_000.0 * (i + 1),
            deployer_hold_seconds=600 + i * 60,
        )
        for i in range(5)
    ]
    registry = DeployerRegistry(engine, provider=provider)

    # Mock the aggregate SELECT that runs inside recompute()
    agg_result = MagicMock()
    agg_result.fetchall.return_value = [
        (NOW - timedelta(days=i), 200_000.0 * (i + 1), 600 + i * 60, True)
        for i in range(5)
    ]
    insert_result = MagicMock()
    insert_result.fetchone.return_value = (True,)  # inserted=True

    # Return sequence: DDL (many) → upsert INSERTs → aggregate SELECT → upsert INSERT (score persist)
    # Simpler: default return_value + side_effect overrides for specific calls.
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    call_count = {"n": 0}

    def _exec(*args, **kwargs):
        call_count["n"] += 1
        sql = str(args[0]).lower()
        if "select launch_at" in sql:
            return agg_result
        if "insert into solana_deployer_launches" in sql:
            return insert_result
        result = MagicMock()
        result.fetchone.return_value = None
        result.fetchall.return_value = []
        return result

    conn.execute.side_effect = _exec

    result = registry.refresh_wallet("W1")
    assert result.stats.n_launches == 5
    assert result.stats.n_graduated == 5
    assert result.score > 0  # Clear signal: 5 launches, all graduated
    provider.list_wallet_deploys.assert_called_once_with("W1", lookback_days=180)
