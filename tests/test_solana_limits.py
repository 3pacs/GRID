"""
Tests for trading/solana/limits.py.

Uses an in-line engine mock so we can return specific SQL row values.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy.engine import Engine

from trading.solana.limits import DailyLimits, LimitConfig


def _make_engine(total_usd: float, count: int, per_mint_usd: float = 0.0) -> MagicMock:
    """Build a mock SQLAlchemy engine whose SELECTs return canned rows.

    The limits module issues two queries: first the aggregate over the
    day, then (optionally) a per-mint aggregate. We return the totals in
    that order.
    """
    engine = MagicMock(spec=Engine)
    conn = MagicMock()

    aggregate_row = (total_usd, count)
    per_mint_row = (per_mint_usd,)

    fetch_queue = [aggregate_row, per_mint_row]

    def _execute(*args, **kwargs):
        result = MagicMock()
        if fetch_queue:
            result.fetchone.return_value = fetch_queue.pop(0)
        else:
            result.fetchone.return_value = (0, 0)
        return result

    conn.execute.side_effect = _execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


# ----------------------------------------------------------------------
# Happy paths
# ----------------------------------------------------------------------
def test_limits_check_passes_when_under_caps():
    engine = _make_engine(total_usd=50.0, count=3, per_mint_usd=0.0)
    limits = DailyLimits(
        engine=engine,
        strategy_id="solana_autohedge",
        config=LimitConfig(
            max_daily_usd=200.0,
            max_daily_trades=20,
            max_per_mint_daily_usd=100.0,
        ),
    )
    decision = limits.check(trade_usd=40.0, mint="MINT1")

    assert decision.passed is True
    assert decision.daily_usd_used == 50.0
    assert decision.daily_trades_used == 3


def test_limits_check_trips_daily_usd_cap():
    engine = _make_engine(total_usd=180.0, count=3)
    limits = DailyLimits(
        engine=engine,
        strategy_id="solana_autohedge",
        config=LimitConfig(max_daily_usd=200.0, max_daily_trades=20),
    )
    decision = limits.check(trade_usd=30.0, mint=None)
    assert decision.passed is False
    assert any("daily USD cap" in r for r in decision.reasons)


def test_limits_check_trips_daily_trade_count():
    engine = _make_engine(total_usd=10.0, count=20)
    limits = DailyLimits(
        engine=engine,
        strategy_id="solana_autohedge",
        config=LimitConfig(max_daily_usd=1000.0, max_daily_trades=20),
    )
    decision = limits.check(trade_usd=1.0, mint=None)
    assert decision.passed is False
    assert any("trade count" in r for r in decision.reasons)


def test_limits_check_trips_per_mint_cap():
    engine = _make_engine(total_usd=60.0, count=4, per_mint_usd=60.0)
    limits = DailyLimits(
        engine=engine,
        strategy_id="solana_autohedge",
        config=LimitConfig(
            max_daily_usd=1000.0,
            max_daily_trades=20,
            max_per_mint_daily_usd=75.0,
        ),
    )
    decision = limits.check(trade_usd=30.0, mint="MINT1")
    assert decision.passed is False
    assert any("per-mint USD cap" in r for r in decision.reasons)


def test_limits_check_disabled_cap_is_ignored():
    engine = _make_engine(total_usd=5000.0, count=999)
    limits = DailyLimits(
        engine=engine,
        strategy_id="solana_autohedge",
        config=LimitConfig(max_daily_usd=0.0, max_daily_trades=0),
    )
    decision = limits.check(trade_usd=100.0, mint=None)
    assert decision.passed is True


def test_limits_check_rejects_negative_size():
    engine = _make_engine(total_usd=0.0, count=0)
    limits = DailyLimits(engine=engine, strategy_id="s")
    decision = limits.check(trade_usd=-1.0, mint=None)
    assert decision.passed is False


def test_limits_check_fails_closed_on_db_error():
    engine = MagicMock(spec=Engine)
    engine.connect.side_effect = RuntimeError("DB down")
    limits = DailyLimits(engine=engine, strategy_id="s")
    decision = limits.check(trade_usd=50.0, mint="MINT1")
    assert decision.passed is False
    assert any("DB error" in r for r in decision.reasons)


def test_limits_summary_formatting():
    engine = _make_engine(total_usd=10.0, count=1)
    limits = DailyLimits(
        engine=engine,
        strategy_id="s",
        config=LimitConfig(max_daily_usd=100.0, max_daily_trades=5),
    )
    decision = limits.check(trade_usd=10.0, mint=None)
    assert "OK" in decision.summary

    engine = _make_engine(total_usd=200.0, count=3)
    limits = DailyLimits(
        engine=engine,
        strategy_id="s",
        config=LimitConfig(max_daily_usd=100.0),
    )
    decision = limits.check(trade_usd=10.0, mint=None)
    assert "BLOCKED" in decision.summary
