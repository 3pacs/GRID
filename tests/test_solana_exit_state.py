"""
Tests for trading/solana/exit_state.py.

The store is a thin SQL wrapper, so these tests focus on:
  * schema DDL is issued on construct
  * the CRUD methods build the expected SQL bind params
  * Welford math in update_variant_stats is correct
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine import Engine

from trading.solana.exit_state import (
    SOURCE_UNKNOWN,
    ExitStateStore,
    PositionStateRow,
    VariantStatsRow,
)


# ----------------------------------------------------------------------
# Engine fixture that can return canned rows per query
# ----------------------------------------------------------------------
def _scripted_engine(rows_queue: list | None = None) -> tuple[MagicMock, MagicMock]:
    """Build a mock engine whose execute().fetchone() pops from ``rows_queue``."""
    engine = MagicMock(spec=Engine)
    conn = MagicMock()
    queue = list(rows_queue or [])

    def _execute(*args, **kwargs):
        result = MagicMock()
        if queue:
            row = queue.pop(0)
            result.fetchone.return_value = row
            result.fetchall.return_value = [row] if row else []
        else:
            result.fetchone.return_value = None
            result.fetchall.return_value = []
        return result

    conn.execute.side_effect = _execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


# ----------------------------------------------------------------------
# Schema init
# ----------------------------------------------------------------------
def test_ensure_tables_issues_all_three_creates():
    engine, conn = _scripted_engine()
    ExitStateStore(engine)
    sqls = [
        str(c.args[0]) if c.args else "" for c in conn.execute.call_args_list
    ]
    joined = " ".join(sqls).lower()
    assert "solana_exit_state" in joined
    assert "solana_exit_events" in joined
    assert "solana_policy_variants" in joined


# ----------------------------------------------------------------------
# ensure_position
# ----------------------------------------------------------------------
def test_ensure_position_inserts_when_missing():
    engine, conn = _scripted_engine()
    store = ExitStateStore(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchone.return_value = None  # SELECT → no existing row
    conn.execute.return_value = result

    created = store.ensure_position(trade_id=42, policy_variant="balanced")
    assert created is True
    insert_calls = [
        c for c in conn.execute.call_args_list
        if "INSERT" in str(c.args[0]).upper()
    ]
    assert insert_calls


def test_ensure_position_skips_when_present():
    engine, conn = _scripted_engine()
    store = ExitStateStore(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchone.return_value = (1,)  # existing row
    conn.execute.return_value = result

    created = store.ensure_position(trade_id=42, policy_variant="balanced")
    assert created is False


# ----------------------------------------------------------------------
# Welford update
# ----------------------------------------------------------------------
def test_update_variant_stats_welford_first_sample():
    engine, conn = _scripted_engine()
    store = ExitStateStore(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None

    # SELECT returns None → auto-create path; then UPDATE is called.
    results = [
        MagicMock(fetchone=MagicMock(return_value=None)),  # SELECT FOR UPDATE
        MagicMock(fetchone=MagicMock(return_value=None)),  # INSERT OR DO NOTHING
        MagicMock(fetchone=MagicMock(return_value=None)),  # UPDATE
    ]
    conn.execute.side_effect = results

    store.update_variant_stats(
        variant_id="balanced",
        source_type="unknown",
        new_reward=0.5,
        pnl_pct=0.5,
    )
    # Last call is the UPDATE; inspect its bind params.
    update_call = conn.execute.call_args_list[-1]
    bound = update_call.args[1]
    assert bound["n"] == 1
    assert bound["m"] == pytest.approx(0.5)
    assert bound["m2"] == pytest.approx(0.0)  # no variance on a single sample
    assert bound["w"] == 1
    assert bound["l"] == 0


def test_update_variant_stats_welford_second_sample():
    engine, conn = _scripted_engine()
    store = ExitStateStore(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None

    # First call is SELECT FOR UPDATE returning a prior row:
    # n=1, mean=0.5, m2=0, total=0.5, wins=1, losses=0
    prior = (1, 0.5, 0.0, 0.5, 1, 0)
    results = [
        MagicMock(fetchone=MagicMock(return_value=prior)),
        MagicMock(fetchone=MagicMock(return_value=None)),  # UPDATE
    ]
    conn.execute.side_effect = results

    store.update_variant_stats(
        variant_id="balanced",
        source_type="unknown",
        new_reward=-0.5,
        pnl_pct=-0.5,
    )
    update_bound = conn.execute.call_args_list[-1].args[1]
    assert update_bound["n"] == 2
    # new mean = 0.5 + (-0.5 - 0.5) / 2 = 0.0
    assert update_bound["m"] == pytest.approx(0.0)
    # Welford m2 = 0 + (-0.5 - 0.5) * (-0.5 - 0.0) = -1.0 * -0.5 = 0.5
    assert update_bound["m2"] == pytest.approx(0.5)
    assert update_bound["l"] == 1
    # variance = m2/n = 0.25 → stddev = 0.5
    variance = update_bound["m2"] / update_bound["n"]
    assert variance == pytest.approx(0.25)


# ----------------------------------------------------------------------
# record_event
# ----------------------------------------------------------------------
def test_record_event_builds_insert():
    engine, conn = _scripted_engine()
    store = ExitStateStore(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchone.return_value = (123,)
    conn.execute.return_value = result

    new_id = store.record_event(
        trade_id=1,
        event_type="tp_rung",
        rung_index=0,
        fraction=0.33,
        price=150.0,
        pnl_pct=0.5,
        peak_pnl_pct=0.5,
        policy_variant="balanced",
        source_type="unknown",
        reason="rung 0",
    )
    assert new_id == 123
    # Verify the bound params
    bound = conn.execute.call_args.args[1]
    assert bound["tid"] == 1
    assert bound["et"] == "tp_rung"
    assert bound["ri"] == 0
    assert bound["f"] == 0.33
    assert bound["p"] == 150.0


# ----------------------------------------------------------------------
# get_variant_stats
# ----------------------------------------------------------------------
def test_get_variant_stats_returns_dto():
    engine, conn = _scripted_engine()
    store = ExitStateStore(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchone.return_value = (
        "balanced", "unknown", 10, 0.3, 2.0, 3.0, 6, 4, 0.25
    )
    conn.execute.return_value = result

    stats = store.get_variant_stats("balanced", "unknown")
    assert isinstance(stats, VariantStatsRow)
    assert stats.variant_id == "balanced"
    assert stats.n_samples == 10
    assert stats.reward_mean == 0.3
    assert stats.reward_variance == pytest.approx(0.2)
    assert stats.wins == 6


def test_get_variant_stats_returns_none_when_missing():
    engine, conn = _scripted_engine()
    store = ExitStateStore(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchone.return_value = None
    conn.execute.return_value = result

    assert store.get_variant_stats("ghost", "unknown") is None


# ----------------------------------------------------------------------
# update_position — partial updates
# ----------------------------------------------------------------------
def test_update_position_builds_dynamic_sql():
    engine, conn = _scripted_engine()
    store = ExitStateStore(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None

    store.update_position(
        trade_id=7,
        peak_pnl_pct=0.6,
        remaining_fraction=0.67,
        tp_rungs_hit=1,
    )
    sql = str(conn.execute.call_args.args[0]).lower()
    assert "update solana_exit_state" in sql
    assert "peak_pnl_pct" in sql
    assert "remaining_fraction" in sql
    assert "tp_rungs_hit" in sql
    bound = conn.execute.call_args.args[1]
    assert bound["tid"] == 7
    assert bound["peak"] == 0.6
    assert bound["rem"] == 0.67


def test_update_position_noop_with_no_fields():
    engine, conn = _scripted_engine()
    store = ExitStateStore(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    store.update_position(trade_id=7)
    # No extra execute calls should be made (only the constructor's DDL)
    assert conn.execute.call_count == 0
