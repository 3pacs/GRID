"""
Tests for trading/solana/smart_money.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine import Engine

from trading.solana.smart_money import (
    SmartMoneyMatch,
    SmartMoneyMatchSet,
    SmartMoneyRegistry,
    SmartMoneyWallet,
)


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


# ----------------------------------------------------------------------
# SmartMoneyMatchSet.combined_trust
# ----------------------------------------------------------------------
def test_combined_trust_empty():
    assert SmartMoneyMatchSet(matches=()).combined_trust == 0.0


def test_combined_trust_single_match():
    ms = SmartMoneyMatchSet(
        matches=(SmartMoneyMatch("W", "x", 0.8, "op"),)
    )
    assert ms.combined_trust == pytest.approx(0.8)


def test_combined_trust_three_diminishing_returns():
    ms = SmartMoneyMatchSet(
        matches=(
            SmartMoneyMatch("W1", "a", 0.7, "op"),
            SmartMoneyMatch("W2", "b", 0.7, "op"),
            SmartMoneyMatch("W3", "c", 0.7, "op"),
        )
    )
    # 1 - (1-0.7)^3 = 1 - 0.027 = 0.973
    assert ms.combined_trust == pytest.approx(0.973, abs=1e-3)


def test_combined_trust_caps_at_one():
    ms = SmartMoneyMatchSet(
        matches=tuple(
            SmartMoneyMatch(f"W{i}", "a", 0.99, "op") for i in range(10)
        )
    )
    assert ms.combined_trust <= 1.0
    assert ms.combined_trust > 0.999


# ----------------------------------------------------------------------
# Registry schema + CRUD
# ----------------------------------------------------------------------
def test_ensure_tables_issues_create():
    engine, conn = _scripted_engine()
    SmartMoneyRegistry(engine)
    joined = " ".join(str(c.args[0]) for c in conn.execute.call_args_list).lower()
    assert "solana_smart_money" in joined


def test_upsert_builds_insert():
    engine, conn = _scripted_engine()
    registry = SmartMoneyRegistry(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None

    registry.upsert(
        SmartMoneyWallet(
            wallet="W1", label="early buyer of SOL 2021",
            source="operator", trust=0.75, notes="manual",
        )
    )
    sql = str(conn.execute.call_args.args[0]).lower()
    assert "insert into solana_smart_money" in sql
    assert "on conflict" in sql
    bound = conn.execute.call_args.args[1]
    assert bound["w"] == "W1"
    assert bound["t"] == 0.75


def test_ensure_seed_returns_inserted_count():
    engine, conn = _scripted_engine()
    registry = SmartMoneyRegistry(engine)
    conn.execute.reset_mock()

    # First insert succeeds, second is a no-op (already exists)
    returns = iter([
        MagicMock(fetchone=MagicMock(return_value=("W1",))),
        MagicMock(fetchone=MagicMock(return_value=None)),
    ])
    conn.execute.side_effect = lambda *a, **kw: next(returns)

    n = registry.ensure_seed([
        SmartMoneyWallet("W1", "a", "op", 0.5),
        SmartMoneyWallet("W2", "b", "op", 0.5),
    ])
    assert n == 1


def test_get_returns_dto():
    engine, conn = _scripted_engine()
    registry = SmartMoneyRegistry(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchone.return_value = ("W1", "early", "operator", 0.9, "note", True)
    conn.execute.return_value = result

    w = registry.get("W1")
    assert w is not None
    assert w.wallet == "W1"
    assert w.label == "early"
    assert w.trust == 0.9
    assert w.active is True


def test_get_returns_none_when_missing():
    engine, conn = _scripted_engine()
    registry = SmartMoneyRegistry(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchone.return_value = None
    conn.execute.return_value = result
    assert registry.get("GHOST") is None


def test_list_active_returns_dtos_in_trust_order():
    engine, conn = _scripted_engine()
    registry = SmartMoneyRegistry(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchall.return_value = [
        ("W1", "top", "op", 0.95, None, True),
        ("W2", "mid", "op", 0.5, None, True),
    ]
    conn.execute.return_value = result

    active = registry.list_active()
    assert [w.wallet for w in active] == ["W1", "W2"]


# ----------------------------------------------------------------------
# match_early_buyers
# ----------------------------------------------------------------------
def test_match_early_buyers_returns_matches():
    engine, conn = _scripted_engine()
    registry = SmartMoneyRegistry(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None

    result = MagicMock()
    result.fetchall.return_value = [
        ("SMART1", "alpha", "op", 0.9),
        ("SMART2", "beta", "op", 0.7),
    ]
    conn.execute.return_value = result

    ms = registry.match_early_buyers(
        ["RANDO1", "SMART1", "RANDO2", "SMART2"]
    )
    assert ms.count == 2
    wallets = [m.wallet for m in ms.matches]
    # Order mirrors input order, not DB order
    assert wallets == ["SMART1", "SMART2"]
    assert ms.combined_trust > 0.9  # 1 - (0.1 * 0.3)


def test_match_early_buyers_empty_input():
    engine, conn = _scripted_engine()
    registry = SmartMoneyRegistry(engine)
    conn.execute.reset_mock()
    ms = registry.match_early_buyers([])
    assert ms.count == 0
    assert ms.combined_trust == 0.0


def test_match_early_buyers_no_matches():
    engine, conn = _scripted_engine()
    registry = SmartMoneyRegistry(engine)
    conn.execute.reset_mock()
    conn.execute.side_effect = None
    result = MagicMock()
    result.fetchall.return_value = []
    conn.execute.return_value = result
    ms = registry.match_early_buyers(["RANDO1", "RANDO2"])
    assert ms.count == 0
