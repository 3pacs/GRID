"""Per-strategy holding period in the paper executor (LEVER-PACKAGE §7 T2.5).

``trading/signal_executor.py`` auto-closed every open paper trade after the
hypothesis ``expected_lag`` (default 1 day), so a 90-day thesis could never
be paper-traded as a 90-day thesis. ``paper_strategies.horizon_days`` now
wins when set.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from trading import paper_engine as pe
from trading import signal_executor as se

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "strategy_horizon,expected_lag,want",
    [
        (90, 1, 90),        # strategy horizon beats hypothesis lag
        (None, 3, 3),       # no strategy horizon → hypothesis lag
        (None, None, 1),    # nothing → legacy default
        (0, 5, 5),          # non-positive strategy horizon ignored
        ("180", 1, 180),    # tolerant of string ints from JSONB/text
        ("bad", None, 1),   # garbage falls through to the default
    ],
)
def test_resolve_hold_days(strategy_horizon, expected_lag, want) -> None:
    assert se.resolve_hold_days(strategy_horizon, expected_lag) == want
    assert se._DEFAULT_EXPECTED_LAG == 1


def test_executor_reads_horizon_days_with_the_strategy_row() -> None:
    src = (ROOT / "trading/signal_executor.py").read_text(encoding="utf-8")
    assert '"SELECT id, hypothesis_id, leader, follower, horizon_days "' in src
    assert "strategy_id, hypothesis_id, leader, follower, strategy_horizon_days = strat" in src
    assert "resolve_hold_days(\n                    strategy_horizon_days, _get_expected_lag(conn, hypothesis_id)\n                )" in src


def _engine_capturing_inserts() -> tuple[MagicMock, list]:
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    calls: list = []

    def execute(stmt, params=None):
        calls.append((str(stmt), params))
        result = MagicMock()
        result.fetchone.return_value = None  # no existing strategy
        return result

    conn.execute.side_effect = execute
    engine = MagicMock()
    engine.begin.return_value = conn
    return engine, calls


def test_register_strategy_persists_horizon_days(monkeypatch) -> None:
    engine, calls = _engine_capturing_inserts()
    monkeypatch.setattr(pe.PaperTradingEngine, "_ensure_tables", lambda self: None, raising=False)
    eng = pe.PaperTradingEngine.__new__(pe.PaperTradingEngine)
    eng.engine = engine
    eng.initial_capital = 10_000.0

    sid = eng.register_strategy(42, "SPY", "NVDA", "long-horizon test", horizon_days=90)

    assert sid == "h42_SPY_NVDA"
    inserts = [(s, p) for s, p in calls if "INSERT INTO paper_strategies" in s]
    assert len(inserts) == 1
    sql, params = inserts[0]
    assert "horizon_days" in sql and ":horizon_days" in sql
    assert params["horizon_days"] == 90


def test_register_strategy_default_horizon_is_null_and_rejects_non_positive() -> None:
    engine, calls = _engine_capturing_inserts()
    eng = pe.PaperTradingEngine.__new__(pe.PaperTradingEngine)
    eng.engine = engine
    eng.initial_capital = 10_000.0
    eng.register_strategy(1, "SPY", "AMD")
    params = [p for s, p in calls if "INSERT INTO paper_strategies" in s][0]
    assert params["horizon_days"] is None
    with pytest.raises(ValueError):
        eng.register_strategy(1, "SPY", "AMD", horizon_days=0)


def test_paper_engine_adds_horizon_column_idempotently() -> None:
    src = (ROOT / "trading/paper_engine.py").read_text(encoding="utf-8")
    assert "ALTER TABLE paper_strategies ADD COLUMN IF NOT EXISTS horizon_days INTEGER" in src
