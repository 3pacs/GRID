"""Tests for the strategy engine — regime-independent strategy assignments."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch
from strategy.engine import StrategyEngine, DEFAULT_STRATEGIES


class FakeRow:
    """Mimics a SQLAlchemy row accessible by index."""

    def __init__(self, *values):
        self._values = values

    def __getitem__(self, idx):
        return self._values[idx]


class FakeConnection:
    """Minimal mock for a SQLAlchemy connection context."""

    def __init__(self, rows=None, fetchone_val=None):
        self._rows = rows or []
        self._fetchone_val = fetchone_val
        self.executed = []

    def execute(self, stmt, params=None):
        self.executed.append((stmt, params))
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._fetchone_val

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeEngine:
    """Minimal mock for a SQLAlchemy engine."""

    def __init__(self, connection=None):
        self._connection = connection or FakeConnection()

    def connect(self):
        return self._connection

    def begin(self):
        return self._connection


# ---- Tests for DEFAULT_STRATEGIES ----

class TestDefaultStrategies:
    def test_all_macro_regimes_have_defaults(self) -> None:
        """Every macro regime must have a default strategy."""
        for regime in ("GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"):
            assert regime in DEFAULT_STRATEGIES
            s = DEFAULT_STRATEGIES[regime]
            assert "name" in s
            assert "posture" in s
            assert "allocation" in s
            assert "risk_level" in s
            assert "action" in s
            assert "rationale" in s

    def test_default_strategies_have_nonempty_fields(self) -> None:
        """Default strategies should have meaningful content."""
        for regime, s in DEFAULT_STRATEGIES.items():
            assert len(s["name"]) > 0, f"{regime} has empty name"
            assert len(s["posture"]) > 0, f"{regime} has empty posture"
            assert len(s["allocation"]) > 0, f"{regime} has empty allocation"


# ---- Tests for StrategyEngine ----

class TestStrategyEngine:
    def _make_engine(self, rows=None, fetchone_val=None) -> tuple[StrategyEngine, FakeEngine]:
        conn = FakeConnection(rows=rows, fetchone_val=fetchone_val)
        fake_engine = FakeEngine(connection=conn)
        # Patch _ensure_table so it doesn't actually create tables
        with patch.object(StrategyEngine, "_ensure_table"):
            se = StrategyEngine(fake_engine)
        return se, fake_engine

    def test_get_active_strategies_returns_defaults_when_db_empty(self) -> None:
        """When DB has no strategies, defaults are returned for all macro regimes."""
        se, _ = self._make_engine(rows=[])
        result = se.get_active_strategies()
        assert len(result) >= len(DEFAULT_STRATEGIES)
        regime_states = {s["regime_state"] for s in result}
        for regime in DEFAULT_STRATEGIES:
            assert regime in regime_states

    def test_get_active_strategies_default_source_is_default(self) -> None:
        """Default strategies should have source='default'."""
        se, _ = self._make_engine(rows=[])
        result = se.get_active_strategies()
        for s in result:
            assert s["source"] == "default"

    def test_get_strategy_for_regime_returns_default(self) -> None:
        """When no DB strategy exists, the default is returned."""
        se, _ = self._make_engine()
        result = se.get_strategy_for_regime("GROWTH")
        assert result is not None
        assert result["regime_state"] == "GROWTH"
        assert result["source"] == "default"
        assert result["name"] == DEFAULT_STRATEGIES["GROWTH"]["name"]

    def test_get_strategy_for_regime_unknown_returns_none(self) -> None:
        """Unknown regime with no default returns None."""
        se, _ = self._make_engine()
        result = se.get_strategy_for_regime("NONEXISTENT_REGIME")
        assert result is None

    def test_get_strategy_for_regime_returns_db_override(self) -> None:
        """When a DB strategy exists, it takes precedence."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        db_row = FakeRow(
            42, "GROWTH", "Custom Growth", "Very Aggressive",
            "80% equities, 20% crypto", "Low", "YOLO",
            "Bull market confirmed", now, True,
        )
        conn = FakeConnection(fetchone_val=db_row)
        fake_engine = FakeEngine(connection=conn)
        with patch.object(StrategyEngine, "_ensure_table"):
            se = StrategyEngine(fake_engine)

        result = se.get_strategy_for_regime("GROWTH")
        assert result is not None
        assert result["id"] == 42
        assert result["name"] == "Custom Growth"
        assert result["source"] == "database"

    def test_default_to_dict_format(self) -> None:
        """Verify _default_to_dict produces the expected dict shape."""
        result = StrategyEngine._default_to_dict("CRISIS", DEFAULT_STRATEGIES["CRISIS"])
        assert result["id"] is None
        assert result["regime_state"] == "CRISIS"
        assert result["active"] is True
        assert result["source"] == "default"
        assert result["posture"] == "Capital Preservation"

    def test_row_to_dict_format(self) -> None:
        """Verify _row_to_dict produces the expected dict shape."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        row = FakeRow(
            1, "NEUTRAL", "Test Strat", "Balanced",
            "50/50", "Medium", "Hold steady", "Because", now, True,
        )
        result = StrategyEngine._row_to_dict(row)
        assert result["id"] == 1
        assert result["regime_state"] == "NEUTRAL"
        assert result["name"] == "Test Strat"
        assert result["source"] == "database"
        assert result["assigned_at"] == now.isoformat()

    def test_assign_strategy_calls_deactivate_then_insert(self) -> None:
        """assign_strategy should deactivate old and insert new."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        inserted_row = FakeRow(
            99, "FRAGILE", "New Defensive", "Very Defensive",
            "All bonds", "High", "Run away", "Markets bad", now, True,
        )
        conn = FakeConnection(fetchone_val=inserted_row)
        fake_engine = FakeEngine(connection=conn)
        with patch.object(StrategyEngine, "_ensure_table"):
            se = StrategyEngine(fake_engine)

        result = se.assign_strategy(
            regime_state="FRAGILE",
            name="New Defensive",
            posture="Very Defensive",
            allocation="All bonds",
            risk_level="High",
            action="Run away",
            rationale="Markets bad",
        )

        assert result["id"] == 99
        assert result["name"] == "New Defensive"
        assert result["source"] == "database"
        # Should have executed 2 statements: UPDATE (deactivate) + INSERT
        assert len(conn.executed) == 2

    def test_assign_strategy_raises_on_null_insert(self) -> None:
        """If INSERT returns no row, RuntimeError should be raised."""
        conn = FakeConnection(fetchone_val=None)
        fake_engine = FakeEngine(connection=conn)
        with patch.object(StrategyEngine, "_ensure_table"):
            se = StrategyEngine(fake_engine)

        with pytest.raises(RuntimeError, match="Failed to insert"):
            se.assign_strategy(
                regime_state="GROWTH",
                name="Test",
                posture="Test",
            )


# ---- Tests for SQL safety ----

class TestSQLSafety:
    """Verify no f-strings or .format() are used for SQL in the engine."""

    def test_no_fstrings_in_engine_source(self) -> None:
        """The engine module should not use f-strings for SQL construction."""
        import inspect
        import strategy.engine as mod

        source = inspect.getsource(mod)
        # Check there are no f"SELECT or f"INSERT or f"UPDATE patterns
        import re
        fstring_sql = re.findall(r'f["\'](?:SELECT|INSERT|UPDATE|DELETE|CREATE)', source)
        assert len(fstring_sql) == 0, f"Found f-string SQL: {fstring_sql}"

    def test_no_format_in_engine_source(self) -> None:
        """The engine module should not use .format() for SQL construction."""
        import inspect
        import strategy.engine as mod

        source = inspect.getsource(mod)
        import re
        format_sql = re.findall(r'\.format\(.*(?:SELECT|INSERT|UPDATE|DELETE)', source)
        assert len(format_sql) == 0, f"Found .format() SQL: {format_sql}"
