"""Tests for ``scripts.auto_disable_underperforming_rules``.

In-memory FakeEngine exposes the two SELECTs and one UPDATE the script
issues so the policy logic can be exercised without a real database.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from scripts.auto_disable_underperforming_rules import (
    DEFAULT_DISABLE_THRESHOLD,
    DEFAULT_MIN_SAMPLES,
    DEFAULT_PROMOTE_THRESHOLD,
    RuleDecision,
    autotune_rules,
)


class _FakeConn:
    def __init__(self, engine: "_FakeEngine") -> None:
        self.engine = engine

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> Any:
        text_sql = str(sql).strip()
        if "FROM v_rule_win_rate" in text_sql:
            return _Result(self.engine.win_rate_rows)
        if "FROM gem_rules_config" in text_sql and "UPDATE" not in text_sql.upper():
            return _Result([(name, en) for name, en in self.engine.config.items()])
        if "UPDATE gem_rules_config" in text_sql:
            self.engine.updates.append(dict(params or {}))
            rule = params["rule_name"]
            self.engine.config[rule] = bool(params["enabled"])
            return MagicMock()
        return _Result([])


class _Result:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = list(rows)

    def fetchall(self) -> list[Any]:
        return list(self._rows)


class _FakeEngine:
    def __init__(
        self,
        win_rate_rows: list[tuple[Any, ...]],
        config: dict[str, bool],
    ) -> None:
        self.win_rate_rows = win_rate_rows
        self.config = dict(config)
        self.updates: list[dict[str, Any]] = []

    def connect(self) -> _FakeConn:
        return _FakeConn(self)

    def begin(self) -> _FakeConn:
        return _FakeConn(self)


# ── Policy tests ──────────────────────────────────────────────────────────


def test_disables_rule_below_threshold_with_enough_samples():
    # 40 decisive verdicts, 30% win-rate → DISABLE
    rows = [("correlation_break", 50, 12, 20, 8, 0.30)]
    config = {"correlation_break": True}
    engine = _FakeEngine(rows, config)

    decisions = autotune_rules(engine)

    assert len(decisions) == 1
    d = decisions[0]
    assert d.action == "disable"
    assert d.current_enabled is True
    assert d.new_enabled is False
    # State must be flipped in the DB
    assert engine.config["correlation_break"] is False
    assert len(engine.updates) == 1


def test_promotes_rule_above_threshold_with_enough_samples():
    # 35 decisive verdicts, 70% win-rate → PROMOTE (re-enable)
    rows = [("nn_cluster", 40, 28, 8, 4, 0.70)]
    config = {"nn_cluster": False}
    engine = _FakeEngine(rows, config)

    decisions = autotune_rules(engine)

    assert decisions[0].action == "promote"
    assert engine.config["nn_cluster"] is True


def test_keeps_rule_in_middle_band():
    # 50 decisive verdicts, 50% win-rate → KEEP (no flip)
    rows = [("pagerank_delta", 60, 25, 20, 5, 0.50)]
    config = {"pagerank_delta": True}
    engine = _FakeEngine(rows, config)

    decisions = autotune_rules(engine)

    assert decisions[0].action == "keep"
    assert engine.config["pagerank_delta"] is True
    assert len(engine.updates) == 0  # no DB write for a keep


def test_skips_rule_below_min_samples():
    # 5 decisive verdicts, 30% win-rate → SKIP (insufficient data)
    rows = [("bootstrap_ci_break", 10, 1, 3, 1, 0.20)]
    config = {"bootstrap_ci_break": True}
    engine = _FakeEngine(rows, config)

    decisions = autotune_rules(engine)

    assert decisions[0].action == "skip_low_n"
    assert decisions[0].n == 5  # decisive count = hit + miss + wrong
    assert engine.config["bootstrap_ci_break"] is True
    assert len(engine.updates) == 0


def test_dry_run_does_not_write_db():
    # 50 decisive, 30% win-rate → DISABLE in policy, but dry_run prevents write
    rows = [("correlation_break", 60, 15, 25, 10, 0.30)]
    config = {"correlation_break": True}
    engine = _FakeEngine(rows, config)

    decisions = autotune_rules(engine, dry_run=True)

    assert decisions[0].action == "disable"
    assert decisions[0].new_enabled is False
    # No state change in the engine
    assert engine.config["correlation_break"] is True
    assert len(engine.updates) == 0


def test_handles_no_win_rate_data():
    rows = []
    config = {"correlation_break": True, "nn_cluster": True}
    engine = _FakeEngine(rows, config)

    decisions = autotune_rules(engine)

    assert decisions == []
    assert len(engine.updates) == 0


def test_handles_null_win_rate_safely():
    # n_decisive=0 with NULL win_rate (all INCONCLUSIVE) → SKIP
    rows = [("nn_cluster", 100, 0, 0, 0, None)]
    config = {"nn_cluster": True}
    engine = _FakeEngine(rows, config)

    decisions = autotune_rules(engine)

    assert decisions[0].action == "skip_low_n"
    assert engine.config["nn_cluster"] is True


def test_custom_thresholds_respected():
    # 50 decisive, 50% win-rate. With default thresholds → KEEP.
    # With disable_below=0.55 → DISABLE.
    rows = [("correlation_break", 60, 25, 20, 5, 0.50)]
    engine = _FakeEngine(rows, {"correlation_break": True})

    # default: KEEP
    d = autotune_rules(engine, dry_run=True)[0]
    assert d.action == "keep"

    # custom: DISABLE
    engine2 = _FakeEngine(rows, {"correlation_break": True})
    d = autotune_rules(engine2, disable_threshold=0.55, dry_run=True)[0]
    assert d.action == "disable"


def test_rule_decision_to_dict_roundtrip():
    d = RuleDecision(
        rule_name="x", n=42, win_rate=0.4567,
        current_enabled=True, new_enabled=False, action="disable",
    )
    out = d.to_dict()
    assert out["rule_name"] == "x"
    assert out["n"] == 42
    assert out["win_rate"] == 0.4567
    assert out["action"] == "disable"


def test_thresholds_constants_are_consistent():
    assert 0.0 < DEFAULT_DISABLE_THRESHOLD < DEFAULT_PROMOTE_THRESHOLD < 1.0
    assert DEFAULT_MIN_SAMPLES > 0
