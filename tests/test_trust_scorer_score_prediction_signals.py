"""Tests for ``TrustScorer.score_prediction_signals``.

Regression coverage for the 2026-05-13 fix. Pre-fix the function was
**double-broken**:

  1. ``outcome_map`` keys were uppercase (``HIT``/``PARTIAL``/``MISS``)
     but oracle_predictions.verdict is lowercase, so every call defaulted
     to ``"WRONG"`` — silently feeding the same family of bugs as #119.
  2. The UPDATE referenced ``outcome_notes`` which doesn't exist in the
     signal_sources schema, so every call raised ``UndefinedColumn``,
     was caught by a bare DEBUG-level except, and silently returned 0.

These tests pin the post-fix behaviour: case-insensitive verdict
matching, unknown verdicts leave rows PENDING (not WRONG), and the
UPDATE only touches existing columns.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence.trust_scorer import TrustScorer


# ── Fixture helpers ───────────────────────────────────────────────────────


class _CapturingResult:
    def __init__(self, rowcount: int = 1) -> None:
        self.rowcount = rowcount


class _CapturingConn:
    """Records every execute() call so tests can assert on the SQL +
    bind params. Returns a MagicMock-like result with a rowcount.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.binds: list[dict[str, Any]] = []
        self.next_rowcount = 1

    def __enter__(self) -> "_CapturingConn":
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> _CapturingResult:
        self.statements.append(str(stmt))
        self.binds.append(dict(params or {}))
        return _CapturingResult(rowcount=self.next_rowcount)


def _scorer_with_conn() -> tuple[TrustScorer, _CapturingConn]:
    engine = MagicMock()
    conn = _CapturingConn()
    engine.begin.return_value = conn
    return TrustScorer(engine), conn


class _SignalRef:
    """Mimic the contracts SignalRef Pydantic model — just needs .signal_id."""

    def __init__(self, signal_id: str) -> None:
        self.signal_id = signal_id


# ── Verdict-mapping cases ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "verdict,expected_outcome",
    [
        # Canonical uppercase contract enum (preserved)
        ("HIT", "CORRECT"),
        ("PARTIAL", "CORRECT"),
        ("MISS", "WRONG"),
        # Lowercase oracle_predictions verdict (the historical bug case)
        ("hit", "CORRECT"),
        ("partial", "CORRECT"),
        ("miss", "WRONG"),
        # Mixed case / whitespace tolerance
        ("Hit", "CORRECT"),
        ("  miss  ", "WRONG"),
    ],
)
def test_verdict_mapping_is_case_insensitive(verdict, expected_outcome):
    scorer, conn = _scorer_with_conn()
    n = scorer.score_prediction_signals(
        prediction_id="p1",
        verdict=verdict,
        signals=[_SignalRef("s1"), _SignalRef("s2")],
    )
    assert n == 1, f"verdict={verdict!r} should produce an UPDATE"
    assert len(conn.binds) == 1
    assert conn.binds[0]["o"] == expected_outcome
    assert conn.binds[0]["ids"] == ["s1", "s2"]


# ── Unknown / no-data verdicts must NOT force WRONG ───────────────────────


@pytest.mark.parametrize(
    "verdict",
    ["", None, "pending", "unknown", "no_data", "weird"],
)
def test_unknown_verdict_leaves_rows_pending(verdict):
    # Pre-fix, every one of these returned "WRONG" via the dict-get
    # default. Post-fix they short-circuit with 0 rows updated.
    scorer, conn = _scorer_with_conn()
    n = scorer.score_prediction_signals(
        prediction_id="p1",
        verdict=verdict,
        signals=[_SignalRef("s1")],
    )
    assert n == 0
    assert conn.statements == []  # no UPDATE attempted


# ── SQL no longer references the non-existent outcome_notes column ────────


def test_update_sql_does_not_reference_outcome_notes_column():
    scorer, conn = _scorer_with_conn()
    scorer.score_prediction_signals(
        prediction_id="p42",
        verdict="HIT",
        signals=[_SignalRef("s1")],
    )
    assert len(conn.statements) == 1
    sql = conn.statements[0]
    assert "outcome_notes" not in sql, (
        "outcome_notes column does not exist in signal_sources; pre-fix "
        "the UPDATE raised UndefinedColumn and was silently swallowed"
    )
    # Sanity: must still set outcome + scored_at on PENDING rows only.
    assert "SET outcome" in sql
    assert "scored_at" in sql
    assert "outcome IS NULL OR outcome = 'PENDING'" in sql


# ── Argument robustness ───────────────────────────────────────────────────


def test_empty_signals_list_returns_zero_without_executing():
    scorer, conn = _scorer_with_conn()
    n = scorer.score_prediction_signals(prediction_id="p1", verdict="HIT", signals=[])
    assert n == 0
    assert conn.statements == []


def test_signals_without_signal_id_are_dropped_silently():
    scorer, conn = _scorer_with_conn()
    # Mix valid SignalRefs with malformed entries — the malformed ones
    # should be skipped without breaking the call.
    n = scorer.score_prediction_signals(
        prediction_id="p1",
        verdict="HIT",
        signals=[
            _SignalRef("s1"),
            {"signal_id": "s2"},
            {"no_id_here": True},
            object(),  # no signal_id attribute
        ],
    )
    assert n == 1
    assert conn.binds[0]["ids"] == ["s1", "s2"]
