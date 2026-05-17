"""Tests for intelligence.postmortem.apply_contagion_feedback.

The feedback loop reads ``contagion_backtest_results`` joined with
``contagion_predictions``, walks the implicated ``supply_chain_edges``,
and either decays ``pct_downstream_cogs`` (low accuracy) or flags the
edge as backtest_validated (high accuracy). Every update also writes to
``supply_chain_edge_adjustments``.

These tests use a FakeEngine so they can run offline without touching
Postgres.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

from intelligence.postmortem import (
    HIGH_ACCURACY_THRESHOLD,
    LOW_ACCURACY_THRESHOLD,
    MAX_DELTA_PER_UPDATE,
    NEW_WEIGHT,
    OLD_WEIGHT,
    _blend_and_cap,
    _compute_implied_pct,
    apply_contagion_feedback,
)


# ── Pure math unit tests ────────────────────────────────────────────────

def test_compute_implied_pct_basic() -> None:
    # shock 0.30, pass-through 0.70, actual -0.021
    # implied = 0.021 / (0.30 * 0.70) = 0.1
    implied = _compute_implied_pct(
        actual_price_move_pct=-0.021,
        shock_magnitude=0.30,
        pass_through=0.70,
    )
    assert implied == pytest.approx(0.10, rel=1e-3)


def test_compute_implied_pct_caps_above_one() -> None:
    # absurdly large actual move -> implied > 1.0 -> None
    implied = _compute_implied_pct(
        actual_price_move_pct=-0.90,
        shock_magnitude=0.10,
        pass_through=0.70,
    )
    assert implied is None


def test_blend_and_cap_pulls_toward_implied() -> None:
    new_value, delta, capped = _blend_and_cap(
        old_value=0.05,
        implied_value=0.10,
    )
    expected_raw = OLD_WEIGHT * 0.05 + NEW_WEIGHT * 0.10
    assert new_value == pytest.approx(expected_raw)
    assert delta == pytest.approx(expected_raw - 0.05)
    assert not capped


def test_blend_and_cap_limits_delta() -> None:
    # old=0.01 implied=0.50 → raw blend = 0.007 + 0.15 = 0.157, delta=0.147
    # capped to +0.02 → new = 0.03
    new_value, delta, capped = _blend_and_cap(
        old_value=0.01,
        implied_value=0.50,
    )
    assert capped is True
    assert delta == pytest.approx(MAX_DELTA_PER_UPDATE)
    assert new_value == pytest.approx(0.01 + MAX_DELTA_PER_UPDATE)


# ── Fake engine plumbing ────────────────────────────────────────────────


@dataclass
class _Row:
    values: tuple

    def __getitem__(self, idx: int) -> Any:
        return self.values[idx]

    def __iter__(self):
        return iter(self.values)


class _Result:
    def __init__(self, rows: list[_Row]) -> None:
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


@dataclass
class _FakeBacktestRow:
    result_id: int
    prediction_id: int
    ticker: str
    predicted_margin: float
    actual_move: float
    accuracy: float
    shock_node: str
    shock_magnitude: float


@dataclass
class _FakeEdge:
    id: int
    upstream_id: str
    downstream_id: str
    pct_downstream_cogs: float | None
    confidence: str = "estimated"
    backtest_validated: bool = False


@dataclass
class _FakeDB:
    backtest_rows: list[_FakeBacktestRow] = field(default_factory=list)
    edges: list[_FakeEdge] = field(default_factory=list)
    audits: list[dict[str, Any]] = field(default_factory=list)
    edge_updates: list[dict[str, Any]] = field(default_factory=list)
    edge_confirms: list[int] = field(default_factory=list)


class _FakeConn:
    def __init__(self, db: _FakeDB) -> None:
        self._db = db

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).lower().split())
        p: dict[str, Any] = dict(params or {})

        if "from contagion_backtest_results" in sql:
            rows = [
                _Row(
                    (
                        row.result_id,
                        row.prediction_id,
                        row.ticker,
                        row.predicted_margin,
                        row.actual_move,
                        row.accuracy,
                        row.shock_node,
                        row.shock_magnitude,
                    )
                )
                for row in self._db.backtest_rows
            ]
            return _Result(rows)

        if "from supply_chain_edges" in sql:
            u = p.get("u")
            d = p.get("d", "")
            rows = [
                _Row(
                    (
                        e.id,
                        e.upstream_id,
                        e.downstream_id,
                        e.pct_downstream_cogs,
                        e.confidence,
                    )
                )
                for e in self._db.edges
                if e.upstream_id == u and e.downstream_id.upper() == str(d).upper()
            ]
            return _Result(rows)

        if "update supply_chain_edges" in sql and "pct_downstream_cogs" in sql:
            self._db.edge_updates.append(dict(p))
            # Mutate in-place so subsequent reads see the new value.
            for e in self._db.edges:
                if e.id == p.get("id"):
                    e.pct_downstream_cogs = p.get("v")
                    e.confidence = "derived_from_backtest"
            return _Result([])

        if "update supply_chain_edges" in sql and "backtest_validated" in sql:
            self._db.edge_confirms.append(int(p.get("id")))
            for e in self._db.edges:
                if e.id == p.get("id"):
                    e.backtest_validated = True
            return _Result([])

        if "insert into supply_chain_edge_adjustments" in sql:
            self._db.audits.append(dict(p))
            return _Result([])

        return _Result([])


class _FakeEngine:
    def __init__(self, db: _FakeDB) -> None:
        self._db = db

    def connect(self):
        return _FakeConn(self._db)

    def begin(self):
        return _FakeConn(self._db)


# ── DB-touching behaviour tests ─────────────────────────────────────────


def _make_db_with_edge(pct: float, accuracy: float, actual: float) -> _FakeDB:
    db = _FakeDB()
    db.edges.append(
        _FakeEdge(
            id=101,
            upstream_id="cocoa_beans",
            downstream_id="HSY",
            pct_downstream_cogs=pct,
        )
    )
    db.backtest_rows.append(
        _FakeBacktestRow(
            result_id=1,
            prediction_id=42,
            ticker="HSY",
            predicted_margin=-0.05,
            actual_move=actual,
            accuracy=accuracy,
            shock_node="cocoa_beans",
            shock_magnitude=0.30,
        )
    )
    return db


def test_overshoot_decays_pct_downward() -> None:
    """Predicted -5% margin hit, actual -0.5% (accuracy low): implied pct
    is much smaller than stored → stored value decays down."""
    db = _make_db_with_edge(pct=0.10, accuracy=0.2, actual=-0.005)
    summary = apply_contagion_feedback(_FakeEngine(db), since_hours=24)

    assert summary["decayed"] == 1
    assert summary["confirmed"] == 0
    assert len(db.edge_updates) == 1
    new_value = db.edge_updates[0]["v"]
    # Implied ≈ 0.005 / (0.30 * 0.70) ≈ 0.0238.
    # Blended = 0.7*0.10 + 0.3*0.0238 ≈ 0.0771, delta ≈ -0.0229.
    # Capped to -0.02 → new = 0.08.
    assert new_value == pytest.approx(0.08, abs=1e-6)

    assert len(db.audits) == 1
    audit = db.audits[0]
    assert audit["ev"] == "decay"
    assert audit["old"] == pytest.approx(0.10)
    assert audit["new"] == pytest.approx(0.08)
    assert audit["capped"] is True
    assert audit["delta"] == pytest.approx(-MAX_DELTA_PER_UPDATE)


def test_undershoot_increases_pct_upward() -> None:
    """Predicted small margin hit, actual much larger: implied pct is
    larger than stored → stored value increases (capped)."""
    db = _make_db_with_edge(pct=0.02, accuracy=0.2, actual=-0.10)
    summary = apply_contagion_feedback(_FakeEngine(db), since_hours=24)

    assert summary["decayed"] == 1
    assert len(db.edge_updates) == 1
    new_value = db.edge_updates[0]["v"]
    # Implied ≈ 0.10 / (0.30 * 0.70) ≈ 0.476.
    # Raw blended = 0.7*0.02 + 0.3*0.476 ≈ 0.157, delta ≈ 0.137.
    # Capped to +0.02 → new = 0.04.
    assert new_value == pytest.approx(0.04, abs=1e-6)
    audit = db.audits[0]
    assert audit["ev"] == "decay"
    assert audit["delta"] == pytest.approx(MAX_DELTA_PER_UPDATE)
    assert audit["capped"] is True


def test_accuracy_threshold_high_confirms_without_update() -> None:
    """High accuracy score flips backtest_validated but does not touch
    pct_downstream_cogs."""
    db = _make_db_with_edge(
        pct=0.10,
        accuracy=HIGH_ACCURACY_THRESHOLD + 0.05,
        actual=-0.05,
    )
    summary = apply_contagion_feedback(_FakeEngine(db), since_hours=24)

    assert summary["confirmed"] == 1
    assert summary["decayed"] == 0
    assert db.edge_updates == []
    assert db.edge_confirms == [101]

    audit = db.audits[0]
    assert audit["ev"] == "confirm"
    # Value unchanged.
    assert audit["old"] == pytest.approx(0.10)
    assert audit["new"] == pytest.approx(0.10)

    # Edge in memory should now be marked validated.
    assert db.edges[0].backtest_validated is True


def test_accuracy_mid_band_skipped() -> None:
    """Accuracy in [LOW_THRESHOLD, HIGH_THRESHOLD) does nothing."""
    mid = (LOW_ACCURACY_THRESHOLD + HIGH_ACCURACY_THRESHOLD) / 2.0
    db = _make_db_with_edge(pct=0.10, accuracy=mid, actual=-0.02)
    summary = apply_contagion_feedback(_FakeEngine(db), since_hours=24)

    assert summary["considered"] == 1
    assert summary["decayed"] == 0
    assert summary["confirmed"] == 0
    assert db.edge_updates == []
    assert db.edge_confirms == []
    assert db.audits == []


def test_audit_row_written_for_each_update() -> None:
    """An audit row should be written for every decay and every confirm,
    and the count should match the number of edge updates + confirms."""
    db = _FakeDB()
    # Two edges for the same upstream/downstream pair should get two
    # audits when the backtest decays them.
    db.edges.extend(
        [
            _FakeEdge(
                id=1,
                upstream_id="cocoa_beans",
                downstream_id="HSY",
                pct_downstream_cogs=0.10,
            ),
            _FakeEdge(
                id=2,
                upstream_id="cocoa_beans",
                downstream_id="HSY",
                pct_downstream_cogs=0.08,
            ),
        ]
    )
    db.backtest_rows.append(
        _FakeBacktestRow(
            result_id=7,
            prediction_id=42,
            ticker="HSY",
            predicted_margin=-0.05,
            actual_move=-0.005,
            accuracy=0.2,
            shock_node="cocoa_beans",
            shock_magnitude=0.30,
        )
    )
    summary = apply_contagion_feedback(_FakeEngine(db), since_hours=24)

    assert summary["decayed"] == 2
    assert len(db.audits) == 2
    edge_ids_in_audits = sorted(a["edge_id"] for a in db.audits)
    assert edge_ids_in_audits == [1, 2]
    for audit in db.audits:
        assert audit["ev"] == "decay"
        assert audit["pid"] == 42
        assert audit["brid"] == 7
        assert audit["acc"] == pytest.approx(0.2)


def test_no_backtest_rows_returns_zero_without_error() -> None:
    """Graceful no-op when backtest hasn't scored anything yet."""
    db = _FakeDB()  # empty
    summary = apply_contagion_feedback(_FakeEngine(db), since_hours=720)

    assert summary["considered"] == 0
    assert summary["decayed"] == 0
    assert summary["confirmed"] == 0
    assert summary["errors"] == 0
    assert db.edge_updates == []
    assert db.audits == []


def test_dry_run_does_not_write() -> None:
    """--dry-run path still computes counts but never writes."""
    db = _make_db_with_edge(pct=0.10, accuracy=0.2, actual=-0.005)
    summary = apply_contagion_feedback(
        _FakeEngine(db), since_hours=24, dry_run=True
    )

    assert summary["decayed"] == 1
    assert summary["dry_run"] is True
    assert db.edge_updates == []
    assert db.audits == []


def test_missing_edge_counts_as_skipped() -> None:
    """Backtest for a pair with no matching supply_chain_edges is a
    skip, not an error."""
    db = _FakeDB()
    db.backtest_rows.append(
        _FakeBacktestRow(
            result_id=1,
            prediction_id=42,
            ticker="HSY",
            predicted_margin=-0.05,
            actual_move=-0.005,
            accuracy=0.2,
            shock_node="cocoa_beans",
            shock_magnitude=0.30,
        )
    )
    summary = apply_contagion_feedback(_FakeEngine(db), since_hours=24)

    assert summary["considered"] == 1
    assert summary["decayed"] == 0
    assert summary["skipped_no_edge"] == 1
    assert summary["errors"] == 0


# ── batch_postmortem limit param ──────────────────────────────────────────


def test_batch_postmortem_limit_param_caps_per_source():
    """The optional ``limit`` argument should be threaded into both
    SQL queries (trades + predictions). When provided, each source pool
    is capped at ``limit`` rows so cron runs stay bounded.
    """
    from unittest.mock import MagicMock, patch
    from intelligence.postmortem import batch_postmortem

    # Spy on the two query strings + bind params.
    captured: list[dict] = []

    class _SpyConn:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def execute(self, sql, params=None):
            captured.append({"sql": str(sql), "params": dict(params or {})})
            result = MagicMock()
            result.fetchall.return_value = []  # no rows → loop body skipped
            return result

    class _SpyEngine:
        def begin(self):
            return _SpyConn()
        def connect(self):
            return _SpyConn()

    with patch("intelligence.postmortem._ensure_tables"):
        # First: no limit → SQL should NOT contain LIMIT, params should
        # NOT carry "lim".
        batch_postmortem(_SpyEngine(), days=30)
        # The DDL/ensure path may also call execute; filter to the trade
        # + pred SELECTs by SQL contents.
        select_calls = [c for c in captured if "FROM options_recommendations" in c["sql"]
                        or "FROM oracle_predictions" in c["sql"]]
        assert len(select_calls) == 2  # one trades, one predictions
        for call in select_calls:
            assert "LIMIT" not in call["sql"].upper()
            assert "lim" not in call["params"]

        # Now: limit=20 → both SELECTs gain LIMIT :lim and params carry lim=20.
        captured.clear()
        batch_postmortem(_SpyEngine(), days=30, limit=20)
        select_calls = [c for c in captured if "FROM options_recommendations" in c["sql"]
                        or "FROM oracle_predictions" in c["sql"]]
        assert len(select_calls) == 2
        for call in select_calls:
            assert "LIMIT :lim" in call["sql"]
            assert call["params"].get("lim") == 20

        # Edge: limit=0 (or negative) → behave like no limit (off-switch).
        captured.clear()
        batch_postmortem(_SpyEngine(), days=30, limit=0)
        select_calls = [c for c in captured if "FROM options_recommendations" in c["sql"]
                        or "FROM oracle_predictions" in c["sql"]]
        for call in select_calls:
            assert "LIMIT" not in call["sql"].upper()
