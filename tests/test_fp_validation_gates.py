"""Regression tests for verified defects in validation & promotion machinery.

Covers:
1. validation/gates.py: STAGING->PRODUCTION OOS evidence + SHADOW->STAGING drift gate.
2. contracts/handlers/journal.py + validation/gates.py: provisional row pinning
   kept for NOT NULL safety, while STAGING->PRODUCTION refuses without OOS evidence.
3. validation/backtest.py + scripts/autoresearch.py: turnover costs, required predict_fn,
   warmup/baseline row alignment, autoresearch backtest guard.
4. alpha_research/validation/gauntlet.py: gross null draws, signal-persistence block,
   ROBUST requiring deflated_sharpe_passed.
5. alpha_research/ensemble.py + alpha_research/validation/metrics.py: embargo gap,
   inner early-stopping holdout, removal of extra forward_returns shift.
"""

from __future__ import annotations

import json
import math
from datetime import date, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest

from alpha_research import ensemble
from alpha_research.validation import gauntlet
from alpha_research.validation import metrics as metrics_mod
from alpha_research.validation.metrics import compute_signal_metrics, long_short_returns, rank_ic
from contracts.handlers import journal as journal_handler
from contracts.schemas import SignalFired
from tests.fixtures.evaluator.fake_pit_store import FakePITStore, Row
from validation import gates
from validation.backtest import WalkForwardBacktest
from validation.gates import GateChecker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_engine() -> MagicMock:
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine._mock_conn = conn
    return engine


def _res(row):
    r = MagicMock()
    r.fetchone.return_value = row
    return r


def _ar1_panel(rng: np.random.Generator, n: int, k: int, phi: float) -> pd.DataFrame:
    e = rng.standard_normal((n, k))
    x = np.empty((n, k))
    x[0] = e[0] / math.sqrt(1.0 - phi * phi)
    for t in range(1, n):
        x[t] = phi * x[t - 1] + e[t]
    dates = pd.bdate_range("2022-01-03", periods=n)
    return pd.DataFrame(x, index=dates, columns=[f"T{i:02d}" for i in range(k)])


def _fast_long_short_with_costs(
    signal: pd.DataFrame,
    forward_returns: pd.DataFrame,
    top_n: int = 5,
    cost_bps: float = 10.0,
) -> pd.Series:
    """Vectorized drop-in for ``long_short_returns`` on complete aligned panels.

    Preserves the exact turnover-cost formula so a regression that stops
    zeroing ``cost_bps`` inside ``permutation_test`` still blows up FPR.
    """
    s = signal.to_numpy(dtype=float, copy=False)
    f = forward_returns.to_numpy(dtype=float, copy=False)
    order = np.argsort(s, axis=1, kind="mergesort")
    rows = np.arange(len(s))[:, None]
    short_idx = order[:, :top_n]
    long_idx = order[:, -top_n:]
    gross = f[rows, long_idx].mean(axis=1) - f[rows, short_idx].mean(axis=1)
    if cost_bps == 0.0:
        return pd.Series(gross, index=signal.index, name="ls_return")
    n_rows, n_cols = s.shape
    long_mask = np.zeros((n_rows, n_cols), dtype=bool)
    short_mask = np.zeros((n_rows, n_cols), dtype=bool)
    long_mask[rows, long_idx] = True
    short_mask[rows, short_idx] = True
    prev_long = np.zeros((n_rows, n_cols), dtype=bool)
    prev_short = np.zeros((n_rows, n_cols), dtype=bool)
    prev_long[1:] = long_mask[:-1]
    prev_short[1:] = short_mask[:-1]
    long_to = (long_mask & ~prev_long).sum(axis=1) / max(top_n, 1)
    short_to = (short_mask & ~prev_short).sum(axis=1) / max(top_n, 1)
    cost = ((long_to + short_to) / 2.0) * cost_bps * 2.0 / 10_000.0
    return pd.Series(gross - cost, index=signal.index, name="ls_return")


# ===========================================================================
# Defect 1: validation/gates.py
# ===========================================================================


def _typed_val_row(
    evidence: dict[str, Any],
    *,
    verdict: str = "PASS",
    version: Any = 1,
    extra_gate_detail: dict[str, Any] | None = None,
) -> tuple:
    """A validation_results row whose gate_detail carries a typed OOS record.

    Row shape: (overall_verdict, full_period_metrics, baseline_comparison, gate_detail).
    """
    record = {"schema": gates.OOS_EVIDENCE_SCHEMA, "version": version, **evidence}
    gate_detail = {gates.OOS_EVIDENCE_KEY: record, **(extra_gate_detail or {})}
    return (verdict, json.dumps({}), json.dumps({}), json.dumps(gate_detail))


def _production_check(val_row, model_id: int = 10) -> tuple[dict, MagicMock]:
    engine = _mock_engine()
    conn = engine._mock_conn
    conn.execute.side_effect = [
        _res(("REGIME",)),  # model_registry
        _res((25,)),        # decision_journal count
        _res(None),         # existing PRODUCTION
        _res(val_row),      # validation_results for model_version_id
    ]
    return GateChecker(engine).check_staging_to_production(model_id), conn


@pytest.fixture
def stand_in_oos_reader(monkeypatch):
    """Register a stand-in v1 typed OOS reader. None ships (see validation.gates)."""
    calls: list[int] = []

    def _reader(record: dict[str, Any], model_id: int) -> dict[str, Any]:
        calls.append(model_id)
        return dict(record)

    monkeypatch.setitem(gates._OOS_EVIDENCE_READERS, 1, _reader)
    return calls


class TestGatesStagingToProduction:
    def test_refuses_when_no_oos_evidence_record_exists(self):
        engine = _mock_engine()
        conn = engine._mock_conn
        conn.execute.side_effect = [
            _res(("REGIME",)),  # model_registry
            _res((25,)),        # decision_journal count
            _res(None),         # existing PRODUCTION
            _res(None),         # validation_results for model_version_id -> missing
        ]
        res = GateChecker(engine).check_staging_to_production(10)
        assert res["passed"] is False
        assert any("missing_oos_evidence" in d for d in res["details"])

    def test_refuses_when_fewer_than_30_scored_forward_predictions(self, stand_in_oos_reader):
        res, _ = _production_check(_typed_val_row({"n_scored": 29, "hit_rate": 0.90}))
        assert res["passed"] is False
        assert any("insufficient_scored_predictions" in d for d in res["details"])

    def test_refuses_when_wilson_95_lower_bound_at_or_below_half(self, stand_in_oos_reader):
        # n=30, k=20 -> hit_rate=0.6667, Wilson 95% lower bound = 0.4878 <= 0.5
        res, _ = _production_check(_typed_val_row({"n_scored": 30, "n_hits": 20}))
        assert res["passed"] is False
        assert any("oos_skill_below_threshold" in d for d in res["details"])

    def test_passes_when_wilson_95_lower_bound_exceeds_half(self, stand_in_oos_reader):
        # n=30, k=21 -> hit_rate=0.70, Wilson 95% lower bound = 0.5212 > 0.5
        res, _ = _production_check(_typed_val_row({"n_scored": 30, "n_hits": 21}))
        assert res["passed"] is True
        assert stand_in_oos_reader == [10]  # the reader saw this model_version_id

    def test_passes_when_brier_strictly_below_climatology(self, stand_in_oos_reader):
        res, _ = _production_check(
            _typed_val_row({"n_scored": 40, "brier": 0.19, "climatology_brier": 0.25})
        )
        assert res["passed"] is True

    def test_refuses_when_recorded_overfitting_check_failed(self, stand_in_oos_reader):
        res, _ = _production_check(
            _typed_val_row(
                {"n_scored": 50, "hit_rate": 0.76},
                extra_gate_detail={
                    "overfit_gate": {"passed": False, "reasons": ["PBO 0.62 >= 0.5"]}
                },
            )
        )
        assert res["passed"] is False
        assert any("overfitting_check_failed" in d for d in res["details"])


class TestGateRefusesUntypedOOSEvidence:
    """Codex HOLD 6011907730: STAGING -> PRODUCTION must not accept generic
    recent counts or rates. Only a typed, versioned OOS record read by a
    registered reader (which proves cohort and clock provenance) counts, and
    no reader ships, so the gate is fail-closed."""

    _GENERIC_PASSING = {"n_scored": 40, "hit_rate": 0.75, "n_hits": 30,
                        "brier": 0.10, "climatology_brier": 0.25}

    @pytest.mark.parametrize("where", ["full_period_metrics", "gate_detail"])
    def test_refuses_generic_counts_without_typed_provenance(self, where):
        fpm = self._GENERIC_PASSING if where == "full_period_metrics" else {}
        gd = self._GENERIC_PASSING if where == "gate_detail" else {}
        res, _ = _production_check(("PASS", json.dumps(fpm), json.dumps({}), json.dumps(gd)))
        assert res["passed"] is False
        assert any(d.startswith("untyped_oos_evidence") for d in res["details"])
        # Generic counts are never read as skill evidence.
        assert not any("scored forward predictions" in d for d in res["details"])

    def test_generic_counts_refused_even_when_a_reader_is_registered(self, stand_in_oos_reader):
        res, _ = _production_check(
            ("PASS", json.dumps(self._GENERIC_PASSING), json.dumps({}), json.dumps({}))
        )
        assert res["passed"] is False
        assert any(d.startswith("untyped_oos_evidence") for d in res["details"])
        assert stand_in_oos_reader == []

    @pytest.mark.parametrize(
        "record",
        [
            {"n_scored": 40, "n_hits": 35},                                   # no schema, no version
            {"schema": "grid.oos_forward_evidence", "n_scored": 40, "n_hits": 35},  # no version
            {"schema": "grid.oos_forward_evidence", "version": "1", "n_scored": 40},
            {"schema": "grid.oos_forward_evidence", "version": True, "n_scored": 40},
            {"schema": "some.other_schema", "version": 1, "n_scored": 40, "n_hits": 35},
        ],
    )
    def test_refuses_unversioned_typed_record(self, record, stand_in_oos_reader):
        gd = {"oos_evidence": record}
        res, _ = _production_check(("PASS", json.dumps({}), json.dumps({}), json.dumps(gd)))
        assert res["passed"] is False
        assert any(d.startswith("unversioned_oos_evidence") for d in res["details"])
        assert stand_in_oos_reader == []

    def test_no_typed_reader_ships_so_a_typed_record_is_still_refused(self):
        assert gates._OOS_EVIDENCE_READERS == {}
        res, _ = _production_check(_typed_val_row({"n_scored": 400, "n_hits": 380}))
        assert res["passed"] is False
        assert any(d.startswith("unsupported_oos_evidence_version") for d in res["details"])

    def test_reader_refusal_or_bad_output_fails_closed(self, monkeypatch):
        def _refuses(record, model_id):
            raise ValueError("cohort predates model_version_id 10")

        monkeypatch.setitem(gates._OOS_EVIDENCE_READERS, 1, _refuses)
        res, _ = _production_check(_typed_val_row({"n_scored": 400, "n_hits": 380}))
        assert res["passed"] is False
        assert any(
            d.startswith("oos_evidence_rejected") and "cohort predates" in d
            for d in res["details"]
        )

        monkeypatch.setitem(gates._OOS_EVIDENCE_READERS, 1, lambda record, mid: None)
        res, _ = _production_check(_typed_val_row({"n_scored": 400, "n_hits": 380}))
        assert res["passed"] is False
        assert any(d.startswith("oos_evidence_rejected") for d in res["details"])

    def test_shipped_backtest_writer_row_is_unversioned_and_refused(self):
        """The shipped writer stores no model_version_id and no record version.

        The gate already reads only rows bound to this model_version_id (no
        hypothesis_id fallback), so a shipped-writer row never matches; and
        even if that payload were attributed to the model, generic counts in
        it are refused.
        """
        write_engine = _mock_engine()
        bt = WalkForwardBacktest(write_engine, pit_store=None)
        bt._store_result(
            {
                "hypothesis_id": 3,
                "vintage_policy": "FIRST_RELEASE",
                "era_results": [{"status": "OK", "return": 0.02}],
                "full_period_metrics": {"return": 0.05, "sharpe": 1.9, "n_days": 400,
                                        "n_scored": 40, "hit_rate": 0.75},
                "baseline_comparison": {"sharpe": 0.4},
                "simplicity_comparison": {},
                "walk_forward_splits": 5,
                "cost_assumption_bps": 10.0,
                "overall_verdict": "PASS",
                "gate_detail": {"era_consistency": True, "beats_baseline": True,
                                "positive_in_all_eras": True},
            }
        )
        ins_sql, params = write_engine._mock_conn.execute.call_args.args
        assert "INSERT INTO validation_results" in str(ins_sql)
        assert "model_version_id" not in str(ins_sql)

        res, conn = _production_check((params["ov"], params["fpm"], params["bc"], params["gd"]))
        val_sql, val_params = conn.execute.call_args_list[3].args
        assert "FROM validation_results" in str(val_sql)
        assert "WHERE model_version_id = :mid" in str(val_sql)
        assert "hypothesis_id" not in str(val_sql)
        assert val_params == {"mid": 10}
        assert res["passed"] is False
        assert any(d.startswith("untyped_oos_evidence") for d in res["details"])


class TestGatesShadowToStagingDrift:
    def _base_shadow_side_effects(self):
        return [
            _res(("xgboost",)),
            _res((99,)),       # artifact exists
            _res(("PASS",)),   # validation verdict
            _res((20,)),       # 20 shadow days
        ]

    def test_refuses_when_drift_check_unavailable(self):
        engine = _mock_engine()
        engine._mock_conn.execute.side_effect = self._base_shadow_side_effects()
        res = GateChecker(engine).check_shadow_to_staging(5)
        assert res["passed"] is False
        assert any("drift_check_unavailable" in d for d in res["details"])

    def test_force_without_override_reason_still_refuses(self):
        for bad_reason in (None, "", "   "):
            engine = _mock_engine()
            engine._mock_conn.execute.side_effect = self._base_shadow_side_effects()
            res = GateChecker(engine).check_shadow_to_staging(
                5, force=True, override_reason=bad_reason
            )
            assert res["passed"] is False
            assert any("drift_check_unavailable" in d for d in res["details"])

    def test_force_with_override_reason_passes_and_records_override(self):
        engine = _mock_engine()
        engine._mock_conn.execute.side_effect = self._base_shadow_side_effects()
        res = GateChecker(engine).check_all_gates(
            5, "STAGING", force=True, override_reason="OPS-42: drift pipeline offline"
        )
        assert res["passed"] is True
        assert any("OPS-42: drift pipeline offline" in d for d in res["details"])
        assert res.get("override", {}).get("reason") == "OPS-42: drift pipeline offline"

    def test_refuses_when_critical_drift_detected_even_with_force(self):
        engine = _mock_engine()
        engine._mock_conn.execute.side_effect = self._base_shadow_side_effects()
        critical_checker = lambda _mid: {
            "overall_drift_score": 0.90,
            "recommended_action": "FLAG",
            "data_distribution_drift": {"1": {"drifted": True}},
        }
        gc = GateChecker(engine, drift_checker=critical_checker)
        res = gc.check_shadow_to_staging(5, force=True, override_reason="override attempt")
        assert res["passed"] is False
        assert any("critical_drift_detected" in d for d in res["details"])

    def test_passes_when_drift_checker_reports_ok_with_evidence(self):
        engine = _mock_engine()
        engine._mock_conn.execute.side_effect = self._base_shadow_side_effects()
        ok_checker = lambda _mid: {
            "overall_drift_score": 0.05,
            "recommended_action": "OK",
            "data_distribution_drift": {"1": {"drifted": False}},
        }
        gc = GateChecker(engine, drift_checker=ok_checker)
        res = gc.check_shadow_to_staging(5)
        assert res["passed"] is True
        assert any("No critical drift detected" in d for d in res["details"])


# ===========================================================================
# Defect 2: contracts/handlers/journal.py provisional-row pinning + gate guard
# ===========================================================================


class TestJournalProvisionalPinning:
    def _signal(self, **extra) -> SignalFired:
        return SignalFired(
            producer_module="intelligence.chain_contagion",
            correlation_id=uuid4(),
            signal_id=uuid4(),
            source="chain_contagion:brent_crude",
            signal_type="contagion_ranked_impact",
            strength=0.92,
            ticker="XOM",
            raw_row_ids=[1],
            **extra,
        )

    def test_pins_provisional_row_to_oldest_model_registry_row_for_not_null(self):
        engine = _mock_engine()
        conn = engine._mock_conn
        conn.execute.side_effect = [_res((7,)), MagicMock()]

        journal_handler.on_signal_fired(self._signal(), engine=engine)

        assert conn.execute.call_count == 2
        sel_sql = str(conn.execute.call_args_list[0].args[0])
        ins_sql, params = conn.execute.call_args_list[1].args
        assert "SELECT id FROM model_registry" in sel_sql
        assert "INSERT INTO decision_journal" in str(ins_sql)
        assert params["mvid"] == 7

    def test_signal_fired_schema_has_no_model_version_id_field(self):
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            self._signal(model_version_id=42)

    def test_pinned_provisional_journal_rows_alone_cannot_promote_to_production(self):
        """Even after 25 provisional journal rows are pinned to model 1,
        STAGING -> PRODUCTION refuses when no per-model OOS evidence exists."""
        pinned_rows: list[dict] = []
        journal_engine = _mock_engine()
        jconn = journal_engine._mock_conn

        def _journal_exec(stmt, params=None):
            sql = str(stmt)
            if "SELECT id FROM model_registry" in sql:
                return _res((1,))
            if "INSERT INTO decision_journal" in sql:
                pinned_rows.append(dict(params))
                return MagicMock()
            raise AssertionError(f"Unexpected SQL: {sql}")

        jconn.execute.side_effect = _journal_exec
        for _ in range(25):
            journal_handler.on_signal_fired(self._signal(), engine=journal_engine)

        assert len(pinned_rows) == 25
        assert all(r["mvid"] == 1 for r in pinned_rows)

        gate_engine = _mock_engine()
        gconn = gate_engine._mock_conn
        gconn.execute.side_effect = [
            _res(("REGIME",)),          # model_registry lookup
            _res((len(pinned_rows),)),  # decision_journal count = 25 (all pinned)
            _res(None),                 # no existing PRODUCTION
            _res(None),                 # no validation_results for model_version_id=1
        ]
        res = GateChecker(gate_engine).check_staging_to_production(1)
        assert res["passed"] is False
        assert any("missing_oos_evidence" in d for d in res["details"])


# ===========================================================================
# Defect 3: validation/backtest.py + scripts/autoresearch.py
# ===========================================================================


class TestBacktestCostsAndAlignment:
    def test_flat_position_pays_zero_cost(self):
        bt = WalkForwardBacktest(db_engine=None, pit_store=None)
        dates = pd.DatetimeIndex([date(2024, 1, i) for i in range(1, 11)], name="obs_date")
        matrix = pd.DataFrame({1: [100.0 + i for i in range(10)], 2: range(10)}, index=dates)

        metrics, adj = bt._compute_era_metrics_with_returns(
            matrix,
            predict_fn=lambda f: pd.Series(0.0, index=f.index),
            cost_bps=50.0,
            target_feature_id=1,
        )
        assert metrics["return"] == pytest.approx(0.0, abs=1e-12)
        assert adj is not None
        assert (adj == 0.0).all()

    def test_turnover_cost_formula_exact_on_flat_prices(self):
        bt = WalkForwardBacktest(db_engine=None, pit_store=None)
        # 6 dates -> 5 returns (all 0.0). Signals at d0..d4 become lagged positions at d1..d5:
        # positions = [0.0, 1.0, 1.0, -1.0, 0.0]
        # |Δpos|    = [0.0, 1.0, 0.0,  2.0, 1.0]
        # cost (10 bps = 0.001) = [0.0, 0.001, 0.0, 0.002, 0.001]
        dates = pd.DatetimeIndex([date(2024, 1, i) for i in range(1, 7)], name="obs_date")
        matrix = pd.DataFrame({1: [100.0] * 6, 2: [0.0] * 6}, index=dates)
        raw = [0.0, 1.0, 1.0, -1.0, 0.0, 0.0]

        _, adj = bt._compute_era_metrics_with_returns(
            matrix,
            predict_fn=lambda f: pd.Series(raw, index=f.index),
            cost_bps=10.0,
            target_feature_id=1,
        )
        assert adj is not None
        expected = np.array([0.0, -0.001, 0.0, -0.002, -0.001])
        np.testing.assert_allclose(adj.to_numpy(), expected, atol=1e-12)

    def test_predict_fn_none_raises_value_error(self):
        bt = WalkForwardBacktest(db_engine=None, pit_store=None)
        dates = pd.DatetimeIndex([date(2024, 1, 1), date(2024, 1, 2)], name="obs_date")
        matrix = pd.DataFrame({1: [100.0, 110.0]}, index=dates)

        with pytest.raises(ValueError, match="predict_fn"):
            bt._compute_era_metrics(matrix, None, cost_bps=0.0, target_feature_id=1)

        with pytest.raises(ValueError, match="predict_fn"):
            bt.run_validation(
                hypothesis_id=1,
                feature_ids=[1],
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 10),
                predict_fn=None,
                fit_fn=None,
            )

    def test_warmup_constant_long_starts_on_same_row_as_baseline_and_never_beats_it(self):
        rng = np.random.default_rng(123)
        beats = 0
        for trial in range(40):
            dates = [date(2024, 1, 1) + timedelta(days=k) for k in range(35)]
            prices = 100.0 * np.cumprod(1.0 + rng.normal(0.0, 0.015, len(dates)))
            store = FakePITStore(
                rows=[
                    Row(feature_id=1, obs_date=d, value=float(p), release_date=d, vintage_date=d)
                    for d, p in zip(dates, prices)
                ]
                + [
                    Row(feature_id=2, obs_date=d, value=float(k), release_date=d, vintage_date=d)
                    for k, d in enumerate(dates)
                ]
            )
            bt = WalkForwardBacktest(db_engine=None, pit_store=store)

            def warmup_const_long(features: pd.DataFrame) -> pd.Series:
                s = pd.Series(1.0, index=features.index)
                s.iloc[0] = np.nan  # 1-row warmup (like any lag/diff predictor)
                return s

            res_gross = bt.run_validation(
                hypothesis_id=1,
                feature_ids=[1, 2],
                start_date=dates[0],
                end_date=dates[-1],
                n_splits=2,
                cost_bps=0.0,
                predict_fn=warmup_const_long,
                target_feature_id=1,
            )
            assert res_gross["full_period_metrics"]["return"] == pytest.approx(
                res_gross["baseline_comparison"]["return"], abs=1e-6
            )

            res_net = bt.run_validation(
                hypothesis_id=1,
                feature_ids=[1, 2],
                start_date=dates[0],
                end_date=dates[-1],
                n_splits=2,
                cost_bps=10.0,
                predict_fn=warmup_const_long,
                target_feature_id=1,
            )
            if res_net["full_period_metrics"]["sharpe"] > res_net["baseline_comparison"]["sharpe"]:
                beats += 1

        assert beats == 0


class TestAutoresearchBacktestGuard:
    def test_autoresearch_refuses_hypothesis_without_predictor_and_target(self, monkeypatch):
        import psycopg2
        import scripts.autoresearch as autoresearch
        import store.snapshots as snapshots_module

        class _NoopSnapshot:
            def __init__(self, db_engine=None):
                pass

            def save_snapshot(self, *a, **k):
                return 1

        class _FakeCur:
            def __init__(self):
                self.updates = []
                self._last = None

            def execute(self, sql, params=None):
                s = " ".join(sql.split())
                if s.startswith("SELECT id, state FROM hypothesis_registry"):
                    self._last = None
                elif s.startswith("INSERT INTO hypothesis_registry"):
                    self._last = (101,)
                elif s.startswith("UPDATE hypothesis_registry"):
                    self.updates.append(params)
                    self._last = None
                else:
                    self._last = None

            def fetchone(self):
                return self._last

            def fetchall(self):
                return []

        fake_cur = _FakeCur()

        class _FakeConn:
            autocommit = False

            def cursor(self):
                return fake_cur

            def close(self):
                pass

        backtest_called = []

        class _SpyBacktester:
            def run_validation(self, **kwargs):
                backtest_called.append(kwargs)
                return {
                    "overall_verdict": "PASS",
                    "full_period_metrics": {"sharpe": 2.0},
                    "baseline_comparison": {"sharpe": 0.1},
                    "era_results": [],
                }

        monkeypatch.setattr(snapshots_module, "AnalyticalSnapshotStore", _NoopSnapshot)
        monkeypatch.setattr(autoresearch, "_ortho_cache", None)
        monkeypatch.setattr(autoresearch, "get_engine", lambda: object())
        monkeypatch.setattr(autoresearch, "PITStore", lambda engine: object())
        monkeypatch.setattr(autoresearch, "WalkForwardBacktest", lambda e, p: _SpyBacktester())
        monkeypatch.setattr(
            autoresearch,
            "get_ollama",
            lambda: SimpleNamespace(is_available=True, chat=lambda *a, **k: "stub"),
        )
        monkeypatch.setattr(autoresearch, "OllamaReasoner", lambda o: object())
        monkeypatch.setattr(autoresearch, "get_feature_list", lambda cur: "f1, f2")
        monkeypatch.setattr(autoresearch, "get_feature_name_map", lambda cur: {1: "a", 2: "b"})
        monkeypatch.setattr(autoresearch, "get_market_snapshot", lambda cur: "snap")
        monkeypatch.setattr(autoresearch, "_select_orthogonal_features", lambda cur, **k: [1, 2])
        monkeypatch.setattr(
            autoresearch,
            "parse_hypothesis_json",
            lambda _txt: {
                "statement": "Dummy LLM hypothesis with no predictor",
                "feature_ids": [1, 2],
                "lag_structure": {"1": 0, "2": 5},
                "layer": "REGIME",
                "proposed_metric": "sharpe",
                "proposed_threshold": 0.5,
            },
        )
        monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: _FakeConn())

        result = autoresearch.run_autoresearch(max_iterations=1, run_id="guard-test-1")

        assert backtest_called == []
        assert result["passed"] is False
        assert fake_cur.updates, "Hypothesis should be marked FAILED on refused backtest"
        assert "Refusing backtest" in fake_cur.updates[0][0]


# ===========================================================================
# Defect 4: alpha_research/validation/gauntlet.py
# ===========================================================================


class TestGauntletCalibrationAndVerdict:
    def test_permutation_null_draws_use_zero_cost(self, monkeypatch):
        rng = np.random.default_rng(7)
        signal = _ar1_panel(rng, 120, 12, 0.95)
        fwd = pd.DataFrame(
            rng.normal(0.0, 0.01, size=signal.shape),
            index=signal.index,
            columns=signal.columns,
        )
        recorded_costs: list[float] = []
        real_fn = gauntlet.long_short_returns

        def spy_ls(sig, ret, top_n=5, cost_bps=10.0):
            recorded_costs.append(cost_bps)
            return real_fn(sig, ret, top_n, cost_bps)

        monkeypatch.setattr(gauntlet, "long_short_returns", spy_ls)
        gauntlet.permutation_test(signal, fwd, n_shuffles=10, top_n=3, cost_bps=10.0, seed=42)

        assert len(recorded_costs) == 11
        assert all(c == 0.0 for c in recorded_costs)

    def test_block_length_reflects_signal_persistence_even_on_white_returns(self):
        rng = np.random.default_rng(19)
        persistent_sig = _ar1_panel(rng, 250, 16, 0.95)
        white_ls = pd.Series(rng.normal(0.0, 0.01, size=250), index=persistent_sig.index)

        block_p, basis_p = gauntlet.permutation_block_length(
            white_ls, block=0, overlap_depth=0, signal=persistent_sig
        )
        # phi=0.95 -> half-life ~14; old rule gave ~1 because white_ls has acf1 ~0.
        assert block_p >= 8
        assert basis_p.get("signal_rank_acf1", 0.0) > 0.8

        iid_sig = pd.DataFrame(
            rng.normal(size=(250, 16)),
            index=persistent_sig.index,
            columns=persistent_sig.columns,
        )
        block_iid, _ = gauntlet.permutation_block_length(
            white_ls, block=0, overlap_depth=0, signal=iid_sig
        )
        assert block_iid == 1

    def test_persistent_null_with_10bps_costs_fast(self, monkeypatch):
        """Fast default-suite calibration check on persistent null worlds with 10 bps costs."""
        # Verify the vectorized cost-aware helper matches production long_short_returns exactly.
        rng_eq = np.random.default_rng(7)
        s_eq = _ar1_panel(rng_eq, 60, 12, 0.95)
        f_eq = pd.DataFrame(
            rng_eq.normal(0.0, 0.01, size=s_eq.shape),
            index=s_eq.index,
            columns=s_eq.columns,
        )
        for bps in (0.0, 10.0):
            np.testing.assert_allclose(
                _fast_long_short_with_costs(s_eq, f_eq, top_n=3, cost_bps=bps).to_numpy(),
                long_short_returns(s_eq, f_eq, top_n=3, cost_bps=bps).to_numpy(),
                atol=1e-12,
            )

        monkeypatch.setattr(gauntlet, "long_short_returns", _fast_long_short_with_costs)
        rng = np.random.default_rng(2026)
        rejections = 0
        worlds = 20
        for w in range(worlds):
            sig = _ar1_panel(rng, 250, 12, 0.95)
            fwd = pd.DataFrame(
                rng.normal(0.0, 0.01, size=sig.shape),
                index=sig.index,
                columns=sig.columns,
            )
            perm = gauntlet.permutation_test(
                sig, fwd, n_shuffles=199, top_n=3, cost_bps=10.0, seed=w
            )
            assert perm.min_attainable_p <= 0.005
            assert perm.block >= 6
            if perm.p_value < 0.05:
                rejections += 1
        assert rejections / worlds <= 0.15

    @pytest.mark.slow
    def test_persistent_null_with_10bps_costs_100_worlds_calibrated(self, monkeypatch):
        """Full deterministic null-calibration check: 100 persistent-noise worlds,
        199 permutations (min_attainable_p=0.005 << 0.05), 10 bps costs, FPR <= 0.10."""
        monkeypatch.setattr(gauntlet, "long_short_returns", _fast_long_short_with_costs)
        rng = np.random.default_rng(42)
        rejections = 0
        worlds = 100
        for w in range(worlds):
            sig = _ar1_panel(rng, 250, 12, 0.95)
            fwd = pd.DataFrame(
                rng.normal(0.0, 0.01, size=sig.shape),
                index=sig.index,
                columns=sig.columns,
            )
            perm = gauntlet.permutation_test(
                sig, fwd, n_shuffles=199, top_n=3, cost_bps=10.0, seed=w
            )
            assert perm.min_attainable_p <= 0.005
            assert perm.block >= 6
            if perm.p_value < 0.05:
                rejections += 1
        # Pre-fix rejected ~80%; calibrated nominal size is ~4.5% (9/200).
        assert rejections / worlds <= 0.10

    def test_robust_requires_deflated_sharpe_passed_and_reports_reason(self):
        rng = np.random.default_rng(99)
        n, k = 300, 12
        dates = pd.bdate_range("2021-01-04", periods=n)
        cols = [f"T{i:02d}" for i in range(k)]
        sig_arr = rng.normal(size=(n, k))
        # Strong planted cross-sectional alpha so CV, perm, subsample, net_sharpe all pass.
        fwd_arr = 0.008 * sig_arr + rng.normal(0.0, 0.01, size=(n, k))
        signal = pd.DataFrame(sig_arr, index=dates, columns=cols)
        fwd = pd.DataFrame(fwd_arr, index=dates, columns=cols)

        # 1. Missing trial_sharpes with n_models_tested=4 -> DSR unavailable -> cannot be ROBUST
        res_unavail = gauntlet.run_gauntlet(
            signal,
            fwd,
            n_models_tested=4,
            top_n=3,
            cost_bps=0.0,
            n_permutations=39,
            n_subsample_splits=10,
            seed=42,
        )
        assert res_unavail.cv_consistency >= 0.75
        assert res_unavail.permutation_p < 0.05
        assert res_unavail.subsample_stability > 0.50
        assert res_unavail.deflated_sharpe is None
        assert res_unavail.deflated_sharpe_passed is False
        assert res_unavail.verdict == "MARGINAL"
        assert "deflated_sharpe_unavailable" in res_unavail.details
        assert "verdict_downgrade_reason" in res_unavail.details

        # 2. DSR computed and FAILED (huge trial Sharpe variance + many trials) -> MARGINAL
        wide_trials = np.linspace(-2.0, 2.0, 100)
        res_fail_dsr = gauntlet.run_gauntlet(
            signal,
            fwd,
            n_models_tested=100,
            top_n=3,
            cost_bps=0.0,
            n_permutations=39,
            n_subsample_splits=10,
            seed=42,
            trial_sharpes=wide_trials,
        )
        assert res_fail_dsr.deflated_sharpe_passed is False
        assert res_fail_dsr.verdict == "MARGINAL"
        assert "verdict_downgrade_reason" in res_fail_dsr.details

        # 3. DSR computed and PASSED (single trial, strong alpha) -> ROBUST
        res_pass = gauntlet.run_gauntlet(
            signal,
            fwd,
            n_models_tested=1,
            top_n=3,
            cost_bps=0.0,
            n_permutations=39,
            n_subsample_splits=10,
            seed=42,
        )
        assert res_pass.deflated_sharpe_passed is True
        assert res_pass.verdict == "ROBUST"


# ===========================================================================
# Defect 5: alpha_research/validation/metrics.py & alpha_research/ensemble.py
# ===========================================================================


class TestMetricsAlignment:
    def test_long_short_returns_aligns_signal_t_with_forward_returns_t_no_extra_shift(self):
        dates = pd.bdate_range("2024-01-02", periods=6)
        cols = ["A", "B", "C", "D", "E", "F"]
        # Alternating signs day-by-day: if metrics shifts by -1, every day's sign flips!
        signs = np.array([1, -1, 1, -1, 1, -1])[:, None]
        base = np.array([-3.0, -2.0, -1.0, 1.0, 2.0, 3.0])[None, :]
        signal = pd.DataFrame(signs * base, index=dates, columns=cols)
        # Forward returns ALREADY aligned to decision row t: +0.02 for top, -0.02 for bottom.
        fwd = pd.DataFrame(signs * base * 0.01, index=dates, columns=cols)

        ls = long_short_returns(signal, fwd, top_n=2, cost_bps=0.0)
        # All 6 decision dates must be scored (no dropped final date from extra shift(-1))
        assert len(ls) == 6
        # Long top 2 (+0.02, +0.03 -> mean +0.025) minus short bottom 2 (-0.03, -0.02 -> mean -0.025) = +0.05
        np.testing.assert_allclose(ls.to_numpy(), np.full(6, 0.05), atol=1e-12)

    def test_rank_ic_default_horizon_does_not_double_shift(self):
        dates = pd.bdate_range("2024-01-02", periods=6)
        cols = ["A", "B", "C", "D", "E", "F"]
        signs = np.array([1, -1, 1, -1, 1, -1])[:, None]
        base = np.array([-3.0, -2.0, -1.0, 1.0, 2.0, 3.0])[None, :]
        signal = pd.DataFrame(signs * base, index=dates, columns=cols)
        fwd = pd.DataFrame(signs * base * 0.01, index=dates, columns=cols)

        ic = rank_ic(signal, fwd)
        assert len(ic) == 6
        np.testing.assert_allclose(ic.to_numpy(), np.ones(6), atol=1e-12)


class TestFrozenBookMissingOutcomes:
    """Codex HOLD 6011907730: the book is picked from the decision-time
    universe before any forward return is read. A book member whose outcome
    later goes missing (delisted, halted, data gap) stays in the book, its
    outcome is counted as missing, and coverage is reported. It is never
    replaced by the next-ranked name."""

    _COLS = ["A", "B", "C", "D", "E", "F"]

    def _frames(self, signal_rows, fwd_rows):
        dates = pd.bdate_range("2024-03-01", periods=len(signal_rows))
        signal = pd.DataFrame(signal_rows, index=dates, columns=self._COLS, dtype=float)
        fwd = pd.DataFrame(fwd_rows, index=dates, columns=self._COLS, dtype=float)
        return signal, fwd

    def test_top_ranked_name_with_missing_outcome_is_not_replaced(self):
        # F is top-ranked at the decision; its outcome later goes missing.
        # D is the name a reselection would pull in, and it returned +50%.
        signal, fwd = self._frames(
            [[1, 2, 3, 4, 5, 6]],
            [[-0.01, -0.02, 0.0, 0.50, 0.03, np.nan]],
        )
        ls = long_short_returns(signal, fwd, top_n=2, cost_bps=0.0)
        # Frozen book: long {E, F}, short {A, B}. F's outcome is not imputed,
        # so the long leg is E alone; D never enters the book.
        assert len(ls) == 1
        assert ls.iloc[0] == pytest.approx(0.03 - (-0.01 - 0.02) / 2)

    def test_book_reports_missing_name_and_coverage(self):
        signal, fwd = self._frames(
            [[1, 2, 3, 4, 5, 6]],
            [[-0.01, -0.02, 0.0, 0.50, 0.03, np.nan]],
        )
        book = metrics_mod.long_short_book(signal, fwd, top_n=2, cost_bps=0.0)
        row = book.iloc[0]
        assert row["n_book"] == 4
        assert row["n_observed"] == 3
        assert row["coverage"] == pytest.approx(0.75)
        assert tuple(row["missing"]) == ("F",)
        cov = metrics_mod.outcome_coverage(book)
        assert cov["outcome_coverage"] == pytest.approx(0.75)
        assert cov["n_missing_outcomes"] == 1
        assert cov["n_unscored_days"] == 0

    def test_leg_without_any_observed_outcome_leaves_date_unscored(self):
        # Day 2: both long names (E, F) lose their outcomes. Reselection
        # would score the day with C and D instead.
        signal, fwd = self._frames(
            [[1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6]],
            [[-0.01, -0.02, 0.0, 0.01, 0.03, 0.02],
             [-0.01, -0.02, 0.40, 0.50, np.nan, np.nan]],
        )
        ls = long_short_returns(signal, fwd, top_n=2, cost_bps=0.0)
        assert list(ls.index) == [signal.index[0]]
        book = metrics_mod.long_short_book(signal, fwd, top_n=2, cost_bps=0.0)
        assert len(book) == 2
        assert np.isnan(book["return"].iloc[1])
        assert book["coverage"].iloc[1] == pytest.approx(0.5)
        cov = metrics_mod.outcome_coverage(book)
        assert cov["n_unscored_days"] == 1
        assert cov["outcome_coverage"] == pytest.approx(6 / 8)

    def test_held_name_with_missing_outcome_costs_no_turnover(self):
        # Same ranking on both days; F's day-1 outcome is missing. With the
        # book frozen F is held both days, so day 2 has zero turnover cost.
        signal, fwd = self._frames(
            [[1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6]],
            [[-0.01, -0.02, 0.0, 0.50, 0.03, np.nan],
             [-0.01, -0.02, 0.0, 0.01, 0.03, 0.05]],
        )
        ls = long_short_returns(signal, fwd, top_n=2, cost_bps=10.0)
        day2_gross = (0.03 + 0.05) / 2 - (-0.01 - 0.02) / 2
        assert ls.iloc[1] == pytest.approx(day2_gross, rel=1e-9)

    def test_compute_signal_metrics_reports_outcome_coverage(self):
        rng = np.random.default_rng(5)
        dates = pd.bdate_range("2024-01-02", periods=40)
        cols = [f"T{i:02d}" for i in range(12)]
        signal = pd.DataFrame(rng.normal(size=(40, 12)), index=dates, columns=cols)
        fwd = pd.DataFrame(rng.normal(0, 0.01, size=(40, 12)), index=dates, columns=cols)
        top = signal.iloc[7].idxmax()
        fwd.loc[dates[7], top] = np.nan  # a top-ranked name's outcome goes missing
        m = compute_signal_metrics(signal, fwd, top_n=5, cost_bps=10.0)
        assert m["n_missing_outcomes"] == 1
        assert m["outcome_coverage"] == pytest.approx(1 - 1 / (40 * 10))
        assert m["n_days"] == 40


class TestFrozenBookCostLedgerAndCoverageLabels:
    """Codex HOLD 6031219554: a rebalance on an unscored day (a whole leg's
    outcomes missing) must not vanish from the net series. Every formed book
    pays its turnover on its own date in the cost ledger; an unscored date's
    cost is carried into the next scored net return. Coverage is labelled
    complete/partial per series, and a leg mean over observed members only is
    labelled a conditional estimate."""

    _COLS = ["A", "B", "C", "D", "E", "F"]
    # top_n=2, cost_bps=10: a full one-leg swap costs (1 + 0) / 2 * 10 * 2 / 1e4.
    _ONE_LEG_SWAP = 0.001
    _ENTRY = 0.002  # first book: both legs enter from flat

    def _frames(self, signal_rows, fwd_rows):
        dates = pd.bdate_range("2024-04-01", periods=len(signal_rows))
        signal = pd.DataFrame(signal_rows, index=dates, columns=self._COLS, dtype=float)
        fwd = pd.DataFrame(fwd_rows, index=dates, columns=self._COLS, dtype=float)
        return signal, fwd

    def test_selected_name_change_on_missing_leg_day_is_charged_next_scored_day(self):
        # Day 2 swaps the long leg {E, F} -> {C, D}, and both of C's and D's
        # outcomes are missing, so the day is unscored. Day 3 holds {C, D}:
        # its own turnover is zero, so the day-2 swap must be carried in.
        signal, fwd = self._frames(
            [[1, 2, 3, 4, 5, 6], [1, 2, 5, 6, 3, 4], [1, 2, 5, 6, 3, 4]],
            [[-0.01, -0.02, 0.0, 0.01, 0.03, 0.02],
             [-0.01, -0.02, np.nan, np.nan, 0.03, 0.02],
             [-0.01, -0.02, 0.01, 0.03, 0.0, 0.0]],
        )
        d1, d2, d3 = signal.index
        ls = long_short_returns(signal, fwd, top_n=2, cost_bps=10.0)
        assert list(ls.index) == [d1, d3]
        assert ls.loc[d1] == pytest.approx(0.04 - self._ENTRY, abs=1e-12)
        # Pre-fix: 0.035 (the swap's cost vanished from every later net return).
        assert ls.loc[d3] == pytest.approx(0.035 - self._ONE_LEG_SWAP, abs=1e-12)

        book = metrics_mod.long_short_book(signal, fwd, top_n=2, cost_bps=10.0)
        assert book.loc[d2, "cost"] == pytest.approx(self._ONE_LEG_SWAP, abs=1e-12)
        assert book.loc[d2, "cost_charged"] == 0.0
        assert book.loc[d3, "cost_charged"] == pytest.approx(self._ONE_LEG_SWAP, abs=1e-12)
        # The ledger's total cost lands in the net series: none of it vanishes.
        assert ls.sum() == pytest.approx(
            book["gross"].dropna().sum() - book["cost"].sum(), abs=1e-12
        )
        cov = metrics_mod.outcome_coverage(book)
        assert cov["turnover_cost_carried"] == pytest.approx(self._ONE_LEG_SWAP, abs=1e-12)
        assert cov["turnover_cost_uncharged"] == 0.0

    def test_rebalance_on_market_wide_gap_day_is_a_book_date_not_skipped(self):
        # Day 2 has no realized outcome at all (a data gap inside the window).
        # The book still rotates long {E, F} -> {C, D} there and back on day 3:
        # a round trip that costs two one-leg swaps. Pre-fix day 2 was skipped,
        # day 3 compared against day 1, and the round trip cost nothing.
        nan_row = [np.nan] * 6
        signal, fwd = self._frames(
            [[1, 2, 3, 4, 5, 6], [1, 2, 5, 6, 3, 4], [1, 2, 3, 4, 5, 6]],
            [[-0.01, -0.02, 0.0, 0.01, 0.03, 0.02],
             nan_row,
             [-0.01, -0.02, 0.0, 0.01, 0.03, 0.02]],
        )
        d1, d2, d3 = signal.index
        book = metrics_mod.long_short_book(signal, fwd, top_n=2, cost_bps=10.0)
        assert list(book.index) == [d1, d2, d3]
        assert book.loc[d2, "n_observed"] == 0
        assert np.isnan(book.loc[d2, "return"])
        ls = long_short_returns(signal, fwd, top_n=2, cost_bps=10.0)
        assert ls.loc[d3] == pytest.approx(0.04 - 2 * self._ONE_LEG_SWAP, abs=1e-12)
        assert metrics_mod.outcome_coverage(book)["n_unscored_days"] == 1

    def test_cost_after_last_scored_day_is_reported_uncharged(self):
        # Day 2 swaps the long leg into names with no outcome; day 3 is not
        # yet scorable, declared as a one-row outcome horizon (Codex HOLD
        # 6031771955: the window is declared, never read from which outcomes
        # are null), so it forms no book. The day-2 cost has no later scored
        # date to land on: it is reported as uncharged and the net series is
        # partial.
        nan_row = [np.nan] * 6
        signal, fwd = self._frames(
            [[1, 2, 3, 4, 5, 6], [1, 2, 5, 6, 3, 4], [1, 2, 3, 4, 5, 6]],
            [[-0.01, -0.02, 0.0, 0.01, 0.03, 0.02],
             [-0.01, -0.02, np.nan, np.nan, 0.03, 0.02],
             nan_row],
        )
        book = metrics_mod.long_short_book(
            signal, fwd, top_n=2, cost_bps=10.0, outcome_horizon=1
        )
        assert list(book.index) == list(signal.index[:2])
        cov = metrics_mod.outcome_coverage(book)
        assert cov["n_immature_excluded"] == 1
        assert cov["turnover_cost_uncharged"] == pytest.approx(self._ONE_LEG_SWAP, abs=1e-12)
        assert cov["turnover_cost_total"] == pytest.approx(
            self._ENTRY + self._ONE_LEG_SWAP, abs=1e-12
        )
        assert cov["net_coverage"] == "partial"

    def test_coverage_labels_on_complete_book(self):
        signal, fwd = self._frames(
            [[1, 2, 3, 4, 5, 6], [1, 2, 5, 6, 3, 4]],
            [[-0.01, -0.02, 0.0, 0.01, 0.03, 0.02],
             [-0.01, -0.02, 0.01, 0.03, 0.0, 0.0]],
        )
        cov = metrics_mod.outcome_coverage(
            metrics_mod.long_short_book(signal, fwd, top_n=2, cost_bps=10.0)
        )
        assert cov["gross_coverage"] == "complete"
        assert cov["net_coverage"] == "complete"
        assert cov["estimate"] == "full_book"
        assert cov["n_partial_days"] == 0
        assert cov["n_unscored_days"] == 0
        assert cov["turnover_cost_carried"] == 0.0
        assert cov["turnover_cost_uncharged"] == 0.0

        rng = np.random.default_rng(8)
        dates = pd.bdate_range("2024-01-02", periods=40)
        cols = [f"T{i:02d}" for i in range(12)]
        sig = pd.DataFrame(rng.normal(size=(40, 12)), index=dates, columns=cols)
        ret = pd.DataFrame(rng.normal(0, 0.01, size=(40, 12)), index=dates, columns=cols)
        m = compute_signal_metrics(sig, ret, top_n=5, cost_bps=10.0)
        assert (m["gross_coverage"], m["net_coverage"], m["estimate"]) == (
            "complete", "complete", "full_book",
        )

    def test_coverage_labels_on_partial_book(self):
        # Day 1: F (long) missing, leg mean over E alone -> a partial scored day.
        # Day 2: the whole long leg missing -> unscored. Day 3: complete.
        signal, fwd = self._frames(
            [[1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6], [1, 2, 3, 4, 5, 6]],
            [[-0.01, -0.02, 0.0, 0.01, 0.03, np.nan],
             [-0.01, -0.02, 0.0, 0.01, np.nan, np.nan],
             [-0.01, -0.02, 0.0, 0.01, 0.03, 0.02]],
        )
        cov = metrics_mod.outcome_coverage(
            metrics_mod.long_short_book(signal, fwd, top_n=2, cost_bps=10.0)
        )
        assert cov["n_partial_days"] == 1
        assert cov["n_unscored_days"] == 1
        assert cov["gross_coverage"] == "partial"
        assert cov["net_coverage"] == "partial"
        assert cov["estimate"] == "conditional_on_observed_outcomes"

        rng = np.random.default_rng(5)
        dates = pd.bdate_range("2024-01-02", periods=40)
        cols = [f"T{i:02d}" for i in range(12)]
        sig = pd.DataFrame(rng.normal(size=(40, 12)), index=dates, columns=cols)
        ret = pd.DataFrame(rng.normal(0, 0.01, size=(40, 12)), index=dates, columns=cols)
        ret.loc[dates[7], sig.iloc[7].idxmax()] = np.nan
        m = compute_signal_metrics(sig, ret, top_n=5, cost_bps=10.0)
        assert m["n_partial_days"] == 1
        assert (m["gross_coverage"], m["net_coverage"], m["estimate"]) == (
            "partial", "partial", "conditional_on_observed_outcomes",
        )

    def test_gauntlet_reports_coverage_labels_next_to_net_sharpe(self):
        rng = np.random.default_rng(13)
        n, k = 200, 8
        dates = pd.bdate_range("2023-01-02", periods=n)
        cols = [f"T{i:02d}" for i in range(k)]
        sig = pd.DataFrame(rng.normal(size=(n, k)), index=dates, columns=cols)
        ret = pd.DataFrame(rng.normal(0, 0.01, size=(n, k)), index=dates, columns=cols)
        ret.loc[dates[50], sig.iloc[50].idxmax()] = np.nan
        res = gauntlet.run_gauntlet(
            sig, ret, n_models_tested=1, top_n=2, cost_bps=10.0,
            n_permutations=19, n_subsample_splits=4, seed=1,
        )
        assert res.details["net_coverage"] == "partial"
        assert res.details["gross_coverage"] == "partial"
        assert res.details["estimate"] == "conditional_on_observed_outcomes"
        assert res.details["n_partial_days"] == 1


_EF = [1, 2, 3, 4, 5, 6]  # top_n=2: long {E, F}, short {A, B}
_CD = [1, 2, 5, 6, 3, 4]  # top_n=2: long {C, D}, short {A, B}
_OUT = [-0.01, -0.02, 0.0, 0.01, 0.03, 0.02]  # E, F mean 0.025; A, B mean -0.015
_GAP = [np.nan] * 6  # no outcome observed for any name


def _strong_alpha_panels(n: int = 300, k: int = 12) -> tuple[pd.DataFrame, pd.DataFrame]:
    """A planted cross-sectional alpha strong enough for an unqualified ROBUST."""
    rng = np.random.default_rng(99)
    dates = pd.bdate_range("2021-01-04", periods=n)
    cols = [f"T{i:02d}" for i in range(k)]
    sig_arr = rng.normal(size=(n, k))
    fwd_arr = 0.008 * sig_arr + rng.normal(0.0, 0.01, size=(n, k))
    return (
        pd.DataFrame(sig_arr, index=dates, columns=cols),
        pd.DataFrame(fwd_arr, index=dates, columns=cols),
    )


_GAUNTLET_KW: dict[str, Any] = dict(
    n_models_tested=1, top_n=3, cost_bps=10.0, n_permutations=39,
    n_subsample_splits=10, seed=42,
)


class TestEligibilityWindowAndFullBookNet:
    """Codex HOLD 6031771955: the evaluation window is chronological and
    declared, never the first or last non-null realized outcome. A wholly
    missing mature first or last decision forms its book and pays its
    turnover; only a declared immature tail is excluded, and it is
    labelled. A partial or uncharged book never yields an unqualified ROBUST
    or a net-threshold pass, and the net Sharpe is labelled carried-cost
    accounting, not daily executable P&L."""

    _COLS = ["A", "B", "C", "D", "E", "F"]
    _ONE_LEG_SWAP = 0.001  # top_n=2, cost_bps=10: one leg fully swapped
    _ENTRY = 0.002  # first book: both legs enter from flat

    def _frames(self, signal_rows, fwd_rows):
        dates = pd.bdate_range("2024-05-01", periods=len(signal_rows))
        signal = pd.DataFrame(signal_rows, index=dates, columns=self._COLS, dtype=float)
        fwd = pd.DataFrame(fwd_rows, index=dates, columns=self._COLS, dtype=float)
        return signal, fwd

    def test_wholly_missing_mature_first_date_forms_book_and_pays_its_entry(self):
        # d1 enters long {C, D} and has no observed outcome at all; d2
        # rebalances into {E, F}. Pre-fix the window began at d2 (the first
        # realized outcome): d1 vanished and d2 paid a flat entry instead.
        signal, fwd = self._frames([_CD, _EF, _EF], [_GAP, _OUT, _OUT])
        d1, d2, d3 = signal.index
        book = metrics_mod.long_short_book(signal, fwd, top_n=2, cost_bps=10.0)
        assert list(book.index) == [d1, d2, d3]
        assert book.loc[d1, "n_observed"] == 0
        assert book.loc[d1, "cost"] == pytest.approx(self._ENTRY, abs=1e-12)
        assert book.loc[d2, "cost"] == pytest.approx(self._ONE_LEG_SWAP, abs=1e-12)
        assert book.loc[d2, "cost_charged"] == pytest.approx(
            self._ENTRY + self._ONE_LEG_SWAP, abs=1e-12
        )
        ls = long_short_returns(signal, fwd, top_n=2, cost_bps=10.0)
        assert ls.loc[d2] == pytest.approx(0.04 - self._ENTRY - self._ONE_LEG_SWAP, abs=1e-12)
        cov = metrics_mod.outcome_coverage(book)
        assert cov["n_unscored_days"] == 1
        assert (cov["gross_coverage"], cov["net_coverage"]) == ("partial", "partial")
        assert cov["turnover_cost_carried"] == pytest.approx(self._ENTRY, abs=1e-12)
        assert cov["n_immature_excluded"] == 0

    def test_wholly_missing_mature_last_date_forms_book_with_cost_uncharged(self):
        # d3 rebalances into {C, D} and has no observed outcome. It is a
        # mature row of the outcome calendar (no horizon declared). Pre-fix
        # the window ended at d2, d3 vanished and the book said complete.
        signal, fwd = self._frames([_EF, _EF, _CD], [_OUT, _OUT, _GAP])
        d1, d2, d3 = signal.index
        book = metrics_mod.long_short_book(signal, fwd, top_n=2, cost_bps=10.0)
        assert list(book.index) == [d1, d2, d3]
        assert book.loc[d3, "cost"] == pytest.approx(self._ONE_LEG_SWAP, abs=1e-12)
        cov = metrics_mod.outcome_coverage(book)
        assert cov["n_unscored_days"] == 1
        assert cov["turnover_cost_uncharged"] == pytest.approx(self._ONE_LEG_SWAP, abs=1e-12)
        assert (cov["gross_coverage"], cov["net_coverage"], cov["estimate"]) == (
            "partial", "partial", "conditional_on_observed_outcomes",
        )
        assert cov["net_pass_blocked_reason"] is not None

    def test_declared_immature_tail_stays_excluded_and_is_labelled(self):
        # The same panel, but d3's outcome is declared not yet due (a
        # one-row outcome horizon): d3 forms no book and the book is complete.
        signal, fwd = self._frames([_EF, _EF, _CD], [_OUT, _OUT, _GAP])
        d1, d2, d3 = signal.index
        book = metrics_mod.long_short_book(
            signal, fwd, top_n=2, cost_bps=10.0, outcome_horizon=1
        )
        assert list(book.index) == [d1, d2]
        cov = metrics_mod.outcome_coverage(book)
        assert (cov["gross_coverage"], cov["net_coverage"], cov["estimate"]) == (
            "complete", "complete", "full_book",
        )
        assert cov["outcome_horizon"] == 1
        assert cov["mature_through"] == str(d2)
        assert cov["n_immature_excluded"] == 1
        assert cov["turnover_cost_uncharged"] == 0.0
        assert cov["net_pass_blocked_reason"] is None

    def test_as_of_bounds_the_window_on_the_calendar(self):
        # Every outcome row holds a value, but by as_of=d3 with a one-row
        # horizon only d1 and d2 are mature: d3 and d4 form no book, whatever
        # their outcome rows hold.
        signal, fwd = self._frames([_EF] * 4, [_OUT] * 4)
        d1, d2, d3, d4 = signal.index
        book = metrics_mod.long_short_book(
            signal, fwd, top_n=2, cost_bps=10.0, outcome_horizon=1, as_of=d3
        )
        assert list(book.index) == [d1, d2]
        assert metrics_mod.outcome_coverage(book)["n_immature_excluded"] == 2
        # The calendar cannot date a horizon past its own last row.
        with pytest.raises(ValueError, match="as_of"):
            metrics_mod.long_short_book(
                signal, fwd, top_n=2, outcome_horizon=1, as_of=d4 + pd.Timedelta(days=3)
            )
        with pytest.raises(ValueError, match="outcome_horizon"):
            metrics_mod.long_short_book(signal, fwd, top_n=2, outcome_horizon=-1)

    @pytest.mark.parametrize("gap", [0, 1, 2])
    def test_coverage_partial_whenever_an_eligible_date_is_unscored(self, gap):
        fwd_rows = [_OUT, _OUT, _OUT]
        fwd_rows[gap] = _GAP
        signal, fwd = self._frames([_EF, _CD, _EF], fwd_rows)
        book = metrics_mod.long_short_book(signal, fwd, top_n=2, cost_bps=10.0)
        assert len(book) == 3
        cov = metrics_mod.outcome_coverage(book)
        assert cov["n_unscored_days"] == 1
        assert (cov["gross_coverage"], cov["net_coverage"]) == ("partial", "partial")
        assert cov["net_pass_blocked_reason"] is not None

    def test_net_series_is_labelled_carried_cost_accounting(self):
        signal, fwd = self._frames([_EF, _CD], [_OUT, _OUT])
        cov = metrics_mod.outcome_coverage(
            metrics_mod.long_short_book(signal, fwd, top_n=2, cost_bps=10.0)
        )
        assert cov["net_basis"] == "carried_cost_accounting"
        sig, ret = _strong_alpha_panels(n=40)
        m = compute_signal_metrics(sig, ret, top_n=3, cost_bps=10.0)
        assert m["net_basis"] == "carried_cost_accounting"

    def test_signal_metrics_partial_book_never_passes_net_threshold(self):
        sig, ret = _strong_alpha_panels()
        complete = compute_signal_metrics(sig, ret, top_n=3, cost_bps=10.0)
        assert complete["sharpe_net"] > 1.4
        assert complete["passes_threshold"] is True
        assert complete["net_pass_blocked_reason"] is None

        ret = ret.copy()
        ret.loc[ret.index[150], sig.iloc[150].idxmax()] = np.nan  # one partial date
        partial = compute_signal_metrics(sig, ret, top_n=3, cost_bps=10.0)
        assert partial["sharpe_net"] > 1.4  # the Sharpe alone would pass
        assert partial["passes_threshold"] is False
        assert "net coverage partial" in partial["net_pass_blocked_reason"]

    def test_gauntlet_complete_book_reaches_robust_unchanged(self):
        sig, ret = _strong_alpha_panels()
        res = gauntlet.run_gauntlet(sig, ret, **_GAUNTLET_KW)
        assert res.verdict == "ROBUST"
        assert res.details["passes_net_sharpe_threshold"] is True
        assert res.details["net_coverage"] == "complete"
        assert res.details["net_pass_blocked_reason"] is None
        assert res.details["net_basis"] == "carried_cost_accounting"
        assert "verdict_downgrade_reason" not in res.details

    def test_gauntlet_partial_book_cannot_reach_robust_or_net_pass(self):
        sig, ret = _strong_alpha_panels()
        ret = ret.copy()
        ret.loc[ret.index[150], sig.iloc[150].idxmax()] = np.nan  # one partial date
        res = gauntlet.run_gauntlet(sig, ret, **_GAUNTLET_KW)
        assert res.details["net_sharpe"] > 1.4  # the Sharpe alone would pass
        assert res.deflated_sharpe_passed is True
        assert res.verdict == "MARGINAL"
        assert res.details["passes_net_sharpe_threshold"] is False
        reason = res.details["verdict_downgrade_reason"]
        assert reason.startswith("Downgraded from ROBUST to MARGINAL")
        assert "net coverage partial" in reason

    def test_gauntlet_uncharged_book_cannot_reach_robust_or_net_pass(self):
        # The last decision is mature but wholly missing: its rebalance cost
        # has no later scored date. Pre-fix it fell outside the window and the
        # remaining book said complete and ROBUST.
        sig, ret = _strong_alpha_panels()
        ret = ret.copy()
        ret.iloc[-1] = np.nan
        res = gauntlet.run_gauntlet(sig, ret, **_GAUNTLET_KW)
        assert res.details["turnover_cost_uncharged"] > 0.0
        assert res.details["net_coverage"] == "partial"
        assert res.verdict == "MARGINAL"
        assert res.details["passes_net_sharpe_threshold"] is False
        assert "uncharged" in res.details["verdict_downgrade_reason"]

    def test_gauntlet_declared_immature_tail_keeps_robust(self):
        sig, ret = _strong_alpha_panels()
        ret = ret.copy()
        ret.iloc[-1] = np.nan  # the last outcome is not yet due
        res = gauntlet.run_gauntlet(sig, ret, outcome_horizon=1, **_GAUNTLET_KW)
        assert res.details["n_immature_excluded"] == 1
        assert res.details["mature_through"] == str(sig.index[-2])
        assert res.details["net_coverage"] == "complete"
        assert res.verdict == "ROBUST"
        assert res.details["passes_net_sharpe_threshold"] is True


class TestEnsembleEmbargoAndEarlyStopping:
    def _mock_lgb(self, monkeypatch):
        recorded: dict = {"datasets": [], "train_calls": []}

        class _FakeDataset:
            def __init__(self, data, label=None, reference=None, feature_name=None):
                self.data = data
                self.label = label
                self.dates = data.index.get_level_values(0).unique().sort_values()
                recorded["datasets"].append(self)

        class _FakeBooster:
            def __init__(self, n_features):
                self._n = n_features

            def feature_importance(self, importance_type="gain"):
                return np.ones(self._n)

            def predict(self, X):
                return np.asarray(X.iloc[:, 0], dtype=float)

        def _fake_train(params, train_set, num_boost_round=200, valid_sets=None, valid_names=None, callbacks=None):
            recorded["train_calls"].append(
                {"train_set": train_set, "valid_sets": list(valid_sets or []), "valid_names": list(valid_names or [])}
            )
            return _FakeBooster(train_set.data.shape[1])

        fake_lgb = SimpleNamespace(
            Dataset=_FakeDataset,
            train=_fake_train,
            early_stopping=lambda n: ("early_stopping", n),
            log_evaluation=lambda n: ("log_evaluation", n),
        )
        monkeypatch.setattr(ensemble, "_HAS_LGB", True)
        monkeypatch.setattr(ensemble, "lgb", fake_lgb)
        return recorded

    def _synthetic_panels(self, n_dates: int = 100, n_tickers: int = 12):
        rng = np.random.default_rng(11)
        dates = pd.bdate_range("2023-01-02", periods=n_dates)
        cols = [f"T{i:02d}" for i in range(n_tickers)]
        f1 = pd.DataFrame(rng.normal(size=(n_dates, n_tickers)), index=dates, columns=cols)
        f2 = pd.DataFrame(rng.normal(size=(n_dates, n_tickers)), index=dates, columns=cols)
        fwd = pd.DataFrame(0.01 * f1.to_numpy() + rng.normal(0, 0.02, size=(n_dates, n_tickers)), index=dates, columns=cols)
        return dates, {"f1": f1, "f2": f2}, fwd

    def test_embargo_gap_and_inner_early_stopping_never_use_dval(self, monkeypatch):
        recorded = self._mock_lgb(monkeypatch)
        dates, panels, fwd = self._synthetic_panels(n_dates=100, n_tickers=12)

        res = ensemble.train_ensemble(
            panels,
            fwd,
            train_frac=0.7,
            label_horizon=5,
            embargo_days=5,
            n_rounds=20,
            early_stopping=5,
        )
        assert len(recorded["train_calls"]) == 1
        call = recorded["train_calls"][0]
        train_ds = call["train_set"]
        valid_sets = call["valid_sets"]
        assert len(valid_sets) == 2
        es_ds = valid_sets[1]

        # Outer train ends at dates[69]; with embargo=5, outer val starts at dates[75].
        date_pos = {d: i for i, d in enumerate(dates)}
        val_dates = dates[75:]

        # Early stopping set must NEVER overlap outer validation dates
        assert set(es_ds.dates).isdisjoint(set(val_dates))
        assert set(train_ds.dates).isdisjoint(set(val_dates))

        # Gap between inner train end and early-stopping start >= 5 excluded dates
        fit_last_pos = date_pos[train_ds.dates.max()]
        es_first_pos = date_pos[es_ds.dates.min()]
        es_last_pos = date_pos[es_ds.dates.max()]
        val_first_pos = date_pos[val_dates[0]]

        assert (es_first_pos - fit_last_pos - 1) >= 5
        assert (val_first_pos - es_last_pos - 1) >= 5
        assert res.n_train_samples == len(train_ds.data)

    def test_refuses_embargo_smaller_than_label_horizon(self, monkeypatch):
        self._mock_lgb(monkeypatch)
        _, panels, fwd = self._synthetic_panels(n_dates=100, n_tickers=12)

        with pytest.raises(ValueError, match="embargo"):
            ensemble.train_ensemble(panels, fwd, label_horizon=5, embargo_days=2)

    def test_top_ranked_name_with_missing_outcome_stays_in_decision_universe(self, monkeypatch):
        recorded = self._mock_lgb(monkeypatch)
        dates, panels, fwd = self._synthetic_panels(n_dates=100, n_tickers=12)
        # The fake booster predicts f1, so T00 is top-ranked on validation
        # date dates[80] (outer val starts at dates[75]); its outcome is missing.
        val_day = dates[80]
        panels["f1"].loc[val_day, "T00"] = 50.0
        fwd = fwd.copy()
        fwd.loc[val_day, "T00"] = np.nan
        # A training row with a missing label too: it must not be trained on.
        fwd.loc[dates[10], "T03"] = np.nan

        res = ensemble.train_ensemble(
            panels, fwd, train_frac=0.7, label_horizon=5, embargo_days=5,
            n_rounds=20, early_stopping=5,
        )
        # Ranked over the decision-time universe, not dropped for its outcome.
        assert res.predictions.loc[val_day, "T00"] == pytest.approx(1.0)
        # Training and early-stopping sets carry no unlabeled row.
        for ds in recorded["datasets"]:
            assert not pd.isna(ds.label).any()
        # Coverage is reported: 25 val dates x 10 book slots, one missing.
        assert res.val_outcome_coverage == pytest.approx(1 - 1 / 250)

    def test_reports_coverage_labels_for_train_and_val_books(self, monkeypatch):
        """Codex HOLD 6031219554: train and val Sharpe carry coverage labels."""
        self._mock_lgb(monkeypatch)
        dates, panels, fwd = self._synthetic_panels(n_dates=100, n_tickers=12)
        val_day = dates[80]
        panels["f1"].loc[val_day, "T00"] = 50.0
        fwd = fwd.copy()
        fwd.loc[val_day, "T00"] = np.nan

        res = ensemble.train_ensemble(
            panels, fwd, train_frac=0.7, label_horizon=5, embargo_days=5,
            n_rounds=20, early_stopping=5,
        )
        val = res.book_coverage["val"]
        assert val["net_coverage"] == "partial"
        assert val["estimate"] == "conditional_on_observed_outcomes"
        assert val["n_partial_days"] == 1
        train = res.book_coverage["train"]
        assert train["net_coverage"] == "complete"
        assert train["estimate"] == "full_book"

