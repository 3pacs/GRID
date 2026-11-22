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
from unittest.mock import MagicMock
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest

from alpha_research import ensemble
from alpha_research.validation import gauntlet
from alpha_research.validation.metrics import long_short_returns, rank_ic
from contracts.handlers import journal as journal_handler
from contracts.schemas import SignalFired
from tests.fixtures.evaluator.fake_pit_store import FakePITStore, Row
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

    def test_refuses_when_fewer_than_30_scored_forward_predictions(self):
        engine = _mock_engine()
        conn = engine._mock_conn
        val_row = (
            "PASS",
            json.dumps({"n_scored": 29, "hit_rate": 0.90}),
            json.dumps({}),
            json.dumps({}),
        )
        conn.execute.side_effect = [
            _res(("REGIME",)),
            _res((25,)),
            _res(None),
            _res(val_row),
        ]
        res = GateChecker(engine).check_staging_to_production(10)
        assert res["passed"] is False
        assert any("insufficient_scored_predictions" in d for d in res["details"])

    def test_refuses_when_wilson_95_lower_bound_at_or_below_half(self):
        # n=30, k=20 -> hit_rate=0.6667, Wilson 95% lower bound = 0.4878 <= 0.5
        engine = _mock_engine()
        conn = engine._mock_conn
        val_row = (
            "PASS",
            json.dumps({"n_scored": 30, "n_hits": 20}),
            json.dumps({}),
            json.dumps({}),
        )
        conn.execute.side_effect = [
            _res(("REGIME",)),
            _res((25,)),
            _res(None),
            _res(val_row),
        ]
        res = GateChecker(engine).check_staging_to_production(10)
        assert res["passed"] is False
        assert any("oos_skill_below_threshold" in d for d in res["details"])

    def test_passes_when_wilson_95_lower_bound_exceeds_half(self):
        # n=30, k=21 -> hit_rate=0.70, Wilson 95% lower bound = 0.5212 > 0.5
        engine = _mock_engine()
        conn = engine._mock_conn
        val_row = (
            "PASS",
            json.dumps({"n_scored": 30, "n_hits": 21}),
            json.dumps({}),
            json.dumps({}),
        )
        conn.execute.side_effect = [
            _res(("REGIME",)),
            _res((25,)),
            _res(None),
            _res(val_row),
        ]
        res = GateChecker(engine).check_staging_to_production(10)
        assert res["passed"] is True

    def test_passes_when_brier_strictly_below_climatology(self):
        engine = _mock_engine()
        conn = engine._mock_conn
        val_row = (
            "PASS",
            json.dumps({"n_scored": 40, "brier": 0.19}),
            json.dumps({"climatology_brier": 0.25}),
            json.dumps({}),
        )
        conn.execute.side_effect = [
            _res(("REGIME",)),
            _res((25,)),
            _res(None),
            _res(val_row),
        ]
        res = GateChecker(engine).check_staging_to_production(10)
        assert res["passed"] is True

    def test_refuses_when_recorded_overfitting_check_failed(self):
        engine = _mock_engine()
        conn = engine._mock_conn
        val_row = (
            "PASS",
            json.dumps({"n_scored": 50, "hit_rate": 0.76}),
            json.dumps({}),
            json.dumps({"overfit_gate": {"passed": False, "reasons": ["PBO 0.62 >= 0.5"]}}),
        )
        conn.execute.side_effect = [
            _res(("REGIME",)),
            _res((25,)),
            _res(None),
            _res(val_row),
        ]
        res = GateChecker(engine).check_staging_to_production(10)
        assert res["passed"] is False
        assert any("overfitting_check_failed" in d for d in res["details"])


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
