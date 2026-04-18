"""ALPHA-7 — calibration drift detection tests.

Pure-function tests on the drift math (z-score thresholds, brier directionality,
minimum sample gates) plus a mocked-DB integration test for
detect_calibration_drift. snapshot_calibration_history is tested against a
mocked engine — real DB path is exercised on the server manually.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from oracle.calibration import (
    DriftAlert,
    _DRIFT_MIN_HISTORY,
    _DRIFT_MIN_SCORED,
    detect_calibration_drift,
    snapshot_calibration_history,
)


# ── DriftAlert dataclass ──────────────────────────────────────────────────


class TestDriftAlertDataclass:
    def test_to_dict_roundtrip(self):
        a = DriftAlert(
            model_name="flow_momentum", horizon_days=7, metric="brier",
            current=0.25, baseline_mean=0.20, baseline_std=0.02,
            z_score=2.5, sigma_threshold=2.0, window_days=30,
            severity="warning",
        )
        d = a.to_dict()
        assert d["model_name"] == "flow_momentum"
        assert d["horizon_days"] == 7
        assert d["metric"] == "brier"
        assert abs(d["z_score"] - 2.5) < 1e-6
        assert d["severity"] == "warning"


# ── Mocked-engine helpers ─────────────────────────────────────────────────


def _build_engine(*, models, per_horizon_map, history_map):
    """Build a MagicMock engine that:

    - `SELECT name FROM oracle_models` returns ``models``
    - `compute_per_horizon_calibration` (via patched return) uses
      ``per_horizon_map[model_name]``
    - history SELECT returns ``history_map[(model, horizon)]`` as list of
      (brier, ece) tuples.
    """
    eng = MagicMock()

    def execute_router(query, params=None):
        result = MagicMock()
        sql = str(query)
        if "SELECT name FROM oracle_models" in sql:
            result.fetchall.return_value = [(m,) for m in models]
        elif "FROM oracle_calibration_history" in sql:
            key = (params.get("m"), params.get("h")) if params else None
            result.fetchall.return_value = history_map.get(key, [])
        else:
            result.fetchall.return_value = []
            result.fetchone.return_value = None
        return result

    conn = MagicMock()
    conn.execute = execute_router
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    eng.connect.return_value = conn
    eng.begin.return_value = conn
    return eng


# ── detect_calibration_drift ──────────────────────────────────────────────


class TestDetectCalibrationDrift:
    def test_no_models_empty(self):
        eng = _build_engine(models=[], per_horizon_map={}, history_map={})
        with patch("oracle.calibration.compute_per_horizon_calibration") as mock_ph:
            mock_ph.return_value = {}
            alerts = detect_calibration_drift(eng, window_days=30)
        assert alerts == []

    def test_cold_start_bucket_skipped(self):
        eng = _build_engine(
            models=["flow_momentum"],
            per_horizon_map={"flow_momentum": {
                7: {"brier": 0.5, "ece": 0.3, "scored": 2},  # below threshold
            }},
            history_map={},
        )
        with patch("oracle.calibration.compute_per_horizon_calibration") as mock_ph:
            mock_ph.return_value = {
                7: {"brier": 0.5, "ece": 0.3, "scored": 2},
            }
            alerts = detect_calibration_drift(eng)
        assert alerts == []

    def test_sparse_history_skipped(self):
        # Bucket has 100 scored predictions but only 2 history rows → skip
        eng = _build_engine(
            models=["flow_momentum"],
            per_horizon_map={},
            history_map={
                ("flow_momentum", 7): [(0.20, 0.10), (0.21, 0.11)],
            },
        )
        with patch("oracle.calibration.compute_per_horizon_calibration") as mock_ph:
            mock_ph.return_value = {
                7: {"brier": 0.40, "ece": 0.20, "scored": 100},
            }
            alerts = detect_calibration_drift(eng)
        assert alerts == []

    def test_brier_regression_fires_alert(self):
        hist = [(0.20, 0.10)] * 10 + [(0.21, 0.11)] * 10  # mean~0.205, std~0.005
        eng = _build_engine(
            models=["flow_momentum"],
            per_horizon_map={},
            history_map={("flow_momentum", 7): hist},
        )
        with patch("oracle.calibration.compute_per_horizon_calibration") as mock_ph:
            mock_ph.return_value = {
                7: {"brier": 0.30, "ece": 0.10, "scored": 200},  # much worse Brier
            }
            alerts = detect_calibration_drift(eng, sigma_threshold=2.0)
        brier_alerts = [a for a in alerts if a.metric == "brier"]
        assert len(brier_alerts) == 1
        a = brier_alerts[0]
        assert a.model_name == "flow_momentum"
        assert a.horizon_days == 7
        assert a.z_score > 2.0
        assert a.severity in ("warning", "critical")

    def test_brier_improvement_not_alerted(self):
        hist = [(0.30, 0.15)] * 10 + [(0.32, 0.16)] * 10
        eng = _build_engine(
            models=["flow_momentum"],
            per_horizon_map={},
            history_map={("flow_momentum", 7): hist},
        )
        with patch("oracle.calibration.compute_per_horizon_calibration") as mock_ph:
            mock_ph.return_value = {
                7: {"brier": 0.15, "ece": 0.15, "scored": 200},  # much better
            }
            alerts = detect_calibration_drift(eng, sigma_threshold=2.0)
        # Brier improvement must NOT fire (positive z only). ECE might.
        brier_alerts = [a for a in alerts if a.metric == "brier"]
        assert len(brier_alerts) == 0

    def test_ece_either_direction_fires(self):
        hist = [(0.20, 0.10)] * 10 + [(0.21, 0.11)] * 10  # ece mean ~ 0.105
        eng = _build_engine(
            models=["flow_momentum"],
            per_horizon_map={},
            history_map={("flow_momentum", 7): hist},
        )
        # ECE dropped sharply (improvement) but we still flag it
        with patch("oracle.calibration.compute_per_horizon_calibration") as mock_ph:
            mock_ph.return_value = {
                7: {"brier": 0.205, "ece": 0.02, "scored": 200},  # huge ECE drop
            }
            alerts = detect_calibration_drift(eng, sigma_threshold=2.0)
        ece_alerts = [a for a in alerts if a.metric == "ece"]
        assert len(ece_alerts) == 1
        assert ece_alerts[0].z_score < 0

    def test_critical_severity_at_3_sigma(self):
        hist = [(0.20, 0.10)] * 10 + [(0.21, 0.11)] * 10  # std ~ 0.005
        eng = _build_engine(
            models=["flow_momentum"],
            per_horizon_map={},
            history_map={("flow_momentum", 7): hist},
        )
        with patch("oracle.calibration.compute_per_horizon_calibration") as mock_ph:
            mock_ph.return_value = {
                7: {"brier": 0.45, "ece": 0.10, "scored": 200},  # huge jump
            }
            alerts = detect_calibration_drift(eng, sigma_threshold=2.0)
        brier_alerts = [a for a in alerts if a.metric == "brier"]
        assert len(brier_alerts) == 1
        assert brier_alerts[0].severity == "critical"

    def test_degenerate_zero_std_no_alert(self):
        # Every historical value identical → std = 0 → skip
        hist = [(0.20, 0.10)] * 20
        eng = _build_engine(
            models=["flow_momentum"],
            per_horizon_map={},
            history_map={("flow_momentum", 7): hist},
        )
        with patch("oracle.calibration.compute_per_horizon_calibration") as mock_ph:
            mock_ph.return_value = {
                7: {"brier": 0.50, "ece": 0.50, "scored": 200},
            }
            alerts = detect_calibration_drift(eng)
        assert alerts == []

    def test_multiple_models_independent(self):
        hist_good = [(0.20, 0.10)] * 20
        hist_bad_pattern = [(0.20, 0.10)] * 10 + [(0.22, 0.11)] * 10
        eng = _build_engine(
            models=["flow_momentum", "regime_contrarian"],
            per_horizon_map={},
            history_map={
                ("flow_momentum", 7): hist_good,        # zero std → skipped
                ("regime_contrarian", 7): hist_bad_pattern,  # non-zero std
            },
        )
        with patch("oracle.calibration.compute_per_horizon_calibration") as mock_ph:
            mock_ph.side_effect = lambda engine, name, horizons=None: {
                7: {"brier": 0.35, "ece": 0.15, "scored": 200},
            }
            alerts = detect_calibration_drift(eng)
        # flow_momentum degenerate → skipped, regime_contrarian → brier alert
        alert_models = {a.model_name for a in alerts}
        assert "regime_contrarian" in alert_models
        assert "flow_momentum" not in alert_models


# ── snapshot_calibration_history ──────────────────────────────────────────


class TestSnapshotCalibrationHistory:
    def test_skips_cold_start_buckets(self):
        eng = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        eng.begin.return_value = conn

        # Model list
        models_result = MagicMock()
        models_result.fetchall.return_value = [("flow_momentum",)]

        insert_calls = []

        def execute_router(query, params=None):
            sql = str(query)
            if "SELECT name FROM oracle_models" in sql:
                return models_result
            if "INSERT INTO oracle_calibration_history" in sql:
                insert_calls.append(params)
                return MagicMock()
            return MagicMock()

        conn.execute = execute_router

        with patch("oracle.calibration.compute_per_horizon_calibration") as mock_ph:
            # Two buckets: one cold-start (below threshold), one scored enough
            mock_ph.return_value = {
                1: {"brier": 0.25, "ece": 0.15, "scored": 3, "weight": 1.0},
                7: {"brier": 0.22, "ece": 0.12, "scored": 50, "weight": 1.0},
            }
            counts = snapshot_calibration_history(eng)

        # Only the 7d bucket should have been inserted
        assert counts["buckets"] == 1
        assert counts["skipped"] == 1
        assert len(insert_calls) == 1
        assert insert_calls[0]["h"] == 7

    def test_handles_db_error_non_fatal(self):
        eng = MagicMock()
        eng.begin.side_effect = RuntimeError("db down")
        counts = snapshot_calibration_history(eng)
        assert counts["buckets"] == 0
