"""ALPHA-6 — per-horizon feature importance tests.

Mocks the DB engine so the horizon routing + SQL bind params are tested
in isolation. The real DB path is exercised by a smoke on the server.
"""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock


from features.importance import FeatureImportanceTracker


def _build_tracker(exec_results):
    """Build a FeatureImportanceTracker with a MagicMock engine.

    `exec_results` is a list of objects returned by `.fetchall()` /
    `.fetchone()` in sequence — first call returns index 0, etc.
    """
    tracker = FeatureImportanceTracker.__new__(FeatureImportanceTracker)
    tracker.engine = MagicMock()
    tracker.pit_store = MagicMock()

    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    tracker.engine.connect.return_value = conn
    tracker.engine.begin.return_value = conn

    call_count = [0]

    def execute(*args, **kwargs):
        r = MagicMock()
        idx = call_count[0]
        payload = exec_results[idx] if idx < len(exec_results) else []
        if isinstance(payload, list):
            r.fetchall.return_value = payload
            r.fetchone.return_value = payload[0] if payload else None
        elif payload is None:
            r.fetchall.return_value = []
            r.fetchone.return_value = None
        else:
            r.fetchall.return_value = [payload]
            r.fetchone.return_value = payload
        r.scalar.return_value = payload[0] if (isinstance(payload, tuple) and len(payload) > 0) else None
        call_count[0] += 1
        return r

    conn.execute = execute
    return tracker


# ── record_importance horizon_days param ──────────────────────────────────


class TestRecordImportanceHorizon:
    def test_horizon_kwarg_passed_through(self):
        """Verify record_importance accepts horizon_days and stores it."""
        tracker = _build_tracker([
            (1,),  # _resolve_feature_id returns feature_id=1
            None,  # INSERT returns nothing
        ])

        captured = []
        original_execute = tracker.engine.begin.return_value.execute

        def capture(query, params=None):
            sql = str(query)
            if "INSERT INTO feature_importance_log" in sql:
                captured.append(params)
            return original_execute(query, params) if params else original_execute(query)

        # Patch execute to capture the insert
        tracker.engine.begin.return_value.execute = capture

        # Bypass the static method by patching at the class level
        FeatureImportanceTracker._resolve_feature_id = staticmethod(lambda conn, name: 1)
        tracker.record_importance(
            model_version_id=42,
            feature_importances={"test_feature": 0.75},
            as_of_date=date(2026, 4, 13),
            method="permutation",
            horizon_days=7,
        )
        # Captured the INSERT params
        assert len(captured) == 1
        assert captured[0]["h"] == 7
        assert captured[0]["score"] == 0.75
        assert captured[0]["method"] == "permutation"

    def test_horizon_none_preserved(self):
        tracker = _build_tracker([(1,)])
        captured = []
        tracker.engine.begin.return_value.execute = lambda query, params=None: (
            captured.append(params) if params and "INSERT" in str(query) else None
        ) or MagicMock()
        # Bypass the static method by patching at the class level
        FeatureImportanceTracker._resolve_feature_id = staticmethod(lambda conn, name: 1)
        tracker.record_importance(
            model_version_id=42,
            feature_importances={"legacy_feature": 0.5},
            as_of_date=date(2026, 4, 13),
            # horizon_days not passed → None
        )
        assert len(captured) == 1
        assert captured[0]["h"] is None


# ── get_rankings_by_horizon ───────────────────────────────────────────────


class TestGetRankingsByHorizon:
    def test_returns_sorted_dataframe(self):
        tracker = _build_tracker([
            [
                ("feature_a", 0.85, 10),
                ("feature_b", 0.72, 8),
                ("feature_c", 0.33, 12),
            ],
        ])
        df = tracker.get_rankings_by_horizon(horizon_days=7, days_back=30)
        assert len(df) == 3
        assert list(df.columns) == ["feature_name", "avg_score", "n_samples"]
        assert df.iloc[0]["feature_name"] == "feature_a"

    def test_top_n_truncation(self):
        tracker = _build_tracker([
            [
                ("a", 0.9, 10),
                ("b", 0.8, 10),
                ("c", 0.7, 10),
                ("d", 0.6, 10),
            ],
        ])
        df = tracker.get_rankings_by_horizon(horizon_days=30, days_back=30, top_n=2)
        assert len(df) == 2
        assert df.iloc[0]["feature_name"] == "a"
        assert df.iloc[1]["feature_name"] == "b"

    def test_empty_returns_empty_dataframe(self):
        tracker = _build_tracker([[]])
        df = tracker.get_rankings_by_horizon(horizon_days=90)
        assert df.empty
        assert list(df.columns) == ["feature_name", "avg_score", "n_samples"]


# ── get_horizon_profile ───────────────────────────────────────────────────


class TestGetHorizonProfile:
    def test_missing_feature_returns_empty(self):
        tracker = _build_tracker([])
        tracker._feature_id_for_name = MagicMock(return_value=None)
        profile = tracker.get_horizon_profile("nonexistent")
        assert profile == {}

    def test_returns_per_horizon_avgs(self):
        tracker = _build_tracker([
            (0.42,),  # horizon=1
            (0.51,),  # horizon=7
            (0.67,),  # horizon=30
            (0.71,),  # horizon=90
        ])
        tracker._feature_id_for_name = MagicMock(return_value=42)
        profile = tracker.get_horizon_profile("test_feature")
        assert set(profile.keys()) == {1, 7, 30, 90}
        assert profile[1] == 0.42
        assert profile[90] == 0.71

    def test_missing_horizon_buckets_omitted(self):
        tracker = _build_tracker([
            (0.42,),
            (None,),   # horizon=7 has no data
            (0.67,),
            (None,),   # horizon=90 has no data
        ])
        tracker._feature_id_for_name = MagicMock(return_value=42)
        profile = tracker.get_horizon_profile("test_feature")
        assert set(profile.keys()) == {1, 30}

    def test_custom_horizons_list(self):
        tracker = _build_tracker([
            (0.5,),
            (0.6,),
        ])
        tracker._feature_id_for_name = MagicMock(return_value=42)
        profile = tracker.get_horizon_profile("test_feature", horizons=[14, 60])
        assert set(profile.keys()) == {14, 60}
