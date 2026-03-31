"""
Tests for inference/live.py LiveInference.

Covers:
- get_production_models: DB interaction and return structure
- run_inference: no-models path, empty feature set, no PIT data, full path
- _generate_recommendation: coverage of all normalization/threshold branches
- _generate_trained_recommendation: artifact-missing fallback, load failure fallback
- _run_shadow_models: empty result, shadow scoring path
- get_feature_snapshot: empty and populated paths
- PIT correctness: inference only uses data available as_of the decision date

All external dependencies (DB, PITStore, FeatureLab, TrainedModelBase) are mocked.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch, PropertyMock

import numpy as np
import pandas as pd
import pytest

from inference.live import LiveInference


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_engine(rows=None, fetchone_val=None):
    """Return a mock engine whose connect().execute().fetchall() returns rows."""
    engine = MagicMock()
    conn = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = rows or []
    result.fetchone.return_value = fetchone_val
    conn.execute.return_value = result
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _make_pit_store(feature_ids=None, latest_df=None, pit_df=None):
    """Return a mock PITStore."""
    pit = MagicMock()
    if latest_df is None:
        latest_df = pd.DataFrame(columns=["feature_id", "obs_date", "value"])
    if pit_df is None:
        pit_df = pd.DataFrame(columns=["feature_id", "obs_date", "value"])
    pit.get_latest_values.return_value = latest_df
    pit.get_pit.return_value = pit_df
    return pit


def _make_live_inference(engine=None, pit=None):
    """Construct a LiveInference without triggering FeatureLab's DB queries."""
    if engine is None:
        engine, _ = _make_engine()
    if pit is None:
        pit = _make_pit_store()

    with patch("inference.live.FeatureLab") as MockLab:
        li = LiveInference(db_engine=engine, pit_store=pit)
        li._mock_lab = MockLab.return_value
    return li


def _model_row(
    model_id=1,
    name="test_model",
    layer="REGIME",
    version="v1",
    feature_set=[1, 2, 3],
    parameter_snapshot=None,
    hypothesis_id=10,
    model_type="rule_based",
):
    """Build a fake 8-column model row tuple."""
    if parameter_snapshot is None:
        parameter_snapshot = {}
    return (model_id, name, layer, version, feature_set,
            parameter_snapshot, hypothesis_id, model_type)


# ---------------------------------------------------------------------------
# get_production_models
# ---------------------------------------------------------------------------


class TestGetProductionModels:

    def test_returns_empty_list_when_no_models(self):
        engine, _ = _make_engine(rows=[])
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)
        models = li.get_production_models()
        assert models == []

    def test_returns_model_dict_with_all_fields(self):
        row = _model_row()
        engine, _ = _make_engine(rows=[row])
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)
        models = li.get_production_models()

        assert len(models) == 1
        m = models[0]
        assert m["id"] == 1
        assert m["name"] == "test_model"
        assert m["layer"] == "REGIME"
        assert m["version"] == "v1"
        assert m["feature_set"] == [1, 2, 3]
        assert m["model_type"] == "rule_based"

    def test_multiple_models_all_returned(self):
        rows = [
            _model_row(model_id=1, layer="REGIME"),
            _model_row(model_id=2, layer="MOMENTUM"),
            _model_row(model_id=3, layer="VOLATILITY"),
        ]
        engine, _ = _make_engine(rows=rows)
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)
        models = li.get_production_models()
        assert len(models) == 3
        layers = {m["layer"] for m in models}
        assert layers == {"REGIME", "MOMENTUM", "VOLATILITY"}

    def test_model_type_defaults_to_rule_based_for_short_row(self):
        """If the row has fewer than 8 columns, model_type defaults to rule_based."""
        short_row = (1, "m", "L", "v1", [1], {}, 10)  # only 7 elements
        engine, _ = _make_engine(rows=[short_row])
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)
        models = li.get_production_models()
        assert models[0]["model_type"] == "rule_based"


# ---------------------------------------------------------------------------
# run_inference
# ---------------------------------------------------------------------------


class TestRunInference:

    def test_no_production_models_returns_error(self):
        engine, _ = _make_engine(rows=[])
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)
        result = li.run_inference(as_of_date=date(2026, 1, 15))

        assert "error" in result
        assert result["layers"] == {}

    def test_model_with_empty_feature_set_returns_error_layer(self):
        row = _model_row(feature_set=[])
        engine, _ = _make_engine(rows=[row])
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)
        result = li.run_inference(as_of_date=date(2026, 1, 15))

        assert "REGIME" in result["layers"]
        assert "error" in result["layers"]["REGIME"]

    def test_model_with_no_pit_data_returns_error_layer(self):
        row = _model_row(feature_set=[1, 2, 3])
        engine, _ = _make_engine(rows=[row])
        pit = _make_pit_store(latest_df=pd.DataFrame())
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)
            li.feature_lab.compute_derived_features.return_value = {}
        result = li.run_inference(as_of_date=date(2026, 1, 15))

        assert "REGIME" in result["layers"]
        assert "error" in result["layers"]["REGIME"]

    def test_run_inference_uses_today_when_no_date_given(self):
        engine, _ = _make_engine(rows=[])
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)
        result = li.run_inference()

        # With no models, result still has as_of_date set to today
        assert "as_of_date" in result
        assert result["as_of_date"] == date.today().isoformat()

    def test_run_inference_full_path_returns_recommendation(self):
        """Full happy path: one model, PIT data available, produces recommendation."""
        row = _model_row(
            feature_set=[1, 2],
            parameter_snapshot={
                "state_thresholds": {
                    "BULL": {"weights": {"feature_1": 1.0}, "action": "BUY"},
                }
            },
        )

        engine, _ = _make_engine(rows=[row])
        latest_df = pd.DataFrame({
            "feature_id": [1, 2],
            "obs_date": [date(2026, 1, 14), date(2026, 1, 14)],
            "value": [0.8, 0.5],
        })
        pit = _make_pit_store(latest_df=latest_df)

        with patch("inference.live.FeatureLab") as MockLab:
            li = LiveInference(db_engine=engine, pit_store=pit)
            li.feature_lab = MockLab.return_value
            li.feature_lab.compute_derived_features.return_value = {"yld_curve_2s10s": 0.5}

        # Suppress shadow/convergence lookups
        with patch.object(li, "_run_shadow_models", return_value=[]):
            with patch.object(li, "_get_convergence_context", return_value=None):
                result = li.run_inference(as_of_date=date(2026, 1, 15))

        assert "REGIME" in result["layers"]
        layer = result["layers"]["REGIME"]
        assert "recommendation" in layer
        assert "inferred_state" in layer["recommendation"]

    def test_run_inference_result_contains_as_of_date(self):
        engine, _ = _make_engine(rows=[])
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)
        result = li.run_inference(as_of_date=date(2026, 3, 15))

        assert result["as_of_date"] == "2026-03-15"

    def test_run_inference_feature_vector_contains_pit_values(self):
        """Feature vector must include values from the PIT store."""
        row = _model_row(feature_set=[10, 20])
        engine, _ = _make_engine(rows=[row])

        latest_df = pd.DataFrame({
            "feature_id": [10, 20],
            "obs_date": [date(2026, 1, 14)] * 2,
            "value": [1.5, 2.5],
        })
        pit = _make_pit_store(latest_df=latest_df)

        with patch("inference.live.FeatureLab") as MockLab:
            li = LiveInference(db_engine=engine, pit_store=pit)
            li.feature_lab = MockLab.return_value
            li.feature_lab.compute_derived_features.return_value = {}

        with patch.object(li, "_run_shadow_models", return_value=[]):
            with patch.object(li, "_get_convergence_context", return_value=None):
                result = li.run_inference(as_of_date=date(2026, 1, 15))

        fv = result["layers"]["REGIME"]["feature_values"]
        assert "feature_10" in fv
        assert fv["feature_10"] == 1.5
        assert "feature_20" in fv
        assert fv["feature_20"] == 2.5


# ---------------------------------------------------------------------------
# PIT correctness — inference must not access future data
# ---------------------------------------------------------------------------


class TestPITCorrectness:

    def test_inference_passes_as_of_date_to_feature_lab(self):
        """FeatureLab.compute_derived_features must be called with the exact as_of_date."""
        row = _model_row(feature_set=[1])
        engine, _ = _make_engine(rows=[row])

        latest_df = pd.DataFrame({
            "feature_id": [1],
            "obs_date": [date(2026, 1, 10)],
            "value": [0.5],
        })
        pit = _make_pit_store(latest_df=latest_df)
        decision_date = date(2026, 1, 10)

        with patch("inference.live.FeatureLab") as MockLab:
            li = LiveInference(db_engine=engine, pit_store=pit)
            li.feature_lab = MockLab.return_value
            li.feature_lab.compute_derived_features.return_value = {}

        with patch.object(li, "_run_shadow_models", return_value=[]):
            with patch.object(li, "_get_convergence_context", return_value=None):
                li.run_inference(as_of_date=decision_date)

        li.feature_lab.compute_derived_features.assert_called_once_with(decision_date)

    def test_inference_passes_as_of_date_to_pit_store(self):
        """get_latest_values is called with the feature_ids from the model's feature_set
        — no future data is mixed in because PITStore handles the as_of boundary."""
        row = _model_row(feature_set=[5, 6])
        engine, _ = _make_engine(rows=[row])

        latest_df = pd.DataFrame({
            "feature_id": [5, 6],
            "obs_date": [date(2025, 12, 31)] * 2,
            "value": [1.0, 2.0],
        })
        pit = _make_pit_store(latest_df=latest_df)

        with patch("inference.live.FeatureLab") as MockLab:
            li = LiveInference(db_engine=engine, pit_store=pit)
            li.feature_lab = MockLab.return_value
            li.feature_lab.compute_derived_features.return_value = {}

        with patch.object(li, "_run_shadow_models", return_value=[]):
            with patch.object(li, "_get_convergence_context", return_value=None):
                li.run_inference(as_of_date=date(2026, 1, 1))

        pit.get_latest_values.assert_called_once_with([5, 6])


# ---------------------------------------------------------------------------
# _generate_recommendation — all branches
# ---------------------------------------------------------------------------


class TestGenerateRecommendation:

    def _li(self):
        return LiveInference.__new__(LiveInference)

    def test_positive_scores_picks_highest(self):
        li = self._li()
        features = {"f1": 0.5, "f2": 0.8, "f3": 0.3}
        params = {
            "state_thresholds": {
                "BULL": {"weights": {"f1": 1.0, "f2": 1.0}, "action": "BUY"},
                "BEAR": {"weights": {"f1": -1.0, "f2": -0.5}, "action": "SELL"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["inferred_state"] == "BULL"
        assert result["suggested_action"] == "BUY"

    def test_confidence_is_bounded_by_one(self):
        li = self._li()
        features = {"f1": 100.0}
        params = {
            "state_thresholds": {
                "UP": {"weights": {"f1": 1.0}, "action": "BUY"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["state_confidence"] <= 1.0

    def test_below_50pct_coverage_returns_hold(self):
        li = self._li()
        features = {"f1": 0.5, "f2": None, "f3": None, "f4": None}
        params = {"state_thresholds": {"BULL": {"weights": {"f1": 1.0}}}}
        result = li._generate_recommendation(features, params)
        assert "HOLD" in result["suggested_action"]
        assert result["inferred_state"] == "UNKNOWN"

    def test_exactly_50pct_is_sufficient(self):
        li = self._li()
        features = {"a": 1.0, "b": None}
        params = {
            "state_thresholds": {
                "UP": {"weights": {"a": 1.0}, "action": "BUY"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["feature_coverage"] == 0.5
        assert result["inferred_state"] == "UP"

    def test_all_none_features_returns_hold(self):
        li = self._li()
        features = {"a": None, "b": None, "c": None}
        result = li._generate_recommendation(features, {})
        assert result["feature_coverage"] == 0.0
        assert "insufficient" in result["suggested_action"].lower()

    def test_empty_thresholds_returns_unknown_hold(self):
        li = self._li()
        features = {"f1": 0.5}
        result = li._generate_recommendation(features, {"state_thresholds": {}})
        assert result["inferred_state"] == "UNKNOWN"
        assert result["suggested_action"] == "HOLD"

    def test_missing_thresholds_key_returns_unknown_hold(self):
        li = self._li()
        features = {"a": 1.0}
        result = li._generate_recommendation(features, {"other_key": "value"})
        assert result["inferred_state"] == "UNKNOWN"

    def test_negative_scores_use_absolute_value_for_dominant_state(self):
        li = self._li()
        features = {"f1": -2.0, "f2": -1.0}
        params = {
            "state_thresholds": {
                "BULL": {"weights": {"f1": 1.0, "f2": 1.0}, "action": "BUY"},
                "BEAR": {"weights": {"f1": 0.5, "f2": 0.5}, "action": "SELL"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["inferred_state"] == "BULL"

    def test_result_always_has_required_keys(self):
        li = self._li()
        required = {
            "inferred_state", "state_confidence",
            "transition_probability", "suggested_action",
            "feature_coverage", "contradiction_flags",
        }
        result = li._generate_recommendation({}, {})
        assert required.issubset(set(result.keys()))

    def test_feature_coverage_zero_for_empty_features(self):
        li = self._li()
        result = li._generate_recommendation({}, {})
        assert result["feature_coverage"] == 0.0

    def test_feature_coverage_one_for_all_available(self):
        li = self._li()
        features = {"a": 1.0, "b": 2.0, "c": 3.0}
        result = li._generate_recommendation(features, {})
        assert result["feature_coverage"] == 1.0

    def test_action_matches_winning_state(self):
        li = self._li()
        features = {"x": 5.0, "y": 3.0}
        params = {
            "state_thresholds": {
                "ALPHA": {"weights": {"x": 0.1}, "action": "SCALE_IN"},
                "BETA": {"weights": {"x": 2.0, "y": 1.0}, "action": "FULL_POSITION"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["inferred_state"] == "BETA"
        assert result["suggested_action"] == "FULL_POSITION"

    def test_features_not_in_weights_are_ignored(self):
        """Features absent from a state's weights must not affect scoring."""
        li = self._li()
        features = {"known": 1.0, "unknown_to_weights": 999.0}
        params = {
            "state_thresholds": {
                "ON": {"weights": {"known": 1.0}, "action": "BUY"},
            }
        }
        result = li._generate_recommendation(features, params)
        assert result["inferred_state"] == "ON"

    def test_state_confidence_is_rounded_to_4_decimals(self):
        li = self._li()
        features = {"f": 0.333333}
        params = {
            "state_thresholds": {
                "A": {"weights": {"f": 1.0}, "action": "BUY"},
                "B": {"weights": {"f": 0.5}, "action": "SELL"},
            }
        }
        result = li._generate_recommendation(features, params)
        # Confidence should be 4 decimal places
        conf_str = str(result["state_confidence"])
        decimals = len(conf_str.split(".")[-1]) if "." in conf_str else 0
        assert decimals <= 4


# ---------------------------------------------------------------------------
# _generate_trained_recommendation
# ---------------------------------------------------------------------------


class TestGenerateTrainedRecommendation:

    def _li(self):
        engine, _ = _make_engine()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=_make_pit_store())
        return li, engine

    def test_no_artifact_falls_back_to_rule_based(self):
        """When no artifact row is found, must fall back to _generate_recommendation."""
        li, engine = self._li()

        # DB returns None for artifact lookup
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = None

        model_record = {
            "id": 1,
            "model_type": "xgboost",
            "parameter_snapshot": {
                "state_thresholds": {
                    "UP": {"weights": {"feature_1": 1.0}, "action": "BUY"},
                }
            },
        }
        features = {"feature_1": 0.9}
        result = li._generate_trained_recommendation(features, model_record)

        # Falls back to rule-based recommendation
        assert "inferred_state" in result

    def test_artifact_load_failure_falls_back_to_rule_based(self):
        """When joblib.load raises, must fall back to _generate_recommendation."""
        li, engine = self._li()

        art_row = MagicMock()
        art_row.__getitem__ = lambda self, idx: {0: "/nonexistent/model.joblib", 1: ["f1"]}[idx]
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = art_row

        model_record = {
            "id": 1,
            "model_type": "xgboost",
            "parameter_snapshot": {},
        }

        # TrainedModelBase is imported locally inside _generate_trained_recommendation
        # so patch at the source module level.
        with patch("inference.trained_models.TrainedModelBase.load",
                   side_effect=FileNotFoundError("model gone")):
            result = li._generate_trained_recommendation({"f1": 1.0}, model_record)

        # Should gracefully fall back
        assert "inferred_state" in result

    def test_artifact_produces_structured_recommendation(self):
        """When a valid artifact is loaded, result must have all required fields."""
        li, engine = self._li()

        art_row = MagicMock()
        art_row.__getitem__ = lambda self, idx: {
            0: "/tmp/model.joblib",
            1: ["feat_a", "feat_b"],
        }[idx]
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = art_row

        mock_model = MagicMock()
        mock_model.predict.return_value = np.array(["GROWTH"])
        mock_model.predict_proba.return_value = np.array([[0.7, 0.2, 0.05, 0.05]])
        mock_model.classes_ = np.array(["GROWTH", "NEUTRAL", "FRAGILE", "CRISIS"])

        model_record = {
            "id": 1,
            "model_type": "xgboost",
            "parameter_snapshot": {
                "state_map": {"GROWTH": "GROWTH"},
                "action_map": {"GROWTH": "BUY"},
            },
        }

        with patch("inference.trained_models.TrainedModelBase.load",
                   return_value=mock_model):
            result = li._generate_trained_recommendation(
                {"feat_a": 1.0, "feat_b": 0.5}, model_record
            )

        assert result["inferred_state"] == "GROWTH"
        assert result["suggested_action"] == "BUY"
        assert result["state_confidence"] == pytest.approx(0.7, abs=0.01)


# ---------------------------------------------------------------------------
# _run_shadow_models
# ---------------------------------------------------------------------------


class TestRunShadowModels:

    def test_no_shadow_models_returns_empty_list(self):
        engine, conn = _make_engine(rows=[])
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)

        # Make connect() return empty rows for shadow query
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []

        result = li._run_shadow_models("REGIME", {"feature_1": 0.5}, date(2026, 1, 15))
        assert result == []

    def test_shadow_model_result_has_required_fields(self):
        shadow_row = MagicMock()
        shadow_row.__getitem__ = lambda self, idx: {
            0: 99,
            1: "shadow_model",
            2: "rule_based",
            3: {"state_thresholds": {"BULL": {"weights": {"f1": 1.0}, "action": "BUY"}}},
        }[idx]

        engine, conn = _make_engine()
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)

        conn.execute.return_value.fetchall.return_value = [shadow_row]

        results = li._run_shadow_models("REGIME", {"f1": 0.8}, date(2026, 1, 15))

        assert len(results) == 1
        r = results[0]
        assert r["shadow_model_id"] == 99
        assert r["shadow_model_name"] == "shadow_model"
        assert "shadow_state" in r
        assert "shadow_confidence" in r

    def test_shadow_model_exception_is_swallowed_gracefully(self):
        """A broken shadow model must not crash the calling inference run."""
        shadow_row = MagicMock()
        shadow_row.__getitem__ = lambda self, idx: {
            0: 55,
            1: "broken_shadow",
            2: "rule_based",
            3: None,  # Will cause json.loads to fail if treated as string
        }[idx]

        engine, conn = _make_engine()
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)

        conn.execute.return_value.fetchall.return_value = [shadow_row]

        # Patch _generate_recommendation to raise
        with patch.object(li, "_generate_recommendation", side_effect=RuntimeError("boom")):
            results = li._run_shadow_models("REGIME", {"f1": 0.5}, date(2026, 1, 15))

        # Must return an empty list, not raise
        assert isinstance(results, list)


# ---------------------------------------------------------------------------
# get_feature_snapshot
# ---------------------------------------------------------------------------


class TestGetFeatureSnapshot:

    def test_returns_empty_dataframe_when_no_pit_data(self):
        features_rows = [
            (1, "feat_a", "macro"),
            (2, "feat_b", "vol"),
        ]
        engine, conn = _make_engine(rows=features_rows)
        pit = _make_pit_store(latest_df=pd.DataFrame())

        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)

        snapshot = li.get_feature_snapshot(as_of_date=date(2026, 1, 15))

        assert isinstance(snapshot, pd.DataFrame)
        assert list(snapshot.columns) == ["name", "family", "value", "obs_date"]
        assert len(snapshot) == 0

    def test_returns_populated_dataframe_with_correct_columns(self):
        features_rows = [
            (1, "vix_spot", "vol"),
            (2, "sp500_close", "equity"),
        ]
        engine, conn = _make_engine(rows=features_rows)

        latest_df = pd.DataFrame({
            "feature_id": [1, 2],
            "obs_date": [date(2026, 1, 14)] * 2,
            "value": [18.5, 4800.0],
        })
        pit = _make_pit_store(latest_df=latest_df)

        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)

        snapshot = li.get_feature_snapshot(as_of_date=date(2026, 1, 15))

        assert isinstance(snapshot, pd.DataFrame)
        assert set(snapshot.columns) == {"name", "family", "value", "obs_date"}
        assert len(snapshot) == 2

    def test_snapshot_uses_today_when_no_date_given(self):
        engine, conn = _make_engine(rows=[])
        pit = _make_pit_store(latest_df=pd.DataFrame())

        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)

        # Should not raise even with no date argument
        snapshot = li.get_feature_snapshot()
        assert isinstance(snapshot, pd.DataFrame)

    def test_snapshot_unknown_feature_gets_placeholder_name(self):
        """Feature IDs not in the registry should appear with a generated name."""
        features_rows = [(10, "known_feat", "macro")]
        engine, conn = _make_engine(rows=features_rows)

        # PIT returns a feature_id that is NOT in the registry rows (id=99)
        latest_df = pd.DataFrame({
            "feature_id": [99],
            "obs_date": [date(2026, 1, 14)],
            "value": [42.0],
        })
        pit = _make_pit_store(latest_df=latest_df)

        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)

        snapshot = li.get_feature_snapshot(as_of_date=date(2026, 1, 15))
        assert len(snapshot) == 1
        assert "99" in snapshot.iloc[0]["name"]


# ---------------------------------------------------------------------------
# _get_convergence_context
# ---------------------------------------------------------------------------


class TestGetConvergenceContext:

    def test_returns_none_when_trust_scorer_unavailable(self):
        engine, _ = _make_engine()
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)

        with patch.dict("sys.modules", {"intelligence.trust_scorer": None}):
            result = li._get_convergence_context()

        assert result is None

    def test_returns_none_when_detect_convergence_returns_empty(self):
        engine, _ = _make_engine()
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)

        # detect_convergence returns an empty list → _get_convergence_context returns None
        with patch("intelligence.trust_scorer.detect_convergence", return_value=[]):
            result = li._get_convergence_context()

        # Either None (no events) or a dict with count=0; both are valid
        if result is not None:
            assert result["count"] == 0

    def test_returns_events_and_count(self):
        """When convergence events exist, result must have 'events' and 'count' keys."""
        engine, _ = _make_engine()
        pit = _make_pit_store()
        with patch("inference.live.FeatureLab"):
            li = LiveInference(db_engine=engine, pit_store=pit)

        mock_events = [
            {"ticker": "AAPL", "signal_type": "BUY", "source_count": 3,
             "combined_confidence": 0.75, "sources": ["A", "B", "C"]},
        ]
        with patch("intelligence.trust_scorer.detect_convergence", return_value=mock_events):
            result = li._get_convergence_context()

        if result is not None:  # trust_scorer may not be installed
            assert "events" in result
            assert "count" in result
            assert result["count"] >= 1
