"""Tests for ``intelligence.confidence_calibration``.

The pure helpers (``_bucket_for``) are tested directly. The DB-touching
paths (``calibrate_confidence``, ``build_reliability_curves``) use a
minimal FakeEngine that stores rows in a dict so the full lookup loop
is exercised without postgres.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from intelligence.confidence_calibration import (
    BUCKET_BOUNDARIES,
    MIN_PREDICTIONS_PER_BUCKET,
    MIN_PREDICTIONS_TOTAL,
    _bucket_for,
    build_reliability_curves,
    calibrate_confidence,
    invalidate_cache,
)


# ── Pure helper: _bucket_for ─────────────────────────────────────────────


class TestBucketFor:
    def test_lower_edge(self):
        assert _bucket_for(0.0) == (0.0, 0.10)

    def test_mid_buckets(self):
        assert _bucket_for(0.25) == (0.20, 0.30)
        assert _bucket_for(0.55) == (0.50, 0.60)
        assert _bucket_for(0.95) == (0.90, 1.0001)

    def test_upper_edge(self):
        # raw=1.0 lands in the top bucket
        lower, upper = _bucket_for(1.0)
        assert lower == 0.90
        assert upper == 1.0001

    def test_out_of_range_clamped(self):
        # Negative → bottom bucket
        assert _bucket_for(-0.5) == (0.0, 0.10)
        # >1 → top bucket
        assert _bucket_for(1.5) == (0.90, 1.0001)

    def test_bucket_boundaries_are_inclusive_lower(self):
        # 0.5 should land in [0.50, 0.60), not [0.40, 0.50)
        assert _bucket_for(0.50) == (0.50, 0.60)


# ── Fake engine for calibrate_confidence + build_reliability_curves ──────


class _FakeConn:
    def __init__(self, engine: "_FakeEngine") -> None:
        self.engine = engine

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> Any:
        text_sql = str(sql).strip()
        params = params or {}

        if text_sql.startswith("CREATE TABLE") or text_sql.startswith("CREATE INDEX"):
            return MagicMock()

        if text_sql.startswith("TRUNCATE"):
            self.engine.curves.clear()
            return MagicMock()

        if "FROM confidence_reliability_curves" in text_sql:
            model = params["m"]
            rows = [
                (lower, n, hits, rate)
                for (m, lower), (n, hits, rate) in self.engine.curves.items()
                if m == model
            ]
            return _Result(rows)

        if "FROM oracle_predictions" in text_sql:
            # Returns the fake predictions verbatim
            return _Result(self.engine.predictions)

        if text_sql.startswith("INSERT INTO confidence_reliability_curves"):
            key = (params["m"], params["lo"])
            self.engine.curves[key] = (
                int(params["n"]),
                int(params["h"]),
                float(params["r"]),
            )
            return MagicMock()

        return MagicMock()


class _Result:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = list(rows)

    def fetchall(self) -> list[Any]:
        return list(self._rows)


class _FakeEngine:
    def __init__(
        self,
        curves: dict[tuple[str, float], tuple[int, int, float]] | None = None,
        predictions: list[tuple[str, float, int]] | None = None,
    ) -> None:
        self.curves = dict(curves or {})
        # Predictions: (model_name, confidence, hit_flag)
        self.predictions = list(predictions or [])

    def connect(self) -> _FakeConn:
        return _FakeConn(self)

    def begin(self) -> _FakeConn:
        return _FakeConn(self)


# ── calibrate_confidence ─────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _clear_cache():
    invalidate_cache()
    yield
    invalidate_cache()


class TestCalibrateConfidence:
    def test_returns_raw_when_model_is_none(self):
        engine = _FakeEngine()
        assert calibrate_confidence(0.95, None, engine) == 0.95
        assert calibrate_confidence(0.95, "", engine) == 0.95

    def test_returns_raw_when_no_curve_exists(self):
        engine = _FakeEngine(curves={})
        assert calibrate_confidence(0.95, "unknown_model", engine) == 0.95

    def test_returns_raw_when_total_below_min(self):
        # Total = 10 (below MIN_PREDICTIONS_TOTAL=30)
        engine = _FakeEngine(curves={
            ("thin_model", 0.90): (10, 1, 0.10),
        })
        assert calibrate_confidence(0.95, "thin_model", engine) == 0.95

    def test_returns_calibrated_rate_when_bucket_has_data(self):
        # Model has 100 total predictions; bucket [0.90, 1.0001) has
        # 50 predictions, 5 hits → calibrated rate = 0.10
        curves: dict[tuple[str, float], tuple[int, int, float]] = {
            ("options_flow_mut_g34098", 0.90): (50, 5, 0.10),
            ("options_flow_mut_g34098", 0.50): (50, 25, 0.50),
        }
        engine = _FakeEngine(curves=curves)
        # Raw 0.95 lands in [0.90, 1.0] → calibrated to 0.10
        assert calibrate_confidence(0.95, "options_flow_mut_g34098", engine) == pytest.approx(0.10)
        # Raw 0.55 lands in [0.50, 0.60) → calibrated to 0.50
        assert calibrate_confidence(0.55, "options_flow_mut_g34098", engine) == pytest.approx(0.50)

    def test_returns_raw_when_bucket_thin(self):
        # 100 total predictions but the queried bucket has only 2
        curves: dict[tuple[str, float], tuple[int, int, float]] = {
            ("model_x", 0.20): (98, 49, 0.50),
            ("model_x", 0.90): (2, 1, 0.50),  # < MIN_PREDICTIONS_PER_BUCKET
        }
        engine = _FakeEngine(curves=curves)
        # Raw 0.95 → bucket has only 2 preds → return raw unchanged
        assert calibrate_confidence(0.95, "model_x", engine) == 0.95
        # Raw 0.25 → well-populated bucket → calibrated to 0.50
        assert calibrate_confidence(0.25, "model_x", engine) == pytest.approx(0.50)

    def test_clamped_to_valid_range(self):
        # Pathological: bucket rate stored as 1.5 (corrupt data)
        curves: dict[tuple[str, float], tuple[int, int, float]] = {
            ("evil", 0.50): (100, 150, 1.5),  # impossible but defensive
        }
        engine = _FakeEngine(curves=curves)
        assert calibrate_confidence(0.55, "evil", engine) == 1.0

    def test_caches_curve_per_model(self):
        # Calling twice for the same model should hit the cache, not
        # re-read from the engine. Verify by mutating the engine
        # between calls and confirming the second call returns the
        # cached (old) data.
        engine = _FakeEngine(curves={
            ("cached", 0.50): (100, 50, 0.50),
        })
        first = calibrate_confidence(0.55, "cached", engine)
        # Mutate the underlying curves to a new value
        engine.curves[("cached", 0.50)] = (100, 0, 0.0)
        second = calibrate_confidence(0.55, "cached", engine)
        # Cache should serve the original value
        assert first == pytest.approx(0.50)
        assert second == pytest.approx(0.50)

        # After invalidate, the new value is picked up
        invalidate_cache()
        third = calibrate_confidence(0.55, "cached", engine)
        assert third == pytest.approx(0.0)


# ── build_reliability_curves ─────────────────────────────────────────────


class TestBuildReliabilityCurves:
    def test_skips_models_below_min_total(self):
        engine = _FakeEngine(predictions=[
            ("thin", 0.5, 0),
            ("thin", 0.5, 1),
            # Only 2 total — below MIN_PREDICTIONS_TOTAL=30
        ])
        summary = build_reliability_curves(engine, min_total=30)
        assert summary["models_processed"] == 0
        assert summary["models_skipped_low_n"] == 1
        assert engine.curves == {}

    def test_writes_one_row_per_populated_bucket(self):
        # 40 predictions all in the same bucket [0.50, 0.60)
        engine = _FakeEngine(predictions=[
            ("m", 0.55, i % 2)  # 50% hit rate
            for i in range(40)
        ])
        summary = build_reliability_curves(engine, min_total=30)
        assert summary["models_processed"] == 1
        assert summary["rows_written"] == 1
        # Bucket lower 0.50 should be populated
        assert ("m", 0.50) in engine.curves
        n, hits, rate = engine.curves[("m", 0.50)]
        assert n == 40
        assert hits == 20
        assert rate == pytest.approx(0.50)

    def test_separate_buckets_get_separate_rows(self):
        # 20 in [0.10, 0.20) all miss; 20 in [0.80, 0.90) all hit
        preds = [("m", 0.15, 0) for _ in range(20)]
        preds.extend([("m", 0.85, 1) for _ in range(20)])
        engine = _FakeEngine(predictions=preds)
        summary = build_reliability_curves(engine, min_total=30)
        assert summary["models_processed"] == 1
        assert summary["rows_written"] == 2
        assert engine.curves[("m", 0.10)][2] == pytest.approx(0.0)
        assert engine.curves[("m", 0.80)][2] == pytest.approx(1.0)

    def test_truncate_clears_prior_curves(self):
        engine = _FakeEngine(
            curves={("old_model", 0.50): (100, 50, 0.50)},
            predictions=[("new_model", 0.55, 1) for _ in range(40)],
        )
        build_reliability_curves(engine, min_total=30)
        # The old curve is gone (TRUNCATE cleared the table)
        assert ("old_model", 0.50) not in engine.curves
        assert ("new_model", 0.50) in engine.curves
