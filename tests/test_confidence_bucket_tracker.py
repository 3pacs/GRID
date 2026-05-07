"""Tests for ``intelligence/confidence_bucket_tracker.py`` (CAT-180).

The pure helpers (``_bucket_for``, ``_compute_gap``, ``_multiplier_from_gap``)
are tested directly. The DB-touching paths use an in-memory FakeEngine
that stores bucket rows keyed on ``(horizon_days, bucket_low, bucket_high)``.
The FakeEngine services every SELECT/INSERT/UPDATE shape produced by the
module (ensure_schema, record write, bucket read, rank read, bootstrap
replay).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from intelligence.confidence_bucket_tracker import (
    MULT_HIGH_OVERCONF,
    MULT_MILD_OVERCONF,
    MULT_MILD_UNDERCONF,
    MULT_NEUTRAL,
    MULT_SEVERE_OVERCONF,
    MULT_STRONG_UNDERCONF,
    _bucket_for,
    _compute_gap,
    _ensure_schema,
    _multiplier_from_gap,
    _reset_initialized_engines,
    bootstrap_from_oracle_predictions,
    conviction_multiplier_for_bucket,
    get_bucket_calibration,
    rank_buckets_by_calibration,
    record_scored_prediction,
)
from features.per_signal_brier import MIN_CALIBRATED_SAMPLES


# ── Fake engine ───────────────────────────────────────────────────────────


class _FakeResult:
    def __init__(
        self,
        *,
        rows: list[tuple[Any, ...]] | None = None,
    ) -> None:
        self._rows = list(rows or [])

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _FakeCtx:
    def __init__(self, engine: "FakeEngine") -> None:
        self._engine = engine

    def __enter__(self) -> "FakeEngine":
        return self._engine

    def __exit__(self, *args: Any) -> bool:
        return False


class FakeEngine:
    """In-memory engine that stores bucket rows in a dict keyed on
    ``(horizon_days, bucket_low, bucket_high)``.

    Recognized SQL shapes (matched by substring):
      - CREATE TABLE / CREATE INDEX → no-op, DDL recorded
      - SELECT n_predictions, n_hits, running_brier ... WHERE horizon_days/bucket_low/bucket_high (record_scored_prediction)
      - INSERT INTO confidence_bucket_history ... ON CONFLICT DO NOTHING (new row)
      - UPDATE confidence_bucket_history (Welford update)
      - SELECT horizon_days, bucket_low, bucket_high, ... WHERE horizon_days AND bucket_low AND bucket_high (get_bucket_calibration)
      - SELECT ... WHERE horizon_days = :h AND n_predictions > 0 ORDER BY bucket_low ASC (rank)
      - SELECT confidence, verdict, created_at, expiry FROM oracle_predictions (bootstrap)
    """

    def __init__(
        self,
        *,
        oracle_rows: list[tuple[Any, ...]] | None = None,
        raise_on_execute: bool = False,
    ) -> None:
        self.store: dict[tuple[int, float, float], dict[str, Any]] = {}
        self.oracle_rows = oracle_rows or []
        self.raise_on_execute = raise_on_execute
        self.ddl_calls: int = 0

    def begin(self) -> _FakeCtx:
        return _FakeCtx(self)

    def connect(self) -> _FakeCtx:
        return _FakeCtx(self)

    # ── execute dispatcher ────────────────────────────────────────────
    def execute(self, stmt: Any, params: dict[str, Any] | None = None):
        if self.raise_on_execute:
            raise RuntimeError("fake DB outage")
        sql = str(stmt)
        params = params or {}

        if "CREATE TABLE" in sql or "CREATE INDEX" in sql:
            self.ddl_calls += 1
            return _FakeResult(rows=[])

        if "FROM oracle_predictions" in sql:
            return _FakeResult(rows=self.oracle_rows)

        if "FROM confidence_bucket_history" in sql and "SELECT" in sql.upper():
            h = int(params.get("h", 0))
            if "bucket_low" in sql and "bucket_high" in sql and "=" in sql and "n_predictions > 0" not in sql:
                bl = float(params.get("bl", 0.0))
                bh = float(params.get("bh", 0.0))
                row = self.store.get((h, bl, bh))
                if row is None:
                    return _FakeResult(rows=[])
                if "SELECT n_predictions" in sql:
                    # record_scored_prediction read shape
                    return _FakeResult(
                        rows=[(row["n"], row["hits"], row["brier"])]
                    )
                # get_bucket_calibration read shape
                return _FakeResult(
                    rows=[(
                        h, bl, bh, row["n"], row["hits"], row["brier"],
                        row["last_updated"],
                    )]
                )
            if "n_predictions > 0" in sql:
                out = []
                for (hh, bl, bh), row in self.store.items():
                    if hh == h and row["n"] > 0:
                        out.append((
                            hh, bl, bh, row["n"], row["hits"],
                            row["brier"], row["last_updated"],
                        ))
                out.sort(key=lambda r: r[1])
                return _FakeResult(rows=out)
            return _FakeResult(rows=[])

        if "INSERT INTO confidence_bucket_history" in sql:
            h = int(params["h"])
            bl = float(params["bl"])
            bh = float(params["bh"])
            key = (h, bl, bh)
            if key not in self.store:
                self.store[key] = {
                    "n": 1,
                    "hits": float(params["hits"]),
                    "brier": float(params["brier"]),
                    "last_updated": datetime.now(timezone.utc),
                }
            return _FakeResult(rows=[])

        if "UPDATE confidence_bucket_history" in sql:
            h = int(params["h"])
            bl = float(params["bl"])
            bh = float(params["bh"])
            key = (h, bl, bh)
            if key in self.store:
                self.store[key]["n"] = int(params["n"])
                self.store[key]["hits"] = float(params["hits"])
                self.store[key]["brier"] = float(params["brier"])
                self.store[key]["last_updated"] = datetime.now(timezone.utc)
            return _FakeResult(rows=[])

        return _FakeResult(rows=[])


@pytest.fixture(autouse=True)
def _reset_engine_cache():
    """Clear the per-engine DDL cache before every test so FakeEngine
    instances always re-run the schema path. Mirrors the pattern from
    test_per_signal_brier.py."""
    _reset_initialized_engines()
    yield
    _reset_initialized_engines()


# ── _bucket_for truth table ───────────────────────────────────────────────


class TestBucketFor:
    def test_bucket_050_inclusive(self):
        assert _bucket_for(0.50) == (0.50, 0.55)

    def test_bucket_055_rolls_up(self):
        assert _bucket_for(0.55) == (0.55, 0.60)

    def test_bucket_boundary_070_rolls_up(self):
        # Exactly on a boundary falls into the HIGHER bucket per the
        # [low, high) semantics documented in the module.
        assert _bucket_for(0.70) == (0.70, 0.75)

    def test_bucket_mid_range(self):
        assert _bucket_for(0.80) == (0.80, 0.85)
        assert _bucket_for(0.88) == (0.85, 0.90)

    def test_bucket_0549_stays_low(self):
        assert _bucket_for(0.549) == (0.50, 0.55)

    def test_bucket_0999_is_top(self):
        assert _bucket_for(0.999) == (0.95, 1.01)

    def test_bucket_10_is_top(self):
        assert _bucket_for(1.0) == (0.95, 1.01)

    def test_bucket_below_050_is_none(self):
        assert _bucket_for(0.49) is None
        assert _bucket_for(0.0) is None
        assert _bucket_for(-0.1) is None

    def test_bucket_nan_is_none(self):
        assert _bucket_for(float("nan")) is None

    def test_bucket_invalid_is_none(self):
        assert _bucket_for("not a number") is None  # type: ignore[arg-type]
        assert _bucket_for(None) is None  # type: ignore[arg-type]


# ── Gap → multiplier truth table ──────────────────────────────────────────


class TestMultiplierFromGap:
    def test_severe_overconf(self):
        # gap > 0.20
        assert _multiplier_from_gap(0.25) == MULT_SEVERE_OVERCONF
        assert _multiplier_from_gap(0.30) == MULT_SEVERE_OVERCONF

    def test_high_overconf(self):
        # 0.10 < gap <= 0.20
        assert _multiplier_from_gap(0.15) == MULT_HIGH_OVERCONF
        assert _multiplier_from_gap(0.20) == MULT_HIGH_OVERCONF  # not > 0.20

    def test_mild_overconf(self):
        # 0.05 < gap <= 0.10
        assert _multiplier_from_gap(0.08) == MULT_MILD_OVERCONF
        assert _multiplier_from_gap(0.10) == MULT_MILD_OVERCONF

    def test_neutral_zone(self):
        for g in (-0.05, -0.01, 0.0, 0.01, 0.05):
            assert _multiplier_from_gap(g) == MULT_NEUTRAL

    def test_mild_underconf(self):
        # -0.10 <= gap < -0.05
        assert _multiplier_from_gap(-0.08) == MULT_MILD_UNDERCONF
        assert _multiplier_from_gap(-0.10) == MULT_MILD_UNDERCONF

    def test_strong_underconf(self):
        assert _multiplier_from_gap(-0.15) == MULT_STRONG_UNDERCONF
        assert _multiplier_from_gap(-0.25) == MULT_STRONG_UNDERCONF


class TestComputeGap:
    def test_positive_when_overconfident(self):
        assert _compute_gap(0.80, 0.60) == pytest.approx(0.20)

    def test_negative_when_underconfident(self):
        assert _compute_gap(0.60, 0.80) == pytest.approx(-0.20)


# ── record_scored_prediction ──────────────────────────────────────────────


class TestRecordScoredPrediction:
    def test_first_record_inserts_row(self):
        engine = FakeEngine()
        record_scored_prediction(
            engine, confidence=0.80, outcome=1.0, horizon_days=7,
        )
        assert (7, 0.80, 0.85) in engine.store
        row = engine.store[(7, 0.80, 0.85)]
        assert row["n"] == 1
        assert row["hits"] == 1.0
        # First squared error: (0.80 - 1.0)^2 = 0.04
        assert row["brier"] == pytest.approx(0.04)

    def test_second_record_welford_update(self):
        engine = FakeEngine()
        record_scored_prediction(
            engine, confidence=0.80, outcome=1.0, horizon_days=7,
        )
        record_scored_prediction(
            engine, confidence=0.80, outcome=0.0, horizon_days=7,
        )
        row = engine.store[(7, 0.80, 0.85)]
        assert row["n"] == 2
        assert row["hits"] == 1.0
        # Welford: old 0.04, new sq_err 0.64, new_count 2
        # new = 0.04 + (0.64 - 0.04) / 2 = 0.04 + 0.30 = 0.34
        assert row["brier"] == pytest.approx(0.34)

    def test_partial_outcome_contributes_half(self):
        engine = FakeEngine()
        record_scored_prediction(
            engine, confidence=0.70, outcome=0.5, horizon_days=7,
        )
        row = engine.store[(7, 0.70, 0.75)]
        assert row["hits"] == 0.5

    def test_sub_50_confidence_no_record(self):
        engine = FakeEngine()
        record_scored_prediction(
            engine, confidence=0.30, outcome=1.0, horizon_days=7,
        )
        assert engine.store == {}

    def test_horizon_snapping(self):
        engine = FakeEngine()
        # 45 days should snap to 30
        record_scored_prediction(
            engine, confidence=0.80, outcome=1.0, horizon_days=45,
        )
        assert (30, 0.80, 0.85) in engine.store
        assert (45, 0.80, 0.85) not in engine.store

    def test_record_never_raises_on_db_failure(self):
        engine = FakeEngine(raise_on_execute=True)
        # Should not raise
        record_scored_prediction(
            engine, confidence=0.80, outcome=1.0, horizon_days=7,
        )

    def test_nan_confidence_noop(self):
        engine = FakeEngine()
        record_scored_prediction(
            engine, confidence=float("nan"), outcome=1.0, horizon_days=7,
        )
        assert engine.store == {}

    def test_boundary_070_records_to_upper_bucket(self):
        engine = FakeEngine()
        record_scored_prediction(
            engine, confidence=0.70, outcome=1.0, horizon_days=7,
        )
        assert (7, 0.70, 0.75) in engine.store

    def test_confidence_1_records_to_top_bucket(self):
        engine = FakeEngine()
        record_scored_prediction(
            engine, confidence=1.0, outcome=1.0, horizon_days=7,
        )
        assert (7, 0.95, 1.01) in engine.store


# ── get_bucket_calibration ────────────────────────────────────────────────


class TestGetBucketCalibration:
    def test_empty_history_returns_none(self):
        engine = FakeEngine()
        cal = get_bucket_calibration(
            engine, confidence=0.80, horizon_days=7,
        )
        assert cal is None

    def test_sub_50_confidence_returns_none(self):
        engine = FakeEngine()
        cal = get_bucket_calibration(
            engine, confidence=0.30, horizon_days=7,
        )
        assert cal is None

    def test_calibrated_bucket_flag_true(self):
        engine = FakeEngine()
        # Record enough predictions to cross the calibrated threshold
        for _ in range(MIN_CALIBRATED_SAMPLES):
            record_scored_prediction(
                engine, confidence=0.80, outcome=1.0, horizon_days=7,
            )
        cal = get_bucket_calibration(
            engine, confidence=0.80, horizon_days=7,
        )
        assert cal is not None
        assert cal.is_calibrated is True
        assert cal.n_predictions == MIN_CALIBRATED_SAMPLES
        assert cal.empirical_hit_rate == pytest.approx(1.0)

    def test_under_populated_bucket_is_not_calibrated(self):
        engine = FakeEngine()
        for _ in range(3):
            record_scored_prediction(
                engine, confidence=0.80, outcome=1.0, horizon_days=7,
            )
        cal = get_bucket_calibration(
            engine, confidence=0.80, horizon_days=7,
        )
        assert cal is not None
        assert cal.is_calibrated is False
        assert cal.n_predictions == 3

    def test_db_failure_returns_none(self):
        engine = FakeEngine(raise_on_execute=True)
        cal = get_bucket_calibration(
            engine, confidence=0.80, horizon_days=7,
        )
        assert cal is None

    def test_to_dict_roundtrip(self):
        engine = FakeEngine()
        for _ in range(MIN_CALIBRATED_SAMPLES):
            record_scored_prediction(
                engine, confidence=0.80, outcome=1.0, horizon_days=7,
            )
        cal = get_bucket_calibration(
            engine, confidence=0.80, horizon_days=7,
        )
        assert cal is not None
        d = cal.to_dict()
        assert d["horizon_days"] == 7
        assert d["bucket_low"] == 0.80
        assert d["bucket_high"] == 0.85
        assert d["n_predictions"] == MIN_CALIBRATED_SAMPLES
        assert d["empirical_hit_rate"] == 1.0
        assert d["is_calibrated"] is True


# ── conviction_multiplier_for_bucket ──────────────────────────────────────


class TestConvictionMultiplier:
    def _populate_bucket(
        self,
        engine: FakeEngine,
        *,
        confidence: float,
        horizon: int,
        n: int,
        n_hits: float,
    ) -> None:
        """Directly populate the FakeEngine store to simulate a calibrated
        bucket with a specific hit rate. Bypasses record_scored_prediction
        so the test controls n_hits exactly.
        """
        bucket = _bucket_for(confidence)
        assert bucket is not None
        low, high = bucket
        engine.store[(horizon, low, high)] = {
            "n": n,
            "hits": n_hits,
            "brier": 0.0,
            "last_updated": datetime.now(timezone.utc),
        }

    def test_no_history_returns_neutral(self):
        engine = FakeEngine()
        mult = conviction_multiplier_for_bucket(
            engine, confidence=0.80, horizon_days=7,
        )
        assert mult == MULT_NEUTRAL

    def test_cold_start_below_min_samples_returns_neutral(self):
        engine = FakeEngine()
        self._populate_bucket(
            engine,
            confidence=0.80,
            horizon=7,
            n=5,
            n_hits=1.0,  # would be severe overconf if calibrated
        )
        mult = conviction_multiplier_for_bucket(
            engine, confidence=0.80, horizon_days=7,
        )
        assert mult == MULT_NEUTRAL

    def test_severe_overconfidence_returns_060(self):
        engine = FakeEngine()
        # confidence 0.80, bucket hit rate 0.55 → gap 0.25 → severe
        self._populate_bucket(
            engine,
            confidence=0.80,
            horizon=7,
            n=MIN_CALIBRATED_SAMPLES * 2,
            n_hits=MIN_CALIBRATED_SAMPLES * 2 * 0.55,
        )
        mult = conviction_multiplier_for_bucket(
            engine, confidence=0.80, horizon_days=7,
        )
        assert mult == MULT_SEVERE_OVERCONF

    def test_high_overconfidence_returns_080(self):
        engine = FakeEngine()
        # confidence 0.80, hit rate 0.65 → gap 0.15 → high
        self._populate_bucket(
            engine,
            confidence=0.80,
            horizon=7,
            n=MIN_CALIBRATED_SAMPLES * 2,
            n_hits=MIN_CALIBRATED_SAMPLES * 2 * 0.65,
        )
        mult = conviction_multiplier_for_bucket(
            engine, confidence=0.80, horizon_days=7,
        )
        assert mult == MULT_HIGH_OVERCONF

    def test_mild_overconfidence_returns_092(self):
        engine = FakeEngine()
        # confidence 0.80, hit rate 0.72 → gap 0.08 → mild
        self._populate_bucket(
            engine,
            confidence=0.80,
            horizon=7,
            n=MIN_CALIBRATED_SAMPLES * 5,
            n_hits=MIN_CALIBRATED_SAMPLES * 5 * 0.72,
        )
        mult = conviction_multiplier_for_bucket(
            engine, confidence=0.80, horizon_days=7,
        )
        assert mult == MULT_MILD_OVERCONF

    def test_well_calibrated_returns_neutral(self):
        engine = FakeEngine()
        # confidence 0.80, hit rate 0.80 → gap 0.0 → neutral
        self._populate_bucket(
            engine,
            confidence=0.80,
            horizon=7,
            n=MIN_CALIBRATED_SAMPLES * 5,
            n_hits=MIN_CALIBRATED_SAMPLES * 5 * 0.80,
        )
        mult = conviction_multiplier_for_bucket(
            engine, confidence=0.80, horizon_days=7,
        )
        assert mult == MULT_NEUTRAL

    def test_mild_underconfidence_returns_105(self):
        engine = FakeEngine()
        # confidence 0.70, hit rate 0.78 → gap -0.08 → mild underconf
        self._populate_bucket(
            engine,
            confidence=0.70,
            horizon=7,
            n=MIN_CALIBRATED_SAMPLES * 5,
            n_hits=MIN_CALIBRATED_SAMPLES * 5 * 0.78,
        )
        mult = conviction_multiplier_for_bucket(
            engine, confidence=0.70, horizon_days=7,
        )
        assert mult == MULT_MILD_UNDERCONF

    def test_strong_underconfidence_returns_108(self):
        engine = FakeEngine()
        # confidence 0.70, hit rate 0.90 → gap -0.20 → strong underconf
        self._populate_bucket(
            engine,
            confidence=0.70,
            horizon=7,
            n=MIN_CALIBRATED_SAMPLES * 5,
            n_hits=MIN_CALIBRATED_SAMPLES * 5 * 0.90,
        )
        mult = conviction_multiplier_for_bucket(
            engine, confidence=0.70, horizon_days=7,
        )
        assert mult == MULT_STRONG_UNDERCONF

    def test_db_failure_returns_neutral(self):
        engine = FakeEngine(raise_on_execute=True)
        mult = conviction_multiplier_for_bucket(
            engine, confidence=0.80, horizon_days=7,
        )
        assert mult == MULT_NEUTRAL


# ── rank_buckets_by_calibration ───────────────────────────────────────────


class TestRankBuckets:
    def test_empty_history_returns_empty(self):
        engine = FakeEngine()
        assert rank_buckets_by_calibration(engine, horizon_days=7) == []

    def test_sort_worst_first(self):
        engine = FakeEngine()
        now = datetime.now(timezone.utc)
        # Three buckets, all at 7d, with different miscalibrations.
        engine.store[(7, 0.55, 0.60)] = {
            "n": 100,
            "hits": 56.0,  # midpoint 0.575, rate 0.56, |diff| ≈ 0.015
            "brier": 0.0,
            "last_updated": now,
        }
        engine.store[(7, 0.75, 0.80)] = {
            "n": 100,
            "hits": 50.0,  # midpoint 0.775, rate 0.50, |diff| ≈ 0.275
            "brier": 0.0,
            "last_updated": now,
        }
        engine.store[(7, 0.85, 0.90)] = {
            "n": 100,
            "hits": 70.0,  # midpoint 0.875, rate 0.70, |diff| ≈ 0.175
            "brier": 0.0,
            "last_updated": now,
        }
        ranked = rank_buckets_by_calibration(engine, horizon_days=7)
        assert len(ranked) == 3
        # Worst first: 0.75 bucket, then 0.85, then 0.55
        assert ranked[0].bucket_low == 0.75
        assert ranked[1].bucket_low == 0.85
        assert ranked[2].bucket_low == 0.55

    def test_horizon_filter(self):
        engine = FakeEngine()
        now = datetime.now(timezone.utc)
        engine.store[(7, 0.80, 0.85)] = {
            "n": 10, "hits": 8.0, "brier": 0.0, "last_updated": now,
        }
        engine.store[(30, 0.80, 0.85)] = {
            "n": 10, "hits": 5.0, "brier": 0.0, "last_updated": now,
        }
        ranked_7 = rank_buckets_by_calibration(engine, horizon_days=7)
        ranked_30 = rank_buckets_by_calibration(engine, horizon_days=30)
        assert len(ranked_7) == 1
        assert len(ranked_30) == 1
        assert ranked_7[0].horizon_days == 7
        assert ranked_30[0].horizon_days == 30


# ── _ensure_schema idempotency ────────────────────────────────────────────


class TestEnsureSchema:
    def test_single_call_runs_ddl(self):
        engine = FakeEngine()
        _ensure_schema(engine)
        assert engine.ddl_calls > 0

    def test_second_call_is_noop(self):
        engine = FakeEngine()
        _ensure_schema(engine)
        first_count = engine.ddl_calls
        _ensure_schema(engine)
        # Should still be first_count — no new DDL was issued.
        assert engine.ddl_calls == first_count

    def test_after_reset_ddl_reruns(self):
        engine = FakeEngine()
        _ensure_schema(engine)
        first_count = engine.ddl_calls
        _reset_initialized_engines()
        _ensure_schema(engine)
        assert engine.ddl_calls > first_count


# ── Verdict mapping (imported from bootstrap) ─────────────────────────────


class TestVerdictMapping:
    def test_verdict_mapping_through_bootstrap(self):
        # Indirectly verify verdict_to_outcome is imported and drives the
        # bootstrap outcome. Direct import avoids flakiness if the
        # bootstrap module changes internals.
        from scripts.bootstrap_per_signal_brier import verdict_to_outcome
        assert verdict_to_outcome("hit") == 1.0
        assert verdict_to_outcome("partial") == 0.5
        assert verdict_to_outcome("miss") == 0.0


# ── bootstrap_from_oracle_predictions ─────────────────────────────────────


class TestBootstrapReplay:
    def test_empty_oracle_predictions_returns_zero(self):
        engine = FakeEngine(oracle_rows=[])
        count = bootstrap_from_oracle_predictions(engine, days=30)
        assert count == 0
        assert engine.store == {}

    def test_replay_updates_buckets(self):
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(days=7)
        # Three scored predictions in the same bucket.
        rows = [
            (0.80, "hit", now, expiry),
            (0.80, "miss", now, expiry),
            (0.80, "partial", now, expiry),
        ]
        engine = FakeEngine(oracle_rows=rows)
        count = bootstrap_from_oracle_predictions(engine, days=30)
        assert count == 3
        assert (7, 0.80, 0.85) in engine.store
        row = engine.store[(7, 0.80, 0.85)]
        assert row["n"] == 3
        assert row["hits"] == pytest.approx(1.5)

    def test_replay_skips_sub_50_confidences(self):
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(days=7)
        rows = [
            (0.30, "hit", now, expiry),
            (0.80, "hit", now, expiry),
        ]
        engine = FakeEngine(oracle_rows=rows)
        count = bootstrap_from_oracle_predictions(engine, days=30)
        assert count == 1
        assert (7, 0.80, 0.85) in engine.store

    def test_replay_skips_invalid_verdicts(self):
        now = datetime.now(timezone.utc)
        expiry = now + timedelta(days=7)
        rows = [
            (0.80, "pending", now, expiry),
            (0.80, None, now, expiry),
            (0.80, "hit", now, expiry),
        ]
        engine = FakeEngine(oracle_rows=rows)
        count = bootstrap_from_oracle_predictions(engine, days=30)
        assert count == 1

    def test_replay_db_failure_returns_zero(self):
        engine = FakeEngine(raise_on_execute=True)
        count = bootstrap_from_oracle_predictions(engine, days=30)
        assert count == 0
