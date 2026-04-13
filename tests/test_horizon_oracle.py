"""Tests for ALPHA-3 horizon-conditional oracle (task #106).

Covers the horizon_buckets schema extension on ``oracle_models``, the
per-horizon weight / calibration paths in ``oracle.engine`` and
``oracle.calibration``, and the evolve_weights drift surface. All tests
use the ``mock_engine`` fixture — no live DB calls.

Structure:

* TestHorizonBucketDefaults — default factory + dataclass wiring
* TestUnknownHorizonMapping — documented nearest-bucket / 7d fallback
* TestModelRegistryHorizonNudge — per-bucket Bayesian nudge via jsonb_set
* TestPredictHorizonAware — EnsemblePredictor.predict horizon kwarg
* TestUpdateRunningMetricsHorizonAware — per-bucket Brier / ECE persist
* TestEvolveWeightsBucketAware — per-bucket drift flagging
* TestSchemaMigration — idempotency + grants via static SQL assertions

The mock_engine fixture's default ``fetchone()`` returns ``None`` so every
test that wants a specific DB response must wire its own return value.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from contracts.schemas import PredictionScored, SignalRef


# ── Shared fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def signal_refs() -> list[SignalRef]:
    return [
        SignalRef(
            signal_id=uuid4(),
            source="insider",
            trust_at_prediction=0.6,
            weight_at_prediction=1.0,
        ),
    ]


def _make_evt(
    verdict: str,
    weights: dict[str, float],
    *,
    signal_refs: list[SignalRef],
    horizon: int = 7,
) -> PredictionScored:
    return PredictionScored(
        producer_module="oracle.engine",
        correlation_id=uuid4(),
        prediction_id=uuid4(),
        decision_id=1,
        ticker="AAPL",
        verdict=verdict,
        expected_direction="UP",
        realized_direction="UP" if verdict == "HIT" else "DOWN",
        confidence=0.7,
        brier_component=0.09,
        signals_used=signal_refs,
        model_weights_at_prediction=weights,
        horizon=horizon,
    )


def _wire_bucket_row(mock_engine, bucket_payload: dict | None) -> None:
    """Configure the mock_engine so SELECTs return a bucket-shaped row.

    The registry / calibration paths issue the legacy row SELECT first
    and then an extra bucket SELECT. We route both through the same
    fetchone return so unit tests can focus on the bucket payload.
    """
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    result = MagicMock()
    # Shape: (weight, hits, partials, misses, predictions_made, horizon_buckets)
    result.fetchone.return_value = (
        1.0, 0, 0, 0, 0, bucket_payload,
    )
    mock_conn.execute.return_value = result


# ──────────────────────────────────────────────────────────────────────────
# 1. Default factory and dataclass wiring
# ──────────────────────────────────────────────────────────────────────────


class TestHorizonBucketDefaults:
    def test_factory_returns_four_buckets(self):
        from oracle.engine import HORIZON_BUCKETS, _default_horizon_buckets

        buckets = _default_horizon_buckets()
        assert set(buckets.keys()) == set(HORIZON_BUCKETS)
        assert len(buckets) == 4

    def test_every_bucket_starts_at_weight_one(self):
        from oracle.engine import _default_horizon_buckets

        for key, bucket in _default_horizon_buckets().items():
            assert bucket["weight"] == 1.0, key

    def test_every_bucket_has_zero_counters(self):
        from oracle.engine import _default_horizon_buckets

        for key, bucket in _default_horizon_buckets().items():
            assert bucket["hits"] == 0, key
            assert bucket["misses"] == 0, key
            assert bucket["partials"] == 0, key
            assert bucket["scored"] == 0, key
            assert bucket["brier"] == 0.0, key
            assert bucket["ece"] == 0.0, key

    def test_default_factory_is_fresh_per_model(self):
        from oracle.engine import _default_horizon_buckets

        a = _default_horizon_buckets()
        b = _default_horizon_buckets()
        a["7d"]["weight"] = 99.0
        assert b["7d"]["weight"] == 1.0, "default factory is sharing state"

    def test_oracle_model_default_has_buckets(self):
        from oracle.engine import OracleModel

        m = OracleModel(
            name="unit_test",
            version="1.0",
            description="",
            signal_families=[],
        )
        assert set(m.horizon_buckets.keys()) == {"1d", "7d", "30d", "90d"}
        assert m.horizon_buckets["7d"]["weight"] == 1.0

    def test_oracle_model_bucket_weight_falls_back_to_legacy(self):
        from oracle.engine import OracleModel

        m = OracleModel(
            name="unit_test",
            version="1.0",
            description="",
            signal_families=[],
            weight=1.7,
        )
        # Zero out the 30d bucket weight — should fall back to legacy 1.7.
        m.horizon_buckets["30d"]["weight"] = 0.0
        assert m.bucket_weight(30) == pytest.approx(1.7)
        # A live 7d bucket stays at 1.0 and is used directly.
        assert m.bucket_weight(7) == pytest.approx(1.0)


# ──────────────────────────────────────────────────────────────────────────
# 2. Horizon key mapping (documented behaviour)
# ──────────────────────────────────────────────────────────────────────────


class TestUnknownHorizonMapping:
    def test_canonical_horizons_pass_through(self):
        from oracle.engine import _horizon_key

        assert _horizon_key(1) == "1d"
        assert _horizon_key(7) == "7d"
        assert _horizon_key(30) == "30d"
        assert _horizon_key(90) == "90d"

    def test_unknown_horizon_snaps_to_nearest(self):
        from oracle.engine import _horizon_key

        # 14 is equidistant to 7 and 30 — spec snaps to the shorter horizon.
        assert _horizon_key(14) == "7d"
        # 20 is closer to 30 than 7 (|20-30|=10, |20-7|=13)
        assert _horizon_key(20) == "30d"
        # 70 is closer to 90 than 30 (|70-90|=20, |70-30|=40)
        assert _horizon_key(70) == "90d"
        # 2 is closer to 1 than 7 (|2-1|=1, |2-7|=5)
        assert _horizon_key(2) == "1d"
        # 60 is equidistant to 30 and 90 — tie breaks to shorter horizon.
        assert _horizon_key(60) == "30d"

    def test_none_falls_back_to_7d(self):
        from oracle.engine import _horizon_key

        assert _horizon_key(None) == "7d"

    def test_string_key_accepted(self):
        from oracle.engine import _horizon_key

        assert _horizon_key("7d") == "7d"
        assert _horizon_key("30d") == "30d"

    def test_malformed_string_falls_back_to_default(self):
        from oracle.engine import _horizon_key

        assert _horizon_key("garbage") == "7d"


# ──────────────────────────────────────────────────────────────────────────
# 3. ModelRegistry horizon-aware nudge
# ──────────────────────────────────────────────────────────────────────────


class TestModelRegistryHorizonNudge:
    """The per-event nudge must target the horizon bucket carried on the
    contract while keeping the legacy scalar weight in sync as the
    unweighted mean across buckets. When the DB returns no row the
    fallback path stays byte-for-byte identical to Wave E so parity
    tests continue to hold.
    """

    def _collect_update_calls(self, mock_engine) -> list[tuple]:
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        return list(mock_conn.execute.call_args_list)

    def test_hit_nudges_7d_bucket_via_jsonb_set(self, signal_refs, mock_engine):
        from oracle.engine import ModelRegistry, _default_horizon_buckets

        buckets = _default_horizon_buckets()
        _wire_bucket_row(mock_engine, buckets)

        registry = ModelRegistry(mock_engine)
        evt = _make_evt(
            "HIT",
            {"flow_momentum": 1.0},
            signal_refs=signal_refs,
            horizon=7,
        )
        registry.update_from_contract(evt)

        calls = self._collect_update_calls(mock_engine)
        # At least one jsonb_set call with path == '{7d}'
        bucket_calls = [
            c for c in calls
            if "jsonb_set" in str(c[0][0]) and c[0][1].get("path") == "{7d}"
        ]
        assert bucket_calls, "expected a jsonb_set UPDATE targeting {7d}"
        # The targeted bucket's hits counter must have been incremented
        # and its weight nudged upward.
        last_bucket = bucket_calls[-1][0][1]
        bucket_json = json.loads(last_bucket["bucket"])
        assert bucket_json["hits"] == 1
        assert bucket_json["scored"] == 1
        assert bucket_json["weight"] > 1.0

    def test_miss_nudges_30d_bucket_and_leaves_others_untouched(
        self, signal_refs, mock_engine
    ):
        from oracle.engine import ModelRegistry, _default_horizon_buckets

        buckets = _default_horizon_buckets()
        _wire_bucket_row(mock_engine, buckets)

        registry = ModelRegistry(mock_engine)
        evt = _make_evt(
            "MISS",
            {"flow_momentum": 1.0},
            signal_refs=signal_refs,
            horizon=30,
        )
        registry.update_from_contract(evt)

        calls = self._collect_update_calls(mock_engine)
        bucket_calls = [
            c for c in calls
            if "jsonb_set" in str(c[0][0]) and c[0][1].get("path") == "{30d}"
        ]
        assert bucket_calls, "expected a jsonb_set UPDATE targeting {30d}"
        bucket_json = json.loads(bucket_calls[-1][0][1]["bucket"])
        assert bucket_json["misses"] == 1
        assert bucket_json["scored"] == 1
        assert bucket_json["weight"] < 1.0  # MISS pulls 7d target = 0.5

        # No jsonb_set UPDATE should target the other buckets.
        for bk in ("{1d}", "{7d}", "{90d}"):
            other = [
                c for c in calls
                if "jsonb_set" in str(c[0][0]) and c[0][1].get("path") == bk
            ]
            assert not other, f"bucket {bk} was modified but should not be"

    def test_unknown_horizon_snaps_to_nearest_bucket(
        self, signal_refs, mock_engine
    ):
        """Documented behaviour: horizon=14 snaps to 7d (the nearest
        canonical bucket, breaking ties toward the shorter horizon).
        """
        from oracle.engine import ModelRegistry, _default_horizon_buckets

        buckets = _default_horizon_buckets()
        _wire_bucket_row(mock_engine, buckets)

        registry = ModelRegistry(mock_engine)
        evt = _make_evt(
            "HIT",
            {"flow_momentum": 1.0},
            signal_refs=signal_refs,
            horizon=14,
        )
        registry.update_from_contract(evt)

        calls = self._collect_update_calls(mock_engine)
        paths = {
            c[0][1].get("path") for c in calls
            if "jsonb_set" in str(c[0][0])
        }
        assert "{7d}" in paths

    def test_missing_buckets_json_falls_back_to_legacy_weight(
        self, signal_refs, mock_engine
    ):
        """When the DB returns None for horizon_buckets, the fallback
        branch still nudges the legacy weight and does not raise. Wave
        A / E parity is preserved because the final execute call remains
        the legacy ``SET weight = :w`` UPDATE.
        """
        from oracle.engine import ModelRegistry

        # mock_engine default: fetchone returns None (see conftest.py)
        registry = ModelRegistry(mock_engine)
        evt = _make_evt(
            "HIT",
            {"flow_momentum": 1.0},
            signal_refs=signal_refs,
            horizon=7,
        )
        n = registry.update_from_contract(evt)
        assert n == 1

        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        last_call_args = mock_conn.execute.call_args_list[-1][0]
        bound = last_call_args[1]
        assert "w" in bound  # legacy scalar UPDATE is still the last call
        # HIT target = 2.5, new = 1.0 + 0.05*(2.5-1.0) = 1.075
        assert bound["w"] == pytest.approx(1.075, abs=1e-4)


# ──────────────────────────────────────────────────────────────────────────
# 4. EnsemblePredictor.predict horizon-aware
# ──────────────────────────────────────────────────────────────────────────


class TestPredictHorizonAware:
    """``predict(ticker, horizon=…)`` must route per-horizon bucket weights
    into the vote aggregation. We mock the hit-rate / bucket-weight
    lookups directly so the test doesn't need a full OracleEngine stack.
    """

    def _build_predictor_with_stub_factory(self, mock_engine):
        from oracle.engine import EnsemblePredictor

        predictor = EnsemblePredictor.__new__(EnsemblePredictor)
        predictor.engine = mock_engine

        # Stub factory / aggregator so predict() doesn't hit the real
        # model_factory / signal_aggregator modules.
        class _Model:
            def __init__(self, name):
                self.name = name
                self.weight_config = None
                self.min_signals = 0

        class _Agg:
            direction = "bullish"
            strength = 1.0
            confidence = 0.8
            coherence = 1.0
            signal_count = 5

        stub_factory = MagicMock()
        stub_factory.list_active_models.return_value = [
            _Model("model_a"),
            _Model("model_b"),
        ]
        stub_factory.get_signals_for_model.return_value = [1, 2, 3, 4, 5]
        stub_aggregator = MagicMock()
        stub_aggregator.aggregate.return_value = _Agg()
        predictor.factory = stub_factory
        predictor.aggregator = stub_aggregator
        return predictor

    def test_different_horizons_produce_different_bucket_weights(
        self, mock_engine
    ):
        predictor = self._build_predictor_with_stub_factory(mock_engine)

        # Divergent buckets: model_a weighted toward 1d, model_b toward 30d.
        weights_by_horizon = {
            ("model_a", 1): 3.0,
            ("model_a", 30): 0.2,
            ("model_b", 1): 0.2,
            ("model_b", 30): 3.0,
        }
        predictor._get_hit_rate = lambda name, *, horizon=None: 0.7
        predictor._get_bucket_weight = (
            lambda name, *, horizon=None: weights_by_horizon.get(
                (name, int(horizon)), 1.0
            )
        )

        pred_1d = predictor.predict("AAPL", horizon=1)
        pred_30d = predictor.predict("AAPL", horizon=30)

        assert pred_1d.horizon == 1
        assert pred_30d.horizon == 30
        # model_a dominates the 1d ensemble and model_b dominates 30d —
        # so the top vote by weight must differ between calls.
        top_1d = pred_1d.model_votes[0]["model_name"]
        top_30d = pred_30d.model_votes[0]["model_name"]
        assert top_1d == "model_a"
        assert top_30d == "model_b"
        assert top_1d != top_30d

    def test_default_horizon_is_7(self, mock_engine):
        predictor = self._build_predictor_with_stub_factory(mock_engine)
        predictor._get_hit_rate = lambda name, *, horizon=None: 0.6
        predictor._get_bucket_weight = lambda name, *, horizon=None: 1.0
        pred = predictor.predict("AAPL")
        assert pred.horizon == 7

    def test_predict_batch_forwards_horizon(self, mock_engine):
        predictor = self._build_predictor_with_stub_factory(mock_engine)
        seen_horizons = []

        def _fake_predict(ticker, as_of=None, *, horizon=7):
            seen_horizons.append(horizon)
            # Return a tiny neutral prediction via the real dataclass.
            from oracle.engine import EnsemblePrediction

            return EnsemblePrediction(
                ticker=ticker, direction="neutral", score=50,
                confidence=0.0, strength=0.0, coherence=0.0,
                model_count=0, level="meta", model_votes=[],
                as_of=None, horizon=horizon,
            )

        predictor.predict = _fake_predict
        out = predictor.predict_batch(["AAPL", "MSFT"], horizon=30)
        assert set(out.keys()) == {"AAPL", "MSFT"}
        assert seen_horizons == [30, 30]


# ──────────────────────────────────────────────────────────────────────────
# 5. update_running_metrics horizon-aware
# ──────────────────────────────────────────────────────────────────────────


class TestUpdateRunningMetricsHorizonAware:
    def test_persists_per_horizon_brier_via_jsonb_set(self, mock_engine):
        from oracle.calibration import update_running_metrics

        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        # First SELECT: legacy row. Second SELECT: bucket row (same mock,
        # return a real dict so the override branch fires).
        fetch_sequence = [
            (0.0, 0.0, 0),                            # legacy row
            ({"7d": {"weight": 1.0, "hits": 0,
                     "misses": 0, "partials": 0,
                     "scored": 0, "brier": 0.0,
                     "ece": 0.0}},),                   # bucket row
        ]

        def _fetchone():
            return fetch_sequence.pop(0) if fetch_sequence else None

        execute_result = MagicMock()
        execute_result.fetchone.side_effect = _fetchone
        mock_conn.execute.return_value = execute_result

        out = update_running_metrics(
            mock_engine,
            model_id="flow_momentum",
            prediction=0.8,
            actual=1.0,
            horizon=7,
        )
        assert out["count"] == 1
        # Bucket-derived legacy Brier for a single scored bucket equals
        # the squared error (0.04) directly — override path fired.
        assert out["running_brier"] == pytest.approx(0.04, abs=1e-4)

        # Confirm a jsonb_set UPDATE with path == '{7d}' was issued.
        paths = [
            call[0][1].get("path") for call in mock_conn.execute.call_args_list
            if "jsonb_set" in str(call[0][0])
        ]
        assert "{7d}" in paths

    def test_legacy_running_brier_is_unweighted_average(self, mock_engine):
        """When only one bucket has scored events, the legacy running_brier
        matches that bucket's Brier. Wave A's ``test_subsequent_value_is
        _running_mean`` exercises the non-dict payload path so its Welford
        expectation stays intact.
        """
        from oracle.calibration import update_running_metrics

        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        fetch_sequence = [
            (0.04, 0.2, 1),   # legacy row w/ prior running stats
            (
                {
                    "7d":  {"scored": 2, "brier": 0.10, "ece": 0.25,
                            "weight": 1.0, "hits": 1, "misses": 0,
                            "partials": 0},
                    "30d": {"scored": 2, "brier": 0.20, "ece": 0.35,
                            "weight": 1.0, "hits": 0, "misses": 1,
                            "partials": 0},
                    "1d":  {"scored": 0, "brier": 0.0, "ece": 0.0,
                            "weight": 1.0, "hits": 0, "misses": 0,
                            "partials": 0},
                    "90d": {"scored": 0, "brier": 0.0, "ece": 0.0,
                            "weight": 1.0, "hits": 0, "misses": 0,
                            "partials": 0},
                },
            ),
        ]

        def _fetchone():
            return fetch_sequence.pop(0) if fetch_sequence else None

        result = MagicMock()
        result.fetchone.side_effect = _fetchone
        mock_conn.execute.return_value = result

        out = update_running_metrics(
            mock_engine,
            model_id="flow_momentum",
            prediction=0.5,
            actual=1.0,
            horizon=7,
        )
        # After the bucket nudge 7d brier = incremental Welford of (0.10, 0.25)
        # over scored=3: b = 0.10 + (0.25 - 0.10)/3 = 0.15
        # 30d brier stays 0.20 (unchanged, only 7d fired).
        # Unweighted mean across scored buckets = (0.15 + 0.20) / 2 = 0.175
        assert out["running_brier"] == pytest.approx(0.175, abs=1e-3)

    def test_compute_per_horizon_calibration_reads_json(self, mock_engine):
        from oracle.calibration import compute_per_horizon_calibration

        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        result = MagicMock()
        result.fetchone.return_value = (
            {
                "1d":  {"brier": 0.11, "ece": 0.12, "scored": 3,
                        "weight": 1.2, "hits": 2, "misses": 1,
                        "partials": 0},
                "7d":  {"brier": 0.21, "ece": 0.22, "scored": 5,
                        "weight": 1.5, "hits": 4, "misses": 1,
                        "partials": 0},
                "30d": {"brier": 0.31, "ece": 0.32, "scored": 1,
                        "weight": 0.8, "hits": 0, "misses": 1,
                        "partials": 0},
                "90d": {"brier": 0.41, "ece": 0.42, "scored": 0,
                        "weight": 1.0, "hits": 0, "misses": 0,
                        "partials": 0},
            },
        )
        mock_conn.execute.return_value = result

        out = compute_per_horizon_calibration(mock_engine, "flow_momentum")
        assert set(out.keys()) == {1, 7, 30, 90}
        assert out[7]["brier"] == pytest.approx(0.21)
        assert out[30]["weight"] == pytest.approx(0.8)
        assert out[1]["hits"] == 2


# ──────────────────────────────────────────────────────────────────────────
# 6. evolve_weights bucket-aware drift
# ──────────────────────────────────────────────────────────────────────────


class TestEvolveWeightsBucketAware:
    def test_per_bucket_drift_is_flagged(self, mock_engine, caplog):
        """A model whose 30d bucket drifts more than DRIFT_THRESHOLD
        (2%) from the legacy scalar weight must appear in the returned
        ``bucket_drift`` dict even when the aggregate batch vs event
        counter drift is clean.
        """
        from oracle.engine import OracleEngine

        eng = OracleEngine.__new__(OracleEngine)
        eng.engine = mock_engine

        # Row shape: (name, weight, hits, partials, misses, predictions_made,
        #             scored_prediction_count, horizon_buckets)
        row = (
            "flow_momentum",
            1.0,
            10, 0, 10,   # hits=10, partials=0, misses=10 → batch_total 20
            20,          # predictions_made
            20,          # event_count — matches batch_total, clean
            {
                "1d":  {"weight": 1.0, "hits": 0, "misses": 0,
                        "partials": 0, "scored": 0,
                        "brier": 0.0, "ece": 0.0},
                "7d":  {"weight": 1.0, "hits": 0, "misses": 0,
                        "partials": 0, "scored": 0,
                        "brier": 0.0, "ece": 0.0},
                "30d": {"weight": 2.5,  # 150% drift from legacy 1.0
                        "hits": 0, "misses": 0, "partials": 0,
                        "scored": 0, "brier": 0.0, "ece": 0.0},
                "90d": {"weight": 1.0, "hits": 0, "misses": 0,
                        "partials": 0, "scored": 0,
                        "brier": 0.0, "ece": 0.0},
            },
        )

        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        result = MagicMock()
        result.fetchall.return_value = [row]
        mock_conn.execute.return_value = result

        out = eng.evolve_weights(event_driven=True)
        assert out["mode"] == "event_driven"
        # Aggregate drift stays clean — the batch and event counts match.
        assert "flow_momentum" not in out["drift"]
        # Per-bucket drift surfaces the 30d divergence.
        assert "flow_momentum" in out["bucket_drift"]
        assert "30d" in out["bucket_drift"]["flow_momentum"]
        delta = out["bucket_drift"]["flow_momentum"]["30d"]["delta_pct"]
        assert delta > 2.0

    def test_event_driven_does_not_update_weight(self, mock_engine):
        """Regression: the bucket-aware reconciliation pass must still
        not issue any ``SET weight = :w`` UPDATE. Wave E invariants
        are preserved.
        """
        from oracle.engine import OracleEngine

        eng = OracleEngine.__new__(OracleEngine)
        eng.engine = mock_engine

        eng.evolve_weights(event_driven=True)
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        for call in mock_conn.execute.call_args_list:
            sql = str(call[0][0])
            assert "SET weight = :w" not in sql, sql


# ──────────────────────────────────────────────────────────────────────────
# 7. Migration 0042 — static SQL assertions
# ──────────────────────────────────────────────────────────────────────────


_MIGRATION = (
    Path(__file__).resolve().parent.parent
    / "migrations" / "0042_oracle_horizon_aware.sql"
)


class TestSchemaMigration:
    def test_migration_file_exists(self):
        assert _MIGRATION.exists(), f"{_MIGRATION} missing"

    def test_migration_is_idempotent(self):
        sql = _MIGRATION.read_text()
        # ADD COLUMN IF NOT EXISTS → idempotent
        assert "ADD COLUMN IF NOT EXISTS horizon_buckets JSONB" in sql
        # Index creation must also be idempotent
        assert "CREATE INDEX IF NOT EXISTS" in sql

    def test_migration_has_grant_footer(self):
        sql = _MIGRATION.read_text()
        # Required by the runtime 'grid' role — migrations run as postgres.
        assert "GRANT ALL ON oracle_models TO grid" in sql

    def test_migration_seeds_four_buckets(self):
        sql = _MIGRATION.read_text()
        for bucket in ("'1d'", "'7d'", "'30d'", "'90d'"):
            assert bucket in sql, f"missing {bucket} seed"
