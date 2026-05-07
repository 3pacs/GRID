"""Tests for ALPHA-13 per-regime submodel router (task #116).

Covers the regime_buckets schema extension on ``oracle_models``, the new
``oracle/regime_router.py`` module, the predict-path integration in
``oracle.engine.EnsemblePredictor``, the contract-driven nudge in
``ModelRegistry.update_from_contract``, and the regime kwarg on
``oracle.calibration.compute_per_horizon_calibration``. All tests use
the ``mock_engine`` fixture from conftest — no live DB calls.

Structure:

* TestDefaultRegimeBuckets — factory + wiring
* TestParseRegimeBuckets — JSON / dict / None / garbage coercion
* TestRegimeRouter — read/write/summary APIs
* TestUpdateFromContractRegimeNudge — contract-driven nudge isolation
* TestPredictRegimeRouting — EnsemblePredictor.predict integration
* TestPerHorizonCalibrationRegimeKwarg — calibration filter passthrough
* TestSchemaMigration — idempotency + GIN index + GRANT footer

This file is the regression gate for Phase 0's multiplicative closer.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from contracts.schemas import PredictionScored, SignalRef


# ── Shared helpers ─────────────────────────────────────────────────────────


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
    regime: str | None = None,
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
        regime=regime,
    )


def _wire_row(
    mock_engine,
    *,
    horizon_payload: dict | None,
    regime_payload: dict | None,
) -> None:
    """Route the mock_engine SELECT so the registry sees a 7-tuple row with
    both horizon_buckets and regime_buckets populated.
    """
    mock_conn = mock_engine.begin.return_value.__enter__.return_value
    result = MagicMock()
    result.fetchone.return_value = (
        1.0, 0, 0, 0, 0, horizon_payload, regime_payload,
    )
    mock_conn.execute.return_value = result


# ──────────────────────────────────────────────────────────────────────────
# 1. Default factory
# ──────────────────────────────────────────────────────────────────────────


class TestDefaultRegimeBuckets:
    def test_factory_returns_five_states(self):
        from oracle.regime_router import REGIME_STATES, _default_regime_buckets

        buckets = _default_regime_buckets()
        assert set(buckets.keys()) == set(REGIME_STATES)
        assert len(buckets) == 5

    def test_every_state_starts_at_weight_one(self):
        from oracle.regime_router import _default_regime_buckets

        for state, bucket in _default_regime_buckets().items():
            assert bucket["weight"] == 1.0, state

    def test_every_state_has_zero_counters(self):
        from oracle.regime_router import _default_regime_buckets

        for state, bucket in _default_regime_buckets().items():
            assert bucket["hits"] == 0, state
            assert bucket["misses"] == 0, state
            assert bucket["partials"] == 0, state
            assert bucket["scored"] == 0, state
            assert bucket["brier"] == 0.0, state
            assert bucket["ece"] == 0.0, state

    def test_default_factory_is_fresh_per_model(self):
        from oracle.regime_router import _default_regime_buckets

        a = _default_regime_buckets()
        b = _default_regime_buckets()
        a["NEUTRAL"]["weight"] = 99.0
        assert b["NEUTRAL"]["weight"] == 1.0

    def test_regime_default_exposed_as_module_constant(self):
        from oracle.regime_router import REGIME_WEIGHTS_DEFAULT, REGIME_STATES

        assert set(REGIME_WEIGHTS_DEFAULT.keys()) == set(REGIME_STATES)
        for s in REGIME_STATES:
            assert REGIME_WEIGHTS_DEFAULT[s]["weight"] == 1.0


# ──────────────────────────────────────────────────────────────────────────
# 2. Parse regime buckets
# ──────────────────────────────────────────────────────────────────────────


class TestParseRegimeBuckets:
    def test_dict_passthrough(self):
        from oracle.regime_router import parse_regime_buckets

        src = {"NEUTRAL": {"weight": 1.5}, "CRISIS": {"weight": 0.3}}
        out = parse_regime_buckets(src)
        assert out["NEUTRAL"]["weight"] == 1.5
        assert out["CRISIS"]["weight"] == 0.3
        # Missing states are seeded from defaults, not omitted.
        assert out["EXPANSION"]["weight"] == 1.0

    def test_json_string_parse(self):
        from oracle.regime_router import parse_regime_buckets

        raw = json.dumps({"TIGHTENING": {"weight": 0.7}})
        out = parse_regime_buckets(raw)
        assert out["TIGHTENING"]["weight"] == pytest.approx(0.7)

    def test_none_falls_back_to_defaults(self):
        from oracle.regime_router import (
            parse_regime_buckets,
            REGIME_STATES,
        )

        out = parse_regime_buckets(None)
        assert set(out.keys()) == set(REGIME_STATES)
        for s in REGIME_STATES:
            assert out[s]["weight"] == 1.0

    def test_garbage_falls_back_to_defaults(self):
        from oracle.regime_router import (
            parse_regime_buckets,
            REGIME_STATES,
        )

        for garbage in ("not json", 42, ["list"], object()):
            out = parse_regime_buckets(garbage)
            assert set(out.keys()) == set(REGIME_STATES)
            assert out["NEUTRAL"]["weight"] == 1.0

    def test_malformed_field_falls_back_to_neutral(self):
        from oracle.regime_router import parse_regime_buckets

        src = {"NEUTRAL": {"weight": "nonsense"}}
        out = parse_regime_buckets(src)
        # Bad weight value → default 1.0 kept
        assert out["NEUTRAL"]["weight"] == 1.0


# ──────────────────────────────────────────────────────────────────────────
# 3. RegimeRouter read/write
# ──────────────────────────────────────────────────────────────────────────


class TestRegimeRouter:
    def test_model_regime_weight_returns_db_value(self, mock_engine):
        from oracle.regime_router import RegimeRouter

        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        result = MagicMock()
        result.fetchone.return_value = (
            {"CRISIS": {"weight": 0.4}, "NEUTRAL": {"weight": 1.2}},
        )
        mock_conn.execute.return_value = result

        router = RegimeRouter(mock_engine)
        assert router.model_regime_weight("m1", "CRISIS") == pytest.approx(0.4)

    def test_model_regime_weight_falls_back_to_one_when_row_missing(
        self, mock_engine
    ):
        from oracle.regime_router import RegimeRouter

        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        result = MagicMock()
        result.fetchone.return_value = None
        mock_conn.execute.return_value = result

        router = RegimeRouter(mock_engine)
        assert router.model_regime_weight("missing", "NEUTRAL") == 1.0

    def test_model_regime_weight_falls_back_when_regime_bucket_absent(
        self, mock_engine
    ):
        from oracle.regime_router import RegimeRouter

        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        result = MagicMock()
        result.fetchone.return_value = ({"CRISIS": {"weight": 0.4}},)
        mock_conn.execute.return_value = result

        router = RegimeRouter(mock_engine)
        # EXPANSION_STRONG not in the stored dict → default to 1.0
        assert router.model_regime_weight("m1", "EXPANSION_STRONG") == 1.0

    def test_model_regime_weight_unknown_state_maps_to_neutral(
        self, mock_engine
    ):
        from oracle.regime_router import RegimeRouter

        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        result = MagicMock()
        result.fetchone.return_value = ({"NEUTRAL": {"weight": 1.8}},)
        mock_conn.execute.return_value = result

        router = RegimeRouter(mock_engine)
        assert router.model_regime_weight("m1", "BOGUS") == pytest.approx(1.8)
        assert router.model_regime_weight("m1", None) == pytest.approx(1.8)

    def test_nudge_regime_weight_writes_via_jsonb_set_and_clamps(
        self, mock_engine
    ):
        from oracle.regime_router import MAX_WEIGHT, RegimeRouter

        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        # First SELECT returns a 4.9 weight; the +1.0 delta clamps to MAX.
        result = MagicMock()
        result.fetchone.return_value = ({"CRISIS": {"weight": 4.9}},)
        mock_conn.execute.return_value = result

        router = RegimeRouter(mock_engine)
        new_w = router.nudge_regime_weight("m1", "CRISIS", 1.0)
        assert new_w == pytest.approx(MAX_WEIGHT)

        # The last UPDATE must be a jsonb_set on regime_buckets with
        # path == {CRISIS}.
        calls = list(mock_conn.execute.call_args_list)
        update_calls = [
            c for c in calls
            if "jsonb_set" in str(c[0][0])
            and "regime_buckets" in str(c[0][0])
        ]
        assert update_calls, "expected a regime_buckets jsonb_set UPDATE"
        assert update_calls[-1][0][1]["path"] == "{CRISIS}"

    def test_nudge_regime_weight_clamps_lower_bound(self, mock_engine):
        from oracle.regime_router import MIN_WEIGHT, RegimeRouter

        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        result = MagicMock()
        result.fetchone.return_value = ({"TIGHTENING": {"weight": 0.15}},)
        mock_conn.execute.return_value = result

        router = RegimeRouter(mock_engine)
        new_w = router.nudge_regime_weight("m1", "TIGHTENING", -1.0)
        assert new_w == pytest.approx(MIN_WEIGHT)

    def test_summary_returns_matrix(self, mock_engine):
        from oracle.regime_router import REGIME_STATES, RegimeRouter

        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        result = MagicMock()
        result.fetchall.return_value = [
            ("flow_momentum",
             {"CRISIS": {"weight": 0.6}, "NEUTRAL": {"weight": 1.1}}),
            ("contagion",
             {"EXPANSION": {"weight": 1.3}}),
        ]
        mock_conn.execute.return_value = result

        router = RegimeRouter(mock_engine)
        summary = router.summary()
        assert set(summary.keys()) == {"flow_momentum", "contagion"}
        assert summary["flow_momentum"]["CRISIS"] == pytest.approx(0.6)
        assert summary["flow_momentum"]["NEUTRAL"] == pytest.approx(1.1)
        # Missing states fall back to 1.0 in the matrix.
        assert summary["flow_momentum"]["EXPANSION_STRONG"] == 1.0
        assert summary["contagion"]["EXPANSION"] == pytest.approx(1.3)
        # Every regime state must appear in every row.
        for name, row in summary.items():
            assert set(row.keys()) == set(REGIME_STATES), name


# ──────────────────────────────────────────────────────────────────────────
# 4. ModelRegistry.update_from_contract regime nudge
# ──────────────────────────────────────────────────────────────────────────


class TestUpdateFromContractRegimeNudge:
    def _collect_calls(self, mock_engine) -> list:
        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        return list(mock_conn.execute.call_args_list)

    def test_hit_in_neutral_nudges_only_neutral_bucket(
        self, signal_refs, mock_engine
    ):
        from oracle.engine import ModelRegistry, _default_horizon_buckets
        from oracle.regime_router import _default_regime_buckets

        _wire_row(
            mock_engine,
            horizon_payload=_default_horizon_buckets(),
            regime_payload=_default_regime_buckets(),
        )

        registry = ModelRegistry(mock_engine)
        evt = _make_evt(
            "HIT",
            {"flow_momentum": 1.0},
            signal_refs=signal_refs,
            horizon=7,
            regime="NEUTRAL",
        )
        registry.update_from_contract(evt)

        calls = self._collect_calls(mock_engine)
        regime_calls = [
            c for c in calls
            if "jsonb_set" in str(c[0][0]) and "regime_buckets" in str(c[0][0])
        ]
        assert regime_calls, "expected a regime_buckets jsonb_set UPDATE"
        last = regime_calls[-1][0][1]
        assert last["path"] == "{NEUTRAL}"
        bucket_json = json.loads(last["bucket"])
        assert bucket_json["hits"] == 1
        assert bucket_json["scored"] == 1
        assert bucket_json["weight"] > 1.0  # HIT nudges up

        # No other regime bucket should be touched.
        other_paths = {
            c[0][1].get("path") for c in regime_calls
        }
        assert other_paths == {"{NEUTRAL}"}

    def test_miss_in_crisis_nudges_crisis_bucket_down(
        self, signal_refs, mock_engine
    ):
        from oracle.engine import ModelRegistry, _default_horizon_buckets
        from oracle.regime_router import _default_regime_buckets

        _wire_row(
            mock_engine,
            horizon_payload=_default_horizon_buckets(),
            regime_payload=_default_regime_buckets(),
        )

        registry = ModelRegistry(mock_engine)
        evt = _make_evt(
            "MISS",
            {"flow_momentum": 1.0},
            signal_refs=signal_refs,
            horizon=30,
            regime="CRISIS",
        )
        registry.update_from_contract(evt)

        calls = self._collect_calls(mock_engine)
        regime_calls = [
            c for c in calls
            if "jsonb_set" in str(c[0][0]) and "regime_buckets" in str(c[0][0])
        ]
        assert regime_calls, "expected a regime_buckets jsonb_set UPDATE"
        last = regime_calls[-1][0][1]
        assert last["path"] == "{CRISIS}"
        bucket_json = json.loads(last["bucket"])
        assert bucket_json["misses"] == 1
        assert bucket_json["weight"] < 1.0  # MISS nudges down

    def test_missing_evt_regime_falls_back_to_horizon_only(
        self, signal_refs, mock_engine
    ):
        from oracle.engine import ModelRegistry, _default_horizon_buckets
        from oracle.regime_router import _default_regime_buckets

        _wire_row(
            mock_engine,
            horizon_payload=_default_horizon_buckets(),
            regime_payload=_default_regime_buckets(),
        )

        registry = ModelRegistry(mock_engine)
        evt = _make_evt(
            "HIT",
            {"flow_momentum": 1.0},
            signal_refs=signal_refs,
            horizon=7,
            regime=None,  # Pre-ALPHA-13 producer
        )
        registry.update_from_contract(evt)

        calls = self._collect_calls(mock_engine)
        regime_calls = [
            c for c in calls
            if "jsonb_set" in str(c[0][0]) and "regime_buckets" in str(c[0][0])
        ]
        assert not regime_calls, (
            "regime_buckets must not be touched when evt.regime is None"
        )
        # Horizon bucket update still fires.
        horizon_calls = [
            c for c in calls
            if "jsonb_set" in str(c[0][0]) and "horizon_buckets" in str(c[0][0])
        ]
        assert horizon_calls

    def test_legacy_weight_update_remains_last_call_with_regime(
        self, signal_refs, mock_engine
    ):
        """Wave E parity: when a regime is present the legacy ``SET weight =
        :w`` UPDATE must STILL be the final execute call so
        ``call_args_list[-1][0][1]["w"]`` continues to land on the scalar
        weight (the regime jsonb_set runs in between, not after).
        """
        from oracle.engine import ModelRegistry, _default_horizon_buckets
        from oracle.regime_router import _default_regime_buckets

        _wire_row(
            mock_engine,
            horizon_payload=_default_horizon_buckets(),
            regime_payload=_default_regime_buckets(),
        )

        registry = ModelRegistry(mock_engine)
        evt = _make_evt(
            "HIT",
            {"flow_momentum": 1.0},
            signal_refs=signal_refs,
            horizon=7,
            regime="EXPANSION",
        )
        registry.update_from_contract(evt)

        mock_conn = mock_engine.begin.return_value.__enter__.return_value
        last_call = mock_conn.execute.call_args_list[-1][0]
        assert "w" in last_call[1], "legacy scalar UPDATE must be last"
        assert "SET weight = :w" in str(last_call[0])


# ──────────────────────────────────────────────────────────────────────────
# 5. EnsemblePredictor.predict regime routing
# ──────────────────────────────────────────────────────────────────────────


def _build_predictor_with_stub_factory(mock_engine):
    """Mirror of test_horizon_oracle helper — bypasses ModelFactory +
    SignalAggregator so predict() can be exercised without a real DB.
    """
    from oracle.engine import EnsemblePredictor

    predictor = EnsemblePredictor.__new__(EnsemblePredictor)
    predictor.engine = mock_engine

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
    predictor._get_hit_rate = MagicMock(return_value=0.6)
    predictor._get_bucket_weight = MagicMock(return_value=1.0)
    return predictor


def _fake_regime(state: str):
    """Return a shaped LiquidityRegimeResult-like object for predict-path
    patching. The predict path only reads ``.state`` and
    ``.level_percentile`` plus uses ``apply_to_confidence`` from the
    same module.
    """
    from datetime import date
    from intelligence.liquidity_regime import LiquidityRegimeResult

    return LiquidityRegimeResult(
        state=state,
        as_of=date.today(),
        net_liquidity=5e12,
        level_percentile=50.0,
        weekly_change=0.0,
        weekly_change_z=0.0,
        monthly_change=0.0,
        confidence_multiplier=1.0,
        sample_size=100,
        reason="test",
    )


class TestPredictRegimeRouting:
    def test_crisis_regime_reads_crisis_weights(self, mock_engine):
        predictor = _build_predictor_with_stub_factory(mock_engine)
        crisis_weights = {"model_a": 0.3, "model_b": 0.3}

        with patch(
            "intelligence.liquidity_regime.classify_current_regime",
            return_value=_fake_regime("CRISIS"),
        ), patch(
            "oracle.regime_router.RegimeRouter.model_regime_weight",
            side_effect=lambda name, regime: crisis_weights.get(name, 1.0)
            if regime == "CRISIS" else 999.0,
        ), patch(
            "intelligence.catalyst_aggregator.proximity_score",
            return_value={"score": 0.0, "catalyst_type": None,
                          "nearest": None, "days_to_event": None,
                          "window_density": 0},
        ):
            pred = predictor.predict("AAPL")

        assert pred.regime == "CRISIS"
        assert pred.regime_router_weights == {
            "model_a": pytest.approx(0.3),
            "model_b": pytest.approx(0.3),
        }

    def test_different_regimes_yield_different_vote_weights(self, mock_engine):
        predictor = _build_predictor_with_stub_factory(mock_engine)

        def _weights_for(name, regime):
            matrix = {
                "CRISIS": {"model_a": 2.5, "model_b": 0.2},
                "EXPANSION": {"model_a": 0.2, "model_b": 2.5},
            }
            return matrix.get(regime, {}).get(name, 1.0)

        with patch(
            "oracle.regime_router.RegimeRouter.model_regime_weight",
            side_effect=_weights_for,
        ), patch(
            "intelligence.catalyst_aggregator.proximity_score",
            return_value={"score": 0.0, "catalyst_type": None,
                          "nearest": None, "days_to_event": None,
                          "window_density": 0},
        ):
            with patch(
                "intelligence.liquidity_regime.classify_current_regime",
                return_value=_fake_regime("CRISIS"),
            ):
                pred_crisis = predictor.predict("AAPL")
            with patch(
                "intelligence.liquidity_regime.classify_current_regime",
                return_value=_fake_regime("EXPANSION"),
            ):
                pred_exp = predictor.predict("AAPL")

        top_crisis = pred_crisis.model_votes[0]["model_name"]
        top_exp = pred_exp.model_votes[0]["model_name"]
        # model_a dominates in CRISIS, model_b in EXPANSION
        assert top_crisis == "model_a"
        assert top_exp == "model_b"
        assert top_crisis != top_exp

    def test_prediction_regime_and_liquidity_state_agree(self, mock_engine):
        """ALPHA-5 dampener and ALPHA-13 router must read the SAME regime
        string — capturing the classifier result once at the top of
        predict() is the whole point of task #116.
        """
        predictor = _build_predictor_with_stub_factory(mock_engine)

        with patch(
            "intelligence.liquidity_regime.classify_current_regime",
            return_value=_fake_regime("TIGHTENING"),
        ), patch(
            "oracle.regime_router.RegimeRouter.model_regime_weight",
            return_value=1.0,
        ), patch(
            "intelligence.catalyst_aggregator.proximity_score",
            return_value={"score": 0.0, "catalyst_type": None,
                          "nearest": None, "days_to_event": None,
                          "window_density": 0},
        ):
            pred = predictor.predict("AAPL")

        # No double-call drift — both fields must agree.
        assert pred.regime == "TIGHTENING"
        assert pred.liquidity_state == "TIGHTENING"

    def test_classifier_only_called_once_per_predict(self, mock_engine):
        """Guard against re-introducing the second classify_current_regime
        call in the ALPHA-5 dampening block. One call per predict.
        """
        predictor = _build_predictor_with_stub_factory(mock_engine)

        with patch(
            "intelligence.liquidity_regime.classify_current_regime",
            return_value=_fake_regime("NEUTRAL"),
        ) as mock_classify, patch(
            "oracle.regime_router.RegimeRouter.model_regime_weight",
            return_value=1.0,
        ), patch(
            "intelligence.catalyst_aggregator.proximity_score",
            return_value={"score": 0.0, "catalyst_type": None,
                          "nearest": None, "days_to_event": None,
                          "window_density": 0},
        ):
            predictor.predict("AAPL")

        assert mock_classify.call_count == 1


# ──────────────────────────────────────────────────────────────────────────
# 6. compute_per_horizon_calibration regime kwarg
# ──────────────────────────────────────────────────────────────────────────


class TestPerHorizonCalibrationRegimeKwarg:
    def test_regime_multiplier_applied_to_weight(self, mock_engine):
        from oracle.calibration import compute_per_horizon_calibration

        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        result = MagicMock()
        # 2-element tuple (horizon_buckets, regime_buckets)
        result.fetchone.return_value = (
            {
                "7d": {"brier": 0.2, "ece": 0.1, "scored": 5,
                       "weight": 1.4, "hits": 3, "misses": 2,
                       "partials": 0},
            },
            {
                "CRISIS": {"weight": 0.5},
                "NEUTRAL": {"weight": 1.0},
            },
        )
        mock_conn.execute.return_value = result

        out = compute_per_horizon_calibration(
            mock_engine, "flow_momentum", regime="CRISIS",
        )
        # 1.4 horizon weight × 0.5 regime multiplier = 0.7
        assert out[7]["weight"] == pytest.approx(0.7)
        # Brier untouched — per-regime Brier is CAT-180 scope.
        assert out[7]["brier"] == pytest.approx(0.2)

    def test_legacy_none_path_untouched(self, mock_engine):
        from oracle.calibration import compute_per_horizon_calibration

        mock_conn = mock_engine.connect.return_value.__enter__.return_value
        result = MagicMock()
        # 1-element tuple (horizon_buckets only) — legacy path
        result.fetchone.return_value = (
            {
                "7d": {"brier": 0.2, "ece": 0.1, "scored": 5,
                       "weight": 1.4, "hits": 3, "misses": 2,
                       "partials": 0},
            },
        )
        mock_conn.execute.return_value = result

        out = compute_per_horizon_calibration(mock_engine, "flow_momentum")
        assert out[7]["weight"] == pytest.approx(1.4)


# ──────────────────────────────────────────────────────────────────────────
# 7. Migration 0045 — static SQL assertions
# ──────────────────────────────────────────────────────────────────────────


_MIGRATION = (
    Path(__file__).resolve().parent.parent
    / "migrations" / "0045_oracle_regime_buckets.sql"
)


class TestSchemaMigration:
    def test_migration_file_exists(self):
        assert _MIGRATION.exists(), f"{_MIGRATION} missing"

    def test_migration_is_idempotent(self):
        sql = _MIGRATION.read_text()
        assert "ADD COLUMN IF NOT EXISTS regime_buckets JSONB" in sql
        assert "CREATE INDEX IF NOT EXISTS" in sql

    def test_migration_has_gin_index(self):
        sql = _MIGRATION.read_text()
        assert "USING GIN" in sql
        assert "jsonb_path_ops" in sql

    def test_migration_has_grant_footer(self):
        sql = _MIGRATION.read_text()
        assert "GRANT ALL ON oracle_models TO grid" in sql

    def test_migration_seeds_five_states(self):
        sql = _MIGRATION.read_text()
        for state in (
            "'CRISIS'", "'TIGHTENING'", "'NEUTRAL'",
            "'EXPANSION'", "'EXPANSION_STRONG'",
        ):
            assert state in sql, f"missing {state} seed"
