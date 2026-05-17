"""Tests for intelligence.signal_cooccurrence — pairwise lift tracker."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime
from typing import Any

import pytest

from intelligence.signal_cooccurrence import (
    COOCCURRENCE_MIN_SHAPLEY,
    MAX_LIFT_MULTIPLIER,
    MIN_COOCCURRENCE_SAMPLES,
    MIN_LIFT_MULTIPLIER,
    CooccurrenceStats,
    SignalPair,
    _extract_signal_contributions,
    bootstrap_from_oracle_predictions,
    canonical_pair,
    compute_independence_baseline,
    compute_lift,
    compute_pair_lift_multiplier,
    ensure_cooccurrence_table,
    get_cooccurrence_stats,
    get_firing_signals,
    get_lift_multiplier,
    get_stats_for_signal,
    record_joint_prediction,
)


# ── Fake in-memory engine ─────────────────────────────────────────────────


class _FakeRow:
    """Mimics SQLAlchemy Row with ._mapping dict access."""

    def __init__(self, mapping: dict[str, Any]) -> None:
        self._mapping = dict(mapping)

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, int):
            return list(self._mapping.values())[key]
        return self._mapping[key]


class _FakeResult:
    def __init__(self, rows: list[_FakeRow]) -> None:
        self._rows = rows

    def fetchone(self) -> _FakeRow | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[_FakeRow]:
        return list(self._rows)


class _FakeConn:
    def __init__(self, engine: "FakeEngine") -> None:
        self.engine = engine

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *args: Any) -> None:
        pass

    def execute(self, sql: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        params = params or {}
        text_sql = str(getattr(sql, "text", sql))
        return self.engine._dispatch(text_sql, params)


class FakeEngine:
    """In-memory stand-in for a SQLAlchemy Engine.

    Implements just enough of the Engine surface to make
    ``signal_cooccurrence_history`` round-trip without a database.
    """

    def __init__(self, should_fail: bool = False) -> None:
        # Keyed by (signal_a, signal_b)
        self.pairs: dict[tuple[str, str], dict[str, Any]] = {}
        self.table_created: bool = False
        self.should_fail: bool = should_fail
        # For bootstrap replay tests
        self.oracle_rows: list[dict[str, Any]] = []

    def connect(self) -> _FakeConn:
        if self.should_fail:
            raise RuntimeError("fake engine failure")
        return _FakeConn(self)

    def begin(self) -> _FakeConn:
        if self.should_fail:
            raise RuntimeError("fake engine failure")
        return _FakeConn(self)

    # ── SQL dispatch ──────────────────────────────────────────────────

    def _dispatch(self, sql: str, params: dict[str, Any]) -> _FakeResult:
        sql_upper = sql.upper()
        if "CREATE TABLE" in sql_upper:
            self.table_created = True
            return _FakeResult([])
        if "CREATE INDEX" in sql_upper:
            return _FakeResult([])
        if "INSERT INTO SIGNAL_COOCCURRENCE_HISTORY" in sql_upper:
            self._upsert(params)
            return _FakeResult([])
        if "FROM SIGNAL_COOCCURRENCE_HISTORY" in sql_upper and "WHERE SIGNAL_A = :A AND" in sql_upper:
            key = (params["a"], params["b"])
            row = self.pairs.get(key)
            return _FakeResult([_FakeRow(row)] if row else [])
        if "FROM SIGNAL_COOCCURRENCE_HISTORY" in sql_upper and ":S" in sql_upper:
            s = params["s"]
            rows = [
                _FakeRow(data)
                for key, data in self.pairs.items()
                if s in key
            ]
            return _FakeResult(rows)
        if "FROM ORACLE_PREDICTIONS" in sql_upper:
            return _FakeResult([_FakeRow(r) for r in self.oracle_rows])
        return _FakeResult([])

    def _upsert(self, params: dict[str, Any]) -> None:
        key = (params["signal_a"], params["signal_b"])
        hit = int(params["hit"])
        miss = int(params["miss"])
        if key in self.pairs:
            row = self.pairs[key]
            row["sample_count"] += 1
            row["joint_hits"] += hit
            row["joint_misses"] += miss
            row["marginal_hits_a"] += hit
            row["marginal_hits_b"] += hit
            row["marginal_fires_a"] += 1
            row["marginal_fires_b"] += 1
            row["last_updated"] = datetime.utcnow()
        else:
            self.pairs[key] = {
                "signal_a": params["signal_a"],
                "signal_b": params["signal_b"],
                "sample_count": 1,
                "joint_hits": hit,
                "joint_misses": miss,
                "marginal_hits_a": hit,
                "marginal_hits_b": hit,
                "marginal_fires_a": 1,
                "marginal_fires_b": 1,
                "last_updated": datetime.utcnow(),
            }


def _saturate_pair(
    engine: FakeEngine,
    a: str,
    b: str,
    hits: int,
    total: int,
) -> None:
    """Drive a pair up to ``total`` samples with ``hits`` hits."""

    for i in range(total):
        outcome = 1.0 if i < hits else 0.0
        record_joint_prediction(
            engine,
            outcome=outcome,
            signal_contributions={a: 0.4, b: 0.4},
        )


# ── canonical_pair ────────────────────────────────────────────────────────


class TestCanonicalPair:
    def test_ordered_input_preserved(self) -> None:
        pair = canonical_pair("alpha", "beta")
        assert pair == SignalPair(signal_a="alpha", signal_b="beta")

    def test_reversed_input_collapsed(self) -> None:
        pair = canonical_pair("beta", "alpha")
        assert pair == SignalPair(signal_a="alpha", signal_b="beta")

    def test_self_pair(self) -> None:
        pair = canonical_pair("same", "same")
        assert pair.signal_a == "same"
        assert pair.signal_b == "same"

    def test_collapse_equality(self) -> None:
        assert canonical_pair("x", "y") == canonical_pair("y", "x")


# ── compute_independence_baseline ─────────────────────────────────────────


class TestIndependenceBaseline:
    def test_happy(self) -> None:
        assert compute_independence_baseline(0.5, 0.4) == pytest.approx(0.2)

    def test_zero_marginal(self) -> None:
        assert compute_independence_baseline(0.0, 0.5) == 0.0
        assert compute_independence_baseline(0.5, 0.0) == 0.0

    def test_negative_clamped(self) -> None:
        # Defensive: negative marginals treated as 0
        assert compute_independence_baseline(-0.1, 0.5) == 0.0


# ── compute_lift ──────────────────────────────────────────────────────────


class TestComputeLift:
    def test_compounding(self) -> None:
        # joint 0.5 vs baseline 0.25 -> raw 2.0 -> clamped to MAX
        assert compute_lift(0.5, 0.25) == MAX_LIFT_MULTIPLIER

    def test_mild_compounding_within_clamp(self) -> None:
        # 0.5 / 0.45 = ~1.111 within [0.75, 1.25]
        lift = compute_lift(0.5, 0.45)
        assert 1.0 < lift < MAX_LIFT_MULTIPLIER

    def test_redundant(self) -> None:
        # joint 0.1 vs baseline 0.25 -> raw 0.4 -> clamped to MIN
        assert compute_lift(0.1, 0.25) == MIN_LIFT_MULTIPLIER

    def test_zero_baseline_defensive(self) -> None:
        assert compute_lift(0.5, 0.0) == MAX_LIFT_MULTIPLIER

    def test_clamp_lower_bound(self) -> None:
        assert compute_lift(0.0, 0.5) == MIN_LIFT_MULTIPLIER

    def test_clamp_upper_bound(self) -> None:
        assert compute_lift(0.99, 0.01) == MAX_LIFT_MULTIPLIER


# ── get_firing_signals ────────────────────────────────────────────────────


class TestGetFiringSignals:
    def test_empty(self) -> None:
        assert get_firing_signals({}) == set()

    def test_filters_weak(self) -> None:
        result = get_firing_signals({"strong": 0.4, "weak": 0.05})
        assert result == {"strong"}

    def test_includes_all_strong(self) -> None:
        result = get_firing_signals(
            {"a": 0.4, "b": 0.3, "c": COOCCURRENCE_MIN_SHAPLEY}
        )
        assert result == {"a", "b", "c"}

    def test_ignores_none_values(self) -> None:
        result = get_firing_signals({"a": 0.5, "b": None})  # type: ignore[dict-item]
        assert result == {"a"}


# ── compute_pair_lift_multiplier ─────────────────────────────────────────


def _calibrated_stats(
    a: str, b: str, lift: float
) -> CooccurrenceStats:
    return CooccurrenceStats(
        pair=canonical_pair(a, b),
        sample_count=MIN_COOCCURRENCE_SAMPLES + 5,
        joint_hits=10,
        joint_misses=5,
        joint_hit_rate=0.667,
        marginal_hit_rate_a=0.5,
        marginal_hit_rate_b=0.5,
        independence_baseline=0.25,
        lift=lift,
        is_calibrated=True,
        last_updated=None,
    )


def _uncalibrated_stats(a: str, b: str) -> CooccurrenceStats:
    return CooccurrenceStats(
        pair=canonical_pair(a, b),
        sample_count=1,
        joint_hits=1,
        joint_misses=0,
        joint_hit_rate=1.0,
        marginal_hit_rate_a=1.0,
        marginal_hit_rate_b=1.0,
        independence_baseline=1.0,
        lift=1.0,
        is_calibrated=False,
        last_updated=None,
    )


class TestComputePairLiftMultiplier:
    def test_empty_firing_set(self) -> None:
        assert compute_pair_lift_multiplier(set(), {}) == 1.0

    def test_single_signal(self) -> None:
        assert compute_pair_lift_multiplier({"only"}, {}) == 1.0

    def test_two_signals_one_calibrated_pair(self) -> None:
        stats = _calibrated_stats("a", "b", 1.15)
        lookup = {stats.pair: stats}
        result = compute_pair_lift_multiplier({"a", "b"}, lookup)
        assert result == pytest.approx(1.15)

    def test_three_signals_mixed_lifts(self) -> None:
        stats_ab = _calibrated_stats("a", "b", 1.20)
        stats_ac = _calibrated_stats("a", "c", 0.80)
        stats_bc = _calibrated_stats("b", "c", 1.00)
        lookup = {
            stats_ab.pair: stats_ab,
            stats_ac.pair: stats_ac,
            stats_bc.pair: stats_bc,
        }
        result = compute_pair_lift_multiplier({"a", "b", "c"}, lookup)
        assert result == pytest.approx((1.20 + 0.80 + 1.00) / 3)

    def test_uncalibrated_pairs_skipped(self) -> None:
        stats_ab = _calibrated_stats("a", "b", 1.10)
        stats_ac = _uncalibrated_stats("a", "c")
        lookup = {stats_ab.pair: stats_ab, stats_ac.pair: stats_ac}
        result = compute_pair_lift_multiplier({"a", "b", "c"}, lookup)
        assert result == pytest.approx(1.10)

    def test_no_calibrated_pairs_returns_neutral(self) -> None:
        stats = _uncalibrated_stats("a", "b")
        result = compute_pair_lift_multiplier({"a", "b"}, {stats.pair: stats})
        assert result == 1.0


# ── Dataclass immutability ───────────────────────────────────────────────


class TestDataclasses:
    def test_signal_pair_frozen(self) -> None:
        pair = canonical_pair("a", "b")
        with pytest.raises(FrozenInstanceError):
            pair.signal_a = "mutated"  # type: ignore[misc]

    def test_cooccurrence_stats_frozen(self) -> None:
        stats = _calibrated_stats("a", "b", 1.1)
        with pytest.raises(FrozenInstanceError):
            stats.sample_count = 999  # type: ignore[misc]

    def test_to_dict_roundtrip(self) -> None:
        stats = _calibrated_stats("a", "b", 1.1)
        d = stats.to_dict()
        assert d["pair"] == {"signal_a": "a", "signal_b": "b"}
        assert d["sample_count"] == MIN_COOCCURRENCE_SAMPLES + 5
        assert d["lift"] == pytest.approx(1.1)
        assert d["is_calibrated"] is True
        assert d["last_updated"] is None


# ── record_joint_prediction ──────────────────────────────────────────────


class TestRecordJointPrediction:
    def test_first_insert_hit(self) -> None:
        engine = FakeEngine()
        touched = record_joint_prediction(
            engine,
            outcome=1.0,
            signal_contributions={"alpha": 0.4, "beta": 0.4},
        )
        assert touched == 1
        row = engine.pairs[("alpha", "beta")]
        assert row["sample_count"] == 1
        assert row["joint_hits"] == 1
        assert row["joint_misses"] == 0

    def test_first_insert_miss(self) -> None:
        engine = FakeEngine()
        record_joint_prediction(
            engine,
            outcome=0.0,
            signal_contributions={"alpha": 0.4, "beta": 0.4},
        )
        row = engine.pairs[("alpha", "beta")]
        assert row["joint_hits"] == 0
        assert row["joint_misses"] == 1

    def test_welford_update_second_call(self) -> None:
        engine = FakeEngine()
        record_joint_prediction(
            engine,
            outcome=1.0,
            signal_contributions={"alpha": 0.4, "beta": 0.4},
        )
        record_joint_prediction(
            engine,
            outcome=0.0,
            signal_contributions={"alpha": 0.4, "beta": 0.4},
        )
        row = engine.pairs[("alpha", "beta")]
        assert row["sample_count"] == 2
        assert row["joint_hits"] == 1
        assert row["joint_misses"] == 1

    def test_three_signals_three_pairs(self) -> None:
        engine = FakeEngine()
        touched = record_joint_prediction(
            engine,
            outcome=1.0,
            signal_contributions={"a": 0.3, "b": 0.3, "c": 0.3},
        )
        assert touched == 3
        assert set(engine.pairs.keys()) == {
            ("a", "b"),
            ("a", "c"),
            ("b", "c"),
        }

    def test_filters_below_min_shapley(self) -> None:
        engine = FakeEngine()
        touched = record_joint_prediction(
            engine,
            outcome=1.0,
            signal_contributions={"strong": 0.5, "weak": 0.05},
        )
        # only one signal passes the floor -> no pairs
        assert touched == 0
        assert engine.pairs == {}

    def test_canonicalizes_insert(self) -> None:
        engine = FakeEngine()
        # Pass in reversed lexical order; should still land in (a, b)
        record_joint_prediction(
            engine,
            outcome=1.0,
            signal_contributions={"zebra": 0.4, "alpha": 0.4},
        )
        assert ("alpha", "zebra") in engine.pairs
        assert ("zebra", "alpha") not in engine.pairs

    def test_db_error_returns_zero(self) -> None:
        engine = FakeEngine(should_fail=True)
        touched = record_joint_prediction(
            engine,
            outcome=1.0,
            signal_contributions={"a": 0.4, "b": 0.4},
        )
        assert touched == 0


# ── get_cooccurrence_stats / get_stats_for_signal ────────────────────────


class TestReadPath:
    def test_missing_pair_returns_none(self) -> None:
        engine = FakeEngine()
        assert get_cooccurrence_stats(engine, "a", "b") is None

    def test_returns_canonical_pair_either_direction(self) -> None:
        engine = FakeEngine()
        record_joint_prediction(
            engine,
            outcome=1.0,
            signal_contributions={"a": 0.4, "b": 0.4},
        )
        stats = get_cooccurrence_stats(engine, "b", "a")
        assert stats is not None
        assert stats.pair == canonical_pair("a", "b")
        assert stats.sample_count == 1

    def test_get_stats_for_signal_both_positions(self) -> None:
        engine = FakeEngine()
        record_joint_prediction(
            engine,
            outcome=1.0,
            signal_contributions={"a": 0.4, "b": 0.4},
        )
        record_joint_prediction(
            engine,
            outcome=1.0,
            signal_contributions={"b": 0.4, "c": 0.4},
        )
        results = get_stats_for_signal(engine, "b")
        pairs = {s.pair.to_tuple() for s in results}
        assert pairs == {("a", "b"), ("b", "c")}

    def test_calibration_flag_low_samples(self) -> None:
        engine = FakeEngine()
        record_joint_prediction(
            engine,
            outcome=1.0,
            signal_contributions={"a": 0.4, "b": 0.4},
        )
        stats = get_cooccurrence_stats(engine, "a", "b")
        assert stats is not None
        assert stats.is_calibrated is False

    def test_calibration_flag_when_saturated(self) -> None:
        engine = FakeEngine()
        _saturate_pair(engine, "a", "b", hits=12, total=MIN_COOCCURRENCE_SAMPLES + 3)
        stats = get_cooccurrence_stats(engine, "a", "b")
        assert stats is not None
        assert stats.is_calibrated is True


# ── get_lift_multiplier ──────────────────────────────────────────────────


class TestGetLiftMultiplier:
    def test_no_history_returns_neutral(self) -> None:
        engine = FakeEngine()
        result = get_lift_multiplier(
            engine, {"a": 0.4, "b": 0.4}
        )
        assert result == 1.0

    def test_single_firing_signal_returns_neutral(self) -> None:
        engine = FakeEngine()
        result = get_lift_multiplier(engine, {"a": 0.5, "b": 0.05})
        assert result == 1.0

    def test_compounding_pair(self) -> None:
        engine = FakeEngine()
        # Pair (a, b) hits frequently together (joint rate 0.8)
        _saturate_pair(engine, "a", "b", hits=16, total=20)
        # Signal a and b both have LOW marginals elsewhere
        # (pair (a, x) and (b, y) hit rarely).
        _saturate_pair(engine, "a", "x", hits=3, total=20)
        _saturate_pair(engine, "b", "y", hits=3, total=20)
        result = get_lift_multiplier(engine, {"a": 0.4, "b": 0.4})
        # Joint 0.8 vs marginal product ~0.5*0.5=0.25 -> clamps up.
        assert MIN_LIFT_MULTIPLIER <= result <= MAX_LIFT_MULTIPLIER
        assert result > 1.0

    def test_redundant_pair(self) -> None:
        engine = FakeEngine()
        # Joint pair (a, b) has a LOW joint rate
        _saturate_pair(engine, "a", "b", hits=4, total=20)
        # But signal a and b both have HIGH marginals elsewhere
        _saturate_pair(engine, "a", "x", hits=18, total=20)
        _saturate_pair(engine, "b", "y", hits=18, total=20)
        result = get_lift_multiplier(engine, {"a": 0.4, "b": 0.4})
        # Joint 0.2 vs marginal product ~0.55*0.55=0.3 -> below 1
        assert result < 1.0
        assert result >= MIN_LIFT_MULTIPLIER

    def test_lift_at_lift_1_20(self) -> None:
        """Directly test the 'get_lift_multiplier at lift 1.20' case
        via a controlled setup where we know the pair and its lift."""

        engine = FakeEngine()
        # Joint pair 12/15 = 0.8. Marginal a across all its rows = 0.8
        # / marginal b across all its rows = 0.8 -> baseline 0.64
        # raw lift = 0.8 / 0.64 = 1.25 -> clamps to MAX_LIFT_MULTIPLIER.
        _saturate_pair(engine, "a", "b", hits=12, total=15)
        result = get_lift_multiplier(engine, {"a": 0.4, "b": 0.4})
        assert 1.0 < result <= MAX_LIFT_MULTIPLIER

    def test_db_failure_returns_neutral(self) -> None:
        engine = FakeEngine(should_fail=True)
        result = get_lift_multiplier(
            engine, {"a": 0.4, "b": 0.4}
        )
        assert result == 1.0


# ── ensure_cooccurrence_table ────────────────────────────────────────────


class TestEnsureTable:
    def test_ensure_runs_ddl(self) -> None:
        engine = FakeEngine()
        ensure_cooccurrence_table(engine)
        assert engine.table_created is True

    def test_ensure_swallows_errors(self) -> None:
        engine = FakeEngine(should_fail=True)
        # Must not raise
        ensure_cooccurrence_table(engine)


# ── bootstrap_from_oracle_predictions ────────────────────────────────────


class TestBootstrapFromOraclePredictions:
    def test_walks_mocked_engine(self) -> None:
        engine = FakeEngine()
        engine.oracle_rows = [
            {
                "id": "p1",
                "verdict": "hit",
                "signals": {"alpha_signal": 0.4, "beta_signal": 0.4},
                "signal_strength": 0.8,
                "confidence": 0.7,
                "model_weights": None,
            },
            {
                "id": "p2",
                "verdict": "miss",
                "signals": {"alpha_signal": 0.4, "beta_signal": 0.4},
                "signal_strength": 0.8,
                "confidence": 0.6,
                "model_weights": None,
            },
            {
                "id": "p3",
                "verdict": "partial",
                "signals": ["alpha_signal", "gamma_signal"],
                "signal_strength": 0.6,
                "confidence": 0.5,
                "model_weights": None,
            },
        ]
        summary = bootstrap_from_oracle_predictions(engine, days=30)
        assert summary["rows_scanned"] == 3
        assert summary["rows_used"] >= 2
        assert summary["pairs_updated"] > 0
        assert summary["errors"] == 0
        # (alpha_signal, beta_signal) pair should exist with 2 samples
        pair_row = engine.pairs.get(("alpha_signal", "beta_signal"))
        assert pair_row is not None
        assert pair_row["sample_count"] == 2

    def test_skips_rows_with_single_signal(self) -> None:
        engine = FakeEngine()
        engine.oracle_rows = [
            {
                "id": "p1",
                "verdict": "hit",
                "signals": {"alpha_signal": 0.9},
                "signal_strength": 0.9,
                "confidence": 0.9,
                "model_weights": None,
            },
        ]
        summary = bootstrap_from_oracle_predictions(engine)
        assert summary["rows_scanned"] == 1
        assert summary["rows_used"] == 0
        assert summary["pairs_updated"] == 0

    def test_empty_history(self) -> None:
        engine = FakeEngine()
        summary = bootstrap_from_oracle_predictions(engine)
        assert summary["rows_scanned"] == 0
        assert summary["pairs_updated"] == 0


# ── _extract_signal_contributions: dict-list pollution regression ─────────


class TestExtractSignalContributionsListHandling:
    """The 2026-05-17 audit found all 410 rows of
    ``signal_cooccurrence_history`` were stringified-dict pairs like
    ``{'name': 'astrogrid_grid', 'detail': 'leader:GOOGL'}``. The bug was
    ``str(s)`` on a dict entry from ``signals`` lists emitted by
    ``oracle/publish.py:publish_astrogrid_prediction``. The cooccurrence_
    lift adjuster matched zero pairs against real predictions as a result.
    """

    def _make_row(self, signals_field):
        # Minimal duck-typed prediction row.
        return {
            "signals": signals_field,
            "signal_strength": 0.5,
            "model_weights": None,
        }

    def test_list_of_dicts_extracts_name(self):
        # astrogrid_grid / astrogrid_mystical emit dict entries
        row = self._make_row([
            {"name": "astrogrid_grid", "detail": "leader:GOOGL / laggard:AAPL"},
            {"name": "astrogrid_mystical", "detail": "moon:Quarter"},
        ])
        contributions = _extract_signal_contributions(row)
        assert set(contributions.keys()) == {"astrogrid_grid", "astrogrid_mystical"}
        # No stringified-dict keys
        for k in contributions:
            assert not k.startswith("{")

    def test_list_of_strings_still_works(self):
        row = self._make_row(["sentiment", "equity", "vol"])
        contributions = _extract_signal_contributions(row)
        assert set(contributions.keys()) == {"sentiment", "equity", "vol"}

    def test_list_with_dict_signal_field_falls_back(self):
        # Dict entries with `signal` instead of `name` are still extracted.
        row = self._make_row([
            {"signal": "vol", "detail": "x"},
            {"id": "rates", "weight": 1.0},
        ])
        contributions = _extract_signal_contributions(row)
        assert set(contributions.keys()) == {"vol", "rates"}

    def test_list_with_invalid_dicts_skips_not_pollutes(self):
        # Dict entries without name/signal/id are dropped, not stringified.
        row = self._make_row([
            {"foo": "bar", "baz": 1},  # no name/signal/id
            {"name": "good_signal"},
        ])
        contributions = _extract_signal_contributions(row)
        assert set(contributions.keys()) == {"good_signal"}

    def test_mixed_string_and_dict_entries(self):
        row = self._make_row([
            "macro",
            {"name": "astrogrid_grid", "detail": "x"},
            None,  # skipped
            "",    # skipped
        ])
        contributions = _extract_signal_contributions(row)
        assert set(contributions.keys()) == {"macro", "astrogrid_grid"}
