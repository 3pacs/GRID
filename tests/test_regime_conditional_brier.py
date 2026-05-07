"""Tests for features/regime_conditional_brier.py.

The pure functions (``_canonical_regime``, the import re-exports, the
scorecard reconstruction) are tested directly. The DB-touching paths
use an in-memory ``_FakeEngine`` modeled after
``tests/test_per_signal_brier.py`` — stores rows in a dict keyed on
``(signal_source, horizon_days, regime)`` and recognizes the exact
SELECT / INSERT / UPDATE shapes emitted by
``features/regime_conditional_brier.py``.

The fallback semantics (``get_scorecard_with_regime_fallback``) are the
non-negotiable test surface — if this file passes, the production
consumer never has to worry about whether a particular regime bucket is
warm or cold.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


from features.per_signal_brier import (
    MIN_CALIBRATED_SAMPLES,
    SignalScorecard,
)
from features.regime_conditional_brier import (
    CANONICAL_REGIMES,
    MIN_REGIME_SAMPLES,
    _canonical_regime,
    _row_to_scorecard,
    bootstrap_from_oracle_predictions,
    ensure_regime_brier_table,
    get_regime_conditional_scorecard,
    get_scorecard_with_regime_fallback,
    rank_signals_by_regime,
    record_scored_prediction,
)


# ── _canonical_regime ────────────────────────────────────────────────────


class TestCanonicalRegime:
    def test_lowercase_expansion(self):
        assert _canonical_regime("expansion") == "EXPANSION"

    def test_whitespace_stripped(self):
        assert _canonical_regime("  CRISIS  ") == "CRISIS"

    def test_unknown_falls_back_to_neutral(self):
        assert _canonical_regime("unknown_regime") == "NEUTRAL"

    def test_none_falls_back_to_neutral(self):
        assert _canonical_regime(None) == "NEUTRAL"

    def test_empty_string_falls_back_to_neutral(self):
        assert _canonical_regime("") == "NEUTRAL"
        assert _canonical_regime("   ") == "NEUTRAL"

    def test_all_canonical_roundtrip(self):
        for reg in CANONICAL_REGIMES:
            assert _canonical_regime(reg) == reg
            assert _canonical_regime(reg.lower()) == reg

    def test_alias_strong_expansion(self):
        assert _canonical_regime("strong_expansion") == "EXPANSION_STRONG"
        assert _canonical_regime("EXPANSIONSTRONG") == "EXPANSION_STRONG"

    def test_alias_tightening_abbrev(self):
        assert _canonical_regime("tight") == "TIGHTENING"

    def test_hyphen_normalization(self):
        assert _canonical_regime("expansion-strong") == "EXPANSION_STRONG"


# ── CANONICAL_REGIMES constant ───────────────────────────────────────────


class TestCanonicalRegimesConstant:
    def test_exact_tuple(self):
        assert CANONICAL_REGIMES == (
            "CRISIS",
            "TIGHTENING",
            "NEUTRAL",
            "EXPANSION",
            "EXPANSION_STRONG",
        )

    def test_length_five(self):
        assert len(CANONICAL_REGIMES) == 5


# ── FakeEngine (in-memory) ───────────────────────────────────────────────


class _FakeEngine:
    """In-memory stand-in for a SQLAlchemy Engine.

    Stores rows in a dict keyed on ``(signal_source, horizon_days,
    regime)``. Recognizes the exact SELECT / INSERT / UPDATE shapes
    emitted by ``features/regime_conditional_brier.py``. Also handles
    the bootstrap SELECT against ``oracle_predictions`` via an optional
    ``oracle_rows`` attribute.
    """

    def __init__(self) -> None:
        self.store: dict[tuple[str, int, str], dict] = {}
        self._ddl_seen: list[str] = []
        self.oracle_rows: list[tuple] = []

    def begin(self) -> "_FakeEngine._Ctx":
        return _FakeEngine._Ctx(self)

    def connect(self) -> "_FakeEngine._Ctx":
        return _FakeEngine._Ctx(self)

    class _Ctx:
        def __init__(self, parent: "_FakeEngine") -> None:
            self.parent = parent

        def __enter__(self) -> "_FakeEngine._Ctx":
            return self

        def __exit__(self, *args) -> bool:
            return False

        def execute(self, query, params=None):
            sql = str(query).strip()
            params = params or {}

            # DDL — just record it so tests can assert idempotency.
            if sql.startswith("CREATE TABLE") or sql.startswith("CREATE INDEX"):
                self.parent._ddl_seen.append(sql)
                return MagicMock()

            # Bootstrap read against oracle_predictions.
            if sql.startswith("SELECT id, created_at, expiry, confidence"):
                return _FakeResult(fetchall_value=list(self.parent.oracle_rows))

            # Read: rank_signals_by_regime (regime filter).
            if (
                sql.startswith(
                    "SELECT signal_source, horizon_days, scored_count"
                )
                and "regime = :r" in sql
                and "horizon_days = :h" in sql
                and "scored_count >=" in sql
            ):
                r = params["r"]
                h = int(params["h"])
                n = int(params["n"])
                rows = [
                    (
                        v["signal_source"],
                        v["horizon_days"],
                        v["scored_count"],
                        v["running_brier"],
                        v["running_ece"],
                        v["hit_count"],
                        v["last_updated"],
                    )
                    for k, v in self.parent.store.items()
                    if k[1] == h and k[2] == r and v["scored_count"] >= n
                ]
                rows.sort(key=lambda r_: r_[3])
                return _FakeResult(fetchall_value=rows)

            # Read: get_regime_conditional_scorecard (single row).
            if (
                sql.startswith(
                    "SELECT scored_count, running_brier, running_ece"
                )
                and "regime = :r" in sql
            ):
                key = (params["s"], int(params["h"]), params["r"])
                row = self.parent.store.get(key)
                if row is None:
                    return _FakeResult(fetchone_value=None)
                return _FakeResult(
                    fetchone_value=(
                        row["scored_count"],
                        row["running_brier"],
                        row["running_ece"],
                        row["hit_count"],
                        row["last_updated"],
                    )
                )

            # Update-path lookup inside record_scored_prediction.
            if sql.startswith("SELECT scored_count, running_brier, running_ece, hit_count"):
                key = (params["s"], int(params["h"]), params["r"])
                row = self.parent.store.get(key)
                if row is None:
                    return _FakeResult(fetchone_value=None)
                return _FakeResult(
                    fetchone_value=(
                        row["scored_count"],
                        row["running_brier"],
                        row["running_ece"],
                        row["hit_count"],
                    )
                )

            # INSERT.
            if sql.startswith("INSERT INTO regime_conditional_brier_history"):
                key = (params["s"], int(params["h"]), params["r"])
                if key in self.parent.store:
                    return MagicMock()
                self.parent.store[key] = {
                    "signal_source": params["s"],
                    "horizon_days": int(params["h"]),
                    "regime": params["r"],
                    "scored_count": 1,
                    "running_brier": float(params["b"]),
                    "running_ece": float(params["e"]),
                    "hit_count": int(params["hit"]),
                    "last_updated": datetime.now(timezone.utc),
                }
                return MagicMock()

            # UPDATE.
            if sql.startswith("UPDATE regime_conditional_brier_history"):
                key = (params["s"], int(params["hz"]), params["r"])
                if key not in self.parent.store:
                    return MagicMock()
                self.parent.store[key].update(
                    {
                        "scored_count": int(params["n"]),
                        "running_brier": float(params["b"]),
                        "running_ece": float(params["e"]),
                        "hit_count": int(params["h"]),
                        "last_updated": datetime.now(timezone.utc),
                    }
                )
                return MagicMock()

            return MagicMock()


class _FakeResult:
    def __init__(self, fetchone_value=None, fetchall_value=None):
        self._one = fetchone_value
        self._all = fetchall_value or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


# ── ensure_regime_brier_table ────────────────────────────────────────────


class TestEnsureTable:
    def test_idempotent_creation(self):
        engine = _FakeEngine()
        ensure_regime_brier_table(engine)
        first = len(engine._ddl_seen)
        assert first >= 1  # at least the CREATE TABLE statement
        ensure_regime_brier_table(engine)
        second = len(engine._ddl_seen)
        # Idempotent — each call runs the same DDL again, but the store
        # state is unchanged.
        assert second == 2 * first
        assert engine.store == {}

    def test_does_not_raise_on_db_error(self):
        class BrokenEngine:
            def begin(self):
                raise RuntimeError("db down")

        # Should swallow the error per the module's "never raise" rule.
        ensure_regime_brier_table(BrokenEngine())  # no assertion — just survive


# ── record_scored_prediction ─────────────────────────────────────────────


class TestRecordScoredPrediction:
    def test_first_insert(self):
        engine = _FakeEngine()
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"flow_momentum": 1.0},
            regime="EXPANSION",
        )
        assert "flow_momentum" in updates
        assert updates["flow_momentum"]["scored_count"] == 1
        assert updates["flow_momentum"]["regime"] == "EXPANSION"
        # squared error = (0.7 - 1.0)^2 = 0.09
        assert abs(updates["flow_momentum"]["running_brier"] - 0.09) < 1e-9
        assert ("flow_momentum", 7, "EXPANSION") in engine.store

    def test_welford_update_same_bucket(self):
        engine = _FakeEngine()
        # First: err = 0.09
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"flow_momentum": 1.0},
            regime="EXPANSION",
        )
        # Second: err = 0.64
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.8,
            outcome=0.0,
            signal_contributions={"flow_momentum": 1.0},
            regime="EXPANSION",
        )
        row = engine.store[("flow_momentum", 7, "EXPANSION")]
        assert row["scored_count"] == 2
        # Running mean of {0.09, 0.64} = 0.365
        assert abs(row["running_brier"] - 0.365) < 1e-6

    def test_different_regimes_are_separate_rows(self):
        engine = _FakeEngine()
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.9,
            outcome=1.0,
            signal_contributions={"flow_momentum": 1.0},
            regime="EXPANSION",
        )
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.9,
            outcome=0.0,
            signal_contributions={"flow_momentum": 1.0},
            regime="CRISIS",
        )
        assert ("flow_momentum", 7, "EXPANSION") in engine.store
        assert ("flow_momentum", 7, "CRISIS") in engine.store
        # They must be distinct — EXPANSION had outcome=1 (hit) with
        # conf=0.9, so brier=(0.9-1.0)^2 = 0.01; CRISIS had outcome=0
        # with conf=0.9, so brier=(0.9-0.0)^2 = 0.81.
        expansion = engine.store[("flow_momentum", 7, "EXPANSION")]
        crisis = engine.store[("flow_momentum", 7, "CRISIS")]
        assert abs(expansion["running_brier"] - 0.01) < 1e-9
        assert abs(crisis["running_brier"] - 0.81) < 1e-9

    def test_zero_weight_skipped(self):
        engine = _FakeEngine()
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"flow_momentum": 0.0, "jodi_oil": 1.0},
            regime="EXPANSION",
        )
        assert "flow_momentum" not in updates
        assert "jodi_oil" in updates
        assert ("flow_momentum", 7, "EXPANSION") not in engine.store
        assert ("jodi_oil", 7, "EXPANSION") in engine.store

    def test_empty_contributions_noop(self):
        engine = _FakeEngine()
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={},
            regime="EXPANSION",
        )
        assert updates == {}
        assert engine.store == {}

    def test_unknown_regime_normalized_to_neutral(self):
        engine = _FakeEngine()
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"flow_momentum": 1.0},
            regime="mystery_state",
        )
        assert ("flow_momentum", 7, "NEUTRAL") in engine.store

    def test_db_error_returns_empty_dict(self):
        class BrokenEngine:
            def begin(self):
                raise RuntimeError("connection lost")

            def connect(self):  # used by ensure_tables path
                raise RuntimeError("connection lost")

        # ensure_regime_brier_table swallows its exception, but then
        # the record path tries to open a transaction and fails. The
        # outer try/except must catch it and return {}.
        result = record_scored_prediction(
            BrokenEngine(),
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"flow_momentum": 1.0},
            regime="EXPANSION",
        )
        assert result == {}


# ── get_regime_conditional_scorecard ─────────────────────────────────────


class TestGetRegimeConditionalScorecard:
    def test_missing_row_returns_none(self):
        engine = _FakeEngine()
        card = get_regime_conditional_scorecard(
            engine, "flow_momentum", 7, "EXPANSION"
        )
        assert card is None

    def test_thin_row_returns_none(self):
        # Row exists but has fewer than MIN_REGIME_SAMPLES — we want the
        # caller to fall back, not expose the noisy bucket.
        engine = _FakeEngine()
        # Seed fewer than MIN_REGIME_SAMPLES records
        for _ in range(MIN_REGIME_SAMPLES - 1):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=1.0,
                signal_contributions={"flow_momentum": 1.0},
                regime="EXPANSION",
            )
        # Row exists:
        assert ("flow_momentum", 7, "EXPANSION") in engine.store
        # But API reports None because it's below the fallback threshold.
        assert (
            get_regime_conditional_scorecard(
                engine, "flow_momentum", 7, "EXPANSION"
            )
            is None
        )

    def test_calibrated_row_returns_scorecard(self):
        engine = _FakeEngine()
        # Seed enough samples to pass BOTH MIN_REGIME_SAMPLES AND
        # MIN_CALIBRATED_SAMPLES so conviction_weight is populated from
        # the compute_conviction_weight curve.
        for _ in range(MIN_CALIBRATED_SAMPLES):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=1.0,
                signal_contributions={"flow_momentum": 1.0},
                regime="EXPANSION",
            )
        card = get_regime_conditional_scorecard(
            engine, "flow_momentum", 7, "EXPANSION"
        )
        assert card is not None
        assert isinstance(card, SignalScorecard)
        assert card.scored_count == MIN_CALIBRATED_SAMPLES
        assert card.is_calibrated is True
        # Brier ≈ 0.01 → top of the conviction curve → 1.5
        assert card.conviction_weight == 1.5
        assert card.signal_source == "flow_momentum"
        assert card.horizon_days == 7

    def test_regime_is_case_insensitive(self):
        engine = _FakeEngine()
        for _ in range(MIN_REGIME_SAMPLES):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=1.0,
                signal_contributions={"flow_momentum": 1.0},
                regime="expansion",  # lowercase
            )
        # Stored as EXPANSION
        assert ("flow_momentum", 7, "EXPANSION") in engine.store
        # Read can use any case
        card_upper = get_regime_conditional_scorecard(
            engine, "flow_momentum", 7, "EXPANSION"
        )
        card_lower = get_regime_conditional_scorecard(
            engine, "flow_momentum", 7, "expansion"
        )
        assert card_upper is not None
        assert card_lower is not None
        assert card_upper.scored_count == card_lower.scored_count


# ── get_scorecard_with_regime_fallback ───────────────────────────────────


class TestFallbackAPI:
    def _seed_regime_bucket(
        self, engine: _FakeEngine, *, source: str, regime: str, count: int
    ) -> None:
        for _ in range(count):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=1.0,
                signal_contributions={source: 1.0},
                regime=regime,
            )

    def test_warm_regime_returns_regime_scorecard(self):
        engine = _FakeEngine()
        self._seed_regime_bucket(
            engine,
            source="flow_momentum",
            regime="EXPANSION",
            count=MIN_REGIME_SAMPLES + 5,
        )
        # The fallback path should NOT delegate when the regime bucket
        # has enough samples.
        called: dict[str, int] = {"n": 0}

        def _spy(engine, source, horizon):
            called["n"] += 1
            return None

        with patch(
            "features.regime_conditional_brier.get_signal_scorecard",
            side_effect=_spy,
        ):
            card = get_scorecard_with_regime_fallback(
                engine, "flow_momentum", 7, "EXPANSION"
            )
        assert card is not None
        assert called["n"] == 0  # never fell back

    def test_thin_regime_falls_back_to_per_signal_brier(self):
        engine = _FakeEngine()
        self._seed_regime_bucket(
            engine,
            source="flow_momentum",
            regime="EXPANSION",
            count=MIN_REGIME_SAMPLES - 1,  # below threshold
        )
        sentinel = SignalScorecard(
            signal_source="flow_momentum",
            horizon_days=7,
            scored_count=99,
            running_brier=0.12,
            running_ece=0.15,
            hit_rate=0.72,
            last_updated=datetime(2026, 4, 13, tzinfo=timezone.utc),
            is_calibrated=True,
            conviction_weight=0.9,
        )
        with patch(
            "features.regime_conditional_brier.get_signal_scorecard",
            return_value=sentinel,
        ) as spy:
            card = get_scorecard_with_regime_fallback(
                engine, "flow_momentum", 7, "EXPANSION"
            )
        spy.assert_called_once_with(engine, "flow_momentum", 7)
        assert card is sentinel

    def test_regime_none_goes_directly_to_per_signal_brier(self):
        engine = _FakeEngine()
        sentinel = SignalScorecard(
            signal_source="flow_momentum",
            horizon_days=7,
            scored_count=99,
            running_brier=0.1,
            running_ece=0.12,
            hit_rate=0.8,
            last_updated=None,
            is_calibrated=True,
            conviction_weight=1.0,
        )
        with patch(
            "features.regime_conditional_brier.get_signal_scorecard",
            return_value=sentinel,
        ) as spy:
            card = get_scorecard_with_regime_fallback(
                engine, "flow_momentum", 7, None
            )
        spy.assert_called_once_with(engine, "flow_momentum", 7)
        assert card is sentinel

    def test_missing_everything_returns_none(self):
        engine = _FakeEngine()
        with patch(
            "features.regime_conditional_brier.get_signal_scorecard",
            return_value=None,
        ):
            card = get_scorecard_with_regime_fallback(
                engine, "never_seen", 7, "EXPANSION"
            )
        assert card is None


# ── rank_signals_by_regime ───────────────────────────────────────────────


class TestRankSignalsByRegime:
    def test_sorted_best_first(self):
        engine = _FakeEngine()
        # "good" signal: low brier
        for _ in range(MIN_REGIME_SAMPLES):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=1.0,
                signal_contributions={"good_signal": 1.0},
                regime="EXPANSION",
            )
        # "bad" signal: high brier
        for _ in range(MIN_REGIME_SAMPLES):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=0.0,
                signal_contributions={"bad_signal": 1.0},
                regime="EXPANSION",
            )
        # Unrelated regime — should NOT appear in the result
        for _ in range(MIN_REGIME_SAMPLES):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=1.0,
                signal_contributions={"other_signal": 1.0},
                regime="CRISIS",
            )
        ranked = rank_signals_by_regime(engine, "EXPANSION", 7)
        sources = [c.signal_source for c in ranked]
        # best-first, no cross-regime leakage
        assert sources == ["good_signal", "bad_signal"]
        assert "other_signal" not in sources
        assert ranked[0].running_brier < ranked[1].running_brier

    def test_empty_regime_returns_empty_list(self):
        engine = _FakeEngine()
        assert rank_signals_by_regime(engine, "EXPANSION", 7) == []


# ── bootstrap_from_oracle_predictions ────────────────────────────────────


class TestBootstrapFromOraclePredictions:
    def test_multi_regime_replay(self):
        engine = _FakeEngine()
        # Simulated oracle_predictions rows, one per regime, with a
        # pre-populated signal_contributions blob.
        now = datetime.now(timezone.utc)
        from datetime import timedelta

        engine.oracle_rows = [
            (
                "pred-1",
                now - timedelta(days=10),
                (now - timedelta(days=10) + timedelta(days=7)).date(),
                0.9,
                "hit",
                "model_a",
                None,
                '{"flow_momentum": 1.0}',
                "EXPANSION",
            ),
            (
                "pred-2",
                now - timedelta(days=9),
                (now - timedelta(days=9) + timedelta(days=7)).date(),
                0.9,
                "miss",
                "model_a",
                None,
                '{"flow_momentum": 1.0}',
                "CRISIS",
            ),
            (
                "pred-3",
                now - timedelta(days=8),
                (now - timedelta(days=8) + timedelta(days=7)).date(),
                0.7,
                "partial",
                "model_b",
                None,
                '{"jodi_oil": 0.6, "sge_premium": 0.4}',
                "NEUTRAL",
            ),
        ]

        summary = bootstrap_from_oracle_predictions(engine, days=30)
        assert summary["replayed_count"] == 3
        assert summary["skipped_count"] == 0
        # Regime histogram covers all three regimes that appeared.
        assert summary["regime_histogram"]["EXPANSION"] == 1
        assert summary["regime_histogram"]["CRISIS"] == 1
        assert summary["regime_histogram"]["NEUTRAL"] == 1
        # Buckets were actually written.
        assert ("flow_momentum", 7, "EXPANSION") in engine.store
        assert ("flow_momentum", 7, "CRISIS") in engine.store
        assert ("jodi_oil", 7, "NEUTRAL") in engine.store
        assert ("sge_premium", 7, "NEUTRAL") in engine.store

    def test_db_read_error_returns_empty_summary(self):
        class BrokenConnect:
            def connect(self):
                raise RuntimeError("no db")

            def begin(self):
                raise RuntimeError("no db")

        summary = bootstrap_from_oracle_predictions(BrokenConnect(), days=30)
        assert summary["replayed_count"] == 0
        assert summary["skipped_count"] == 0


# ── _row_to_scorecard pure helper ────────────────────────────────────────


class TestRowToScorecard:
    def test_calibrated_row(self):
        row = (
            MIN_CALIBRATED_SAMPLES,
            0.05,
            0.08,
            MIN_CALIBRATED_SAMPLES,  # all hits
            datetime(2026, 4, 13, tzinfo=timezone.utc),
        )
        card = _row_to_scorecard(row, signal_source="flow_momentum", horizon_days=7)
        assert card.signal_source == "flow_momentum"
        assert card.horizon_days == 7
        assert card.scored_count == MIN_CALIBRATED_SAMPLES
        assert card.is_calibrated is True
        # Brier = 0.05 → top of the conviction curve → 1.5
        assert card.conviction_weight == 1.5
        assert card.hit_rate == 1.0

    def test_cold_start_row_neutral_conviction(self):
        row = (3, 0.05, 0.08, 3, None)
        card = _row_to_scorecard(row, signal_source="flow_momentum", horizon_days=7)
        assert card.is_calibrated is False
        assert card.conviction_weight == 1.0
