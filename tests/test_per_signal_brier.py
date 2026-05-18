"""Tests for features/per_signal_brier.py (ALPHA-15 / #118).

The pure functions (conviction weighter + canonical horizon snapping)
are tested directly. The DB-touching paths use a fake engine built on
``unittest.mock`` that stores state in a dict so the full
record→read roundtrip is exercised without touching postgres.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock


from features.per_signal_brier import (
    MIN_CALIBRATED_SAMPLES,
    SignalScorecard,
    _canonical_horizon,
    compute_conviction_weight,
    get_full_scorecard_table,
    get_signal_scorecard,
    rank_signals_by_horizon,
    record_scored_prediction,
)


# ── compute_conviction_weight (pure function) ────────────────────────────


class TestConvictionWeight:
    def test_cold_start_returns_neutral(self):
        # Below MIN_CALIBRATED_SAMPLES → 1.0 regardless of Brier
        assert compute_conviction_weight(0.01, 5) == 1.0
        assert compute_conviction_weight(0.5, 10) == 1.0

    def test_anti_predictive_returns_zero(self):
        assert compute_conviction_weight(0.25, 100) == 0.0
        assert compute_conviction_weight(0.40, 100) == 0.0

    def test_best_in_class_returns_max(self):
        assert compute_conviction_weight(0.05, 100) == 1.5
        assert compute_conviction_weight(0.01, 100) == 1.5

    def test_linear_interpolation_midpoint(self):
        # Brier 0.15 is midway between 0.05 (best) and 0.25 (worst)
        # → conviction should be midway between 1.5 and 0.0 = 0.75
        w = compute_conviction_weight(0.15, 100)
        assert abs(w - 0.75) < 0.001

    def test_monotonically_decreasing(self):
        # Higher Brier → lower conviction
        w1 = compute_conviction_weight(0.08, 100)
        w2 = compute_conviction_weight(0.12, 100)
        w3 = compute_conviction_weight(0.20, 100)
        assert w1 > w2 > w3

    def test_output_clamped_to_valid_range(self):
        for brier in (-0.5, 0.0, 0.1, 0.2, 0.5, 1.0):
            w = compute_conviction_weight(brier, 100)
            assert 0.0 <= w <= 1.5


# ── _canonical_horizon (pure) ────────────────────────────────────────────


class TestCanonicalHorizon:
    def test_snap_1d(self):
        assert _canonical_horizon(1) == 1
        assert _canonical_horizon(2) == 1

    def test_snap_7d(self):
        assert _canonical_horizon(3) == 7
        assert _canonical_horizon(7) == 7
        assert _canonical_horizon(14) == 7

    def test_snap_30d(self):
        assert _canonical_horizon(15) == 30
        assert _canonical_horizon(30) == 30
        assert _canonical_horizon(60) == 30

    def test_snap_90d(self):
        assert _canonical_horizon(61) == 90
        assert _canonical_horizon(90) == 90
        assert _canonical_horizon(180) == 90

    def test_string_coercion(self):
        assert _canonical_horizon("7") == 7
        assert _canonical_horizon("30d") == 7  # non-numeric → fallback to 7


# ── Fake engine for record+read roundtrip ────────────────────────────────


class _FakeEngine:
    """Tiny in-memory stand-in for a SQLAlchemy Engine. Stores rows in
    a dict keyed on (signal_source, horizon_days) and understands the
    exact SELECT / INSERT / UPDATE shapes produced by
    features/per_signal_brier.py.
    """

    def __init__(self) -> None:
        self.store: dict[tuple[str, int], dict] = {}
        # Append-only snapshot rows in insert order.
        self.snapshots: list[dict] = []
        self._ddl_seen: list[str] = []

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

            if sql.startswith("CREATE TABLE") or sql.startswith("CREATE INDEX"):
                self.parent._ddl_seen.append(sql)
                return MagicMock()

            # NEW SNAPSHOT PATHS — must fire BEFORE the generic
            # "SELECT signal_source, horizon_days, scored_count" branch
            # below; otherwise the legacy full-table-scan returns rows
            # without honouring the horizon filter.
            if "FROM per_signal_brier_snapshots" in sql:
                horizon = int(params["h"])
                as_of = params["as_of"]
                eligible = [
                    s for s in self.parent.snapshots
                    if s["horizon_days"] == horizon and s["snapshot_at"] <= as_of
                ]
                eligible.sort(key=lambda s: s["snapshot_at"], reverse=True)
                latest: dict[str, dict] = {}
                for s in eligible:
                    src = s["signal_source"]
                    if src not in latest:
                        latest[src] = s
                rows = [
                    (
                        s["signal_source"], s["horizon_days"], s["scored_count"],
                        s["running_brier"], s["running_ece"], s["hit_count"],
                        s["snapshot_at"],
                    )
                    for s in latest.values()
                ]
                return _FakeResult(fetchall_value=rows)

            if (
                "FROM per_signal_brier_history" in sql
                and "WHERE horizon_days" in sql
                and "last_updated <=" in sql
            ):
                horizon = int(params["h"])
                as_of = params["as_of"]
                rows = [
                    _row_tuple(v)
                    for (src, h), v in self.parent.store.items()
                    if h == horizon
                    and v.get("last_updated")
                    and v["last_updated"] <= as_of
                ]
                return _FakeResult(fetchall_value=rows)

            if sql.startswith("SELECT signal_source, horizon_days, scored_count"):
                # Read path — either one row or all rows
                if "WHERE signal_source" in sql and "AND horizon_days" in sql:
                    key = (params["s"], int(params["h"]))
                    row = self.parent.store.get(key)
                    return _FakeResult(
                        fetchone_value=_row_tuple(row) if row else None
                    )
                if "WHERE horizon_days" in sql and "scored_count >=" in sql:
                    rows = [
                        _row_tuple(v)
                        for k, v in self.parent.store.items()
                        if k[1] == int(params["h"]) and v["scored_count"] >= int(params["n"])
                    ]
                    rows.sort(key=lambda r: r[3])  # running_brier asc
                    return _FakeResult(fetchall_value=rows)
                # Full table scan
                rows = sorted(
                    (_row_tuple(v) for v in self.parent.store.values()),
                    key=lambda r: (r[0], r[1]),
                )
                return _FakeResult(fetchall_value=rows)

            if sql.startswith("SELECT scored_count, running_brier"):
                key = (params["s"], int(params["h"]))
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

            if sql.startswith("INSERT INTO per_signal_brier_history"):
                key = (params["s"], int(params["h"]))
                if key in self.parent.store:
                    return MagicMock()
                # Mirror SQL ``COALESCE(:scored_at, NOW())`` — backdated
                # value wins when provided, else fall back to wall clock.
                stamp = params.get("scored_at") or datetime.now(timezone.utc)
                self.parent.store[key] = {
                    "signal_source": params["s"],
                    "horizon_days": int(params["h"]),
                    "scored_count": 1,
                    "running_brier": float(params["b"]),
                    "running_ece": float(params["e"]),
                    "hit_count": int(params["hit"]),
                    "last_updated": stamp,
                }
                return MagicMock()

            if sql.startswith("UPDATE per_signal_brier_history"):
                key = (params["s"], int(params["hz"]))
                if key not in self.parent.store:
                    return MagicMock()
                # Mirror SQL ``GREATEST(last_updated, COALESCE(:scored_at, NOW()))``.
                proposed = params.get("scored_at") or datetime.now(timezone.utc)
                existing = self.parent.store[key].get("last_updated")
                if existing is not None and proposed < existing:
                    proposed = existing
                self.parent.store[key].update(
                    {
                        "scored_count": int(params["n"]),
                        "running_brier": float(params["b"]),
                        "running_ece": float(params["e"]),
                        "hit_count": int(params["h"]),
                        "last_updated": proposed,
                    }
                )
                return MagicMock()

            if sql.startswith("INSERT INTO per_signal_brier_snapshots"):
                # Append-only-ish: ON CONFLICT DO UPDATE overwrites the
                # row when the (source, horizon, snapshot_at) tuple
                # collides (batch-scored predictions sharing scored_at).
                # The overwrite keeps the LATEST running state at that
                # moment, which is the correct PIT semantic.
                stamp = params.get("scored_at") or datetime.now(timezone.utc)
                snap = {
                    "signal_source": params["s"],
                    "horizon_days": int(params["h"]),
                    "snapshot_at": stamp,
                    "scored_count": int(params.get("n", 1)),
                    "running_brier": float(params["b"]),
                    "running_ece": float(params["e"]),
                    "hit_count": int(params.get("hit_count", params.get("hit", 0))),
                }
                key = (snap["signal_source"], snap["horizon_days"], snap["snapshot_at"])
                for i, existing in enumerate(self.parent.snapshots):
                    if (
                        existing["signal_source"],
                        existing["horizon_days"],
                        existing["snapshot_at"],
                    ) == key:
                        self.parent.snapshots[i] = snap
                        break
                else:
                    self.parent.snapshots.append(snap)
                return MagicMock()

            # Snapshot read + history fallback are dispatched earlier
            # in this method (above the generic SELECT branch) so they
            # honour the horizon filter before the legacy full-scan kicks in.

            return MagicMock()


class _FakeResult:
    def __init__(self, fetchone_value=None, fetchall_value=None):
        self._one = fetchone_value
        self._all = fetchall_value or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


def _row_tuple(d: dict) -> tuple:
    return (
        d["signal_source"],
        d["horizon_days"],
        d["scored_count"],
        d["running_brier"],
        d["running_ece"],
        d["hit_count"],
        d["last_updated"],
    )


# ── record_scored_prediction ──────────────────────────────────────────────


class TestRecordScoredPrediction:
    def test_empty_contributions_noop(self):
        engine = _FakeEngine()
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={},
        )
        assert updates == {}
        assert engine.store == {}

    def test_single_signal_first_insert(self):
        engine = _FakeEngine()
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,  # hit
            signal_contributions={"jodi_oil": 1.0},
        )
        assert "jodi_oil" in updates
        assert updates["jodi_oil"]["scored_count"] == 1
        # squared error weighted by 1.0 = (0.7-1.0)^2 = 0.09
        assert abs(updates["jodi_oil"]["running_brier"] - 0.09) < 1e-9

    def test_multi_signal_proportional_attribution(self):
        engine = _FakeEngine()
        # 60/40 split, big error so differences are visible
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.2,
            outcome=1.0,  # hit but low confidence — squared error = 0.64
            signal_contributions={"sge_premium": 0.6, "reddit_options_pulse": 0.4},
        )
        assert abs(updates["sge_premium"]["running_brier"] - 0.64 * 0.6) < 1e-9
        assert abs(updates["reddit_options_pulse"]["running_brier"] - 0.64 * 0.4) < 1e-9

    def test_welford_running_average(self):
        engine = _FakeEngine()
        # First: conf=0.7, outcome=1.0 → err=0.09
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        # Second: conf=0.8, outcome=0.0 → err=0.64
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.8,
            outcome=0.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        row = engine.store[("jodi_oil", 7)]
        assert row["scored_count"] == 2
        # Running mean of {0.09, 0.64} = 0.365
        assert abs(row["running_brier"] - 0.365) < 1e-6

    def test_directional_hit_counter(self):
        engine = _FakeEngine()
        # conf >= 0.5 AND outcome >= 0.5 → hit
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.8,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        # conf < 0.5 AND outcome < 0.5 → hit (correctly bearish)
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.3,
            outcome=0.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        # conf >= 0.5 AND outcome < 0.5 → miss
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=0.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        row = engine.store[("jodi_oil", 7)]
        assert row["hit_count"] == 2
        assert row["scored_count"] == 3

    def test_horizon_snapping(self):
        engine = _FakeEngine()
        # horizon=10 should snap to 7
        record_scored_prediction(
            engine,
            horizon_days=10,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        assert ("jodi_oil", 7) in engine.store
        assert ("jodi_oil", 10) not in engine.store

    def test_zero_weight_skipped(self):
        engine = _FakeEngine()
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 0.0, "sge_premium": 1.0},
        )
        assert "jodi_oil" not in updates
        assert "sge_premium" in updates


# ── get_signal_scorecard ─────────────────────────────────────────────────


class TestGetSignalScorecard:
    def test_missing_returns_none(self):
        engine = _FakeEngine()
        assert get_signal_scorecard(engine, "jodi_oil", 7) is None

    def test_cold_start_not_calibrated(self):
        engine = _FakeEngine()
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        card = get_signal_scorecard(engine, "jodi_oil", 7)
        assert card is not None
        assert card.scored_count == 1
        assert card.is_calibrated is False
        assert card.conviction_weight == 1.0  # neutral cold-start

    def test_calibrated_with_good_brier_gets_high_conviction(self):
        engine = _FakeEngine()
        # Seed MIN_CALIBRATED_SAMPLES predictions with ~perfect accuracy
        for _ in range(MIN_CALIBRATED_SAMPLES):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=1.0,  # weighted brier = 0.01
                signal_contributions={"jodi_oil": 1.0},
            )
        card = get_signal_scorecard(engine, "jodi_oil", 7)
        assert card is not None
        assert card.is_calibrated is True
        assert card.conviction_weight == 1.5  # best-in-class
        assert card.hit_rate == 1.0

    def test_calibrated_with_bad_brier_gets_zero_conviction(self):
        engine = _FakeEngine()
        for _ in range(MIN_CALIBRATED_SAMPLES):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=0.0,  # squared error = 0.81
                signal_contributions={"jodi_oil": 1.0},
            )
        card = get_signal_scorecard(engine, "jodi_oil", 7)
        assert card is not None
        assert card.is_calibrated is True
        assert card.conviction_weight == 0.0


# ── rank_signals_by_horizon + full table ─────────────────────────────────


class TestRanking:
    def test_empty_returns_empty(self):
        assert rank_signals_by_horizon(_FakeEngine(), 7) == []

    def test_sorted_best_first(self):
        engine = _FakeEngine()
        # Good signal
        for _ in range(MIN_CALIBRATED_SAMPLES):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=1.0,
                signal_contributions={"jodi_oil": 1.0},
            )
        # Bad signal
        for _ in range(MIN_CALIBRATED_SAMPLES):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=0.0,
                signal_contributions={"reddit_options_pulse": 1.0},
            )
        ranked = rank_signals_by_horizon(engine, 7)
        assert len(ranked) == 2
        assert ranked[0].signal_source == "jodi_oil"  # best first
        assert ranked[1].signal_source == "reddit_options_pulse"
        assert ranked[0].running_brier < ranked[1].running_brier

    def test_min_samples_filter(self):
        engine = _FakeEngine()
        # Only 5 samples — below default MIN_CALIBRATED_SAMPLES
        for _ in range(5):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.9,
                outcome=1.0,
                signal_contributions={"jodi_oil": 1.0},
            )
        # Default threshold hides it
        assert rank_signals_by_horizon(engine, 7) == []
        # min_samples=0 shows cold-start
        relaxed = rank_signals_by_horizon(engine, 7, min_samples=0)
        assert len(relaxed) == 1

    def test_full_scorecard_table_returns_all_sources(self):
        engine = _FakeEngine()
        record_scored_prediction(
            engine,
            horizon_days=1,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"sge_premium": 1.0},
        )
        record_scored_prediction(
            engine,
            horizon_days=30,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        table = get_full_scorecard_table(engine)
        sources = {c.signal_source for c in table}
        assert sources == {"sge_premium", "jodi_oil"}


# ── Scorecard dataclass ──────────────────────────────────────────────────


class TestScorecardDataclass:
    def test_to_dict_serializes_all_fields(self):
        card = SignalScorecard(
            signal_source="jodi_oil",
            horizon_days=7,
            scored_count=50,
            running_brier=0.12,
            running_ece=0.15,
            hit_rate=0.72,
            last_updated=datetime(2026, 4, 13, tzinfo=timezone.utc),
            is_calibrated=True,
            conviction_weight=1.05,
        )
        d = card.to_dict()
        for k in (
            "signal_source", "horizon_days", "scored_count",
            "running_brier", "running_ece", "hit_rate",
            "last_updated", "is_calibrated", "conviction_weight",
        ):
            assert k in d
        assert d["signal_source"] == "jodi_oil"
        assert d["is_calibrated"] is True


# ── record_scored_prediction: backdated last_updated ──────────────────────


class TestRecordScoredPredictionBackdated:
    """The bootstrap replays historical predictions in chronological order.
    Walk-forward audits later filter scorecards by ``last_updated <= as_of``
    to prevent lookahead leak. If the bootstrap stamps every row with NOW(),
    every historical replay sees zero scorecards (the substrate is invisible
    to the very replay it was built for). The ``scored_at`` parameter
    backdates the per-bucket ``last_updated`` to when the prediction was
    actually scored against reality so the substrate becomes consumable.
    """

    def test_insert_uses_scored_at_when_provided(self):
        engine = _FakeEngine()
        backdate = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
            scored_at=backdate,
        )
        row = engine.store[("jodi_oil", 7)]
        assert row["last_updated"] == backdate

    def test_insert_defaults_to_now_when_scored_at_none(self):
        engine = _FakeEngine()
        before = datetime.now(timezone.utc)
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        after = datetime.now(timezone.utc)
        stamp = engine.store[("jodi_oil", 7)]["last_updated"]
        assert before <= stamp <= after

    def test_update_takes_greatest_of_existing_and_scored_at(self):
        engine = _FakeEngine()
        older = datetime(2026, 1, 1, tzinfo=timezone.utc)
        newer = datetime(2026, 2, 1, tzinfo=timezone.utc)
        # First call backdates to NEWER (so the bucket carries that)
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
            scored_at=newer,
        )
        # Second call tries to backdate to OLDER — must not go backwards
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.8,
            outcome=0.0,
            signal_contributions={"jodi_oil": 1.0},
            scored_at=older,
        )
        row = engine.store[("jodi_oil", 7)]
        assert row["last_updated"] == newer
        # And conversely, when the new scored_at is strictly later, it wins
        latest = datetime(2026, 3, 1, tzinfo=timezone.utc)
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.6,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
            scored_at=latest,
        )
        assert engine.store[("jodi_oil", 7)]["last_updated"] == latest


# ── per_signal_brier_snapshots: append + PIT reconstruction ───────────────


class TestSnapshotWritePath:
    """Each ``record_scored_prediction`` invocation must append a row to
    ``per_signal_brier_snapshots`` so historical audits can reconstruct
    the running state at any prediction's ``scored_at``.
    """

    def test_first_prediction_writes_initial_snapshot(self):
        engine = _FakeEngine()
        t0 = datetime(2026, 1, 15, 9, 0, tzinfo=timezone.utc)
        record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
            scored_at=t0,
        )
        assert len(engine.snapshots) == 1
        s = engine.snapshots[0]
        assert s["signal_source"] == "jodi_oil"
        assert s["horizon_days"] == 7
        assert s["snapshot_at"] == t0
        assert s["scored_count"] == 1

    def test_subsequent_predictions_append_running_state(self):
        engine = _FakeEngine()
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        t1 = datetime(2026, 1, 2, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 3, tzinfo=timezone.utc)
        for stamp in (t0, t1, t2):
            record_scored_prediction(
                engine,
                horizon_days=7,
                confidence=0.6,
                outcome=1.0,
                signal_contributions={"jodi_oil": 1.0},
                scored_at=stamp,
            )
        # 3 snapshots, monotonically increasing scored_count
        assert len(engine.snapshots) == 3
        counts = [s["scored_count"] for s in engine.snapshots]
        assert counts == [1, 2, 3]


class TestGetScorecardsAsOf:
    """``get_scorecards_as_of`` must return the PIT-correct running
    state for each (signal_source, horizon_days) bucket.
    """

    def test_returns_state_as_of_timestamp(self):
        from features.per_signal_brier import get_scorecards_as_of
        engine = _FakeEngine()
        # Three predictions: t0 (count=1), t1 (count=2), t2 (count=3)
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        t1 = datetime(2026, 1, 5, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 10, tzinfo=timezone.utc)
        for stamp in (t0, t1, t2):
            record_scored_prediction(
                engine, horizon_days=7, confidence=0.7, outcome=1.0,
                signal_contributions={"jodi_oil": 1.0}, scored_at=stamp,
            )
        # Query as_of t1: should see only the t0+t1 contributions → count=2
        midpoint = datetime(2026, 1, 6, tzinfo=timezone.utc)
        cards = get_scorecards_as_of(engine, midpoint, horizon_days=7)
        assert "jodi_oil" in cards
        assert cards["jodi_oil"].scored_count == 2
        # Query as_of long-before: nothing
        before = datetime(2025, 12, 1, tzinfo=timezone.utc)
        assert get_scorecards_as_of(engine, before, horizon_days=7) == {}
        # Query as_of long-after: latest state
        after = datetime(2026, 12, 1, tzinfo=timezone.utc)
        assert get_scorecards_as_of(engine, after, horizon_days=7)["jodi_oil"].scored_count == 3

    def test_horizon_snapping(self):
        from features.per_signal_brier import get_scorecards_as_of
        engine = _FakeEngine()
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        # Raw h=10 snaps to 7
        record_scored_prediction(
            engine, horizon_days=10, confidence=0.7, outcome=1.0,
            signal_contributions={"jodi_oil": 1.0}, scored_at=t0,
        )
        midpoint = datetime(2026, 1, 5, tzinfo=timezone.utc)
        # Query with raw 9 — should snap to 7 and find the snapshot
        cards = get_scorecards_as_of(engine, midpoint, horizon_days=9)
        assert "jodi_oil" in cards
        # Query with 30 — different bucket, empty
        assert get_scorecards_as_of(engine, midpoint, horizon_days=30) == {}

    def test_falls_back_to_history_table_when_snapshots_empty(self):
        from features.per_signal_brier import get_scorecards_as_of
        engine = _FakeEngine()
        # Manually populate history but NOT snapshots — simulates a
        # legacy state where the snapshot table hasn't been backfilled.
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        engine.store[("jodi_oil", 7)] = {
            "signal_source": "jodi_oil", "horizon_days": 7,
            "scored_count": 50, "running_brier": 0.1, "running_ece": 0.1,
            "hit_count": 30, "last_updated": t0,
        }
        midpoint = datetime(2026, 1, 5, tzinfo=timezone.utc)
        cards = get_scorecards_as_of(engine, midpoint, horizon_days=7)
        assert "jodi_oil" in cards
        assert cards["jodi_oil"].scored_count == 50
