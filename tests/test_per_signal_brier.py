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
    _reset_skipped_live_update_counts,
    compute_conviction_weight,
    get_full_scorecard_table,
    get_signal_scorecard,
    get_skipped_live_update_counts,
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
        self._ddl_seen: list[str] = []

    def seed_row(
        self,
        source: str,
        horizon_days: int,
        *,
        scored_count: int,
        running_brier: float,
        running_ece: float = 0.0,
        hit_count: int = 0,
        last_updated: datetime | None = None,
    ) -> None:
        self.store[(source, int(horizon_days))] = {
            "signal_source": source,
            "horizon_days": int(horizon_days),
            "scored_count": int(scored_count),
            "running_brier": float(running_brier),
            "running_ece": float(running_ece),
            "hit_count": int(hit_count),
            "last_updated": last_updated or datetime.now(timezone.utc),
        }

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

            if sql.startswith("DELETE FROM per_signal_brier_history"):
                keys = sorted(self.parent.store)
                self.parent.store.clear()
                return _FakeResult(fetchall_value=keys)

            if sql.startswith("INSERT INTO per_signal_brier_history"):
                key = (params["s"], int(params["h"]))
                if key in self.parent.store and "DO NOTHING" in sql:
                    return MagicMock()
                stamp = params.get("scored_at") or datetime.now(timezone.utc)
                existing = self.parent.store.get(key)
                if existing is not None and existing.get("last_updated") is not None and stamp < existing["last_updated"]:
                    stamp = existing["last_updated"]
                self.parent.store[key] = {
                    "signal_source": params["s"],
                    "horizon_days": int(params["h"]),
                    "scored_count": int(params.get("n", 1)),
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
        _reset_skipped_live_update_counts()
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
        assert get_skipped_live_update_counts() == {}

    def test_live_path_skips_new_and_existing_without_inserting_count1(self):
        _reset_skipped_live_update_counts()
        engine = _FakeEngine()
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        assert updates == {}
        assert ("jodi_oil", 7) not in engine.store
        assert get_skipped_live_update_counts() == {("jodi_oil", 7): 1}

    def test_multi_signal_skips_and_counts_each_contributor(self):
        _reset_skipped_live_update_counts()
        engine = _FakeEngine()
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.2,
            outcome=1.0,
            signal_contributions={"sge_premium": 0.6, "reddit_options_pulse": 0.4},
        )
        assert updates == {}
        assert engine.store == {}
        assert get_skipped_live_update_counts() == {
            ("sge_premium", 7): 1,
            ("reddit_options_pulse", 7): 1,
        }

    def test_existing_row_is_never_mutated_without_weight_sum_column(self):
        _reset_skipped_live_update_counts()
        engine = _FakeEngine()
        engine.seed_row("jodi_oil", 7, scored_count=25, running_brier=0.09, hit_count=20)
        before = dict(engine.store[("jodi_oil", 7)])
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.8,
            outcome=0.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        assert updates == {}
        assert engine.store[("jodi_oil", 7)] == before
        assert get_skipped_live_update_counts() == {("jodi_oil", 7): 1}

    def test_horizon_snapping_in_skip_counter(self):
        _reset_skipped_live_update_counts()
        engine = _FakeEngine()
        # horizon=10 should snap to 7
        record_scored_prediction(
            engine,
            horizon_days=10,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
        )
        assert engine.store == {}
        assert get_skipped_live_update_counts() == {("jodi_oil", 7): 1}

    def test_zero_weight_not_counted_as_skip(self):
        _reset_skipped_live_update_counts()
        engine = _FakeEngine()
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 0.0, "sge_premium": 1.0},
        )
        assert updates == {}
        counts = get_skipped_live_update_counts()
        assert ("jodi_oil", 7) not in counts
        assert counts[("sge_premium", 7)] == 1


# ── get_signal_scorecard ─────────────────────────────────────────────────


class TestGetSignalScorecard:
    def test_missing_returns_none(self):
        engine = _FakeEngine()
        assert get_signal_scorecard(engine, "jodi_oil", 7) is None

    def test_cold_start_not_calibrated(self):
        engine = _FakeEngine()
        engine.seed_row("jodi_oil", 7, scored_count=1, running_brier=0.09, hit_count=1)
        card = get_signal_scorecard(engine, "jodi_oil", 7)
        assert card is not None
        assert card.scored_count == 1
        assert card.is_calibrated is False
        assert card.conviction_weight == 1.0  # neutral cold-start

    def test_calibrated_with_good_brier_gets_high_conviction(self):
        engine = _FakeEngine()
        engine.seed_row(
            "jodi_oil",
            7,
            scored_count=MIN_CALIBRATED_SAMPLES,
            running_brier=0.01,
            hit_count=MIN_CALIBRATED_SAMPLES,
        )
        card = get_signal_scorecard(engine, "jodi_oil", 7)
        assert card is not None
        assert card.is_calibrated is True
        assert card.conviction_weight == 1.5  # best-in-class
        assert card.hit_rate == 1.0

    def test_calibrated_with_bad_brier_gets_zero_conviction(self):
        engine = _FakeEngine()
        engine.seed_row(
            "jodi_oil",
            7,
            scored_count=MIN_CALIBRATED_SAMPLES,
            running_brier=0.81,
            hit_count=0,
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
        engine.seed_row(
            "jodi_oil",
            7,
            scored_count=MIN_CALIBRATED_SAMPLES,
            running_brier=0.01,
            hit_count=MIN_CALIBRATED_SAMPLES,
        )
        engine.seed_row(
            "reddit_options_pulse",
            7,
            scored_count=MIN_CALIBRATED_SAMPLES,
            running_brier=0.81,
            hit_count=0,
        )
        ranked = rank_signals_by_horizon(engine, 7)
        assert len(ranked) == 2
        assert ranked[0].signal_source == "jodi_oil"  # best first
        assert ranked[1].signal_source == "reddit_options_pulse"
        assert ranked[0].running_brier < ranked[1].running_brier

    def test_min_samples_filter(self):
        engine = _FakeEngine()
        engine.seed_row("jodi_oil", 7, scored_count=5, running_brier=0.01, hit_count=5)
        # Default threshold hides it
        assert rank_signals_by_horizon(engine, 7) == []
        # min_samples=0 shows cold-start
        relaxed = rank_signals_by_horizon(engine, 7, min_samples=0)
        assert len(relaxed) == 1

    def test_full_scorecard_table_returns_all_sources(self):
        engine = _FakeEngine()
        engine.seed_row("sge_premium", 1, scored_count=1, running_brier=0.09, hit_count=1)
        engine.seed_row("jodi_oil", 30, scored_count=1, running_brier=0.09, hit_count=1)
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


# ── Backdated last_updated on bootstrap write path ────────────────────────


class TestRecordScoredPredictionBackdated:
    """The bootstrap replays historical predictions in chronological order and
    backdates ``last_updated`` to the latest ``scored_at`` so walk-forward
    audits filtering ``last_updated <= as_of`` see the scorecards. Live
    ``record_scored_prediction`` accepts ``scored_at`` without inserting dead
    count=1 rows.
    """

    def test_bootstrap_insert_uses_scored_at_when_provided(self):
        from scripts import bootstrap_per_signal_brier as boot

        engine = _FakeEngine()
        backdate = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)
        boot._write_bootstrapped_buckets(
            engine,
            {
                ("jodi_oil", 7): {
                    "scored_count": 20,
                    "weight_sum": 10.0,
                    "weighted_brier_sum": 0.9,
                    "weighted_ece_sum": 3.0,
                    "hit_count": 20,
                    "last_updated": backdate,
                }
            },
        )
        row = engine.store[("jodi_oil", 7)]
        assert row["last_updated"] == backdate

    def test_bootstrap_insert_defaults_to_now_when_scored_at_none(self):
        from scripts import bootstrap_per_signal_brier as boot

        engine = _FakeEngine()
        before = datetime.now(timezone.utc)
        boot._write_bootstrapped_buckets(
            engine,
            {
                ("jodi_oil", 7): {
                    "scored_count": 20,
                    "weight_sum": 10.0,
                    "weighted_brier_sum": 0.9,
                    "weighted_ece_sum": 3.0,
                    "hit_count": 20,
                    "last_updated": None,
                }
            },
        )
        after = datetime.now(timezone.utc)
        stamp = engine.store[("jodi_oil", 7)]["last_updated"]
        assert before <= stamp <= after

    def test_bootstrap_replacement_stamps_each_bucket_with_its_own_latest_scored_at(self):
        """Each run replaces the table, so a retired row's timestamp is never carried
        onto the new row: last_updated is the replayed bucket's latest scored_at."""
        from scripts import bootstrap_per_signal_brier as boot

        engine = _FakeEngine()
        older = datetime(2026, 1, 1, tzinfo=timezone.utc)
        newer = datetime(2026, 2, 1, tzinfo=timezone.utc)
        latest = datetime(2026, 3, 1, tzinfo=timezone.utc)
        base_bucket = {
            "scored_count": 20,
            "weight_sum": 10.0,
            "weighted_brier_sum": 0.9,
            "weighted_ece_sum": 3.0,
            "hit_count": 20,
        }
        boot._write_bootstrapped_buckets(engine, {("jodi_oil", 7): {**base_bucket, "last_updated": newer}})
        boot._write_bootstrapped_buckets(engine, {("jodi_oil", 7): {**base_bucket, "last_updated": older}})
        assert engine.store[("jodi_oil", 7)]["last_updated"] == older
        boot._write_bootstrapped_buckets(engine, {("jodi_oil", 7): {**base_bucket, "last_updated": latest}})
        assert engine.store[("jodi_oil", 7)]["last_updated"] == latest

    def test_live_record_scored_prediction_accepts_scored_at_and_skips(self):
        _reset_skipped_live_update_counts()
        engine = _FakeEngine()
        backdate = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)
        updates = record_scored_prediction(
            engine,
            horizon_days=7,
            confidence=0.7,
            outcome=1.0,
            signal_contributions={"jodi_oil": 1.0},
            scored_at=backdate,
        )
        assert updates == {}
        assert engine.store == {}
