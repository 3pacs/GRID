"""Oracle predictions carry an explicit forecast horizon (LEVER-PACKAGE §5.1).

Until 2026-09-10 every oracle prediction expired at the next monthly options
expiry (35 d at most), so the 90 d / 180 d calibration buckets could never
fill and "long-horizon tailwinds" were unprovable. These tests pin:

* ``_expiry_for_horizon`` — explicit horizon → ``today + N``; ``None`` keeps
  the legacy monthly expiry.
* ``_horizon_days_for`` — the recorded horizon is the request, else derived
  from the expiry.
* ``run_cycle(horizon_days=...)`` hands the horizon to
  ``generate_predictions``.
* The ``oracle_predictions`` INSERT persists ``horizon_days`` and the schema
  guard adds the column.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from oracle import engine as oe

ROOT = Path(__file__).resolve().parents[1]
SRC = (ROOT / "oracle" / "engine.py").read_text(encoding="utf-8")


def _bare_oracle() -> oe.OracleEngine:
    return oe.OracleEngine.__new__(oe.OracleEngine)


class TestExpiryForHorizon:
    def test_explicit_horizon_is_calendar_days_from_now(self) -> None:
        now = datetime(2026, 9, 10, 14, 30)
        o = _bare_oracle()
        assert o._expiry_for_horizon(now, 90) == date(2026, 12, 9)
        assert o._expiry_for_horizon(now, 180) == date(2027, 3, 9)

    def test_none_or_non_positive_falls_back_to_monthly_expiry(self, monkeypatch) -> None:
        o = _bare_oracle()
        monkeypatch.setattr(o, "_next_monthly_expiry", lambda: date(2026, 10, 16))
        now = datetime(2026, 9, 10)
        assert o._expiry_for_horizon(now, None) == date(2026, 10, 16)
        assert o._expiry_for_horizon(now, 0) == date(2026, 10, 16)
        assert o._expiry_for_horizon(now, -5) == date(2026, 10, 16)


class TestHorizonDaysFor:
    def test_explicit_request_wins(self) -> None:
        now = datetime(2026, 9, 10)
        assert oe.OracleEngine._horizon_days_for(now, date(2026, 10, 16), 90) == 90

    def test_derived_from_expiry_when_not_requested(self) -> None:
        now = datetime(2026, 9, 10)
        assert oe.OracleEngine._horizon_days_for(now, date(2026, 10, 16), None) == 36
        # Never below one day, even for a same-day expiry.
        assert oe.OracleEngine._horizon_days_for(now, now.date(), None) == 1


class TestRunCyclePassesHorizon:
    def test_horizon_reaches_generate_predictions(self, monkeypatch) -> None:
        o = _bare_oracle()
        o.engine = object()
        seen: dict = {}

        def fake_generate(tickers=None, horizon_days=None):
            seen["tickers"] = tickers
            seen["horizon_days"] = horizon_days
            return []

        monkeypatch.setattr(o, "score_expired_predictions", lambda: {"scored": 0})
        monkeypatch.setattr(o, "evolve_weights", lambda: {})
        monkeypatch.setattr(o, "_load_models", lambda: [])
        monkeypatch.setattr(o, "generate_predictions", fake_generate)
        monkeypatch.setattr(o, "_get_leaderboard", lambda: [])

        result = o.run_cycle(["NVDA", "AMD"], horizon_days=90)

        assert seen == {"tickers": ["NVDA", "AMD"], "horizon_days": 90}
        assert result["new_predictions"] == 0

    def test_default_keeps_legacy_none(self, monkeypatch) -> None:
        o = _bare_oracle()
        o.engine = object()
        seen: dict = {}
        monkeypatch.setattr(o, "score_expired_predictions", lambda: {})
        monkeypatch.setattr(o, "evolve_weights", lambda: {})
        monkeypatch.setattr(o, "_load_models", lambda: [])
        monkeypatch.setattr(
            o, "generate_predictions",
            lambda tickers=None, horizon_days=None: seen.setdefault("h", horizon_days) or [],
        )
        monkeypatch.setattr(o, "_get_leaderboard", lambda: [])
        o.run_cycle()
        assert seen["h"] is None


class TestPersistence:
    def test_prediction_dataclass_carries_horizon(self) -> None:
        p = oe.OraclePrediction(
            id="x", timestamp=datetime(2026, 9, 10), ticker="NVDA",
            prediction_type=oe.PredictionType.DIRECTION, direction="LONG",
            target_price=None, current_price=100.0,
            expiry=date(2026, 12, 9), confidence=0.6, expected_move_pct=5.0,
            horizon_days=90,
        )
        assert p.horizon_days == 90
        # Legacy construction (no horizon) still works and is explicit None.
        assert oe.OraclePrediction(
            id="y", timestamp=datetime(2026, 9, 10), ticker="NVDA",
            prediction_type=oe.PredictionType.DIRECTION, direction="LONG",
            target_price=None, current_price=100.0,
            expiry=date(2026, 10, 16), confidence=0.6, expected_move_pct=5.0,
        ).horizon_days is None

    def test_insert_persists_horizon_days(self) -> None:
        insert = SRC[SRC.index("INSERT INTO oracle_predictions"):]
        insert = insert[: insert.index("ON CONFLICT")]
        cols = re.search(r"\((.*?)\)\s*VALUES\s*\((.*?)\)", insert, re.S)
        assert cols is not None
        column_names = [c.strip() for c in cols.group(1).split(",")]
        placeholders = [v.strip() for v in cols.group(2).split(",")]
        assert "horizon_days" in column_names
        assert ":hd" in placeholders
        assert column_names.index("horizon_days") == placeholders.index(":hd")
        assert '"hd": int(p.horizon_days) if p.horizon_days is not None else None' in SRC

    def test_schema_guard_adds_column(self) -> None:
        assert "ADD COLUMN IF NOT EXISTS horizon_days INTEGER" in SRC


class TestDistinctPositionCounts:
    """4,340 model votes upserted onto 14 rows must be reported as 14 (task #29)."""

    def test_store_returns_inserted_vs_updated_and_cycle_exposes_them(self) -> None:
        assert "DO NOTHING RETURNING id" in SRC
        assert '"inserted": len(accepted), "updated": 0' in SRC
        assert "self._last_store_counts = {" in SRC
        assert '"distinct_positions_written": _counts.get("qualified", _counts.get("inserted"))' in SRC
        assert '"upsert_statements": _counts.get("upsert_statements")' in SRC

    def test_report_subject_prefers_distinct_positions(self) -> None:
        rsrc = (ROOT / "oracle" / "report.py").read_text(encoding="utf-8")
        assert 'cycle_result.get("distinct_positions_written")' in rsrc
        assert "New prediction rows" in rsrc and "model votes" in rsrc


def _batch_oracle(monkeypatch):
    from oracle import calibration, hallucination_guard

    oracle = _bare_oracle()
    oracle.engine = object()
    oracle._load_models = lambda: [SimpleNamespace(name="fixture", hits=0, misses=0, partials=0)]
    monkeypatch.setenv("GRID_ORACLE_PARALLELISM", "1")
    monkeypatch.setenv("GRID_ORACLE_BATCH_SIZE", "2")
    monkeypatch.setattr(calibration, "compute_calibration", lambda engine: None)
    monkeypatch.setattr(hallucination_guard, "verify_predictions", lambda preds, **kw: (preds, []))
    return oracle


def _batch_prediction(ticker, timestamp):
    return oe.OraclePrediction(
        id=ticker, timestamp=timestamp, ticker=ticker,
        prediction_type=oe.PredictionType.DIRECTION, direction="CALL",
        target_price=102, current_price=100, expiry=date(2026, 11, 11),
        confidence=.7, expected_move_pct=2, model_name="fixture", model_version="1",
    )


def test_generation_commits_bounded_batches_with_distinct_completion_clocks(monkeypatch):
    from oracle import hallucination_guard

    oracle = _batch_oracle(monkeypatch)
    events, clocks = [], []
    current = [datetime(2026, 11, 10, 17, tzinfo=timezone.utc)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return current[0]

    monkeypatch.setattr(oe, "datetime", Clock)

    def infer(idx, ticker, total, now, cache, **kw):
        events.append("input:" + ticker)
        current[0] += timedelta(seconds=15)
        return [_batch_prediction(ticker, now)]

    def guard(preds, **kw):
        events.append("guard")
        current[0] += timedelta(seconds=10)
        return preds, []

    def store(preds):
        events.append("store:" + ",".join(p.ticker for p in preds))
        assert all(p.timestamp == current[0] for p in preds)
        clocks.append(current[0])
        oracle._last_store_counts = {"inserted": len(preds), "qualified": len(preds),
                                    "predictions": len(preds), "duplicates": 1,
                                    "abstentions": 0, "rejected": 1, "upsert_statements": len(preds)}
        return preds

    oracle._oracle_one_ticker = infer
    oracle._store_predictions = store
    monkeypatch.setattr(hallucination_guard, "verify_predictions", guard)
    result = oracle.generate_predictions(["A", "B", "C", "D", "E"], horizon_days=1)
    assert len(result) == 5
    assert events.index("store:A,B") < events.index("input:C")
    assert events.index("store:C,D") < events.index("input:E")
    assert clocks[0] < clocks[1] < clocks[2]
    assert oracle._last_store_counts["inserted"] == 5
    assert oracle._last_store_counts["duplicates"] == 3
    assert oracle._last_store_counts["rejected"] == 3
    assert oracle._last_store_counts["completed_batches"] == 3


@pytest.mark.parametrize("expire_during", ["input", "guard"])
def test_generation_abandoned_worker_cannot_start_storage(monkeypatch, expire_during):
    from oracle import hallucination_guard

    oracle = _batch_oracle(monkeypatch)
    allowed = [True]

    def infer(idx, ticker, total, now, cache, **kw):
        if expire_during == "input":
            allowed[0] = False
        return [_batch_prediction(ticker, now)]

    def guard(preds, **kw):
        allowed[0] = False
        return preds, []

    oracle._oracle_one_ticker = infer
    oracle._store_predictions = lambda *a, **kw: pytest.fail("Late worker reached storage")
    if expire_during == "guard":
        monkeypatch.setattr(hallucination_guard, "verify_predictions", guard)
    with pytest.raises(TimeoutError):
        oracle.generate_predictions(["A"], should_continue=lambda: allowed[0])
    assert oracle._last_store_counts["inserted"] == 0


def test_generation_failed_later_batch_preserves_only_committed_counts(monkeypatch):
    oracle = _batch_oracle(monkeypatch)
    oracle._oracle_one_ticker = lambda idx, ticker, total, now, cache, **kw: [_batch_prediction(ticker, now)]
    calls = []

    def store(preds, *, should_continue):
        assert should_continue()
        calls.append(preds)
        if len(calls) == 2:
            raise RuntimeError("synthetic storage failure")
        oracle._last_store_counts = {"inserted": 2, "qualified": 2, "predictions": 2}
        return preds

    oracle._store_predictions = store
    with pytest.raises(RuntimeError, match="synthetic storage failure"):
        oracle.generate_predictions(["A", "B", "C"], should_continue=lambda: True)
    assert oracle._last_store_counts["inserted"] == 2
    assert oracle._last_store_counts["completed_batches"] == 1


def test_generation_guard_failure_cannot_publish_unguarded_forecasts(monkeypatch):
    from oracle import hallucination_guard

    oracle = _batch_oracle(monkeypatch)
    oracle._oracle_one_ticker = lambda idx, ticker, total, now, cache, **kw: [_batch_prediction(ticker, now)]
    oracle._store_predictions = lambda *a, **kw: pytest.fail("Unguarded batch reached storage")

    def failed_guard(*a, **kw):
        raise RuntimeError("synthetic verification failure")

    monkeypatch.setattr(hallucination_guard, "verify_predictions", failed_guard)
    with pytest.raises(RuntimeError, match="batch not published"):
        oracle.generate_predictions(["A"])
    assert oracle._last_store_counts["inserted"] == 0
