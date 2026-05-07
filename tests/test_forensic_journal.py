"""Tests for intelligence/forensic_journal.py — CAT-189."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from intelligence.forensic_journal import (
    FAILURE_MULTIPLIER_FORMULA,
    HIGH_CONFIDENCE_THRESHOLD,
    MAX_FAILURE_MULTIPLIER,
    ROOT_CAUSE_CATEGORIES,
    FailedPredictionPostmortem,
    FailingSignal,
    _apply_failure_multiplier_to_signals,
    classify_root_cause,
    compose_narrative_template,
    compute_failure_multiplier,
    get_failing_signals,
    get_recent_postmortems,
    is_high_confidence_failure,
    record_failure,
)


# ── FakeEngine plumbing (mirrors tests/test_strategy.py) ──────────────────


class FakeResult:
    def __init__(self, rows: list[Any] | None = None, rowcount: int = 1) -> None:
        self._rows = rows or []
        self.rowcount = rowcount

    def fetchall(self) -> list[Any]:
        return self._rows

    def fetchone(self) -> Any | None:
        return self._rows[0] if self._rows else None


class FakeConnection:
    """Mock SQLAlchemy connection. Records every execute() call."""

    def __init__(self, rows_for_select: list[Any] | None = None) -> None:
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self._rows_for_select = rows_for_select or []
        self.fail_on_select = False

    def execute(self, stmt: Any, params: dict[str, Any] | None = None) -> FakeResult:
        sql = str(stmt)
        self.executed.append((sql, params or {}))
        if "SELECT" in sql.upper():
            if self.fail_on_select:
                raise RuntimeError("fake select failure")
            return FakeResult(rows=self._rows_for_select)
        return FakeResult(rowcount=1)

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class FakeEngine:
    """Mock SQLAlchemy engine. .begin() and .connect() both return the same connection."""

    def __init__(self, rows_for_select: list[Any] | None = None) -> None:
        self.connection = FakeConnection(rows_for_select=rows_for_select)
        self.begin_calls = 0
        self.connect_calls = 0

    def begin(self) -> FakeConnection:
        self.begin_calls += 1
        return self.connection

    def connect(self) -> FakeConnection:
        self.connect_calls += 1
        return self.connection


class ExplodingEngine:
    """Engine where every begin/connect raises — used for DB-error tolerance tests."""

    def begin(self) -> Any:
        raise RuntimeError("DB unavailable")

    def connect(self) -> Any:
        raise RuntimeError("DB unavailable")


# ── is_high_confidence_failure ────────────────────────────────────────────


class TestIsHighConfidenceFailure:
    def test_high_conf_miss_is_failure(self) -> None:
        assert is_high_confidence_failure(0.85, "miss") is True

    def test_high_conf_hit_is_not_failure(self) -> None:
        assert is_high_confidence_failure(0.85, "hit") is False

    def test_low_conf_miss_is_not_failure(self) -> None:
        assert is_high_confidence_failure(0.65, "miss") is False

    def test_threshold_exact_miss_is_failure(self) -> None:
        # exact 0.7 + miss should count
        assert is_high_confidence_failure(HIGH_CONFIDENCE_THRESHOLD, "miss") is True

    def test_none_inputs_are_not_failures(self) -> None:
        assert is_high_confidence_failure(None, "miss") is False  # type: ignore[arg-type]
        assert is_high_confidence_failure(0.9, None) is False  # type: ignore[arg-type]


# ── compute_failure_multiplier ────────────────────────────────────────────


class TestComputeFailureMultiplier:
    def test_anchor_07_is_1x(self) -> None:
        assert compute_failure_multiplier(0.7) == pytest.approx(1.0, abs=1e-9)

    def test_anchor_085_is_3x(self) -> None:
        assert compute_failure_multiplier(0.85) == pytest.approx(3.0, abs=1e-9)

    def test_anchor_10_is_5x(self) -> None:
        assert compute_failure_multiplier(1.0) == pytest.approx(5.0, abs=1e-9)

    def test_below_threshold_clamped_to_1x(self) -> None:
        assert compute_failure_multiplier(0.6) == pytest.approx(1.0, abs=1e-9)
        assert compute_failure_multiplier(0.0) == pytest.approx(1.0, abs=1e-9)

    def test_above_one_clamped_to_max(self) -> None:
        assert compute_failure_multiplier(1.5) == pytest.approx(MAX_FAILURE_MULTIPLIER, abs=1e-9)
        assert compute_failure_multiplier(99.0) == pytest.approx(MAX_FAILURE_MULTIPLIER, abs=1e-9)


# ── classify_root_cause ───────────────────────────────────────────────────


class TestClassifyRootCause:
    def test_crowd_aligned_in_bullish_regime(self) -> None:
        meta = {"crowd_aligned": True, "regime": "bullish"}
        cat, evidence = classify_root_cause(meta)
        assert cat == "crowd_aligned"
        assert "crowd_aligned" in evidence

    def test_fragility_below_threshold(self) -> None:
        meta = {"fragility_multiplier": 0.5}
        cat, evidence = classify_root_cause(meta)
        assert cat == "single_leg_fragile"
        assert "0.50" in evidence or "0.5" in evidence

    def test_regime_mismatch_is_regime_shift(self) -> None:
        meta = {"regime": "bullish", "fci_regime": "bearish"}
        cat, evidence = classify_root_cause(meta)
        assert cat == "regime_shift"
        assert "bullish" in evidence and "bearish" in evidence

    def test_default_is_unknown(self) -> None:
        cat, evidence = classify_root_cause({})
        assert cat == "unknown"
        assert cat in ROOT_CAUSE_CATEGORIES

    def test_data_age_triggers_data_stale(self) -> None:
        cat, _ = classify_root_cause({"data_age_hours": 48})
        assert cat == "data_stale"


# ── compose_narrative_template ────────────────────────────────────────────


class TestComposeNarrativeTemplate:
    def test_template_contains_key_fields(self) -> None:
        pm = FailedPredictionPostmortem(
            prediction_id="p-001",
            ticker="AAPL",
            confidence=0.85,
            verdict="miss",
            horizon_days=5,
            asof=datetime(2026, 4, 1, tzinfo=timezone.utc),
            contributing_signals={"insider": 0.4, "darkpool": 0.3},
            root_cause="crowd_aligned",
            root_cause_evidence="crowd_aligned=True in regime=bullish",
            failure_multiplier=3.0,
            narrative=None,
            generated_at="2026-04-01T00:00:00+00:00",
        )
        text = compose_narrative_template(pm)
        assert "AAPL" in text
        assert "0.85" in text
        assert "miss" in text
        assert "crowd_aligned" in text
        assert "insider" in text
        assert "darkpool" in text
        assert "3.00" in text


# ── record_failure ────────────────────────────────────────────────────────


class TestRecordFailure:
    def _hit_row(self) -> dict[str, Any]:
        return {
            "prediction_id": "p-hit",
            "ticker": "MSFT",
            "confidence": 0.85,
            "verdict": "hit",
            "horizon_days": 5,
            "asof": datetime(2026, 4, 1, tzinfo=timezone.utc),
            "metadata": {},
        }

    def _low_conf_miss_row(self) -> dict[str, Any]:
        return {
            "prediction_id": "p-low",
            "ticker": "GOOG",
            "confidence": 0.55,
            "verdict": "miss",
            "horizon_days": 3,
            "asof": datetime(2026, 4, 1, tzinfo=timezone.utc),
            "metadata": {},
        }

    def _high_conf_miss_row(self) -> dict[str, Any]:
        return {
            "prediction_id": "p-bad",
            "ticker": "TSLA",
            "confidence": 0.9,
            "verdict": "miss",
            "horizon_days": 7,
            "asof": datetime(2026, 4, 1, tzinfo=timezone.utc),
            "metadata": {"fragility_multiplier": 0.5},
        }

    def test_returns_none_on_hit(self) -> None:
        engine = FakeEngine()
        result = record_failure(engine, self._hit_row(), signal_contributions={"a": 0.5})
        assert result is None

    def test_returns_none_on_low_conf_miss(self) -> None:
        engine = FakeEngine()
        result = record_failure(engine, self._low_conf_miss_row(), signal_contributions={"a": 0.5})
        assert result is None

    def test_returns_postmortem_on_high_conf_miss(self) -> None:
        engine = FakeEngine()
        result = record_failure(
            engine,
            self._high_conf_miss_row(),
            signal_contributions={"insider": 0.6, "darkpool": 0.3},
        )
        assert result is not None
        assert isinstance(result, FailedPredictionPostmortem)
        assert result.ticker == "TSLA"
        assert result.confidence == pytest.approx(0.9)
        assert result.verdict == "miss"
        assert result.failure_multiplier > 1.0
        # fragility metadata → single_leg_fragile category
        assert result.root_cause == "single_leg_fragile"
        assert result.narrative is not None
        assert "TSLA" in result.narrative

    def test_record_failure_calls_apply_multiplier_with_correct_inputs(self) -> None:
        engine = FakeEngine()
        contributions = {"insider": 0.6, "darkpool": 0.3}
        record_failure(
            engine,
            self._high_conf_miss_row(),
            signal_contributions=contributions,
        )
        # Verify per_signal_brier_history UPDATEs were issued for both signals
        update_stmts = [
            (sql, params)
            for sql, params in engine.connection.executed
            if "UPDATE per_signal_brier_history" in sql
        ]
        assert len(update_stmts) == 2
        sources = {p["source"] for _, p in update_stmts}
        assert sources == {"insider", "darkpool"}
        for _, p in update_stmts:
            assert p["horizon"] == 7
            # multiplier for confidence=0.9 → 1 + 4*(0.2)/0.3 ≈ 3.667
            assert p["multiplier"] == pytest.approx(compute_failure_multiplier(0.9))

    def test_db_error_in_record_failure_returns_none_no_raise(self) -> None:
        engine = ExplodingEngine()
        # Should not raise even though every begin/connect blows up.
        result = record_failure(
            engine,
            self._high_conf_miss_row(),
            signal_contributions={"insider": 0.6},
        )
        # high-conf miss still yields a postmortem object; persistence failures are swallowed
        assert result is not None
        assert result.ticker == "TSLA"


# ── _apply_failure_multiplier_to_signals ──────────────────────────────────


class TestApplyFailureMultiplier:
    def test_updates_per_signal_brier_with_multiplier_and_weight(self) -> None:
        engine = FakeEngine()
        n = _apply_failure_multiplier_to_signals(
            engine,
            signal_contributions={"insider": 0.4, "social": 0.2},
            horizon_days=5,
            multiplier=3.0,
        )
        assert n == 2
        update_stmts = [
            (sql, params)
            for sql, params in engine.connection.executed
            if "UPDATE per_signal_brier_history" in sql
        ]
        assert len(update_stmts) == 2
        # SQL should reference all the right pieces
        sql_text = update_stmts[0][0]
        assert "running_brier" in sql_text
        assert ":multiplier" in sql_text
        assert ":weight" in sql_text
        params_by_source = {p["source"]: p for _, p in update_stmts}
        assert params_by_source["insider"]["weight"] == pytest.approx(0.4)
        assert params_by_source["insider"]["multiplier"] == pytest.approx(3.0)
        assert params_by_source["social"]["weight"] == pytest.approx(0.2)

    def test_zero_weight_signals_skipped(self) -> None:
        engine = FakeEngine()
        n = _apply_failure_multiplier_to_signals(
            engine,
            signal_contributions={"a": 0.0, "b": 0.5},
            horizon_days=5,
            multiplier=2.0,
        )
        assert n == 1

    def test_empty_contributions_returns_zero(self) -> None:
        engine = FakeEngine()
        n = _apply_failure_multiplier_to_signals(
            engine, signal_contributions={}, horizon_days=5, multiplier=2.0,
        )
        assert n == 0


# ── get_failing_signals ───────────────────────────────────────────────────


def _make_postmortem_row(sigs: dict[str, float], days_ago: int = 1) -> dict[str, Any]:
    """A row shaped like the SELECT in get_failing_signals returns."""
    import json
    return {
        "contributing_signals": json.dumps(sigs),
        "failure_multiplier": 3.0,
        "generated_at": datetime.now(timezone.utc) - timedelta(days=days_ago),
    }


class _DictRow:
    """Row that supports both [key] and [int] indexing like SQLAlchemy Row."""

    def __init__(self, mapping: dict[str, Any]) -> None:
        self._mapping = mapping
        self._values = list(mapping.values())

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str):
            return self._mapping[key]
        return self._values[key]


class TestGetFailingSignals:
    def test_aggregates_and_classifies(self) -> None:
        # Source A: 1 failure → cooling
        # Source B: 4 failures → cold
        # Source C: 7 failures → frozen
        rows: list[_DictRow] = []
        rows.append(_DictRow(_make_postmortem_row({"A": 0.5})))
        for _ in range(4):
            rows.append(_DictRow(_make_postmortem_row({"B": 0.5})))
        for _ in range(7):
            rows.append(_DictRow(_make_postmortem_row({"C": 0.5})))

        engine = FakeEngine(rows_for_select=rows)
        result = get_failing_signals(engine, window_days=14)

        by_source = {r.signal_source: r for r in result}
        assert "A" in by_source
        assert "B" in by_source
        assert "C" in by_source
        assert by_source["A"].recent_failure_count == 1
        assert by_source["A"].classification == "cooling"
        assert by_source["B"].recent_failure_count == 4
        assert by_source["B"].classification == "cold"
        assert by_source["C"].recent_failure_count == 7
        assert by_source["C"].classification == "frozen"

    def test_empty_table_returns_empty_list(self) -> None:
        engine = FakeEngine(rows_for_select=[])
        assert get_failing_signals(engine, window_days=14) == []

    def test_db_error_returns_empty(self) -> None:
        engine = FakeEngine()
        engine.connection.fail_on_select = True
        # Need to force first SELECT to raise — but the func has a fallback,
        # so make BOTH SELECTs fail by simulating an exploding engine instead.
        exploding = ExplodingEngine()
        assert get_failing_signals(exploding, window_days=14) == []


# ── get_recent_postmortems ────────────────────────────────────────────────


def _make_recent_pm_row(ticker: str = "AAPL", pid: str = "p-1") -> _DictRow:
    import json
    return _DictRow({
        "prediction_id": pid,
        "ticker": ticker,
        "confidence": 0.85,
        "verdict": "miss",
        "horizon_days": 5,
        "asof": datetime(2026, 4, 1, tzinfo=timezone.utc),
        "contributing_signals": json.dumps({"insider": 0.4}),
        "root_cause": "crowd_aligned",
        "root_cause_evidence": "test evidence",
        "failure_multiplier": 3.0,
        "narrative": "test narrative",
        "generated_at": datetime(2026, 4, 1, tzinfo=timezone.utc),
    })


class TestGetRecentPostmortems:
    def test_ticker_filter_uses_ticker_param(self) -> None:
        engine = FakeEngine(rows_for_select=[_make_recent_pm_row(ticker="AAPL")])
        result = get_recent_postmortems(engine, ticker="AAPL", limit=10)
        assert len(result) == 1
        assert result[0].ticker == "AAPL"
        # confirm ticker filter SQL was used and param was bound
        select_calls = [
            (sql, params)
            for sql, params in engine.connection.executed
            if "SELECT" in sql.upper() and "FROM failed_prediction_postmortems" in sql
        ]
        assert select_calls, "expected a SELECT call"
        sql, params = select_calls[0]
        assert "WHERE ticker = :ticker" in sql
        assert params["ticker"] == "AAPL"
        assert params["limit"] == 10

    def test_limit_is_passed_to_query(self) -> None:
        engine = FakeEngine(rows_for_select=[_make_recent_pm_row()])
        get_recent_postmortems(engine, ticker=None, limit=7)
        select_calls = [
            (sql, params)
            for sql, params in engine.connection.executed
            if "SELECT" in sql.upper() and "FROM failed_prediction_postmortems" in sql
        ]
        assert select_calls
        _, params = select_calls[0]
        assert params["limit"] == 7
        assert "ticker" not in params  # no filter

    def test_db_error_returns_empty(self) -> None:
        engine = ExplodingEngine()
        assert get_recent_postmortems(engine, ticker=None, limit=10) == []


# ── Frozen dataclass + to_dict roundtrip ──────────────────────────────────


class TestDataclassRoundtrip:
    def test_failed_prediction_postmortem_to_dict(self) -> None:
        pm = FailedPredictionPostmortem(
            prediction_id="p-1",
            ticker="AAPL",
            confidence=0.9,
            verdict="miss",
            horizon_days=5,
            asof=datetime(2026, 4, 1, tzinfo=timezone.utc),
            contributing_signals={"insider": 0.5},
            root_cause="single_leg_fragile",
            root_cause_evidence="frag=0.5",
            failure_multiplier=4.0,
            narrative="narr",
            generated_at="2026-04-01T00:00:00+00:00",
        )
        d = pm.to_dict()
        assert d["ticker"] == "AAPL"
        assert d["root_cause"] == "single_leg_fragile"
        assert d["asof"] == "2026-04-01T00:00:00+00:00"
        # frozen dataclass — cannot mutate
        with pytest.raises((AttributeError, Exception)):
            pm.ticker = "MSFT"  # type: ignore[misc]

    def test_failing_signal_to_dict(self) -> None:
        fs = FailingSignal(
            signal_source="insider",
            recent_failure_count=4,
            cumulative_failure_multiplier=12.0,
            last_failed_at=datetime(2026, 4, 1, tzinfo=timezone.utc),
            classification="cold",
        )
        d = fs.to_dict()
        assert d["signal_source"] == "insider"
        assert d["classification"] == "cold"
        assert d["last_failed_at"] == "2026-04-01T00:00:00+00:00"


# ── Sanity: constants ─────────────────────────────────────────────────────


class TestConstants:
    def test_root_cause_categories_has_all_5(self) -> None:
        assert len(ROOT_CAUSE_CATEGORIES) == 5
        for cat in ("regime_shift", "data_stale", "single_leg_fragile",
                    "crowd_aligned", "unknown"):
            assert cat in ROOT_CAUSE_CATEGORIES

    def test_failure_multiplier_formula_is_callable(self) -> None:
        assert callable(FAILURE_MULTIPLIER_FORMULA)
        assert FAILURE_MULTIPLIER_FORMULA(0.7) == pytest.approx(1.0)
