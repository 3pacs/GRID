"""CAT-190 — thesis invalidation monitor tests."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from intelligence.thesis_invalidation_monitor import (
    INVAL_EVENT,
    INVAL_PRICE_LEVEL,
    INVAL_SIGNAL_FLIP,
    InvalidationEvent,
    MonitorRun,
    determine_size_down,
    evaluate_condition,
    evaluate_event,
    evaluate_price_level,
    evaluate_signal_flip,
    run_monitor,
)


_NOW = datetime(2026, 4, 13, tzinfo=timezone.utc)


# ── Price-level tests ────────────────────────────────────────────────────


class TestEvaluatePriceLevel:
    def test_below_triggered(self):
        cond = {"operator": "below", "threshold": 180.0}
        triggered, reason = evaluate_price_level(cond, current_price=175.0)
        assert triggered is True
        assert "175.00" in reason

    def test_below_not_triggered(self):
        cond = {"operator": "below", "threshold": 180.0}
        triggered, _ = evaluate_price_level(cond, current_price=185.0)
        assert triggered is False

    def test_above_triggered(self):
        cond = {"operator": "above", "threshold": 100.0}
        triggered, _ = evaluate_price_level(cond, current_price=105.0)
        assert triggered is True

    def test_triggers_on_close_uses_close_price(self):
        cond = {
            "operator": "below", "threshold": 180.0,
            "triggers_on_close": True,
        }
        # Intraday price below threshold but close is above → NOT triggered
        triggered, _ = evaluate_price_level(
            cond, current_price=175.0, last_close_price=185.0,
        )
        assert triggered is False

    def test_malformed_returns_false(self):
        assert evaluate_price_level({}, current_price=100.0)[0] is False
        assert evaluate_price_level(
            {"operator": "below"}, current_price=100.0,
        )[0] is False

    def test_non_numeric_returns_false(self):
        cond = {"operator": "below", "threshold": "junk"}
        assert evaluate_price_level(cond, current_price=100.0)[0] is False


class TestEvaluateEvent:
    def test_event_in_recent(self):
        cond = {"event_name": "fomc_hawkish", "window_days": 7}
        triggered, _ = evaluate_event(
            cond, recent_events=["fomc_hawkish", "cpi_beat"], as_of=_NOW,
        )
        assert triggered is True

    def test_event_not_in_recent(self):
        cond = {"event_name": "fomc_hawkish"}
        triggered, _ = evaluate_event(
            cond, recent_events=["nothing_relevant"], as_of=_NOW,
        )
        assert triggered is False

    def test_missing_event_name(self):
        triggered, reason = evaluate_event(
            {}, recent_events=[], as_of=_NOW,
        )
        assert triggered is False
        assert "missing" in reason


class TestEvaluateSignalFlip:
    def test_flip_triggered(self):
        cond = {"from_state": "EASY", "to_state": "TIGHT"}
        triggered, _ = evaluate_signal_flip(
            cond, current_state="TIGHT", prior_state="EASY",
        )
        assert triggered is True

    def test_no_flip_static(self):
        cond = {"from_state": "EASY", "to_state": "TIGHT"}
        triggered, _ = evaluate_signal_flip(
            cond, current_state="TIGHT", prior_state="TIGHT",
        )
        assert triggered is False

    def test_missing_states_returns_false(self):
        cond = {"from_state": "EASY", "to_state": "TIGHT"}
        assert evaluate_signal_flip(
            cond, current_state=None, prior_state="EASY",
        )[0] is False

    def test_case_insensitive(self):
        cond = {"from_state": "easy", "to_state": "tight"}
        triggered, _ = evaluate_signal_flip(
            cond, current_state="TIGHT", prior_state="EASY",
        )
        assert triggered is True


# ── Dispatcher ──────────────────────────────────────────────────────────


class TestEvaluateCondition:
    def test_unknown_type(self):
        triggered, reason = evaluate_condition({"type": "weird"})
        assert triggered is False
        assert "unknown" in reason

    def test_price_level_dispatched(self):
        cond = {"type": "price_level", "operator": "below", "threshold": 100.0}
        triggered, _ = evaluate_condition(cond, current_price=95.0)
        assert triggered is True

    def test_event_dispatched(self):
        cond = {"type": "event", "event_name": "fomc_hawkish"}
        triggered, _ = evaluate_condition(
            cond, recent_events=["fomc_hawkish"],
        )
        assert triggered is True

    def test_signal_flip_dispatched(self):
        cond = {"type": "signal_flip", "from_state": "EASY", "to_state": "TIGHT"}
        triggered, _ = evaluate_condition(
            cond, current_state="TIGHT", prior_state="EASY",
        )
        assert triggered is True

    def test_no_price_data_price_level(self):
        cond = {"type": "price_level", "operator": "below", "threshold": 100.0}
        triggered, reason = evaluate_condition(cond)
        assert triggered is False
        assert "no price" in reason.lower()


class TestDetermineSizeDown:
    def test_event_shrinks_to_20pct(self):
        assert determine_size_down(INVAL_EVENT) == 0.20

    def test_price_level_closes(self):
        assert determine_size_down(INVAL_PRICE_LEVEL) == 0.0

    def test_signal_flip_closes(self):
        assert determine_size_down(INVAL_SIGNAL_FLIP) == 0.0


# ── run_monitor integration (mocked DB) ──────────────────────────────────


class TestRunMonitor:
    def _build_engine(self, predictions_rows, price_rows=None):
        eng = MagicMock()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False

        call_count = [0]

        def execute(query, params=None):
            sql = str(query)
            result = MagicMock()
            if "FROM decision_journal" in sql:
                result.fetchall.return_value = predictions_rows
                result.fetchone.return_value = predictions_rows[0] if predictions_rows else None
            elif "FROM ticker_metrics_daily" in sql:
                ticker = params.get("t") if params else None
                result.fetchone.return_value = (price_rows or {}).get(ticker)
            else:
                result.fetchall.return_value = []
                result.fetchone.return_value = None
            call_count[0] += 1
            return result

        conn.execute = execute
        eng.connect.return_value = conn
        return eng

    def test_empty_scan(self):
        eng = self._build_engine([])
        run = run_monitor(eng, as_of=_NOW)
        assert run.predictions_scanned == 0
        assert run.triggered_count == 0

    def test_price_level_triggered(self):
        preds = [(
            42, "AAPL",
            {"invalidation": {
                "type": "price_level",
                "operator": "below",
                "threshold": 180.0,
            }},
            _NOW,
        )]
        prices = {"AAPL": (175.0,)}
        eng = self._build_engine(preds, prices)
        run = run_monitor(eng, as_of=_NOW)
        assert run.predictions_scanned == 1
        assert run.triggered_count == 1
        assert run.events[0].inval_type == INVAL_PRICE_LEVEL
        assert run.events[0].auto_size_down_to == 0.0

    def test_price_level_not_triggered(self):
        preds = [(
            42, "AAPL",
            {"invalidation": {
                "type": "price_level",
                "operator": "below",
                "threshold": 180.0,
            }},
            _NOW,
        )]
        prices = {"AAPL": (185.0,)}
        eng = self._build_engine(preds, prices)
        run = run_monitor(eng, as_of=_NOW)
        assert run.triggered_count == 0

    def test_event_triggered_with_shrink(self):
        preds = [(
            99, "SPY",
            {"invalidation": {"type": "event", "event_name": "fomc_hawkish"}},
            _NOW,
        )]
        eng = self._build_engine(preds, {})
        run = run_monitor(
            eng, as_of=_NOW, recent_events=["fomc_hawkish"],
        )
        assert run.triggered_count == 1
        assert run.events[0].auto_size_down_to == 0.20

    def test_signal_flip_triggered(self):
        preds = [(
            100, "SPY",
            {"invalidation": {
                "type": "signal_flip",
                "from_state": "EASY",
                "to_state": "TIGHT",
            }},
            _NOW,
        )]
        eng = self._build_engine(preds, {})
        run = run_monitor(
            eng, as_of=_NOW,
            current_regime_state="TIGHT",
            prior_regime_state="EASY",
        )
        assert run.triggered_count == 1

    def test_malformed_invalidation_recorded_as_error(self):
        preds = [(1, "X", {"invalidation": "not a dict"}, _NOW)]
        eng = self._build_engine(preds, {})
        run = run_monitor(eng, as_of=_NOW)
        assert run.triggered_count == 0
        assert len(run.errors) == 1


class TestMonitorRunSerialization:
    def test_to_dict_shape(self):
        run = MonitorRun(as_of=_NOW, predictions_scanned=5)
        d = run.to_dict()
        for k in ("as_of", "predictions_scanned", "triggered_count",
                  "events", "errors"):
            assert k in d

    def test_event_to_dict(self):
        ev = InvalidationEvent(
            journal_id=42, ticker="AAPL", inval_type=INVAL_PRICE_LEVEL,
            triggered_at=_NOW, reason="close 170 < 180",
            current_value=170.0, threshold_value=180.0,
            auto_size_down_to=0.0,
        )
        d = ev.to_dict()
        assert d["journal_id"] == 42
        assert d["ticker"] == "AAPL"
