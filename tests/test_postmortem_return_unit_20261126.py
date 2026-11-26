"""PostMortem.actual_return is a fraction on both postmortem paths.

oracle_predictions.pnl_pct is stored in percent points (-50.24 = -50.24%);
options_recommendations.actual_return is a fraction. The prediction path used
to copy pnl_pct unconverted, so the ``{:+.2%}`` formatters printed -5024.00%
and the lessons report averaged the two units together ("Average return:
-841.60%" for five prediction misses averaging -8.4%).

Offline: a fake engine and mocked emit / LLM; nothing opens a database.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from intelligence import postmortem as pm_mod
from intelligence.postmortem import PostMortem, generate_lessons_learned, generate_prediction_postmortem


def _prediction_engine(pnl_pct):
    created = datetime(2026, 4, 10, 14, 0, tzinfo=timezone.utc)
    scored = datetime(2026, 5, 11, 21, 0, tzinfo=timezone.utc)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, stmt, params=None):
            sql = " ".join(str(stmt).split())
            res = MagicMock()
            if "FROM oracle_predictions" in sql:
                # entry 94.07 -> 141.33 on a PUT: move 50.24, pnl -50.24 (percent points)
                res.fetchone.return_value = (
                    "pred-1", "INSM", "PUT", 92.48, 94.07, date(2026, 5, 11),
                    0.7, 5.0, "some_model",
                    json.dumps([{"name": "iv_skew", "direction": "bearish"}]),
                    "[]", "{}", "miss", 141.33, 50.24, pnl_pct, scored, "missed",
                    created,
                )
            else:
                res.fetchone.return_value = None
                res.fetchall.return_value = []
            return res

    class _Engine:
        def connect(self):
            return _Conn()

        def begin(self):
            return _Conn()

    return _Engine()


def _setup(monkeypatch):
    captured: list = []
    emit_mod = sys.modules.get("contracts.emit")
    if emit_mod is None:
        import contracts.emit as _imported  # noqa: F401
        emit_mod = sys.modules["contracts.emit"]
    monkeypatch.setattr(emit_mod, "emit", lambda c: captured.append(c) or c.event_id)
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: None)
    seen: dict = {}

    def _llm(**k):
        seen.update(k)
        return {"recommended_fix": k["root_cause"], "what_we_missed": "", "confidence": 0.6}

    monkeypatch.setattr(pm_mod, "_get_llm_postmortem", _llm)
    return captured, seen


def test_prediction_path_stores_a_fraction_and_narrates_the_true_percent(monkeypatch):
    captured, seen = _setup(monkeypatch)
    pm = generate_prediction_postmortem(_prediction_engine(-50.24), prediction_id="pred-1")
    assert pm is not None
    assert pm.actual_return == pytest.approx(-0.5024)
    assert seen["actual_return"] == pytest.approx(-0.5024)
    assert "Return: -50.24%" in pm.what_actually_happened
    assert "-5024" not in pm.what_actually_happened + pm.root_cause
    assert len(captured) == 1
    assert float(captured[0].realized_pnl) == pytest.approx(-0.5024)


def test_prediction_path_without_a_pnl_is_zero_not_a_scaled_value(monkeypatch):
    _setup(monkeypatch)
    pm = generate_prediction_postmortem(_prediction_engine(None), prediction_id="pred-1")
    assert pm is not None
    assert pm.actual_return == 0.0


def _pm(ret: float) -> PostMortem:
    return PostMortem(
        trade_id=0, ticker="X", direction="PUT", outcome="miss", actual_return=ret,
        data_at_decision={}, thesis_at_decision="", sanity_results_at_decision={},
        what_actually_happened="", price_path=[], failure_category="wrong_signal",
        root_cause="", which_signals_were_wrong=[], which_signals_were_right=[],
        what_we_missed="", recommended_fix="", confidence_in_analysis=0.5,
        generated_at="2026-11-26T00:00:00+00:00",
    )


def test_lessons_average_over_trade_and_prediction_rows_is_one_unit(monkeypatch):
    monkeypatch.setattr(pm_mod, "_get_llm_lessons_learned", lambda *a, **k: None)
    prediction = [_pm(-0.0381), _pm(-0.0039), _pm(-0.5024), _pm(-0.0373), _pm(-0.1037)]
    trade = [_pm(-1.0)]
    report = generate_lessons_learned(MagicMock(), prediction + trade)
    # mean of the six fractions = -0.2809 -> -28.09%
    assert "Average return:    -28.09%" in report
    assert "-841" not in report
