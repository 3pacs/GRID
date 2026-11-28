"""Trade post-mortems: a missing return is not a measured 0% return (#959, Codex hold).

Same defect class as the prediction path (#1486): `float(actual_return) if actual_return else 0.0`
stored a LOSS/EXPIRED row with no recorded return as a measured 0%, narrated "Return: +0.00%",
averaged it into the lessons report and emitted it as realized_pnl.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

import intelligence.postmortem as pm_mod


class _Row:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeEngine:
    def __init__(self, row):
        self._row = row

    @contextmanager
    def connect(self):
        row = self._row

        class _Conn:
            def execute(self, *_a, **_k):
                return _Row(row)

        yield _Conn()


def _generate(monkeypatch, actual_return, outcome="LOSS"):
    row = (7, "SPY", "CALL", 700.0, "2026-12-18", 1.0, 2.0, 0.5, 0.6, "thesis",
           json.dumps({}), "2026-11-20T16:00:00Z", outcome, actual_return,
           datetime(2026, 11, 21, 21, 0, tzinfo=timezone.utc), "", None, None)
    seen: dict = {}
    stored: list = []
    emitted: list = []
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    # A known close inside the event window on a fixed clock: only a known recent miss emits (2026-11-27).
    monkeypatch.setattr(pm_mod, "_utcnow", lambda: datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(pm_mod, "_reconstruct_decision_data", lambda _e, _t, _g, s, d: {})
    monkeypatch.setattr(pm_mod, "_load_price_path_with_source", lambda *_a, **_k: ([], None))
    monkeypatch.setattr(pm_mod, "_load_underlying_entry_spot", lambda *_a, **_k: (100.0, "fixture"))
    monkeypatch.setattr(pm_mod, "_get_llm_postmortem", lambda **kw: seen.update(kw) or {})
    monkeypatch.setattr(pm_mod, "_store_postmortem", lambda _e, pm, **_k: stored.append(pm))
    monkeypatch.setattr(pm_mod, "_emit_trade_postmortem", lambda **k: emitted.append(k))
    pm = pm_mod.generate_postmortem(_FakeEngine(row), 7)
    return pm, seen, stored, emitted


@pytest.mark.parametrize("missing", [None, float("nan"), float("inf"), "", "n/a"])
@pytest.mark.parametrize("outcome", ["LOSS", "EXPIRED"])
def test_trade_failure_without_a_return_writes_nothing(monkeypatch, missing, outcome):
    pm, seen, stored, emitted = _generate(monkeypatch, missing, outcome)
    assert pm is None
    assert stored == []      # no zero persisted
    assert emitted == []     # no realized_pnl event
    assert seen == {}        # the LLM was never asked to narrate "Return: +0.00%"


def test_trade_failure_with_a_measured_zero_return_is_still_written(monkeypatch):
    pm, seen, stored, emitted = _generate(monkeypatch, 0.0)
    assert pm is not None and pm.actual_return == 0.0
    assert stored == [pm] and len(emitted) == 1


def test_trade_failure_with_a_return_keeps_it(monkeypatch):
    pm, seen, stored, _ = _generate(monkeypatch, -0.62)
    assert pm.actual_return == pytest.approx(-0.62)
    assert seen["actual_return"] == pytest.approx(-0.62)


def test_batch_selection_skips_trades_with_no_recorded_return(monkeypatch):
    seen_sql: list[str] = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, stmt, params=None):
            seen_sql.append(" ".join(str(stmt).split()))
            res = MagicMock()
            res.fetchall.return_value = []
            return res

    class _Engine:
        def connect(self):
            return _Conn()

    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    pm_mod.batch_postmortem(_Engine(), days=30, limit=5)
    trade_sql = [q for q in seen_sql if "FROM options_recommendations" in q and "'LOSS', 'EXPIRED'" in q]
    assert trade_sql and all("actual_return IS NOT NULL" in q for q in trade_sql)
    # NaN (and +/-Infinity where NUMERIC allows it) must not fill the LIMIT either.
    assert all("ABS(actual_return) < 1e300" in q for q in trade_sql)
    # Compared in NUMERIC: a float8 cast raises out of range on e.g. 1e400 and aborts the whole selection.
    assert all("::float8" not in q.split("actual_return", 1)[1] for q in trade_sql)


def _real_pm(ret):
    return pm_mod.PostMortem(
        trade_id=7, ticker="SPY", direction="CALL", outcome="LOSS", actual_return=ret,
        data_at_decision={}, thesis_at_decision="", sanity_results_at_decision={},
        what_actually_happened="", price_path=[], failure_category="wrong_signal",
        root_cause="rc", which_signals_were_wrong=[], which_signals_were_right=[],
        what_we_missed="", recommended_fix="", confidence_in_analysis=0.5,
        generated_at="2026-11-26T00:00:00+00:00",
    )


def _spy_emit(monkeypatch):
    import sys

    emit_mod = sys.modules.get("contracts.emit")
    if emit_mod is None:
        import contracts.emit as _imported  # noqa: F401
        emit_mod = sys.modules["contracts.emit"]
    emitted: list = []
    monkeypatch.setattr(emit_mod, "emit", lambda c: emitted.append(c) or c.event_id)
    return emitted


@pytest.mark.parametrize("emit_fn", ["trade", "prediction"])
def test_emitters_emit_a_real_zero_pnl(monkeypatch, emit_fn):
    # Positive control: a measured 0.0 is a value and still emits.
    emitted = _spy_emit(monkeypatch)
    if emit_fn == "trade":
        pm_mod._emit_trade_postmortem(pm=_real_pm(0.0), trade_id=7)
    else:
        pm_mod._emit_prediction_postmortem(pm=_real_pm(0.0), prediction_id="pred-1", model_name="m")
    assert len(emitted) == 1 and float(emitted[0].realized_pnl) == 0.0


@pytest.mark.parametrize("bad", [None, float("nan"), float("inf")])
@pytest.mark.parametrize("emit_fn", ["trade", "prediction"])
def test_emitters_never_invent_a_zero_pnl(monkeypatch, emit_fn, bad):
    # Defence in depth: a post-mortem that somehow carries no finite return emits nothing,
    # it does not emit realized_pnl = 0.
    emitted = _spy_emit(monkeypatch)
    if emit_fn == "trade":
        pm_mod._emit_trade_postmortem(pm=_real_pm(bad), trade_id=7)
    else:
        pm_mod._emit_prediction_postmortem(pm=_real_pm(bad), prediction_id="pred-1", model_name="m")
    assert emitted == []
