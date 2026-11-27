"""Every miss gets a postmortem record; LLM narration upgrades the newest (owner decision 2026-11-27).

Read-only measurement on 2026-11-27: 318,763 eligible Oracle misses had no postmortem. 317,899 were
scored in May 2026 and aged out of the 30-day batch window during the spring LLM bulk run; recent
coverage was 411 of 1,275. Narrating 319 K rows by LLM is 74-111 GPU-days; the rule-based classifier
alone is ~20 ms a row. So: a rule-based record for every failure (``analysis_method='rules'``),
narration in place for the newest, readers windowed by the miss's own date, at most one record per
miss, and no PostmortemCompleted (trust update, model-weight decay) for a miss older than the window.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DB_PASSWORD", "ci_placeholder_pw")

import intelligence.postmortem as pm_mod
from intelligence.postmortem import (
    ANALYSIS_LLM,
    ANALYSIS_RULES,
    LEGACY_LLM_FALLBACK_TEXT,
    UNNARRATED_SQL,
    generate_prediction_postmortem,
    miss_on_or_after_sql,
)


def _norm(stmt) -> str:
    return " ".join(str(stmt).split())


def _prediction_engine(scored_at, statements=None):
    """A fake engine that serves one scored miss (pnl -4.0 percent points)."""
    created = (scored_at or FIXED_NOW) - timedelta(days=7)

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, stmt, params=None):
            sql = _norm(stmt)
            if statements is not None:
                statements.append((sql, dict(params or {})))
            res = MagicMock()
            if "FROM (SELECT schwab_o.*" in sql and "WHERE id = :id" in sql:
                res.fetchone.return_value = (
                    "pred-1", "SPY", "CALL", 600.0, 590.0, date(2026, 11, 20),
                    0.7, 2.0, "some_model",
                    json.dumps([{"name": "iv_skew", "direction": "bullish"}]),
                    "[]", "{}", "miss", 580.0, -1.7, -4.0, scored_at, "missed", created,
                )
            else:
                res.fetchone.return_value = None
                res.fetchall.return_value = []
                res.rowcount = 1
            return res

    class _Engine:
        def connect(self):
            return _Conn()

        def begin(self):
            return _Conn()

    return _Engine()


@pytest.fixture
def harness(monkeypatch):
    emitted: list = []
    stored: list = []
    llm_calls: list = []
    emit_mod = sys.modules.get("contracts.emit")
    if emit_mod is None:
        import contracts.emit as _imported  # noqa: F401
        emit_mod = sys.modules["contracts.emit"]
    monkeypatch.setattr(emit_mod, "emit", lambda c: emitted.append(c) or c.event_id)
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)

    def _store(engine, pm, trade_id, prediction_id):
        stored.append(pm)
        return True

    monkeypatch.setattr(pm_mod, "_store_postmortem", _store)
    state = SimpleNamespace(emitted=emitted, stored=stored, llm_calls=llm_calls, llm_reply=None)

    def _llm(**k):
        llm_calls.append(k)
        return state.llm_reply

    monkeypatch.setattr(pm_mod, "_get_llm_postmortem", _llm)
    return state


# A fixed clock: no test here reads the real one (pm_mod._utcnow is pinned by the autouse fixture).
FIXED_NOW = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)
RECENT = FIXED_NOW - timedelta(days=2)
OLD = datetime(2026, 5, 20, 21, 0, tzinfo=timezone.utc)
EDGE_IN = FIXED_NOW - timedelta(days=pm_mod.POSTMORTEM_EVENT_WINDOW_DAYS)            # exactly at the edge: emits
EDGE_OUT = EDGE_IN - timedelta(seconds=1)


@pytest.fixture(autouse=True)
def _fixed_clock(monkeypatch):
    monkeypatch.setattr(pm_mod, "_utcnow", lambda: FIXED_NOW)


# ── the record ──────────────────────────────────────────────────────────────────────────────────────


def test_a_record_without_narration_never_calls_the_llm_and_says_it_is_rule_based(harness):
    pm = generate_prediction_postmortem(_prediction_engine(RECENT), "pred-1", narrate=False)
    assert pm is not None and harness.llm_calls == []
    assert pm.analysis_method == ANALYSIS_RULES
    assert pm.what_we_missed and pm.what_we_missed != LEGACY_LLM_FALLBACK_TEXT  # the classifier's own text
    assert pm.recommended_fix == pm.root_cause
    assert pm.confidence_in_analysis is None          # no analysis was made to be confident in
    assert pm.to_dict()["analysis_method"] == ANALYSIS_RULES
    assert pm.miss_at == RECENT.isoformat()


def test_an_unreachable_llm_writes_a_rule_based_record_not_the_old_placeholder(harness):
    harness.llm_reply = {"recommended_fix": "x", "what_we_missed": LEGACY_LLM_FALLBACK_TEXT,
                         "confidence": 0.5, "narrated": False}
    pm = generate_prediction_postmortem(_prediction_engine(RECENT), "pred-1")
    assert len(harness.llm_calls) == 1
    assert pm.analysis_method == ANALYSIS_RULES
    assert pm.what_we_missed != LEGACY_LLM_FALLBACK_TEXT and pm.confidence_in_analysis is None


def test_a_narrated_record_says_so(harness):
    harness.llm_reply = {"recommended_fix": "cut size", "what_we_missed": "FOMC", "confidence": 0.7, "narrated": True}
    pm = generate_prediction_postmortem(_prediction_engine(RECENT), "pred-1")
    assert (pm.analysis_method, pm.what_we_missed, pm.recommended_fix, pm.confidence_in_analysis) == (
        ANALYSIS_LLM, "FOMC", "cut size", 0.7)


def test_the_llm_helper_reports_no_narrative_when_it_is_unavailable(monkeypatch):
    import llm.router as router

    monkeypatch.setattr(router, "get_llm", lambda _tier: SimpleNamespace(is_available=False))
    out = pm_mod._get_llm_postmortem(
        ticker="SPY", direction="CALL", thesis="t", outcome="miss", actual_return=-0.04,
        category="wrong_signal", root_cause="rc", what_happened="w", data_at_decision={},
        price_path=[], signals_wrong=[], signals_right=[],
    )
    assert out["narrated"] is False


@pytest.mark.parametrize("reply,narrated", [
    ("MISSED: the FOMC print\nFIX: wait a day\nCONFIDENCE: 0.6", True),
    ("FIX: wait a day", True),
    ("I cannot help with that.", False),
])
def test_the_llm_helper_counts_a_reply_as_narration_only_with_a_missed_or_fix_line(monkeypatch, reply, narrated):
    import llm.router as router

    monkeypatch.setattr(router, "get_llm", lambda _tier: SimpleNamespace(is_available=True, generate=lambda **k: reply))
    out = pm_mod._get_llm_postmortem(
        ticker="SPY", direction="CALL", thesis="t", outcome="miss", actual_return=-0.04,
        category="wrong_signal", root_cause="rc", what_happened="w", data_at_decision={},
        price_path=[], signals_wrong=[], signals_right=[],
    )
    assert out["narrated"] is narrated


# ── events: only a new record of a recent miss ──────────────────────────────────────────────────────


@pytest.mark.parametrize("scored_at,emits", [(RECENT, 1), (EDGE_IN, 1), (EDGE_OUT, 0), (OLD, 0), (None, 1)])
def test_only_a_recent_or_undated_miss_emits_postmortem_completed(harness, scored_at, emits):
    pm = generate_prediction_postmortem(_prediction_engine(scored_at), "pred-1", narrate=False)
    assert pm is not None
    assert len(harness.emitted) == emits


def test_an_existing_record_is_not_written_again_and_emits_nothing(harness, monkeypatch):
    monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: False)
    assert generate_prediction_postmortem(_prediction_engine(RECENT), "pred-1", narrate=False) is None
    assert harness.emitted == []


# ── narration in place ──────────────────────────────────────────────────────────────────────────────


def test_narration_updates_the_rule_based_row_in_place_and_emits_nothing(harness, monkeypatch):
    harness.llm_reply = {"recommended_fix": "cut size", "what_we_missed": "FOMC", "confidence": 0.7}
    statements: list = []
    engine = _prediction_engine(RECENT, statements)
    pm = generate_prediction_postmortem(engine, "pred-1", narrate=True, update_row_id=42)
    assert pm is not None and pm.analysis_method == ANALYSIS_LLM
    updates = [(s, p) for s, p in statements if s.startswith("UPDATE trade_postmortems")]
    assert len(updates) == 1
    sql, params = updates[0]
    assert params["id"] == 42 and "FOMC" == params["what_we_missed"] and params["confidence"] == 0.7
    assert _norm(UNNARRATED_SQL) in sql                    # only a row that is still rule-based
    assert json.loads(params["full_analysis"])["analysis_method"] == ANALYSIS_LLM
    assert "generated_at" not in sql                        # the first-written time is kept
    assert harness.stored == [] and harness.emitted == []   # no insert, no second event


def test_narration_without_an_llm_narrative_changes_nothing(harness):
    harness.llm_reply = {"narrated": False}
    statements: list = []
    assert generate_prediction_postmortem(
        _prediction_engine(RECENT, statements), "pred-1", narrate=True, update_row_id=42) is None
    assert not any(s.startswith("UPDATE") for s, _ in statements)


def test_narrate_postmortem_only_takes_a_rule_based_row(monkeypatch):
    calls: list = []
    monkeypatch.setattr(pm_mod, "generate_prediction_postmortem", lambda e, pid, **k: calls.append((pid, k)) or "pm")
    seen: list = []

    def _engine(row):
        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, stmt, params=None):
                seen.append(_norm(stmt))
                res = MagicMock()
                res.fetchone.return_value = row
                return res

        return SimpleNamespace(connect=lambda: _Conn())

    assert pm_mod.narrate_postmortem(_engine((None, "pred-9")), 7) == "pm"
    assert calls == [("pred-9", {"narrate": True, "update_row_id": 7})]
    assert _norm(UNNARRATED_SQL) in seen[0]
    assert pm_mod.narrate_postmortem(_engine(None), 8) is None   # already narrated (or gone): nothing


# ── the nightly batch: record all, then narrate the newest ───────────────────────────────────────────


class _Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now


def _batch(monkeypatch, *, pred_ids, narrate_ids, record_s=0.02, narrate_s=25.0, deadline=None, limit=20):
    clock = _Clock()
    monkeypatch.setattr(pm_mod, "time", SimpleNamespace(monotonic=clock.monotonic))
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    monkeypatch.setattr(pm_mod, "_log_aggregate_patterns", lambda *_a, **_k: None)
    seen_sql: list = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, stmt, params=None):
            sql = _norm(stmt)
            seen_sql.append((sql, dict(params or {})))
            res = MagicMock()
            if sql.startswith("SELECT tp.id"):
                res.fetchall.return_value = [(i, None) for i in narrate_ids]
            elif sql.startswith("SELECT id FROM (SELECT"):
                res.fetchall.return_value = [(i,) for i in pred_ids]
            else:
                res.fetchall.return_value = []
            res.scalar.return_value = 0
            return res

    engine = SimpleNamespace(connect=lambda: _Conn())
    events: list = []

    def _record(_e, ident, **kw):
        events.append(("record", ident, kw))
        clock.now += record_s
        return SimpleNamespace(ticker="X", failure_category="c", actual_return=-0.1, which_signals_were_wrong=[])

    def _narrate(_e, row_id):
        events.append(("narrate", row_id, {}))
        clock.now += narrate_s
        return SimpleNamespace(ticker="X", failure_category="c", actual_return=-0.1, which_signals_were_wrong=[])

    monkeypatch.setattr(pm_mod, "generate_prediction_postmortem", _record)
    monkeypatch.setattr(pm_mod, "generate_postmortem", _record)
    monkeypatch.setattr(pm_mod, "narrate_postmortem", _narrate)
    result = pm_mod.batch_postmortem(engine, days=30, limit=limit, deadline=deadline)
    return result, events, seen_sql


def test_the_batch_records_every_selected_miss_without_the_llm_then_narrates_newest_first(monkeypatch):
    result, events, seen_sql = _batch(monkeypatch, pred_ids=[f"p{i}" for i in range(40)], narrate_ids=[11, 10, 9])
    records = [e for e in events if e[0] == "record"]
    assert [e[1] for e in records] == [f"p{i}" for i in range(40)]          # all 40, not a limit of 20
    assert all(e[2] == {"narrate": False} for e in records)
    assert [e[1] for e in events if e[0] == "narrate"] == [11, 10, 9]
    assert events.index(("narrate", 11, {})) > events.index(records[-1])     # recording first
    assert len(result) == 43
    narr_sql, narr_params = next((s, p) for s, p in seen_sql if s.startswith("SELECT tp.id"))
    assert "ORDER BY miss_at DESC" in narr_sql and narr_params["lim"] == 20  # newest miss first, capped
    assert _norm(UNNARRATED_SQL) in narr_sql


def test_narration_stops_at_the_deadline_and_leaves_the_rest_labelled_rule_based(monkeypatch):
    # 40 records at 20 ms (0.8 s), then narration at 25 s a row against a 60 s deadline: 3 start.
    _result, events, _ = _batch(monkeypatch, pred_ids=[f"p{i}" for i in range(40)],
                                narrate_ids=list(range(10)), deadline=60.0)
    assert len([e for e in events if e[0] == "record"]) == 40
    assert [e[1] for e in events if e[0] == "narrate"] == [0, 1, 2]


def test_no_narration_starts_when_recording_used_the_whole_window(monkeypatch):
    _result, events, _ = _batch(monkeypatch, pred_ids=["a", "b", "c"], narrate_ids=[1], record_s=50.0, deadline=60.0)
    assert [e[1] for e in events] == ["a", "b"]


# ── readers window on the miss's own date ───────────────────────────────────────────────────────────


def test_the_window_predicate_checks_the_miss_date_of_both_sources_behind_the_write_time_prefilter():
    sql = miss_on_or_after_sql(":cutoff")
    assert sql.startswith("(tp.generated_at >= :cutoff AND (")
    assert "op_w.id = tp.prediction_id AND op_w.scored_at >= :cutoff" in sql
    assert "orr_w.id = tp.trade_id AND orr_w.closed_at >= :cutoff" in sql


@pytest.mark.parametrize("reader", ["load_postmortems", "load_postmortems_top_n", "count_postmortems"])
def test_every_postmortem_reader_uses_the_miss_date_window(monkeypatch, reader):
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    seen: list = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, stmt, params=None):
            seen.append(_norm(stmt))
            res = MagicMock()
            res.fetchall.return_value = []
            res.scalar.return_value = 0
            return res

    getattr(pm_mod, reader)(SimpleNamespace(connect=lambda: _Conn()), days=30)
    assert len(seen) == 1 and _norm(miss_on_or_after_sql(":cutoff")) in seen[0]


def test_trace_evolver_and_auto_improve_read_by_miss_date():
    import inspect

    import oracle.trace_evolver as te
    import scripts.auto_improve_from_postmortems as ai

    assert "miss_on_or_after_sql" in inspect.getsource(te.TraceAnalyzer._load_postmortems)
    src = inspect.getsource(ai)
    assert "generated_at >=" not in src            # every window is on the miss date now
    assert src.count("miss_on_or_after_sql(") >= 6


# ── the backfill CLI ────────────────────────────────────────────────────────────────────────────────


def test_the_backfill_dry_run_writes_nothing(monkeypatch, capsys):
    import scripts.postmortem_backfill as bf

    monkeypatch.setattr(bf, "count_unrecorded", lambda *a, **k: {
        "predictions": {"by_month": {"2026-05": 3}, "total": 3}, "trades": {"by_month": {}, "total": 0}})
    monkeypatch.setattr(bf, "run_backfill", lambda *a, **k: pytest.fail("dry run must not write"))
    import db

    monkeypatch.setattr(db, "get_engine", lambda: object())
    assert bf.main([]) == 0
    assert "Dry run: nothing written" in capsys.readouterr().out


def test_the_backfill_records_without_the_llm_and_honours_its_limit(monkeypatch):
    import scripts.postmortem_backfill as bf

    pages = {"predictions": ["p1", "p2", "p3"], "trades": [7]}
    monkeypatch.setattr(bf, "iter_unrecorded", lambda _e, source, *a, **k: iter(pages[source]))
    calls: list = []
    monkeypatch.setattr(pm_mod, "generate_prediction_postmortem",
                        lambda _e, i, **k: calls.append((i, k)) or (None if i == "p2" else "pm"))
    monkeypatch.setattr(pm_mod, "generate_postmortem", lambda _e, i, **k: calls.append((i, k)) or "pm")
    stats = bf.run_backfill(object())
    assert calls == [("p1", {"narrate": False}), ("p2", {"narrate": False}),
                     ("p3", {"narrate": False}), (7, {"narrate": False})]
    assert stats == {"selected": 4, "written": 3, "skipped": 1, "errors": 0}
    calls.clear()
    assert bf.run_backfill(object(), limit=2)["selected"] == 2 and len(calls) == 2


def test_the_backfill_counts_errors_and_stops_at_its_error_budget(monkeypatch):
    import scripts.postmortem_backfill as bf

    monkeypatch.setattr(bf, "iter_unrecorded", lambda _e, source, *a, **k: iter(["a", "b", "c"] if source == "predictions" else []))

    def _boom(*_a, **_k):
        raise RuntimeError("db blip")

    monkeypatch.setattr(pm_mod, "generate_prediction_postmortem", _boom)
    assert bf.run_backfill(object(), max_errors=2) == {"selected": 2, "written": 0, "skipped": 0, "errors": 2}


def test_the_hermes_step_still_narrates_at_most_its_batch_limit():
    import inspect

    from scripts import hermes_operator as ho

    src = inspect.getsource(ho._daily_intel_postmortem_batch)
    assert "limit=POSTMORTEM_BATCH_LIMIT" in src and ho.POSTMORTEM_BATCH_LIMIT == 20


def test_the_api_trigger_is_bounded_like_the_nightly_and_never_reaches_past_the_window(monkeypatch):
    # Partner review A: the route narrated every rule-based row of up to 365 days in one request.
    import api.routers.intelligence_actors as ia

    seen: dict = {}

    def _batch(engine, **kw):
        seen.update(kw)
        return []

    monkeypatch.setattr(pm_mod, "batch_postmortem", _batch)
    monkeypatch.setattr(ia, "get_db_engine", lambda: object())
    fn = getattr(ia.trigger_batch_postmortem, "__wrapped__", ia.trigger_batch_postmortem)
    out = fn(days=365, token="t")
    assert seen["days"] == pm_mod.POSTMORTEM_EVENT_WINDOW_DAYS and out["days_applied"] == 30
    assert seen["limit"] == pm_mod.API_POSTMORTEM_NARRATE_LIMIT
    assert seen["deadline"] is not None
