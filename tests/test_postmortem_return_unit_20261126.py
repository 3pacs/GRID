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
    # The fixture miss is dated May 2026; these tests pin the emitted P&L, so treat it as recent
    # (old misses emit nothing since 2026-11-27, POSTMORTEM_EVENT_WINDOW_DAYS).
    monkeypatch.setattr(pm_mod, "_miss_is_recent", lambda _when: True)
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


@pytest.mark.parametrize("missing", [None, float("nan"), float("inf"), "", "n/a"])
def test_prediction_miss_without_a_return_writes_no_postmortem_and_no_zero(monkeypatch, missing):
    # Codex hold on #959: `float(pnl_pct) / 100.0 if pnl_pct else 0.0` stored a missing
    # return as a measured 0%. No return means no post-mortem, never a 0.0 one.
    captured, seen = _setup(monkeypatch)
    stored: list = []
    monkeypatch.setattr(pm_mod, "_store_postmortem", lambda *a, **k: stored.append(a))
    assert generate_prediction_postmortem(_prediction_engine(missing), prediction_id="pred-1") is None
    assert stored == []          # nothing persisted
    assert captured == []        # no realized_pnl event
    assert seen == {}            # the LLM was never asked to narrate "Return: +0.00%"


def test_prediction_miss_with_a_measured_zero_return_is_still_written(monkeypatch):
    # A stated 0.0 pnl is a measurement, not a missing value.
    _setup(monkeypatch)
    pm = generate_prediction_postmortem(_prediction_engine(0.0), prediction_id="pred-1")
    assert pm is not None
    assert pm.actual_return == 0.0


def test_batch_selection_skips_predictions_with_no_recorded_return(monkeypatch):
    # Unwritable rows must not be re-selected every batch and crowd out newer ones under LIMIT.
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
    pred_sql = [q for q in seen_sql if "oracle_predictions" in q and "verdict = 'miss'" in q]
    assert pred_sql and all("pnl_pct IS NOT NULL" in q for q in pred_sql)
    # NULL is not the only return the writer refuses: NaN and +/-Infinity must not fill the LIMIT either.
    assert all("pnl_pct::float8 NOT IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)" in q for q in pred_sql)


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


# ── Reading a mixed history (Codex 6069091963) ─────────────────────────────
#
# Stored rows from before the fix carry no unit marker: prediction rows hold
# percent points, trade rows hold fractions, and the table's trade_id /
# prediction_id columns tell them apart. Rows written after the fix carry
# actual_return_unit="fraction".

from datetime import timedelta  # noqa: E402

from api.routers import postmortem_lessons as pml  # noqa: E402
from intelligence.postmortem import load_postmortems, postmortems_from_records, stored_return_fraction  # noqa: E402


def _rec(*, ret, prediction_id=None, trade_id=None, unit=None, ticker="X", gen="2026-11-25T00:00:00+00:00"):
    full = {"ticker": ticker, "direction": "PUT", "outcome": "miss", "actual_return": ret,
            "failure_category": "wrong_signal", "root_cause": "r", "generated_at": gen}
    if unit is not None:
        full["actual_return_unit"] = unit
    return {"id": 1, "trade_id": trade_id, "prediction_id": prediction_id, "ticker": ticker,
            "outcome": "miss", "failure_category": "wrong_signal", "root_cause": "r",
            "full_analysis": full, "generated_at": gen}


LEGACY_PRED = lambda: _rec(ret=-50.24, prediction_id="p1")            # noqa: E731
NEW_PRED = lambda: _rec(ret=-0.5024, prediction_id="p2", unit="fraction")  # noqa: E731
TRADE = lambda: _rec(ret=-1.0, trade_id=7)                             # noqa: E731


@pytest.mark.parametrize("record, expected", [
    (_rec(ret=-50.24, prediction_id="p"), -0.5024),               # legacy prediction: points
    (_rec(ret=-0.39, prediction_id="p"), -0.0039),                # small legacy value still points: no magnitude rule
    (_rec(ret=-1.0, trade_id=3), -1.0),                           # legacy trade: fraction
    (_rec(ret=-50.0, trade_id=3), -50.0),                         # not guessed from magnitude either
    (_rec(ret=-0.5024, prediction_id="p", unit="fraction"), -0.5024),
    (_rec(ret=-50.24, prediction_id="p", unit="percent_points"), -0.5024),
    (_rec(ret=-0.5, prediction_id="p", unit="basis_points"), None),   # unknown marker
    (_rec(ret=-0.5), None),                                           # writer cannot be told
    (_rec(ret=-0.5, prediction_id="p", trade_id=3), None),            # contradictory provenance
    (_rec(ret=None, prediction_id="p"), None),
    (_rec(ret=float("nan"), prediction_id="p"), None),
    ({"prediction_id": "p", "full_analysis": {}}, None),
])
def test_stored_return_fraction(record, expected):
    got = stored_return_fraction(record)
    if expected is None:
        assert got is None
    else:
        assert got == pytest.approx(expected)


def test_hydration_counts_what_it_cannot_put_in_a_unit():
    h = postmortems_from_records([LEGACY_PRED(), TRADE(), _rec(ret=-0.5)])
    assert [round(p.actual_return, 4) for p in h.postmortems] == [-0.5024, -1.0]
    assert h.excluded == 1
    assert len(h.included) == 2


def _patch_window(monkeypatch, records):
    monkeypatch.setattr("intelligence.postmortem.load_postmortems", lambda engine, days, **k: records)
    monkeypatch.setattr(pm_mod, "_get_llm_lessons_learned", lambda *a, **k: None)


def test_served_lessons_average_one_unit_across_legacy_new_and_trade(monkeypatch):
    _patch_window(monkeypatch, [LEGACY_PRED(), NEW_PRED(), TRADE()])
    out = pml._generate(MagicMock(), n=10, days=30)
    # (-0.5024 - 0.5024 - 1.0) / 3 = -0.6683; unconverted this read -1724.75%
    assert "Average return:    -66.83%" in out["text"]
    assert "1724" not in out["text"]
    assert out["return_units"] == "fraction"
    assert out["excluded_unknown_return"] == 0
    assert out["count"] == 3


def test_second_lessons_route_uses_the_same_normalisation(monkeypatch):
    from api.routers import intelligence_actors as ia

    _patch_window(monkeypatch, [LEGACY_PRED(), NEW_PRED(), TRADE(), _rec(ret=-0.5)])
    monkeypatch.setattr(ia, "get_db_engine", lambda: MagicMock())
    out = ia.get_lessons_learned(days=30, _token="t")
    assert "Average return:    -66.83%" in out["lessons"]
    assert out["count"] == 3
    assert out["excluded_unknown_return"] == 1


def test_loader_adds_the_normalised_return(monkeypatch):
    monkeypatch.setattr(pm_mod, "_ensure_tables", lambda _e: None)
    gen = datetime(2026, 11, 25, tzinfo=timezone.utc)

    def row(i, trade_id, prediction_id, full):
        return (i, trade_id, prediction_id, "X", "miss", "c", "r", "[]", "[]", "", "", json.dumps(full), 0.5, gen)

    rows = [
        row(1, None, "p1", {"actual_return": -50.24}),
        row(2, 7, None, {"actual_return": -1.0}),
        row(3, None, "p2", {"actual_return": -0.5, "actual_return_unit": "fraction"}),
    ]
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = rows
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    got = load_postmortems(engine, days=30)
    assert [round(r["actual_return_fraction"], 4) for r in got] == [-0.5024, -1.0, -0.5]


# Cache: a summary cached before the fix has no unit marker and is never served.

def _cached(lessons, age=timedelta(minutes=5)):
    ts = datetime.now(timezone.utc) - age
    return {"lessons": lessons, "generated_at": ts.isoformat(), "n": 5, "days": 30, "_ts": ts}


LEGACY_CACHE = {"text": "Average return:    -841.60%", "count": 5, "label": "LLM summary of 5 postmortems"}
MARKED_CACHE = {"text": "Average return:    -8.42%", "count": 5, "label": "x", "return_units": "fraction"}


def _route(monkeypatch, cached, *, generate=None):
    monkeypatch.setattr(pml, "get_db_engine", lambda: MagicMock())
    monkeypatch.setattr(pml, "_read_cache", lambda engine: cached)
    writes = MagicMock(return_value="2026-11-26T00:00:00+00:00")
    monkeypatch.setattr(pml, "_write_cache", writes)
    if generate is not None:
        monkeypatch.setattr(pml, "_generate", generate)
    return writes


def test_fresh_legacy_cache_is_not_served_and_is_regenerated(monkeypatch):
    fresh = {"text": "fresh", "count": 3, "label": "l", "return_units": "fraction"}
    writes = _route(monkeypatch, _cached(LEGACY_CACHE), generate=lambda e, n, days: fresh)
    out = pml.get_postmortem_lessons(n=5, days=30, refresh=0, _token="t")
    assert out["cached"] is False and out["lessons"] is fresh
    writes.assert_called_once()


def test_fresh_marked_cache_is_served(monkeypatch):
    writes = _route(monkeypatch, _cached(MARKED_CACHE), generate=lambda e, n, days: pytest.fail("regenerated"))
    out = pml.get_postmortem_lessons(n=5, days=30, refresh=0, _token="t")
    assert out["cached"] is True and out["lessons"] is MARKED_CACHE
    writes.assert_not_called()


def test_synthesis_failure_never_falls_back_to_a_legacy_cache(monkeypatch):
    def boom(e, n, days):
        raise RuntimeError("llm down")

    _route(monkeypatch, _cached(LEGACY_CACHE, age=timedelta(days=2)), generate=boom)
    out = pml.get_postmortem_lessons(n=5, days=30, refresh=0, _token="t")
    assert out["lessons"] is None and out["cached"] is False
    assert "withheld" in out["note"] and out["error"] == "llm down"


def test_synthesis_failure_still_falls_back_to_a_marked_cache(monkeypatch):
    def boom(e, n, days):
        raise RuntimeError("llm down")

    _route(monkeypatch, _cached(MARKED_CACHE, age=timedelta(days=2)), generate=boom)
    out = pml.get_postmortem_lessons(n=5, days=30, refresh=0, _token="t")
    assert out["cached"] is True and out["lessons"] is MARKED_CACHE and out["error"] == "llm down"


def test_empty_window_does_not_resurrect_a_legacy_cache(monkeypatch):
    _route(monkeypatch, _cached(LEGACY_CACHE, age=timedelta(days=2)),
           generate=lambda e, n, days: {"text": "none", "count": 0, "label": None})
    out = pml.get_postmortem_lessons(n=5, days=30, refresh=0, _token="t")
    assert out["cached"] is False and out["label"] is None
    assert "withheld" in out["note"]
    assert out["lessons"]["count"] == 0


# ── Codex 6069248064: prompt prose and the date label ───────────────────────

import types  # noqa: E402


def _llm_capturing(monkeypatch, prompts: list) -> None:
    class _Llm:
        is_available = True

        def generate(self, prompt, **kw):
            prompts.append(prompt)
            return "- lever: weight less"

    fake = types.SimpleNamespace(get_llm=lambda tier: _Llm(), Tier=types.SimpleNamespace(RESEARCH="research"))
    monkeypatch.setitem(sys.modules, "llm.router", fake)


PARAPHRASES = [
    "Direction was wrong. Actual return: -5024.00%.",
    "Loss of 5024.00% followed an incorrect signal.",
    "The position fell 5024.00% after entry.",
    "Down fifty-one hundred percent; 5024 pct drawdown.",
]


@pytest.mark.parametrize("legacy_text", PARAPHRASES)
def test_lessons_prompt_carries_no_free_text_of_legacy_prediction_rows(monkeypatch, legacy_text):
    prompts: list = []
    _llm_capturing(monkeypatch, prompts)
    legacy = LEGACY_PRED()
    legacy["full_analysis"]["root_cause"] = legacy_text
    legacy["full_analysis"]["what_actually_happened"] = legacy_text
    legacy["full_analysis"]["what_we_missed"] = legacy_text
    legacy["full_analysis"]["recommended_fix"] = legacy_text
    new = NEW_PRED()
    new["full_analysis"]["root_cause"] = "Fresh row. Actual return: -50.24%."
    trade = TRADE()
    trade["full_analysis"]["root_cause"] = "Trade row. Actual return: -100.00%."
    monkeypatch.setattr("intelligence.postmortem.load_postmortems", lambda engine, days, **k: [legacy, new, trade])

    out = pml._generate(MagicMock(), n=10, days=30)

    assert len(prompts) == 1
    prompt = prompts[0]
    assert "Average return: -66.83%" in prompt
    assert "5024" not in prompt and "fifty-one hundred" not in prompt
    assert f"wrong_signal {pm_mod.PROSE_OMITTED}" in prompt          # structured stand-in for the legacy row
    assert "Fresh row. Actual return: -50.24%." in prompt            # marked row: its prose is right
    assert "Trade row. Actual return: -100.00%." in prompt           # trade rows were always fractions
    assert out["count"] == 3
    assert legacy["full_analysis"]["root_cause"] == legacy_text      # raw stored prose untouched


def test_hydration_leaves_legacy_prose_empty_and_marks_it_untrusted():
    legacy = LEGACY_PRED()
    legacy["full_analysis"]["root_cause"] = "The position fell 5024.00% after entry."
    h = postmortems_from_records([legacy, NEW_PRED(), TRADE()])
    old, new, trade = h.postmortems
    assert (old.prose_trusted, old.root_cause) == (False, "")
    assert new.prose_trusted and trade.prose_trusted
    assert new.root_cause == "r" and trade.root_cause == "r"


def test_label_uses_the_rows_actually_included_and_exclusion_does_not_shrink_the_window(monkeypatch):
    def dated(day, **kw):
        return _rec(ret=-50.0, prediction_id=f"p{day}", gen=f"2026-11-{day:02d}T12:00:00+00:00", **kw)

    unknown_newest = _rec(ret=-0.5, gen="2026-11-25T12:00:00+00:00")   # writer unknown -> excluded
    records = [unknown_newest, dated(24), dated(23), dated(22), dated(21)]
    _patch_window(monkeypatch, records)
    out = pml._generate(MagicMock(), n=3, days=30)
    assert out["count"] == 3                                          # not 2
    assert out["label"] == ("LLM summary of 3 postmortems dated 2026-11-22 to 2026-11-24"
                            " (3 narrated, 0 rule-based: classified by rule, not a proven cause)")
    assert out["excluded_unknown_return"] == 1


def test_window_where_every_row_is_ineligible_says_so(monkeypatch):
    _patch_window(monkeypatch, [_rec(ret=-0.5), _rec(ret=-0.7)])
    out = pml._generate(MagicMock(), n=5, days=30)
    assert out["count"] == 0 and out["label"] is None
    assert out["excluded_unknown_return"] == 2
    assert "2 post-mortems" in out["text"] and "no return in a known unit" in out["text"]
