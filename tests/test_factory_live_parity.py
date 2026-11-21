"""Research-versus-live parity for factory features (3pacs/GRID#1160).

* The same factory function runs on both sides.
* A feature is live-ready only with five matched sessions, no value out of
  tolerance and no session where the two sides disagree about availability.
* A live recording with a missing read bar, or more than two lost minutes, fails:
  values that agree are not parity when live would abstain.
* Version 3: the verdict is taken on the five most recent overlapping sessions.
  An older bad session stays in the whole-window counts and no longer blocks.
* Version 2: the live side is judged by the live recorder's lost-minute rule.
  Up to two lost minutes leave the return features exact; a range feature needs
  each lost minute's backstop bar, first seen at or before the decision time.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from analysis import factory_live_parity as flp
from analysis import module_factory as mf

NY = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
SESSIONS = ["2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06", "2026-11-09", "2026-11-10"]
FULL = [f"{m // 60:02d}:{m % 60:02d}" for m in range(570, 960)]


def _bar(day_index: int, minute_index: int) -> tuple:
    """A deterministic bar with a real range and a drift that differs by day."""
    close = 700.0 + 2.0 * day_index + 0.01 * minute_index * (1 if day_index % 2 else -1)
    return (close - 0.02, close + 0.05, close - 0.06, close, 1000 + minute_index)


def _session(day_index: int, minutes=FULL) -> dict:
    return {hm: _bar(day_index, FULL.index(hm)) for hm in minutes}


def _days(sessions=SESSIONS) -> dict:
    return {day: _session(i) for i, day in enumerate(sessions)}


def _research_db(path, bars):
    con = sqlite3.connect(path)
    con.execute("create table bars (sym text, day text, hm text, o real, h real, l real, c real, v integer, "
                "primary key (sym, day, hm))")
    for day, session in bars.items():
        con.executemany("insert into bars values ('SPY', ?, ?, ?, ?, ?, ?, ?)",
                        [(day, hm, *b) for hm, b in session.items()])
    con.commit()
    con.close()
    return path


def _utc(day, hm, second=0):
    return datetime.fromisoformat(f"{day}T{hm}:{second:02d}").replace(tzinfo=NY).astimezone(UTC).isoformat()


AS_THE_RULE_GIVES = "as the rule gives"


def _live_db(path, bars, src_of=lambda day, hm: "schwab", backstop=None, recorded=AS_THE_RULE_GIVES, decisions=("15:00",)):
    """``backstop``: {(day, hm): (bar, "HH:MM" New York time it was first seen that day)}; None = no such table.
    ``recorded``: the recorder's own rows. Default: for every session and decision, the value these terms give on
    these bars, each bound to the terms with the value's own stamp. None = no such tables. A function gets the rows
    (dicts with day, decision, feature, raw, eligible, recorded_at, terms_sha256, terms_recorded_at) and may change
    them; a row whose terms_sha256 is None is written with no binding."""
    con = sqlite3.connect(path)
    con.execute("create table bars_1m_live (symbol text, ts_utc text, src text, day text, o real, h real, "
                "l real, c real, v integer, primary key (symbol, ts_utc))")
    for day, session in bars.items():
        for hm, b in session.items():
            con.execute("insert into bars_1m_live values ('SPY', ?, ?, ?, ?, ?, ?, ?, ?)",
                        (_utc(day, hm), src_of(day, hm), day, *b))
    if backstop is not None:
        con.execute("create table factory_backstop_bars (symbol text, ts_utc text, day text, source text, o real, "
                    "h real, l real, c real, v integer, fetch_started_at text, first_seen_at text, "
                    "primary key (symbol, ts_utc, source))")
        for (day, hm), (b, seen) in backstop.items():
            con.execute("insert into factory_backstop_bars values ('SPY', ?, ?, 'yahoo-1m', ?, ?, ?, ?, ?, ?, ?)",
                        (_utc(day, hm), day, *b, _utc(day, seen), _utc(day, seen, 2)))
    con.commit()
    con.close()
    if recorded is not None:
        first, last = min(bars), max(bars)
        live, _ = flp.load_live(path, "SPY", first, last)
        held = flp.load_backstop(path, "SPY", first, last)
        rows = []
        for day in sorted(live):
            for decision in decisions:
                values, why, _used = flp.live_features(live, held, day, decision)
                at = _utc(day, decision, 20)
                rows += [{"day": day, "decision": decision, "feature": f, "raw": values[f], "eligible": int(not why[f]),
                          "recorded_at": at, "terms_sha256": flp.LIVE_RULE_TERMS_SHA256, "terms_recorded_at": at} for f in mf.FEATURES]
        if recorded != AS_THE_RULE_GIVES:
            recorded(rows)
        con = sqlite3.connect(path)
        con.execute("create table factory_feature_values (day text, decision text, feature text, raw real, eligible integer, "
                    "reasons text, recorded_at text, primary key (day, decision, feature))")
        con.execute("create table factory_feature_terms (day text, decision text, feature text, terms_sha256 text, "
                    "recorded_at text, primary key (day, decision, feature))")
        for r in rows:
            con.execute("insert into factory_feature_values values (?, ?, ?, ?, ?, '[]', ?)",
                        (r["day"], r["decision"], r["feature"], r["raw"], r["eligible"], r["recorded_at"]))
            if r["terms_sha256"] is not None:
                con.execute("insert into factory_feature_terms values (?, ?, ?, ?, ?)",
                            (r["day"], r["decision"], r["feature"], r["terms_sha256"], r["terms_recorded_at"]))
        con.commit()
        con.close()
    return path


def _after_close(day):
    return datetime.fromisoformat(f"{day}T17:00:00").replace(tzinfo=NY)


@pytest.fixture
def run(tmp_path, monkeypatch):
    calls = []

    def go(research, live, decisions=("15:00",), last=SESSIONS[-1], clock=None, **kwargs):
        calls.append(1)                                   # a test may build more than one pair of databases
        r, lv = tmp_path / f"r{len(calls)}.db", tmp_path / f"l{len(calls)}.db"
        monkeypatch.setattr(flp, "_clock", lambda: clock or _after_close(last))     # the report is made after that close
        return flp.report(_research_db(r, research), _live_db(lv, live, decisions=decisions, **kwargs),
                          first=SESSIONS[0], last=last, decisions=list(decisions), horizons=["close"])
    return go


def test_both_sides_run_the_factorys_own_feature_function():
    assert flp.raw_features is mf.raw_features and flp.Session is mf.Session
    assert set(flp.FEATURE_TYPE) == set(mf.FEATURES)


def test_identical_complete_recordings_are_live_ready(run):
    doc = run(_days(), _days())

    assert doc["live_ready"] == sorted(mf.FEATURES) and doc["research_only"] == []
    f1 = doc["comparison"]["features"]["F1"]
    assert f1["matched_sessions"] == 6 and f1["overlapping_sessions"] == 7   # the first has no prior
    assert f1["max_abs_diff"] == 0 and f1["values_out_of_tolerance"] == 0
    assert doc["comparison"]["availability_mismatch"] == []
    live = doc["live_availability"]
    assert live["signal_eligible"] == 6 and live["signal_rows"] == 7
    assert live["signal_withheld_reasons"] == {flp.PRIOR_MISSING: 1}
    assert live["label_available"] == 7
    assert doc["promotion_allowed"] is False


def test_four_matched_sessions_are_not_enough(run):
    few = SESSIONS[:5]                                    # five sessions, four with a prior

    doc = run({d: _days()[d] for d in few}, {d: _days()[d] for d in few}, last=few[-1])

    f1 = doc["comparison"]["features"]["F1"]
    assert f1["matched_sessions"] == 4 and f1["status"] == "research_only"
    assert f1["reasons"] == ["insufficient_matched_sessions"]
    assert doc["live_ready"] == []


def test_a_recorder_that_never_stores_the_last_minute_cannot_take_a_slot(run):
    """What grid-svr's live recording looked like on 2026-11-19: 15:59 missing every day."""
    live = {day: {hm: b for hm, b in session.items() if hm != "15:59"} for day, session in _days().items()}

    doc = run(_days(), live)

    availability = doc["live_availability"]
    assert availability["signal_eligible"] == 0           # no prior session is ever complete
    assert availability["label_available"] == 0           # the 15:00 -> close exit bar never exists
    assert availability["signal_withheld_reasons"] == {flp.PRIOR_INCOMPLETE: 6, flp.PRIOR_MISSING: 1}
    assert availability["label_missing_reasons"] == {flp.EXIT_MISSING: 7}
    assert doc["live_missing_minutes"]["2026-11-03"] == ["15:59"]
    f1 = doc["comparison"]["features"]["F1"]
    assert f1["values_compared"] == 6 and f1["values_out_of_tolerance"] == 0   # the numbers agree...
    assert f1["status"] == "research_only"                                     # ...and it is still not parity
    assert set(f1["reasons"]) == {"insufficient_matched_sessions", "availability_mismatch"}
    assert len(doc["comparison"]["availability_mismatch"]) == 6
    assert doc["comparison"]["labels"]["missing_on_a_side"] == 7


def test_three_lost_morning_minutes_withhold_that_day_and_the_next(run):
    live = _days()
    for hm in ("10:01", "10:02", "11:15"):
        del live["2026-11-04"][hm]

    doc = run(_days(), live)

    by_day = {r["day"]: r["signal_withheld"] for r in doc["live_availability"]["rows"]}
    assert by_day["2026-11-04"] == [flp.PREFIX_INCOMPLETE]
    assert by_day["2026-11-05"] == [flp.PRIOR_INCOMPLETE]
    assert by_day["2026-11-06"] == []
    assert doc["live_availability"]["signal_eligible"] == 4
    assert doc["comparison"]["features"]["F1"]["matched_sessions"] == 4
    assert doc["research_only"] == sorted(mf.FEATURES)
    assert doc["schema"] == "factory-live-parity-v3"


def test_one_lost_minute_keeps_the_return_features_and_withholds_the_range_that_reads_it(run):
    live = _days()
    del live["2026-11-04"]["10:01"]

    doc = run(_days(), live)

    rows = {r["day"]: r for r in doc["live_availability"]["rows"]}
    assert rows["2026-11-04"]["signal_withheld"] == [] and rows["2026-11-05"]["signal_withheld"] == []
    assert rows["2026-11-04"]["feature_withheld"] == {"F10": [flp.GAP_UNFILLED]}      # today's range
    assert rows["2026-11-05"]["feature_withheld"] == {"F11": [flp.GAP_UNFILLED]}      # the prior session's range
    live_side = doc["live_availability"]
    assert live_side["signal_eligible"] == 6
    assert live_side["feature_eligible"] == {"F1": 6, "F10": 5, "F11": 5, "F1_shift30": 6}
    assert doc["live_ready"] == ["F1", "F1_shift30"]
    features = doc["comparison"]["features"]
    assert features["F1"]["matched_sessions"] == 6 and features["F1"]["max_abs_diff"] == 0
    assert features["F10"]["reasons"] == ["insufficient_matched_sessions", "availability_mismatch"]
    assert features["F10"]["matched_sessions"] == 5 and features["F10"]["verdict_on"]["matched"] == 4
    assert features["F10"]["values_out_of_tolerance"] == 0 and features["F10"]["values_compared"] == 5
    mismatch = {m["day"]: (m["features"], m["live"]) for m in doc["comparison"]["availability_mismatch"]}
    assert mismatch == {"2026-11-04": (["F10"], {"F10": [flp.GAP_UNFILLED]}),
                        "2026-11-05": (["F11"], {"F11": [flp.GAP_UNFILLED]})}
    assert doc["live_rule"]["backstop_table"] is False and doc["live_rule"]["gap_max"] == 2


def _spiked():
    """Sessions whose 2026-11-04 low is one minute's spike at 11:00."""
    days = _days()
    o, h, low, c, v = days["2026-11-04"]["11:00"]
    days["2026-11-04"]["11:00"] = (o, h, low - 3.0, c, v)
    return days


def test_a_backstop_bar_seen_before_the_decision_restores_the_lost_extreme(run):
    research, live = _spiked(), _spiked()
    lost = live["2026-11-04"].pop("11:00")
    naive = mf.raw_features(mf.Session(live["2026-11-04"]), mf.Session(live["2026-11-03"]), "14:59")["F10"]
    truth = mf.raw_features(mf.Session(research["2026-11-04"]), mf.Session(research["2026-11-03"]), "14:59")["F10"]
    assert abs(naive - truth) > flp.TOLERANCE["range"]          # the live bars alone give a wrong value

    without = run(research, live)
    assert without["live_ready"] == ["F1", "F1_shift30"]
    assert without["comparison"]["features"]["F10"]["values_out_of_tolerance"] == 0    # withheld, never compared

    doc = run(research, live, backstop={("2026-11-04", "11:00"): (lost, "11:04")})

    assert doc["live_ready"] == sorted(mf.FEATURES) and doc["comparison"]["availability_mismatch"] == []
    assert doc["live_rule"] == {**doc["live_rule"], "backstop_table": True, "backstop_bars": 1}
    used = {(r["day"], r["feature"]): r["backstop_minutes"] for r in doc["comparison"]["value_rows"]}
    assert used[("2026-11-04", "F10")] == [["today", "11:00"]]
    assert used[("2026-11-05", "F11")] == [["prior", "11:00"]]
    assert used[("2026-11-04", "F1")] == [] and used[("2026-11-04", "F11")] == []       # they do not read it
    assert doc["live_availability"]["feature_values_using_the_backstop"] == {"F1": 0, "F10": 1, "F11": 1, "F1_shift30": 0}
    assert doc["comparison"]["features"]["F10"]["max_abs_diff"] == 0


def test_a_backstop_bar_first_seen_after_the_decision_time_is_not_used(run):
    research, live = _spiked(), _spiked()
    lost = live["2026-11-04"].pop("11:00")

    doc = run(research, live, backstop={("2026-11-04", "11:00"): (lost, "15:00")})   # the insert is stamped 15:00:02

    rows = {r["day"]: r["feature_withheld"] for r in doc["live_availability"]["rows"]}
    assert rows["2026-11-04"] == {"F10": [flp.GAP_UNFILLED]}
    assert rows["2026-11-05"] == {}                       # by the next session's decision it had long been seen
    assert doc["live_ready"] == sorted(["F1", "F11", "F1_shift30"])


def test_the_bars_a_feature_reads_are_never_among_the_tolerated_gaps():
    def reasons(day, *lost):
        bars = _days()
        for d, hm in lost:
            del bars[d][hm]
        return flp.live_signal_reasons(bars, day, "15:00"), flp.live_features(bars, None, day, "15:00")[1]

    assert reasons("2026-11-04", ("2026-11-04", "14:59"))[0] == [flp.PREFIX_INCOMPLETE]     # closes at the decision
    assert reasons("2026-11-04", ("2026-11-03", "15:59"))[0] == [flp.PRIOR_INCOMPLETE]      # the prior close
    assert reasons("2026-11-04", ("2026-11-03", "10:00"), ("2026-11-03", "10:01"), ("2026-11-03", "12:00"))[0] == [flp.PRIOR_INCOMPLETE]
    session, per_feature = reasons("2026-11-04", ("2026-11-04", "14:29"))
    assert session == []
    assert per_feature == {"F1": [], "F10": [flp.GAP_UNFILLED], "F11": [], "F1_shift30": [flp.READ_BAR_MISSING]}
    session, per_feature = reasons("2026-11-04", ("2026-11-04", "10:00"), ("2026-11-04", "10:01"))
    assert session == [] and per_feature["F1"] == [] and per_feature["F1_shift30"] == []


def test_research_keeps_the_strict_rule():
    bars = _days()
    del bars["2026-11-04"]["10:01"]

    assert flp.signal_reasons(bars, "2026-11-04", "15:00") == [flp.PREFIX_INCOMPLETE]
    assert flp.signal_reasons(bars, "2026-11-05", "15:00") == [flp.PRIOR_INCOMPLETE]
    assert flp.live_signal_reasons(bars, "2026-11-04", "15:00") == []


def test_a_value_that_does_not_exist_by_definition_is_not_applicable_and_never_a_match(run):
    """F1_shift30 at 10:00 needs the bar 30 minutes earlier, which is before the open."""
    assert flp.structurally_undefined("F1_shift30", "10:00") and not flp.structurally_undefined("F1_shift30", "10:30")
    assert not any(flp.structurally_undefined(f, "10:00") for f in ("F1", "F10", "F11"))

    only_1000 = run(_days(), _days(), decisions=("10:00",))

    shift = only_1000["comparison"]["features"]["F1_shift30"]
    assert only_1000["live_ready"] == ["F1", "F10", "F11"]                 # identical recordings, and still:
    assert shift["status"] == "research_only" and shift["matched_sessions"] == 0
    assert shift["reasons"] == ["no_applicable_decision_time", "insufficient_matched_sessions"]
    assert shift["values_compared"] == 0 and shift["values_matched"] == 0 and shift["values_required"] == 0
    assert shift["applicable_decision_times"] == []
    rows = [r for r in only_1000["comparison"]["value_rows"] if r["feature"] == "F1_shift30"]
    assert rows and all(r["applicable"] is False and r["matched"] is None for r in rows)

    both = run(_days(), _days(), decisions=("10:00", "15:00"))

    shift = both["comparison"]["features"]["F1_shift30"]
    assert both["live_ready"] == sorted(mf.FEATURES)
    assert shift["matched_sessions"] == 6 and shift["applicable_decision_times"] == ["15:00"]
    assert shift["values_required"] == 7 and shift["values_matched"] == 6      # the first session has no prior
    f1 = both["comparison"]["features"]["F1"]
    assert f1["values_required"] == 14 and f1["values_matched"] == 12          # both decision times, every session


def test_two_missing_values_on_an_applicable_row_are_a_failure_not_an_agreement(run):
    flat = _days()
    flat["2026-11-05"] = {hm: (710.0, 710.0, 710.0, 710.0, 1000) for hm in FULL}   # no range: F10 has no value

    doc = run(flat, flat)

    f10 = doc["comparison"]["features"]["F10"]
    assert f10["status"] == "research_only" and f10["eligible_rows_with_no_value"] == 1
    assert f10["matched_sessions"] == 5 and f10["values_matched"] == 5 and f10["values_required"] == 7
    row = next(r for r in doc["comparison"]["value_rows"] if r["feature"] == "F10" and r["day"] == "2026-11-05")
    assert row["research"] is None and row["live"] is None and row["both_eligible"] and row["matched"] is False
    assert "F1" in doc["live_ready"]


def test_the_terms_text_and_its_sha256_are_the_pinned_ones():
    """The same text and digest are pinned in gex-signals' tests (factory_slots.GAP_TERMS). Change both or neither."""
    assert flp.LIVE_RULE_TERMS_JSON == (
        '{"backstop_clock":"first_seen_at <= decision time","backstop_fields":["h","l"],"backstop_settle_s":30,'
        '"backstop_source":"yahoo-1m","feature_reads":{"F1":null,"F10":"todays_range","F11":"prior_range",'
        '"F1_shift30":"bar_30_minutes_before"},"gap_max":2,"prior_last_bar":"15:59","terms":"panel-factory-gaps/1"}')
    assert flp.LIVE_RULE_TERMS_SHA256 == "31e017f78c4d676ce6e405b1beb708cde9fbc370448a4a6c923b3e39db4b50d9"


def test_without_the_recorders_own_rows_nothing_is_ready(run):
    without = run(_days(), _days(), recorded=None)                    # a recording made before the recorder wrote values

    assert without["live_ready"] == [] and without["live_rule"]["recorded_values_table"] is False
    f1 = without["comparison"]["features"]["F1"]
    assert f1["reasons"] == [flp.RULE_NOT_RECORDED, "insufficient_matched_sessions"]
    assert f1["values_compared"] == 6 and f1["values_out_of_tolerance"] == 0 and f1["values_matched"] == 0
    assert f1["values_not_bound"] == 7 and without["comparison"]["sessions_not_recorded_under_the_rule"] == SESSIONS
    rows = [r for r in without["comparison"]["value_rows"] if r["feature"] == "F1"]
    assert {r["binding"] for r in rows} == {"no_recorded_value"} and all(r["recorded_value"] is None for r in rows)

    bound = run(_days(), _days())
    assert bound["live_ready"] == sorted(mf.FEATURES)
    assert bound["live_rule"]["recorded_values"] == 28 and bound["live_rule"]["recorded_values_bound_to_these_terms"] == 28
    assert {r["binding"] for r in bound["comparison"]["value_rows"]} == {"bound_to_these_terms"}


def _only(day, feature, **change):
    def tamper(rows):
        for r in rows:
            if r["day"] == day and r["feature"] == feature:
                r.update(change)
    return tamper


@pytest.mark.parametrize("change, binding, reason", [
    # Codex's witness on 09bd974c: an old 15:00 value of 999999.0, and the terms stamped at 15:01
    ({"raw": 999999.0, "terms_recorded_at": "2026-11-06T20:01:00+00:00"}, "binding_not_written_with_the_value", "live_rule_not_recorded"),
    ({"terms_recorded_at": "2026-11-06T20:01:00+00:00"}, "binding_not_written_with_the_value", "live_rule_not_recorded"),
    ({"raw": 999999.0, "terms_sha256": None}, "value_not_bound_to_any_terms", "live_rule_not_recorded"),     # an older generation's row
    ({"terms_sha256": None}, "value_not_bound_to_any_terms", "live_rule_not_recorded"),
    ({"terms_sha256": "f" * 64}, "value_bound_to_other_terms", "live_rule_not_recorded"),
    ({"raw": 999999.0}, "bound_to_these_terms", "recorded_value_differs_from_bars"),                         # bound, and not what the bars give
    ({"eligible": 0, "raw": None}, "bound_to_these_terms", "recorded_value_differs_from_bars"),              # the recorder withheld it
])
def test_a_value_counts_only_on_its_own_binding_and_only_if_it_is_what_the_bars_give(run, change, binding, reason):
    doc = run(_days(), _days(), recorded=_only("2026-11-06", "F1", **change))

    f1 = doc["comparison"]["features"]["F1"]
    assert "F1" not in doc["live_ready"] and doc["live_ready"] == ["F10", "F11", "F1_shift30"]    # the other features' rows are their own
    assert reason in f1["reasons"] and f1["matched_sessions"] == 5
    row = next(r for r in doc["comparison"]["value_rows"] if r["feature"] == "F1" and r["day"] == "2026-11-06")
    assert row["binding"] == binding and row["matched"] is False
    assert row["within_tolerance"] is True               # the bars agree with research, and that is not enough
    assert row["recorded_value"] == change.get("raw", row["live"])


def test_a_session_that_changed_generation_part_way_is_mixed_decision_by_decision(run):
    def restart(rows):                                   # 10:00 was recorded by an older generation; 15:00 by this one
        for r in rows:
            if r["decision"] == "10:00":
                r["terms_sha256"] = None

    doc = run(_days(), _days(), decisions=("10:00", "15:00"), recorded=restart)

    assert doc["live_ready"] == ["F1_shift30"]           # its only applicable decision time is 15:00
    f1 = doc["comparison"]["features"]["F1"]
    assert f1["values_not_bound"] == 7 and f1["values_bound_to_the_terms"] == 7 and f1["matched_sessions"] == 0
    assert f1["reasons"] == [flp.RULE_NOT_RECORDED, "insufficient_matched_sessions"]
    by_decision = {(r["decision"], r["binding"]) for r in doc["comparison"]["value_rows"] if r["feature"] == "F1"}
    assert by_decision == {("10:00", "value_not_bound_to_any_terms"), ("15:00", "bound_to_these_terms")}


def test_what_a_binding_must_be():
    good = {"raw": 0.1, "eligible": True, "recorded_at": "t", "terms_sha256": flp.LIVE_RULE_TERMS_SHA256, "terms_recorded_at": "t"}
    assert flp.binding_problem(good) is None and flp.binding_problem(None) == "no_recorded_value"
    assert flp.binding_problem({**good, "terms_sha256": None}) == "value_not_bound_to_any_terms"
    assert flp.binding_problem({**good, "terms_sha256": "0" * 64}) == "value_bound_to_other_terms"
    assert flp.binding_problem({**good, "terms_recorded_at": "later"}) == "binding_not_written_with_the_value"


def test_a_value_outside_tolerance_is_counted_and_blocks_the_feature(run):
    live = _days()
    o, h, low, c, v = live["2026-11-05"]["14:59"]
    live["2026-11-05"]["14:59"] = (o, h, low, c + 0.30, v)     # about 4 bp on the return

    doc = run(_days(), live)

    f1 = doc["comparison"]["features"]["F1"]
    assert f1["values_out_of_tolerance"] == 1 and f1["matched_sessions"] == 5
    assert f1["status"] == "research_only"
    assert f1["reasons"] == ["insufficient_matched_sessions", "value_out_of_tolerance"]
    assert f1["max_abs_diff"] > flp.TOLERANCE["return"]
    bad = [r for r in doc["comparison"]["value_rows"] if r["feature"] == "F1" and r["within_tolerance"] is False]
    assert [r["day"] for r in bad] == ["2026-11-05"]


def test_a_label_that_differs_by_more_than_five_cents_is_counted(run):
    live = _days()
    o, h, low, c, v = live["2026-11-06"]["15:59"]
    live["2026-11-06"]["15:59"] = (o, h, low, c + 0.20, v)

    labels = run(_days(), live)["comparison"]["labels"]

    assert labels["compared"] == 7 and labels["out_of_tolerance"] == 1


def test_sources_are_counted_per_day(run):
    doc = run(_days(), _days(), src_of=lambda day, hm: "rtd" if hm in ("10:05", "10:06") else "schwab")

    assert doc["live_sources_per_day"]["2026-11-02"] == {"rtd": 2, "schwab": 388}


def test_a_shortened_prior_session_is_withheld_and_a_late_decision_is_after_the_close():
    friday, monday = "2026-11-27", "2026-11-30"            # 13:00 close after Thanksgiving
    half = [hm for hm in FULL if hm < "13:00"]
    bars = {friday: _session(0, half), monday: _session(1)}

    assert flp.close_minute(friday) == 13 * 60 and flp.close_minute("2026-11-26") is None
    assert flp.previous_session(monday) == friday
    assert flp.signal_reasons(bars, friday, "15:00") == [flp.AFTER_CLOSE]
    assert flp.signal_reasons(bars, monday, "15:00") == [flp.PRIOR_SHORT]
    assert flp.label_reasons(bars, friday, "12:00", "close") == [flp.EXIT_AFTER_CLOSE]


@pytest.mark.parametrize(("horizon", "expected"), [
    ("close", ("15:01", "15:59", 16 * 60)),
    ("30m", ("15:01", "15:29", 15 * 60 + 30)),
])
def test_label_bars_follow_the_research_builder(horizon, expected):
    assert flp.label_bars("15:00", horizon) == expected


def test_days_before_dev_are_refused(tmp_path):
    db = _research_db(tmp_path / "r.db", _days())

    with pytest.raises(flp.ParityError, match="holdout refused"):
        flp.load_research(db, "SPY", "2022-11-01", "2022-11-30")


def test_command_writes_the_report_once_and_exits_1_when_not_ready(tmp_path, capsys):
    live = {day: {hm: b for hm, b in s.items() if hm != "15:59"} for day, s in _days().items()}
    r, lv, out = _research_db(tmp_path / "r.db", _days()), _live_db(tmp_path / "l.db", live), tmp_path / "p.json"
    args = ["--research-db", str(r), "--signals-db", str(lv), "--first", SESSIONS[0], "--last", SESSIONS[-1],
            "--out", str(out), "--summary"]

    assert flp.main(args) == 1

    printed = json.loads(capsys.readouterr().out)
    assert printed["research_only"] == sorted(mf.FEATURES) and "rows" not in printed["live_availability"]
    assert json.loads(out.read_text())["live_availability"]["rows"]            # the file keeps the detail
    with pytest.raises(FileExistsError):
        flp.main(args)


LONGER = SESSIONS + ["2026-11-11", "2026-11-12"]


def test_the_verdict_is_taken_on_the_five_most_recent_sessions_and_an_older_failure_stays_on_the_report(run):
    live = _days(LONGER)
    for hm in ("10:01", "10:02", "11:15"):
        del live["2026-11-03"][hm]                         # withholds 11-03, and 11-04 through its prior session

    doc = run(_days(LONGER), live, last=LONGER[-1])

    f1 = doc["comparison"]["features"]["F1"]
    assert doc["live_ready"] == sorted(mf.FEATURES)
    assert f1["verdict_on"] == {"sessions": LONGER[-5:], "matched": 5, "required": 5,
                                "missing": {"research": [], "live": []}, "not_recorded_under_the_rule": [],
                                "recorded_values_differ_from_the_bars": [],
                                "values_out_of_tolerance": 0, "eligible_rows_with_no_value": 0,
                                "sessions_with_an_availability_disagreement": []}
    assert doc["verdict_sessions"] == LONGER[-5:] and doc["as_of"] == "2026-11-12T17:00:00-05:00"
    assert f1["matched_sessions"] == 6 and f1["overlapping_sessions"] == 9             # the whole window, as before
    assert f1["whole_window"] == {"sessions_with_an_availability_disagreement": 2}
    assert {m["day"] for m in doc["comparison"]["availability_mismatch"]} == {"2026-11-03", "2026-11-04"}

    earlier = run(_days(LONGER), live, last="2026-11-10")   # the same recording, judged two sessions earlier
    f1 = earlier["comparison"]["features"]["F1"]
    assert earlier["live_ready"] == [] and f1["verdict_on"]["matched"] == 4
    assert f1["verdict_on"]["sessions_with_an_availability_disagreement"] == ["2026-11-04"]
    assert f1["reasons"] == ["insufficient_matched_sessions", "availability_mismatch"]


def test_an_old_value_out_of_tolerance_is_still_counted_and_a_recent_one_still_blocks(run):
    live = _days(LONGER)
    o, h, low, c, v = live["2026-11-04"]["14:59"]
    live["2026-11-04"]["14:59"] = (o, h, low, c + 0.30, v)

    doc = run(_days(LONGER), live, last=LONGER[-1])

    f1 = doc["comparison"]["features"]["F1"]
    assert f1["status"] == "live_ready" and f1["values_out_of_tolerance"] == 1         # counted, outside the verdict
    assert f1["verdict_on"]["values_out_of_tolerance"] == 0

    o, h, low, c, v = live["2026-11-12"]["14:59"]
    live["2026-11-12"]["14:59"] = (o, h, low, c + 0.30, v)
    f1 = run(_days(LONGER), live, last=LONGER[-1])["comparison"]["features"]["F1"]
    assert f1["status"] == "research_only" and f1["verdict_on"]["values_out_of_tolerance"] == 1
    assert f1["reasons"] == ["insufficient_matched_sessions", "value_out_of_tolerance"]


def test_fewer_than_five_recent_sessions_is_never_ready_and_the_number_is_not_an_argument(run):
    few = SESSIONS[:5]
    doc = run({d: _days()[d] for d in few}, {d: _days()[d] for d in few}, last=few[-1])

    assert doc["comparison"]["features"]["F1"]["verdict_on"]["matched"] == 4 and doc["live_ready"] == []
    with pytest.raises(SystemExit):
        flp.main(["--research-db", "r", "--signals-db", "l", "--first", few[0], "--last", few[-1], "--min-sessions", "1"])


def test_the_verdict_sessions_come_from_the_calendar_and_the_clock_not_from_the_data():
    assert flp.expected_recent_sessions(_after_close("2026-11-12")) == LONGER[-5:]
    before_the_close = datetime.fromisoformat("2026-11-12T15:59:00").replace(tzinfo=NY)
    assert flp.expected_recent_sessions(before_the_close) == LONGER[-6:-1]                 # 11-12 has not closed yet
    saturday = datetime.fromisoformat("2026-11-14T10:00:00").replace(tzinfo=NY)
    assert flp.expected_recent_sessions(saturday)[-1] == "2026-11-13"
    after_thanksgiving = datetime.fromisoformat("2026-11-27T13:00:00").replace(tzinfo=NY)   # the 13:00 close has passed
    assert flp.expected_recent_sessions(after_thanksgiving)[-2:] == ["2026-11-25", "2026-11-27"]
    with pytest.raises(TypeError):
        flp.compare({}, {}, ["15:00"], ["close"], None, None)                              # no verdict on the data's own dates


def test_a_feed_that_lacks_the_newest_session_is_not_ready_on_five_older_ones(run):
    """Codex on 4a6f1039: the missing date left the intersection and five older overlaps still passed."""
    for side in ("research", "live"):
        research, live = _days(LONGER), _days(LONGER)
        del {"research": research, "live": live}[side]["2026-11-12"]

        doc = run(research, live, last=LONGER[-1])

        f1 = doc["comparison"]["features"]["F1"]
        assert doc["live_ready"] == [], side
        assert f1["reasons"] == [flp.SESSION_MISSING, "insufficient_matched_sessions"], side
        assert f1["verdict_on"]["missing"][side] == ["2026-11-12"] and f1["verdict_on"]["matched"] == 4, side
        assert doc["expected_sessions_missing"][side] == ["2026-11-12"], side
        assert f1["matched_sessions"] == 7, side              # seven older sessions match, and it is still not ready


def test_a_feed_that_lacks_a_session_in_the_middle_is_not_ready_on_non_consecutive_ones(run):
    research, live = _days(LONGER), _days(LONGER)
    del live["2026-11-10"]                                    # inside the five; its absence also costs 11-11 its prior

    doc = run(research, live, last=LONGER[-1])

    f1 = doc["comparison"]["features"]["F1"]
    assert doc["live_ready"] == []
    assert f1["verdict_on"]["missing"] == {"research": [], "live": ["2026-11-10"]}
    assert f1["verdict_on"]["matched"] == 3 and flp.SESSION_MISSING in f1["reasons"]
    assert doc["expected_sessions_missing"] == {"research": [], "live": ["2026-11-10"]}

    older = _days(LONGER)
    del older["2026-11-04"]                                   # outside the five: reported, and it does not block
    doc = run(_days(LONGER), older, last=LONGER[-1])
    assert doc["expected_sessions_missing"]["live"] == ["2026-11-04"] and doc["live_ready"] == sorted(mf.FEATURES)


def test_a_report_made_days_later_is_judged_on_the_sessions_expected_then(run):
    """Complete, identical recordings through 11-12, read on 11-17: three expected sessions are missing on both sides."""
    doc = run(_days(LONGER), _days(LONGER), last="2026-11-17", clock=_after_close("2026-11-17"))

    assert doc["verdict_sessions"] == ["2026-11-11", "2026-11-12", "2026-11-13", "2026-11-16", "2026-11-17"]
    assert doc["live_ready"] == []
    f1 = doc["comparison"]["features"]["F1"]
    assert f1["verdict_on"]["missing"]["live"] == ["2026-11-13", "2026-11-16", "2026-11-17"] and f1["verdict_on"]["matched"] == 2


def test_the_as_of_time_is_not_an_argument_and_the_zero_evidence_gate_carries_over(run):
    with pytest.raises(SystemExit):
        flp.main(["--research-db", "r", "--signals-db", "l", "--first", "2026-11-02", "--last", "2026-11-12",
                  "--as-of", "2026-11-12T17:00:00-05:00"])

    doc = run(_days(LONGER), _days(LONGER), decisions=("10:00",), last=LONGER[-1])
    shift = doc["comparison"]["features"]["F1_shift30"]
    assert doc["live_ready"] == ["F1", "F10", "F11"]
    assert shift["reasons"] == ["no_applicable_decision_time", "insufficient_matched_sessions"]
    assert shift["verdict_on"]["matched"] == 0 and shift["values_matched"] == 0


def test_the_binding_gate_is_applied_to_the_verdict_sessions(run):
    """Codex on c4511aca: complete matching bars, a pre-existing 15:00 F1 value of 999999.0, terms stamped at 15:01."""
    def late_marker(rows):
        for r in rows:
            if r["feature"] == "F1":
                r["raw"], r["terms_recorded_at"] = 999999.0, r["recorded_at"].replace(":00:20", ":01:00")

    doc = run(_days(LONGER), _days(LONGER), last=LONGER[-1], recorded=late_marker)

    f1 = doc["comparison"]["features"]["F1"]
    assert doc["live_ready"] == ["F10", "F11", "F1_shift30"]
    assert f1["verdict_on"]["matched"] == 0 and f1["verdict_on"]["not_recorded_under_the_rule"] == LONGER[-5:]
    assert f1["reasons"] == [flp.RULE_NOT_RECORDED, "insufficient_matched_sessions"]
    assert doc["comparison"]["sessions_not_recorded_under_the_rule"] == LONGER

    def old_generation_until(day):
        def older(rows):
            for r in rows:
                if r["day"] < day:
                    r["terms_sha256"] = None
        return older

    recent_only = run(_days(LONGER), _days(LONGER), last=LONGER[-1], recorded=old_generation_until(LONGER[-5]))
    assert recent_only["live_ready"] == sorted(mf.FEATURES)          # the five verdict sessions are this generation's own
    assert recent_only["comparison"]["features"]["F1"]["values_not_bound"] == 4
    one_short = run(_days(LONGER), _days(LONGER), last=LONGER[-1], recorded=old_generation_until(LONGER[-4]))
    assert one_short["live_ready"] == []
    assert one_short["comparison"]["features"]["F1"]["verdict_on"]["not_recorded_under_the_rule"] == [LONGER[-5]]

    wrong = run(_days(LONGER), _days(LONGER), last=LONGER[-1], recorded=_only(LONGER[-2], "F10", raw=0.123456))
    f10 = wrong["comparison"]["features"]["F10"]
    assert "F10" not in wrong["live_ready"] and f10["verdict_on"]["recorded_values_differ_from_the_bars"] == [LONGER[-2]]
    assert flp.RECORDED_DIFFERS in f10["reasons"]
