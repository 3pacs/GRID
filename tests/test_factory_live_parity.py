"""Research-versus-live parity for factory features (3pacs/GRID#1160).

* The same factory function runs on both sides.
* A feature is live-ready only with five matched sessions, no value out of
  tolerance and no session where the two sides disagree about availability.
* A live recording with a missing read bar, or more than two lost minutes, fails:
  values that agree are not parity when live would abstain.
* Version 4: a return value is judged on the research feature's own spread (a tenth of it
  per value, and at each decision time the mean signed difference over the verdict sessions
  near zero), not in basis points. A return value with no research spread to judge it on is
  not a match. A slot is judged only as a named candidate, at its own decision time, with
  each side's z on its own history; the feature-level verdict is a diagnostic.
* Version 3: the verdict is taken on the five most recent closed exchange sessions the
  calendar expects; a session either side lacks is missing, not skipped.
  An older bad session stays in the whole-window counts and no longer blocks.
* Version 2: the live side is judged by the live recorder's lost-minute rule.
  Up to two lost minutes leave the return features exact; a range feature needs
  each lost minute's backstop bar, first seen at or before the decision time.
"""

from __future__ import annotations

import json
import sqlite3
import statistics
from collections import deque
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


HISTORY_DAYS = flp.expected_sessions_between("2026-07-15", "2026-10-30")     # research sessions before the window


def _history_session(i: int) -> dict:
    """A session whose level, drift and last half hour differ from day to day, so every return feature has a spread."""
    level = 700.0 + 0.9 * ((i * 37) % 17 - 8)
    slope = 0.004 * ((i * 13) % 11 - 5)
    late = 0.006 * ((i * 7) % 13 - 6)
    out = {}
    for m, hm in enumerate(FULL):
        close = level + slope * m + late * max(0, m - 300)
        out[hm] = (close - 0.02, close + 0.05, close - 0.06, close, 1000 + m)
    return out


def _history() -> dict:
    """Research bars before the window: read only for the spread a return difference is judged on (version 4)."""
    return {day: _history_session(i) for i, day in enumerate(HISTORY_DAYS)}


def _f1_dollars(day: str, spreads: float, sessions=None, decision: str = "15:00") -> float:
    """What to add to a live close so that F1 at ``decision`` on ``day`` moves by ``spreads`` research spreads."""
    days = _days(sessions or SESSIONS)
    spread = flp.research_spreads({**_history(), **days}, [decision])[("F1", decision, day)]
    return spreads * spread * days[flp.previous_session(day)]["15:59"][3]


def _nudged(live: dict, day: str, hm: str, dollars: float) -> None:
    o, h, low, c, v = live[day][hm]
    live[day][hm] = (o, h, low, c + dollars, v)


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

    def go(research, live, decisions=("15:00",), last=SESSIONS[-1], clock=None, history=True, candidates=None, **kwargs):
        calls.append(1)                                   # a test may build more than one pair of databases
        r, lv = tmp_path / f"r{len(calls)}.db", tmp_path / f"l{len(calls)}.db"
        monkeypatch.setattr(flp, "_clock", lambda: clock or _after_close(last))     # the report is made after that close
        research = {**(_history() if history else {}), **research}                 # the history is research only
        return flp.report(_research_db(r, research), _live_db(lv, live, decisions=decisions, **kwargs),
                          first=SESSIONS[0], last=last, decisions=list(decisions), horizons=["close"], candidates=candidates)
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
    assert doc["schema"] == "factory-live-parity-v5"


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
    _nudged(live, "2026-11-05", "14:59", _f1_dollars("2026-11-05", 0.12))     # 0.12 of the spread: over the tenth allowed

    doc = run(_days(), live)

    f1 = doc["comparison"]["features"]["F1"]
    assert f1["values_out_of_tolerance"] == 1 and f1["matched_sessions"] == 5
    assert f1["status"] == "research_only"
    assert f1["reasons"] == ["insufficient_matched_sessions", "value_out_of_tolerance"]
    assert f1["max_abs_diff_in_spreads"] == pytest.approx(0.12, rel=1e-3) and f1["max_abs_diff_in_spreads"] > flp.TOLERANCE["return"]
    assert (f1["tolerance"], f1["tolerance_unit"]) == (0.1, "research_spreads")
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
                                "recorded_outside_the_decision_window": [],
                                "values_out_of_tolerance": 0, "eligible_rows_with_no_value": 0,
                                "sessions_with_an_availability_disagreement": [],
                                "sessions_with_no_research_spread": [],
                                "mean_signed_diff_in_spreads_by_decision_time": {"15:00": 0.0},
                                "largest_abs_mean_signed_diff_in_spreads": 0.0, "decision_times_biased": [],
                                "bias_limit_in_spreads": flp.RETURN_BIAS_SPREADS}
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
    _nudged(live, "2026-11-04", "14:59", _f1_dollars("2026-11-04", 0.12, LONGER))

    doc = run(_days(LONGER), live, last=LONGER[-1])

    f1 = doc["comparison"]["features"]["F1"]
    assert f1["status"] == "live_ready" and f1["values_out_of_tolerance"] == 1         # counted, outside the verdict
    assert f1["verdict_on"]["values_out_of_tolerance"] == 0

    _nudged(live, "2026-11-12", "14:59", _f1_dollars("2026-11-12", 0.12, LONGER))
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


# ── version 4: a return value is judged on the research feature's own spread ──────────────


def test_the_spread_is_the_scale_of_the_factorys_own_z():
    history = deque([0.001 * ((i * 7) % 13 - 6) + 0.0001 * i for i in range(60)], maxlen=60)
    spread = flp.robust_spread(history)
    for x in (0.0031, -0.0007, 0.0):
        assert (x - statistics.median(history)) / spread == pytest.approx(mf.robust_z(x, history))
    assert flp.robust_spread([0.002] * 60) is None                       # no spread at all: nothing to judge on
    two_valued = [0.001] * 40 + [0.003] * 20                             # MAD is zero, the standard deviation is not
    assert flp.robust_spread(two_valued) == pytest.approx(statistics.pstdev(two_valued))
    assert (flp.RETURN_SPREAD_HISTORY, flp.RETURN_TOLERANCE_SPREADS, flp.RETURN_BIAS_SPREADS) == (60, 0.1, 0.03)
    assert flp.RETURN_SPREAD_HISTORY == mf.RunFile.__dataclass_fields__["z_history"].default


def test_a_days_spread_is_taken_from_the_sessions_before_it_only():
    bars = {**_history(), **_days()}
    spreads = flp.research_spreads(bars, ["15:00"])
    day = SESSIONS[3]
    before = {d: s for d, s in bars.items() if d < day}
    changed = {**before, **{d: _history_session(40 + i) for i, d in enumerate(sorted(d for d in bars if d >= day))}}
    assert flp.research_spreads(changed, ["15:00"])[("F1", "15:00", day)] == spreads[("F1", "15:00", day)]
    assert ("F1", "15:00", HISTORY_DAYS[30]) not in spreads               # fewer than 60 sessions before it
    assert {f for f, _t, _d in spreads} == {"F1", "F1_shift30"}           # range features are judged on the raw value
    short = {d: s for d, s in bars.items() if d >= HISTORY_DAYS[-50]}
    assert not any(d == SESSIONS[0] for _f, _t, d in flp.research_spreads(short, ["15:00"]))


def test_sampling_sized_differences_match_and_one_value_over_a_tenth_of_the_spread_does_not(run):
    live = _days()
    for day, k in zip(SESSIONS[-5:], (0.08, -0.07, 0.06, -0.08, 0.05)):   # each inside a tenth, no common sign
        _nudged(live, flp.previous_session(day), "15:59", -_f1_dollars(day, k))    # the prior close, as a sampled quote would miss it

    doc = run(_days(), live)

    f1 = doc["comparison"]["features"]["F1"]
    assert "F1" in doc["live_ready"] and f1["reasons"] == []
    assert f1["max_abs_diff_in_spreads"] == pytest.approx(0.08, rel=2e-2)
    assert f1["verdict_on"]["largest_abs_mean_signed_diff_in_spreads"] < flp.RETURN_BIAS_SPREADS
    assert f1["max_abs_diff"] > 1e-4, "these differences are over the 1 bp of version 3 and are now a match"
    row = next(r for r in doc["comparison"]["value_rows"] if r["feature"] == "F1" and r["day"] == SESSIONS[-1])
    assert row["diff_in_spreads"] == pytest.approx(0.05, rel=2e-2) and row["within_tolerance"] is True
    assert row["research_spread"] == pytest.approx(row["abs_diff"] / abs(row["diff_in_spreads"]))

    _nudged(live, flp.previous_session(SESSIONS[-2]), "15:59", -_f1_dollars(SESSIONS[-2], 0.2))
    f1 = run(_days(), live)["comparison"]["features"]["F1"]
    assert f1["status"] == "research_only" and "value_out_of_tolerance" in f1["reasons"]


def test_a_constant_error_inside_the_tolerance_is_a_bias_and_blocks(run):
    """Every value is inside a tenth of the spread and all on one side: the prior close read wrong the same way."""
    live = _days()
    for day in SESSIONS[-5:]:
        _nudged(live, flp.previous_session(day), "15:59", -_f1_dollars(day, 0.06))

    doc = run(_days(), live)

    f1 = doc["comparison"]["features"]["F1"]
    assert f1["verdict_on"]["values_out_of_tolerance"] == 0 and f1["verdict_on"]["matched"] == 5
    assert f1["status"] == "research_only" and f1["reasons"] == [flp.VALUE_BIASED]
    assert f1["verdict_on"]["mean_signed_diff_in_spreads_by_decision_time"]["15:00"] == pytest.approx(0.06, rel=2e-2)
    assert f1["verdict_on"]["decision_times_biased"] == ["15:00"] and f1["verdict_on"]["bias_limit_in_spreads"] == 0.03
    assert "F1" not in doc["live_ready"] and "F10" in doc["live_ready"]
    f10 = doc["comparison"]["features"]["F10"]["verdict_on"]                                       # ranges have no bias test
    assert f10["mean_signed_diff_in_spreads_by_decision_time"] == {} and f10["bias_limit_in_spreads"] is None


def test_a_bias_is_judged_on_the_verdict_sessions_only(run):
    live = _days(LONGER)
    for day in LONGER[1:4]:                                              # three older sessions, all one side
        _nudged(live, flp.previous_session(day), "15:59", -_f1_dollars(day, 0.09, LONGER))

    f1 = run(_days(LONGER), live, last=LONGER[-1])["comparison"]["features"]["F1"]

    assert f1["verdict_on"]["sessions"] == LONGER[-5:]
    assert f1["status"] == "live_ready" and f1["verdict_on"]["mean_signed_diff_in_spreads_by_decision_time"] == {"15:00": 0.0}


def test_a_return_value_with_no_research_spread_is_not_a_match(run):
    doc = run(_days(), _days(), history=False)                           # identical bars, and nothing to judge a return on

    f = doc["comparison"]["features"]
    assert doc["live_ready"] == ["F10", "F11"]
    for name in ("F1", "F1_shift30"):
        assert f[name]["reasons"] == ["insufficient_matched_sessions", flp.SPREAD_UNAVAILABLE]
        assert f[name]["verdict_on"]["sessions_with_no_research_spread"] == SESSIONS[-5:]
        assert f[name]["values_with_no_research_spread"] == f[name]["values_compared"] == 6
        assert f[name]["values_out_of_tolerance"] == 0                   # not judged is not "out of tolerance"
        assert f[name]["verdict_on"]["mean_signed_diff_in_spreads_by_decision_time"] == {"15:00": None}
        assert f[name]["verdict_on"]["largest_abs_mean_signed_diff_in_spreads"] is None
    row = next(r for r in doc["comparison"]["value_rows"] if r["feature"] == "F1" and r["day"] == SESSIONS[-1])
    assert (row["abs_diff"], row["research_spread"], row["diff_in_spreads"], row["matched"]) == (0.0, None, None, False)
    assert row["within_tolerance"] is None
    assert doc["return_tolerance"]["research_sessions_read_before_the_window"] == 0


def test_the_history_is_read_for_the_spread_and_never_compared(run, tmp_path):
    doc = run(_days(), _days())

    assert doc["comparison"]["overlapping_sessions"] == SESSIONS         # the sessions both sides hold: no history day
    assert doc["research_sessions"] == len(SESSIONS)                     # the window's own sessions
    assert doc["return_tolerance"] == {
        "unit": "research_spreads", "per_value": 0.1, "bias": 0.03, "spread_history_sessions": 60,
        "research_sessions_read_before_the_window": len(HISTORY_DAYS), "note": doc["return_tolerance"]["note"]}
    assert not any(r["day"] < SESSIONS[0] for r in doc["comparison"]["value_rows"])

    many = {d: _history_session(i) for i, d in enumerate(flp.expected_sessions_between("2026-01-05", "2026-10-30"))}
    db = _research_db(tmp_path / "many.db", many)
    held = flp.load_research_history(db, "SPY", SESSIONS[0])
    assert len(held) == flp.SPREAD_LOOKBACK_SESSIONS == 90 and max(held) == "2026-10-30"
    assert flp.load_research_history(db, "SPY", "2026-01-05") == {}


def test_the_history_never_reaches_before_dev(tmp_path):
    days = ["2022-11-10", "2022-11-11", mf.DEV_FIRST, "2022-11-15"]
    db = _research_db(tmp_path / "old.db", {d: _history_session(i) for i, d in enumerate(days)})

    assert sorted(flp.load_research_history(db, "SPY", "2022-11-16")) == [mf.DEV_FIRST, "2022-11-15"]


# ── after the hold on fb69ebcf: the bias at each decision time, and a named candidate's own gate ──────────


def test_opposite_errors_at_two_decision_times_do_not_cancel(run):
    """The reviewer's case: +0.06 at 15:00 and -0.06 at 14:30. Averaged over decision times that is no bias at all."""
    live = _days()
    for day in SESSIONS[-5:]:
        _nudged(live, day, "14:59", _f1_dollars(day, 0.06, decision="15:00"))
        _nudged(live, day, "14:29", -_f1_dollars(day, 0.06, decision="14:30"))

    doc = run(_days(), live, decisions=("14:30", "15:00"))

    f1 = doc["comparison"]["features"]["F1"]
    by_time = f1["verdict_on"]["mean_signed_diff_in_spreads_by_decision_time"]
    assert by_time["15:00"] == pytest.approx(0.06, rel=2e-2) and by_time["14:30"] == pytest.approx(-0.06, rel=2e-2)
    assert abs(by_time["15:00"] + by_time["14:30"]) / 2 < 0.005, "the average over both times is inside the limit"
    assert f1["verdict_on"]["values_out_of_tolerance"] == 0                  # every value is inside a tenth
    assert f1["status"] == "research_only" and flp.VALUE_BIASED in f1["reasons"]
    assert f1["verdict_on"]["decision_times_biased"] == ["14:30", "15:00"]
    assert f1["verdict_on"]["largest_abs_mean_signed_diff_in_spreads"] == pytest.approx(0.06, rel=2e-2)


def test_a_bias_at_one_decision_time_is_found_beside_a_clean_one(run):
    live = _days()
    for day in SESSIONS[-5:]:
        _nudged(live, day, "14:59", _f1_dollars(day, 0.06, decision="15:00"))

    f1 = run(_days(), live, decisions=("14:30", "15:00"))["comparison"]["features"]["F1"]

    assert f1["verdict_on"]["decision_times_biased"] == ["15:00"]
    assert f1["verdict_on"]["mean_signed_diff_in_spreads_by_decision_time"]["14:30"] == 0.0


def _recorder_history(shift: float = 0.0, feature: str = "F1", decision: str = "15:00"):
    """A ``recorded`` hook that gives the recorder its own earlier values of one (feature, decision): research's
    value on each history session, plus ``shift``, each bound to the terms with its own stamp."""
    bars = {**_history(), **_days()}

    def add(rows):
        for day in HISTORY_DAYS:
            if flp.signal_reasons(bars, day, decision):
                continue
            value = flp.feature_values(bars, day, decision)[feature]
            at = _utc(day, decision, 20)
            rows.append({"day": day, "decision": decision, "feature": feature, "raw": value + shift, "eligible": 1,
                         "recorded_at": at, "terms_sha256": flp.LIVE_RULE_TERMS_SHA256, "terms_recorded_at": at})
    return add


def test_without_a_candidate_no_slot_is_judged(run):
    doc = run(_days(), _days())

    assert doc["live_ready"] == sorted(mf.FEATURES)                          # the diagnostic
    assert doc["slot_gate"]["candidates"] == [] and doc["slot_gate"]["ready"] == []
    assert "no candidate was named, so no slot was judged" in doc["slot_gate"]["note"]
    assert doc["promotion_allowed"] is False


def test_a_candidate_is_not_ready_until_the_recorder_has_its_own_history(run):
    doc = run(_days(), _days(), candidates=[("F1", "15:00", ">=", 0.5)])

    assert "F1" in doc["live_ready"], "the values agree, and that alone judges no slot"
    gate = doc["slot_gate"]["candidates"][0]
    assert (gate["feature"], gate["decision"], gate["op"], gate["k"]) == ("F1", "15:00", ">=", 0.5)
    assert gate["status"] == "not_ready" and gate["reasons"] == [flp.TRIGGER_UNAVAILABLE]
    assert gate["trigger_unavailable"] == SESSIONS[-5:] and gate["value_not_matched"] == []
    assert gate["recorder_history"] == {"needed": 60, "have_min": 1, "have_max": 5}     # the window's own earlier sessions
    assert all(e["live_z"] is None and e["research_z"] is not None and e["agree"] is None for e in gate["sessions"])
    assert doc["slot_gate"]["ready"] == []


def _a_threshold_research_crosses(run):
    """(op, k) that research's z crosses on at least one verdict session, read from a run with the full history."""
    gate = run(_days(), _days(), candidates=[("F1", "15:00", ">=", 0.5)], recorded=_recorder_history())["slot_gate"]["candidates"][0]
    zs = [e["research_z"] for e in gate["sessions"]]
    top, bottom = max(zs), min(zs)
    return (">=", round(top - 0.05, 4)) if top > 0.1 else ("<=", round(-bottom - 0.05, 4))


def test_a_candidate_with_both_histories_is_ready_when_the_trigger_agrees(run):
    op, k = _a_threshold_research_crosses(run)

    doc = run(_days(), _days(), candidates=[("F1", "15:00", op, k)], recorded=_recorder_history())

    gate = doc["slot_gate"]["candidates"][0]
    assert gate["status"] == "ready" and gate["reasons"] == [], gate
    assert gate["recorder_history"]["have_min"] == 60
    assert any(e["research_fires"] for e in gate["sessions"]), "the threshold is one research crosses"
    for e in gate["sessions"]:
        assert e["agree"] is True and e["value_matched"] is True
        assert e["live_z"] == pytest.approx(e["research_z"], abs=0.15)
        assert e["research_fires"] == (e["research_z"] >= k if op == ">=" else e["research_z"] <= -k)
    # The two histories differ by one value: the window's first session has no live prior session, so the recorder's
    # history reaches one session further back. The z values are close, and on some sessions not equal.
    assert any(e["live_z"] != e["research_z"] for e in gate["sessions"])
    assert doc["slot_gate"]["ready"] == [f"F1@15:00{op}{k:g}"]
    assert "separate gates" in gate["note"]


def test_the_same_values_on_another_history_can_disagree_about_the_trigger(run):
    """Every value matches and nothing is biased. The recorder's own history sits elsewhere, so its z does not cross."""
    op, k = _a_threshold_research_crosses(run)
    spread = flp.research_spreads({**_history(), **_days()}, ["15:00"])[("F1", "15:00", SESSIONS[-1])]
    shift = 3 * spread if op == ">=" else -3 * spread                        # the recorder's earlier values, three spreads away

    doc = run(_days(), _days(), candidates=[("F1", "15:00", op, k)], recorded=_recorder_history(shift))

    gate = doc["slot_gate"]["candidates"][0]
    assert "F1" in doc["live_ready"] and gate["value_not_matched"] == [] and gate["mean_signed_diff_in_spreads"] == 0.0
    assert gate["status"] == "not_ready" and gate["reasons"] == [flp.TRIGGER_DISAGREES]
    crossed = [e["day"] for e in gate["sessions"] if e["research_fires"]]
    assert gate["trigger_disagrees"] == crossed and crossed
    assert not any(e["live_fires"] for e in gate["sessions"])


def test_a_candidate_is_judged_at_its_own_decision_time(run):
    live = _days()
    for day in SESSIONS[-5:]:
        _nudged(live, day, "14:59", _f1_dollars(day, 0.06, decision="15:00"))        # biased at 15:00, clean at 14:30
    _nudged(live, SESSIONS[-2], "14:29", _f1_dollars(SESSIONS[-2], 0.2, decision="14:30"))   # one bad value at 14:30

    doc = run(_days(), live, decisions=("14:30", "15:00"),
              candidates=[("F1", "15:00", ">=", 0.5), ("F1", "14:30", "<=", 1.0)])

    at_1500, at_1430 = doc["slot_gate"]["candidates"]
    assert at_1500["reasons"] == [flp.VALUE_BIASED, flp.TRIGGER_UNAVAILABLE]
    assert at_1500["mean_signed_diff_in_spreads"] == pytest.approx(0.06, rel=2e-2) and at_1500["value_not_matched"] == []
    assert at_1430["value_not_matched"] == [SESSIONS[-2]]
    assert at_1430["reasons"] == [flp.VALUE_NOT_MATCHED, flp.VALUE_BIASED, flp.TRIGGER_UNAVAILABLE]   # 0.2 / 5 = 0.04 at that time


def test_the_recorders_history_is_its_own_bound_eligible_values_before_the_day():
    at = "2026-11-02T15:00:20+00:00"
    row = lambda raw, eligible=True, sha=flp.LIVE_RULE_TERMS_SHA256, bound_at=at: {
        "raw": raw, "eligible": eligible, "recorded_at": at, "terms_sha256": sha, "terms_recorded_at": bound_at}
    days = flp.expected_sessions_between("2026-06-01", "2026-10-30")
    recorded = {(d, "15:00", "F1"): row(float(i)) for i, d in enumerate(days)}
    recorded[(days[-1], "15:00", "F1")] = row(999.0, sha=None)               # an earlier generation's value
    recorded[(days[-2], "15:00", "F1")] = row(998.0, eligible=False)
    recorded[(days[-3], "15:00", "F1")] = row(None)
    recorded[(days[-4], "15:00", "F1")] = row(997.0, bound_at="another stamp")
    recorded[(days[-5], "14:30", "F1")] = row(996.0)                         # another decision time
    recorded[("2026-11-03", "15:00", "F1")] = row(995.0)                     # not before the day

    history = flp.recorder_history(recorded, "F1", "15:00", "2026-11-02")

    assert len(history) == 60 and history == tuple(float(i) for i in range(len(days) - 64, len(days) - 4))
    assert flp.recorder_history(None, "F1", "15:00", "2026-11-02") == ()
    assert len(flp.recorder_history(recorded, "F1", "15:00", days[10])) == 10


@pytest.mark.parametrize("candidate, why", [
    (("F9", "15:00", ">=", 0.5), "not a factory feature"),
    (("F1", "12:00", ">=", 0.5), "not one of the compared decision times"),
    (("F1_shift30", "10:00", ">=", 0.5), "no value at 10:00 by definition"),
    (("F1", "15:00", ">", 0.5), "is not one of"),
    (("F1", "15:00", ">=", 0), "is not in (0, 4.0]"),
    (("F1", "15:00", ">=", 4.5), "is not in (0, 4.0]"),
    (("F1", "15:00", ">=", "high"), "is not a number"),
])
def test_a_candidate_that_cannot_be_a_slot_is_refused(candidate, why):
    with pytest.raises(flp.ParityError, match=why.replace("(", r"\(").replace("]", r"\]")):
        flp.parse_candidate(*candidate, ["10:00", "15:00"])


def test_the_command_takes_a_candidate(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(flp, "_clock", lambda: _after_close(SESSIONS[-1]))
    r = _research_db(tmp_path / "r.db", {**_history(), **_days()})
    lv = _live_db(tmp_path / "l.db", _days())
    args = ["--research-db", str(r), "--signals-db", str(lv), "--first", SESSIONS[0], "--last", SESSIONS[-1],
            "--candidate", "F1", "15:00", ">=", "0.5", "--summary"]

    assert flp.main(args) == 0                                               # the exit code is the diagnostic's, as before

    gate = json.loads(capsys.readouterr().out)["slot_gate"]
    assert [(g["feature"], g["decision"], g["op"], g["k"], g["status"]) for g in gate["candidates"]] == [("F1", "15:00", ">=", 0.5, "not_ready")]
    with pytest.raises(flp.ParityError):
        flp.main(args[:-6] + ["--candidate", "F1", "09:45", ">=", "0.5"])


# ── version 5 and the 2026-11-16 addendum to #1160: a value is a decision input only if it was there at the decision ──


def _restamped(hm, second=0, when=lambda r: True):
    """The recorder's rows as written at ``hm`` New York time on their own day, value and binding together."""
    def change(rows):
        for r in rows:
            if when(r):
                r["recorded_at"] = r["terms_recorded_at"] = _utc(r["day"], hm, second)
    return change


def test_what_a_decision_stamp_must_be():
    """An eligible value is a decision input only when stamped at or after the decision time and under 120 seconds after it."""
    day = "2026-11-06"

    def row(at, eligible=True):
        return {"raw": 0.1, "eligible": eligible, "recorded_at": at}

    assert flp.DECISION_WINDOW_S == 120                   # gex-signals factory_slots.DECISION_WINDOW_S: change both or neither
    assert flp.stamp_problem(row(_utc(day, "15:00")), day, "15:00") is None
    assert flp.stamp_problem(row(_utc(day, "15:01", 59)), day, "15:00") is None
    assert flp.stamp_problem(row(_utc(day, "15:01", 59).replace("+00:00", ".900000+00:00")), day, "15:00") is None   # whole seconds, as the recorder counts
    assert flp.stamp_problem(row(_utc(day, "15:02")), day, "15:00") == "value_stamped_after_the_decision_window"
    assert flp.stamp_problem(row(_utc(day, "14:59", 59)), day, "15:00") == "value_stamped_before_the_decision_time"
    assert flp.stamp_problem(row(_utc("2026-11-09", "15:00", 20)), day, "15:00") == "value_stamped_after_the_decision_window"   # another day's 15:00
    assert flp.stamp_problem(row("2026-11-06T20:00:20"), day, "15:00") is None             # no zone is read as UTC, as the recorder reads it
    assert flp.stamp_problem(row("15:00"), day, "15:00") == "value_stamp_unreadable"
    assert flp.stamp_problem(row(None), day, "15:00") == "value_stamp_unreadable"
    assert flp.stamp_problem(row(_utc(day, "18:30"), eligible=False), day, "15:00") is None   # no value is claimed
    assert flp.stamp_problem(None, day, "15:00") is None


@pytest.mark.parametrize(("hm", "second", "stamp"), [
    ("15:02", 0, "value_stamped_after_the_decision_window"),
    ("18:30", 0, "value_stamped_after_the_decision_window"),
    ("14:59", 59, "value_stamped_before_the_decision_time"),
])
def test_an_equal_value_written_outside_the_decision_window_is_not_a_decision_input(run, hm, second, stamp):
    """The addendum: equivalent values delivered after a decision are not equivalent decision inputs."""
    day = "2026-11-06"

    doc = run(_days(), _days(), recorded=_restamped(hm, second, when=lambda r: r["day"] == day and r["feature"] == "F1"))

    f1 = doc["comparison"]["features"]["F1"]
    assert doc["live_ready"] == ["F10", "F11", "F1_shift30"] and doc["schema"] == "factory-live-parity-v5"
    assert f1["reasons"] == [flp.RECORDED_LATE, "insufficient_matched_sessions"]
    assert f1["verdict_on"]["recorded_outside_the_decision_window"] == [day]
    assert f1["values_recorded_outside_the_decision_window"] == 1 and f1["matched_sessions"] == 5
    row = next(r for r in doc["comparison"]["value_rows"] if r["feature"] == "F1" and r["day"] == day)
    # Everything else about the row is in order: the bars agree with research, the value is the one the bars give,
    # and it is bound to these terms with its own stamp. Only the time it was written is wrong.
    assert row["within_tolerance"] is True and row["binding"] == "bound_to_these_terms"
    assert row["recorded_agrees_with_bars"] is True and row["recorded_value"] == row["live"] == row["research"]
    assert row["decision_stamp"] == stamp and row["matched"] is False
    others = {r["decision_stamp"] for r in doc["comparison"]["value_rows"] if r["day"] > SESSIONS[0] and r is not row}
    assert others == {"inside_the_decision_window"}
    assert doc["live_rule"]["decision_window_s"] == 120


def test_no_feature_becomes_ready_by_a_matching_later_recomputation(run):
    """The addendum: no research-only feature becomes live-ready through backward filling. Every value recomputed
    that evening from complete bars equals research, is bound to the terms, and is what the bars give."""
    doc = run(_days(), _days(), recorded=_restamped("18:30"))

    assert doc["live_ready"] == []
    for f in mf.FEATURES:
        feature = doc["comparison"]["features"][f]
        assert feature["reasons"] == [flp.RECORDED_LATE, "insufficient_matched_sessions"]
        assert feature["values_matched"] == 0 and feature["values_out_of_tolerance"] == 0
        assert feature["values_bound_to_the_terms"] == feature["values_required"] == 7
        assert feature["values_recorded_outside_the_decision_window"] == 6          # the first session has no prior: no value claimed
    gate = run(_days(), _days(), recorded=_restamped("18:30"), candidates=[("F1", "15:00", ">=", "0.5")])["slot_gate"]
    assert gate["ready"] == [] and flp.VALUE_NOT_MATCHED in gate["candidates"][0]["reasons"]


def test_replay_a_live_bar_that_arrived_after_the_decision(run):
    """Late arrival: at 15:00 the bar closing at the decision was not there, so the recorder withheld the session and
    wrote no value. The bar came later and the recording is now complete. The bars agree with research; the decision
    did not have them."""
    day = "2026-11-06"
    at_the_time = _days()
    del at_the_time[day]["14:59"]
    _, withheld, _ = flp.live_features(at_the_time, None, day, "15:00")
    assert set(map(tuple, withheld.values())) == {(flp.PREFIX_INCOMPLETE,)}

    def as_first_recorded(rows):
        for r in rows:
            if r["day"] == day:
                r["raw"], r["eligible"] = None, 0
                r["recorded_at"] = r["terms_recorded_at"] = _utc(day, "15:02")       # the window was over: missed_decision_window

    doc = run(_days(), _days(), recorded=as_first_recorded)

    assert doc["live_ready"] == [] and doc["comparison"]["availability_mismatch"] == []   # on today's bars the two sides agree
    for f in mf.FEATURES:
        feature = doc["comparison"]["features"][f]
        assert feature["reasons"] == [flp.RECORDED_DIFFERS, "insufficient_matched_sessions"]
        assert feature["verdict_on"]["recorded_values_differ_from_the_bars"] == [day]
        row = next(r for r in doc["comparison"]["value_rows"] if r["feature"] == f and r["day"] == day)
        assert row["within_tolerance"] is True and row["live"] is not None
        assert row["recorded_value"] is None and row["recorded_agrees_with_bars"] is False and row["matched"] is False
        assert row["decision_stamp"] is None              # the recorder claimed no value, so there is no stamp to judge


def test_replay_a_live_bar_revised_after_the_decision_keeps_the_value_first_recorded(run):
    """Revision: the bar closing at the decision was corrected after the recorder wrote. The report shows the value
    as first recorded beside what the corrected bars give, and the corrected one is never taken in its place."""
    day = "2026-11-06"
    first, _, _ = flp.live_features(_days(), None, day, "15:00")
    revised = _days()
    _nudged(revised, day, "14:59", _f1_dollars(day, 0.05))                # a correction well inside the tolerance

    def as_first_recorded(rows):
        for r in rows:
            if r["day"] == day:
                r["raw"] = first[r["feature"]]

    doc = run(_days(), revised, recorded=as_first_recorded)

    assert doc["live_ready"] == []
    for f in mf.FEATURES:
        feature = doc["comparison"]["features"][f]
        row = next(r for r in doc["comparison"]["value_rows"] if r["feature"] == f and r["day"] == day)
        assert row["recorded_value"] == first[f] == row["research"] and row["live"] != first[f]
        assert row["binding"] == "bound_to_these_terms" and row["decision_stamp"] == "inside_the_decision_window"
        assert row["recorded_agrees_with_bars"] is False and row["matched"] is False
        assert flp.RECORDED_DIFFERS in feature["reasons"] and feature["recorded_values_that_differ_from_the_bars"] == 1
    f1 = next(r for r in doc["comparison"]["value_rows"] if r["feature"] == "F1" and r["day"] == day)
    assert f1["within_tolerance"] is True and f1["diff_in_spreads"] == pytest.approx(0.05, rel=1e-3)


def test_only_the_recorded_price_features_can_be_judged_and_level_features_are_tagged_unchecked(run):
    """The addendum's watch and contact clocks apply when a level feature (#1158, #1132) is integrated. None is: this
    report judges four price features, and no other feature can be named ready, whatever its tag says. A level
    feature's tag changing from "parity unchecked" fails here, which is when those clock tests are owed."""
    assert set(flp.FEATURE_TYPE) == set(mf.FEATURES) == {"F1", "F10", "F11", "F1_shift30"}
    doc = run(_days(), _days())
    assert set(doc["comparison"]["features"]) == set(mf.FEATURES)
    assert set(doc["live_ready"]) | set(doc["research_only"]) == set(mf.FEATURES)
    outside = [f for f in mf.REGISTRY if f not in mf.FEATURES]
    assert len(outside) == len(mf.REGISTRY) - 4
    for f in outside:
        with pytest.raises(flp.ParityError, match="is not a factory feature"):
            flp.parse_candidate(f, "15:00", ">=", 1.0, ["15:00"])
    levels = [f for f in mf.REGISTRY if mf.FEATURE_CLASS[f] == "options"]
    assert levels and all("parity unchecked" in mf.REGISTRY[f]["live_source"] and "#1160" in mf.REGISTRY[f]["live_source"]
                          for f in levels)
    # Two features carry the SPY live tag and are not recorded live: the tag is a claim this report never tested.
    tagged_live = sorted(f for f in outside if mf.REGISTRY[f]["live_source"] == mf.FEATURE_SPECS["F1"]["live_source"])
    assert tagged_live == ["F10_shift30", "F11_shift30"]


def test_the_report_reads_intraday_receipts_and_no_daily_context(run, monkeypatch):
    """The addendum: daily context (#1191) is kept apart from intraday receipts. The report reads the research bars,
    the live bars, the backstop bars and the recorder's own rows, and nothing else in either database."""
    read: set[str] = set()
    real = flp._connect

    def watched(path):
        con = real(path)

        def note(action, table, *_):
            if action == sqlite3.SQLITE_READ:
                read.add(table)
            return sqlite3.SQLITE_OK

        con.set_authorizer(note)
        return con

    monkeypatch.setattr(flp, "_connect", watched)
    run(_days(), _days(), backstop={}, candidates=[("F1", "15:00", ">=", "0.5")])

    assert read == {"bars", "bars_1m_live", "factory_backstop_bars", "factory_feature_values", "factory_feature_terms",
                    "sqlite_master"}


def test_every_count_has_its_denominator_and_every_excluded_row_its_cause(run):
    """The addendum: valid matched rows, and excluded, missing and late cases, each against a denominator. One window
    with a lost minute, a session the feed lacks, a value out of tolerance, an unbound value and a late one."""
    live = _days(LONGER)
    del live["2026-11-04"]["10:01"]                                           # a lost minute, no backstop
    del live["2026-11-06"]                                                    # a session the feed lacks
    _nudged(live, "2026-11-10", "14:59", _f1_dollars("2026-11-10", 0.12, LONGER))    # out of tolerance

    def recorder(rows):
        for r in rows:
            if r["feature"] == "F1" and r["day"] == "2026-11-11":
                r["terms_sha256"] = None                                      # an older generation's value
            if r["feature"] == "F1" and r["day"] == "2026-11-12":
                r["recorded_at"] = r["terms_recorded_at"] = _utc(r["day"], "15:05")   # late

    doc = run(_days(LONGER), live, last=LONGER[-1], recorded=recorder)

    held = [d for d in LONGER if d != "2026-11-06"]
    assert doc["expected_sessions_missing"] == {"research": [], "live": ["2026-11-06"]}
    assert doc["comparison"]["overlapping_sessions"] == held and doc["research_sessions"] == len(LONGER)
    assert doc["live_ready"] == []
    for f in mf.FEATURES:
        feature = doc["comparison"]["features"][f]
        rows = [r for r in doc["comparison"]["value_rows"] if r["feature"] == f]
        assert len(rows) == len(held) and all(r["applicable"] for r in rows)
        matched = [r for r in rows if r["matched"]]
        excluded = [r for r in rows if not r["matched"]]
        assert feature["values_required"] == len(rows) == len(matched) + len(excluded)
        assert feature["values_matched"] == len(matched)
        assert feature["values_compared"] == sum(r["abs_diff"] is not None for r in rows)
        assert feature["values_out_of_tolerance"] == sum(r["within_tolerance"] is False for r in rows)
        assert feature["values_not_bound"] == sum(r["binding"] != "bound_to_these_terms" for r in rows)
        assert feature["values_bound_to_the_terms"] + feature["values_not_bound"] == len(rows)
        assert feature["values_recorded_outside_the_decision_window"] == sum((r["decision_stamp"] or "").startswith("value_") for r in rows)
        assert feature["recorded_values_that_differ_from_the_bars"] == sum(
            r["binding"] == "bound_to_these_terms" and r["recorded_agrees_with_bars"] is False for r in rows)
        for r in excluded:                                # no row is dropped without a stated cause
            assert (not r["both_eligible"] or r["within_tolerance"] is not True or r["binding"] != "bound_to_these_terms"
                    or r["recorded_agrees_with_bars"] is not True or (r["decision_stamp"] or "").startswith("value_")), r
        verdict = feature["verdict_on"]
        assert verdict["sessions"] == LONGER[-5:] and verdict["required"] == 5
        assert verdict["matched"] == sum(all(r["matched"] for r in rows if r["day"] == d) for d in LONGER[-5:] if d in held)
        assert verdict["missing"] == {"research": [], "live": ["2026-11-06"]}
    f1 = doc["comparison"]["features"]["F1"]
    assert (f1["values_out_of_tolerance"], f1["values_not_bound"], f1["values_recorded_outside_the_decision_window"]) == (1, 1, 1)
    assert f1["reasons"] == [flp.SESSION_MISSING, flp.RULE_NOT_RECORDED, flp.RECORDED_LATE, "insufficient_matched_sessions",
                             "value_out_of_tolerance", "availability_mismatch"]
    live_side = doc["live_availability"]
    withheld = sum(bool(r["signal_withheld"]) for r in live_side["rows"])
    assert live_side["sessions"] == len(held) and live_side["signal_rows"] == len(held) == live_side["signal_eligible"] + withheld
    assert live_side["label_rows"] == live_side["label_available"] + sum(bool(r["label_missing"]) for r in live_side["rows"])
    assert sum(live_side["signal_withheld_reasons"].values()) == withheld == 2         # the first session, and the one after the gap
    for f in mf.FEATURES:
        per_feature = sum(f in r["feature_withheld"] for r in live_side["rows"])
        assert live_side["feature_eligible"][f] == live_side["signal_eligible"] - per_feature
    labels = doc["comparison"]["labels"]
    assert labels["rows"] == labels["compared"] + labels["missing_on_a_side"] == len(held)


def test_the_receipt_rule_terms_and_their_sha256_are_the_pinned_ones(run):
    """The decision window and the history admission rule are frozen in this report's own terms. The recorder's terms
    do not carry the window, so a change to either is a change of this text and digest, in review."""
    assert flp.RECEIPT_RULE_TERMS_JSON == (
        '{"decision_window":"an eligible value counts only if decision time <= recorded_at < decision time + '
        'decision_window_s, by whole seconds, on the row\'s New York day","decision_window_s":120,'
        '"history_admission":"a candidate\'s trigger is taken on the recorder\'s own history (eligible, a number, bound '
        'to the live rule terms with the value\'s stamp); a history holding a value recorded outside the decision window '
        'certifies no trigger","live_rule_terms_sha256":"31e017f78c4d676ce6e405b1beb708cde9fbc370448a4a6c923b3e39db4b50d9",'
        '"stamp_limit":"a stamp is when the value row was written; it does not show which vintage of a bar was read",'
        '"terms":"factory-live-parity-receipt/1"}')
    assert flp.RECEIPT_RULE_TERMS_SHA256 == "9a617ca7c7d8afbb965a9209de4a36c74daae8c006da550117732ef90cb8677a"
    assert flp.RECEIPT_RULE_TERMS["decision_window_s"] == flp.DECISION_WINDOW_S
    assert flp.RECEIPT_RULE_TERMS["live_rule_terms_sha256"] == flp.LIVE_RULE_TERMS_SHA256
    rule = run(_days(), _days())["live_rule"]["receipt_rule"]
    assert rule == {"terms": flp.RECEIPT_RULE_TERMS, "terms_sha256": flp.RECEIPT_RULE_TERMS_SHA256}


def test_one_late_value_in_the_earlier_history_changes_a_ready_candidate_to_not_ready(run):
    """Codex on e20cd7d1: the history behind a trigger lies mostly outside the verdict sessions. One equal value there,
    recorded after its decision window, would feed the z that the verdict sessions certify."""
    op, k = _a_threshold_research_crosses(run)
    earlier = HISTORY_DAYS[-10]                           # well before the verdict sessions, inside every one's history
    history = _recorder_history()

    def one_late(rows):
        history(rows)
        for r in rows:
            if r["day"] == earlier and r["feature"] == "F1":
                r["recorded_at"] = r["terms_recorded_at"] = _utc(earlier, "18:30")

    ready = run(_days(), _days(), candidates=[("F1", "15:00", op, k)], recorded=history)["slot_gate"]["candidates"][0]
    doc = run(_days(), _days(), candidates=[("F1", "15:00", op, k)], recorded=one_late)

    gate = doc["slot_gate"]["candidates"][0]
    assert ready["status"] == "ready" and ready["recorder_history_not_admitted"] == []
    assert gate["status"] == "not_ready" and gate["reasons"] == [flp.HISTORY_NOT_ADMITTED]
    assert gate["recorder_history_not_admitted"] == SESSIONS[-5:] and doc["slot_gate"]["ready"] == []
    # Nothing else moved: the value is equal, bound and in the recorder's own history, so every z and every
    # agreement is what it was. Only the time that one earlier value was written differs.
    assert gate["value_not_matched"] == [] and gate["trigger_unavailable"] == [] and gate["trigger_disagrees"] == []
    for was, now in zip(ready["sessions"], gate["sessions"]):
        assert now["recorder_history_recorded_outside_the_decision_window"] == [earlier]
        assert was["recorder_history_recorded_outside_the_decision_window"] == []
        assert (now["live_z"], now["research_z"], now["agree"]) == (was["live_z"], was["research_z"], was["agree"])
    assert "F1" in doc["live_ready"], "the feature diagnostic does not read the history, and judges no slot"


def test_a_late_value_blocks_only_while_it_is_in_the_history():
    days = flp.expected_sessions_between("2026-06-01", "2026-10-30")

    def at(day, hm="15:00"):
        return _utc(day, hm, 20)

    def row(day, stamp):
        return {"raw": 0.001, "eligible": True, "recorded_at": stamp, "terms_sha256": flp.LIVE_RULE_TERMS_SHA256,
                "terms_recorded_at": stamp}

    recorded = {(d, "15:00", "F1"): row(d, at(d)) for d in days}
    late = days[5]
    recorded[(late, "15:00", "F1")] = row(late, at(late, "18:30"))

    assert flp.recorder_history_not_admitted(recorded, "F1", "15:00", days[40]) == [late]
    assert len(flp.recorder_history(recorded, "F1", "15:00", days[40])) == 40             # the recorder's own history keeps it
    assert flp.recorder_history_not_admitted(recorded, "F1", "15:00", days[66]) == []    # sixty later values: it has left
    assert flp.recorder_history_not_admitted(recorded, "F1", "15:00", late) == []        # not before its own day
    assert flp.recorder_history_not_admitted(None, "F1", "15:00", days[40]) == []
