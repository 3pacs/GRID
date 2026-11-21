"""Research-versus-live parity for factory features (3pacs/GRID#1160).

* The same factory function runs on both sides.
* A feature is live-ready only with five matched sessions, no value out of
  tolerance and no session where the two sides disagree about availability.
* A live recording with a missing read bar, or more than two lost minutes, fails:
  values that agree are not parity when live would abstain.
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


def _live_db(path, bars, src_of=lambda day, hm: "schwab", backstop=None):
    """``backstop``: {(day, hm): (bar, "HH:MM" New York time it was first seen that day)}; None = no such table."""
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
    return path


@pytest.fixture
def run(tmp_path):
    calls = []

    def go(research, live, decisions=("15:00",), **kwargs):
        calls.append(1)                                   # a test may build more than one pair of databases
        r, lv = tmp_path / f"r{len(calls)}.db", tmp_path / f"l{len(calls)}.db"
        return flp.report(_research_db(r, research), _live_db(lv, live, **kwargs),
                          first=SESSIONS[0], last=SESSIONS[-1], decisions=list(decisions), horizons=["close"])
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

    doc = run({d: _days()[d] for d in few}, {d: _days()[d] for d in few})

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
    assert doc["schema"] == "factory-live-parity-v2"


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
    assert features["F10"]["reasons"] == ["availability_mismatch"] and features["F10"]["matched_sessions"] == 5
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


def test_a_feature_with_no_value_on_either_side_by_definition_agrees(run):
    """F1_shift30 at 10:00 needs the bar 30 minutes earlier, which is before the open. It blocked the feature in version 1."""
    doc = run(_days(), _days(), decisions=("10:00", "15:00"))

    assert doc["live_ready"] == sorted(mf.FEATURES)
    shift = doc["comparison"]["features"]["F1_shift30"]
    assert shift["matched_sessions"] == 6 and shift["values_compared"] == 6            # the 10:00 rows have no value
    live = _days()
    del live["2026-11-05"]["09:59"]                        # a value on one side only is still a disagreement
    assert "F1_shift30" in run(_days(), live, decisions=("10:00", "15:00"))["research_only"]


def test_a_value_outside_tolerance_is_counted_and_blocks_the_feature(run):
    live = _days()
    o, h, low, c, v = live["2026-11-05"]["14:59"]
    live["2026-11-05"]["14:59"] = (o, h, low, c + 0.30, v)     # about 4 bp on the return

    doc = run(_days(), live)

    f1 = doc["comparison"]["features"]["F1"]
    assert f1["values_out_of_tolerance"] == 1 and f1["matched_sessions"] == 5
    assert f1["status"] == "research_only" and f1["reasons"] == ["value_out_of_tolerance"]
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
