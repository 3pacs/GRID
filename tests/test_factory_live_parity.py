"""Research-versus-live parity for factory features (3pacs/GRID#1160).

* The same factory function runs on both sides.
* A feature is live-ready only with five matched sessions, no value out of
  tolerance and no session where the two sides disagree about availability.
* A live recording with a missing minute fails the research rules: values that
  agree are not parity when live would abstain.
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


def _live_db(path, bars, src_of=lambda day, hm: "schwab"):
    con = sqlite3.connect(path)
    con.execute("create table bars_1m_live (symbol text, ts_utc text, src text, day text, o real, h real, "
                "l real, c real, v integer, primary key (symbol, ts_utc))")
    for day, session in bars.items():
        for hm, b in session.items():
            ts = datetime.fromisoformat(f"{day}T{hm}").replace(tzinfo=NY).astimezone(UTC).isoformat()
            con.execute("insert into bars_1m_live values ('SPY', ?, ?, ?, ?, ?, ?, ?, ?)",
                        (ts, src_of(day, hm), day, *b))
    con.commit()
    con.close()
    return path


@pytest.fixture
def run(tmp_path):
    def go(research, live, **kwargs):
        return flp.report(_research_db(tmp_path / "r.db", research), _live_db(tmp_path / "l.db", live, **kwargs),
                          first=SESSIONS[0], last=SESSIONS[-1], decisions=["15:00"], horizons=["close"])
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


def test_one_missing_morning_minute_withholds_that_day_and_the_next(run):
    live = _days()
    del live["2026-11-04"]["10:01"]

    doc = run(_days(), live)

    by_day = {r["day"]: r["signal_withheld"] for r in doc["live_availability"]["rows"]}
    assert by_day["2026-11-04"] == [flp.PREFIX_INCOMPLETE]
    assert by_day["2026-11-05"] == [flp.PRIOR_INCOMPLETE]
    assert by_day["2026-11-06"] == []
    assert doc["live_availability"]["signal_eligible"] == 4
    assert doc["comparison"]["features"]["F1"]["matched_sessions"] == 4
    assert doc["research_only"] == sorted(mf.FEATURES)


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
