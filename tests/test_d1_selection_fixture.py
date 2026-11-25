"""§8 fixed-clock fixture and the §5.6 leakage fixture for D1 selection. Synthetic, offline, no outcomes.

Every time is written out below; nothing reads a wall clock. One on-time original passes; late,
revised, missing and duplicate observations each get their own refusal and admit nothing.
"""

from __future__ import annotations

import ast
import dataclasses
from datetime import date, datetime, timedelta
import math
from pathlib import Path

import pytest

from analysis import d1_ranking as R
from analysis import d1_selection as S

ET = S.ET
CAL = S.RegularCalendar()
BP = R.Breakpoints(R.OK, (0.5, 1.0, 1.5, 2.0), None, 10)


def at(day: date, hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hh, mm, ss, tzinfo=ET)


def make_tape(first=date(2026, 6, 1), last=date(2027, 1, 20), clocks=True) -> dict:
    """A deterministic, non-flat close for every regular session; each close is seen a minute after it prints."""
    tape, i, d = {}, 0, first
    while d <= last:
        if CAL.is_open(d):
            close = round(100 + 4 * math.sin(i * 0.9) + 0.05 * i, 4)
            seen = at(d, CAL.close_time(d).hour, CAL.close_time(d).minute) + timedelta(minutes=1)
            tape[d] = S.Price(close, seen, seen) if clocks else S.Price(close)
            i += 1
        d += timedelta(days=1)
    return tape


TAPE = make_tape()


def filing(acc: str, a: datetime | None, items=("2.02",), ret=None, vis=None, sha=None) -> S.Filing:
    default = a + timedelta(minutes=1) if a is not None else None
    return S.Filing(acc, tuple(items), a, default if ret is None else ret, default + timedelta(minutes=1)
                    if vis is None and default is not None else vis, sha or f"sha-{acc}")


def run(filings, tape=None, **kw) -> S.SelectionResult:
    return S.select_events(filings, TAPE if tape is None else tape, breakpoints=BP, **kw)


def only(res: S.SelectionResult) -> S.Decision:
    assert len(res.decisions) == 1
    return res.decisions[0]


def refused(res, status):
    d = only(res)
    assert d.status == status, d
    assert res.selected == () and res.diagnostic == ()  # zero admitted output
    assert d.deadline_d is None or d.deadline_d.tzinfo is not None  # the cutoff is logged where it is known
    assert d.reason
    return d


D17 = date(2026, 11, 17)  # a Tuesday, regular close 16:00
ON_TIME = at(D17, 11, 30)


# ---- the on-time original ----

def test_one_on_time_original_is_selected_with_every_clock_logged():
    res = run([filing("A1", ON_TIME)])
    d = only(res)
    assert d.status == S.SELECTED and len(res.selected) == 1 and res.diagnostic == ()
    ev = res.selected[0]
    assert (ev.day0, ev.p_prev_date, ev.entry_day, ev.family) == (D17, date(2026, 11, 16), date(2026, 11, 18), R.F1)
    assert ev.cutoff_o == at(D17, 16, 0) and ev.deadline_d == at(D17, 20, 0) == d.deadline_d
    # publication, arrival and decision times are all preserved on the decision
    assert d.acceptance == ON_TIME and d.retrieved_at == at(D17, 11, 31) and d.visible_at == at(D17, 11, 32)
    assert d.reason and d.duplicates_dropped == 0 and d.ignored_revisions == ()
    closes = [TAPE[x].close for x in S._sessions_ending(ev.p_prev_date, 61, CAL)]
    assert ev.strength == R.event_strength(closes, TAPE[D17].close).value
    assert ev.quintile == R.quintile(ev.strength, BP.values)


def test_a_selected_event_carries_no_outcome_field():
    names = {f.name for f in dataclasses.fields(S.SelectedEvent)}
    assert not {n for n in names if "outcome" in n or "return" in n or "pnl" in n}


# ---- day 0, P-1, O and D ----

@pytest.mark.parametrize("a, day0, p_prev", [
    (at(D17, 7, 0), D17, date(2026, 11, 16)),                              # pre-open: same day
    (at(D17, 11, 30), D17, date(2026, 11, 16)),                            # intraday: same day
    (at(D17, 16, 30), date(2026, 11, 18), D17),                            # after the close: next session
    (at(D17, 16, 0, 0), date(2026, 11, 18), date(2026, 11, 16)),           # A at the close: that close is not "before A"
    (at(D17, 16, 0, 1), date(2026, 11, 18), D17),                          # a second later it is
    (at(D17, 15, 54, 59), D17, date(2026, 11, 16)),                        # just inside the 5-minute margin
    (at(D17, 15, 55, 0), date(2026, 11, 18), date(2026, 11, 16)),          # exactly 5 minutes: close not strictly after
    (at(date(2026, 11, 27), 12, 54, 59), date(2026, 11, 27), date(2026, 11, 25)),  # early close 13:00: same day
    (at(date(2026, 11, 27), 12, 55, 0), date(2026, 11, 30), date(2026, 11, 25)),
    (at(date(2026, 11, 27), 12, 58), date(2026, 11, 30), date(2026, 11, 25)),      # §5.6: day 0 is the next session
    (at(date(2026, 11, 26), 10, 0), date(2026, 11, 27), date(2026, 11, 25)),       # Thanksgiving: market closed
    (at(date(2026, 11, 21), 10, 0), date(2026, 11, 23), date(2026, 11, 20)),       # Saturday
])
def test_day0_and_p_prev(a, day0, p_prev):
    assert S.day0_for(a, CAL) == day0 and S.prev_close_date(a, CAL) == p_prev


def test_an_acceptance_in_the_last_five_minutes_leaves_that_close_unread():
    """Literal §5.1: day 0 is tomorrow, P-1 is the close before A (yesterday's); today's close is neither."""
    a = at(D17, 15, 57)
    ev = run([filing("G1", a)]).selected[0]
    assert (ev.p_prev_date, ev.day0) == (date(2026, 11, 16), date(2026, 11, 18))
    assert D17 not in (ev.p_prev_date, ev.day0)


def test_the_deadline_is_20_00_et_on_day0_whatever_the_close():
    early = run([filing("E1", at(date(2026, 11, 27), 12, 58))]).selected[0]
    assert early.cutoff_o == at(date(2026, 11, 30), 16, 0) and early.deadline_d == at(date(2026, 11, 30), 20, 0)
    half = run([filing("E2", at(date(2026, 11, 27), 9, 0))]).selected[0]
    assert half.cutoff_o == at(date(2026, 11, 27), 13, 0) and half.deadline_d == at(date(2026, 11, 27), 20, 0)


# ---- late ----

def test_a_filing_retrieved_after_the_deadline_is_late():
    refused(run([filing("L1", ON_TIME, ret=at(D17, 20, 0, 1), vis=at(D17, 20, 1))]), S.EXCLUDED_LATE_INPUT)


def test_a_filing_visible_after_the_deadline_is_late():
    refused(run([filing("L2", ON_TIME, ret=at(D17, 11, 31), vis=at(D17, 20, 0, 1))]), S.EXCLUDED_LATE_INPUT)


def test_a_filing_exactly_at_the_deadline_is_on_time():
    res = run([filing("L3", ON_TIME, ret=at(D17, 20, 0), vis=at(D17, 20, 0))])
    assert only(res).status == S.SELECTED


def test_c0_visible_a_minute_after_the_deadline_is_late_and_not_deferred():
    tape = dict(TAPE)
    tape[D17] = S.Price(TAPE[D17].close, at(D17, 20, 1), at(D17, 20, 1))
    d = refused(run([filing("L4", ON_TIME)], tape), S.EXCLUDED_LATE_INPUT)
    assert "after the deadline" in d.reason


@pytest.mark.parametrize("which", ["retrieved_at", "visible_at"])
def test_c0_needs_both_clocks_at_or_before_the_deadline(which):
    tape = dict(TAPE)
    late = at(D17, 20, 0, 1)
    ok = at(D17, 20, 0)
    tape[D17] = S.Price(TAPE[D17].close, late if which == "retrieved_at" else ok, late if which == "visible_at" else ok)
    refused(run([filing("L5", ON_TIME)], tape), S.EXCLUDED_LATE_INPUT)


def test_c0_exactly_at_the_deadline_is_on_time():
    tape = dict(TAPE)
    tape[D17] = S.Price(TAPE[D17].close, at(D17, 20, 0), at(D17, 20, 0))
    assert only(run([filing("L6", ON_TIME)], tape)).status == S.SELECTED


def test_a_late_price_before_p_prev_is_late_too():
    tape = dict(TAPE)
    tape[date(2026, 11, 16)] = S.Price(TAPE[date(2026, 11, 16)].close, at(D17, 20, 5), at(D17, 20, 5))
    refused(run([filing("L7", ON_TIME)], tape), S.EXCLUDED_LATE_INPUT)


def test_a_missing_c0_is_late_and_counted_never_deferred():
    tape = {k: v for k, v in TAPE.items() if k != D17}
    d = refused(run([filing("L8", ON_TIME)], tape), S.EXCLUDED_LATE_INPUT)
    assert "never deferred" in d.reason


# ---- missing ----

@pytest.mark.parametrize("case", ["acceptance", "retrieved", "visible", "naive-acceptance", "naive-retrieved"])
def test_a_missing_or_zoneless_filing_clock_is_unknown_availability(case):
    f = filing("M1", ON_TIME)
    f = {"acceptance": dataclasses.replace(f, acceptance=None),
         "retrieved": dataclasses.replace(f, retrieved_at=None),
         "visible": dataclasses.replace(f, visible_at=None),
         "naive-acceptance": dataclasses.replace(f, acceptance=datetime(2026, 11, 17, 11, 30)),
         "naive-retrieved": dataclasses.replace(f, retrieved_at=datetime(2026, 11, 17, 11, 31))}[case]
    refused(run([f]), S.EXCLUDED_UNKNOWN_AVAILABILITY)


@pytest.mark.parametrize("which", ["retrieved_at", "visible_at"])
def test_a_present_c0_with_an_unknown_clock_is_unknown_availability(which):
    tape = dict(TAPE)
    p = TAPE[D17]
    tape[D17] = dataclasses.replace(p, **{which: None})
    refused(run([filing("M2", ON_TIME)], tape), S.EXCLUDED_UNKNOWN_AVAILABILITY)


@pytest.mark.parametrize("bad", [0.0, -1.0, None, float("nan"), "x"])
def test_a_zero_or_unusable_c0_value_is_excluded_input(bad):
    tape = dict(TAPE)
    tape[D17] = dataclasses.replace(TAPE[D17], close=bad)
    refused(run([filing("M3", ON_TIME)], tape), S.EXCLUDED_INPUT)


@pytest.mark.parametrize("bad", [None, 0.0, float("nan")])
def test_a_zero_or_missing_p_prev_is_excluded_input(bad):
    tape = dict(TAPE)
    tape[date(2026, 11, 16)] = dataclasses.replace(TAPE[date(2026, 11, 16)], close=bad)
    refused(run([filing("M4", ON_TIME)], tape), S.EXCLUDED_INPUT)


def test_an_absent_p_prev_is_excluded_input():
    tape = {k: v for k, v in TAPE.items() if k != date(2026, 11, 16)}
    refused(run([filing("M5", ON_TIME)], tape), S.EXCLUDED_INPUT)


def test_a_flat_history_has_zero_volatility_and_is_excluded_with_no_substitute():
    flat = {k: dataclasses.replace(v, close=100.0) for k, v in TAPE.items()}
    d = refused(run([filing("M6", ON_TIME)], flat), S.EXCLUDED_INPUT)
    assert "volatility" in d.reason


def test_too_short_a_history_is_excluded_input():
    short = {k: v for k, v in TAPE.items() if k >= date(2026, 10, 1)}
    refused(run([filing("M7", ON_TIME)], short), S.EXCLUDED_INPUT)


def test_a_filing_with_no_family_is_refused():
    refused(run([filing("N1", ON_TIME, items=("7.01", "9.01"))]), S.EXCLUDED_NO_FAMILY)
    refused(run([filing("N2", ON_TIME, items=())]), S.EXCLUDED_NO_FAMILY)


@pytest.mark.parametrize("bad", [None, "not a filing", 7, {"accession": "x"}])
def test_a_member_that_is_not_a_filing_is_refused_not_raised(bad):
    refused(run([bad]), S.EXCLUDED_MALFORMED)


def test_a_filing_with_a_non_string_accession_is_refused():
    f = dataclasses.replace(filing("X", ON_TIME), accession=["a"])
    refused(run([f]), S.EXCLUDED_MALFORMED)


# ---- revised ----

def _versions(second_visible, second_sha="sha-B"):
    a = filing("R1", ON_TIME, sha="sha-A")
    b = dataclasses.replace(a, content_sha256=second_sha, visible_at=second_visible, retrieved_at=second_visible)
    return [a, b]


def test_two_versions_visible_by_the_deadline_are_refused_as_revised():
    refused(run(_versions(at(D17, 14, 0))), S.EXCLUDED_REVISED_INPUT)


def test_a_revision_exactly_at_the_deadline_is_still_a_revision():
    refused(run(_versions(at(D17, 20, 0))), S.EXCLUDED_REVISED_INPUT)


def test_a_revision_after_the_deadline_is_ignored_and_the_decision_stands():
    base = run([filing("R1", ON_TIME, sha="sha-A")])
    res = run(_versions(at(D17, 20, 0, 1)))
    d = only(res)
    assert d.status == S.SELECTED and d.ignored_revisions == ("sha-B",)
    assert res.selected == base.selected  # the later version changed nothing that was decided


def test_a_revision_with_no_visibility_time_cannot_be_placed_and_is_unknown():
    refused(run(_versions(None)), S.EXCLUDED_UNKNOWN_AVAILABILITY)


def test_the_order_the_versions_arrive_in_does_not_change_the_decision():
    v = _versions(at(D17, 20, 0, 1))
    assert run(v) == run(list(reversed(v)))
    w = _versions(at(D17, 14, 0))
    assert only(run(w)).status == only(run(list(reversed(w)))).status == S.EXCLUDED_REVISED_INPUT


# ---- duplicate ----

def test_identical_re_observations_are_one_event_with_the_earliest_availability():
    a = filing("D1", ON_TIME, sha="same")
    later = dataclasses.replace(a, retrieved_at=at(D17, 22, 0), visible_at=at(D17, 22, 1))
    again = dataclasses.replace(a, retrieved_at=at(D17, 23, 0), visible_at=at(D17, 23, 1))
    res = run([later, again, a])
    d = only(res)
    assert d.status == S.SELECTED and len(res.selected) == 1 and d.duplicates_dropped == 2
    assert d.visible_at == a.visible_at  # first availability, not the re-ingestion


def test_one_event_per_accession_and_item_2_02_makes_it_f1_only():
    both = filing("D2", ON_TIME, items=("1.01", "2.02", "2.01"))
    other = filing("D3", ON_TIME, items=("1.01", "2.01"))
    res = run([both, other])
    fam = {e.accession: e.family for e in res.selected}
    assert fam == {"D2": R.F1, "D3": R.F2} and len(res.selected) == 2


def test_the_same_content_under_two_accessions_is_two_events():
    res = run([filing("D4", ON_TIME, sha="x"), filing("D5", ON_TIME, sha="x")])
    assert [e.accession for e in res.selected] == ["D4", "D5"]


# ---- historical ----

def test_a_historical_run_is_diagnostic_only_and_never_selected():
    tape = make_tape(date(2023, 6, 1), date(2024, 6, 1), clocks=False)
    res = run([filing("H1", at(date(2024, 3, 12), 11, 30))], tape, period=S.HISTORICAL)
    d = only(res)
    assert d.status == S.DIAGNOSTIC_ONLY and d.label == S.PROXY_EOD_BY_DEADLINE
    assert res.selected == () and len(res.diagnostic) == 1


def test_a_historical_run_never_applies_the_late_exclusion():
    tape = make_tape(date(2023, 6, 1), date(2024, 6, 1), clocks=False)
    late = filing("H2", at(date(2024, 3, 12), 11, 30), ret=at(date(2024, 3, 14), 9, 0), vis=at(date(2024, 3, 14), 9, 0))
    assert only(run([late], tape, period=S.HISTORICAL)).status == S.DIAGNOSTIC_ONLY
    assert only(run([late], make_tape(date(2023, 6, 1), date(2024, 6, 1)), period=S.HISTORICAL)).status == S.DIAGNOSTIC_ONLY


def test_a_historical_missing_c0_is_an_input_exclusion_not_a_late_one():
    tape = {k: v for k, v in make_tape(date(2023, 6, 1), date(2024, 6, 1), clocks=False).items() if k != date(2024, 3, 12)}
    d = refused(run([filing("H3", at(date(2024, 3, 12), 11, 30))], tape, period=S.HISTORICAL), S.EXCLUDED_INPUT)
    assert d.status != S.EXCLUDED_LATE_INPUT


def test_an_unknown_period_is_refused():
    with pytest.raises(ValueError):
        run([filing("P1", ON_TIME)], period="forward ")


# ---- the refusals are distinct and admit nothing ----

def test_the_refusal_codes_are_distinct_and_each_admits_nothing():
    cases = {
        S.EXCLUDED_LATE_INPUT: run([filing("Z1", ON_TIME, ret=at(D17, 21, 0), vis=at(D17, 21, 0))]),
        S.EXCLUDED_REVISED_INPUT: run(_versions(at(D17, 14, 0))),
        S.EXCLUDED_UNKNOWN_AVAILABILITY: run([dataclasses.replace(filing("Z3", ON_TIME), acceptance=None)]),
        S.EXCLUDED_INPUT: run([filing("Z4", ON_TIME)], {k: dataclasses.replace(v, close=0.0) if k == D17 else v
                                                         for k, v in TAPE.items()}),
        S.EXCLUDED_NO_FAMILY: run([filing("Z5", ON_TIME, items=("7.01",))]),
    }
    assert len(set(cases)) == len(cases) == 5
    for status, res in cases.items():
        assert only(res).status == status and res.selected == () and res.diagnostic == ()
    assert set(cases) <= set(S.REFUSALS)


def test_a_zero_day0_move_has_direction_zero_and_is_not_excluded():
    tape = dict(TAPE)
    tape[D17] = dataclasses.replace(TAPE[D17], close=TAPE[date(2026, 11, 16)].close)
    ev = run([filing("Q1", ON_TIME)], tape).selected[0]
    assert ev.direction == 0 and ev.strength == 0.0 and ev.quintile == 1


# ---- fixed clock and offline ----

def test_the_module_reads_no_wall_clock_database_or_network():
    path = Path(S.__file__)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {n.attr for n in ast.walk(tree)
                                                                          if isinstance(n, ast.Attribute)}
    assert not names & {"now", "utcnow", "today", "time_ns", "monotonic", "perf_counter", "urlopen", "connect"}
    imported = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            imported.update(a.name for a in n.names)
        elif isinstance(n, ast.ImportFrom):
            imported.add(n.module or "")
    assert imported <= {"__future__", "dataclasses", "datetime", "typing", "zoneinfo", "analysis",
                        "ingestion"}, imported


# ---- §5.6 leakage fixture: five filings, outcomes PENDING ----

LEAK = {
    "pre-open 07:00": filing("K1", at(date(2026, 11, 16), 7, 0)),
    "intraday 11:30": filing("K2", at(D17, 11, 30)),
    "after-close 16:30": filing("K3", at(date(2026, 11, 18), 16, 30)),
    "early close 12:58": filing("K4", at(date(2026, 11, 27), 12, 58)),
}
LEAK_DELAYED = filing("K5", at(date(2026, 11, 19), 11, 30))


def _delayed_tape() -> dict:
    tape = dict(TAPE)
    day0 = date(2026, 11, 19)
    d = at(day0, 20, 0) + timedelta(minutes=1)  # C0 becomes visible at D + 1 minute
    tape[day0] = S.Price(TAPE[day0].close, d, d)
    return tape


def _case(name):
    return (LEAK_DELAYED, _delayed_tape()) if name == "delayed C0" else (LEAK[name], TAPE)


ALL = [*LEAK, "delayed C0"]


def _mutate_after(tape: dict, day0: date, how: str) -> dict:
    if how == "absent":
        return {k: v for k, v in tape.items() if k <= day0}
    out = dict(tape)
    for k, v in tape.items():
        if k > day0:
            out[k] = {"x3": dataclasses.replace(v, close=v.close * 3), "zero": dataclasses.replace(v, close=0.0),
                      "nan": dataclasses.replace(v, close=float("nan")), "none": dataclasses.replace(v, close=None),
                      "late": S.Price(v.close, None, None)}[how]
    return out


def test_the_five_leakage_filings_land_where_the_contract_says():
    got = {n: only(run([_case(n)[0]], _case(n)[1])) for n in ALL}
    assert [got[n].status for n in ALL] == [S.SELECTED] * 4 + [S.EXCLUDED_LATE_INPUT]
    sel = {n: run([_case(n)[0]], _case(n)[1]).selected[0].day0 for n in ALL[:4]}
    assert sel == {"pre-open 07:00": date(2026, 11, 16), "intraday 11:30": D17,
                   "after-close 16:30": date(2026, 11, 19), "early close 12:58": date(2026, 11, 30)}


@pytest.mark.parametrize("how", ["x3", "zero", "nan", "none", "late", "absent"])
@pytest.mark.parametrize("name", ALL)
def test_mutating_every_price_after_o_changes_nothing(name, how):
    f, tape = _case(name)
    base = run([f], tape)
    day0 = S.day0_for(f.acceptance, CAL)
    mutated = run([f], _mutate_after(tape, day0, how))
    assert mutated == base  # direction, eligibility, strength, quintile and the log, all unchanged


@pytest.mark.parametrize("name", ALL[:4])
def test_mutating_c0_changes_direction_and_strength(name):
    f, tape = _case(name)
    base = run([f], tape).selected[0]
    day0 = base.day0
    up = dict(tape)
    up[day0] = dataclasses.replace(tape[day0], close=tape[day0].close * 1.2)
    dn = dict(tape)
    dn[day0] = dataclasses.replace(tape[day0], close=tape[day0].close * 0.8)
    a, b = run([f], up).selected[0], run([f], dn).selected[0]
    assert a.direction == 1 and b.direction == -1
    assert a.strength != base.strength and b.strength != base.strength and a.strength != b.strength


def test_mutating_p_prev_changes_the_strength():
    f, tape = _case("intraday 11:30")
    base = run([f], tape).selected[0]
    mut = dict(tape)
    mut[base.p_prev_date] = dataclasses.replace(tape[base.p_prev_date], close=tape[base.p_prev_date].close * 1.1)
    assert run([f], mut).selected[0].strength != base.strength


def test_the_tape_is_read_only_through_day0():
    class Spy(dict):
        def __init__(self, d):
            super().__init__(d)
            self.read: set = set()

        def get(self, k, default=None):
            self.read.add(k)
            return super().get(k, default)

        def __getitem__(self, k):
            self.read.add(k)
            return super().__getitem__(k)

        def __contains__(self, k):
            self.read.add(k)
            return super().__contains__(k)

    for name in ALL:
        f, tape = _case(name)
        spy = Spy(tape)
        run([f], spy)
        assert spy.read and max(spy.read) <= S.day0_for(f.acceptance, CAL), name
