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


@pytest.mark.parametrize("retrieved, visible", [(at(D17, 20, 0), at(D17, 20, 0, 1)),     # visible after D only
                                                 (at(D17, 20, 0, 1), at(D17, 20, 0, 1)),   # both after D
                                                 (at(D17, 20, 0, 1), at(D17, 21, 0))])
def test_c0_needs_both_clocks_at_or_before_the_deadline(retrieved, visible):
    tape = dict(TAPE)
    tape[D17] = S.Price(TAPE[D17].close, retrieved, visible)
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


# ---- chronology: acceptance <= retrieved <= visible <= D, and close <= retrieved <= visible <= D (Codex 6054842777) ----

def test_a_filing_visible_before_it_was_retrieved_and_retrieved_before_it_was_accepted_is_malformed():
    a = at(D17, 11, 0)
    d = refused(run([filing("C1", a, ret=at(D17, 10, 0), vis=at(D17, 9, 0))]), S.EXCLUDED_MALFORMED)
    assert "impossible chronology" in d.reason and d.status != S.EXCLUDED_LATE_INPUT


@pytest.mark.parametrize("ret, vis, needle", [(at(D17, 10, 59, 59), at(D17, 11, 30), "before the acceptance"),
                                              (at(D17, 11, 5), at(D17, 11, 4), "visible_at"),
                                              (at(D17, 10, 0), at(D17, 12, 0), "before the acceptance")])
def test_each_filing_chronology_violation_is_malformed_not_unknown_or_late(ret, vis, needle):
    d = refused(run([filing("C2", at(D17, 11, 0), ret=ret, vis=vis)]), S.EXCLUDED_MALFORMED)
    assert needle in d.reason


def test_equal_filing_clocks_are_chronological():
    a = at(D17, 11, 0)
    assert only(run([filing("C3", a, ret=a, vis=a)])).status == S.SELECTED


def test_a_filing_first_visible_after_the_deadline_is_late_even_if_its_record_is_inconsistent():
    """Read as of D, the filing was not yet visible: late. The inconsistency is logged in the reason, not promoted."""
    d = refused(run([filing("C4", at(D17, 11, 0), ret=at(D17, 21, 0), vis=at(D17, 20, 30))]), S.EXCLUDED_LATE_INPUT)
    assert "also inconsistent" in d.reason


def test_an_impossible_duplicate_that_claims_to_be_visible_first_refuses_the_accession():
    good = filing("C5", ON_TIME, sha="s")
    bogus_dup = dataclasses.replace(good, retrieved_at=at(D17, 12, 0), visible_at=at(D17, 8, 0))  # claims 08:00
    refused(run([good, bogus_dup]), S.EXCLUDED_MALFORMED)


def test_an_inconsistent_revision_claimed_visible_by_the_deadline_refuses_the_accession():
    good = filing("C5b", ON_TIME, sha="s")
    rev = dataclasses.replace(good, content_sha256="t", retrieved_at=at(D17, 19, 0), visible_at=at(D17, 18, 0))
    d = refused(run([good, rev]), S.EXCLUDED_MALFORMED)
    assert "claimed to be visible by the deadline" in d.reason


def test_c0_retrieved_before_its_session_closed_is_malformed():
    tape = dict(TAPE)
    tape[D17] = S.Price(TAPE[D17].close, at(D17, 10, 0), at(D17, 11, 0))  # a 16:00 close "seen" at 10:00 / 11:00
    d = refused(run([filing("C6", ON_TIME)], tape), S.EXCLUDED_MALFORMED)
    assert "before its session closed" in d.reason


@pytest.mark.parametrize("ret, vis", [(at(D17, 15, 59, 59), at(D17, 17, 0)), (at(D17, 10, 0), at(D17, 15, 0))])
def test_c0_retrieved_before_the_close_is_malformed_whatever_its_visibility(ret, vis):
    tape = dict(TAPE)
    tape[D17] = S.Price(TAPE[D17].close, ret, vis)
    refused(run([filing("C7", ON_TIME)], tape), S.EXCLUDED_MALFORMED)


def test_c0_visible_before_it_was_retrieved_is_malformed():
    tape = dict(TAPE)
    tape[D17] = S.Price(TAPE[D17].close, at(D17, 17, 0), at(D17, 16, 30))
    d = refused(run([filing("C8", ON_TIME)], tape), S.EXCLUDED_MALFORMED)
    assert "before it was retrieved" in d.reason


def test_c0_retrieved_exactly_at_the_close_is_chronological():
    tape = dict(TAPE)
    tape[D17] = S.Price(TAPE[D17].close, at(D17, 16, 0), at(D17, 16, 0))
    assert only(run([filing("C9", ON_TIME)], tape)).status == S.SELECTED


def test_every_close_used_must_be_chronological_not_only_c0():
    early = date(2026, 11, 16)
    tape = dict(TAPE)
    tape[early] = S.Price(TAPE[early].close, at(early, 9, 0), at(early, 9, 1))  # P-1 "seen" before it printed
    refused(run([filing("C10", ON_TIME)], tape), S.EXCLUDED_MALFORMED)
    old = S._sessions_ending(early, 61, CAL)[3]  # a vol-window close deep in the history
    tape2 = dict(TAPE)
    tape2[old] = S.Price(TAPE[old].close, at(old, 8, 0), at(old, 8, 1))
    refused(run([filing("C11", ON_TIME)], tape2), S.EXCLUDED_MALFORMED)


def test_malformed_unknown_and_late_are_three_different_refusals():
    tape_late = dict(TAPE)
    tape_late[D17] = S.Price(TAPE[D17].close, at(D17, 20, 0, 1), at(D17, 20, 0, 1))
    tape_unknown = dict(TAPE)
    tape_unknown[D17] = S.Price(TAPE[D17].close, None, None)
    tape_bad = dict(TAPE)
    tape_bad[D17] = S.Price(TAPE[D17].close, at(D17, 10, 0), at(D17, 11, 0))
    got = {only(run([filing(f"T{i}", ON_TIME)], t)).status for i, t in enumerate((tape_late, tape_unknown, tape_bad))}
    assert got == {S.EXCLUDED_LATE_INPUT, S.EXCLUDED_UNKNOWN_AVAILABILITY, S.EXCLUDED_MALFORMED}


def test_the_historical_proxy_stays_separate_from_the_forward_chronology_checks():
    tape = make_tape(date(2023, 6, 1), date(2024, 6, 1), clocks=False)
    res = run([filing("HC", at(date(2024, 3, 12), 11, 30))], tape, period=S.HISTORICAL)
    assert only(res).status == S.DIAGNOSTIC_ONLY and res.selected == ()


# ---- forward point-in-time stability: later records cannot rewrite a decision made at D (Codex 6055145346) ----

ORIGINAL = filing("PIT", ON_TIME, sha="orig")
BASE = run([ORIGINAL])
D_END = at(D17, 20, 0)
AFTER = [at(D17, 20, 0, 1), at(D17, 21, 0), at(D17, 22, 0), at(date(2026, 11, 18), 9, 0), at(date(2026, 12, 1), 12, 0)]
BEFORE_ACCEPTANCE = [at(D17, 8, 0), at(D17, 10, 59)]


def _later(retrieved, visible, sha="late", acceptance=ON_TIME):
    return S.Filing("PIT", ("2.02",), acceptance, retrieved, visible, sha)


def test_the_decision_at_d_is_the_same_with_any_record_first_visible_after_d_whatever_its_other_clocks():
    """Replay the same as-of with every combination of later additions: retrieved before or after visible,
    before the acceptance, the original's own content or a different one. The decision at D never moves."""
    assert only(BASE).status == S.SELECTED
    n = 0
    for vis in AFTER:
        for ret in [*BEFORE_ACCEPTANCE, at(D17, 19, 0), D_END, *AFTER]:  # includes ret < acceptance and ret > vis
            for sha in ("late", "orig"):
                res = run([ORIGINAL, _later(ret, vis, sha)])
                d = only(res)
                assert d.status == S.SELECTED, (ret, vis, sha, d)
                assert res.selected == BASE.selected, (ret, vis, sha)
                assert (d.acceptance, d.retrieved_at, d.visible_at, d.deadline_d) == (
                    only(BASE).acceptance, only(BASE).retrieved_at, only(BASE).visible_at, only(BASE).deadline_d)
                n += 1
    assert n == len(AFTER) * 9 * 2


def test_codexs_exact_control_a_revision_retrieved_22_00_and_visible_21_00_does_not_rewrite_the_original():
    res = run([ORIGINAL, _later(at(D17, 22, 0), at(D17, 21, 0))])
    d = only(res)
    assert d.status == S.SELECTED and res.selected == BASE.selected
    assert d.ignored_revisions == ("late",) and d.invalid_after_deadline == ("late",)  # logged separately


def test_a_later_record_with_a_placed_visibility_after_d_is_ignored_even_if_its_retrieval_is_unknown():
    d = only(run([ORIGINAL, _later(None, at(D17, 21, 0))]))
    assert d.status == S.SELECTED and d.ignored_revisions == ("late",)


def test_a_valid_later_revision_is_ignored_and_not_marked_invalid():
    d = only(run([ORIGINAL, _later(at(D17, 21, 0), at(D17, 21, 30))]))
    assert d.status == S.SELECTED and d.ignored_revisions == ("late",) and d.invalid_after_deadline == ()


def test_the_order_later_records_arrive_in_cannot_change_the_decision():
    adds = [_later(at(D17, 22, 0), at(D17, 21, 0), "x"), _later(at(D17, 21, 0), at(D17, 21, 30), "y")]
    for perm in ([ORIGINAL, *adds], [adds[0], ORIGINAL, adds[1]], [*adds, ORIGINAL]):
        res = run(perm)
        assert only(res).status == S.SELECTED and res.selected == BASE.selected


@pytest.mark.parametrize("late", [_later(None, None), _later(at(D17, 21, 0), None),
                                  _later(datetime(2026, 11, 17, 21, 0), datetime(2026, 11, 17, 21, 30))])
def test_a_later_record_whose_visibility_cannot_be_placed_stays_refused(late):
    """Unresolved records that could affect the by-D set are refused, not ignored: it might have been visible by D."""
    refused(run([ORIGINAL, late]), S.EXCLUDED_UNKNOWN_AVAILABILITY)


@pytest.mark.parametrize("ret, vis", [(at(D17, 19, 0), at(D17, 18, 0)),       # visible before retrieved, both by D
                                      (at(D17, 10, 0), at(D17, 12, 0)),        # retrieved before its own acceptance
                                      (at(D17, 20, 30), at(D17, 19, 59))])     # retrieved after D yet visible by D
def test_an_inconsistent_record_that_claims_visibility_by_d_stays_refused(ret, vis):
    refused(run([ORIGINAL, _later(ret, vis)]), S.EXCLUDED_MALFORMED)


def test_the_as_of_decision_is_the_same_whenever_it_is_replayed():
    adds = [_later(at(D17, 22, 0), at(D17, 21, 0))]
    a, b = run([ORIGINAL, *adds]), run([ORIGINAL, *adds, *adds])
    assert a.selected == b.selected == BASE.selected


# ---- order independence: the second order-dependence found (Codex 6056389309) ----

import itertools  # noqa: E402
import random  # noqa: E402
from datetime import timezone  # noqa: E402
from zoneinfo import ZoneInfo  # noqa: E402

VIS = at(D17, 11, 32)  # the original's visible clock (acceptance 11:30, retrieved 11:31)


def _tied(ret_a, ret_b, vis=VIS):
    a = S.Filing("TIE", ("2.02",), ON_TIME, ret_a, vis, "same")
    b = S.Filing("TIE", ("2.02",), ON_TIME, ret_b, vis, "same")
    return a, b


def test_two_observations_of_one_content_tied_on_visibility_but_conflicting_on_retrieval_fail_closed_in_both_orders():
    """Codex's control: same content hash, exactly the same visible clock, different retrieval clocks."""
    a, b = _tied(at(D17, 11, 31), at(D17, 11, 20))   # b was "retrieved" before the 11:30 acceptance: inconsistent
    one, two = run([a, b]), run([b, a])
    assert one == two
    d = only(one)
    assert d.status == S.EXCLUDED_MALFORMED and "tied" in d.reason and one.selected == ()


def test_the_tied_conflict_fails_closed_even_when_both_records_are_individually_consistent():
    a, b = _tied(at(D17, 11, 31), at(D17, 11, 32))   # both consistent; they still disagree on retrieval
    assert run([a, b]) == run([b, a]) and only(run([a, b])).status == S.EXCLUDED_MALFORMED


def test_a_tied_conflict_on_the_acceptance_or_the_items_fails_closed_in_both_orders():
    a = S.Filing("TIE", ("2.02",), ON_TIME, at(D17, 11, 31), VIS, "same")
    for b in (dataclasses.replace(a, acceptance=at(D17, 11, 29)), dataclasses.replace(a, items=("2.02", "9.01"))):
        assert run([a, b]) == run([b, a])
        assert only(run([a, b])).status == S.EXCLUDED_MALFORMED


def test_truly_identical_observations_are_still_one_event_in_both_orders():
    a = S.Filing("TIE", ("2.02",), ON_TIME, at(D17, 11, 31), VIS, "same")
    res = run([a, dataclasses.replace(a)])
    assert only(res).status == S.SELECTED and only(res).duplicates_dropped == 1
    assert res == run([dataclasses.replace(a), a])


def test_a_later_tied_conflict_does_not_rewrite_a_frozen_decision_and_is_logged():
    orig = filing("TIE", ON_TIME, sha="orig")
    base = run([orig])
    x, y = (S.Filing("TIE", ("2.02",), ON_TIME, r, at(D17, 21, 0), "late") for r in (at(D17, 21, 5), at(D17, 21, 1)))
    for order in ([orig, x, y], [y, x, orig], [x, orig, y]):
        res = run(order)
        d = only(res)
        assert d.status == S.SELECTED and res.selected == base.selected
        assert d.ignored_revisions == ("late",) and d.invalid_after_deadline == ("late",)


def test_a_tied_conflict_in_a_revision_claimed_visible_by_d_refuses_in_both_orders():
    orig = filing("TIE", ON_TIME, sha="orig")
    x, y = (S.Filing("TIE", ("2.02",), ON_TIME, r, at(D17, 18, 0), "rev") for r in (at(D17, 17, 59), at(D17, 17, 58)))
    assert run([orig, x, y]) == run([y, x, orig])
    assert only(run([orig, x, y])).status == S.EXCLUDED_MALFORMED


def test_two_different_contents_tied_at_the_earliest_visible_clock_decide_the_same_in_both_orders():
    a = S.Filing("TIE", ("2.02",), ON_TIME, at(D17, 11, 31), VIS, "one")
    b = S.Filing("TIE", ("2.02",), at(D17, 11, 29), at(D17, 11, 31), VIS, "two")
    assert run([a, b]) == run([b, a])
    assert only(run([a, b])).status == S.EXCLUDED_REVISED_INPUT


def test_the_non_filing_members_come_back_in_a_fixed_order():
    assert run([None, "x", 3]) == run([3, None, "x"]) == run(["x", 3, None])


# A generator of messy inputs: ties, inconsistent clocks, revisions, duplicates, junk, several accessions.
_TIMES = [None, at(D17, 10, 0), at(D17, 11, 0), at(D17, 11, 31), at(D17, 11, 32), at(D17, 19, 59), at(D17, 20, 0),
          at(D17, 20, 1), at(D17, 21, 0), at(D17, 22, 0), datetime(2026, 11, 17, 11, 32),
          datetime(2026, 11, 1, 1, 50, tzinfo=ET), datetime(2026, 11, 1, 1, 10, tzinfo=ET, fold=1),
          at(D17, 11, 32).astimezone(timezone(timedelta(hours=9)))]
_ACCEPT = [None, at(D17, 11, 30), at(D17, 11, 29), at(D17, 16, 30), datetime(2026, 11, 17, 11, 30),
           datetime(2026, 11, 1, 0, 30, tzinfo=ET), datetime(2026, 11, 1, 1, 50, tzinfo=ET)]


def _messy(rng: random.Random) -> list:
    out = []
    for acc in rng.sample(["Q1", "Q2", "Q3"], rng.randint(1, 3)):
        for _ in range(rng.randint(1, 4)):
            out.append(S.Filing(acc, rng.choice([("2.02",), ("1.01",), ("7.01",), ("2.02", "9.01")]),
                                rng.choice(_ACCEPT), rng.choice(_TIMES), rng.choice(_TIMES), rng.choice("abc")))
    if rng.random() < 0.4:  # a consistent original and a consistent second version, both visible by D
        for sha in rng.sample("abc", 2):
            out.append(S.Filing("Q9", ("2.02",), at(D17, 11, 30), at(D17, 11, 31), at(D17, 11, 32), sha))
    if rng.random() < 0.3:
        out += [None, "junk"]
    return out


def test_selection_is_invariant_under_any_ordering_of_a_messy_input():
    """Property test: 400 seeded messy inputs, each replayed under every permutation (up to 5 records) or 24
    shuffles. The whole result (selected, diagnostic and every decision, in order) must be identical."""
    rng = random.Random(20261125)
    checked = 0
    for _ in range(400):
        items = _messy(rng)
        base = run(items)
        perms = (itertools.permutations(items) if len(items) <= 5
                 else (rng.sample(items, len(items)) for _ in range(24)))
        for perm in perms:
            assert run(list(perm)) == base, (items, list(perm))
            checked += 1
    assert checked > 3000


def test_the_property_test_actually_reaches_every_outcome():
    """The generator is only useful if it exercises the refusals; pin that it does."""
    rng = random.Random(20261125)
    seen = set()
    for _ in range(400):
        for d in run(_messy(rng)).decisions:
            seen.add(d.status)
    assert {S.SELECTED, S.EXCLUDED_MALFORMED, S.EXCLUDED_LATE_INPUT, S.EXCLUDED_REVISED_INPUT,
            S.EXCLUDED_UNKNOWN_AVAILABILITY, S.EXCLUDED_NO_FAMILY} <= seen
    reasons = {d.reason for _ in range(1) for d in run(_messy(random.Random(1))).decisions}
    assert reasons


def test_a_consistent_later_tied_conflict_is_still_logged_as_invalid_and_never_rewrites_the_decision():
    orig = filing("TIE", ON_TIME, sha="orig")
    x, y = (S.Filing("TIE", ("2.02",), ON_TIME, r, at(D17, 21, 30), "late") for r in (at(D17, 21, 0), at(D17, 21, 10)))
    res = run([x, orig, y])
    d = only(res)
    assert d.status == S.SELECTED and res.selected == run([orig]).selected
    assert d.ignored_revisions == ("late",) and d.invalid_after_deadline == ("late",)


def test_non_filing_members_with_different_accessions_come_back_in_a_fixed_order_in_every_permutation():
    import types
    junk = [types.SimpleNamespace(accession="J2"), types.SimpleNamespace(accession="J1"), None, 3]
    base = run(junk)
    assert all(run(list(p)) == base for p in itertools.permutations(junk))
    assert [d.accession for d in base.decisions] == sorted([d.accession for d in base.decisions], key=repr)


# ---- clocks are UTC instants: the repeated hour of the autumn clock change (Codex 6061842262) ----

from datetime import timezone  # noqa: E402

UTC = timezone.utc
SUN = date(2026, 11, 1)  # US daylight saving time ends 02:00 EDT -> 01:00 EST; 01:00-02:00 ET happens twice
EDT_0150 = datetime(2026, 11, 1, 1, 50, tzinfo=ET)            # fold=0: 05:50Z
EST_0110 = datetime(2026, 11, 1, 1, 10, tzinfo=ET, fold=1)    # fold=1: 06:10Z, twenty minutes later
ACC_SUN = datetime(2026, 11, 1, 0, 30, tzinfo=ET)             # 04:30Z, before the repeated hour


def test_the_control_clocks_are_what_the_review_says():
    assert EDT_0150.astimezone(UTC) == datetime(2026, 11, 1, 5, 50, tzinfo=UTC)
    assert EST_0110.astimezone(UTC) == datetime(2026, 11, 1, 6, 10, tzinfo=UTC)
    assert EST_0110 < EDT_0150  # Python's own comparison of two datetimes sharing one ZoneInfo: by wall clock, wrong


def _obs(ret, vis, sha="fold", acc=ACC_SUN):
    return S.Filing("FOLD", ("2.02",), acc, ret, vis, sha)


def test_the_earlier_utc_instant_is_the_first_observation_across_the_fold_in_both_orders():
    first = _obs(EDT_0150, EDT_0150)     # 05:50Z
    later = _obs(EST_0110, EST_0110)     # 06:10Z, but its wall clock reads earlier
    one, two = run([first, later]), run([later, first])
    assert one == two
    d = only(one)
    # compared as UTC instants: a fold-ambiguous wall time never compares equal across different zone objects (PEP 495)
    assert d.status == S.SELECTED and d.retrieved_at.astimezone(UTC) == datetime(2026, 11, 1, 5, 50, tzinfo=UTC)
    assert d.visible_at.astimezone(UTC) == datetime(2026, 11, 1, 5, 50, tzinfo=UTC)
    assert d.duplicates_dropped == 1


def test_a_retrieval_twenty_minutes_after_the_acceptance_across_the_fold_is_chronological():
    """Accepted 01:50 EDT (05:50Z), retrieved 01:10 EST (06:10Z): later, though the wall clock reads earlier."""
    res = run([_obs(EST_0110, EST_0110, acc=EDT_0150)])
    assert only(res).status == S.SELECTED


def test_a_visibility_twenty_minutes_after_retrieval_across_the_fold_is_chronological():
    res = run([_obs(EDT_0150, EST_0110)])
    assert only(res).status == S.SELECTED
    bad = run([_obs(EST_0110, EDT_0150)])  # visible 20 minutes BEFORE it was retrieved
    assert only(bad).status == S.EXCLUDED_MALFORMED


def test_a_close_retrieved_across_the_fold_is_ordered_by_instant():
    tape = dict(TAPE)
    day0 = date(2026, 11, 2)  # the Monday after: day 0 of a Sunday-morning filing
    assert S.day0_for(ACC_SUN, CAL) == day0
    tape[day0] = S.Price(TAPE[D17].close, at(day0, 16, 1), at(day0, 16, 1))
    assert only(run([_obs(EDT_0150, EDT_0150)], tape)).status in (S.SELECTED, S.EXCLUDED_INPUT)


def test_offset_equivalent_copies_are_truly_identical_not_a_conflict():
    a = _obs(EDT_0150, EDT_0150)
    zones = [UTC, ZoneInfo("America/New_York"), ZoneInfo("Asia/Tokyo"), timezone(timedelta(hours=-5))]
    copies = [S.Filing("FOLD", ("2.02",), x.acceptance.astimezone(z), x.retrieved_at.astimezone(z),
                       x.visible_at.astimezone(z), "fold") for x, z in zip([a] * 4, zones)]
    res = run(copies)
    d = only(res)
    assert d.status == S.SELECTED and d.duplicates_dropped == 3
    for perm in itertools.permutations(copies):
        assert run(list(perm)) == res


def test_a_real_disagreement_between_offset_equivalent_looking_copies_is_still_a_conflict():
    a = _obs(EDT_0150, EDT_0150)
    b = _obs(EDT_0150 + timedelta(minutes=1), EDT_0150)  # same visible instant, retrieval one minute apart
    b = dataclasses.replace(b, retrieved_at=(EDT_0150 - timedelta(minutes=1)).astimezone(UTC))
    assert run([a, b]) == run([b, a])
    assert only(run([a, b])).status == S.EXCLUDED_MALFORMED


def test_decisions_and_events_are_the_same_whatever_zone_the_clocks_are_written_in():
    """Re-express every clock of a messy input in another zone: the result is equal (instants, not wall clocks)."""
    rng = random.Random(20261201)
    zones = [UTC, ZoneInfo("America/New_York"), ZoneInfo("Asia/Tokyo"), timezone(timedelta(hours=5, minutes=30))]

    def rezone(f, z):
        if not isinstance(f, S.Filing):
            return f
        t = lambda x: x.astimezone(z) if isinstance(x, datetime) and x.tzinfo is not None else x  # noqa: E731
        return dataclasses.replace(f, acceptance=t(f.acceptance), retrieved_at=t(f.retrieved_at), visible_at=t(f.visible_at))

    for _ in range(150):
        items = _messy(rng)
        base = run(items)
        for z in zones:
            assert run([rezone(f, z) for f in items]) == base, (z, items)


def test_the_property_generator_now_includes_fold_and_offset_variants():
    assert EDT_0150 in _TIMES and EST_0110 in _TIMES
    assert any(t is not None and t.tzinfo is not None and t.utcoffset() != ET.utcoffset(t.replace(tzinfo=None))
               for t in _TIMES[1:] if isinstance(t, datetime))


def test_a_price_seen_across_the_fold_is_ordered_by_instant():
    """P-1 is Friday's close, first seen on the Sunday of the clock change: retrieved 01:50 EDT, visible 01:10 EST."""
    p_prev = S.prev_close_date(ACC_SUN, CAL)
    assert p_prev == date(2026, 10, 30)
    tape = dict(TAPE)
    tape[p_prev] = S.Price(TAPE[p_prev].close, EDT_0150, EST_0110)          # visible twenty minutes after retrieval
    assert only(run([_obs(EDT_0150, EDT_0150)], tape)).status == S.SELECTED
    tape[p_prev] = S.Price(TAPE[p_prev].close, EST_0110, EDT_0150)          # visible twenty minutes before retrieval
    d = only(run([_obs(EDT_0150, EDT_0150)], tape))
    assert d.status == S.EXCLUDED_MALFORMED and "before it was retrieved" in d.reason


def test_the_ordering_key_and_the_text_form_work_on_raw_instants_not_wall_clocks():
    first, later = _obs(EDT_0150, EDT_0150), _obs(EST_0110, EST_0110)
    assert sorted([later, first], key=S._obs_key) == [first, later]
    assert sorted([first, later], key=S._obs_key) == [first, later]
    assert S._t(EDT_0150) == S._t(EDT_0150.astimezone(UTC)) == S._t(EDT_0150.astimezone(ZoneInfo("Asia/Tokyo")))
    assert S._t(EDT_0150) != S._t(EST_0110)
