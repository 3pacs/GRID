"""trade_edge v2: known_at, entry session, line rules, one position per event."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from paper_log.trade_edge import events as ev
from paper_log.trade_edge.config import EASTERN


def et(y, m, d, hh=0, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=EASTERN)


# ── known_at ────────────────────────────────────────────────────────────────


def test_known_at_uses_acceptance_when_later_than_ingest():
    k = ev.compute_known_at(date(2026, 10, 7), et(2026, 10, 7, 15, 0), et(2026, 10, 7, 9, 0))
    assert k == et(2026, 10, 7, 15, 0).astimezone(timezone.utc)


def test_known_at_uses_ingest_plus_margin_when_later():
    k = ev.compute_known_at(date(2026, 10, 7), et(2026, 10, 7, 7, 57), et(2026, 10, 7, 16, 5))
    assert k == et(2026, 10, 7, 16, 20).astimezone(timezone.utc)


def test_known_at_falls_back_to_filing_date_2200_et_without_acceptance():
    k = ev.compute_known_at(date(2026, 10, 7), None, et(2026, 10, 7, 12, 0))
    assert k == et(2026, 10, 7, 22, 0).astimezone(timezone.utc)
    # ingest after 22:00 wins
    k2 = ev.compute_known_at(date(2026, 10, 7), None, et(2026, 10, 8, 6, 20))
    assert k2 == et(2026, 10, 8, 6, 35).astimezone(timezone.utc)


def test_known_at_rejects_naive_timestamps():
    with pytest.raises(ValueError):
        ev.compute_known_at(date(2026, 10, 7), None, datetime(2026, 10, 7, 12, 0))


# ── entry session ───────────────────────────────────────────────────────────


def test_entry_same_day_when_known_before_close():
    assert ev.entry_session(et(2026, 10, 7, 10, 0)) == date(2026, 10, 7)


def test_entry_next_session_when_filed_after_close():
    assert ev.entry_session(et(2026, 10, 7, 16, 5)) == date(2026, 10, 8)
    assert ev.entry_session(et(2026, 10, 7, 22, 0)) == date(2026, 10, 8)


def test_entry_strictly_after_close_instant():
    assert ev.entry_session(et(2026, 10, 7, 16, 0)) == date(2026, 10, 8)
    assert ev.entry_session(et(2026, 10, 7, 15, 59)) == date(2026, 10, 7)


def test_entry_over_weekend():
    assert ev.entry_session(et(2026, 10, 9, 22, 0)) == date(2026, 10, 12)  # Fri night -> Mon
    assert ev.entry_session(et(2026, 10, 10, 11, 0)) == date(2026, 10, 12)  # Saturday


def test_entry_over_holidays():
    # Good Friday 2026-04-03 (NYSE closed, not a federal holiday)
    assert ev.entry_session(et(2026, 4, 2, 22, 0)) == date(2026, 4, 6)
    # Thanksgiving 2026-11-26 -> Friday 11-27 (13:00 early close)
    assert ev.entry_session(et(2026, 11, 25, 22, 0)) == date(2026, 11, 27)
    # known after the 13:00 early close on 11-27 -> Monday 11-30
    assert ev.entry_session(et(2026, 11, 27, 13, 30)) == date(2026, 11, 30)
    assert ev.entry_session(et(2026, 11, 27, 12, 30)) == date(2026, 11, 27)
    # Christmas 2026-12-25 (Friday) -> Monday 12-28
    assert ev.entry_session(et(2026, 12, 24, 22, 0)) == date(2026, 12, 28)


def test_shift_and_last_completed_session():
    assert ev.shift_sessions(date(2026, 10, 7), 0) == date(2026, 10, 7)
    assert ev.shift_sessions(date(2026, 10, 9), 1) == date(2026, 10, 12)
    assert ev.shift_sessions(date(2026, 11, 25), 1) == date(2026, 11, 27)
    assert ev.sessions_after(date(2026, 10, 7), date(2026, 10, 12)) == 3
    assert ev.last_completed_session(et(2026, 10, 7, 16, 29)) == date(2026, 10, 6)
    assert ev.last_completed_session(et(2026, 10, 7, 16, 30)) == date(2026, 10, 7)
    assert ev.last_completed_session(et(2026, 10, 11, 9, 0)) == date(2026, 10, 9)


def test_entry_matches_vs1_entry_positions_on_ordinary_sessions():
    pd = pytest.importorskip("pandas")
    vs1 = pytest.importorskip("analysis.panel_insider_density")
    sessions = [date(2026, 10, d) for d in (5, 6, 7, 8, 9, 12, 13)]
    knowns = [et(2026, 10, 5, 10), et(2026, 10, 5, 16), et(2026, 10, 6, 22), et(2026, 10, 9, 18)]
    frame = pd.DataFrame({
        "issuer_cik": [1, 1, 1, 2], "actor": [10, 11, 10, 20], "value": [1e6, 2e5, 3e5, 7e5],
        "known_at": pd.to_datetime([k.astimezone(timezone.utc) for k in knowns], utc=True),
    })
    got = vs1.entry_positions(frame, sessions)
    vs1_entries = sorted((int(r.issuer_cik), r.entry_close.tz_convert(EASTERN).date()) for r in got.itertuples())
    ours = sorted({(c, ev.entry_session(k)) for c, k in zip([1, 1, 1, 2], knowns)})
    assert vs1_entries == ours


# ── lines ───────────────────────────────────────────────────────────────────


def line(**kw):
    base = {"code": "P", "acq_disp": "A", "shares": 10_000.0, "price": 60.0, "trans_date": "2026-10-05",
            "is_derivative": False, "equity_swap": False, "is_10b5_1": False}
    base.update(kw)
    return base


@pytest.mark.parametrize("kw,stype,expected", [
    ({}, "4", None),
    ({}, "4/A", ev.EXCL_NOT_FORM_4),
    ({"code": "S"}, "4", ev.EXCL_NOT_PURCHASE),
    ({"acq_disp": "D"}, "4", ev.EXCL_NOT_ACQUIRED),
    ({"equity_swap": True}, "4", ev.EXCL_EQUITY_SWAP),
    ({"is_10b5_1": True}, "4", ev.EXCL_10B5_1),
    ({"trans_date": "2025-09-01"}, "4", ev.EXCL_LATE_FILING),
    ({"trans_date": "2026-10-09"}, "4", ev.EXCL_LATE_FILING),
    ({"shares": 50.0}, "4", ev.EXCL_SMALL),
    ({"shares": 100.0, "price": 50.0}, "4", ev.EXCL_SMALL),
    ({"price": 0.0}, "4", ev.EXCL_SMALL),
])
def test_line_exclusion(kw, stype, expected):
    assert ev.line_exclusion(line(**kw), date(2026, 10, 7), stype) == expected


def test_ticker_rules_and_buckets():
    assert ev.is_resolvable_ticker("BORR")
    assert ev.is_resolvable_ticker("BRK.B")
    assert not ev.is_resolvable_ticker("NONE")
    assert not ev.is_resolvable_ticker("")
    assert ev.yahoo_symbol("BRK.B") == "BRK-B"
    assert ev.cap_bucket(None) == "unknown"
    assert ev.cap_bucket(299e6) == "<300M"
    assert ev.cap_bucket(300e6) == "300M-2B"
    assert ev.cap_bucket(2e9) == ">=2B"


# ── one position per event ─────────────────────────────────────────────────


def filing(acc, cik, actor, known, lines, ticker="ABC", filed="2026-10-06"):
    return {"accession": acc, "issuer_cik": cik, "issuer_name": "Abc Corp", "ticker": ticker,
            "actor": actor, "owners": [{"cik": None, "name": actor}], "filing_date": filed,
            "acceptance_at": None, "first_ingest_at": None, "known_at": known.isoformat(),
            "source": "sec", "lines": lines}


def ok(trans="2026-10-05", shares=10_000.0, price=60.0):
    return {"trans_date": trans, "shares": shares, "price": price, "exclusion": None}


def test_one_position_per_issuer_and_entry_session():
    fs = [
        filing("a1", 1, "cik:10", et(2026, 10, 6, 22), [ok(), ok(shares=2_000.0, price=60.0)]),
        filing("a2", 1, "cik:11", et(2026, 10, 7, 9), [ok(shares=20_000.0, price=61.0)]),
        filing("a3", 1, "cik:10", et(2026, 10, 7, 22), [ok(trans="2026-10-07")]),
        filing("a4", 2, "cik:20", et(2026, 10, 7, 9), [ok(shares=1_000.0, price=20.0)], ticker="XYZ"),
    ]
    pos = ev.group_positions(ev.build_purchases(fs))
    assert set(pos) == {"cik:1|2026-10-07", "cik:1|2026-10-08", "cik:2|2026-10-07"}
    p = pos["cik:1|2026-10-07"]
    assert p["n_actors"] == 2 and p["n_purchases"] == 3
    assert p["total_value"] == pytest.approx(600_000 + 120_000 + 1_220_000)
    assert p["largest_value"] == pytest.approx(1_220_000)
    assert p["stratum"] == "large"
    assert p["accessions"] == ["a1", "a2"]
    assert pos["cik:2|2026-10-07"]["stratum"] == "small"


def test_joint_filers_reporting_one_purchase_collapse():
    fs = [
        filing("j1", 1, "cik:30", et(2026, 10, 6, 22), [ok()]),
        filing("j2", 1, "cik:31", et(2026, 10, 7, 8), [ok()]),
    ]
    purchases = ev.build_purchases(fs)
    assert len(purchases) == 1
    assert purchases[0]["actor"] == "cik:30" and purchases[0]["n_reports"] == 2
    pos = ev.group_positions(purchases)
    assert list(pos) == ["cik:1|2026-10-07"]
    assert pos["cik:1|2026-10-07"]["n_purchases"] == 1


def test_excluded_lines_are_not_purchases_and_grid_db_ticker_maps_to_cik():
    fs = [
        filing("e1", 1, "cik:10", et(2026, 10, 6, 22), [{**ok(), "exclusion": "rule_10b5_1"}]),
        filing("g1", None, "name:jane", et(2026, 10, 6, 22), [ok(shares=5_000.0)]),
        filing("s1", 1, "cik:10", et(2026, 10, 6, 23), [ok(shares=9_000.0)]),
    ]
    pos = ev.group_positions(ev.build_purchases(fs))
    assert list(pos) == ["cik:1|2026-10-07"]
    assert pos["cik:1|2026-10-07"]["accessions"] == ["g1", "s1"]


def test_unresolved_ticker_is_kept_and_flagged():
    fs = [filing("u1", 5, "cik:50", et(2026, 10, 6, 22), [ok()], ticker="NONE")]
    pos = ev.group_positions(ev.build_purchases(fs))
    assert pos["cik:5|2026-10-07"]["ticker_resolved"] is False


def test_close_instant_handles_early_close():
    assert ev.close_instant(date(2026, 11, 27)) == et(2026, 11, 27, 13, 0)
    assert ev.close_instant(date(2026, 10, 7)) == et(2026, 10, 7, 16, 0)
    assert ev.close_instant(date(2026, 10, 7)) - ev.close_instant(date(2026, 10, 6)) == timedelta(days=1)


# ── issuer identity across runs (review finding F2) ────────────────────────


def test_aliases_are_learned_once_and_a_persisted_alias_wins():
    fs = [
        filing("g1", None, "name:jane", et(2026, 10, 6, 22), [ok()]),
        filing("s1", 1, "cik:10", et(2026, 10, 7, 9), [ok(shares=9_000.0)]),
        filing("s2", 2, "cik:20", et(2026, 10, 8, 9), [ok(shares=8_000.0)]),  # same ticker, another CIK
    ]
    assert ev.learn_aliases(fs) == [{"ticker": "ABC", "cik": 1, "accession": "s1"}]
    assert ev.learn_aliases(fs, {"ABC": 1}) == []
    assert ev.issuer_keys(fs)["g1"] == "cik:1"
    assert ev.issuer_keys(fs, {"ABC": 9})["g1"] == "cik:9"  # the journal's association is append-only


def _moved_day_filings():
    return {
        "g1": filing("g1", None, "name:jane", et(2026, 10, 7, 22), [ok()]),                      # fallback, no CIK
        "s2": filing("s2", 1, "cik:10", et(2026, 10, 7, 14, 15), [ok()]),                       # same purchase, enriched
        "s3": filing("s3", 1, "cik:10", et(2026, 10, 8, 9), [ok(trans="2026-10-07", shares=5_000.0)]),  # distinct
    }


def _recorded(accessions, kind="signal", pid="ticker:ABC|2026-10-08", issuer_key="ticker:ABC"):
    return {pid: {"kind": kind, "position_id": pid, "issuer_key": issuer_key, "entry_session": "2026-10-08",
                  "entry_close_at": "2026-10-08T16:00:00-04:00", "accessions": list(accessions)}}


def _witness(recorded, members, late=()):
    """The proved recorded membership ``tracker.prove_membership`` derives from a journal's ordered prefixes."""
    return ev.Witness(recorded=dict(recorded), members={pid: list(keys) for pid, keys in members.items()},
                      late=list(late))


def _noted(note):
    return {"kind": ev.LATE_MEMBER_KIND, "run_at": "2026-10-09T08:30:00-04:00", **note}


PID = "ticker:ABC|2026-10-08"
Q_T = ("ticker:ABC", "2026-10-05", 10_000, 60.0)  # g1's purchase, recorded before ABC -> CIK 1 was known
Q_C = ("cik:1", "2026-10-05", 10_000, 60.0)  # the same purchase under its CIK (s2)
P3 = ("cik:1", "2026-10-07", 5_000, 60.0)  # s3's distinct purchase


def _one_slot_each(out, fs, aliases):
    by_ticker = ev._ticker_ciks(list(fs.values()), aliases)
    slots = [(ev.canonical_issuer(p["issuer_key"], by_ticker), p["entry_session"]) for p in out.values()]
    return len(slots) == len(set(slots))


def test_reconcile_keeps_the_recorded_identity_and_one_slot_before_entry():
    fs, aliases = _moved_day_filings(), {"ABC": 1}
    recorded = _witness(_recorded(["g1"]), {PID: [Q_T]})  # only the signal of the fallback report is recorded
    rebuilt = ev.group_positions(ev.build_purchases(fs.values(), aliases))
    # the enriched duplicate has the earlier known_at: the rebuilt position moved to 10-07, s3 stays on 10-08
    assert set(rebuilt) == {"cik:1|2026-10-07", "cik:1|2026-10-08"}
    out, applied, late, members = ev.reconcile_positions(rebuilt, fs, recorded, aliases)
    assert late == []  # nothing is late before entry
    # one position for issuer CIK 1 on the recorded session 10-08, carrying both purchases
    assert set(out) == {PID} and _one_slot_each(out, fs, aliases)
    kept = out[PID]
    assert kept["issuer_key"] == "ticker:ABC" and kept["issuer_alias"] == "cik:1"
    assert kept["entry_session"] == "2026-10-08" and kept["entry_close_at"] == "2026-10-08T16:00:00-04:00"
    assert kept["accessions"] == ["s2", "s3"] and kept["n_purchases"] == 2  # kept report + distinct purchase
    assert kept["total_value"] == pytest.approx(600_000 + 300_000)
    assert kept["largest_value"] == pytest.approx(600_000) and kept["stratum"] == "large"
    assert kept["actors"] == ["cik:10"] and kept["n_actors"] == 1
    assert members == {PID: [Q_C, P3]}  # the exact membership of the slot's next revision
    assert applied == [
        {"position_id": PID, "superseded_id": "cik:1|2026-10-07", "issuer_key": "cik:1",
         "entry_session": "2026-10-07"},
        {"position_id": PID, "superseded_id": "cik:1|2026-10-08", "issuer_key": "cik:1",
         "entry_session": "2026-10-08"},
    ]
    assert ev.reconcile_positions(rebuilt, fs, ev.Witness(), aliases)[:3] == (rebuilt, [], [])
    # a journal that recorded both identities of one purchase is refused, not merged
    both = _witness({**recorded.recorded, "cik:1|2026-10-07": {**rebuilt["cik:1|2026-10-07"]}},
                    {PID: [Q_T], "cik:1|2026-10-07": [Q_C]})
    with pytest.raises(ev.IdentityPolicyError, match=r"is consumed by the recorded positions"):
        ev.reconcile_positions(rebuilt, fs, both, aliases)


def test_reconcile_after_restart_keeps_the_folded_slot_without_refusal():
    fs, aliases = _moved_day_filings(), {"ABC": 1}
    rebuilt = ev.group_positions(ev.build_purchases(fs.values(), aliases))
    first, _, _, members = ev.reconcile_positions(rebuilt, fs, _witness(_recorded(["g1"]), {PID: [Q_T]}), aliases)
    # replay: the revised signal (and then the entry) recorded the folded membership
    for kind in ("signal", "entry"):
        replay = _witness(_recorded(["s2", "s3"], kind=kind), members)
        out, applied, late, again = ev.reconcile_positions(rebuilt, fs, replay, aliases)
        assert out == first and _one_slot_each(out, fs, aliases) and late == [] and again == members
        assert [a["superseded_id"] for a in applied] == ["cik:1|2026-10-07", "cik:1|2026-10-08"]


def test_reconcile_notes_a_moved_day_purchase_on_an_entered_slot_once():
    fs, aliases = _moved_day_filings(), {"ABC": 1}
    rebuilt = ev.group_positions(ev.build_purchases(fs.values(), aliases))
    entered = _witness(_recorded(["g1"], kind="entry"), {PID: [Q_T]})
    out, applied, late, members = ev.reconcile_positions(rebuilt, fs, entered, aliases)
    # no refusal and no second position: the distinct purchase s3 is a late member of the entered slot
    assert set(out) == {PID} and _one_slot_each(out, fs, aliases)
    assert late == [{"position_id": PID, "issuer_key": "cik:1", "entry_session": "2026-10-08",
                     "accession": "s3", "known_at": fs["s3"]["known_at"],
                     "purchase_keys": [["cik:1", "2026-10-07", 5000, 60.0]],
                     "reason": ev.LATE_MOVED_DAY, "rebuilt_id": "cik:1|2026-10-08"}]
    # the late purchase never joins the entered slot's membership
    assert out[PID]["accessions"] == ["s2"] and out[PID]["n_purchases"] == 1 and members == {PID: [Q_C]}
    assert entered.recorded == _recorded(["g1"], kind="entry") and entered.members == {PID: [Q_T]}  # untouched
    # restart: the journaled note binds s3's purchase to the slot, so it is never routed or noted again
    noted = _witness(entered.recorded, entered.members, [_noted(late[0])])
    out2, applied2, late2, members2 = ev.reconcile_positions(rebuilt, fs, noted, aliases)
    assert (out2, late2, members2) == (out, [], members) and applied2 == applied[:1]
    # only the consumed purchase arriving (no distinct group) reconciles under the entered id, nothing is late
    fs2 = {k: fs[k] for k in ("g1", "s2")}
    rebuilt2 = ev.group_positions(ev.build_purchases(fs2.values(), aliases))
    out, _, late, _ = ev.reconcile_positions(rebuilt2, fs2, entered, aliases)
    assert set(out) == {PID} and out[PID]["accessions"] == ["s2"]
    assert late == []


def test_reconcile_notes_a_same_group_purchase_on_an_entered_slot_like_a_moved_one():
    fs = {"g1": filing("g1", None, "name:jane", et(2026, 10, 7, 22), [ok()]),
          "g4": filing("g4", None, "name:joe", et(2026, 10, 7, 23), [ok(trans="2026-10-07", shares=5_000.0)])}
    rebuilt = ev.group_positions(ev.build_purchases(fs.values()))
    assert set(rebuilt) == {PID}  # the late purchase rebuilds into the entered group itself
    entered = _witness(_recorded(["g1"], kind="entry"), {PID: [Q_T]})
    out, applied, late, _ = ev.reconcile_positions(rebuilt, fs, entered, {})
    assert set(out) == {PID} and applied == []
    assert [(n["accession"], n["reason"], n["rebuilt_id"], n["issuer_key"], n["purchase_keys"]) for n in late] == [
        ("g4", ev.LATE_SAME_GROUP, PID, "ticker:ABC", [["ticker:ABC", "2026-10-07", 5000, 60.0]])]
    assert out[PID]["accessions"] == ["g1"] and "issuer_alias" not in out[PID]
    # an enriched re-report of g1 later teaches ABC -> CIK 1: the group is reached by purchase identity and
    # the noted purchase (now keyed cik:1) is matched canonically, so it is not noted twice
    fs["s2"] = filing("s2", 1, "cik:10", et(2026, 10, 7, 22, 30), [ok()])
    rebuilt2 = ev.group_positions(ev.build_purchases(fs.values(), {"ABC": 1}))
    assert set(rebuilt2) == {"cik:1|2026-10-08"}
    noted = _witness(entered.recorded, entered.members, [_noted(late[0])])
    out2, _, late2, _ = ev.reconcile_positions(rebuilt2, fs, noted, {"ABC": 1})
    assert set(out2) == {PID} and late2 == [] and out2[PID]["accessions"] == ["g1"]
    _, _, unnoted, _ = ev.reconcile_positions(rebuilt2, fs, entered, {"ABC": 1})
    assert [(n["accession"], n["reason"], n["purchase_keys"]) for n in unnoted] == [
        ("g4", ev.LATE_SAME_GROUP, [["cik:1", "2026-10-07", 5000, 60.0]])]


def test_reconcile_refuses_two_recorded_positions_in_one_issuer_session():
    fs, aliases = _moved_day_filings(), {"ABC": 1}
    rebuilt = ev.group_positions(ev.build_purchases(fs.values(), aliases))
    for kind in ("signal", "entry"):  # a genuine identity conflict is refused before and after entry
        two = _witness({**_recorded(["g1"], kind=kind),
                        **_recorded(["s3"], kind=kind, pid="cik:1|2026-10-08", issuer_key="cik:1")},
                       {PID: [Q_T], "cik:1|2026-10-08": [P3]})
        with pytest.raises(ev.IdentityPolicyError, match=r"one issuer/entry session cik:1\|2026-10-08"):
            ev.reconcile_positions(rebuilt, fs, two, aliases)


# ── review R3/R4: exact purchase membership and complete owner sets ────────


def _split_report_filings():
    """B reports Q (entry 10-07); A, a day later, reports Q again plus a distinct purchase P (entry 10-08)."""
    return {"B": filing("B", 1, "cik:10", et(2026, 10, 7, 9), [ok(trans="2026-10-06")]),
            "A": filing("A", 1, "cik:11", et(2026, 10, 8, 9),
                        [ok(trans="2026-10-06"), ok(trans="2026-10-07", shares=5_000.0)])}


Q = ("cik:1", "2026-10-06", 10_000, 60.0)
P = ("cik:1", "2026-10-07", 5_000, 60.0)


def test_reconcile_uses_exact_purchase_membership_for_a_multi_line_report():
    fs = _split_report_filings()
    rebuilt = ev.group_positions(ev.build_purchases(fs.values()))
    # earliest-report de-duplication keeps Q from B: A's own position holds P only, though A's lines carry Q
    assert len(fs["A"]["lines"]) == 2
    assert {pid: p["accessions"] for pid, p in rebuilt.items()} == {"cik:1|2026-10-07": ["B"],
                                                                     "cik:1|2026-10-08": ["A"]}
    out, applied, late, members = ev.reconcile_positions(rebuilt, fs, ev.Witness(), {})
    assert (out, applied, late) == (rebuilt, [], [])
    assert members == {"cik:1|2026-10-07": [Q], "cik:1|2026-10-08": [P]}  # Q is never assigned to A's position
    # the replay with both recorded (as signals, then as entries; either insertion order) is not a conflict
    for kind in ("signal", "entry"):
        for order in (list(rebuilt), list(rebuilt)[::-1]):
            replay = ev.Witness(recorded={pid: {**rebuilt[pid], "kind": kind} for pid in order},
                                members={pid: members[pid] for pid in order})
            assert ev.reconcile_positions(rebuilt, fs, replay, {}) == (rebuilt, [], [], members)


def _entry_rec(pid, session, accessions, kind="entry"):
    return {"kind": kind, "position_id": pid, "issuer_key": "cik:1", "entry_session": session,
            "entry_close_at": f"{session}T16:00:00-04:00", "accessions": accessions}


def test_reconcile_refuses_one_purchase_consumed_by_two_recorded_positions_whatever_the_order():
    q = ("cik:1", "2026-10-05", 10_000, 60.0)
    fs = {"C": filing("C", 1, "cik:12", et(2026, 10, 6, 9), [ok()]),  # the earliest report of q: entry 10-06
          "A": filing("A", 1, "cik:10", et(2026, 10, 7, 9), [ok()]),
          "B": filing("B", 1, "cik:11", et(2026, 10, 8, 9), [ok()])}
    rebuilt = ev.group_positions(ev.build_purchases(fs.values()))
    assert list(rebuilt) == ["cik:1|2026-10-06"]  # the current rebuilt id matches neither recorded owner
    r2, r3 = "cik:1|2026-10-07", "cik:1|2026-10-08"
    conflict = (r"purchase \['cik:1', '2026-10-05', 10000, 60\.0\] is consumed by the recorded positions "
                r"\['cik:1\|2026-10-07', 'cik:1\|2026-10-08'\]")
    for kind2 in ("signal", "entry"):
        recs = [_entry_rec(r2, "2026-10-07", ["A"], kind=kind2), _entry_rec(r3, "2026-10-08", ["B"])]
        for order in (recs, recs[::-1]):
            two = ev.Witness(recorded={r["position_id"]: r for r in order},
                             members={r["position_id"]: [q] for r in order})
            with pytest.raises(ev.IdentityPolicyError, match=conflict):
                ev.reconcile_positions(rebuilt, fs, two, {})
    # one proved owner is routed, not refused: its recorded id/session keep the purchase
    one = ev.Witness(recorded={r2: _entry_rec(r2, "2026-10-07", ["A"])}, members={r2: [q]})
    out, _, late, _ = ev.reconcile_positions(rebuilt, fs, one, {})
    assert set(out) == {r2} and out[r2]["entry_session"] == "2026-10-07" and late == []


def test_reconcile_refuses_a_purchase_with_two_bindings_and_a_lost_consumed_purchase():
    fs = _split_report_filings()
    rebuilt = ev.group_positions(ev.build_purchases(fs.values()))
    d7, d8 = "cik:1|2026-10-07", "cik:1|2026-10-08"
    recorded = {d7: _entry_rec(d7, "2026-10-07", ["B"]), d8: _entry_rec(d8, "2026-10-08", ["A"])}
    members = {d7: [Q], d8: [P]}
    other = ("cik:1", "2026-10-01", 1_000, 60.0)

    def note(pid, key):
        return {"kind": ev.LATE_MEMBER_KIND, "position_id": pid, "accession": "A", "purchase_keys": [list(key)]}

    cases = [
        ([note(d8, Q)], r"consumed by \['cik:1\|2026-10-07'\] and also noted late on 'cik:1\|2026-10-08'"),
        ([note(d7, other), note(d8, other)], r"noted late on two positions \['cik:1\|2026-10-07', 'cik:1\|2026-10-08'\]"),
        ([note(d8, other), note(d7, other)], r"noted late on two positions \['cik:1\|2026-10-07', 'cik:1\|2026-10-08'\]"),
    ]
    for late, match in cases:
        with pytest.raises(ev.IdentityPolicyError, match=match):
            ev.reconcile_positions(rebuilt, fs, ev.Witness(recorded, members, late), {})
    signalled = {**recorded, d8: _entry_rec(d8, "2026-10-08", ["A"], kind="signal")}
    with pytest.raises(ev.IdentityPolicyError, match=r"targets 'cik:1\|2026-10-08', which is not an entered position"):
        ev.reconcile_positions(rebuilt, fs, ev.Witness(signalled, members, [note(d8, other)]), {})
    with pytest.raises(ev.IdentityPolicyError, match=r"consumed purchase .* which these filings no longer rebuild"):
        ev.reconcile_positions(rebuilt, fs, ev.Witness(recorded, {**members, d7: [other]}), {})


def test_reconcile_keeps_a_noted_purchase_bound_when_an_earlier_duplicate_moves_its_day():
    fs = {"g1": filing("g1", None, "name:jane", et(2026, 10, 7, 22), [ok()]),
          "g4": filing("g4", None, "name:joe", et(2026, 10, 7, 23), [ok(trans="2026-10-07", shares=5_000.0)])}
    entered = _witness(_recorded(["g1"], kind="entry"), {PID: [Q_T]})
    _, _, (note,), _ = ev.reconcile_positions(ev.group_positions(ev.build_purchases(fs.values())), fs, entered, {})
    assert note["accession"] == "g4" and note["purchase_keys"] == [["ticker:ABC", "2026-10-07", 5000, 60.0]]
    noted = _witness(entered.recorded, entered.members, [_noted(note)])
    # an enriched duplicate of g4's purchase with an earlier known_at (s5: entry 10-07, teaches ABC -> CIK 1)
    # and a distinct new purchase on that session (r6)
    fs["s5"] = filing("s5", 1, "cik:13", et(2026, 10, 7, 9), [ok(trans="2026-10-07", shares=5_000.0)])
    fs["r6"] = filing("r6", 1, "cik:12", et(2026, 10, 7, 10), [ok(trans="2026-10-06", shares=7_000.0)])
    aliases = {"ABC": 1}
    rebuilt = ev.group_positions(ev.build_purchases(fs.values(), aliases))
    assert {pid: p["accessions"] for pid, p in rebuilt.items()} == {"cik:1|2026-10-07": ["r6", "s5"],
                                                                     "cik:1|2026-10-08": ["g1"]}
    out, _, late, members = ev.reconcile_positions(rebuilt, fs, noted, aliases)
    # the noted purchase stays bound to the entered slot: no second note, never scored in the moved group
    assert late == [] and set(out) == {PID, "cik:1|2026-10-07"}
    assert out[PID]["accessions"] == ["g1"] and members[PID] == [Q_C]
    moved = out["cik:1|2026-10-07"]
    assert moved["accessions"] == ["r6"] and moved["n_purchases"] == 1
    assert members["cik:1|2026-10-07"] == [("cik:1", "2026-10-06", 7_000, 60.0)]
    # without the binding the same purchase would be scored again in the new 10-07 position
    unbound, _, _, _ = ev.reconcile_positions(rebuilt, fs, entered, aliases)
    assert unbound["cik:1|2026-10-07"]["accessions"] == ["r6", "s5"]
