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


def _one_slot_each(out, fs, aliases):
    by_ticker = ev._ticker_ciks(list(fs.values()), aliases)
    slots = [(ev.canonical_issuer(p["issuer_key"], by_ticker), p["entry_session"]) for p in out.values()]
    return len(slots) == len(set(slots))


def test_reconcile_keeps_the_recorded_identity_and_one_slot_before_entry():
    fs, aliases = _moved_day_filings(), {"ABC": 1}
    recorded = _recorded(["g1"])  # only the signal of the fallback report is recorded
    rebuilt = ev.group_positions(ev.build_purchases(fs.values(), aliases))
    # the enriched duplicate has the earlier known_at: the rebuilt position moved to 10-07, s3 stays on 10-08
    assert set(rebuilt) == {"cik:1|2026-10-07", "cik:1|2026-10-08"}
    out, applied = ev.reconcile_positions(rebuilt, fs, recorded, aliases)
    # one position for issuer CIK 1 on the recorded session 10-08, carrying both purchases
    assert set(out) == {"ticker:ABC|2026-10-08"} and _one_slot_each(out, fs, aliases)
    kept = out["ticker:ABC|2026-10-08"]
    assert kept["issuer_key"] == "ticker:ABC" and kept["issuer_alias"] == "cik:1"
    assert kept["entry_session"] == "2026-10-08" and kept["entry_close_at"] == "2026-10-08T16:00:00-04:00"
    assert kept["accessions"] == ["s2", "s3"] and kept["n_purchases"] == 2  # kept report + distinct purchase
    assert kept["total_value"] == pytest.approx(600_000 + 300_000)
    assert kept["largest_value"] == pytest.approx(600_000) and kept["stratum"] == "large"
    assert kept["actors"] == ["cik:10"] and kept["n_actors"] == 1
    assert applied == [
        {"position_id": "ticker:ABC|2026-10-08", "superseded_id": "cik:1|2026-10-07", "issuer_key": "cik:1",
         "entry_session": "2026-10-07"},
        {"position_id": "ticker:ABC|2026-10-08", "superseded_id": "cik:1|2026-10-08", "issuer_key": "cik:1",
         "entry_session": "2026-10-08"},
    ]
    assert ev.reconcile_positions(rebuilt, fs, {}, aliases) == (rebuilt, [])
    # a journal that recorded both identities of one purchase is refused, not merged
    both = {**recorded, "cik:1|2026-10-07": {**rebuilt["cik:1|2026-10-07"]}}
    with pytest.raises(ev.IdentityPolicyError):
        ev.reconcile_positions(rebuilt, fs, both, aliases)


def test_reconcile_after_restart_keeps_the_folded_slot_without_refusal():
    fs, aliases = _moved_day_filings(), {"ABC": 1}
    rebuilt = ev.group_positions(ev.build_purchases(fs.values(), aliases))
    first, _ = ev.reconcile_positions(rebuilt, fs, _recorded(["g1"]), aliases)
    # replay: the revised signal (and then the entry) recorded the folded accessions
    for kind in ("signal", "entry"):
        out, applied = ev.reconcile_positions(rebuilt, fs, _recorded(["s2", "s3"], kind=kind), aliases)
        assert out == first and _one_slot_each(out, fs, aliases)
        assert [a["superseded_id"] for a in applied] == ["cik:1|2026-10-07", "cik:1|2026-10-08"]


def test_reconcile_refuses_a_distinct_group_joining_an_entered_slot():
    fs, aliases = _moved_day_filings(), {"ABC": 1}
    rebuilt = ev.group_positions(ev.build_purchases(fs.values(), aliases))
    with pytest.raises(ev.IdentityPolicyError, match=r"cik:1\|2026-10-08.*already entered as 'ticker:ABC\|2026-10-08'"):
        ev.reconcile_positions(rebuilt, fs, _recorded(["g1"], kind="entry"), aliases)
    # only the consumed purchase arriving (no distinct group) still reconciles under the entered id
    fs2 = {k: fs[k] for k in ("g1", "s2")}
    rebuilt2 = ev.group_positions(ev.build_purchases(fs2.values(), aliases))
    out, _ = ev.reconcile_positions(rebuilt2, fs2, _recorded(["g1"], kind="entry"), aliases)
    assert set(out) == {"ticker:ABC|2026-10-08"} and out["ticker:ABC|2026-10-08"]["accessions"] == ["s2"]


def test_reconcile_refuses_two_recorded_positions_in_one_issuer_session():
    fs, aliases = _moved_day_filings(), {"ABC": 1}
    rebuilt = ev.group_positions(ev.build_purchases(fs.values(), aliases))
    two = {**_recorded(["g1"]), **_recorded(["s3"], pid="cik:1|2026-10-08", issuer_key="cik:1")}
    with pytest.raises(ev.IdentityPolicyError, match=r"one issuer/entry session cik:1\|2026-10-08"):
        ev.reconcile_positions(rebuilt, fs, two, aliases)
