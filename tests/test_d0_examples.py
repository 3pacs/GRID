"""Executable §5.6 / §6.7 receipts: positive and refusal, each verified by re-derivation. Synthetic records only."""

from __future__ import annotations

import ast
import copy
from datetime import date
import json
from pathlib import Path

import pytest

from analysis import d0_examples as X
from analysis import d1_selection as S
from tests.test_d1_selection_fixture import BP, D17, ON_TIME, TAPE, at, filing, run

# ---- synthetic records ------------------------------------------------------------------------

STUB = {"ticker": "ZZZ", "earnings_date": "2026-09-30", "reported": True}
PERIOD_ENDS = ["2026-06-30", "2026-09-30"]

DETAIL = {"legs": {"sc": "C6000", "sp": "P6000", "lc": "C6005", "lp": "P5995"},
          "in": {"sc": [1.20, 1.25], "sp": [1.10, 1.15], "lc": [0.30, 0.35], "lp": [0.25, 0.30]},
          "x1_t": "2026-11-17T15:50:00-05:00",
          "x1_out": {"sc": [0.30, 0.35], "sp": [0.15, 0.20], "lc": [0.03, 0.05], "lp": [0.02, 0.04]},
          "x2_t": "2026-11-17T15:55:00-05:00",
          "x2_out": {"sc": [0.20, 0.25], "sp": [0.10, 0.15], "lc": [0.02, 0.04], "lp": [0.01, 0.03]},
          "settle_src": "spot_bars"}
# credit = 1.20 + 1.10 - 0.35 - 0.30 = 1.65; cost = 0.25 + 0.15 - 0.02 - 0.01 = 0.37; net = 1.65 - 0.37 - 8 * 0.0003
CREDIT, NET = 1.65, 1.2776


def fly_row(**over):
    row = {"day": "2026-11-17", "version": "fly-shadow/1", "status": "traded", "reason": None,
           "graded_at": "2026-11-17T21:30:01+00:00", "entry_t": "2026-11-17T15:00:00-05:00", "spot_in": 6000.2,
           "k": 6000.0, "credit": CREDIT, "x1": 1.3, "x2": NET, "x3": 1.1, "settle": 6001.0,
           "detail": json.dumps(DETAIL)}
    row.update(over)
    return row


def skipped(reason="missing_leg:lc", status="skipped", day="2026-11-17"):
    return {"day": day, "version": "fly-shadow/1", "status": status, "reason": reason, "graded_at":
            "2026-11-17T21:30:01+00:00", "entry_t": None, "spot_in": None, "k": None, "credit": None, "x1": None,
            "x2": None, "x3": None, "settle": None, "detail": None}


def reseal(rec):
    return X.seal({k: v for k, v in rec.items() if k != "receipt_sha256"})


def d1_receipt(**kw):
    f = filing("0000000000-26-000001", ON_TIME, items=("2.02", "9.01"))
    res = run([f])
    kw = kw or {"materialization": {"table": "event_materialization", "row_id": 17, "row_sha256": "ab" * 32}}
    return f, res, X.d1_traceable_receipt(f, TAPE, breakpoints=BP, **kw)


# ---- D1 refusal (§5.6) ------------------------------------------------------------------------

def test_the_calendar_stub_row_is_refused_as_unknown_availability_and_verifies():
    rec = X.calendar_stub_refusal(STUB, PERIOD_ENDS)
    assert rec["kind"] == "D1/REFUSAL" and rec["refused_as"] == "EXCLUDED_UNKNOWN_AVAILABILITY"
    assert rec["rows_changed"] == 0 and "does not prove every" in rec["note"]
    assert X.verify_receipt(rec) == []


@pytest.mark.parametrize("row", [dict(STUB, reported=False), dict(STUB, reported="TRUE"), dict(STUB, reported=1),
                                 dict(STUB, earnings_date="2026-10-02"), dict(STUB, earnings_date=None), None, "row"])
def test_a_row_that_is_not_a_compat_stub_makes_no_refusal_receipt(row):
    with pytest.raises(X.ReceiptError):
        X.calendar_stub_refusal(row, PERIOD_ENDS)


def test_a_refusal_receipt_cannot_be_relabelled_or_made_to_change_rows():
    rec = X.calendar_stub_refusal(STUB, PERIOD_ENDS)
    assert X.verify_receipt(reseal({**rec, "refused_as": "SELECTED"}))
    assert X.verify_receipt(reseal({**rec, "rows_changed": 3}))
    assert X.verify_receipt(reseal({**rec, "matched_sec_period_end": "2026-06-30"}))
    assert X.verify_receipt({**rec, "refused_as": "SELECTED"})  # unsealed edit: the hash catches it


# ---- D1 traceable (§5.6) ----------------------------------------------------------------------

def test_the_traceable_item_2_02_card_has_every_outcome_pending_and_verifies():
    f, res, rec = d1_receipt()
    ev = res.selected[0]
    assert rec["kind"] == "D1/TRACEABLE" and X.verify_receipt(rec) == []
    assert rec["chain"]["input"]["accession"] == f.accession and rec["chain"]["input"]["filing_date"] == "2026-11-17"
    assert rec["clocks"] == {"day0": "2026-11-17", "p_prev_date": "2026-11-16",
                             "cutoff_o": ev.cutoff_o.isoformat(), "deadline_d": ev.deadline_d.isoformat(),
                             "entry_day": "2026-11-18"}
    card = rec["card"]
    assert card["already_moved"] == TAPE[D17].close / TAPE[date(2026, 11, 16)].close - 1.0 and card["claimed"] is False
    assert (card["direction"], card["strength"], card["quintile"]) == (ev.direction, ev.strength, ev.quintile)
    assert set(card["outcome"].values()) == {"PENDING"} and card["entry_open"]["status"] == "PENDING"
    assert rec["note"] == "M0 does not prove accuracy" and rec["family"] == "D1-F1"
    assert rec["not_verified"] == ["materialization_row", "unavailable_reason"]  # the narrowing is stated
    assert sorted(rec["evidence"]["prices"]) == sorted(d.isoformat() for d in S.used_price_dates(ev))
    assert len(rec["evidence"]["prices"]) == 62


def test_the_traceable_receipt_binds_the_chain_shape_with_a_row_or_a_reason():
    _, _, with_reason = d1_receipt(unavailable_reason="no reasoning output yet (M0)")
    assert with_reason["chain"]["materialization_row"] is None and X.verify_receipt(with_reason) == []
    f = filing("0000000000-26-000002", ON_TIME)
    for kw in ({}, {"materialization": {"a": 1}, "unavailable_reason": "x"}):
        with pytest.raises(X.ReceiptError):
            X.d1_traceable_receipt(f, TAPE, breakpoints=BP, **kw)


def test_only_a_selected_item_2_02_filing_has_a_traceable_card():
    late = filing("L", ON_TIME, ret=ON_TIME.replace(hour=21), vis=ON_TIME.replace(hour=21))
    with pytest.raises(X.ReceiptError):
        X.d1_traceable_receipt(late, TAPE, breakpoints=BP, unavailable_reason="x")
    impossible = filing("L2", at(D17, 11, 0), ret=at(D17, 10, 0), vis=at(D17, 9, 0))  # Codex's chronology control
    with pytest.raises(X.ReceiptError):
        X.d1_traceable_receipt(impossible, TAPE, breakpoints=BP, unavailable_reason="x")
    f2 = filing("F2", ON_TIME, items=("1.01",))
    with pytest.raises(X.ReceiptError):
        X.d1_traceable_receipt(f2, TAPE, breakpoints=BP, unavailable_reason="x")


def _edit(rec, *path, value):
    m = copy.deepcopy(rec)
    t = m
    for k in path[:-1]:
        t = t[k]
    t[path[-1]] = value
    return reseal(m)


def test_every_derived_card_and_clock_field_is_rebound_to_the_evidence():
    _, _, rec = d1_receipt()
    for path, value in [(("card", "outcome", "h5"), 0.031), (("card", "entry_open", "status"), "FILLED"),
                        (("card", "claimed"), True), (("card", "strength"), 9.9), (("card", "quintile"), 5),
                        (("card", "direction"), -1), (("card", "already_moved"), 0.5),
                        (("clocks", "day0"), "2026-11-19"), (("clocks", "p_prev_date"), "2026-11-13"),
                        (("clocks", "entry_day"), "2026-11-20"), (("clocks", "cutoff_o"), "2026-11-17T10:00:00-05:00"),
                        (("clocks", "deadline_d"), "2026-11-17T21:00:00-05:00"), (("family",), "D1-F2"),
                        (("chain", "input", "filing_date"), "2026-11-18"), (("chain", "input", "accession"), "other"),
                        (("chain", "retained_bytes_sha256"), "0" * 64)]:
        assert X.verify_receipt(_edit(rec, *path, value=value)), path
    assert X.verify_receipt({**rec, "unexpected": 1})


def test_the_cutoff_o_is_the_actual_close_of_day_zero_including_an_early_close():
    f = filing("0000000000-26-000003", at(date(2026, 11, 27), 9, 30))  # a 13:00 early-close session
    rec = X.d1_traceable_receipt(f, TAPE, breakpoints=BP, unavailable_reason="x")
    assert rec["clocks"]["cutoff_o"] == at(date(2026, 11, 27), 13, 0).isoformat() and X.verify_receipt(rec) == []
    assert X.verify_receipt(_edit(rec, "clocks", "cutoff_o", value=at(date(2026, 11, 27), 16, 0).isoformat()))


def test_the_receipt_inherits_the_chronology_rules_of_the_selection_it_embeds():
    _, _, rec = d1_receipt()
    prices = rec["evidence"]["prices"]
    c0 = "2026-11-17"
    for retrieved, visible in [("2026-11-17T10:00:00-05:00", "2026-11-17T11:00:00-05:00"),   # before the close
                               ("2026-11-17T17:00:00-05:00", "2026-11-17T16:30:00-05:00"),   # visible before retrieved
                               ("2026-11-17T20:00:01-05:00", "2026-11-17T20:00:01-05:00")]:  # after the deadline
        bad = _edit(rec, "evidence", "prices", c0, value=[prices[c0][0], retrieved, visible])
        errs = X.verify_receipt(bad)
        assert errs and any("EXCLUDED_" in e for e in errs), (retrieved, visible, errs)
    f = copy.deepcopy(rec["evidence"]["filing"])
    f["retrieved_at"], f["visible_at"] = "2026-11-17T10:00:00-05:00", "2026-11-17T09:00:00-05:00"
    assert any("EXCLUDED_MALFORMED" in e for e in X.verify_receipt(_edit(rec, "evidence", "filing", value=f)))


def test_the_embedded_evidence_cannot_be_altered_without_failing_verification():
    _, _, rec = d1_receipt()
    ev = rec["evidence"]
    for path, value in [(("filing", "acceptance"), at(D17, 17, 0).isoformat()), (("filing", "items"), ["7.01"]),
                        (("filing", "items"), ["1.01"]), (("filing", "accession"), "other"),
                        (("filing", "content_sha256"), "zz"), (("breakpoints",), [1, 2, 3, 4]), (("breakpoints",), None),
                        (("prices", "2026-11-17", 0), TAPE[D17].close * 1.5),
                        (("prices", "2026-11-16", 0), TAPE[date(2026, 11, 16)].close * 1.1)]:
        assert X.verify_receipt(_edit(rec, "evidence", *path, value=value)), path
    extra = copy.deepcopy(ev)
    extra["prices"]["2026-11-18"] = [100.0, None, None]  # a close after day 0: not what the event used
    assert any("exactly the closes" in e for e in X.verify_receipt(_edit(rec, "evidence", value=extra)))
    short = copy.deepcopy(ev)
    del short["prices"]["2026-09-01"]
    assert X.verify_receipt(_edit(rec, "evidence", value=short))
    naive = copy.deepcopy(ev)
    naive["filing"]["acceptance"] = "2026-11-17T11:30:00"
    assert X.verify_receipt(_edit(rec, "evidence", value=naive))


def test_the_carried_not_verified_fields_are_named_and_cannot_be_dropped():
    _, _, rec = d1_receipt()
    assert X.verify_receipt(_edit(rec, "not_verified", value=[]))


# ---- D2 traceable (§6.7) ----------------------------------------------------------------------

def test_the_traceable_fly_session_is_rebuilt_from_its_stored_quotes():
    cap = b"synthetic capture bytes"
    rec = X.fly_traceable_receipt(fly_row(), capture_bytes=cap)
    assert rec["kind"] == "D2/TRACEABLE" and X.verify_receipt(rec) == []
    assert rec["credit"] == CREDIT and rec["net"] == NET
    assert rec["capture"]["id"] == X.sha256_hex(cap)
    assert rec["fills"]["label"] == "MODELED_BBO" and rec["fills"]["leg_sides"] == 8
    q = rec["quotes"]
    assert (q["entry"]["target"], q["exit"]["target"]) == ("15:00", "15:55")
    assert (q["entry"]["line_offset_seconds"], q["exit"]["line_offset_seconds"]) == (0, 0)
    assert "NOT_STORED" in q["leg_quote_time"]
    assert rec["ledger_row_sha256"] == X.fly_row_sha256(fly_row()) and rec["regime"] == "JUDGED_SAMPLE"


def test_a_missing_capture_file_says_not_stored_and_does_not_invent_an_id():
    rec = X.fly_traceable_receipt(fly_row())
    assert rec["capture"]["id"] == "NOT_STORED" and X.verify_receipt(rec) == []


def test_a_pre_registration_session_is_labelled_outside_the_judged_sample():
    rec = X.fly_traceable_receipt(fly_row(day="2026-11-09", entry_t="2026-11-09T15:00:00-05:00",
                                          detail=json.dumps(DETAIL).replace("2026-11-17", "2026-11-09")))
    assert rec["regime"].startswith("PRE_REGISTRATION") and X.verify_receipt(rec) == []


def test_the_line_offset_is_the_seconds_between_the_target_and_the_minute_line_at_the_window_edges():
    edge = dict(DETAIL, x2_t="2026-11-17T15:52:00-05:00")
    rec = X.fly_traceable_receipt(fly_row(entry_t="2026-11-17T15:02:00-05:00", detail=json.dumps(edge)))
    assert rec["quotes"]["entry"]["line_offset_seconds"] == 120 and rec["quotes"]["exit"]["line_offset_seconds"] == -180
    assert X.verify_receipt(rec) == []


@pytest.mark.parametrize("entry_t, x2_t", [("2026-11-17T14:59:00-05:00", "15:55"),    # entry before the target
                                           ("2026-11-17T15:03:00-05:00", "15:55"),    # entry more than 2 minutes after
                                           ("2026-11-17T15:00:00-05:00", "15:57"),    # exit after the target
                                           ("2026-11-17T15:00:00-05:00", "15:51"),    # exit more than 3 minutes before
                                           ("2026-11-17T15:00:00-05:00", "10:00"),    # Codex's control: exit at 10:00 ET
                                           ("2026-11-17T15:00:00", "15:55"),          # no time zone
                                           ("2026-11-18T15:00:00-05:00", "15:55")])   # another day
def test_a_line_outside_its_permitted_target_window_or_day_makes_no_receipt(entry_t, x2_t):
    x2 = x2_t if len(x2_t) > 5 else f"2026-11-17T{x2_t}:00-05:00"
    with pytest.raises(X.ReceiptError):
        X.fly_traceable_receipt(fly_row(entry_t=entry_t, detail=json.dumps(dict(DETAIL, x2_t=x2))))


def test_the_exit_line_time_cannot_be_moved_and_resealed_codexs_control():
    rec = X.fly_traceable_receipt(fly_row())
    moved = _edit(rec, "quotes", "exit", "line_time", value="2026-11-17T10:00:00-05:00")
    assert X.verify_receipt(moved)
    both = copy.deepcopy(rec)
    both["quotes"]["exit"]["line_time"] = "2026-11-17T10:00:00-05:00"
    both["quotes"]["exit"]["line_offset_seconds"] = -20100
    assert X.verify_receipt(reseal(both))
    # moving it in the ledger row too, so the receipt agrees with its row, is refused by the window itself
    row = copy.deepcopy(rec["ledger_row"])
    row["detail"] = json.dumps(dict(DETAIL, x2_t="2026-11-17T10:00:00-05:00"))
    forged = copy.deepcopy(rec)
    forged["ledger_row"] = row
    forged["quotes"]["exit"]["line_time"] = "2026-11-17T10:00:00-05:00"
    assert any("allowed" in e for e in X.verify_receipt(reseal(forged)))


def test_leg_identity_session_and_strike_are_bound_to_the_ledger_row():
    with pytest.raises(X.ReceiptError):
        X.fly_traceable_receipt(fly_row(k=6005.0))                       # legs say 6000, the row says 6005
    d = copy.deepcopy(DETAIL)
    d["legs"]["lc"] = "C6010"
    with pytest.raises(X.ReceiptError):
        X.fly_traceable_receipt(fly_row(detail=json.dumps(d)))
    with pytest.raises(X.ReceiptError):
        X.fly_traceable_receipt(fly_row(day="2026-11-16"))              # the lines are on 11-17
    rec = X.fly_traceable_receipt(fly_row())
    for path, value in [(("quotes", "entry", "leg_ids", "sc"), "C6005"), (("quotes", "exit", "leg_ids"), {}),
                        (("session",), "2026-11-16"), (("quotes", "entry", "legs", "sp"), [1.1, 1.2]),
                        (("quotes", "exit", "legs", "lc"), [0.0, 0.0]), (("quotes", "entry", "line_time"),
                                                                       "2026-11-17T15:01:00-05:00"),
                        (("quotes", "entry", "line_offset_seconds"), 0 + 60), (("credit",), 2.0), (("net",), 2.0),
                        (("fills", "leg_sides"), 4), (("fills", "fee_per_leg_side"), 0.0),
                        (("quotes", "leg_quote_time"), "15:00:00")]:
        assert X.verify_receipt(_edit(rec, *path, value=value)), path
    assert X.verify_receipt({**rec, "unexpected": 1})


def test_the_capture_id_is_a_sha256_or_not_stored_and_nothing_else():
    rec = X.fly_traceable_receipt(fly_row(), capture_bytes=b"x")
    assert X.verify_receipt(_edit(rec, "capture", "id", value="not a hash"))
    assert X.verify_receipt(_edit(rec, "capture", "id", value="A" * 64))
    assert X.verify_receipt(_edit(rec, "capture", value={"id": "NOT_STORED"}))


@pytest.mark.parametrize("over", [{"x2": NET + 0.01}, {"credit": CREDIT + 0.01}, {"status": "ungraded"},
                                  {"version": "fly-shadow/2"}, {"detail": "not json"}, {"detail": None},
                                  {"x2": None}])
def test_a_ledger_row_that_does_not_follow_from_its_quotes_makes_no_receipt(over):
    with pytest.raises(X.ReceiptError):
        X.fly_traceable_receipt(fly_row(**over))


@pytest.mark.parametrize("drop", ["sc", "sp", "lc", "lp"])
def test_a_missing_exit_leg_makes_no_traceable_receipt(drop):
    d = copy.deepcopy(DETAIL)
    del d["x2_out"][drop]
    with pytest.raises(X.ReceiptError):
        X.fly_traceable_receipt(fly_row(detail=json.dumps(d)))


def test_a_fly_receipt_fails_if_the_net_the_label_or_the_row_is_altered():
    rec = X.fly_traceable_receipt(fly_row())
    net = copy.deepcopy(rec)
    net["net"] = 2.0
    assert any("net" in e for e in X.verify_receipt(reseal(net)))
    quote = copy.deepcopy(rec)
    quote["quotes"]["exit"]["legs"]["sc"] = [0.2, 0.9]
    assert any("quotes" in e for e in X.verify_receipt(reseal(quote)))
    label = copy.deepcopy(rec)
    label["fills"]["label"] = "FILLED"
    assert any("fills" in e for e in X.verify_receipt(reseal(label)))
    row = copy.deepcopy(rec)
    row["ledger_row"]["spot_in"] = 1.0
    assert any("ledger_row_sha256" in e or "ledger_row" in e for e in X.verify_receipt(reseal(row)))
    regime = copy.deepcopy(rec)
    regime["regime"] = "PRE_REGISTRATION (outside the judged sample)"
    assert any("regime" in e for e in X.verify_receipt(reseal(regime)))


# ---- D2 refusal (§6.7) ------------------------------------------------------------------------

@pytest.mark.parametrize("status, reason", [("skipped", "missing_leg:lc"), ("skipped", "no_entry_line"),
                                            ("skipped", "nonpositive_quote"), ("skipped", "no_quotes"),
                                            ("ungraded", "x2_no_line"), ("ungraded", "x1_no_line;x2_no_line")])
def test_a_session_the_ledger_did_not_grade_is_refused_with_its_stored_reason(status, reason):
    rec = X.fly_refusal_receipt(skipped(reason, status))
    assert rec["kind"] == "D2/REFUSAL" and rec["reason_as_stored"] == reason and rec["graded_value"] is None
    assert X.verify_receipt(rec) == []


@pytest.mark.parametrize("row", [fly_row(), skipped(reason=None), skipped(reason=""), dict(skipped(), version="x"), None])
def test_only_a_skipped_or_ungraded_session_with_a_reason_makes_a_refusal_receipt(row):
    with pytest.raises(X.ReceiptError):
        X.fly_refusal_receipt(row)


def test_a_fly_refusal_cannot_be_rewritten_into_a_grade_or_a_new_reason():
    rec = X.fly_refusal_receipt(skipped())
    assert any("reason_as_stored" in e for e in X.verify_receipt(reseal({**rec, "reason_as_stored": "stale_spot_clock"})))
    assert any("graded_value" in e for e in X.verify_receipt(reseal({**rec, "graded_value": 1.2})))
    assert X.verify_receipt(reseal({**rec, "session": "2026-11-16"})) and X.verify_receipt(reseal({**rec, "extra": 1}))
    forged = copy.deepcopy(rec)
    forged["ledger_row"]["status"] = "traded"
    assert X.verify_receipt(reseal(forged))


# ---- every receipt verifies itself without raising --------------------------------------------

def _all_receipts():
    return [X.calendar_stub_refusal(STUB, PERIOD_ENDS), d1_receipt()[2], X.fly_traceable_receipt(fly_row()),
            X.fly_refusal_receipt(skipped())]


@pytest.mark.parametrize("bad", [None, [], "r", 3, {}, {"schema": X.RECEIPT_SCHEMA}, {"schema": "x", "kind": "D1/REFUSAL"}])
def test_a_non_receipt_is_refused_not_raised(bad):
    assert X.verify_receipt(bad)


def test_a_polarity_that_does_not_match_the_kind_is_refused():
    for rec in _all_receipts():
        flipped = reseal({**rec, "polarity": "positive" if rec["polarity"] == "refusal" else "refusal"})
        assert any("polarity" in e for e in X.verify_receipt(flipped))


def _paths(o, pre=()):
    if isinstance(o, dict):
        for k, v in o.items():
            yield pre + (k,)
            yield from _paths(v, pre + (k,))
    elif isinstance(o, list):
        for i, v in enumerate(o[:5]):
            yield pre + (i,)
            yield from _paths(v, pre + (i,))


def test_no_wrong_typed_field_makes_verification_raise():
    wrong = [None, "x", 7, [], {}, ["a"], True, 1.5, float("nan")]
    for rec in _all_receipts():
        for path in _paths(rec):
            if path == ("receipt_sha256",):
                continue
            for bad in wrong:
                m = copy.deepcopy(rec)
                t = m
                for k in path[:-1]:
                    t = t[k]
                t[path[-1]] = bad
                try:
                    # a NaN cannot be sealed (not canonical JSON); it is verified unsealed and must be refused
                    errs = X.verify_receipt(m if bad != bad else reseal(m))
                except Exception as exc:  # noqa: BLE001 - any exception is the defect under test
                    pytest.fail(f"{rec['kind']} {path} <- {bad!r} raised {type(exc).__name__}: {exc}")
                assert isinstance(errs, list)
                if bad != bad:
                    assert errs


def test_an_unsealed_edit_of_any_field_is_caught_by_the_hash():
    for rec in _all_receipts():
        for path in _paths(rec):
            if path == ("receipt_sha256",):
                continue
            m = copy.deepcopy(rec)
            t = m
            for k in path[:-1]:
                t = t[k]
            old = t[path[-1]]
            t[path[-1]] = [old] if not isinstance(old, list) else "changed"
            assert X.verify_receipt(m), (rec["kind"], path)


# ---- reproducible and offline -----------------------------------------------------------------

GOLDEN = [
    "51ea9baacc917930432660c1ff7c2e8fdbb475065e5639dc6da0a58c3f27d822",
    "326bc7edc856dab9b4db21a0e31b05c77d106625f4abf15705ba1f9a937393a6",
    "56b0e8fa211473c27d43cb9bef14c819f94467e1c6a9804a07f8c91ee098ca0f",
    "1880df35f3fcddd85c9eddfd43ba41e4c7f8794f757bd0a775b45ad171fa09a6",
]


def test_receipts_are_deterministic_and_pinned():
    a = [r["receipt_sha256"] for r in _all_receipts()]
    assert a == [r["receipt_sha256"] for r in _all_receipts()] and len(set(a)) == 4
    assert a == GOLDEN, a


def test_the_module_reads_no_wall_clock_file_database_or_network():
    path = Path(X.__file__)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = ({n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
             | {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)})
    assert not names & {"now", "utcnow", "today", "open", "connect", "urlopen", "read_text", "read_bytes", "execute"}
    imported = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            imported.update(a.name for a in n.names)
        elif isinstance(n, ast.ImportFrom):
            imported.add(n.module or "")
    assert imported <= {"__future__", "hashlib", "json", "datetime", "typing", "analysis"}, imported


def test_a_traceable_receipt_must_carry_exactly_one_of_a_row_or_a_reason():
    _, _, rec = d1_receipt()
    both = copy.deepcopy(rec)
    both["chain"]["unavailable_reason"] = "also a reason"
    neither = copy.deepcopy(rec)
    neither["chain"]["materialization_row"] = None
    for bad in (both, neither):
        assert any("exactly one" in e for e in X.verify_receipt(reseal(bad)))
