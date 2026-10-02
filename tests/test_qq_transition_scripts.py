"""The two QuiverQuant transition scripts: re-key (act keys) and gov_contracts re-date.

Pure fixtures. The planners are tested directly; the database layers run against an
in-memory SQLite table with the same UNIQUE key as ``signal_sources`` (both scripts'
SQL is portable on purpose), so the "never collide, never delete" claims are
exercised by a real unique constraint rather than assumed.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from ingestion.altdata import quiverquant_identity as ident
from scripts import qq_gov_contracts_redate as redate
from scripts import qq_rekey_signal_sources as rekey
from scripts import qq_transition_common as common

UTC = timezone.utc


@pytest.fixture()
def open_window(monkeypatch):
    """The scripts consult the wall clock; the tests must not depend on it."""
    monkeypatch.setattr(common, "check_window", lambda now=None: None)


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE signal_sources ("
            " id INTEGER PRIMARY KEY AUTOINCREMENT, source_type TEXT NOT NULL, source_id TEXT NOT NULL,"
            " ticker TEXT, signal_date DATE NOT NULL, signal_type TEXT NOT NULL, signal_value TEXT,"
            " outcome TEXT, created_at TEXT,"
            " UNIQUE (source_type, source_id, ticker, signal_date, signal_type))"
        ))
    yield eng
    eng.dispose()


def _insert(engine, source_type, source_id, ticker, signal_date, signal_type, payload, created_at="2026-09-01T00:00:00"):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO signal_sources (source_type, source_id, ticker, signal_date, signal_type, signal_value,"
            " outcome, created_at) VALUES (:st, :si, :t, :d, :ty, :v, 'PENDING', :c)"
        ), {"st": source_type, "si": source_id, "t": ticker, "d": signal_date, "ty": signal_type,
            "v": json.dumps(payload), "c": created_at})


def _all_rows(engine):
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text("SELECT * FROM signal_sources ORDER BY id")).mappings()]


# ═══ shared guards ═══════════════════════════════════════════════════════════


@pytest.mark.parametrize("hh,mm,refused", [
    (3, 29, False), (3, 30, True), (7, 0, True), (10, 29, True), (10, 30, False), (23, 59, False),
])
def test_backup_window_edges(hh, mm, refused):
    now = datetime(2026, 10, 2, hh, mm, tzinfo=UTC)
    if refused:
        with pytest.raises(common.WindowClosed):
            common.check_window(now)
    else:
        common.check_window(now)


@pytest.mark.parametrize("script", [rekey, redate])
def test_apply_inside_the_window_is_refused_before_any_connection(script, monkeypatch, tmp_path):
    def _closed(now=None):
        raise common.WindowClosed("refusing database reads at 07:00Z")

    monkeypatch.setattr(common, "assert_db_window_open", _closed)
    monkeypatch.setattr(common, "open_engine", lambda *a, **k: pytest.fail("opened a connection inside the window"))
    assert script.main(["--apply", "--audit-log", str(tmp_path / "audit.jsonl")]) == 3
    assert script.main([]) == 3  # the dry run reads production too: also refused


@pytest.mark.parametrize("script", [rekey, redate])
def test_apply_requires_an_audit_log(script, monkeypatch):
    monkeypatch.setattr(common, "open_engine", lambda *a, **k: pytest.fail("opened a connection"))
    assert script.main(["--apply"]) == 2


@pytest.mark.parametrize("script", [rekey, redate])
def test_default_is_a_read_only_dry_run(script, monkeypatch, tmp_path):
    seen = {}

    class _Boom(Exception):
        pass

    def _open(url, *, read_only, application_name):
        seen["read_only"] = read_only
        raise _Boom

    monkeypatch.setattr(common, "check_window", lambda now=None: None)
    monkeypatch.setattr(common, "database_url", lambda env: "postgresql://unused")
    monkeypatch.setattr(common, "open_engine", _open)
    with pytest.raises(_Boom):
        script.main([])
    assert seen["read_only"] is True


# ═══ re-key ══════════════════════════════════════════════════════════════════

HOUSE = {"Representative": "Jane Doe", "BioGuideID": "D000123", "Transaction": "Purchase",
         "Range": "$1,001 - $15,000", "last_modified": "2026-09-05"}
HOUSE2 = {**HOUSE, "Representative": "Sam Poe", "BioGuideID": "P000777"}
HOUSE_KEY = "qq_house_trading:D000123|purchase|1001-15000"
HOUSE2_KEY = "qq_house_trading:P000777|purchase|1001-15000"


def _legacy(id_, payload, ticker="ACME", day=date(2026, 9, 1), sig="house_trading", sid="qq_house_trading"):
    return {"id": id_, "source_id": sid, "ticker": ticker, "signal_date": day, "signal_type": sig,
            "signal_value": payload}


def test_target_id_equals_what_the_writer_would_have_written():
    assert rekey.target_source_id("quiverquant:house", HOUSE) == HOUSE_KEY
    assert rekey.target_source_id("quiverquant:house", json.dumps(HOUSE) and HOUSE) == HOUSE_KEY
    assert rekey.target_source_id("quiverquant:wsb", {"Mentions": 3}) is None  # aggregate feeds are never re-keyed
    assert rekey.target_source_id("quiverquant:house", {"Amount": 1}) is None  # no identity fields at all


def test_plan_moves_free_rows_and_reports_conflicts():
    existing = {("quiverquant:house", HOUSE2_KEY, "ACME", date(2026, 9, 1), "house_trading")}
    rows = [
        _legacy(1, HOUSE),                                   # free target: move
        _legacy(2, HOUSE2),                                  # keyed row already there: conflict
        _legacy(3, HOUSE, ticker="OTHER"),                   # different ticker: move
        _legacy(4, "not json"),                              # unreadable
        _legacy(5, {"Amount": 9}),                           # nothing to key on
        _legacy(6, HOUSE, sid=HOUSE_KEY),                    # not a legacy row at all
    ]
    plan = rekey.plan_rekey("quiverquant:house", rows, existing)
    assert [m.id for m in plan.moves] == [1, 3]
    assert [m.id for m in plan.conflicts] == [2]
    assert plan.skipped == {"unreadable_payload": 1, "no_identity_fields": 1, "not_a_legacy_row": 1}
    assert plan.legacy_rows == 5


def test_plan_never_claims_the_same_target_twice():
    plan = rekey.plan_rekey("quiverquant:house", [_legacy(1, HOUSE), _legacy(2, HOUSE)], set())
    assert len(plan.moves) == 1 and len(plan.conflicts) == 1


def _seed_rekey(engine):
    # two acts the old writer collapsed into one row per (ticker, date, type): the survivors
    _insert(engine, "quiverquant:house", "qq_house_trading", "ACME", date(2026, 9, 1), "house_trading", HOUSE)
    _insert(engine, "quiverquant:house", "qq_house_trading", "ZZZ", date(2026, 9, 2), "house_trading", HOUSE2)
    # the overlap window: the new writer already wrote ZZZ's act under its keyed id
    _insert(engine, "quiverquant:house", HOUSE2_KEY, "ZZZ", date(2026, 9, 2), "house_trading", HOUSE2,
            created_at="2026-10-01T00:00:00")
    _insert(engine, "quiverquant:insider", "qq_insider_trading", "ACME", date(2026, 9, 30), "insider_sell",
            {"Name": "Smith, John Q.", "TransactionCode": "S", "Shares": 1000, "PricePerShare": 12.5})
    _insert(engine, "quiverquant:wsb", "qq_wsb", "ACME", date(2026, 9, 30), "wsb_bullish", {"Mentions": 4})


def test_dry_run_changes_nothing_and_reports_counts(engine, open_window):
    _seed_rekey(engine)
    before = _all_rows(engine)
    report = rekey.run(engine, source_types=sorted(rekey.KEYED_SOURCE_TYPES), apply=False, audit_path=None,
                       today=date(2026, 10, 2))
    assert _all_rows(engine) == before
    house = report["source_types"]["quiverquant:house"]
    assert (house["legacy_rows"], house["would_move"], house["conflicts_skipped"]) == (2, 1, 1)
    assert house["in_extractor_window"] == 1  # 2026-09-01 is within 45 days of 2026-10-02
    assert house["legacy_rows_by_age"] == {"<=7d": 0, "<=30d": 1, "<=90d": 1, ">90d": 0}  # 09-02 and 09-01
    assert house["conflict_sample"] == [{"id": 2, "ticker": "ZZZ", "signal_date": "2026-09-02",
                                         "signal_type": "house_trading"}]
    assert report["source_types"]["quiverquant:insider"]["would_move"] == 1
    assert report["totals"]["would_move"] == 2 and report["totals"]["conflicts_skipped"] == 1


def test_apply_moves_only_free_rows_and_never_deletes(engine, open_window, tmp_path):
    _seed_rekey(engine)
    before = {r["id"]: r for r in _all_rows(engine)}
    audit = tmp_path / "audit.jsonl"
    report = rekey.run(engine, source_types=sorted(rekey.KEYED_SOURCE_TYPES), apply=True, audit_path=audit,
                       today=date(2026, 10, 2))
    after = {r["id"]: r for r in _all_rows(engine)}

    assert set(after) == set(before)  # nothing deleted, nothing added
    assert report["applied"] == {"moved": 2, "not_moved_target_taken_or_row_changed": 0}
    assert after[1]["source_id"] == HOUSE_KEY
    assert after[2]["source_id"] == "qq_house_trading"          # conflict: left exactly as it was
    assert after[4]["source_id"] == "qq_insider_trading:smith john q|s|1000|12.5"
    assert after[5]["source_id"] == "qq_wsb"                    # aggregate: untouched
    for id_, row in after.items():                              # only source_id may change
        assert {k: v for k, v in row.items() if k != "source_id"} == {k: v for k, v in before[id_].items() if k != "source_id"}
    logged = [json.loads(line) for line in audit.read_text().splitlines()]
    assert {(r["id"], r["old_source_id"]) for r in logged} == {(1, "qq_house_trading"), (4, "qq_insider_trading")}


def test_apply_is_idempotent(engine, open_window, tmp_path):
    _seed_rekey(engine)
    rekey.run(engine, source_types=sorted(rekey.KEYED_SOURCE_TYPES), apply=True, audit_path=tmp_path / "a1.jsonl")
    again = rekey.run(engine, source_types=sorted(rekey.KEYED_SOURCE_TYPES), apply=True,
                      audit_path=tmp_path / "a2.jsonl")
    assert again["applied"]["moved"] == 0
    assert again["totals"]["would_move"] == 0 and again["totals"]["conflicts_skipped"] == 1


def test_apply_loses_gracefully_to_a_concurrent_writer(engine, open_window, tmp_path, monkeypatch):
    """A keyed row that appears between plan and UPDATE blocks the move instead of colliding."""
    _seed_rekey(engine)
    real_plan = rekey.plan_rekey

    def _plan_then_race(source_type, rows, existing_keys):
        plan = real_plan(source_type, rows, existing_keys)
        if source_type == "quiverquant:house":
            _insert(engine, "quiverquant:house", HOUSE_KEY, "ACME", date(2026, 9, 1), "house_trading", HOUSE)
        return plan

    monkeypatch.setattr(rekey, "plan_rekey", _plan_then_race)
    report = rekey.run(engine, source_types=["quiverquant:house"], apply=True, audit_path=tmp_path / "a.jsonl")
    assert report["applied"] == {"moved": 0, "not_moved_target_taken_or_row_changed": 1}
    legacy = [r for r in _all_rows(engine) if r["id"] == 1][0]
    assert legacy["source_id"] == "qq_house_trading"


def test_before_filter_leaves_recent_rows_out(engine, open_window):
    _seed_rekey(engine)
    report = rekey.run(engine, source_types=["quiverquant:house"], apply=False, audit_path=None,
                       before=date(2026, 9, 2))
    house = report["source_types"]["quiverquant:house"]
    assert house["legacy_rows"] == 1 and house["would_move"] == 1


def test_audit_log_is_never_overwritten(tmp_path):
    audit = tmp_path / "audit.jsonl"
    audit.write_text("x")
    assert rekey.main(["--apply", "--audit-log", str(audit)]) == 2


# ═══ gov_contracts re-date ═══════════════════════════════════════════════════


def _gov(id_, ticker, year, qtr, day, amount=1.0):
    return {"id": id_, "source_id": "qq_gov_contracts", "ticker": ticker, "signal_type": "gov_contracts",
            "signal_date": day, "signal_value": {"Year": year, "Qtr": qtr, "Amount": amount}}


def _calendar_chain(ticker="LMT", start_id=1):
    """What #694 wrote for FY2025 Q4 .. FY2026 Q4: one row per quarter at its CALENDAR end."""
    quarters = [(2025, 4), (2026, 1), (2026, 2), (2026, 3), (2026, 4)]
    return [_gov(start_id + i, ticker, y, q, ident.calendar_quarter_end(y, q)) for i, (y, q) in enumerate(quarters)]


def _replay(moves, occupied):
    """Execute moves in order against a slot set; fail on any collision."""
    slots = set(occupied)
    for m in moves:
        assert (m.source_id, m.ticker, m.signal_type, m.new_date) not in slots, f"collision at {m}"
        slots.remove((m.source_id, m.ticker, m.signal_type, m.old_date))
        slots.add((m.source_id, m.ticker, m.signal_type, m.new_date))
    return slots


def test_collision_chain_moves_in_ascending_order_without_a_collision():
    rows = _calendar_chain()
    plan = redate.plan_redate(rows)
    assert [m.old_date for m in plan.moves] == sorted(m.old_date for m in plan.moves)
    assert [(m.year, m.qtr, m.new_date) for m in plan.moves] == [
        (2025, 4, date(2025, 9, 30)), (2026, 1, date(2025, 12, 31)), (2026, 2, date(2026, 3, 31)),
        (2026, 3, date(2026, 6, 30)), (2026, 4, date(2026, 9, 30)),
    ]
    assert not plan.conflicts and plan.longest_chain == 5
    occupied = {(r["source_id"], r["ticker"], r["signal_type"], r["signal_date"]) for r in rows}
    final = _replay(plan.moves, occupied)
    assert {d for *_, d in final} == {date(2025, 9, 30), date(2025, 12, 31), date(2026, 3, 31),
                                      date(2026, 6, 30), date(2026, 9, 30)}


def test_the_naive_order_would_collide():
    """Why the order matters: moving the latest quarter first lands on a slot still held."""
    rows = _calendar_chain()
    plan = redate.plan_redate(rows)
    occupied = {(r["source_id"], r["ticker"], r["signal_type"], r["signal_date"]) for r in rows}
    with pytest.raises(AssertionError, match="collision"):
        _replay(list(reversed(plan.moves)), occupied)


def test_planner_is_independent_of_input_order():
    rows = _calendar_chain()
    forward = redate.plan_redate(rows)
    shuffled = redate.plan_redate([rows[3], rows[0], rows[4], rows[2], rows[1]])
    assert sorted((m.id, m.new_date) for m in forward.moves) == sorted((m.id, m.new_date) for m in shuffled.moves)
    assert [m.old_date for m in shuffled.moves] == sorted(m.old_date for m in shuffled.moves)


def test_chain_with_a_gap_still_moves_every_row():
    rows = [r for r in _calendar_chain() if (r["signal_value"]["Year"], r["signal_value"]["Qtr"]) != (2026, 2)]
    plan = redate.plan_redate(rows)
    assert len(plan.moves) == 4 and not plan.conflicts
    assert plan.longest_chain == 2  # 2025Q4 -> 2026Q1, then the gap, then 2026Q3 -> 2026Q4
    assert plan.chain_count == 2


@pytest.mark.parametrize("qtr", [1, 2, 3, 4])
def test_each_quarter_moves_to_its_fiscal_end(qtr):
    plan = redate.plan_redate([_gov(1, "LMT", 2026, qtr, ident.calendar_quarter_end(2026, qtr))])
    assert [(m.old_date, m.new_date) for m in plan.moves] == [
        (ident.calendar_quarter_end(2026, qtr), ident.fiscal_quarter_end(2026, qtr))]


def test_rows_already_overwritten_by_the_new_writer_are_left_alone_and_the_stale_top_row_is_reported():
    """After the first pull of the fiscal writer: every slot is correct except the old top row."""
    rows = [
        _gov(1, "LMT", 2026, 1, date(2025, 12, 31)),   # new writer inserted
        _gov(2, "LMT", 2026, 2, date(2026, 3, 31)),    # overwrote old Q1 row in place
        _gov(3, "LMT", 2026, 3, date(2026, 6, 30)),    # overwrote old Q2 row in place
        _gov(4, "LMT", 2026, 3, date(2026, 9, 30)),    # old Q3 row: stale, future-ish duplicate of #3
    ]
    plan = redate.plan_redate(rows)
    assert plan.skipped["already_fiscal"] == 3 and not plan.moves
    assert [(c.id, reason) for c, reason in plan.conflicts] == [(4, "duplicate_of_fiscal_row")]


def test_a_conflict_with_another_quarters_unmoved_row_is_reported_as_such():
    rows = [
        _gov(1, "LMT", 2026, 3, date(2026, 3, 31)),    # dated neither end of its quarter: not touched
        _gov(2, "LMT", 2026, 2, date(2026, 6, 30)),    # would move to 2026-03-31, held by row 1
    ]
    plan = redate.plan_redate(rows)
    assert plan.skipped["not_post_fix_row"] == 1
    assert [(c.id, reason) for c, reason in plan.conflicts] == [(2, "target_held_by_other")]


def test_pre_694_snapshots_and_unreadable_payloads_are_not_touched():
    rows = [
        _gov(1, "LMT", 2026, 2, date(2026, 5, 2)),     # daily snapshot dated the pull day
        {**_gov(2, "LMT", 2026, 2, date(2026, 6, 30)), "signal_value": {"Amount": 1}},
        {**_gov(3, "LMT", 2026, 2, date(2026, 6, 30)), "signal_value": "garbage"},
    ]
    plan = redate.plan_redate(rows)
    assert not plan.moves and not plan.conflicts
    assert plan.skipped["not_post_fix_row"] == 1 and plan.skipped["no_period"] == 2


def test_tickers_are_planned_independently():
    plan = redate.plan_redate(_calendar_chain("LMT", 1) + _calendar_chain("NOC", 10))
    assert len(plan.moves) == 10 and not plan.conflicts


def _seed_gov(engine):
    for r in _calendar_chain("LMT"):
        _insert(engine, "quiverquant:gov_contracts", "qq_gov_contracts", "LMT", r["signal_date"], "gov_contracts",
                r["signal_value"])
    # a pre-#694 daily snapshot and an unrelated source_type: neither may move
    _insert(engine, "quiverquant:gov_contracts", "qq_gov_contracts", "LMT", date(2026, 5, 2), "gov_contracts",
            {"Year": 2026, "Qtr": 2, "Amount": 1.0})
    _insert(engine, "quiverquant:lobbying", "qq_lobbying", "LMT", date(2026, 12, 31), "lobbying",
            {"Year": 2026, "Qtr": 4})


def _dates(engine):
    return {r["id"]: r["signal_date"] for r in _all_rows(engine)}


def test_redate_dry_run_changes_nothing(engine, open_window):
    _seed_gov(engine)
    before = _all_rows(engine)
    report = redate.run(engine, apply=False, audit_path=None, today=date(2026, 10, 2))
    assert _all_rows(engine) == before
    s = report["summary"]
    assert (s["rows_examined"], s["would_move"], s["conflicts_skipped"], s["not_post_fix_row"]) == (6, 5, 0, 1)
    assert s["longest_collision_chain"] == 5 and s["collision_chains"] == 1
    assert s["future_dated_moves_before"] == 1 and s["future_dated_moves_after"] == 0  # the 2026-12-31 row


def test_redate_apply_uses_the_real_unique_constraint_and_never_deletes(engine, open_window, tmp_path):
    _seed_gov(engine)
    before = _all_rows(engine)
    audit = tmp_path / "audit.jsonl"
    report = redate.run(engine, apply=True, audit_path=audit, today=date(2026, 10, 2))
    after = _all_rows(engine)
    assert report["applied"] == {"moved": 5, "not_moved_slot_taken_or_row_changed": 0}
    assert [r["id"] for r in after] == [r["id"] for r in before]
    for b, a in zip(before, after):
        assert {k: v for k, v in a.items() if k != "signal_date"} == {k: v for k, v in b.items() if k != "signal_date"}
    assert [a["signal_date"] for a in after] == [
        "2025-09-30", "2025-12-31", "2026-03-31", "2026-06-30", "2026-09-30", "2026-05-02", "2026-12-31"]
    assert len(audit.read_text().splitlines()) == 5


def test_redate_apply_twice_is_a_no_op(engine, open_window, tmp_path):
    _seed_gov(engine)
    redate.run(engine, apply=True, audit_path=tmp_path / "a1.jsonl")
    again = redate.run(engine, apply=True, audit_path=tmp_path / "a2.jsonl")
    assert again["summary"]["would_move"] == 0 and again["applied"]["moved"] == 0


def test_redate_revert_restores_every_date(engine, open_window, tmp_path):
    _seed_gov(engine)
    original = _dates(engine)
    audit = tmp_path / "audit.jsonl"
    redate.run(engine, apply=True, audit_path=audit)
    assert _dates(engine) != original
    reverted = redate.apply_moves(engine, redate.read_audit(audit), audit_path=tmp_path / "revert.jsonl", forward=False)
    assert reverted == {"moved": 5, "not_moved_slot_taken_or_row_changed": 0}
    assert _dates(engine) == original


def test_redate_does_not_move_onto_a_slot_taken_since_the_plan(engine, open_window, tmp_path):
    _insert(engine, "quiverquant:gov_contracts", "qq_gov_contracts", "LMT", date(2026, 6, 30), "gov_contracts",
            {"Year": 2026, "Qtr": 2, "Amount": 1.0})
    plan = redate.plan_redate(redate.load_rows(engine.connect()))
    assert len(plan.moves) == 1
    _insert(engine, "quiverquant:gov_contracts", "qq_gov_contracts", "LMT", date(2026, 3, 31), "gov_contracts",
            {"Year": 2026, "Qtr": 2, "Amount": 2.0})  # the fiscal writer got there first
    result = redate.apply_moves(engine, plan.moves, audit_path=tmp_path / "a.jsonl")
    assert result == {"moved": 0, "not_moved_slot_taken_or_row_changed": 1}
    assert len(_all_rows(engine)) == 2


# ═══ people-events adapter: both writer generations are post-fix rows ═══════


def _frame(*rows):
    return pd.DataFrame([dict(id=i, source_type="quiverquant:gov_contracts", source_id="qq_gov_contracts",
                              ticker="LMT", signal_date=day, signal_type="gov_contracts",
                              signal_value={"Year": 2026, "Qtr": 2, "Amount": 1e6}, created_at=created)
                         for i, (day, created) in enumerate(rows, start=1)])


def test_adapter_treats_fiscal_and_calendar_quarter_ends_as_post_fix_rows():
    from intelligence.people_events_pipeline import adapters as A
    from intelligence.people_events_pipeline import rules as R

    observed = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
    frame = _frame(
        ("2026-05-02", "2026-05-02T09:00:00Z"),   # pre-#694 daily snapshot: bounded by its own day
        ("2026-06-30", "2026-04-01T09:00:00Z"),   # calendar end (#694 writer): rewritten in place
        ("2026-03-31", "2026-04-01T09:00:00Z"),   # fiscal end (current writer): rewritten in place
    )
    cands, _ = A.from_signal_sources(frame, observed)
    known = dict(zip(cands["source_record_id"], cands["known_at"]))
    assert known["quiverquant:gov_contracts:1"] == pd.Timestamp(R.next_session_open_after(date(2026, 5, 2)))
    assert known["quiverquant:gov_contracts:2"] == pd.Timestamp(observed)
    assert known["quiverquant:gov_contracts:3"] == pd.Timestamp(observed)  # not trusted as first_seen
