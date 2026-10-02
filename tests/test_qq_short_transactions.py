"""Transaction budgets, blackout boundaries, and acknowledged commit faults."""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import event
from sqlalchemy.exc import IntegrityError, OperationalError

from ingestion.altdata import quiverquant as qq
from ingestion.altdata import quiverquant_transactions as tx
from scripts import qq_transition_common as common
from scripts import qq_rekey_signal_sources as rekey
from scripts import qq_gov_contracts_redate as redate
from tests.test_qq_transition_scripts import engine as _engine, open_window as _window, _insert, _all_rows, HOUSE, _gov

engine = _engine
open_window = _window


def records(n):
    return [{"Ticker": f"T{i}", "Date": "2026-09-01", "Amount": i} for i in range(n)]


class WriterEngine:
    """Transactional fake, including server-committed/client-unacknowledged fault."""
    def __init__(self, *, bad_ticker=None, fault_batch=None, commit_fault=False):
        self.table = {}
        self.attempts = []
        self.commits = []
        self.bad_ticker = bad_ticker
        self.fault_batch = fault_batch
        self.commit_fault = commit_fault

    @contextmanager
    def begin(self):
        index = len(self.attempts) + 1
        count = [0]
        self.attempts.append(count)
        pending = dict(self.table)

        def execute(statement, params):
            assert "SAVEPOINT" not in str(statement)
            count[0] += 1
            if self.bad_ticker == params["ticker"]:
                raise IntegrityError("INSERT", {}, ValueError("bad fixture row"))
            if self.fault_batch == index and not self.commit_fault:
                raise OperationalError("INSERT", {}, OSError("connection lost"))
            pending[params["ticker"]] = json.loads(params["signal_value"])

        yield SimpleNamespace(execute=execute)
        self.table = pending
        if self.fault_batch == index and self.commit_fault:
            raise OperationalError("COMMIT", {}, OSError("ack lost"))
        self.commits.append(count[0])


def test_writer_uses_at_most_fifty_modifications_and_keeps_all_valid_records():
    eng = WriterEngine()
    assert qq._store_signals(eng, records(103), "quiverquant:lobbying", "lobbying") == 103
    assert eng.commits == [50, 50, 3] and len(eng.table) == 103


def test_writer_cannot_raise_ceiling_by_changing_batch_constant(monkeypatch):
    monkeypatch.setattr(qq,"STORE_BATCH_ROWS",51)
    with pytest.raises(ValueError,match="1 to 50"):
        qq._store_signals(object(),records(1),"quiverquant:lobbying","lobbying")


def test_elapsed_budget_rolls_back_instead_of_committing(monkeypatch):
    clock=[0.0]
    monkeypatch.setattr(tx.time,"monotonic",lambda: clock[0])
    eng=WriterEngine()
    with pytest.raises(RuntimeError,match="five-second"):
        with tx.write_transaction(eng) as (conn,check):
            check()
            conn.execute("INSERT",{"ticker":"T","signal_value":"{}"})
            clock[0]=5.0
    assert eng.table=={} and eng.commits==[]


def test_failed_batch_is_rolled_back_then_valid_rows_survive_new_transactions(monkeypatch):
    monkeypatch.setattr(qq, "STORE_BATCH_ROWS", 3)
    eng = WriterEngine(bad_ticker="T1")
    with pytest.raises(qq.QuiverStoreAborted) as caught:
        qq._store_signals(eng, records(5), "quiverquant:lobbying", "lobbying")
    assert (caught.value.stored, caught.value.failed, caught.value.commit_uncertain) == (4, 1, False)
    assert set(eng.table) == {"T0", "T2", "T3", "T4"}
    assert eng.commits == [1, 1, 2]


@pytest.mark.parametrize("commit_fault", [False, True])
def test_connection_or_commit_loss_never_replays_and_preserves_acknowledged_prefix(monkeypatch, commit_fault):
    monkeypatch.setattr(qq, "STORE_BATCH_ROWS", 2)
    eng = WriterEngine(fault_batch=2, commit_fault=commit_fault)
    with pytest.raises(qq.QuiverStoreAborted) as caught:
        qq._store_signals(eng, records(5), "quiverquant:lobbying", "lobbying")
    assert caught.value.stored == 2 and caught.value.commit_uncertain == commit_fault
    assert len(eng.attempts) == 2 and eng.commits == [2]
    assert len(eng.table) == (4 if commit_fault else 2)  # unknown transaction excluded from count


def test_fetch_completes_before_writes_and_failure_result_retains_prefix(monkeypatch):
    order = []
    monkeypatch.setattr(qq, "transition_guard_blocks", lambda endpoint: False)
    monkeypatch.setattr(qq, "_get_api_key", lambda: "synthetic")
    monkeypatch.setattr(qq, "_fetch_endpoint", lambda *a: order.append("fetch") or records(1))
    def store(*args):
        order.append("write")
        raise qq.QuiverStoreAborted("stopped", stored=3, uncertain=True)
    monkeypatch.setattr(qq, "_store_signals", store)
    result = qq.pull_endpoint(object(), "lobbying")
    assert order == ["fetch", "write"]
    assert result["status"] == "FAILED" and result["stored"] == 3 and result["commit_uncertain"]


@pytest.mark.parametrize("size", [0, -1, 51, 500, True, 1.5])
def test_rekey_direct_cap_is_checked_before_audit_or_connection(size, tmp_path):
    with pytest.raises(ValueError, match="1 to 50"):
        rekey.apply_moves(object(), [], batch_size=size, audit_path=tmp_path / "audit")
    assert not (tmp_path / "audit").exists()


def test_cli_rejects_oversized_batch_and_bad_before_date_before_connection(monkeypatch, tmp_path):
    monkeypatch.setattr(common, "open_engine", lambda *a, **k: pytest.fail("opened engine"))
    assert rekey.main(["--batch-size", "500"]) == 2
    assert rekey.main(["--before", "45d"]) == 2
    assert rekey.DEFAULT_BATCH_SIZE == 50


def test_rekey_default_cap_counts_real_modifications(engine, open_window, tmp_path):
    for i in range(101):  # every fixture transaction contains one row
        _insert(engine, "quiverquant:house", "qq_house_trading", f"T{i}", date(2026, 9, 1), "house_trading", HOUSE)
    counts = []
    @event.listens_for(engine, "begin")
    def begin(conn):
        conn.info["writes"] = 0
    @event.listens_for(engine, "after_cursor_execute")
    def wrote(conn, cursor, statement, params, context, many):
        if statement.lstrip().upper().startswith("UPDATE"):
            conn.info["writes"] += cursor.rowcount
    @event.listens_for(engine, "commit")
    def committed(conn):
        counts.append(conn.info["writes"])
    result = rekey.run(engine, source_types=["quiverquant:house"], apply=True, audit_path=tmp_path / "audit")
    assert result["applied"]["moved"] == 101 and counts == [50, 50, 1]
    assert len((tmp_path / "audit").read_text().splitlines()) == 101


def quarter_moves(n, ticker="LMT", start=1):
    rows = [_gov(start + i, ticker, 2010 + i // 4, i % 4 + 1,
                 redate.calendar_quarter_end(2010 + i // 4, i % 4 + 1)) for i in range(n)]
    return rows, redate.plan_redate(rows).moves


@pytest.mark.parametrize("forward", [False, True])
def test_redate_preflights_all_groups_before_any_write_or_audit(forward, tmp_path):
    _, small = quarter_moves(1, "FIRST")
    _, large = quarter_moves(51, "OVERSIZED", 100)
    with pytest.raises(ValueError, match="OVERSIZED.*51"):
        redate.apply_moves(object(), small + large, audit_path=tmp_path / "audit", forward=forward)
    assert not (tmp_path / "audit").exists()


def test_fifty_row_chain_round_trips_and_never_changes_other_fields(engine, open_window, tmp_path):
    rows, _ = quarter_moves(50)
    for row in rows:
        _insert(engine, redate.SOURCE_TYPE, row["source_id"], row["ticker"], row["signal_date"], row["signal_type"], row["signal_value"])
    original = _all_rows(engine)
    audit = tmp_path / "audit"
    assert redate.run(engine, apply=True, audit_path=audit)["applied"]["moved"] == 50
    assert redate.apply_moves(engine, redate.read_audit(audit), audit_path=tmp_path / "revert", forward=False)["moved"] == 50
    assert _all_rows(engine) == original


@pytest.mark.parametrize("hh,mm,weekday,blocked", [
    (3,29,2,False),(3,30,2,True),(10,29,2,True),(10,30,2,False),
    (10,57,2,False),(10,58,2,True),(11,11,2,True),(11,12,2,False),
    (13,24,2,False),(13,25,2,True),(14,19,2,True),(14,20,2,False),(13,25,3,False),
])
def test_all_blackout_edges(hh, mm, weekday, blocked):
    instant = datetime(2026,10,weekday,hh,mm,tzinfo=timezone.utc)
    if blocked:
        with pytest.raises(common.WindowClosed):
            common.check_window(instant)
    else:
        common.check_window(instant)


@pytest.mark.parametrize("hh,mm", [(3,29),(10,57),(13,24)])
def test_write_headroom_refuses_five_seconds_before_blackout(hh, mm):
    with pytest.raises(common.WindowClosed):
        common.write_guard(guard_check=False, now=datetime(2026,10,2,hh,mm,56,tzinfo=timezone.utc))


@pytest.mark.parametrize("script", [rekey, redate])
def test_guard_closes_at_commit_rolls_back_current_and_keeps_previous_receipt(script, engine, open_window, tmp_path, monkeypatch):
    if script is rekey:
        for i in range(2):
            _insert(engine, "quiverquant:house", "qq_house_trading", f"T{i}", date(2026,9,1), "house_trading", HOUSE)
        with engine.connect() as conn:
            moves = rekey.plan_rekey("quiverquant:house", rekey.load_legacy_rows(conn, "quiverquant:house"), set()).moves
    else:
        for ticker in ("A", "B"):
            row = quarter_moves(1, ticker)[0][0]
            _insert(engine, redate.SOURCE_TYPE, row["source_id"], ticker, row["signal_date"], row["signal_type"], row["signal_value"])
        with engine.connect() as conn:
            moves = redate.plan_redate(redate.load_rows(conn)).moves
    calls = [0]
    def guard(**kwargs):
        calls[0] += 1
        if calls[0] == 6:
            raise common.WindowClosed("closed before second COMMIT")
    monkeypatch.setattr(common, "write_guard", guard)
    kwargs = {"batch_size": 1} if script is rekey else {}
    with pytest.raises(common.WindowClosed) as caught:
        script.apply_moves(engine, moves, audit_path=tmp_path / "audit", **kwargs)
    assert caught.value.committed_rows == 1
    assert len((tmp_path / "audit").read_text().splitlines()) == 1


def test_post_commit_audit_failure_reports_actual_committed_prefix(engine, open_window, tmp_path, monkeypatch):
    _insert(engine, "quiverquant:house", "qq_house_trading", "T", date(2026,9,1), "house_trading", HOUSE)
    with engine.connect() as conn:
        moves = rekey.plan_rekey("quiverquant:house", rekey.load_legacy_rows(conn, "quiverquant:house"), set()).moves
    monkeypatch.setattr(common, "append_audit", lambda *a: (_ for _ in ()).throw(OSError("disk unavailable")))
    with pytest.raises(OSError) as caught:
        rekey.apply_moves(engine, moves, batch_size=1, audit_path=tmp_path / "audit")
    assert caught.value.committed_rows == 1 and not caught.value.commit_uncertain
    assert _all_rows(engine)[0]["source_id"].startswith("qq_house_trading:")


@pytest.mark.parametrize("script", [rekey, redate])
def test_commit_ack_loss_stops_transitions_without_inventing_audit(script, open_window, tmp_path):
    calls = [0]
    class Eng:
        @contextmanager
        def begin(self):
            calls[0] += 1
            yield SimpleNamespace(execute=lambda *a: SimpleNamespace(rowcount=1))
            raise OperationalError("COMMIT", {}, OSError("ack lost"))
    moves = ([rekey.Move(1,"quiverquant:house","T",date(2026,9,1),"house_trading","old","new")]
             if script is rekey else quarter_moves(1)[1])
    kwargs = {"batch_size": 1} if script is rekey else {}
    with pytest.raises(tx.CommitUncertain) as caught:
        script.apply_moves(Eng(), moves, audit_path=tmp_path / "audit", **kwargs)
    assert caught.value.committed_rows == 0 and caught.value.commit_uncertain
    assert calls == [1] and (tmp_path / "audit").read_text() == ""
