"""Actual CLI receipts, real COMMIT/rejection and audit-device failures on private PG14."""
from contextlib import contextmanager
import json
import os
from pathlib import Path

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError

from ingestion.altdata import quiverquant_transactions as tx
from scripts import qq_gov_contracts_redate as redate
from scripts import qq_rekey_signal_sources as rekey
from scripts import qq_transition_common as common
from tests.test_qq_short_transactions import quarter_moves
from tests.test_qq_short_transactions_pg import pg as _pg, seed, all_rows, rekey_plan

pg = _pg

CASES = [
    ("success", None, True), ("reject23514", None, True), ("reject57014", None, True),
    ("ack_cleanup", None, True), ("lost_ack", None, True), ("lost_ack_cleanup", None, True),
    ("lost_ack", "first_fsync", True),
] + [(resolution, fault, close) for resolution in ("success", "ack_cleanup")
     for fault in ("append", "flush", "fsync") for close in (True, False)]


@pytest.mark.parametrize("script", [rekey, redate])
@pytest.mark.parametrize("resolution,fault,close_failure", CASES)
def test_actual_cli_audit_exit_keeps_ack_prefix_cause_and_pending_uncertainty(
    pg, tmp_path, monkeypatch, capsys, script, resolution, fault, close_failure,
):
    engine, counts = pg
    if script is rekey:
        seed(engine, [{"ticker": t} for t in ("FIRST", "MIDDLE", "ZZZ_LAST")])
        moves = rekey_plan(engine)
    else:
        rows = sum((quarter_moves(1, t)[0] for t in ("FIRST", "MIDDLE", "ZZZ_LAST")), [])
        seed(engine, [{**r, "source_type": redate.SOURCE_TYPE} for r in rows])
        with engine.connect() as conn:
            moves = redate.plan_redate(redate.load_rows(conn)).moves
    if resolution.startswith("reject"):
        code = "23514" if resolution == "reject23514" else "57014"
        with engine.begin() as conn:
            conn.execute(text("CREATE FUNCTION reject_middle() RETURNS trigger LANGUAGE plpgsql AS $$ "
                              "BEGIN IF NEW.ticker='MIDDLE' THEN RAISE EXCEPTION 'synthetic rejection' "
                              f"USING ERRCODE='{code}'; END IF; RETURN NEW; END $$"))
            conn.execute(text("CREATE CONSTRAINT TRIGGER reject_middle AFTER UPDATE ON signal_sources "
                              "DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_middle()"))
    before = all_rows(engine)
    audit_path = tmp_path / "audit.jsonl"
    attempts = {"write": 0, "flush": 0, "fsync": 0, "close": 0, "commit": 0, "checkin": 0}
    durable_prefix = []
    incoming = []
    reporting_errors = []
    real_open, real_fsync = Path.open, os.fsync

    class AuditDevice:
        def __init__(self, real):
            self.real = real

        def __enter__(self):
            self.real.__enter__()
            return self

        def __getattr__(self, name):
            return getattr(self.real, name)

        def write(self, data):
            attempts["write"] += 1
            if fault == "append" and attempts["write"] == 2:
                raise OSError("synthetic append failure")
            return self.real.write(data)

        def flush(self):
            attempts["flush"] += 1
            if fault == "flush" and attempts["flush"] == 2:
                raise OSError("synthetic flush failure")
            self.real.flush()

        def __exit__(self, kind, value, tb):
            incoming.append(value)
            attempts["close"] += 1
            self.real.__exit__(kind, value, tb)
            if close_failure:
                raise OSError("synthetic audit close failure")

    def open_file(path, *args, **kwargs):
        real = real_open(path, *args, **kwargs)
        return AuditDevice(real) if path == audit_path and args and args[0] == "x" else real

    def fsync(fd):
        attempts["fsync"] += 1
        if ((fault == "fsync" and attempts["fsync"] == 2)
                or (fault == "first_fsync" and attempts["fsync"] == 1)):
            raise OSError("synthetic fsync failure")
        real_fsync(fd)
        durable_prefix[:] = [json.loads(line)["id"] for line in audit_path.read_text().splitlines()]

    monkeypatch.setattr(Path, "open", open_file)
    monkeypatch.setattr(common.os, "fsync", fsync)
    attached_pool = engine.pool

    def checkin(dbapi, record):
        attempts["checkin"] += 1
        if "cleanup" not in resolution or attempts["checkin"] != 2:
            return
        try:
            with dbapi.cursor() as cursor:
                cursor.execute("DO $$ BEGIN RAISE EXCEPTION 'synthetic cleanup' USING ERRCODE='57014'; END $$")
        except Exception as cause:
            dbapi.rollback()
            assert cause.pgcode == "57014" and dbapi.get_transaction_status() == 0
            raise OperationalError("CHECKIN AFTER COMMIT", {}, cause)

    class ExecutionEngine:
        dialect = engine.dialect

        def connect(self):
            return engine.connect()

        def dispose(self):
            engine.dispose()

        @contextmanager
        def begin(self):
            with engine.begin() as conn:
                real_commit = conn.commit

                def commit():
                    attempts["commit"] += 1
                    real_commit()
                    if resolution.startswith("lost_ack") and attempts["commit"] == 2:
                        raise OperationalError("COMMIT", {}, OSError("synthetic lost transport ACK"))

                conn.commit = commit
                yield conn

    real_apply = script.apply_moves

    def apply(*args, **kwargs):
        try:
            return real_apply(*args, **kwargs)
        except Exception as exc:
            reporting_errors.append(exc)
            raise

    real_write = tx.write_transaction
    installed = [False]

    def write(*args, **kwargs):
        if not installed[0]:
            event.listen(attached_pool, "checkin", checkin)
            installed[0] = True
        return real_write(*args, **kwargs)

    monkeypatch.setattr(tx, "write_transaction", write)
    monkeypatch.setattr(script, "apply_moves", apply)
    monkeypatch.setattr(common, "open_engine", lambda *args, **kwargs: ExecutionEngine())
    monkeypatch.setattr(common, "database_url", lambda *args: "synthetic_private_override")
    counts.clear()
    try:
        args = ["--apply", "--audit-log", str(audit_path)]
        if script is rekey:
            args += ["--source-type", "quiverquant:house", "--batch-size", "1"]
        result = script.main(args)
        output = capsys.readouterr()
        receipt = json.loads(output.err.splitlines()[-1])
    finally:
        if installed[0]:
            event.remove(attached_pool, "checkin", checkin)
    after = all_rows(engine)
    column = "source_id" if script is rekey else "signal_date"
    changed_ids = [a["id"] for a, b in zip(after, before) if a[column] != b[column]]
    for a, b in zip(after, before):
        assert {k: v for k, v in a.items() if k != column} == {k: v for k, v in b.items() if k != column}
    if fault == "first_fsync":
        actual, prefix, writes, audit_lines, durable, uncertain = 1, 1, 1, 1, 0, False
    elif resolution == "reject23514":
        actual, prefix, writes, audit_lines, durable, uncertain = 1, 1, 2, 1, 1, False
    elif resolution == "reject57014":
        actual, prefix, writes, audit_lines, durable, uncertain = 2, 2, 3, 2, 2, False
    elif resolution.startswith("lost_ack"):
        actual, prefix, writes, audit_lines, durable, uncertain = 2, 1, 2, 1, 1, True
    elif fault is not None or resolution == "ack_cleanup":
        actual, prefix, writes, audit_lines, durable, uncertain = 2, 2, 2, 1 if fault == "append" else 2, 1 if fault else 2, False
    else:
        actual, prefix, writes, audit_lines, durable, uncertain = 3, 3, 3, 3, 3, False
    assert result == 5 and receipt["status"] == "ABORTED" and output.out == ""
    assert receipt["acknowledged_committed_rows"] == prefix and receipt["commit_uncertain"] is uncertain
    assert len(changed_ids) == actual and attempts["commit"] == writes and counts == [1] * writes
    assert len(audit_path.read_text().splitlines()) == audit_lines and len(durable_prefix) == durable
    expected_ids = [r["id"] for r in before if r["ticker"] != "MIDDLE"] if resolution == "reject57014" else [r["id"] for r in before[:actual]]
    assert changed_ids == expected_ids
    if actual < 3 and resolution != "reject57014":
        assert after[-1] == before[-1], "later scope must remain untouched after fatal STOP"
    assert attempts["close"] == 1 and attempts["checkin"] == writes
    body_error, reporting_error = incoming[0], reporting_errors[0]
    cause = getattr(reporting_error, "resolution_cause", None)
    if fault == "first_fsync":
        assert isinstance(cause, OSError)
    elif resolution.startswith("lost_ack"):
        assert isinstance(cause, tx.CommitUncertain)
    elif resolution == "ack_cleanup":
        assert isinstance(cause, tx.CommitAcknowledgedCleanupError)
    elif resolution == "reject23514":
        assert cause.orig.pgcode == "23514" and cause.qq_write_rolled_back
    elif fault:
        assert cause is body_error and isinstance(cause, OSError)
    else:
        assert cause is None
    assert receipt["resolution_error_type"] == (type(cause).__name__ if cause is not None else None)
    if body_error is not None and close_failure:
        assert reporting_error.__cause__ is body_error
    if fault:
        assert attempts["write"] <= 2 and attempts["flush"] <= 2 and attempts["fsync"] <= 2
        expected_attempts = {"append": (2, 1, 1), "flush": (2, 2, 1),
                             "fsync": (2, 2, 2), "first_fsync": (1, 1, 1)}[fault]
        assert tuple(attempts[k] for k in ("write", "flush", "fsync")) == expected_attempts
    assert [json.loads(line)["id"] for line in audit_path.read_text().splitlines()] == changed_ids[:audit_lines]
    assert "stop; reconcile" in receipt["action"]
    print("AUDIT_CONTEXT_ACTUAL_CLI", script.__name__, json.dumps({
        "resolution": resolution, "audit_fault": fault, "close_failure": close_failure,
        "CLI_exit": result, "acknowledged_rows": prefix, "actual_changed": actual,
        "audit_lines": audit_lines, "durable_prefix_rows": durable,
        "commit_uncertain": uncertain, "resolution_error_type": receipt["resolution_error_type"],
        "attempt_counts": attempts, "later_scope_untouched": actual < 3 and resolution != "reject57014",
    }))
