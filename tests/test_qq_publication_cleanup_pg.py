"""Actual CLI/ordinary-PG publication and final cleanup; synthetic DATA only.

Original frozen reviewer assertions remain in the private harness unchanged.
Alias expectations are strengthened to refusal before ANY DB acquisition;
publication faults target the new reserved handle, not removed write_text.
"""
import json
import os
from contextlib import contextmanager
from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError

from ingestion.altdata import quiverquant_transactions as tx
from scripts import qq_gov_contracts_redate as redate
from scripts import qq_rekey_signal_sources as rekey
from scripts import qq_transition_common as common
from tests.test_qq_publication_cleanup import BrokenChannel
from tests.test_qq_short_transactions import quarter_moves
from tests.test_qq_short_transactions_pg import all_rows, pg, seed


def invoke(script, audit, out=None, revert=None):
    args = ["--revert", str(revert)] if revert else ["--apply"]
    args += ["--audit-log", str(audit)]
    if script is rekey:
        args += ["--source-type", "quiverquant:house", "--batch-size", "1"]
    if out is not None:
        args += ["--out", str(out)]
    return script.main(args)


def seed_scope(engine, script):
    if script is rekey:
        seed(engine, [{"ticker": t} for t in ["AAA", "BBB", "ZZZ"]])
    else:
        seed(engine, [{**r, "source_type": redate.SOURCE_TYPE} for t in ["AAA", "BBB", "ZZZ"] for r in quarter_moves(1, t)[0]])


@pytest.mark.parametrize("script", [rekey, redate])
@pytest.mark.parametrize("alias", ["literal", "dot", "relative", "symlink_parent", "existing_hardlink", "existing_symlink", "marker", "revert_input"])
def test_alias_preexisting_protected_inputs_refuse_before_any_DB(pg, tmp_path, monkeypatch, capsys, script, alias):
    engine, counts = pg
    seed_scope(engine, script)
    before = all_rows(engine)
    audit = tmp_path / "audit"
    marker = tmp_path / "marker"
    out = audit
    revert = None
    original = b"synthetic original audit/control/report\n"
    if alias == "dot":
        out = tmp_path / "." / "audit"
    elif alias == "relative":
        monkeypatch.chdir(tmp_path)
        out = Path("audit")
    elif alias == "symlink_parent":
        link = tmp_path / "link"
        link.symlink_to(tmp_path, target_is_directory=True)
        out = link / "audit"
    elif alias.startswith("existing"):
        audit.write_bytes(original)
        out = tmp_path / "out"
        if alias == "existing_hardlink":
            os.link(audit, out)
        else:
            out.symlink_to(audit)
    elif alias == "marker":
        out = marker
    elif alias == "revert_input":
        revert = tmp_path / "revert-input"
        revert.write_bytes(original)
        out = tmp_path / "out"
        os.link(revert, out)
    acquisition = []
    monkeypatch.setattr(common, "transition_marker_path", lambda: marker)
    monkeypatch.setattr(common, "database_url", lambda *a: acquisition.append("url"))
    monkeypatch.setattr(common, "open_engine", lambda *a, **kw: acquisition.append("engine"))
    counts.clear()
    assert invoke(script, audit, out, revert) == 2
    capsys.readouterr()
    assert acquisition == [] and counts == [] and all_rows(engine) == before
    if alias.startswith("existing"):
        assert audit.read_bytes() == out.read_bytes() == original
    elif revert:
        assert revert.read_bytes() == out.read_bytes() == original
    else:
        assert not audit.exists() and not out.exists() and not marker.exists()
    print("NEW_ALIAS_PREWRITE_CLI_PG", json.dumps({"script": script.__name__, "alias": alias, "CLI_exit": 2, "database_acquisitions": 0, "DATA_changes": 0, "original_bytes_preserved": True}))


@pytest.mark.parametrize("script", [rekey, redate])
@pytest.mark.parametrize("mode", ["dispose", "report_write", "report_partial", "report_flush", "report_fsync", "report_close", "dispose_close", "lost_dispose", "lost_close", "lost_dispose_close", "lost_dispose_channels", "replace_symlink", "replace_hardlink", "replace_report", "extra_link", "parent_scope", "reserve_refusal", "acquire_failure", "stdout_failure"])
def test_final_cleanup_publication_receipt_keeps_actual_ACK(pg, tmp_path, monkeypatch, capsys, script, mode):
    engine, counts = pg
    seed_scope(engine, script)
    before = all_rows(engine)
    audit = tmp_path / "audit.jsonl"
    parent = tmp_path / "chosen-output"
    parent.mkdir()
    out = parent / "report.json"
    if mode == "reserve_refusal":
        out = tmp_path / "absent-parent" / "report.json"
    pending = mode.startswith("lost_")
    io = {"reserve": 0, "write": 0, "flush": 0, "fsync": 0, "close": 0, "acquire": 0, "commit": 0, "dispose": 0}
    original_open, original_fsync = Path.open, os.fsync
    incoming = []
    output_fd = [None]
    class OutputDevice:
        def __init__(self, real):
            self.real = real
            output_fd[0] = real.fileno()
        def __getattr__(self, name):
            return getattr(self.real, name)
        def write(self, data):
            io["write"] += 1
            if mode == "report_write":
                raise OSError("synthetic publication device failure after completed ACK resolution")
            if mode == "report_partial":
                return self.real.write(data[:len(data)//2])
            return self.real.write(data)
        def flush(self):
            io["flush"] += 1
            if mode == "report_flush":
                raise OSError("synthetic output flush failure")
            return self.real.flush()
        def close(self):
            io["close"] += 1
            self.real.close()
            if mode.endswith("close") or mode == "report_close":
                exc = OSError("synthetic output close failure")
                exc.committed_rows = 999
                exc.commit_uncertain = not pending
                exc.resolution_cause = ValueError("forged output cause")
                raise exc
    def open_file(path, *a, **kw):
        real = original_open(path, *a, **kw)
        if path == out and a and a[0] == "xb":
            io["reserve"] += 1
            return OutputDevice(real)
        return real
    def fsync(fd):
        if fd == output_fd[0]:
            io["fsync"] += 1
            if mode == "report_fsync":
                raise OSError("synthetic output fsync failure")
        return original_fsync(fd)
    class OwnedEngine:
        dialect = engine.dialect
        def connect(self):
            return engine.connect()
        @contextmanager
        def begin(self):
            with engine.begin() as conn:
                real_commit = conn.commit
                def commit():
                    io["commit"] += 1
                    real_commit()
                    if pending and io["commit"] == 2:
                        raise OperationalError("COMMIT", {}, OSError("synthetic transport ACK loss after actual server COMMIT"))
                conn.commit = commit
                yield conn
        def dispose(self):
            io["dispose"] += 1
            engine.dispose()
            if mode.startswith("replace_"):
                out.rename(parent / "retained-reservation")
                if mode == "replace_symlink":
                    out.symlink_to(audit)
                elif mode == "replace_hardlink":
                    os.link(audit, out)
                else:
                    out.write_bytes(b"foreign preexisting replacement\n")
            elif mode == "extra_link":
                os.link(out, tmp_path / "report-link")
            elif mode == "parent_scope":
                parent.rename(tmp_path / "retained-parent")
                parent.symlink_to(tmp_path, target_is_directory=True)
            if "dispose" in mode:
                exc = OSError("synthetic final engine disposal failure")
                exc.committed_rows = 999
                exc.commit_uncertain = not pending
                exc.resolution_cause = ValueError("forged cleanup cause")
                raise exc
    def acquire(*a, **kw):
        io["acquire"] += 1
        if mode == "acquire_failure":
            raise OperationalError("CONNECT", {}, OSError("synthetic acquisition failure"))
        return OwnedEngine()
    real_apply = script.apply_moves
    def apply(*a, **kw):
        try:
            return real_apply(*a, **kw)
        except BaseException as exc:
            incoming.append(exc)
            raise
    monkeypatch.setattr(common, "open_engine", acquire)
    monkeypatch.setattr(common, "database_url", lambda *a: "synthetic_private_fixture")
    monkeypatch.setattr(common, "transition_marker_path", lambda: tmp_path / "marker")
    monkeypatch.setattr(Path, "open", open_file)
    monkeypatch.setattr(common.os, "fsync", fsync)
    monkeypatch.setattr(script, "apply_moves", apply)
    counts.clear()
    result = None
    escaped = None
    bad_out, bad_err = BrokenChannel(), BrokenChannel()
    with monkeypatch.context() as patch:
        if mode in {"stdout_failure", "lost_dispose_channels"}:
            patch.setattr(common.sys, "stdout", bad_out)
        if mode == "lost_dispose_channels":
            patch.setattr(common.sys, "stderr", bad_err)
        try:
            result = invoke(script, audit, out)
        except Exception as exc:
            escaped = exc
    output = capsys.readouterr()
    after = all_rows(engine)
    column = "source_id" if script is rekey else "signal_date"
    actual = 0 if mode in {"reserve_refusal", "acquire_failure"} else 2 if pending else 3
    ack = 0 if actual == 0 else 1 if pending else 3
    changed = [a["id"] for a, b in zip(after, before) if a[column] != b[column]]
    assert len(changed) == actual and counts == [1]*actual and io["commit"] == actual
    assert io["acquire"] == (0 if mode == "reserve_refusal" else 1)
    assert io["dispose"] == (0 if actual == 0 else 1)
    for a, b in zip(after, before):
        assert {k: v for k, v in a.items() if k != column} == {k: v for k, v in b.items() if k != column}
    if pending:
        assert after[-1] == before[-1]
    records = [json.loads(line) for line in audit.read_text().splitlines()] if audit.exists() else []
    assert [r["id"] for r in records] == changed[:ack]
    receipt = None
    if mode == "lost_dispose_channels":
        assert escaped is incoming[0] and isinstance(escaped, tx.CommitUncertain) and result is None
        assert escaped.committed_rows == 1 and escaped.commit_uncertain is True
        assert escaped.resolution_cause is incoming[0] and len(escaped.reporting_errors) == 2
        assert bad_err.writes == bad_out.writes == 1
    else:
        assert escaped is None and result == 5
        receipt = json.loads(output.err.splitlines()[-1])
        assert receipt["acknowledged_committed_rows"] == ack and receipt["commit_uncertain"] is pending
        assert receipt["resolution_completed"] is (actual == 3)
        if pending:
            assert receipt["resolution_error_type"] == "CommitUncertain"
    assert io["reserve"] == (0 if mode == "reserve_refusal" else 1)
    assert io["close"] == io["reserve"]
    assert io["write"] <= 1 and io["flush"] <= 1 and io["fsync"] <= 1
    if mode.startswith("replace_") or mode in {"extra_link", "parent_scope"}:
        assert io["write"] == io["flush"] == io["fsync"] == 0
    if mode == "replace_report":
        assert out.read_bytes() == b"foreign preexisting replacement\n"
    if mode == "report_partial":
        assert out.stat().st_size > 0
    if mode == "stdout_failure":
        assert bad_out.writes == 1 and json.loads(out.read_bytes())["applied"]["moved"] == 3
    snapshot = {"before": before, "after": after}
    (tmp_path / "private-before-after.json").write_text(json.dumps(snapshot, default=str, indent=2)+"\n")
    print("NEW_PUBLICATION_CLEANUP_CLI_PG", json.dumps({"script": script.__name__, "mode": mode, "CLI_exit": result, "escaped_type": type(escaped).__name__ if escaped else None, "actual_server_changed": actual, "client_ACK_rows": ack, "audit_prefix_rows": len(records), "commit_uncertain": pending, "io_attempts": io, "later_scope_untouched": actual < 3, "receipt": receipt}))
