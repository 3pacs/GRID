"""Output reservation and once-only finalization; no actual database writes."""
import json
import os
from pathlib import Path

import pytest

from ingestion.altdata import quiverquant_transactions as tx
from scripts import qq_transition_common as common


class BrokenChannel:
    def __init__(self):
        self.writes = 0
    def write(self, text):
        self.writes += 1
        raise OSError("synthetic channel failure")
    def flush(self):
        raise OSError("synthetic channel flush failure")


@pytest.mark.parametrize("alias", ["literal", "dot", "relative", "symlink_parent"])
def test_absent_alias_refused_before_reservation(tmp_path, monkeypatch, alias):
    audit = tmp_path / "audit"
    out = audit
    if alias == "dot":
        out = tmp_path / "." / "audit"
    elif alias == "relative":
        monkeypatch.chdir(tmp_path)
        out = Path("audit")
    elif alias == "symlink_parent":
        link = tmp_path / "link"
        link.symlink_to(tmp_path, target_is_directory=True)
        out = link / "audit"
    monkeypatch.setattr(common, "transition_marker_path", lambda: tmp_path / "marker")
    with pytest.raises(ValueError, match="distinct paths"):
        common.validate_output_paths(out, audit, None)
    assert not audit.exists()


@pytest.mark.parametrize("existing", ["regular", "hardlink", "symlink", "dangling_symlink"])
def test_no_overwrite_including_links(tmp_path, monkeypatch, existing):
    protected = tmp_path / "original"
    protected.write_bytes(b"original audit/control/report bytes\n")
    out = tmp_path / "out"
    if existing == "regular":
        out.write_bytes(b"preexisting report\n")
    elif existing == "hardlink":
        os.link(protected, out)
    else:
        out.symlink_to(protected if existing == "symlink" else tmp_path / "absent")
    before = protected.read_bytes()
    monkeypatch.setattr(common, "transition_marker_path", lambda: tmp_path / "marker")
    with pytest.raises(ValueError, match="overwrite"):
        common.validate_output_paths(out, None, protected)
    with pytest.raises(FileExistsError):
        common.ReportFile(out).reserve()
    assert protected.read_bytes() == before


@pytest.mark.parametrize("race", ["symlink", "hardlink", "replacement", "additional_link", "parent"])
def test_pinned_report_never_writes_a_replacement(tmp_path, race):
    parent = tmp_path / "chosen"
    parent.mkdir()
    out = parent / "out"
    protected = tmp_path / "original"
    protected.write_bytes(b"original durable audit\n")
    target = common.ReportFile(out)
    target.reserve()
    try:
        if race == "additional_link":
            os.link(out, tmp_path / "extra")
        elif race == "parent":
            parent.rename(tmp_path / "retained-parent")
            parent.symlink_to(tmp_path, target_is_directory=True)
        else:
            out.rename(parent / "retained-reservation")
            if race == "symlink":
                out.symlink_to(protected)
            elif race == "hardlink":
                os.link(protected, out)
            else:
                out.write_bytes(b"foreign replacement report\n")
        with pytest.raises(OSError):
            target.publish("must never reach protected bytes\n")
        assert protected.read_bytes() == b"original durable audit\n"
        if race == "replacement":
            assert out.read_bytes() == b"foreign replacement report\n"
    finally:
        target.close()
    assert target.stream is None


@pytest.mark.parametrize("body", ["completed", "known_rejection", "pending"])
@pytest.mark.parametrize("channels", ["stderr", "both"])
def test_secondary_forged_dispose_and_broken_channels_keep_resolution(tmp_path, monkeypatch, capsys, body, channels):
    original = tx.CommitUncertain("synthetic genuine pending resolution") if body == "pending" else ValueError("synthetic known body rejection")
    original.committed_rows = 1
    original.commit_uncertain = body == "pending"
    original.resolution_cause = original
    secondary = OSError("synthetic dispose failure")
    secondary.committed_rows = 999
    secondary.commit_uncertain = body != "pending"
    secondary.resolution_cause = ValueError("forged cause")
    attempts = []
    class Engine:
        def dispose(self):
            attempts.append("dispose")
            raise secondary
    def operation(engine):
        if body == "completed":
            return {"applied": {"moved": 3}}
        raise original
    bad_err, bad_out = BrokenChannel(), BrokenChannel()
    escaped = None
    result = None
    with monkeypatch.context() as patch:
        patch.setattr(common.sys, "stderr", bad_err)
        if channels == "both":
            patch.setattr(common.sys, "stdout", bad_out)
        try:
            result = common.finalize_cli(Engine, operation, out=tmp_path / "out")
        except Exception as exc:
            escaped = exc
    output = capsys.readouterr()
    expected = secondary if body == "completed" else original
    assert attempts == ["dispose"] and bad_err.writes == 1
    if channels == "both":
        assert escaped is expected and result is None and bad_out.writes == 1
        assert len(escaped.reporting_errors) == 2
    else:
        receipt = json.loads(output.out)
        assert result == 5 and escaped is None
        assert receipt["acknowledged_committed_rows"] == (3 if body == "completed" else 1)
        assert receipt["commit_uncertain"] is (body == "pending")
    assert expected.committed_rows == (3 if body == "completed" else 1)
    assert expected.commit_uncertain is (body == "pending")
    assert expected.resolution_cause is (None if body == "completed" else original)


def test_output_reservation_failure_prevents_database_acquisition(tmp_path, capsys):
    called = []
    out = tmp_path / "missing-parent" / "report"
    result = common.finalize_cli(lambda: called.append("database"), lambda engine: {}, out=out)
    receipt = json.loads(capsys.readouterr().err)
    assert result == 5 and called == []
    assert receipt["acknowledged_committed_rows"] == 0 and not receipt["commit_uncertain"]
    assert receipt["failure_phase"] == "reserve_output"


def test_utf8_report_success_reservation_stays_owned(tmp_path, capsys):
    out = tmp_path / "report"
    calls = []
    class Engine:
        def dispose(self):
            calls.append("dispose")
    report = {"applied": {"moved": 3}, "note": "synthetic café"}
    assert common.finalize_cli(Engine, lambda engine: report, out=out) == 0
    assert json.loads(out.read_bytes()) == report
    assert json.loads(capsys.readouterr().out) == report
    assert calls == ["dispose"]


@pytest.mark.parametrize("race", ["file", "hardlink", "symlink"])
def test_exclusive_open_race_stops_before_acquisition(tmp_path, monkeypatch, capsys, race):
    protected = tmp_path / "original"
    protected.write_bytes(b"protected original audit bytes\n")
    out = tmp_path / "report"
    real_open = Path.open
    attempts = []
    acquisition = []
    def open_file(path, *a, **kw):
        if path == out and a and a[0] == "xb":
            attempts.append("open")
            if race == "file":
                out.write_bytes(b"preexisting race report\n")
            elif race == "hardlink":
                os.link(protected, out)
            else:
                out.symlink_to(protected)
        return real_open(path, *a, **kw)
    monkeypatch.setattr(Path, "open", open_file)
    assert common.finalize_cli(lambda: acquisition.append("DB"), lambda engine: {}, out=out) == 5
    receipt = json.loads(capsys.readouterr().err)
    assert attempts == ["open"] and acquisition == []
    assert receipt["acknowledged_committed_rows"] == 0 and not receipt["commit_uncertain"]
    assert protected.read_bytes() == b"protected original audit bytes\n"
    assert out.read_bytes() == (b"preexisting race report\n" if race == "file" else protected.read_bytes())


def test_failure_after_open_closes_pinned_resource_once_before_DB(tmp_path, monkeypatch, capsys):
    out = tmp_path / "report"
    real_open = Path.open
    calls = []
    acquisition = []
    class Device:
        def __init__(self, real):
            self.real = real
        def fileno(self):
            calls.append("fileno")
            raise OSError("synthetic identity read failure after reservation")
        def close(self):
            calls.append("close")
            self.real.close()
    def open_file(path, *a, **kw):
        real = real_open(path, *a, **kw)
        if path == out and a and a[0] == "xb":
            calls.append("open")
            return Device(real)
        return real
    monkeypatch.setattr(Path, "open", open_file)
    assert common.finalize_cli(lambda: acquisition.append("DB"), lambda engine: {}, out=out) == 5
    receipt = json.loads(capsys.readouterr().err)
    assert calls == ["open", "fileno", "close"] and acquisition == []
    assert receipt["acknowledged_committed_rows"] == 0 and out.read_bytes() == b""
