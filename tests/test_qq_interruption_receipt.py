"""Outer lifetime regressions: trusted resolution, control flow and no I/O retry."""
import json
from pathlib import Path

import pytest

from ingestion.altdata import quiverquant_transactions as tx
from scripts import qq_transition_common as common


@pytest.mark.parametrize("kind", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("phase", ["reserve", "acquire", "body_zero", "body_ack", "pending", "dispose", "render", "write", "close", "stdout"])
def test_interrupt_identity_resolution_and_once_only_cleanup(monkeypatch, tmp_path, capsys, kind, phase):
    interrupt = kind("synthetic original control flow")
    counts = {"acquire": 0, "dispose": 0, "close": 0, "render": 0, "write": 0}
    original_dumps = json.dumps
    original_print = print

    class Engine:
        def dispose(self):
            counts["dispose"] += 1
            if phase == "dispose":
                interrupt.committed_rows = 999
                interrupt.commit_uncertain = True
                raise interrupt

    def reserve(self):
        if phase == "reserve":
            raise interrupt

    def acquire():
        counts["acquire"] += 1
        if phase == "acquire":
            raise interrupt
        return Engine()

    def body(engine):
        if phase in {"body_zero", "body_ack", "pending"}:
            if phase == "pending":
                wrapper = tx.CommitUncertain("synthetic pending COMMIT")
                wrapper.committed_rows = 1
                wrapper.commit_uncertain = True
                wrapper.resolution_cause = wrapper
                raise wrapper from interrupt
            interrupt.committed_rows = 3 if phase == "body_ack" else 0
            interrupt.commit_uncertain = False
            raise interrupt
        return {"applied": {"moved": 3}}

    def close(self):
        counts["close"] += 1
        if phase == "close":
            raise interrupt

    def publish(self, text):
        counts["write"] += 1
        if phase == "write":
            raise interrupt

    def dumps(obj, **kwargs):
        if "applied" in obj:
            counts["render"] += 1
            if phase == "render":
                raise interrupt
        return original_dumps(obj, **kwargs)

    def output(*args, **kwargs):
        if phase == "stdout" and not kwargs.get("file"):
            raise interrupt
        return original_print(*args, **kwargs)

    monkeypatch.setattr(common.ReportFile, "reserve", reserve)
    monkeypatch.setattr(common.ReportFile, "publish", publish)
    monkeypatch.setattr(common.ReportFile, "close", close)
    monkeypatch.setattr(common.json, "dumps", dumps)
    monkeypatch.setattr("builtins.print", output)
    with pytest.raises(kind) as caught:
        common.finalize_cli(acquire, body, out=tmp_path / "out")
    assert caught.value is interrupt
    ack = 0 if phase in {"reserve", "acquire", "body_zero"} else 1 if phase == "pending" else 3
    assert (interrupt.committed_rows, interrupt.commit_uncertain) == (ack, phase == "pending")
    assert counts["close"] == 1
    assert counts["dispose"] == (0 if phase in {"reserve", "acquire"} else 1)
    assert counts["acquire"] == (0 if phase == "reserve" else 1)
    assert all(v <= 1 for v in counts.values())
    receipt = json.loads(capsys.readouterr().err)
    assert receipt["acknowledged_committed_rows"] == ack
    assert receipt["commit_uncertain"] is (phase == "pending")


@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("control", [False, True])
def test_secondary_constructor_and_both_cleanup_faults_keep_trusted_original(monkeypatch, tmp_path, pending, control):
    original = KeyboardInterrupt("original interrupt") if control else tx.CommitUncertain("pending") if pending else OSError("original body")
    original.committed_rows = 1 if pending else 3
    original.commit_uncertain = pending
    original.resolution_cause = original
    calls = {"dispose": 0, "close": 0, "receipt": 0}
    secondary = OSError("secondary receipt constructor")

    class Engine:
        def dispose(self):
            calls["dispose"] += 1
            forged = SystemExit("secondary cleanup control")
            forged.committed_rows = 999
            forged.commit_uncertain = False
            raise forged

    def body(engine):
        raise original

    def close(self):
        calls["close"] += 1
        raise KeyboardInterrupt("secondary descriptor cleanup")

    def dumps(obj, **kwargs):
        calls["receipt"] += 1
        raise secondary

    monkeypatch.setattr(common.ReportFile, "reserve", lambda self: None)
    monkeypatch.setattr(common.ReportFile, "close", close)
    monkeypatch.setattr(common.json, "dumps", dumps)
    with pytest.raises(BaseException) as caught:
        common.finalize_cli(Engine, body, out=tmp_path / "out")
    assert caught.value is original
    assert (original.committed_rows, original.commit_uncertain) == (1 if pending else 3, pending)
    assert original.resolution_cause is original
    assert original.reporting_errors == (secondary,)
    assert calls == {"dispose": 1, "close": 1, "receipt": 1}
    assert len(original.secondary_errors) == 2


@pytest.mark.parametrize("kind", [KeyboardInterrupt, SystemExit])
def test_real_reserved_descriptor_cleanup_on_reservation_interrupt(monkeypatch, tmp_path, kind):
    original = kind("interrupt after actual descriptor reservation")
    acquired, streams = [], []
    real_open = Path.open

    def open_file(path, *args, **kwargs):
        stream = real_open(path, *args, **kwargs)
        streams.append(stream)
        return stream

    def check(self):
        raise original

    monkeypatch.setattr(Path, "open", open_file)
    monkeypatch.setattr(common.ReportFile, "check_identity", check)
    with pytest.raises(kind) as caught:
        common.finalize_cli(lambda: acquired.append(True), lambda engine: {}, out=tmp_path / "out")
    assert caught.value is original and not acquired
    assert len(streams) == 1 and streams[0].closed
    assert (original.committed_rows, original.commit_uncertain) == (0, False)


@pytest.mark.parametrize("kind", [KeyboardInterrupt, SystemExit])
def test_reporting_channel_interruption_cannot_replace_original(monkeypatch, kind):
    original = OSError("original acknowledged body failure")
    original.committed_rows, original.commit_uncertain = 3, False
    interrupt = kind("secondary diagnostic control flow")
    calls = []

    class Broken:
        def write(self, value):
            calls.append(value)
            raise interrupt

    class Engine:
        def dispose(self):
            pass

    def body(engine):
        raise original

    monkeypatch.setattr(common.sys, "stderr", Broken())
    with pytest.raises(OSError) as caught:
        common.finalize_cli(Engine, body, out=None)
    assert caught.value is original and calls and len(calls) == 1
    assert (original.committed_rows, original.commit_uncertain) == (3, False)
    assert original.reporting_errors == (interrupt,)
