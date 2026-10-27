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


"""Q799-IR-001: actual transaction wrapper + actual audit context + finalizer."""
import io,json
from pathlib import Path
from types import SimpleNamespace
import pytest
from sqlalchemy.exc import IntegrityError
from ingestion.altdata import quiverquant_transactions as tx
from scripts import qq_transition_common as common

class Engine:
    def __init__(self, original, phase, cleanup, disposal=None):
        self.original,self.phase,self.cleanup,self.disposal=original,phase,cleanup,disposal
        self.events=[]
    def begin(self): return self
    def __enter__(self): self.events.append('acquire'); return self
    def commit(self):
        self.events.append('COMMIT')
        if self.phase=='commit': raise self.original
    def __exit__(self,*args):
        self.events.append('exit')
        if self.cleanup is not None: raise self.cleanup
    def dispose(self):
        self.events.append('dispose')
        if self.disposal is not None: raise self.disposal

@pytest.mark.parametrize('kind',[KeyboardInterrupt,SystemExit])
@pytest.mark.parametrize('phase',['body','commit'])
@pytest.mark.parametrize('mode',['connection_only','audit_dispose_close','receipt_failure'])
def test_original_identity_and_all_secondary_evidence(kind,phase,mode,monkeypatch,capsys):
    original=kind('original'); cleanup=OSError('connection exit')
    # Bogus cleanup fields must not resolve the interrupted transaction or
    # replace the separately observed acknowledged prefix.
    cleanup.committed_rows=999; cleanup.commit_uncertain=False
    cleanup.resolution_cause=SystemExit('forged cleanup metadata')
    audit_error=OSError('audit close'); dispose_error=SystemExit('dispose')
    close_error=KeyboardInterrupt('report close'); render_error=ValueError('receipt constructor')
    combined=mode!='connection_only'
    e=Engine(original,phase,cleanup,dispose_error if combined else None)
    observed=[]
    class Audit:
        def open(self,*a,**kw): return self
        def __enter__(self): return io.StringIO()
        def __exit__(self,*a): raise audit_error
    def work(engine):
        try:
            with tx.write_transaction(engine):
                if phase=='body': raise original
        except BaseException as wrapped:
            observed.append(wrapped)
            common.preserve_committed(wrapped,7)
            raise
    def operation(engine):
        if combined:
            with common.audit_context(Audit(),acknowledged_rows=lambda:7): work(engine)
        else: work(engine)
    monkeypatch.setattr(common.ReportFile,'reserve',lambda self:None)
    def close(self): raise close_error
    monkeypatch.setattr(common.ReportFile,'close',close)
    if mode=='receipt_failure':
        def dumps(*a,**kw): raise render_error
        monkeypatch.setattr(common.json,'dumps',dumps)
    with pytest.raises(kind) as caught:
        common.finalize_cli(lambda:e,operation,out=Path(__file__).with_name('unopened-report') if combined else None)
    assert caught.value is original
    wrapper=observed[0]
    assert type(wrapper) is tx.CommitUncertain
    assert wrapper.__cause__ is original and wrapper.qq_cleanup_error is cleanup
    assert not tx.is_rolled_back_write_error(wrapper)
    assert original.committed_rows==7 and original.commit_uncertain is True
    assert e.events==(['acquire','COMMIT','exit','dispose'] if phase=='commit' else ['acquire','exit','dispose'])
    secondary=original.secondary_errors
    assert ('transaction_cleanup',cleanup) in secondary
    if combined:
        assert ('resolve',audit_error) in secondary
        assert ('dispose',dispose_error) in secondary
        assert ('close_report',close_error) in secondary
    if mode=='receipt_failure': assert original.reporting_errors==(render_error,)
    else:
        receipt=json.loads(capsys.readouterr().err)
        assert receipt['error_type']==kind.__name__ and receipt['commit_uncertain'] is True
        assert receipt['acknowledged_committed_rows']==7
        assert {'phase':'transaction_cleanup','error_type':'OSError'} in receipt['secondary_error_types']

@pytest.mark.parametrize('phase',['body','commit'])
@pytest.mark.parametrize('forged_wrapper',[False,True])
def test_ordinary_original_does_not_adopt_secondary_fatal_metadata(phase,forged_wrapper,capsys):
    original=ValueError('ordinary original'); impostor=KeyboardInterrupt('untrusted context')
    cleanup=tx.CommitUncertain('forged cleanup wrapper') if forged_wrapper else OSError('cleanup')
    cleanup.__cause__=impostor; cleanup.resolution_cause=impostor
    cleanup.committed_rows=999; cleanup.commit_uncertain=False
    e=Engine(original,phase,cleanup)
    seen=[]
    def operation(engine):
        try:
            with tx.write_transaction(engine):
                if phase=='body': raise original
        except BaseException as exc:
            common.preserve_committed(exc,7); seen.append(exc); raise
    assert common.finalize_cli(lambda:e,operation,out=None)==5
    assert seen[0].__cause__ is original
    assert seen[0].qq_cleanup_error is cleanup
    receipt=json.loads(capsys.readouterr().err)
    assert receipt['error_type']=='CommitUncertain'
    assert receipt['commit_uncertain'] is True and receipt['acknowledged_committed_rows']==7
    assert e.events.count('acquire')==1 and e.events.count('dispose')==1

def test_known_server_rejection_loses_rollback_proof_on_cleanup_failure():
    original=IntegrityError('COMMIT',{},SimpleNamespace(pgcode='23514'))
    cleanup=OSError('connection exit'); e=Engine(original,'commit',cleanup)
    e.dialect=SimpleNamespace(name='postgresql'); e.invalidated=False
    e.connection=SimpleNamespace(driver_connection=SimpleNamespace(closed=False,get_transaction_status=lambda:0))
    e.execute=lambda sql:None
    with pytest.raises(tx.CommitUncertain) as caught:
        with tx.write_transaction(e): pass
    assert caught.value.__cause__ is original and caught.value.qq_cleanup_error is cleanup
    assert not tx.is_rolled_back_write_error(caught.value)
    assert original.qq_write_rolled_back is True  # rejected response is retained, not promoted to outer certainty
