"""Actual ordinary PG outer control-flow and secondary constructor regressions.

Only synthetic three-row scopes, each setup/apply transaction has one DATA row.
Post-driver losses below are synthetic; the unchanged wire witness is separate.
"""
from contextlib import contextmanager
import json
from pathlib import Path

import pytest

from scripts import qq_rekey_signal_sources as rekey, qq_gov_contracts_redate as redate, qq_transition_common as common
from sqlalchemy.exc import OperationalError
from tests.test_qq_short_transactions_pg import all_rows, pg
from tests.test_qq_publication_cleanup_pg import invoke, seed_scope


def install(pg, monkeypatch, script, mode):
    engine, counts = pg
    seed_scope(engine, script)
    before = all_rows(engine)
    attempts = {"commit": 0, "dispose": 0}

    class OwnedEngine:
        dialect = engine.dialect

        def connect(self):
            return engine.connect()

        def dispose(self):
            attempts["dispose"] += 1
            engine.dispose()

        @contextmanager
        def begin(self):
            with engine.begin() as conn:
                commit = conn.commit

                def resolution():
                    attempts["commit"] += 1
                    commit()
                    if mode == "suppress_lost_ack" and attempts["commit"] == 2:
                        raise OperationalError("COMMIT", {}, OSError("synthetic post-driver application ACK loss"))

                conn.commit = resolution
                yield conn

    monkeypatch.setattr(common, "open_engine", lambda *args, **kwargs: OwnedEngine())
    monkeypatch.setattr(common, "database_url", lambda *args: "synthetic_private_fixture")
    return engine, counts, before, attempts


def snapshot(engine, before, script, tmp_path):
    after = all_rows(engine)
    column = "source_id" if script is rekey else "signal_date"
    changed = [a["id"] for a, b in zip(after, before) if a[column] != b[column]]
    assert len(after) == len(before) == 3
    for a, b in zip(after, before):
        assert {k: v for k, v in a.items() if k != column} == {k: v for k, v in b.items() if k != column}
    (tmp_path / "private-before-after.json").write_text(json.dumps({"before": before, "after": after}, default=str, indent=2) + "\n")
    return after, changed


@pytest.mark.parametrize("script", [rekey, redate])
@pytest.mark.parametrize("kind", [KeyboardInterrupt, SystemExit])
@pytest.mark.parametrize("phase", ["acquire", "body_zero", "body_ack", "body_ack_audit_replace", "audit_exit", "dispose", "close", "render", "write", "pending_commit", "pending_cleanup"])
def test_actual_cli_original_control_flow_once_only_cleanup(pg, tmp_path, monkeypatch, capsys, script, kind, phase):
    engine, counts, before, attempts = install(pg, monkeypatch, script, "normal")
    audit, out = tmp_path / "audit", tmp_path / "out"
    interrupt = kind("synthetic original interruption")
    io = {"close": 0, "publish": 0, "render": 0}
    real_open, real_append, real_dumps = Path.open, common.append_audit, json.dumps
    acquired = common.open_engine
    real_apply = script.apply_moves
    devices = []

    def acquire(*args, **kwargs):
        if phase == "acquire":
            interrupt.committed_rows, interrupt.commit_uncertain = 999, True
            raise interrupt
        owned = acquired(*args, **kwargs)
        dispose, begin = owned.dispose, owned.begin

        def disposal():
            dispose()
            if phase == "dispose":
                interrupt.committed_rows, interrupt.commit_uncertain = 999, True
                raise interrupt
            if phase == "pending_cleanup":
                raise OSError("secondary engine cleanup")

        @contextmanager
        def transaction():
            with begin() as conn:
                commit = conn.commit

                def resolution():
                    commit()
                    if phase in {"pending_commit", "pending_cleanup"} and attempts["commit"] == 2:
                        raise interrupt

                conn.commit = resolution
                yield conn

        owned.dispose, owned.begin = disposal, transaction
        return owned

    class Device:
        def __init__(self, real, is_report):
            self.real, self.is_report = real, is_report

        def __getattr__(self, name):
            return getattr(self.real, name)

        def __enter__(self):
            self.real.__enter__()
            return self

        def __exit__(self, *args):
            result = self.real.__exit__(*args)
            if phase == "audit_exit":
                raise interrupt
            if phase == "body_ack_audit_replace":
                raise KeyboardInterrupt("secondary audit cleanup interrupt")
            return result

        def write(self, raw):
            if self.is_report:
                io["publish"] += 1
            if phase == "write":
                raise interrupt
            return self.real.write(raw)

        def close(self):
            io["close"] += 1
            self.real.close()
            if phase == "close":
                raise interrupt
            if phase == "pending_cleanup":
                raise SystemExit("secondary report cleanup")

    def open_file(path, *args, **kwargs):
        real = real_open(path, *args, **kwargs)
        if path == out and args and args[0] == "xb":
            device = Device(real, True)
            devices.append(device)
            return device
        if path == audit and phase in {"audit_exit", "body_ack_audit_replace"} and args and args[0] == "x":
            return Device(real, False)
        return real

    def append(stream, records):
        real_append(stream, records)
        if phase in {"body_ack", "body_ack_audit_replace"} and attempts["commit"] == 3:
            raise interrupt

    def apply(*args, **kwargs):
        if phase == "body_zero":
            raise interrupt
        return real_apply(*args, **kwargs)

    def dumps(obj, *args, **kwargs):
        if isinstance(obj, dict) and "applied" in obj:
            io["render"] += 1
            if phase == "render":
                raise interrupt
        return real_dumps(obj, *args, **kwargs)

    monkeypatch.setattr(common, "transition_marker_path", lambda: tmp_path / "marker")
    monkeypatch.setattr(common, "open_engine", acquire)
    monkeypatch.setattr(Path, "open", open_file)
    monkeypatch.setattr(common, "append_audit", append)
    monkeypatch.setattr(common.json, "dumps", dumps)
    monkeypatch.setattr(script, "apply_moves", apply)
    counts.clear()
    with pytest.raises(kind) as caught:
        invoke(script, audit, out)
    captured = capsys.readouterr()
    after, changed = snapshot(engine, before, script, tmp_path)
    pending = phase.startswith("pending")
    ack = 0 if phase in {"acquire", "body_zero"} else 1 if pending else 3
    actual = 2 if pending else ack
    assert caught.value is interrupt
    assert (interrupt.committed_rows, interrupt.commit_uncertain) == (ack, pending)
    assert attempts == {"commit": actual, "dispose": 0 if phase == "acquire" else 1}
    assert io["close"] == 1 and devices[0].real.closed
    assert io["render"] <= 1 and io["publish"] <= 1
    assert len(changed) == actual and counts == [1] * actual
    assert (len(audit.read_text().splitlines()) if audit.exists() else 0) == ack
    if actual < 3:
        assert after[-1] == before[-1]
    receipt = json.loads(captured.err)
    assert receipt["acknowledged_committed_rows"] == ack and receipt["commit_uncertain"] is pending
    witness = {"script": script.__name__, "phase": phase, "control_type": kind.__name__, "same_original": True, "application_ACK": ack, "actual_server_rows": actual, "uncertain": pending, "attempts": attempts, "IO": io}
    (tmp_path / "new-control-safe-witness.json").write_text(real_dumps(witness, indent=2) + "\n")
    print("NEW_CONTROL_PG", real_dumps(witness))


@pytest.mark.parametrize("script", [rekey, redate])
@pytest.mark.parametrize("phase", ["body", "render", "write", "pending"])
def test_secondary_constructor_combined_cleanup_preserves_original_actual_resolution(pg, tmp_path, monkeypatch, capsys, script, phase):
    engine, counts, before, attempts = install(pg, monkeypatch, script, "suppress_lost_ack" if phase == "pending" else "normal")
    audit, out = tmp_path / "audit", tmp_path / "out"
    original, secondary = [], OSError("secondary fatal receipt constructor")
    io = {"close": 0, "receipt": 0}
    real_open, real_apply, real_append, real_dumps = Path.open, script.apply_moves, common.append_audit, json.dumps
    acquired = common.open_engine

    def acquire(*args, **kwargs):
        owned = acquired(*args, **kwargs)
        disposal = owned.dispose

        def dispose():
            disposal()
            raise SystemExit("secondary disposal with forged fields")

        owned.dispose = dispose
        return owned

    class Device:
        def __init__(self, real):
            self.real = real

        def __getattr__(self, name):
            return getattr(self.real, name)

        def write(self, raw):
            if phase == "write":
                exc = OSError("original pinned write")
                original.append(exc)
                raise exc
            return self.real.write(raw)

        def close(self):
            io["close"] += 1
            self.real.close()
            raise KeyboardInterrupt("secondary descriptor cleanup")

    def open_file(path, *args, **kwargs):
        real = real_open(path, *args, **kwargs)
        return Device(real) if path == out and args and args[0] == "xb" else real

    def append(stream, records):
        real_append(stream, records)
        if phase == "body" and attempts["commit"] == 3:
            exc = OSError("original after acknowledged audit")
            original.append(exc)
            raise exc

    def apply(*args, **kwargs):
        try:
            return real_apply(*args, **kwargs)
        except BaseException as exc:
            if phase == "pending":
                original.append(exc)
            raise

    def dumps(obj, *args, **kwargs):
        if isinstance(obj, dict) and "applied" in obj and phase == "render":
            exc = OSError("original final report rendering")
            original.append(exc)
            raise exc
        if isinstance(obj, dict) and obj.get("status") == "ABORTED":
            io["receipt"] += 1
            raise secondary
        return real_dumps(obj, *args, **kwargs)

    # For render/write, disposal must happen after the first publication error
    # is impossible in this lifecycle; exercise those phases without disposal
    # failure, retaining a combined close + receipt-construction failure.
    if phase in {"body", "pending"}:
        monkeypatch.setattr(common, "open_engine", acquire)
    monkeypatch.setattr(common, "transition_marker_path", lambda: tmp_path / "marker")
    monkeypatch.setattr(Path, "open", open_file)
    monkeypatch.setattr(common, "append_audit", append)
    monkeypatch.setattr(common.json, "dumps", dumps)
    monkeypatch.setattr(script, "apply_moves", apply)
    counts.clear()
    with pytest.raises(BaseException) as caught:
        invoke(script, audit, out)
    capsys.readouterr()
    after, changed = snapshot(engine, before, script, tmp_path)
    pending = phase == "pending"
    ack, actual = (1, 2) if pending else (3, 3)
    assert caught.value is original[0]
    assert (caught.value.committed_rows, caught.value.commit_uncertain) == (ack, pending)
    assert caught.value.reporting_errors == (secondary,)
    assert attempts == {"commit": actual, "dispose": 1} and io == {"close": 1, "receipt": 1}
    assert len(changed) == actual and counts == [1] * actual
    assert len(audit.read_text().splitlines()) == ack
    if pending:
        assert after[-1] == before[-1]
    witness = {"script": script.__name__, "phase": phase, "same_original": True, "application_ACK": ack, "actual_server_rows": actual, "uncertain": pending, "attempts": attempts, "IO": io}
    (tmp_path / "new-secondary-safe-witness.json").write_text(real_dumps(witness, indent=2) + "\n")
    print("NEW_SECONDARY_PG", real_dumps(witness))
