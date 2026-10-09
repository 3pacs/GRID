"""An audit context cannot overwrite or suppress a pending resolution."""
from pathlib import Path

import pytest

from ingestion.altdata import quiverquant_transactions as tx
from scripts import qq_transition_common as common


@pytest.mark.parametrize("failure", [None, ValueError("known rejection"),
                                     tx.CommitUncertain("lost ACK"),
                                     tx.CommitAcknowledgedCleanupError("ACK cleanup")])
@pytest.mark.parametrize("suppress", [False, True])
def test_real_audit_context_close_preserves_resolution(tmp_path, monkeypatch, failure, suppress):
    audit_path = tmp_path / "audit"
    real_open = Path.open
    calls = []

    class ExitFailure:
        def __init__(self, real):
            self.real = real

        def __enter__(self):
            return self.real.__enter__()

        def __exit__(self, kind, value, tb):
            calls.append(value)
            self.real.__exit__(kind, value, tb)
            if suppress:
                return True
            raise OSError("close failed")

    monkeypatch.setattr(Path, "open", lambda path, *args, **kwargs: ExitFailure(real_open(path, *args, **kwargs)))
    if suppress and failure is None:
        with common.audit_context(audit_path, acknowledged_rows=lambda: 3):
            pass
    else:
        with pytest.raises(type(failure) if suppress else OSError) as caught:
            with common.audit_context(audit_path, acknowledged_rows=lambda: 3):
                if failure is not None:
                    raise failure
        assert caught.value.committed_rows == 3
        assert caught.value.commit_uncertain is isinstance(failure, tx.CommitUncertain)
        assert caught.value.resolution_cause is failure
        if not suppress and failure is not None:
            assert caught.value.__cause__ is failure
    assert calls == [failure]
