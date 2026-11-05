"""COMMIT acknowledgment and connection cleanup are separate failure phases."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from ingestion.altdata import quiverquant as qq
from ingestion.altdata import quiverquant_transactions as tx
from tests.test_qq_short_transactions import records


@pytest.mark.parametrize("code", ["23514", "57014"])
def test_cleanup_server_rejection_never_authorizes_replay(code):
    calls = []

    class Original(Exception):
        pgcode = code

    class Driver:
        closed = False

        def get_transaction_status(self):
            return 0

    class Engine:
        @contextmanager
        def begin(self):
            calls.append("acquisition")
            yield SimpleNamespace(
                dialect=SimpleNamespace(name="postgresql"),
                connection=SimpleNamespace(driver_connection=Driver()),
                execute=lambda *args: None,
                commit=lambda: calls.append("acknowledged"),
            )
            raise OperationalError("CHECKIN", {}, Original("synthetic cleanup response"))

    with pytest.raises(qq.QuiverStoreAborted) as caught:
        qq._store_signals(Engine(), records(3), "quiverquant:lobbying", "lobbying")
    assert (caught.value.stored, caught.value.failed, caught.value.commit_uncertain) == (3, 0, False)
    assert isinstance(caught.value.__cause__, tx.CommitAcknowledgedCleanupError)
    assert calls == ["acquisition", "acknowledged"]


def test_cleanup_replacing_rejected_commit_proof_stops_without_fallback():
    class Original(Exception):
        pgcode = "23514"

    class Driver:
        closed = False

        def get_transaction_status(self):
            return 0

    error = OperationalError("COMMIT", {}, Original("synthetic rejected commit"))

    def commit():
        raise error

    class Engine:
        @contextmanager
        def begin(self):
            try:
                yield SimpleNamespace(
                    dialect=SimpleNamespace(name="postgresql"),
                    connection=SimpleNamespace(driver_connection=Driver()),
                    invalidated=False, execute=lambda *args: None, commit=commit,
                )
            except Exception as exc:
                raise OSError("synthetic rollback/checkin cleanup failure") from exc

    with pytest.raises(qq.QuiverStoreAborted) as caught:
        qq._store_signals(Engine(), records(3), "quiverquant:lobbying", "lobbying")
    assert (caught.value.stored, caught.value.failed, caught.value.commit_uncertain) == (0, 0, True)
