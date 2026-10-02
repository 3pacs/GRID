"""Failure-phase and bounded trust propagation regressions; synthetic mocks only."""
from contextlib import contextmanager
from types import SimpleNamespace
from datetime import date

import pytest
from sqlalchemy.exc import OperationalError
from ingestion.altdata import quiverquant as qq
from ingestion.altdata import quiverquant_transactions as tx
from intelligence import trust_scorer as trust
from tests.test_qq_short_transactions import records


@pytest.mark.parametrize("phase", ["acquisition", "begin", "setup"])
def test_phase_failure_stops_once_without_row_fallback(phase):
    calls = [0]
    class Orig(Exception):
        pgcode = "28000" if phase == "acquisition" else "42883"
    class Engine:
        @contextmanager
        def begin(self):
            calls[0] += 1
            if phase in {"acquisition", "begin"}:
                raise OperationalError("CONNECT/BEGIN", {}, Orig("synthetic failure"))
            def execute(*args):
                raise OperationalError("SET LOCAL", {}, Orig("synthetic failure"))
            yield SimpleNamespace(dialect=SimpleNamespace(name="postgresql"),
                connection=SimpleNamespace(driver_connection=object()), execute=execute)
    with pytest.raises(qq.QuiverStoreAborted) as caught:
        qq._store_signals(Engine(), records(3), "quiverquant:lobbying", "lobbying")
    assert calls == [1] and (caught.value.stored, caught.value.failed) == (0, 0)
    assert not caught.value.commit_uncertain


@pytest.mark.parametrize("code,invalidated,idle,known", [
    ("23514", False, True, True), ("57014", False, True, True),
    ("23514", True, True, False), ("23514", False, False, False),
    ("08006", False, True, False), ("XX000", False, True, False),
])
def test_commit_state_requires_narrow_rejection_and_healthy_idle_driver(code, invalidated, idle, known):
    class Orig(Exception): pgcode = code
    class Driver:
        closed = False
        def get_transaction_status(self): return 0 if idle else 2
    error = OperationalError("COMMIT", {}, Orig("synthetic response"), connection_invalidated=invalidated)
    conn = SimpleNamespace(invalidated=invalidated)
    assert tx._known_commit_rejection(error, conn, Driver()) == known


@pytest.mark.parametrize("keyed", [True, False])
def test_trust_mock_pages_all_rows_without_changing_feed_statistics(monkeypatch, keyed):
    monkeypatch.setattr(trust, "_ensure_tables", lambda eng: None)
    source = "qq_house_trading" if keyed else "Jane Doe"
    st = "quiverquant:house" if keyed else "congressional"
    rows = [(st, source + (":" + str(i) if keyed else ""),
             "CORRECT" if i % 2 == 0 else "WRONG", .01, date.today(), "SYNTHETIC") for i in range(101)]
    counts = []
    table = {}
    class Result:
        def __init__(self, rows=(), rowcount=0): self.rows, self.rowcount = rows, rowcount
        def fetchall(self): return self.rows
    class Conn:
        def __init__(self): self.changed = 0
        def execute(self, statement, params=None):
            sql = str(statement)
            if "SELECT source_type" in sql: return Result(rows)
            if "SELECT id FROM" in sql:
                ids = list(range(params["after_id"] + 1, min(101, params["after_id"] + 50) + 1))
                return Result([(i,) for i in ids])
            if "UPDATE" in sql:
                assert "id IN" in sql and len(params["target_ids"]) <= 50
                for i in params["target_ids"]: table[i] = (params["hc"], params["mc"], params["ts"])
                self.changed += len(params["target_ids"])
                return Result(rowcount=len(params["target_ids"]))
    class Engine:
        @contextmanager
        def connect(self): yield Conn()
        @contextmanager
        def begin(self):
            conn = Conn()
            yield conn
            counts.append(conn.changed)
    result = trust.update_trust_scores(Engine())
    assert [n for n in counts if n] == [50, 50, 1]
    assert len(table) == 101 and result["total"] == 1
    assert (result["sources"][0]["hit_count"], result["sources"][0]["miss_count"], result["sources"][0]["propagated_rows"]) == (51, 50, 101)
    assert len(set(table.values())) == 1
