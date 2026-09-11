"""Tests for store/snapshots.py's AnalyticalSnapshotStore.

Focused on the `retention_per_category` parameter added so a heavy writer
(e.g. api/routers/flows.py's sector-flow warm loop, which persists every
~240s) can bound the number of rows it keeps in `analytical_snapshots`
without a hard-coded DELETE living outside this module.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from store.snapshots import AnalyticalSnapshotStore


def _make_engine():
    """MagicMock engine whose `.begin()`/`.connect()` context managers both
    yield the same mock connection, so calls made inside `_ensure_table`,
    `save_snapshot`, and `_prune_category` are all observable on one mock."""
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (1,)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _sql_text(stmt) -> str:
    return str(getattr(stmt, "text", stmt))


def test_retention_defaults_to_none_and_never_prunes():
    engine, conn = _make_engine()
    store = AnalyticalSnapshotStore(db_engine=engine)
    assert store.retention_per_category is None
    conn.execute.reset_mock()

    snap_id = store.save_snapshot(category="sector_flows", payload={"sectors": {}})

    assert snap_id == 1
    executed_sql = [_sql_text(c.args[0]) for c in conn.execute.call_args_list]
    assert not any("DELETE FROM analytical_snapshots" in sql for sql in executed_sql)


def test_retention_prunes_after_insert_when_set():
    engine, conn = _make_engine()
    store = AnalyticalSnapshotStore(db_engine=engine, retention_per_category=5)
    conn.execute.reset_mock()

    store.save_snapshot(category="sector_flows", payload={"sectors": {}})

    delete_calls = [
        c for c in conn.execute.call_args_list
        if "DELETE FROM analytical_snapshots" in _sql_text(c.args[0])
    ]
    assert len(delete_calls) == 1
    stmt, params = delete_calls[0].args[0], delete_calls[0].args[1]
    assert params == {"cat": "sector_flows", "keep_n": 5}
    # Parameterized SQL only — the category/keep_n values are bound, never
    # interpolated into the query string.
    sql_text = _sql_text(stmt)
    assert ":cat" in sql_text and ":keep_n" in sql_text
    assert "sector_flows" not in sql_text


def test_retention_prune_is_scoped_to_the_written_category():
    engine, conn = _make_engine()
    store = AnalyticalSnapshotStore(db_engine=engine, retention_per_category=10)
    conn.execute.reset_mock()

    store.save_snapshot(category="clustering", payload={"result": {}})

    delete_calls = [
        c for c in conn.execute.call_args_list
        if "DELETE FROM analytical_snapshots" in _sql_text(c.args[0])
    ]
    assert len(delete_calls) == 1
    assert delete_calls[0].args[1]["cat"] == "clustering"


def test_prune_failure_does_not_fail_save():
    """Pruning is best-effort: the insert already committed, so a prune
    failure must not turn a successful save into a reported failure."""
    engine, conn = _make_engine()
    store = AnalyticalSnapshotStore(db_engine=engine, retention_per_category=5)

    def flaky_execute(stmt, *args, **kwargs):
        if "DELETE FROM analytical_snapshots" in _sql_text(stmt):
            raise RuntimeError("db unreachable")
        m = MagicMock()
        m.fetchone.return_value = (7,)
        return m

    conn.execute.side_effect = flaky_execute

    snap_id = store.save_snapshot(category="sector_flows", payload={"sectors": {}})

    assert snap_id == 7


def test_save_snapshot_failure_skips_pruning():
    """If the insert itself fails, save_snapshot returns None before ever
    attempting to prune."""
    engine, conn = _make_engine()
    store = AnalyticalSnapshotStore(db_engine=engine, retention_per_category=5)

    def always_fail(stmt, *args, **kwargs):
        raise RuntimeError("db unreachable")

    conn.execute.side_effect = always_fail

    snap_id = store.save_snapshot(category="sector_flows", payload={"sectors": {}})

    assert snap_id is None
