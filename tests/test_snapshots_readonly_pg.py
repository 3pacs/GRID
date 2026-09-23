"""Disposable PostgreSQL contract for read-only snapshot GET store mode."""
from __future__ import annotations
import os
import uuid
import pytest
from sqlalchemy import create_engine, event, text
from store.snapshots import AnalyticalSnapshotStore

_URL = os.environ.get("DB_URL")
pytestmark = pytest.mark.skipif(not _URL, reason="requires CI disposable DB_URL")

def test_read_store_issues_only_selects_and_writer_still_bootstraps():
    admin = create_engine(_URL)
    schema = "snap_ro_" + uuid.uuid4().hex
    with admin.begin() as c: c.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(_URL)
    @event.listens_for(engine, "connect")
    def path(dbapi, _):
        cur = dbapi.cursor(); cur.execute(f'SET search_path TO "{schema}"'); cur.close()
    try:
        writer = AnalyticalSnapshotStore(engine)
        writer.save_snapshot("test", {"ok": True})
        statements=[]
        @event.listens_for(engine, "before_cursor_execute")
        def capture(*args): statements.append(args[2].strip().upper())
        reader = AnalyticalSnapshotStore(engine, ensure_table=False)
        assert reader.get_latest("test")
        assert reader.get_history("test").shape[0] == 1
        assert reader.compare_snapshots("test", reader.get_latest("test")[0]["snapshot_date"], reader.get_latest("test")[0]["snapshot_date"])
        assert reader.list_categories()
        assert statements and all(s.startswith("SELECT") for s in statements)
    finally:
        engine.dispose()
        with admin.begin() as c: c.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()
