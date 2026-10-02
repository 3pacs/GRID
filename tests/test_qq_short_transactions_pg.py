"""Real PG14 contract; run only against an explicitly supplied private fixture DB.

GRID_QQ_REQUIRE_PG=1 turns absence into failure. The writer must be a
nonsuperuser, non-createrole, non-createdb role; no provider is contacted.
Every fixture and writer transaction changes at most 50 rows.
"""
from __future__ import annotations

import json
import os
import uuid
from contextlib import contextmanager
from datetime import date

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

from ingestion.altdata import quiverquant as qq
from ingestion.altdata import quiverquant_transactions as tx
from scripts import qq_transition_common as common
from scripts import qq_rekey_signal_sources as rekey
from scripts import qq_gov_contracts_redate as redate
from tests.test_qq_short_transactions import records, quarter_moves


@pytest.fixture()
def pg(monkeypatch):
    url = os.environ.get("GRID_QQ_TEST_DB_URL")
    if not url:
        if os.environ.get("GRID_QQ_REQUIRE_PG") == "1":
            pytest.fail("GRID_QQ_TEST_DB_URL private nonsuperuser database required")
        pytest.skip("explicit private QuiverQuant PostgreSQL database not supplied")
    engine = create_engine(url)
    schema = "qq_test_" + uuid.uuid4().hex
    with engine.begin() as conn:
        role = conn.execute(text("SELECT rolsuper, rolcreaterole, rolcreatedb FROM pg_roles WHERE rolname=current_user")).one()
        assert tuple(role) == (False, False, False), role
        assert int(conn.execute(text("SHOW server_version_num")).scalar()) // 10000 == 14
        assert conn.execute(text("SELECT host(inet_server_addr())")).scalar() == "127.0.0.1"
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine.dispose()
    engine = create_engine(url, connect_args={"options": f"-c search_path={schema} -c statement_timeout=2000 -c lock_timeout=1000"})
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE signal_sources (
            id BIGSERIAL PRIMARY KEY, source_type TEXT NOT NULL, source_id TEXT NOT NULL,
            ticker TEXT NOT NULL CHECK(ticker <> 'BAD'), signal_type TEXT NOT NULL,
            signal_date DATE NOT NULL, signal_value JSONB, outcome TEXT DEFAULT 'PENDING',
            created_at TIMESTAMPTZ DEFAULT now(),
            UNIQUE(source_type,source_id,ticker,signal_date,signal_type))"""))
    monkeypatch.setattr(common, "check_window", lambda now=None: None)
    monkeypatch.setattr(common, "require_guard_closed", lambda: None)
    counts = []
    @event.listens_for(engine, "begin")
    def begin(conn):
        conn.info["modifications"] = 0
    @event.listens_for(engine, "after_cursor_execute")
    def after(conn, cursor, statement, parameters, context, executemany):
        assert "SAVEPOINT" not in statement.upper()
        if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
            conn.info["modifications"] += max(cursor.rowcount, 0)
            assert conn.info["modifications"] <= 50
    @event.listens_for(engine, "commit")
    def commit(conn):
        assert conn.info["modifications"] <= 50
        counts.append(conn.info["modifications"])
    try:
        yield engine, counts
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
        engine.dispose()


def seed(engine, rows):
    # Each seed INSERT is its own transaction, including fault fixtures.
    for row in rows:
        with engine.begin() as conn:
            conn.execute(text("""INSERT INTO signal_sources
                (source_type, source_id, ticker, signal_date, signal_type, signal_value, created_at)
                VALUES (:st,:si,:ticker,:day,:ty,CAST(:payload AS jsonb),'2026-09-01T00:00:00Z')"""), {
                "st": row.get("source_type", "quiverquant:house"),
                "si": row.get("source_id", "qq_house_trading"),
                "ticker": row["ticker"], "day": row.get("signal_date", date(2026,9,1)),
                "ty": row.get("signal_type", "house_trading"),
                "payload": json.dumps(row.get("signal_value", {"BioGuideID":"SYNTHETIC","Transaction":"Sale","Range":"1-2"})),
            })


def all_rows(engine):
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(text("SELECT * FROM signal_sources ORDER BY id")).mappings()]


def rekey_plan(engine):
    with engine.connect() as conn:
        return rekey.plan_rekey("quiverquant:house", rekey.load_legacy_rows(conn,"quiverquant:house"),set()).moves


def test_pg_writer_budgets_and_repull_preserves_created_at(pg):
    engine, counts = pg
    assert qq._store_signals(engine, records(103), "quiverquant:lobbying", "lobbying") == 103
    assert counts == [50,50,3]
    before = all_rows(engine)
    qq._store_signals(engine, records(2), "quiverquant:lobbying", "lobbying")
    assert all_rows(engine) == before


def test_pg_bad_record_does_not_poison_following_transactions(pg, monkeypatch):
    engine, counts = pg
    monkeypatch.setattr(qq,"STORE_BATCH_ROWS",3)
    rows = records(5)
    rows[1]["Ticker"] = "BAD"
    with pytest.raises(qq.QuiverStoreAborted) as caught:
        qq._store_signals(engine,rows,"quiverquant:lobbying","lobbying")
    assert (caught.value.stored,caught.value.failed) == (4,1)
    assert len(all_rows(engine)) == 4 and counts == [1,1,2]


def test_pg_writer_commit_ack_loss_keeps_only_acknowledged_prefix(pg,monkeypatch):
    engine,counts=pg
    monkeypatch.setattr(qq,"STORE_BATCH_ROWS",2)
    calls=[0]
    class AckLoss:
        @contextmanager
        def begin(self):
            calls[0]+=1
            with engine.begin() as conn:
                yield conn
            if calls[0]==2:
                raise OperationalError("COMMIT",{},OSError("synthetic lost COMMIT acknowledgment"))
    with pytest.raises(qq.QuiverStoreAborted) as caught:
        qq._store_signals(AckLoss(),records(5),"quiverquant:lobbying","lobbying")
    assert caught.value.stored==2 and caught.value.commit_uncertain
    assert calls==[2] and counts==[2,2] and len(all_rows(engine))==4


def test_pg_rekey_fifty_row_cap_and_exact_audit_prefix(pg,tmp_path):
    engine, counts = pg
    seed(engine,[{"ticker":f"T{i}"} for i in range(101)])
    original = all_rows(engine)
    counts.clear()
    audit = tmp_path / "rekey"
    result = rekey.apply_moves(engine,rekey_plan(engine),batch_size=50,audit_path=audit)
    assert result["moved"] == 101 and counts == [50,50,1]
    assert len(audit.read_text().splitlines()) == 101
    for old,new in zip(original,all_rows(engine)):
        assert {k:v for k,v in old.items() if k!="source_id"} == {k:v for k,v in new.items() if k!="source_id"}


def test_pg_real_lock_timeout_skips_only_failed_batch_and_continues(pg,tmp_path):
    engine, counts = pg
    seed(engine,[{"ticker":f"T{i}"} for i in range(4)])
    moves = rekey_plan(engine)
    locker = engine.connect()
    transaction = locker.begin()
    try:
        locker.execute(text("UPDATE signal_sources SET source_id=source_id WHERE id=:id"),{"id":moves[0].id})
        counts.clear()
        result = rekey.apply_moves(engine,moves,batch_size=2,audit_path=tmp_path/"audit")
        assert result["moved"] == 2 and result["skipped_timeout_rows"] == 2
        assert result["batches_skipped_timeout"] == 1 and counts == [2]
        assert len((tmp_path/"audit").read_text().splitlines()) == 2
    finally:
        transaction.rollback()
        locker.close()
    assert sum(r["source_id"]=="qq_house_trading" for r in all_rows(engine)) == 2


def test_pg_redate_fifty_chain_and_manual_reverse_preserve_legacy_and_conflicts(pg,tmp_path):
    engine, counts = pg
    rows,_ = quarter_moves(50)
    seed(engine,[{**r,"source_type":redate.SOURCE_TYPE} for r in rows])
    seed(engine,[{"source_type":redate.SOURCE_TYPE,"source_id":"qq_gov_contracts","ticker":"LEGACY",
                  "signal_type":"gov_contracts","signal_date":date(2026,5,2),"signal_value":{"Year":2026,"Qtr":2}},
                 {"source_type":redate.SOURCE_TYPE,"source_id":"qq_gov_contracts","ticker":"CONFLICT",
                  "signal_type":"gov_contracts","signal_date":date(2026,6,30),"signal_value":{"Year":2026,"Qtr":2}},
                 {"source_type":redate.SOURCE_TYPE,"source_id":"qq_gov_contracts","ticker":"CONFLICT",
                  "signal_type":"gov_contracts","signal_date":date(2026,3,31),"signal_value":{"Year":2026,"Qtr":2}}])
    original = all_rows(engine)
    counts.clear()
    audit = tmp_path/"redate"
    result = redate.run(engine,apply=True,audit_path=audit)
    assert result["summary"]["conflicts_skipped"] == 1 and result["summary"]["not_post_fix_row"] == 1
    assert result["applied"]["moved"] == 50 and counts == [50]
    redate.apply_moves(engine,redate.read_audit(audit),audit_path=tmp_path/"revert",forward=False)
    assert counts == [50,50] and all_rows(engine)==original


def test_pg_oversized_group_refuses_every_group_before_writes(pg,tmp_path):
    engine,counts = pg
    small,_ = quarter_moves(1,"FIRST")
    large,_ = quarter_moves(51,"LARGE",100)
    seed(engine,[{**r,"source_type":redate.SOURCE_TYPE} for r in small+large])
    original=all_rows(engine)
    counts.clear()
    with pytest.raises(ValueError,match="LARGE.*51"):
        redate.run(engine,apply=True,audit_path=tmp_path/"audit")
    assert all_rows(engine)==original and counts==[] and not (tmp_path/"audit").exists()


@pytest.mark.parametrize("script",[rekey,redate])
def test_pg_server_commits_then_ack_is_lost_never_replayed_or_audited(pg,tmp_path,script):
    engine,counts = pg
    if script is rekey:
        seed(engine,[{"ticker":"A"},{"ticker":"B"},{"ticker":"C"}])
        moves = rekey_plan(engine)
    else:
        rows = sum((quarter_moves(1,ticker)[0] for ticker in ("A","B","C")),[])
        seed(engine,[{**r,"source_type":redate.SOURCE_TYPE} for r in rows])
        with engine.connect() as conn:
            moves=redate.plan_redate(redate.load_rows(conn)).moves
    calls=[0]
    class AckLoss:
        @contextmanager
        def begin(self):
            calls[0]+=1
            with engine.begin() as conn:
                yield conn
            if calls[0]==2:
                raise OperationalError("COMMIT",{},OSError("synthetic ack loss after real COMMIT"))
    counts.clear()
    kwargs={"batch_size":1} if script is rekey else {}
    with pytest.raises(tx.CommitUncertain) as caught:
        script.apply_moves(AckLoss(),moves,audit_path=tmp_path/"audit",**kwargs)
    assert caught.value.committed_rows==1 and caught.value.commit_uncertain
    assert calls==[2] and counts==[1,1]
    assert len((tmp_path/"audit").read_text().splitlines())==1
    rows=all_rows(engine)
    if script is rekey:
        assert sum(r["source_id"]!="qq_house_trading" for r in rows)==2
    else:
        assert sum(r["signal_date"]==moves[0].new_date for r in rows)==2


def test_pg_blackout_arrives_before_commit_current_batch_rolls_back(pg,tmp_path,monkeypatch):
    engine,counts=pg
    seed(engine,[{"ticker":"A"},{"ticker":"B"}])
    calls=[0]
    def guard(**kwargs):
        calls[0]+=1
        if calls[0]==6:
            raise common.WindowClosed("synthetic blackout before COMMIT")
    monkeypatch.setattr(common,"write_guard",guard)
    with pytest.raises(common.WindowClosed) as caught:
        rekey.apply_moves(engine,rekey_plan(engine),batch_size=1,audit_path=tmp_path/"audit")
    assert caught.value.committed_rows==1
    assert sum(r["source_id"]!="qq_house_trading" for r in all_rows(engine))==1
