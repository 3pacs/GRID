"""Two-process, same-synthetic-schema forward -> compatibility-HOLD proof.

Run this one case first from the forward commit with PHASE=forward, then from
the exact HOLD commit with PHASE=hold and the same receipt file/database.
Finally run PHASE=cleanup even after a failed phase. No production defaults.
"""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import re
import socket
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import make_url
from sqlalchemy.pool import NullPool

pytestmark=[pytest.mark.timeout(60),pytest.mark.skipif(
    os.environ.get('GRID_OCT25_REQUIRE_PG') is None,reason='explicit synthetic PostgreSQL opt-in required')]


def test_same_database_forward_to_compatibility_hold(monkeypatch):
    assert os.environ.get('GRID_OCT25_REQUIRE_PG')=='1'
    assert os.environ.get('GRID_GIT_SINK_DISABLED')=='1'
    assert os.environ.get('GIT_SINK_PUSH_ENABLED','').lower() not in ('1','true','yes')
    phase=os.environ.get('GRID_OCT25_ROLLBACK_PHASE')
    assert phase in ('forward','hold','cleanup')
    receipt_path=Path(os.environ['GRID_OCT25_ROLLBACK_RECEIPT'])
    assert receipt_path.is_absolute() and receipt_path.parent.is_dir()
    url=make_url(os.environ['GRID_OCT25_TEST_DB_URL'])
    assert url.drivername in ('postgresql','postgresql+psycopg2') and not url.query
    assert (url.host,url.port,url.database,url.username)==('127.0.0.1',15432,'oct25_native_test','oct25_ci_writer')
    assert make_url(os.environ['GRID_TEST_DB_URL'])==url
    assert (os.environ['DB_HOST'],os.environ['DB_PORT'],os.environ['DB_NAME'],os.environ['DB_USER'],os.environ['DB_PASSWORD'])==('127.0.0.1','15432','oct25_native_test','oct25_ci_writer',url.password)
    major=int(os.environ['GRID_OCT25_PG_MAJOR']); assert major in (14,15)
    def no_network(*a,**kw): raise AssertionError('provider/network forbidden')
    monkeypatch.setattr(socket.socket,'connect',no_network)
    monkeypatch.setattr(socket.socket,'connect_ex',no_network)
    monkeypatch.setattr(socket,'create_connection',no_network)
    owner=create_engine(url,poolclass=NullPool,connect_args={'connect_timeout':5})
    scoped=None
    try:
        with owner.connect() as conn:
            identity=conn.execute(text("""SELECT current_database(),current_user,
                current_setting('server_version_num')::int,rolsuper,rolcreatedb,
                rolcreaterole,rolreplication,pg_get_userbyid(d.datdba)=current_user
                FROM pg_roles r,pg_database d WHERE r.rolname=current_user
                AND d.datname=current_database()""")).one()
            assert tuple(identity[:2])==('oct25_native_test','oct25_ci_writer')
            assert major*10000<=identity[2]<(major+1)*10000
            assert tuple(identity[3:])==(False,False,False,False,True)
        if phase=='forward':
            assert not receipt_path.exists(), 'never overwrite a previous phase receipt'
            schema='oct25_rollback_'+uuid4().hex
            # Persist locator before imports/preparation so finally-cleanup can
            # remove this known synthetic schema even if the forward test fails.
            receipt={'schema':schema,'major':major,'state':'planned'}
            receipt_path.write_text(json.dumps(receipt),encoding='utf-8')
            with owner.begin() as conn: conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            receipt['state']='created'
        else:
            receipt=json.loads(receipt_path.read_text(encoding='utf-8'))
            schema=receipt['schema']
            assert re.fullmatch('oct25_rollback_[0-9a-f]{32}',schema)
            assert receipt['major']==major
        if phase=='cleanup':
            with owner.begin() as conn: conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            receipt['cleanup']='complete'
            receipt_path.write_text(json.dumps(receipt,indent=2),encoding='utf-8')
            return
        scoped=create_engine(url,poolclass=NullPool,connect_args={'connect_timeout':5,
            'options':f'-csearch_path={schema} -cstatement_timeout=10000 -clock_timeout=5000'})
        from intelligence import thesis_tracker as tracker
        from analysis import thesis_scorer
        from intelligence.thesis_schema import prepare_thesis_schema
        from intelligence.adapters.thesis_tracker_adapter import ThesisTrackerAdapter
        from intelligence.signal_registry import SignalType
        assert tracker.THESIS_SCORING_HELD is True
        from config import settings
        assert make_url(settings.DB_URL)==url.set(drivername='postgresql')
        values=[(.75,'fraction',.75),(75,'percent',.75),(0,'fraction',0),
                (None,None,None),(75,None,None),(.75,None,None),
                (1,'fraction',1),(1,'percent',.01),(101,'percent',None)]
        if phase=='forward':
            prepare_thesis_schema(scoped,schema)
            # Both real writers execute; explicit rows supply historical and
            # mixed-unit synthetic states without asserting their provenance.
            tracker.snapshot_thesis(scoped,{'conviction':75,'conviction_unit':'percent'})
            assert thesis_scorer.snapshot_thesis(scoped,{'direction':'BULLISH','conviction':75,
                'score':75,'bull_pct':87.5,'bear_pct':12.5,'models':[],
                'active_models':0,'total_models':0,'evaluation_window':'5d'}) is not None
            with scoped.begin() as conn:
                for i,(v,u,_) in enumerate(values):
                    conn.execute(text("INSERT INTO thesis_snapshots(overall_direction,conviction,conviction_unit,narrative,timestamp) VALUES('bullish',:v,:u,:n,NOW() - interval '1 day' + :i * interval '1 second')"),{'v':v,'u':u,'n':f'phase-case-{i}','i':i})
            receipt['state']='forward_seeded'
        else:
            assert receipt['state']=='forward_seeded'
            statements=[]
            def observed(conn,cursor,statement,parameters,context,executemany):statements.append(statement)
            event.listen(scoped,'before_cursor_execute',observed)
            try:
                with pytest.raises(RuntimeError,match='THESIS_COMPATIBILITY_HOLD'):
                    tracker.snapshot_thesis(scoped,{'conviction':0})
                assert thesis_scorer.snapshot_thesis(scoped,{'conviction':75}) is None
                assert tracker.run_thesis_cycle(scoped)['compatibility_hold'] is True
                assert statements==[], 'HOLD must refuse before DB/provider effects'
            finally: event.remove(scoped,'before_cursor_execute',observed)
        with scoped.connect() as conn:
            rows=conn.execute(text('SELECT id,conviction::text,conviction_unit,narrative,timestamp::text FROM thesis_snapshots ORDER BY id')).all()
        digest=hashlib.sha256(json.dumps([list(r) for r in rows]).encode()).hexdigest()
        if phase=='forward': receipt['rows_sha256']=digest
        else: assert receipt['rows_sha256']==digest, 'same persisted state must survive process/code transition'
        # Real history/serializer and adapter run under each exact source tree.
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from api.routers import intelligence_thesis as router
        app=FastAPI();app.include_router(router.router)
        app.dependency_overrides[router.require_auth]=lambda:'synthetic'
        monkeypatch.setattr(router,'get_db_engine',lambda:scoped)
        with TestClient(app) as client:
            response=client.get('/thesis/history?days=365')
        assert response.status_code==200 and 'error' not in response.json()
        assert response.json()['count']==len(rows)
        # Select each actual stored row through the unchanged adapter by an
        # invented timestamp update, inside a transaction rolled back afterward.
        # The adapter is bound to this same connection; no registry materialization.
        for i,(_,_,expected) in enumerate(values):
            with scoped.connect() as conn:
                transaction=conn.begin()
                try:
                    conn.execute(text("UPDATE thesis_snapshots SET timestamp='2099-01-01' WHERE narrative=:n"),{'n':f'phase-case-{i}'})
                    class Borrowed:
                        def connect(self):
                            from contextlib import nullcontext
                            return nullcontext(conn)
                    from datetime import datetime,timezone
                    signals=ThesisTrackerAdapter()._build_signals(Borrowed(),datetime.now(timezone.utc))
                    directional=[s for s in signals if s.signal_type==SignalType.DIRECTIONAL]
                    assert len(directional)==(0 if expected is None else 1)
                    if directional:
                        assert directional[0].confidence==expected
                        assert directional[0].value==expected*100
                finally: transaction.rollback()
        with scoped.connect() as conn:
            after=conn.execute(text('SELECT id,conviction::text,conviction_unit,narrative,timestamp::text FROM thesis_snapshots ORDER BY id')).all()
        assert hashlib.sha256(json.dumps([list(r) for r in after]).encode()).hexdigest()==digest
        if phase=='hold':receipt['hold_qualified']=True
        receipt_path.write_text(json.dumps(receipt,indent=2),encoding='utf-8')
    finally:
        if scoped is not None:scoped.dispose()
        owner.dispose()
