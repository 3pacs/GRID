"""Fresh receiving-boundary controls; real functions, synthetic catalog/SQL I/O.

No server, driver or DB connection is used. This is not native PostgreSQL proof.
"""
from contextlib import contextmanager
from copy import deepcopy
from types import SimpleNamespace
import ast
import json
from pathlib import Path

import pytest
from intelligence import schema_readiness as shared
from intelligence import thesis_tracker as tracker
from ollama.market_briefing import MarketBriefingEngine

ROOT = Path(__file__).resolve().parents[1]
# Independent explicit expectations, not rows derived from product contracts.
SHAPES = {
    'thesis_snapshots': [('id','int4',True,'generated_integer'),
        ('timestamp','timestamptz',True,'transaction_timestamp'),
        ('overall_direction','text',True,None),('conviction','numeric',False,None),
        ('conviction_unit','text',False,None),('key_drivers','jsonb',False,None),
        ('risk_factors','jsonb',False,None),('model_states','jsonb',False,None),
        ('narrative','text',False,None),('outcome','text',False,None),
        ('actual_market_move','numeric',False,None),('scored_at','timestamptz',False,None)],
    'thesis_postmortems': [('id','int4',True,'generated_integer'),
        ('snapshot_id','int4',False,None),('thesis_direction','text',False,None),
        ('actual_direction','text',False,None),('models_right','jsonb',False,None),
        ('models_wrong','jsonb',False,None),('what_we_missed','text',False,None),
        ('root_cause','text',False,None),('lesson','text',False,None),
        ('generated_at','timestamptz',True,'transaction_timestamp')],
    'market_briefings': [('id','int4',True,'generated_integer'),
        ('briefing_type','text',True,None),('briefing_date','date',True,None),
        ('content','text',True,None),('sentiment_score','float4',False,None),
        ('sentiment_label','text',False,None),('sentiment_data','jsonb',False,None),
        ('snapshot_data','jsonb',False,None),('created_at','timestamptz',False,'transaction_timestamp')],
}

class Connection:
    def __init__(self, *, defect=None, returned_id=19):
        self.statements=[]; self.params=[]; self.defect=defect; self.returned_id=returned_id
        self.namespace='synthetic'
    def execute(self, statement, parameters=None):
        sql=str(statement); self.statements.append(sql); self.params.append(deepcopy(parameters))
        if sql == shared._CATALOG:
            relation=parameters['relation']; shape=SHAPES[relation]
            rows=[('r',self.namespace,n,t,'pg_catalog',nn,d is not None,False,d) for n,t,nn,d in shape]
            if self.defect=='missing': rows=[]
            if self.defect=='default': rows=[r[:8]+(None,) if r[2]=='id' else r for r in rows]
            if self.defect=='unit_default': rows=[r[:6]+(True,)+r[7:] if r[2]=='conviction_unit' else r for r in rows]
            if self.defect=='namespace': rows=[r[:1]+('other_schema',)+r[2:] for r in rows]
            return SimpleNamespace(all=lambda:rows)
        if sql == shared._CONSTRAINTS:
            rows=[] if self.defect=='pk' else [('p',['id'],None,None,[],True)]
            if parameters['relation']=='thesis_postmortems':
                rows.append(('f',['snapshot_id'],'synthetic','thesis_snapshots',['id'],True))
            return SimpleNamespace(all=lambda:rows)
        assert sql.lstrip().startswith(('SELECT set_config','INSERT')), sql
        return SimpleNamespace(fetchone=lambda:None if self.returned_id is None else (self.returned_id,))

class Engine:
    def __init__(self, *, defect=None, returned_id=19, fail_commit=False):
        self.conn=Connection(defect=defect,returned_id=returned_id)
        self.commits=0; self.rollbacks=0; self.fail_commit=fail_commit
    @contextmanager
    def connect(self):
        yield self.conn
    @contextmanager
    def begin(self):
        try:
            if self.conn.defect=='operation_namespace': self.conn.namespace='other_schema'
            yield self.conn
            if self.fail_commit: raise RuntimeError('synthetic commit failure')
        except BaseException:
            self.rollbacks+=1
            raise
        else: self.commits+=1

def publication(engine):
    result={'type':'morning','date':'2026-10-31','content':'invented',
            'sentiment':None,'snapshot':{'publication_timestamp':None,'conviction_unit':None}}
    before=deepcopy(result)
    obj=MarketBriefingEngine.__new__(MarketBriefingEngine); obj.engine=engine
    answer=obj._persist_to_db(result)
    assert result==before
    return answer

@pytest.mark.parametrize('value,unit,expected,receipt',[(75,'percent',.75,'fraction'),
    (.75,'fraction',.75,'fraction'),(0,'fraction',0,'fraction'),(None,None,None,None)])
def test_actual_tracker_preserves_input_and_writes_only_after_readiness(value,unit,expected,receipt):
    engine=Engine(); data={'conviction':value,'conviction_unit':unit}; before=deepcopy(data)
    assert tracker.snapshot_thesis(engine,data)==19
    assert data==before and engine.commits==1 and engine.rollbacks==0
    writes=[p for s,p in zip(engine.conn.statements,engine.conn.params) if s.lstrip().startswith('INSERT')]
    assert len(writes)==1 and writes[0]['conviction']==expected and writes[0]['conviction_unit']==receipt
    assert not any(s.lstrip().startswith(('CREATE','ALTER','DROP')) for s in engine.conn.statements)

@pytest.mark.parametrize('defect',['missing','default','pk','unit_default'])
def test_tracker_real_catalog_refusal_no_business_write(defect):
    engine=Engine(defect=defect)
    with pytest.raises(shared.SchemaNotReady): tracker.snapshot_thesis(engine,{'conviction':0})
    assert not any(s.lstrip().startswith(('INSERT','CREATE','ALTER')) for s in engine.conn.statements)
    assert engine.commits==0

@pytest.mark.parametrize('value,unit',[(75,'unknown'),(True,'fraction'),(float('inf'),'fraction'),(1.1,'fraction')])
def test_invalid_unit_value_refuses_before_any_connection(value,unit):
    engine=Engine()
    with pytest.raises(ValueError): tracker.snapshot_thesis(engine,{'conviction':value,'conviction_unit':unit})
    assert engine.conn.statements==[]

def test_actual_publication_commits_once_without_ddl():
    engine=Engine(); assert publication(engine)==19
    assert engine.commits==1 and engine.rollbacks==0
    sql=engine.conn.statements
    assert sql[0].startswith('SELECT set_config')
    assert sum(s.lstrip().startswith('INSERT') for s in sql)==1
    assert not any(s.lstrip().startswith(('CREATE','ALTER','DROP')) for s in sql)

def test_actual_tracker_operation_connection_namespace_drift_refuses():
    engine=Engine(defect='operation_namespace')
    with pytest.raises(shared.SchemaNotReady,match='outside'):
        tracker.snapshot_thesis(engine,{'conviction':0})
    assert engine.rollbacks==1 and engine.commits==0
    assert not any(s.lstrip().startswith('INSERT') for s in engine.conn.statements)

@pytest.mark.parametrize('defect',['missing','default','pk'])
def test_actual_publication_readiness_refuses_before_insert(defect):
    engine=Engine(defect=defect); assert publication(engine) is None
    assert engine.rollbacks==1 and engine.commits==0
    assert not any(s.lstrip().startswith('INSERT') for s in engine.conn.statements)

@pytest.mark.parametrize('returned_id',[None,True])
def test_actual_publication_invalid_ack_rolls_back(returned_id):
    engine=Engine(returned_id=returned_id); assert publication(engine) is None
    assert engine.commits==0 and engine.rollbacks==1

def test_actual_publication_commit_failure_never_acknowledges():
    engine=Engine(fail_commit=True); assert publication(engine) is None
    assert engine.commits==0 and engine.rollbacks==1

def test_scorer_actual_snapshot_checks_operation_connection():
    # Exact function extraction, explicitly not a native module import.
    tree=ast.parse((ROOT/'analysis/thesis_scorer.py').read_text())
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='snapshot_thesis')
    from sqlalchemy import text
    ns={'Engine':object,'Any':object,'json':json,'text':text,'log':SimpleNamespace(info=lambda *a,**k:None,warning=lambda *a,**k:None),'_build_narrative':lambda _: 'invented'}
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<actual scorer snapshot>','exec'),ns)
    data={'direction':'BULLISH','conviction':75,'models':[]}
    engine=Engine(); assert ns['snapshot_thesis'](engine,data)==19
    assert engine.commits==1
    bad=Engine(defect='unit_default'); assert ns['snapshot_thesis'](bad,data) is None
    assert not any(s.lstrip().startswith('INSERT') for s in bad.conn.statements)
