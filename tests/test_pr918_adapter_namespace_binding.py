"""Actual adapter/helper/typed-signal regression; catalog/SQL I/O synthetic."""
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from intelligence import schema_readiness as shared
from intelligence.adapters.thesis_tracker_adapter import ThesisTrackerAdapter
from intelligence.signal_registry import SignalType
from tests.test_pr918_schema_hardening_offline import SHAPES

NOW = datetime(2026,10,31,tzinfo=timezone.utc)

class Engine:
    def __init__(self, namespaces=('primary_schema','primary_schema'), *,
                 value=.75, unit='fraction', legacy=False, defect=None):
        self.namespaces=namespaces;self.connections=[];self.statements=[]
        self.row=['bullish',value,[],'invented',NOW,unit]
        self.legacy=legacy;self.defect=defect
    def connect(self):
        index=len(self.connections)
        assert index<len(self.namespaces),'unexpected extra connection'
        namespace=self.namespaces[index];self.connections.append(namespace)
        engine=self
        class Connection:
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def execute(self,statement,parameters=None):
                sql=str(statement);engine.statements.append((namespace,sql,deepcopy(parameters)))
                if sql==shared._CATALOG:
                    relation=parameters['relation']
                    rows=[('r',namespace,n,t,'pg_catalog',nn,d is not None,False,d)
                          for n,t,nn,d in SHAPES[relation]
                          if not(engine.legacy and n=='conviction_unit')]
                    if engine.defect=='missing_snapshot' and relation=='thesis_snapshots':rows=[]
                    if engine.defect=='default':rows=[r[:8]+(None,) if r[2]=='id' else r for r in rows]
                    if engine.defect=='split_relations' and relation=='thesis_postmortems':
                        rows=[r[:1]+('different_schema',)+r[2:] for r in rows]
                    return SimpleNamespace(all=lambda:rows)
                if sql==shared._CONSTRAINTS:
                    rows=[] if engine.defect=='pk' else [('p',['id'],None,None,[],True)]
                    if parameters['relation']=='thesis_postmortems':
                        rows.append(('f',['snapshot_id'],namespace,'thesis_snapshots',['id'],True))
                    return SimpleNamespace(all=lambda:rows)
                if 'FROM thesis_snapshots ORDER BY' in sql:
                    return SimpleNamespace(fetchone=lambda:engine.row)
                if 'FROM thesis_postmortems WHERE' in sql:
                    return SimpleNamespace(fetchone=lambda:(3,2,5))
                raise AssertionError('UNEXPECTED_SQL:'+sql)
        return Connection()

def run(engine):
    before=deepcopy(engine.row)
    try:return ThesisTrackerAdapter()._build_signals(engine,NOW)
    finally:
        assert engine.row==before
        assert all(sql.lstrip().startswith('SELECT') for _,sql,_ in engine.statements)

@pytest.mark.parametrize('namespace',['primary_schema','other_valid_schema'])
def test_matching_operation_namespaces_preserve_both_typed_signals(namespace):
    engine=Engine((namespace,namespace));signals=run(engine)
    assert engine.connections==[namespace,namespace]
    directional=next(s for s in signals if s.signal_type==SignalType.DIRECTIONAL)
    accuracy=next(s for s in signals if s.signal_type==SignalType.MAGNITUDE)
    assert directional.confidence==.75 and directional.value==75
    assert accuracy.value==.6 and accuracy.metadata=={'correct':3,'wrong':2,'total':5}
    for scope in range(2):
        queries=[s for n,s,_ in engine.statements][scope*5:(scope+1)*5]
        assert queries[:4]==[shared._CATALOG,shared._CONSTRAINTS,shared._CATALOG,shared._CONSTRAINTS]

@pytest.mark.parametrize('first,second',[('primary_schema','other_schema'),('other_schema','primary_schema')])
def test_operation_namespace_drift_refuses_before_second_business_read(first,second):
    engine=Engine((first,second))
    with pytest.raises(shared.SchemaNotReady,match='outside'):run(engine)
    assert engine.connections==[first,second]
    business=[sql for _,sql,_ in engine.statements if sql not in (shared._CATALOG,shared._CONSTRAINTS)]
    assert len(business)==1 and 'FROM thesis_snapshots ORDER BY' in business[0]

@pytest.mark.parametrize('defect',['missing_snapshot','default','pk','split_relations'])
def test_incompatible_catalog_refuses_before_any_business_read(defect):
    engine=Engine(defect=defect)
    with pytest.raises(shared.SchemaNotReady):run(engine)
    assert engine.connections==['primary_schema']
    assert all(sql in (shared._CATALOG,shared._CONSTRAINTS) for _,sql,_ in engine.statements)

@pytest.mark.parametrize('value,unit,expected',[(.75,'fraction',.75),(75,'percent',.75),
    (0,'fraction',0),(1,'percent',.01),(None,None,None),(.75,None,None),
    (75,None,None),(101,'percent',None),(.75,'unknown',None)])
def test_receipt_value_contract_is_unchanged(value,unit,expected):
    signals=run(Engine(value=value,unit=unit))
    directional=[s for s in signals if s.signal_type==SignalType.DIRECTIONAL]
    assert len(directional)==(0 if expected is None else 1)
    if directional:
        assert directional[0].confidence==expected and directional[0].value==expected*100
        assert directional[0].metadata['stored_conviction_unit']==unit
    assert len([s for s in signals if s.signal_type==SignalType.MAGNITUDE])==1

def test_legacy_missing_receipt_column_stays_unknown_and_keeps_accuracy():
    signals=run(Engine(value=75,unit=None,legacy=True))
    assert len(signals)==1 and signals[0].signal_type==SignalType.MAGNITUDE
    assert signals[0].value==.6

def test_wrapper_refuses_without_publishing_partial_signals_on_drift():
    engine=Engine(('primary_schema','other_schema'))
    assert ThesisTrackerAdapter().extract_signals(engine)==[]
    assert engine.connections==['primary_schema','other_schema']
