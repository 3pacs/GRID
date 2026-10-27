"""Pure/inert schema protocol tests; no DB connections or application imports."""
import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from intelligence import schema_readiness as shared
from intelligence import thesis_schema as thesis


class Engine:
    def __init__(self):
        self.statements=[]; self.commits=0; self.rollbacks=0; self.namespace='synthetic'
        self.missing=False; self.change=None; self.fail_at=None; self.constraint_change=None
    def connect(self): return self
    def begin(self): return self
    def __enter__(self): return self
    def __exit__(self,typ,*args):
        if typ: self.rollbacks+=1
        else: self.commits+=1
    def execute(self,statement,parameters=None):
        s=str(statement); self.statements.append(s)
        if self.fail_at and self.fail_at in s: raise RuntimeError('synthetic failure')
        if s==shared._CATALOG:
            name=(parameters['relation'].split('.')[-1])
            if name=='market_briefings':
                from intelligence.runtime_schema import PUBLICATION_COLUMNS
                columns=PUBLICATION_COLUMNS
            else:
                columns=thesis.THESIS_CONTRACTS[name]
            rows=[] if self.missing else [('r',self.namespace,k,v.pg_type,'pg_catalog',v.nullable is False,False,False,v.default_kind) for k,v in columns.items()]
            if self.change:
                rows=self.change(rows)
            return SimpleNamespace(all=lambda:rows)
        if s==shared._CONSTRAINTS:
            name=parameters['relation'].split('.')[-1]
            rows=[('p',['id'],None,None,[],True)]
            if name=='thesis_postmortems': rows.append(('f',['snapshot_id'],self.namespace,'thesis_snapshots',['id'],True))
            if self.constraint_change: rows=self.constraint_change(rows)
            return SimpleNamespace(all=lambda:rows)
        return SimpleNamespace(scalar_one=lambda:False)


def test_ready_is_catalog_only():
    engine=Engine(); assert thesis.require_thesis_schema(engine)=='synthetic'
    assert engine.statements==[shared._CATALOG,shared._CONSTRAINTS,shared._CATALOG,shared._CONSTRAINTS]


@pytest.mark.parametrize('change',[
    lambda rows:[],
    lambda rows:[r for r in rows if r[2]!='conviction_unit'],
    lambda rows:[r[:3]+('numeric',)+r[4:] if r[2]=='conviction_unit' else r for r in rows],
    lambda rows:[r[:5]+(True,)+r[6:] if r[2]=='conviction_unit' else r for r in rows],
    lambda rows:[r[:6]+(True,)+r[7:] if r[2]=='conviction_unit' else r for r in rows],
    lambda rows:[('v',)+r[1:] for r in rows],
    lambda rows:[r[:7]+(True,)+r[8:] for r in rows],
])
def test_incompatible_schema_refuses_without_ddl(change):
    engine=Engine(); engine.change=change
    with pytest.raises(shared.SchemaNotReady): thesis.require_thesis_schema(engine)
    assert engine.statements and all(s.lstrip().startswith('SELECT') for s in engine.statements)


@pytest.mark.parametrize('schema',[None,'','public;DROP TABLE x','pg_catalog','information_schema','two.parts'])
def test_invalid_explicit_namespace_has_no_effect(schema):
    engine=Engine()
    with pytest.raises(ValueError): thesis.prepare_thesis_schema(engine,schema)
    assert not engine.statements


def test_migration_bounds_precede_ddl_and_commit_once():
    engine=Engine(); result=thesis.prepare_thesis_schema(engine,'synthetic')
    assert result=={'schema':'synthetic','state':'ready','historical_rows_changed':False}
    assert engine.statements[:4]==["SET LOCAL lock_timeout = '500ms'","SET LOCAL statement_timeout = '2000ms'","SET LOCAL idle_in_transaction_session_timeout = '5000ms'","SET LOCAL search_path = pg_catalog"]
    ddl=[s for s in engine.statements if s.lstrip().startswith(('CREATE','ALTER'))]
    assert len(ddl)==6
    assert all('"synthetic".' in s for s in ddl)
    assert not any(s.lstrip().startswith(('UPDATE','DELETE','INSERT','GRANT')) for s in engine.statements)
    assert engine.commits==1 and engine.rollbacks==0


@pytest.mark.parametrize('failure',['ALTER TABLE','CREATE INDEX'])
def test_migration_failure_rolls_back_without_retry(failure):
    engine=Engine(); engine.fail_at=failure
    with pytest.raises(RuntimeError): thesis.prepare_thesis_schema(engine,'synthetic')
    assert sum(failure in s for s in engine.statements)==1
    assert engine.rollbacks==1 and engine.commits==0


def test_elapsed_deadline_rolls_back_before_commit(monkeypatch):
    engine=Engine(); now=[0.0]; monkeypatch.setattr(shared.time,'monotonic',lambda:now[0])
    with pytest.raises(shared.SchemaPreparationTimeout):
        with shared.bounded_schema_transaction(engine) as (_,execute):
            execute('SELECT 1'); now[0]=5.0
    assert engine.rollbacks==1 and engine.commits==0


def test_every_legacy_callsite_now_targets_readiness_only():
    root=Path(__file__).resolve().parents[1]
    tree=ast.parse((root/'intelligence/thesis_tracker.py').read_text(encoding='utf-8'))
    function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_ensure_tables')
    literals=[n.value for n in ast.walk(function) if isinstance(n,ast.Constant) and isinstance(n.value,str)]
    assert not any(word in s.upper() for s in literals for word in ('CREATE TABLE','ALTER TABLE','CREATE INDEX'))
    callers={n.name for n in tree.body if isinstance(n,ast.FunctionDef) and any(isinstance(c,ast.Call) and isinstance(c.func,ast.Name) and c.func.id=='_ensure_tables' for c in ast.walk(n))}
    assert callers=={'snapshot_thesis','score_old_theses','generate_thesis_postmortem','get_thesis_history','get_thesis_accuracy','run_thesis_cycle','load_thesis_postmortems'}
    scorer=ast.parse((root/'analysis/thesis_scorer.py').read_text(encoding='utf-8'))
    writer=next(n for n in scorer.body if isinstance(n,ast.FunctionDef) and n.name=='snapshot_thesis')
    assert any(isinstance(c,ast.Call) and isinstance(c.func,ast.Name) and c.func.id=='_ensure_tables' for c in ast.walk(writer))
    assert not any(isinstance(c,ast.Name) and c.id=='prepare_thesis_schema' for c in ast.walk(tree))


@pytest.mark.parametrize('column',['id','timestamp','generated_at'])
def test_missing_functional_default_is_not_ready(column):
    engine=Engine()
    engine.change=lambda rows:[r[:8]+(None,) if r[2]==column else r for r in rows]
    with pytest.raises(shared.SchemaNotReady): thesis.require_thesis_schema(engine)


@pytest.mark.parametrize('change',[
    lambda rows:[r for r in rows if r[0]!='p'],
    lambda rows:[r for r in rows if r[0]!='f'],
    lambda rows:[r[:2]+('other_schema',)+r[3:] if r[0]=='f' else r for r in rows],
    lambda rows:[r[:5]+(False,) if r[0]=='f' else r for r in rows],
])
def test_missing_or_wrong_keys_are_not_ready(change):
    engine=Engine(); engine.constraint_change=change
    with pytest.raises(shared.SchemaNotReady): thesis.require_thesis_schema(engine)


def test_expired_final_set_never_yields_body(monkeypatch):
    engine=Engine(); now=[0.0]; monkeypatch.setattr(shared.time,'monotonic',lambda:now[0])
    original=engine.execute
    def delayed(statement,parameters=None):
        result=original(statement,parameters)
        if str(statement)=='SET LOCAL search_path = pg_catalog':now[0]=6.0
        return result
    engine.execute=delayed
    entered=[]
    with pytest.raises(shared.SchemaPreparationTimeout):
        with shared.bounded_schema_transaction(engine):entered.append(True)
    assert entered==[] and engine.rollbacks==1 and engine.commits==0


def test_actual_snapshot_refuses_second_connection_namespace_drift():
    import json
    class Drifting(Engine):
        entries=0
        def __enter__(self):
            self.entries+=1;self.namespace='schema_a' if self.entries==1 else 'schema_b'
            return self
    root=Path(__file__).resolve().parents[1]
    fn=next(n for n in ast.parse((root/'intelligence/thesis_tracker.py').read_text(encoding='utf-8')).body
            if isinstance(n,ast.FunctionDef) and n.name=='snapshot_thesis')
    ns={'Engine':object,'json':json,'_snapshot_conviction_fraction':lambda data:0.0,
        '_ensure_tables':thesis.require_thesis_schema,
        'require_thesis_connection':thesis.require_thesis_connection}
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<actual snapshot>','exec'),ns)
    engine=Drifting()
    with pytest.raises(shared.SchemaNotReady,match='outside'):
        ns['snapshot_thesis'](engine,{'conviction':0})
    assert engine.entries==2 and engine.rollbacks==1
    assert all(s.lstrip().startswith('SELECT') for s in engine.statements)


@pytest.mark.parametrize('expected,valid',[('synthetic',True),('other_schema',False),(None,False)])
def test_transitive_postmortem_store_binds_actual_connection(expected,valid):
    import json
    root=Path(__file__).resolve().parents[1]
    tree=ast.parse((root/'intelligence/thesis_tracker.py').read_text(encoding='utf-8'))
    fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_store_thesis_postmortem')
    ns={'Engine':object,'ThesisPostMortem':object,'json':json,'text':lambda s:s,
        'require_thesis_connection':thesis.require_thesis_connection}
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'<actual inert persistence helper>','exec'),ns)
    pm=SimpleNamespace(snapshot_id=1,thesis_direction='bullish',actual_direction='bearish',
        models_that_were_right=[],models_that_were_wrong=[],what_we_missed='synthetic',
        root_cause='synthetic',lesson='synthetic',generated_at='2026-01-01T00:00:00Z')
    engine=Engine()
    if valid:
        ns['_store_thesis_postmortem'](engine,pm,expected_schema=expected)
        assert sum(s.lstrip().startswith('INSERT') for s in engine.statements)==1
    else:
        with pytest.raises(shared.SchemaNotReady):
            ns['_store_thesis_postmortem'](engine,pm,expected_schema=expected)
        assert not any(s.lstrip().startswith('INSERT') for s in engine.statements)
        if expected is None: assert engine.statements==[]


def test_postmortem_helper_requires_bound_namespace_and_caller_passes_it():
    root=Path(__file__).resolve().parents[1]
    tree=ast.parse((root/'intelligence/thesis_tracker.py').read_text(encoding='utf-8'))
    helper=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_store_thesis_postmortem')
    assert [a.arg for a in helper.args.kwonlyargs]==['expected_schema']
    assert helper.args.kw_defaults==[None]
    caller=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='generate_thesis_postmortem')
    calls=[n for n in ast.walk(caller) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='_store_thesis_postmortem']
    assert len(calls)==1
    assert any(k.arg=='expected_schema' and isinstance(k.value,ast.Name) and k.value.id=='schema' for k in calls[0].keywords)


def test_combined_schema_preparation_uses_one_transaction():
    from intelligence.runtime_schema import prepare_runtime_schema, PUBLICATION_COLUMNS
    engine=Engine()
    result=prepare_runtime_schema(engine,thesis_schema='synthetic',publication_schema='synthetic')
    assert result['state']=='ready' and result['historical_rows_changed'] is False
    assert engine.commits==1 and engine.rollbacks==0
    assert sum(s.lstrip().startswith(('CREATE','ALTER')) for s in engine.statements)==9
    assert sum(s.startswith('SET LOCAL lock_timeout') for s in engine.statements)==1
    assert PUBLICATION_COLUMNS['id'].default_kind=='generated_integer'
    assert PUBLICATION_COLUMNS['created_at'].default_kind=='transaction_timestamp'
    assert set(PUBLICATION_COLUMNS)=={'id','briefing_type','briefing_date','content',
        'sentiment_score','sentiment_label','sentiment_data','snapshot_data','created_at'}


def test_combined_late_publication_failure_rolls_back_thesis_preparation():
    from intelligence.runtime_schema import prepare_runtime_schema
    engine=Engine();engine.fail_at='idx_market_briefings_date'
    with pytest.raises(RuntimeError):prepare_runtime_schema(engine,thesis_schema='synthetic',publication_schema='synthetic')
    assert any('ALTER TABLE' in s for s in engine.statements)
    assert engine.commits==0 and engine.rollbacks==1


def test_combined_schema_requires_both_explicit_namespaces_before_effects():
    from intelligence.runtime_schema import prepare_runtime_schema
    engine=Engine()
    with pytest.raises(ValueError):prepare_runtime_schema(engine,thesis_schema='synthetic',publication_schema=None)
    assert engine.statements==[]
