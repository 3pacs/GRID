"""Actual producer AST projections with inert SDK/DataFrame/transaction fixtures.

These fixtures test producer control flow and unchanged scheduler classification.
They do not qualify pandas/fedfred/SQLAlchemy/native DB behavior.
"""
from __future__ import annotations
import ast
import builtins
import io
import json
import math
import re
import types
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

DAY = date(2026, 10, 14)


def missing(x):
    return x is None or isinstance(x, float) and math.isnan(x)


class Series(list):
    name = None
    def isna(self): return Series(missing(x) for x in self)
    def sum(self): return sum(self)
    def to_numpy(self): return self


class DateIndex(Series): pass


class Frame:
    def __init__(self, data=None, columns=None, index=None):
        self.data = {k: Series(v) for k, v in (data or {}).items()}
        for k in columns or []: self.data.setdefault(k, Series())
        self.index = index if index is not None else Series(range(len(self)))
        self.iloc = types.SimpleNamespace(__unused__=True)
    def __len__(self): return len(next(iter(self.data.values()), []))
    @property
    def empty(self): return len(self) == 0
    @property
    def columns(self): return list(self.data)
    @columns.setter
    def columns(self, values): self.data = dict(zip(values, self.data.values()))
    def __getitem__(self, k): return self.data[k]
    def __setitem__(self, k, v): self.data[k] = Series(v)
    def iterrows(self):
        for i in range(len(self)): yield i, {k: v[i] for k, v in self.data.items()}
    def dropna(self, subset):
        good = [i for i in range(len(self)) if all(not missing(self.data[k][i]) for k in subset)]
        return Frame({k: [v[i] for i in good] for k, v in self.data.items()})
    def select_dtypes(self, include):
        return Frame({k: v for k, v in self.data.items() if numeric_dtype(v)})


def numeric_dtype(v):
    return any(type(x) in (int, float) for x in v) and all(missing(x) or type(x) in (int, float) for x in v)


def convert(x, kind, errors=None):
    if isinstance(x, (list, tuple, Series)):
        return Series(convert(v, kind, errors) for v in x)
    try:
        if kind == 'date':
            if isinstance(x, datetime): return x
            if isinstance(x, date): return datetime.combine(x, datetime.min.time())
            return datetime.fromisoformat(str(x))
        return float(x)
    except (ValueError, TypeError):
        if errors == 'coerce': return None
        raise


class Pandas(types.SimpleNamespace):
    def __init__(self):
        super().__init__(DataFrame=Frame, Series=Series, DatetimeIndex=DateIndex,
            to_datetime=lambda x, errors=None: convert(x, 'date', errors),
            to_numeric=lambda x, errors=None: convert(x, 'number', errors), isna=missing,
            api=types.SimpleNamespace(types=types.SimpleNamespace(is_numeric_dtype=numeric_dtype)))
        self.frame = Frame({'date': ['2026-10-14'], '2026q4': [2.5]})
        self.ExcelFile = lambda b: types.SimpleNamespace(sheet_names=['Forecast'])
        self.read_excel = lambda *a, **k: self.frame


class NullLog:
    def __getattr__(self, k): return lambda *a, **kw: None


class Result:
    def __init__(self, count=1, rows=()): self.rowcount, self.rows = count, rows
    def fetchall(self): return self.rows


class Engine:
    def __init__(self, count=1, commit_error=False, execute_error=False, rows=()):
        self.count, self.commit_error, self.execute_error, self.rows = count, commit_error, execute_error, rows
        self.executed = []; self.commits = 0; self.last_result = None
    def begin(self): return self
    def connect(self): return self
    def __enter__(self): return self
    def __exit__(self, typ, exc, tb):
        if typ is None:
            if self.commit_error: raise RuntimeError('COMMIT_ACK_LOST')
            self.commits += 1
        return False
    def execute(self, sql, args):
        self.executed.append((str(sql), args))
        if self.execute_error: raise RuntimeError('EXECUTE_FAILED')
        selected = getattr(self, "select_rows", {}).get(args.get("sid"), self.rows)
        self.last_result = Result(self.count, selected)
        return self.last_result


FILES = {'yield': ('ingestion/altdata/yield_curve_full.py', 'FullYieldCurvePuller'),
         'repo': ('ingestion/altdata/repo_market.py', 'RepoMarketPuller'),
         'fed': ('ingestion/altdata/fed_liquidity.py', 'FedLiquidityPuller'),
         'ny': ('ingestion/altdata/nyfed.py', 'NYFedPuller'),
         'tiingo': ('ingestion/tiingo_fundamentals_pull.py', 'TiingoFundamentalsPuller')}


def project(path, pd=None, overrides=None):
    pd = pd or Pandas()
    ns = dict(Any=object, date=date, datetime=datetime, timedelta=timedelta,
        math=math, io=io, log=NullLog(), pd=pd, text=lambda x: x, Engine=object,
        BasePuller=type('BasePuller', (), {}), time=types.SimpleNamespace(sleep=lambda t: None), _TIINGO_API_KEY='SYNTHETIC',
        _BASE_URL='https://api.tiingo.com', _PFX='NYFED')
    ns.update(overrides or {})
    def importer(name, *args, **kw):
        if name == 'pandas': return pd
        if name == 'sqlalchemy': return types.SimpleNamespace(text=lambda x: x)
        if name == 'fedfred': return types.SimpleNamespace(FredAPI=lambda key: ns['fred'])
        if name == 'time': return types.SimpleNamespace(time=lambda: 1791936000.0)
        if name == '_strptime': return builtins.__import__(name)
        if name == '__future__': return builtins.__import__(name)
        if name in ('re', 'json'): return {'re': re, 'json': json}[name]
        raise AssertionError('UNEXPECTED_TARGET_IMPORT:' + name)
    ns['__builtins__'] = dict(vars(builtins), __import__=importer)
    tree = ast.parse(path.read_text())
    body = [ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)]
    for node in tree.body:
        if path.name == "base.py" and not (isinstance(node, ast.ClassDef) and node.name == "BasePuller"):
            continue
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            for child in ast.walk(node):
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)): child.decorator_list = []
            body.append(node)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            try: ast.literal_eval(node.value)
            except (ValueError, TypeError): continue
            body.append(node)
    exec(compile(ast.fix_missing_locations(ast.Module(body=body,type_ignores=[])), str(path), 'exec'),ns)
    return ns


CLASSIFIERS = {}

def classifier(ref):
    if str(ref) not in CLASSIFIERS:
        CLASSIFIERS[str(ref)] = project(ref)["_classify_outcome"]
    return CLASSIFIERS[str(ref)]


def puller(root, key, *, frame=None, engine=None, data=None, overrides=None):
    rel, name = FILES[key]; pd = Pandas()
    if frame is not None: pd.frame = frame
    ns = project(root / rel, pd, overrides)
    obj = ns[name].__new__(ns[name]); obj.engine=engine or Engine(); obj.source_id=1; obj._api_key='SYNTHETIC'
    obj._row_exists = lambda *a: False; obj._get_existing_dates=lambda *a: set()
    base_ns = project(root / 'ingestion/base.py')
    obj.validate_row = lambda *a: None
    obj._insert_raw = types.MethodType(base_ns['BasePuller']._insert_raw, obj)
    ns['fred']=types.SimpleNamespace(get_series_observations=lambda *a, **kw: frame if frame is not None else Frame(columns=['date','value']))
    class HTTPError(Exception): pass
    ns['requests']=types.SimpleNamespace(get=lambda *a, **kw: types.SimpleNamespace(
        raise_for_status=lambda:None,json=lambda:data),exceptions=types.SimpleNamespace(HTTPError=HTTPError))
    return obj, ns, pd


ROOT = Path(__file__).resolve().parents[1]
REF = ROOT / "ingestion/smart_scheduler.py"


def baseline(root, ref):
    classify=classifier(ref); out=[]
    for key in ('yield','repo','fed'):
        p, _, _ = puller(root,key)
        leaf = p._pull_raw_series('x', DAY, DAY) if key=='fed' else p._pull_raw_series('x','FRED',DAY,DAY)
        assert leaf['status']=='NO_DATA' and classify(leaf)[0]=='FAILED'
        out.append({'source':key,'producer':leaf,'classifier':classify(leaf)})
    p,_,_=puller(root,'tiingo',data=[]);leaf=p.pull_ticker('AAPL',DAY,DAY)
    assert leaf['status']=='NO_DATA' and classify(leaf)[0]=='FAILED'
    out.append({'source':'tiingo','producer':leaf,'classifier':classify(leaf)})
    p,_,_=puller(root,'ny');p._fetch_bytes=lambda url:b'SYNTHETIC'
    leaf=p._pull_nowcast()[1]
    assert leaf['status']=='NO_DATA' and classify(leaf)[0]=='FAILED'
    out.append({'source':'ny_next_quarter','producer':leaf,'classifier':classify(leaf)})
    return out


class OutcomeTests(unittest.TestCase):
    def classify(self, x): return classifier(REF)(x)
    def make(self, key, **kw): return puller(ROOT, key, **kw)
    def raw(self, key, **kw):
        p,ns,pd=self.make(key,**kw)
        res=p._pull_raw_series('SERIES',DAY,DAY) if key=='fed' else p._pull_raw_series('feature','SERIES',DAY,DAY)
        return res,p,ns,pd
    def assert_clean(self, leaf):
        self.assertEqual(self.classify(leaf)[0], 'NO_NEW_DATA')
        self.assertEqual(leaf['rows_inserted'],0)
    def assert_failed(self, leaf):
        self.assertIn(self.classify(leaf)[0],('FAILED','SKIPPED'))
        self.assertTrue(leaf.get('error') or leaf.get('errors'))

    def test_base_legacy_and_opt_in_exact_result(self):
        ns=project(ROOT/'ingestion/base.py');p=ns['BasePuller'].__new__(ns['BasePuller']);p.source_id=7
        validation=[];p.validate_row=lambda *a:validation.append(a);e=Engine(2)
        args=dict(conn=e,series_id='series',obs_date=DAY,value=1.25,raw_payload={'x':1})
        self.assertIsNone(p._insert_raw(**args));first=e.executed[-1]
        got=p._insert_raw(**args,return_result=True)
        self.assertIs(got,e.last_result);self.assertEqual(got.rowcount,2)
        self.assertEqual(first,e.executed[-1]);self.assertEqual(len(validation),2)
        self.assertIn('INSERT INTO raw_series',first[0]);self.assertEqual(first[1]['status'],'SUCCESS')
        self.assertEqual(first[1]['val'],1.25);self.assertEqual(first[1]['od'],DAY)
        e.execute_error=True
        with self.assertRaisesRegex(RuntimeError,'EXECUTE_FAILED'):p._insert_raw(**args,return_result=True)

    def test_raw_clean_empty_actual_fetch(self):
        for key in ('yield','repo','fed'):
            with self.subTest(key=key):
                res,p,_,_=self.raw(key);self.assert_clean(res);self.assertEqual(p.engine.executed,[])

    def test_raw_provider_unsupported_and_malformed(self):
        for key in ('yield','repo','fed'):
            for frame in (None,{},False,Frame({'wrong':[]}),Frame({'date':['bad'],'value':['bad']}),Frame({'date':['2026-10-14'],'value':[float('inf')]})):
                with self.subTest(key=key,frame=str(frame)):
                    p,ns,pd=self.make(key,frame=frame)
                    ns['fred'].get_series_observations=lambda *a,**kw:frame
                    res=p._pull_raw_series('SERIES',DAY,DAY) if key=='fed' else p._pull_raw_series('feature','SERIES',DAY,DAY)
                    self.assert_failed(res);self.assertEqual(res['rows_inserted'],0);self.assertEqual(p.engine.executed,[])

    def test_raw_sdk_date_keyerror_not_empty(self):
        p,ns,_=self.make('fed')
        def failed(*a,**kw):raise KeyError('date')
        ns['fred'].get_series_observations=failed
        res=p._pull_raw_series('WALCL',DAY,DAY)
        self.assert_failed(res);self.assertEqual(p.engine.commits,0)

    def test_raw_driver_count_and_committed_values(self):
        for key in ('yield','repo','fed'):
            for count in (0,2):
                with self.subTest(key=key,count=count):
                    res,p,_,_=self.raw(key,frame=Frame({'date':['2026-10-14'],'value':[4.25]}),engine=Engine(count))
                    self.assertEqual(res['rows_inserted'],count);self.assertEqual(p.engine.commits,1)
                    self.assertEqual(self.classify(res)[0],'SUCCESS' if count else 'NO_NEW_DATA')
                    self.assertEqual(p.engine.executed[0][1]['val'],4.25)
                    self.assertEqual(p.engine.executed[0][1]['od'],DAY)

    def test_raw_unknown_count_and_commit_failure(self):
        for key in ('yield','repo','fed'):
            for count,fail in ((None,False),(-1,False),(True,False),(1.0,False),(1,True)):
                with self.subTest(key=key,count=count,commit=fail):
                    res,p,_,_=self.raw(key,frame=Frame({'date':['2026-10-14'],'value':[1]}),engine=Engine(count,commit_error=fail))
                    self.assert_failed(res);self.assertIsNone(res['rows_inserted']);self.assertNotEqual(self.classify(res)[0],'NO_NEW_DATA')

    def test_fed_observation_index_and_liquidity_units(self):
        idx=DateIndex([datetime(2026,10,13)]);idx.name='date'
        p,ns,_=self.make('fed',frame=Frame({'date':['2026-10-14'],'value':[7]},index=idx))
        res=p._pull_raw_series('WALCL',DAY,DAY)
        self.assertEqual(p.engine.executed[0][1]['od'],date(2026,10,13));self.assertEqual(res['rows_inserted'],1)
        self.assertEqual(ns['net_liquidity_millions'](7_000_000,750_000,450),5_800_000)

    def test_tiingo_literal_empty_only(self):
        for data,clean in (([],True),(None,False),({},False),(False,False),(0,False),('',False),([None],False),([{}],False),([{'date':'bad','marketCap':1}],False)):
            with self.subTest(data=data):
                p,_,_=self.make('tiingo',data=data);res=p.pull_ticker('AAPL',DAY,DAY)
                if clean:self.assert_clean(res)
                else:self.assert_failed(res)

    def test_tiingo_counts_commit_and_value_validation(self):
        for count,fail in ((0,False),(2,False),(None,False),(-1,False),(True,False),(1.0,False),(1,True)):
            with self.subTest(count=count,commit=fail):
                p,_,_=self.make('tiingo',data=[{'date':'2026-10-14','marketCap':123.5}],engine=Engine(count,commit_error=fail))
                res=p.pull_ticker('AAPL',DAY,DAY)
                if type(count) is int and count>=0 and not fail:
                    self.assertEqual(res['rows_inserted'],count);self.assertEqual(p.engine.executed[0][1]['val'],123.5)
                else:self.assert_failed(res);self.assertIsNone(res['rows_inserted'])
        for val in (True,float('nan'),float('inf'),'bad'):
            p,_,_=self.make('tiingo',data=[{'date':'2026-10-14','marketCap':val}]);self.assert_failed(p.pull_ticker('AAPL',DAY,DAY))

    def nowcast(self, **kw):
        p,ns,pd=self.make('ny',**kw);p._fetch_bytes=lambda url:b'SYNTHETIC';return p,ns,pd

    def test_nowcast_next_quarter_absence_clean(self):
        p,_,_=self.nowcast();out=p._pull_nowcast()
        self.assertEqual(out[0]['rows_inserted'],1);self.assert_clean(out[1]);self.assertEqual(self.classify(out)[0],'SUCCESS')

    def test_nowcast_no_columns_and_commit_failure(self):
        p,_,_=self.nowcast(frame=Frame({'date':['2026-10-14'],'bad':['text']}))
        for leaf in p._pull_nowcast():self.assert_failed(leaf);self.assertEqual(leaf['rows_inserted'],0)
        for count,commit in ((None,False),(True,False),(1,True)):
            p,_,_=self.nowcast(engine=Engine(count,commit_error=commit));out=p._pull_nowcast()
            self.assertEqual(len(out),2)
            for leaf in out:self.assert_failed(leaf);self.assertIsNone(leaf['rows_inserted'])

    def soma(self, *, summary=None, asof=None, engine=None):
        p,ns,pd=self.make('ny',engine=engine)
        valid={'soma':{'summary':[{'parValue':1_000_000_000,'securityType':'TREASURY'},{'parValue':300_000_000,'securityType':'MBS'}]}}
        values=iter([{'soma':{'asOfDate':'2026-10-14'}} if asof is None else asof,valid if summary is None else summary])
        p._fetch_json=lambda url:next(values)
        return p

    def test_soma_units_and_commit_count(self):
        p=self.soma(engine=Engine(2));out=p._pull_soma()
        self.assertEqual([r['rows_inserted'] for r in out],[2,2,2])
        self.assertEqual([args['val'] for sql,args in p.engine.executed],[1.3,1.0,0.3])
        self.assertEqual([args['od'] for sql,args in p.engine.executed],[DAY]*3)

    def test_soma_missing_fields_not_fabricated_zero(self):
        for summary,asof in (({},None),({'soma':{'summary':[]}},None),({'soma':{'summary':[{}]}},None),({'soma':{'summary':[{'parValue':1,'securityType':'TREASURY'}]}},None),(None,{})):
            with self.subTest(summary=summary,asof=asof):
                p=self.soma(summary=summary,asof=asof);out=p._pull_soma();self.assertEqual(len(out),3)
                for leaf in out:self.assert_failed(leaf);self.assertEqual(leaf['rows_inserted'],0)
                self.assertEqual(p.engine.executed,[])

    def test_soma_commit_failure_discards_staged_successes(self):
        for count,commit in ((None,False),(-1,False),(True,False),(1,True)):
            p=self.soma(engine=Engine(count,commit_error=commit));out=p._pull_soma()
            self.assertEqual(len(out),3)
            for leaf in out:self.assert_failed(leaf);self.assertIsNone(leaf['rows_inserted'])

    def test_treasury_operations_shape_and_math(self):
        for data,clean in (([],True),({},False),([{}],False),([{'date':'bad','accepted':1}],False),([{'date':'2026-10-14'}],False)):
            with self.subTest(data=data):
                p,_,_=self.make('ny');p._fetch_json=lambda url:data;leaf=p._pull_treasury_ops()[0]
                if clean:self.assert_clean(leaf)
                else:self.assert_failed(leaf)
        p,_,_=self.make('ny');p._fetch_json=lambda url:[{'date':'2026-10-14','accepted':10_000_000_000}]
        leaf=p._pull_treasury_ops()[0];self.assertEqual(leaf['rows_inserted'],1);self.assertEqual(p.engine.executed[0][1]['val'],10.0)

    def test_prior_committed_leaves_survive_later_failure(self):
        p,_,_=self.nowcast();p._pull_soma=lambda:[{'feature':'soma','status':'FAILED','rows_inserted':None,'error':'COMMIT_ACK_LOST'}]
        p._pull_treasury_ops=lambda:[{'feature':'ops','status':'NO_NEW_DATA','rows_inserted':0}]
        out=p.pull_all();self.assertEqual(out[0]['rows_inserted'],1)
        self.assertIsNone(out[2]['rows_inserted']);self.assertEqual(self.classify(out)[0],'PARTIAL')

    def test_derived_failure_retained_alongside_prior_raw(self):
        for key in ('yield','repo','fed'):
            with self.subTest(key=key):
                p,ns,_=self.make(key)
                p._pull_raw_series=lambda *a,**kw:{'feature':'raw','status':'SUCCESS','rows_inserted':2}
                p._pull_supporting_series=lambda *a,**kw:[];p._get_latest_date=lambda *a:None
                def fail(*a,**kw):raise RuntimeError('COMMIT_ACK_LOST')
                p._compute_derived=fail
                out=p.pull_all();self.assertEqual(out[-1]['status'],'FAILED')
                self.assertIn('rows_inserted',out[-1]);self.assertIsNone(out[-1]['rows_inserted']);self.assertEqual(self.classify(out)[0],'PARTIAL')

    def test_nowcast_wide_parser_preserved(self):
        p,ns,pd=self.make('ny');frame=Frame({'date':[datetime(2026,3,27)],'2002q1':[None],'2026q1':[3.5],'2026q2':[1.9]})
        out=ns['_extract_nowcast_series'](frame,'date');self.assertEqual(out,[(date(2026,3,27),3.5,1.9,'2026q1','2026q2')])
        self.assertEqual(ns['_quarter_sort_key']('Q4-2025'),(2025,4))


    def test_derived_actual_values_and_commit_boundary(self):
        maps={
          'yield':{'yc_5y':[(DAY,4)],'yc_30y':[(DAY,5)],'yc_2y':[(DAY,3)],'yc_10y':[(DAY,4.5)]},
          'repo':{'sofr_rate':[(DAY,5)],'fed_funds_effective':[(DAY,4)],'reverse_repo_usage':[(DAY,100)],'treasury_bill_3m':[(DAY,5.5)]},
        }
        for key,expected in (('yield',[1.0,0.5]),('repo',[1.0,1.0,0.5])):
            with self.subTest(key=key):
                e=Engine();e.select_rows=maps[key];p,_,_=self.make(key,engine=e)
                out=p._compute_derived(DAY,DAY)
                values=[args['val'] for sql,args in e.executed if 'INSERT INTO raw_series' in sql]
                self.assertEqual(values,expected);self.assertEqual([x['rows_inserted'] for x in out],[1]*len(expected))
                e.commit_error=True
                with self.assertRaisesRegex(RuntimeError,'COMMIT_ACK_LOST'):p._compute_derived(DAY,DAY)
        p,_,_=self.make('fed');series={'WALCL':{DAY:7_000_000},'WTREGEN':{DAY:750_000},'RRPONTSYD':{DAY:450}}
        p._load_series_from_db=lambda sid,*a:series[sid]
        out=p._compute_derived(DAY,DAY)
        insert=[a for sql,a in p.engine.executed if 'INSERT INTO raw_series' in sql]
        self.assertEqual(insert[0]['val'],5_800_000);self.assertEqual(insert[0]['od'],DAY)
        payload=json.loads(insert[0]['payload']);self.assertEqual(payload['rrpontsyd_millions'],450_000)
        self.assertEqual(out[0]['rows_inserted'],1)
        p.engine.commit_error=True
        with self.assertRaisesRegex(RuntimeError,'COMMIT_ACK_LOST'):p._compute_derived(DAY,DAY)

    def test_literal_zero_soma_is_observed_not_fabricated(self):
        p=self.soma(summary={'soma':{'summary':[{'parValue':0,'securityType':'TREASURY'},{'parValue':0,'securityType':'MBS'}]}})
        out=p._pull_soma()
        for leaf in out:self.assert_clean(leaf)
        self.assertEqual(p.engine.executed,[])

    def test_treasury_unknown_commit_count(self):
        for count,commit in ((None,False),(-1,False),(True,False),(1,True)):
            p,_,_=self.make('ny',engine=Engine(count,commit_error=commit))
            p._fetch_json=lambda url:[{'date':'2026-10-14','accepted':10_000_000_000}]
            leaf=p._pull_treasury_ops()[0];self.assert_failed(leaf);self.assertIsNone(leaf['rows_inserted'])


    def test_v2_nowcast_malformed_vintage_refuses_before_writes(self):
        for dates in (['2026-10-14','bad'],['2026-10-14','2026-99-99'],['bad']):
            with self.subTest(dates=dates):
                p,_,_=self.nowcast(frame=Frame({'date':dates,'2026q4':[2.5]*len(dates)}))
                out=p._pull_nowcast()
                self.assertEqual(self.classify(out)[0],'FAILED');self.assertEqual(p.engine.executed,[])
                for leaf in out:self.assertEqual(leaf['rows_inserted'],0)

    def test_v2_forecast_nonfinite_vs_absent_and_valid(self):
        for value in (float('inf'),float('-inf'),'inf'):
            with self.subTest(invalid=repr(value)):
                p,_,_=self.nowcast(frame=Frame({'date':['2026-10-14'],'2026q3':[2.5],'2026q4':[value]}))
                out=p._pull_nowcast();self.assertEqual(self.classify(out)[0],'FAILED');self.assertEqual(p.engine.executed,[])
        for value,rows in ((None,0),(float('nan'),0),(0.0,1),(1.9,1)):
            with self.subTest(valid=repr(value)):
                p,_,_=self.nowcast(frame=Frame({'date':['2026-10-14'],'2026q3':[2.5],'2026q4':[value]}))
                out=p._pull_nowcast();self.assertEqual(self.classify(out)[0],'SUCCESS')
                self.assertEqual(out[1]['rows_inserted'],rows)
                if rows==0:self.assertEqual(out[1]['status'],'NO_NEW_DATA')

    def test_v2_soma_final_fallback_guard_and_valid_units(self):
        for value,valid in (('nan',False),('inf',False),('-inf',False),('-1',False),('-0.001',False),('0',True),('1000000000',True)):
            with self.subTest(total=value):
                packet={'soma':{'summary':[{'parValue':0,'securityType':'TREASURY','totalParValue':value},{'parValue':0,'securityType':'MBS'}]}}
                p=self.soma(summary=packet);out=p._pull_soma()
                if valid:
                    self.assertEqual(self.classify(out)[0],'SUCCESS' if float(value)>0 else 'NO_NEW_DATA')
                    if float(value)>0:self.assertEqual(p.engine.executed[0][1]['val'],float(value)/1_000_000_000)
                else:
                    self.assertEqual(self.classify(out)[0],'FAILED');self.assertEqual(p.engine.executed,[])
