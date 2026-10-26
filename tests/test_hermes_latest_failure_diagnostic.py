"""Actual function/query, SQLite dialect-adapted synthetic rows; PostgreSQL UNRUN."""
import argparse
import ast
import contextlib
import datetime as dt
import json
import re
from pathlib import Path
import sqlite3
import sys
import types
import unittest

ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/'scripts/hermes_fixers.py'
HEALTH=ROOT/'scripts/hermes_health.py'
NOW=dt.datetime(2026,10,21,21,tzinfo=dt.timezone.utc)
WINDOW=(NOW-dt.timedelta(hours=24)).isoformat()

class Log:
 def __init__(self):self.records=[]
 def info(self,template,**kwargs):self.records.append(('INFO',template.format(**kwargs)))
 def warning(self,template,**kwargs):self.records.append(('WARNING',template.format(**kwargs)))

def row(source=1,hours=1,error='SYNTH_NEW',status='FAILED'):
 return (source,status,(NOW-dt.timedelta(hours=hours)).isoformat(),error)

def projection(path):
 tree=ast.parse(path.read_bytes())
 projected=('_normalize_source_key','_classify_pull_failure','diagnose_and_fix_pulls')
 functions={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in projected}
 if set(functions)!=set(projected):raise AssertionError('MISSING_PROJECTED_DIAGNOSTIC_DEPENDENCY')

 # The optional-relation probe is the actual hermes_health helper.
 health=ast.parse(HEALTH.read_bytes())
 probe=[n for n in health.body if isinstance(n,ast.FunctionDef) and n.name=='_relation_exists']
 if len(probe)!=1:raise AssertionError('MISSING_RELATION_PROBE')
 future=ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0)
 mod=ast.fix_missing_locations(ast.Module(body=[future,*probe,*(functions[name] for name in projected)],type_ignores=[]))
 log=Log();ns={'log':log,'json':json,'re':re};exec(compile(mod,str(path),'exec'),ns)
 return ns['diagnose_and_fix_pulls'],log

PROBE='SELECT to_regclass(:relation) IS NOT NULL'

class Adapter:
 def __init__(self,db):self.db=db;self.raw_queries=[];self.adapted_queries=[];self.probes=[];self.rows=None;self.value=None
 def connect(self):return contextlib.nullcontext(self)
 def execute(self,query,params=None):
  if query==PROBE:
   # Sole dialect substitution for to_regclass: SQLite's catalog lookup.
   self.probes.append(params['relation'])
   self.value=self.db.execute("SELECT COUNT(*)>0 FROM sqlite_master WHERE type='table' AND name=?",(params['relation'],)).fetchone()[0]
   return self
  if params is not None:raise AssertionError('UNEXPECTED_PARAMS')
  self.raw_queries.append(query)
  if not isinstance(query,str) or not query.startswith('SELECT '):raise AssertionError('SELECT_ONLY_INERT_FIXTURE')
  # Sole dialect substitutions: TEXT fixture already represents payload::text;
  # fixed synthetic window replaces the PostgreSQL current-time expression.
  if query.count("NOW() - INTERVAL '24 hours'")!=1:raise AssertionError('UNEXPECTED_WINDOW_DIALECT')
  adapted=query.replace('::text','').replace("NOW() - INTERVAL '24 hours'",':window_start')
  if '::' in adapted:raise AssertionError('UNSUPPORTED_CAST')
  self.adapted_queries.append(adapted)
  raw=self.db.execute(adapted,{'window_start':WINDOW}).fetchall()
  self.rows=[(x[0],x[1],dt.datetime.fromisoformat(x[2]) if x[2] else None,x[3]) for x in raw]
  return self
 def fetchall(self):return self.rows
 def scalar(self):return self.value

def plrow(name='synth_job',source=None,hours=1,error='PL_SYNTH',status='FAILED',completed=True):
 stamp=(NOW-dt.timedelta(hours=hours)).isoformat()
 return (name,source,stamp,stamp if completed else None,status,error)

PULL_LOG_DDL='CREATE TABLE pull_log(puller_name TEXT,source_id INTEGER,started_at TEXT,completed_at TEXT,status TEXT,error_message TEXT)'

def exercise(rows,catalog=((1,'SYNTH_A'),(2,'SYNTH_B')),source=None,pull_log=(),pull_log_ddl=PULL_LOG_DDL):
 fn,log=projection(source or SOURCE)
 db=sqlite3.connect(':memory:')
 try:
  db.execute('CREATE TABLE source_catalog(id INTEGER PRIMARY KEY,name TEXT)')
  db.execute('CREATE TABLE raw_series(source_id INTEGER,pull_status TEXT,pull_timestamp TEXT,raw_payload TEXT)')
  db.executemany('INSERT INTO source_catalog VALUES(?,?)',catalog)
  db.executemany('INSERT INTO raw_series VALUES(?,?,?,?)',rows)
  if pull_log_ddl is not None:
   db.execute(pull_log_ddl)
   db.executemany('INSERT INTO pull_log VALUES(?,?,?,?,?,?)',pull_log)
  elif pull_log:raise AssertionError('ROWS_WITHOUT_PULL_LOG')
  adapter=Adapter(db);fake=types.ModuleType('sqlalchemy');fake.text=lambda text:text
  previous=sys.modules.get('sqlalchemy');sys.modules['sqlalchemy']=fake
  try:result=fn(adapter,hermes_available=False,state=object(),dry_run=True)
  finally:
   if previous is None:sys.modules.pop('sqlalchemy',None)
   else:sys.modules['sqlalchemy']=previous
  return adapter,result,log.records
 finally:db.close()

class LatestFailureTests(unittest.TestCase):
 def test_older_lexically_larger_error_cannot_masquerade_as_latest(self):
  a,result,logs=exercise([row(hours=2,error='ZZZ_SYNTH_OLDER'),row(hours=1,error='AAA_SYNTH_NEWER')])
  self.assertEqual(a.rows,[('SYNTH_A',2,NOW-dt.timedelta(hours=1),'AAA_SYNTH_NEWER')])
  warning=next(text for level,text in logs if level=='WARNING')
  self.assertIn('AAA_SYNTH_NEWER',warning);self.assertNotIn('ZZZ_SYNTH_OLDER',warning)
  self.assertEqual(result['retried'],0);self.assertEqual(result['diagnosed'],0)

 def test_latest_null_error_is_unknown_not_an_older_message(self):
  a,_,_=exercise([row(hours=2,error='ZZZ_SYNTH_OLDER'),row(hours=1,error=None)])
  self.assertEqual(a.rows,[('SYNTH_A',2,NOW-dt.timedelta(hours=1),None)])

 def test_equal_latest_timestamp_preserves_lexical_max_tie_policy(self):
  a,_,_=exercise([row(error='AAA'),row(error='ZZZ'),row(error=None),row(hours=3,error='ZZZZ_OLDER')])
  self.assertEqual(a.rows,[('SYNTH_A',4,NOW-dt.timedelta(hours=1),'ZZZ')])

 def test_source_name_grain_counts_and_descending_order_preserved(self):
  a,_,_=exercise([row(1,3,'Z_OLDER'),row(3,1,'A_NEWER'),row(1,2,'MIDDLE'),row(2,1,'ONLY')],catalog=((1,'SYNTH_A'),(2,'SYNTH_B'),(3,'SYNTH_A')))
  self.assertEqual(a.rows,[('SYNTH_A',3,NOW-dt.timedelta(hours=1),'A_NEWER'),('SYNTH_B',1,NOW-dt.timedelta(hours=1),'ONLY')])

 def test_failed_only_and_strict_24h_filter_preserved(self):
  a,_,_=exercise([row(hours=1),row(hours=0,error='SUCCESS_MUST_NOT_COUNT',status='SUCCESS'),row(hours=24,error='BOUNDARY_EXCLUDED'),row(hours=25,error='OLDER_EXCLUDED'),(1,'FAILED',None,'NULL_TIME_EXCLUDED')])
  self.assertEqual(a.rows,[('SYNTH_A',1,NOW-dt.timedelta(hours=1),'SYNTH_NEW')])

 def test_single_failure_payload_and_newest_timestamp_are_unchanged(self):
  a,_,_=exercise([row(error='ONE_SYNTHETIC_ERROR')])
  self.assertEqual(a.rows,[('SYNTH_A',1,NOW-dt.timedelta(hours=1),'ONE_SYNTHETIC_ERROR')])

 def test_future_timestamp_policy_is_not_silently_changed(self):
  # Existing query has no upper bound. Preserve that policy; this is not PIT acceptance.
  a,_,_=exercise([row(hours=-1,error='FUTURE_SYNTH')])
  self.assertEqual(a.rows,[('SYNTH_A',1,NOW+dt.timedelta(hours=1),'FUTURE_SYNTH')])

 def test_empty_is_no_failed_rows_not_ingestion_success(self):
  a,result,logs=exercise([])
  self.assertEqual(a.rows,[]);self.assertEqual(len(a.raw_queries),1)
  self.assertEqual(result,{'retried':0,'fixed':0,'diagnosed':0,'skipped_cooldown':0,'skipped_no_handler':0,'skipped_key_check':0,'escalated':0,'abandoned':0})
  self.assertEqual(logs,[('INFO','No failed pulls in last 24h')])

 def test_projection_runs_actual_database_and_auth_guards_without_retry(self):
  a,result,_=exercise([
   row(source=1,error='(psycopg2.OperationalError) remaining connection slots are reserved'),
   row(source=2,error='HTTP 429 Too Many Requests'),
  ],catalog=((1,'FRED'),(2,'Baltic_Exchange')))
  self.assertEqual(result['fix_actions'],{'fred':'CHECK_DB','baltic_exchange':'CHECK_KEY'})
  self.assertEqual(result['retried'],0);self.assertEqual(result['diagnosed'],0)
  self.assertEqual(len(a.raw_queries),1)
  self.assertTrue(all(query.startswith('SELECT ') for query in a.raw_queries))

 def test_projection_uses_source_helpers_not_classifier_stubs(self):
  fn,_=projection(SOURCE)
  for name in ('_normalize_source_key','_classify_pull_failure'):
   self.assertEqual(fn.__globals__[name].__code__.co_filename,str(SOURCE))
  self.assertEqual(fn.__globals__['_normalize_source_key']('Baltic-Exchange'),'baltic_exchange')
  classify=fn.__globals__['_classify_pull_failure']
  self.assertEqual(classify('HTTP 401 Unauthorized'),'provider_auth_or_quota')
  self.assertEqual(classify('RetryError[JSONDecodeError]'),'unknown')

class PullLogFailureTests(unittest.TestCase):
 # 2026-10-26: BIS/RBI/repo_market/ADS failures exist only in pull_log; a
 # raw_series-only read logged "No failed pulls in last 24h" every cycle.
 def test_pull_log_only_failure_is_not_reported_as_no_failed_pulls(self):
  a,_,logs=exercise([],pull_log=[plrow('BIS',source=1,error='RetryError[HTTPError]')])
  self.assertEqual(a.rows,[('SYNTH_A',1,NOW-dt.timedelta(hours=1),'RetryError[HTTPError]')])
  self.assertNotIn(('INFO','No failed pulls in last 24h'),logs)
  warning=next(text for level,text in logs if level=='WARNING')
  self.assertIn('SYNTH_A',warning);self.assertIn('RetryError[HTTPError]',warning)
  self.assertEqual(len(a.raw_queries),1)

 def test_unmapped_pull_log_row_uses_puller_name(self):
  a,_,_=exercise([],pull_log=[plrow('flow_materialize',source=None,error='JOB_FAILED')])
  self.assertEqual(a.rows,[('flow_materialize',1,NOW-dt.timedelta(hours=1),'JOB_FAILED')])

 def test_partial_counts_success_and_running_do_not(self):
  a,_,_=exercise([],pull_log=[
   plrow('RBI',source=2,hours=2,error='1 of 3 failed',status='PARTIAL'),
   plrow('RBI',source=2,hours=1,error='SUCCESS_MUST_NOT_COUNT',status='SUCCESS'),
   plrow('RBI',source=2,hours=1,error='RUNNING_MUST_NOT_COUNT',status='RUNNING',completed=False),
  ])
  self.assertEqual(a.rows,[('SYNTH_B',1,NOW-dt.timedelta(hours=2),'1 of 3 failed')])

 def test_pull_log_window_uses_completed_then_started_and_is_strict(self):
  a,_,_=exercise([],pull_log=[
   plrow('ADS',source=1,hours=3,error='STARTED_ONLY',completed=False),
   plrow('ADS',source=1,hours=24,error='BOUNDARY_EXCLUDED'),
   plrow('ADS',source=1,hours=30,error='OLDER_EXCLUDED'),
  ])
  self.assertEqual(a.rows,[('SYNTH_A',1,NOW-dt.timedelta(hours=3),'STARTED_ONLY')])

 def test_same_source_merges_counts_and_latest_error_across_both_tables(self):
  a,_,_=exercise([row(source=1,hours=3,error='RAW_OLDER')],
                 pull_log=[plrow('repo_market',source=1,hours=1,error='PL_NEWER'),
                           plrow('other_job',source=None,hours=2,error='OTHER')])
  self.assertEqual(a.rows,[('SYNTH_A',2,NOW-dt.timedelta(hours=1),'PL_NEWER'),
                           ('other_job',1,NOW-dt.timedelta(hours=2),'OTHER')])

 def test_plain_text_pull_log_error_reaches_the_deterministic_guards(self):
  _,result,_=exercise([],catalog=((1,'FRED'),),pull_log=[plrow('fred',source=1,error='HTTP 429 Too Many Requests')])
  self.assertEqual(result['fix_actions'],{'fred':'CHECK_KEY'})
  self.assertEqual(result['retried'],0)

class MissingPullLogTests(unittest.TestCase):
 # 2026-10-26 review F1: the repair query named pull_log unconditionally, so a
 # DB without it (supported by check_db_health) raised before diagnosing the
 # raw_series failures it handled before #913.
 def test_missing_pull_log_still_diagnoses_raw_series_failure(self):
  a,result,logs=exercise([row(source=1,hours=1,error='RAW_ONLY')],pull_log_ddl=None)
  self.assertEqual(a.probes,['pull_log'])
  self.assertEqual(a.rows,[('SYNTH_A',1,NOW-dt.timedelta(hours=1),'RAW_ONLY')])
  self.assertEqual(len(a.raw_queries),1)
  self.assertNotIn('pull_log',a.raw_queries[0])
  self.assertIsNone(result['pull_log_failures'])
  self.assertIn(('WARNING','pull_log relation not found; diagnosing raw_series failures only (pull_log failures UNKNOWN)'),logs)
  warning=[text for level,text in logs if level=='WARNING' and text.startswith('Failed sources')]
  self.assertEqual(len(warning),1);self.assertIn('RAW_ONLY',warning[0])

 def test_missing_pull_log_keeps_raw_window_and_classifier_guards(self):
  a,result,_=exercise([row(source=1,hours=1,error='HTTP 429 Too Many Requests'),
                       row(source=2,hours=24,error='BOUNDARY_EXCLUDED')],
                      catalog=((1,'FRED'),(2,'SYNTH_B')),pull_log_ddl=None)
  self.assertEqual([r[0] for r in a.rows],['FRED'])
  self.assertEqual(result['fix_actions'],{'fred':'CHECK_KEY'})
  self.assertEqual(result['retried'],0)

 def test_missing_pull_log_and_no_raw_failure_is_not_reported_as_no_failed_pulls(self):
  a,result,logs=exercise([],pull_log_ddl=None)
  self.assertEqual(a.rows,[])
  self.assertIsNone(result['pull_log_failures'])
  self.assertNotIn(('INFO','No failed pulls in last 24h'),logs)
  self.assertIn(('INFO','No failed raw_series pulls in last 24h (pull_log not checked)'),logs)

 def test_present_empty_pull_log_reads_both_tables_without_unknown_marker(self):
  a,result,logs=exercise([row(source=1,hours=1,error='RAW_ONLY')])
  self.assertEqual(a.probes,['pull_log'])
  self.assertEqual(a.rows,[('SYNTH_A',1,NOW-dt.timedelta(hours=1),'RAW_ONLY')])
  self.assertEqual(len(a.raw_queries),1);self.assertIn('FROM pull_log',a.raw_queries[0])
  self.assertNotIn('pull_log_failures',result)
  self.assertFalse(any('pull_log relation not found' in text for _,text in logs))

 def test_populated_pull_log_is_still_merged_when_present(self):
  a,result,_=exercise([row(source=1,hours=3,error='RAW_OLDER')],
                      pull_log=[plrow('repo_market',source=1,hours=1,error='PL_NEWER')])
  self.assertEqual(a.rows,[('SYNTH_A',2,NOW-dt.timedelta(hours=1),'PL_NEWER')])
  self.assertNotIn('pull_log_failures',result)

 def test_query_failure_after_probe_propagates_not_read_as_no_failures(self):
  # pull_log resolves but the statement fails: the error must surface (the
  # operator records it), never become "no failed pulls" or a raw-only result.
  with self.assertRaises(sqlite3.OperationalError):
   exercise([row(source=1,hours=1)],pull_log_ddl='CREATE TABLE pull_log(puller_name TEXT,source_id INTEGER,started_at TEXT,completed_at TEXT,status TEXT)')

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,default=SOURCE)
 args,rest=parser.parse_known_args();SOURCE=args.source
 unittest.main(argv=[__file__]+rest,verbosity=2)
