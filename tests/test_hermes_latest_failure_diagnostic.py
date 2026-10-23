"""Actual function/query, SQLite dialect-adapted synthetic rows; PostgreSQL UNRUN."""
import argparse
import ast
import contextlib
import datetime as dt
from pathlib import Path
import sqlite3
import sys
import types
import unittest

ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/'scripts/hermes_fixers.py'
NOW=dt.datetime(2026,10,21,21,tzinfo=dt.timezone.utc)
WINDOW=(NOW-dt.timedelta(hours=24)).isoformat()

class Log:
 def __init__(self):self.records=[]
 def info(self,template,**kwargs):self.records.append(('INFO',template.format(**kwargs)))
 def warning(self,template,**kwargs):self.records.append(('WARNING',template.format(**kwargs)))

def row(source=1,hours=1,error='SYNTH_NEW',status='FAILED'):
 return (source,status,(NOW-dt.timedelta(hours=hours)).isoformat(),error)

def projection(path):
 tree=ast.parse(path.read_bytes());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='diagnose_and_fix_pulls')
 future=ast.ImportFrom(module='__future__',names=[ast.alias(name='annotations')],level=0)
 mod=ast.fix_missing_locations(ast.Module(body=[future,fn],type_ignores=[]))
 log=Log();ns={'log':log};exec(compile(mod,str(path),'exec'),ns)
 return ns['diagnose_and_fix_pulls'],log

class Adapter:
 def __init__(self,db):self.db=db;self.raw_queries=[];self.adapted_queries=[];self.rows=None
 def connect(self):return contextlib.nullcontext(self)
 def execute(self,query):
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

def exercise(rows,catalog=((1,'SYNTH_A'),(2,'SYNTH_B')),source=None):
 fn,log=projection(source or SOURCE)
 db=sqlite3.connect(':memory:')
 try:
  db.execute('CREATE TABLE source_catalog(id INTEGER PRIMARY KEY,name TEXT)')
  db.execute('CREATE TABLE raw_series(source_id INTEGER,pull_status TEXT,pull_timestamp TEXT,raw_payload TEXT)')
  db.executemany('INSERT INTO source_catalog VALUES(?,?)',catalog)
  db.executemany('INSERT INTO raw_series VALUES(?,?,?,?)',rows)
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

if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--source',type=Path,default=SOURCE)
 args,rest=parser.parse_known_args();SOURCE=args.source
 unittest.main(argv=[__file__]+rest,verbosity=2)
