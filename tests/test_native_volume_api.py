from datetime import datetime,timezone
from fastapi import FastAPI,HTTPException
from fastapi.testclient import TestClient
from api.native_volume import build_router

def app(**kw):
 a=FastAPI();a.include_router(build_router(clock=lambda:datetime(2026,11,10,17,tzinfo=timezone.utc),**kw));return TestClient(a)
def test_unconfigured():
 r=app(authorize=lambda:None).get('/api/v1/research/native-volume',params={'symbol':'SPY','session_key':'XNYS:2026-11-10:RTH'});assert r.status_code==503;assert 'private' in r.headers['cache-control']
def test_authorization():
 def denied():raise HTTPException(403)
 assert app(authorize=denied).get('/api/v1/research/native-volume',params={'symbol':'SPY','session_key':'x'}).status_code==403
def test_cutoffs():
 c=app(authorize=lambda:None)
 for s in ['2026-11-10T17:00:00','2026-11-11T17:00:00Z']:
  assert c.get('/api/v1/research/native-volume',params={'symbol':'SPY','session_key':'x','as_of':s}).status_code==422

def test_retained_metrics_private_api_contract():
 from datetime import timedelta
 from ingestion.realtime.research_intervals import MinuteBar,Session
 now=datetime(2026,11,10,17,tzinfo=timezone.utc)
 class Calendar:
  def __call__(self,key):return Session(key,now-timedelta(minutes=2),now+timedelta(minutes=2),'test-calendar')
  def prior_sessions(self,key,count):
   return tuple(Session(str(i),now-timedelta(days=11-i,minutes=2),now-timedelta(days=11-i)+timedelta(minutes=2),'test-calendar') for i in range(1,11))
 def load(symbol,source,s,asof):
  n=int((s.closes-s.opens).total_seconds()//60)
  return tuple(MinuteBar(symbol,source,s.opens+timedelta(minutes=i),now-timedelta(seconds=10),2,3,1,2,100,f'{s.key}-{i}',price_basis='provider_unspecified') for i in range(n) if s.opens+timedelta(minutes=i+1)<=now-timedelta(seconds=10))
 r=app(authorize=lambda:None,loader=load,calendar=Calendar()).get('/api/v1/research/native-volume',params={'symbol':'SPY','session_key':'current'})
 assert r.status_code==200
 p=r.json();assert p['status']=='ready';assert p['rvol']==1;assert p['bar_vwap_estimate']==2
 assert p['exact_trade_vwap'] is False and p['signal_confirmation'] is False
 assert p['historical_request'] is False and len(p['baseline_sessions'])==10
 assert 'private' in r.headers['cache-control']
