import unittest, tempfile, json
from pathlib import Path
from datetime import datetime, timezone
from structural import rebalance_amount, normalize_rtd, adjusted_series
from journal import Journal

class StructuralTests(unittest.TestCase):
 def test_rebalance_accounting(self):
  # 600m equity grows 10%, 400m bonds flat; target 60% of 1060m = 636m.
  self.assertAlmostEqual(rebalance_amount(.1,0),-24e6)
  self.assertAlmostEqual(rebalance_amount(0,.1),24e6)
  self.assertEqual(rebalance_amount(.1,.1),0)
 def test_invalid_returns(self):
  for r in [float('nan'),-1,None]:
   with self.assertRaises(ValueError):rebalance_amount(r,0)
 def feed(self):
  return {'schema':'structural-rtd-v1','collected_at':'2026-09-28T18:00:00Z','heartbeat':1,'records':[{'symbol':'$TICK','field':'LAST','value':0,'callback_seen':True,'callback_count':2,'received_at':'2026-09-28T18:00:00Z'}]}
 def test_zero_not_missing_and_nonnumeric_missing(self):
  now=datetime(2026,9,28,18,tzinfo=timezone.utc).timestamp()
  d=normalize_rtd(self.feed(),now);tick=next(r for r in d['rows'] if r['symbol']=='$TICK')
  self.assertEqual(tick['value'],0);self.assertTrue(tick['direction_usable'])
  self.assertIsNone(d['advance_minus_decline'])
 def test_cached_initial_and_stale_block(self):
  now=datetime(2026,9,28,18,tzinfo=timezone.utc).timestamp();f=self.feed();f['records'][0]['callback_count']=1
  self.assertFalse(any(r['direction_usable'] for r in normalize_rtd(f,now)['rows']))
  f['records'][0]['callback_count']=2
  self.assertFalse(any(r['direction_usable'] for r in normalize_rtd(f,now+21)['rows']))
 def test_after_hours_block(self):
  f=self.feed();f['collected_at']=f['records'][0]['received_at']='2026-09-28T21:00:00Z'
  d=normalize_rtd(f,datetime(2026,9,28,21,tzinfo=timezone.utc).timestamp())
  self.assertFalse(any(r['direction_usable'] for r in d['rows']))
 def test_partial_day_excluded(self):
  today=datetime(2026,9,28,13,30,tzinfo=timezone.utc).timestamp()
  doc={'chart':{'result':[{'timestamp':[today],'indicators':{'adjclose':[{'adjclose':[100]}]}}]}}
  self.assertEqual(adjusted_series(doc,today+3600),{})
  self.assertEqual(adjusted_series(doc,today+8*3600),{'2026-09-28':100})
 def test_persist_once_before_quote_gate(self):
  with tempfile.TemporaryDirectory() as tmp:
   j=Journal(Path(tmp)/'test.db');s={'structural_official':{'computed_at':'v1','sources':{}}}
   j.ingest(s,100);j.ingest(s,200)
   with j.db() as db:r=db.execute("SELECT received,body FROM artifacts WHERE kind='structural_official'").fetchall()
   self.assertEqual(len(r),1);self.assertEqual(r[0][0],100)
if __name__=='__main__':unittest.main()
