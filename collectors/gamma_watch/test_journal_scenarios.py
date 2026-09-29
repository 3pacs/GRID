import tempfile, unittest
from pathlib import Path
from datetime import datetime, timezone
from journal import Journal
from scenarios import price,grid

class JournalTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'journal.db';self.j=Journal(self.path);self.t=1790618400
    def tearDown(self):self.tmp.cleanup()
    def put(self,dt,p,level=770):
        now=self.t+dt;s={'broker_active':True,'quote':{'price':p,'as_of':datetime.fromtimestamp(now,timezone.utc).isoformat(),'source':'RTD','is_rtd':True},'gex':{'as_of':datetime.fromtimestamp(now,timezone.utc).isoformat(),'call_wall':level}}
        self.j.ingest(s,now)
    def kinds(self):return [e['kind'] for e in self.j.read()['events']]
    def test_durable_revision_not_cross(self):
        self.put(0,769);self.put(5,769,768);self.j=Journal(self.path)
        self.assertIn('level_revised',self.kinds());self.assertNotIn('crossed',self.kinds())
    def test_buffer_cross_and_duplicate(self):
        self.put(0,769);self.put(5,770);self.put(10,771);self.put(10,771)
        self.assertEqual(self.kinds().count('crossed'),1)
    def test_gap_not_cross(self):
        self.put(0,769);self.put(60,771);self.assertNotIn('crossed',self.kinds())
    def test_test_requires_duration(self):
        self.put(0,769);self.put(5,770);self.put(10,770)
        self.assertEqual(self.j.read()['interactions']['call_wall']['tests'],0)
        self.put(15,770);self.assertEqual(self.j.read()['interactions']['call_wall']['tests'],1)
    def test_receipt_filter(self):
        self.put(0,769);self.put(5,769,768)
        self.assertEqual(len(self.j.read(self.t+4)['events']),1)
    def test_return(self):
        self.put(0,769);self.put(5,771);self.put(10,769);self.assertIn('returned_through',self.kinds())

class ScenarioTests(unittest.TestCase):
    def test_put_call_parity(self):
        import math
        self.assertAlmostEqual(price(100,100,.1,.2,'C')-price(100,100,.1,.2,'P'),100*math.exp(-.012*.1)-100*math.exp(-.04*.1))
    def test_intrinsic(self):self.assertEqual(price(101,100,0,.2,'C'),1)
    def test_grid_and_invalid_bid(self):
        now=datetime(2026,9,28,18,tzinfo=timezone.utc)
        r={'symbol':'SPY','price':770,'received_at':now.isoformat()}
        c={'expiry':'2026-09-28','side':'C','strike':770,'bid':1,'ask':1.1,'iv':.2}
        d=grid(r,c,now=now);self.assertEqual(len(d['cells']),20);self.assertEqual(d['spread_cost_dollars'],10)
        self.assertLess(d['cells'][17]['value'],d['cells'][2]['value'])
        c['bid']=0;self.assertEqual(grid(r,c,now=now)['status'],'unavailable')
    def test_expired(self):
        now=datetime(2026,9,28,21,tzinfo=timezone.utc)
        self.assertEqual(grid({'symbol':'SPY','price':770},{'expiry':'2026-09-28','side':'C','strike':770,'bid':1,'ask':2,'iv':.2},now=now)['status'],'unavailable')

if __name__=='__main__':unittest.main()
