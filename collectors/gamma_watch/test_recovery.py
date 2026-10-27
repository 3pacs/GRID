import unittest
from unittest.mock import patch
from datetime import datetime,timezone
import broker
class RecoveryTests(unittest.TestCase):
 def test_recovery_and_optional_gamma(self):
  now=datetime.now(timezone.utc);ts=now.isoformat()
  cs=[dict(symbol=s,expiry='2026-09-25',strike=770,opt_type=t) for s,t in [('c','call'),('p','put')]]
  def fields(iv):return {k:dict(value=v,received_at=ts) for k,v in dict(IMPL_VOL=iv,OPEN_INT=10,BID=1,ASK=2).items()}
  fs={'c':fields(None),'p':fields('.2')}
  d=dict(collected_at=ts,heartbeat=1,update_count=1)
  with patch.object(broker,'CONTRACTS',cs):
   r=broker.build_feed(fs,d,now,771,771,771.01,dict(received_at=ts))
   self.assertEqual(r['direct_coverage'],.5);self.assertEqual(r['coverage'],1)
   self.assertEqual(r['recovered_contracts'],1);self.assertIsNone(r['rows'][0]['provider_gamma'])
   fs['p']['IMPL_VOL']['received_at']='2026-09-24T00:00:00Z'
   self.assertEqual(broker.build_feed(fs,d,now,771,771,771.01,dict(received_at=ts))['coverage'],0)
if __name__=='__main__':unittest.main()
